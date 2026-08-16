//! SubLink (subquery) extraction from WHERE clauses.
//!
//! Handles EXISTS, ANY/IN, ALL, and scalar sublinks by converting them
//! into joins (SEMI JOIN, ANTI JOIN, LATERAL JOIN) that the DVM engine
//! can differentiate.
//!
//! ## Module Structure (QUAL-4 v0.61.0)
//!
//! This module is split into focused sub-modules:
//! - [`having`] — HAVING aggregate rewrites (`rewrite_having_expr`, `is_star_only`)
//! - [`exists`] — EXISTS/NOT EXISTS → SemiJoin/AntiJoin
//! - [`in_list`] — IN/NOT IN → SemiJoin/AntiJoin, multi-column NULL-safety
//! - [`scalar`] — Scalar SubLink hoisting and subquery-to-SQL deparsing

pub mod exists;
pub mod having;
pub mod in_list;
pub mod scalar;

// Re-export having functions so callers can use `sublinks::rewrite_having_expr`.
pub(crate) use having::{is_star_only, rewrite_having_expr};

use super::*;
use crate::error::PgTrickleError;
use std::collections::{HashMap, HashSet};

// ── SubLink extraction for WHERE clauses ────────────────────────────────

/// Information about a SubLink extracted from the WHERE clause.
struct SublinkWrapper {
    /// Whether this is negated (NOT EXISTS / NOT IN).
    negated: bool,
    /// The join condition (correlation condition from the inner WHERE,
    /// possibly including the IN equality condition).
    condition: Expr,
    /// Parsed OpTree for the inner subquery's FROM clause.
    inner_tree: OpTree,
}

/// Walk a WHERE clause node tree and extract SubLinks into SemiJoin/AntiJoin
/// wrappers, returning the remaining non-SubLink predicates.
///
/// Handles:
/// - `EXISTS (SELECT ... FROM inner WHERE cond)` → SemiJoin
/// - `NOT EXISTS (SELECT ... FROM inner WHERE cond)` → AntiJoin
/// - `x IN (SELECT col FROM inner WHERE cond)` → SemiJoin with equality
/// - `x NOT IN (SELECT col FROM inner)` → AntiJoin with equality
/// - SubLinks under AND conjunctions (each extracted independently)
/// - SubLinks under OR → not supported (returns error)
///
/// # Safety
/// Caller must ensure `node` points to a valid `pg_sys::Node`.
fn extract_where_sublinks(
    node: *mut pg_sys::Node,
    cte_ctx: &mut CteParseContext,
) -> Result<(Vec<SublinkWrapper>, Option<Expr>), PgTrickleError> {
    // Case 1: The node itself is a SubLink
    // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
    if is_node_type!(node, T_SubLink) {
        let wrapper = parse_sublink_to_wrapper(node, false, cte_ctx)?;
        return Ok((vec![wrapper], None));
    }

    // Case 2: BoolExpr
    if let Some(boolexpr) = cast_node!(node, T_BoolExpr, pg_sys::BoolExpr) {
        // Case 2a: NOT wrapping a SubLink → negated
        if boolexpr.boolop == pg_sys::BoolExprType::NOT_EXPR {
            let args = pg_list::<pg_sys::Node>(boolexpr.args);
            if args.len() == 1 {
                // INVARIANT: args.len() == 1 guarantees head() returns Some.
                let arg = args.head().ok_or_else(|| {
                    PgTrickleError::InternalError(
                        "BoolExpr NOT_EXPR args list unexpectedly empty".into(),
                    )
                })?;
                // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
                if is_node_type!(arg, T_SubLink) {
                    let wrapper = parse_sublink_to_wrapper(arg, true, cte_ctx)?;
                    return Ok((vec![wrapper], None));
                }
            }
        }

        // Case 2b: AND conjunction — extract SubLinks from each arg
        if boolexpr.boolop == pg_sys::BoolExprType::AND_EXPR {
            let args = pg_list::<pg_sys::Node>(boolexpr.args);
            let mut wrappers = Vec::new();
            let mut remaining_exprs = Vec::new();

            for arg_ptr in args.iter_ptr() {
                if arg_ptr.is_null() {
                    continue;
                }

                // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
                if is_node_type!(arg_ptr, T_SubLink) {
                    // Direct SubLink under AND
                    let wrapper = parse_sublink_to_wrapper(arg_ptr, false, cte_ctx)?;
                    wrappers.push(wrapper);
                } else if let Some(inner_bool) = cast_node!(arg_ptr, T_BoolExpr, pg_sys::BoolExpr) {
                    // NOT SubLink under AND → negated
                    if inner_bool.boolop == pg_sys::BoolExprType::NOT_EXPR {
                        let inner_args = pg_list::<pg_sys::Node>(inner_bool.args);
                        if inner_args.len() == 1 {
                            // INVARIANT: inner_args.len() == 1 guarantees head() returns Some.
                            let inner_arg = inner_args.head().ok_or_else(|| {
                                PgTrickleError::InternalError(
                                    "BoolExpr NOT_EXPR inner args list unexpectedly empty".into(),
                                )
                            })?;
                            // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
                            if is_node_type!(inner_arg, T_SubLink) {
                                let wrapper = parse_sublink_to_wrapper(inner_arg, true, cte_ctx)?;
                                wrappers.push(wrapper);
                                continue;
                            }
                        }
                    }
                    // Check for SubLinks inside OR — should have been
                    // rewritten to UNION by rewrite_sublinks_in_or().
                    // If still present, it's a deeply nested case.
                    if inner_bool.boolop == pg_sys::BoolExprType::OR_EXPR
                        && node_tree_contains_sublink(arg_ptr)
                    {
                        return Err(PgTrickleError::UnsupportedOperator(
                            "Subquery expressions (EXISTS, IN) inside OR conditions are not \
                             supported in this nesting pattern. Consider rewriting \
                             using UNION or separate stream tables."
                                .into(),
                        ));
                    }
                    // Regular boolean expression — keep as remaining
                    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                    remaining_exprs.push(safe_node_to_expr(arg_ptr)?);
                } else {
                    // Regular predicate — keep as remaining
                    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                    remaining_exprs.push(safe_node_to_expr(arg_ptr)?);
                }
            }

            let remaining = if remaining_exprs.is_empty() {
                None
            } else {
                Some(
                    remaining_exprs
                        .into_iter()
                        .reduce(|acc, expr| Expr::BinaryOp {
                            op: "AND".to_string(),
                            left: Box::new(acc),
                            right: Box::new(expr),
                        })
                        .ok_or_else(|| {
                            PgTrickleError::InternalError(
                                "AND expression list was non-empty but reduce produced None".into(),
                            )
                        })?,
                )
            };

            return Ok((wrappers, remaining));
        }

        // Case 2c: OR containing SubLinks — should have been
        // rewritten to UNION by rewrite_sublinks_in_or().
        // If still present, it's a deeply nested case.
        if boolexpr.boolop == pg_sys::BoolExprType::OR_EXPR && node_tree_contains_sublink(node) {
            return Err(PgTrickleError::UnsupportedOperator(
                "Subquery expressions (EXISTS, IN) inside OR conditions are not \
                 supported in this nesting pattern. Consider rewriting \
                 using UNION or separate stream tables."
                    .into(),
            ));
        }
    }

    // No SubLinks found — return the whole expression as remaining predicate
    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
    let expr = safe_node_to_expr(node)?;
    Ok((vec![], Some(expr)))
}

/// Check if a node tree contains any T_SubLink node (shallow walk).
///
/// # Safety
/// Caller must ensure `node` points to a valid `pg_sys::Node`.
pub(crate) fn node_tree_contains_sublink(node: *mut pg_sys::Node) -> bool {
    if node.is_null() {
        return false;
    }
    // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
    if is_node_type!(node, T_SubLink) {
        return true;
    }
    if let Some(boolexpr) = cast_node!(node, T_BoolExpr, pg_sys::BoolExpr)
        && !boolexpr.args.is_null()
    {
        let args = pg_list::<pg_sys::Node>(boolexpr.args);
        for arg_ptr in args.iter_ptr() {
            if !arg_ptr.is_null() && node_tree_contains_sublink(arg_ptr) {
                return true;
            }
        }
    }
    // CORR-2 (v0.30.0): Recurse into CASE expressions (T_CaseExpr → arg, defresult, CaseWhen args).
    if let Some(case_expr) = cast_node!(node, T_CaseExpr, pg_sys::CaseExpr) {
        // SAFETY: CaseExpr fields are valid parser-allocated nodes.
        if !case_expr.arg.is_null()
            && node_tree_contains_sublink(case_expr.arg as *mut pg_sys::Node)
        {
            return true;
        }
        if !case_expr.defresult.is_null()
            && node_tree_contains_sublink(case_expr.defresult as *mut pg_sys::Node)
        {
            return true;
        }
        if !case_expr.args.is_null() {
            let whens = pg_list::<pg_sys::CaseWhen>(case_expr.args);
            for when_ptr in whens.iter_ptr() {
                if when_ptr.is_null() {
                    continue;
                }
                // SAFETY: CaseWhen is a valid parser node with expr and result fields.
                let when = unsafe { &*when_ptr };
                if !when.expr.is_null()
                    && node_tree_contains_sublink(when.expr as *mut pg_sys::Node)
                {
                    return true;
                }
                if !when.result.is_null()
                    && node_tree_contains_sublink(when.result as *mut pg_sys::Node)
                {
                    return true;
                }
            }
        }
    }
    // CORR-2 (v0.30.0): Recurse into COALESCE expressions (T_CoalesceExpr → args list).
    if let Some(coalesce) = cast_node!(node, T_CoalesceExpr, pg_sys::CoalesceExpr)
        && !coalesce.args.is_null()
    {
        let args = pg_list::<pg_sys::Node>(coalesce.args);
        for arg_ptr in args.iter_ptr() {
            if !arg_ptr.is_null() && node_tree_contains_sublink(arg_ptr) {
                return true;
            }
        }
    }
    // CORR-2 (v0.30.0): Recurse into function-call argument lists (T_FuncCall → args).
    if let Some(fcall) = cast_node!(node, T_FuncCall, pg_sys::FuncCall)
        && !fcall.args.is_null()
    {
        let args = pg_list::<pg_sys::Node>(fcall.args);
        for arg_ptr in args.iter_ptr() {
            if !arg_ptr.is_null() && node_tree_contains_sublink(arg_ptr) {
                return true;
            }
        }
    }
    false
}

/// Recursively flatten a boolean AND tree into a flat list of conjunct nodes.
///
/// `AND(a, AND(b, c))` → \[a, b, c\]; non-AND nodes are pushed directly.
///
/// # Safety
/// Caller must ensure `node` points to a valid parse-tree Node.
pub(crate) fn flatten_and_conjuncts(node: *mut pg_sys::Node, result: &mut Vec<*mut pg_sys::Node>) {
    if node.is_null() {
        return;
    }
    if let Some(be) = cast_node!(node, T_BoolExpr, pg_sys::BoolExpr)
        && be.boolop == pg_sys::BoolExprType::AND_EXPR
        && !be.args.is_null()
    {
        let args = pg_list::<pg_sys::Node>(be.args);
        for arg_ptr in args.iter_ptr() {
            flatten_and_conjuncts(arg_ptr, result);
        }
        return;
    }
    result.push(node);
}

/// Check if an AND expression (at any depth) contains at least one OR
/// conjunct that itself contains SubLink nodes.
///
/// Recurses into nested AND layers so that `AND(p, AND(q, OR(EXISTS(...))))`
/// is detected correctly. This is stricter than `node_tree_contains_sublink`
/// which returns true for AND + EXISTS (no OR), causing unnecessary deparsing.
///
/// # Safety
/// Caller must ensure `node` points to a valid `pg_sys::Node`.
pub(crate) fn and_contains_or_with_sublink(node: *mut pg_sys::Node) -> bool {
    if node.is_null() {
        return false;
    }
    // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
    if !is_node_type!(node, T_BoolExpr) {
        return false;
    }
    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let boolexpr = pg_deref!(node as *const pg_sys::BoolExpr);
    if boolexpr.boolop != pg_sys::BoolExprType::AND_EXPR {
        return false;
    }
    // Flatten the whole AND tree then check whether any conjunct is
    // an OR containing SubLink nodes.
    let mut conjuncts: Vec<*mut pg_sys::Node> = Vec::new();
    flatten_and_conjuncts(node, &mut conjuncts);
    for conj in conjuncts {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if !conj.is_null() && is_node_type!(conj, T_BoolExpr) {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let inner = pg_deref!(conj as *const pg_sys::BoolExpr);
            if inner.boolop == pg_sys::BoolExprType::OR_EXPR && node_tree_contains_sublink(conj) {
                return true;
            }
        }
    }
    false
}

/// Deparse a SelectStmt (or Node) back to SQL text.
///
/// Uses PostgreSQL's `nodeToString()` for a basic representation, then
/// reconstructs the SQL from the parse tree components. This is a simplified
/// deparsing that handles common cases for scalar subqueries and EXISTS.
///
/// # Safety
/// Caller must ensure `select_node` points to a valid pg_sys::Node
/// (typically a SelectStmt).
pub(crate) fn deparse_select_to_sql(
    select_node: *mut pg_sys::Node,
) -> Result<String, PgTrickleError> {
    if select_node.is_null() {
        return Err(PgTrickleError::QueryParseError(
            "NULL node in deparse_select_to_sql".into(),
        ));
    }

    // Check if it's a SelectStmt
    // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
    if !is_node_type!(select_node, T_SelectStmt) {
        return Err(PgTrickleError::QueryParseError(
            "Expected SelectStmt in deparse_select_to_sql".into(),
        ));
    }

    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let select = pg_deref!(select_node as *const pg_sys::SelectStmt);
    let mut sql = String::from("SELECT ");

    // Deparse target list
    let target_list = pg_list::<pg_sys::Node>(select.targetList);
    let mut targets = Vec::new();
    for node_ptr in target_list.iter_ptr() {
        if let Some(rt) = cast_node!(node_ptr, T_ResTarget, pg_sys::ResTarget)
            && !rt.val.is_null()
        {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let expr = safe_node_to_expr(rt.val)?;
            let expr_sql = expr.to_sql();
            if !rt.name.is_null() {
                let name = pg_cstr_to_str(rt.name).unwrap_or("?");
                targets.push(format!("{expr_sql} AS \"{name}\""));
            } else {
                targets.push(expr_sql);
            }
        }
    }
    sql.push_str(&targets.join(", "));

    // Deparse FROM clause
    if !select.fromClause.is_null() {
        sql.push_str(" FROM ");
        sql.push_str(&extract_from_clause_sql(select)?);
    }

    // Deparse WHERE clause
    if !select.whereClause.is_null() {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let where_expr = safe_node_to_expr(select.whereClause)?;
        sql.push_str(&format!(" WHERE {}", where_expr.to_sql()));
    }

    // Deparse GROUP BY
    if !select.groupClause.is_null() {
        let group_list = pg_list::<pg_sys::Node>(select.groupClause);
        if !group_list.is_empty() {
            let mut groups = Vec::new();
            for node_ptr in group_list.iter_ptr() {
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let expr = safe_node_to_expr(node_ptr)?;
                groups.push(expr.to_sql());
            }
            sql.push_str(&format!(" GROUP BY {}", groups.join(", ")));
        }
    }

    // Deparse HAVING
    if !select.havingClause.is_null() {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let having_expr = safe_node_to_expr(select.havingClause)?;
        sql.push_str(&format!(" HAVING {}", having_expr.to_sql()));
    }

    // Deparse ORDER BY
    let order = deparse_order_clause(select);
    if !order.is_empty() {
        sql.push_str(&order);
    }

    // Deparse LIMIT
    if !select.limitCount.is_null() {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let limit_expr = safe_node_to_expr(select.limitCount)?;
        sql.push_str(&format!(" LIMIT {}", limit_expr.to_sql()));
    }

    // Deparse OFFSET
    if !select.limitOffset.is_null() {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let offset_expr = safe_node_to_expr(select.limitOffset)?;
        sql.push_str(&format!(" OFFSET {}", offset_expr.to_sql()));
    }

    Ok(sql)
}

/// Parse a SubLink node into a SublinkWrapper for SemiJoin/AntiJoin construction.
///
/// # Safety
/// Caller must ensure `node` points to a valid `pg_sys::SubLink` (T_SubLink node).
fn parse_sublink_to_wrapper(
    node: *mut pg_sys::Node,
    negated: bool,
    cte_ctx: &mut CteParseContext,
) -> Result<SublinkWrapper, PgTrickleError> {
    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let sublink = pg_deref!(node as *const pg_sys::SubLink);

    match sublink.subLinkType {
        pg_sys::SubLinkType::EXISTS_SUBLINK => parse_exists_sublink(sublink, negated, cte_ctx),
        pg_sys::SubLinkType::ANY_SUBLINK => {
            // ANY_SUBLINK is used for both `x IN (SELECT ...)` and `x = ANY (SELECT ...)`
            parse_any_sublink(sublink, negated, cte_ctx)
        }
        pg_sys::SubLinkType::ALL_SUBLINK => {
            // ALL_SUBLINK: `x op ALL (SELECT col FROM ...)`.
            // Rewritten as AntiJoin with negated condition:
            // NOT EXISTS (SELECT 1 FROM ... WHERE NOT (x op col))
            parse_all_sublink(sublink, negated, cte_ctx)
        }
        pg_sys::SubLinkType::EXPR_SUBLINK => {
            // Scalar subquery in WHERE — should have been rewritten to
            // CROSS JOIN by rewrite_scalar_subquery_in_where(). If we
            // still reach here, it's a case the rewrite didn't catch
            // (e.g., deeply nested or correlated). Reject with a helpful
            // error message.
            Err(PgTrickleError::UnsupportedOperator(
                "Scalar subqueries in WHERE clauses are not supported for DIFFERENTIAL mode. \
                 This usually means the query has a correlated scalar subquery that cannot \
                 be automatically rewritten. Consider rewriting as a JOIN or CTE."
                    .into(),
            ))
        }
        _ => Err(PgTrickleError::UnsupportedOperator(
            "Unsupported subquery type in WHERE clause. \
             Rewrite using a JOIN or LATERAL subquery."
                .to_string(),
        )),
    }
}

/// Parse an EXISTS SubLink into a SublinkWrapper.
///
/// Simple form (no GROUP BY / HAVING):
/// `EXISTS (SELECT ... FROM inner_table WHERE correlation_cond AND inner_filter)`
/// → The inner WHERE becomes the semi/anti-join condition.
///
/// Grouped form (with GROUP BY / HAVING):
/// `EXISTS (SELECT 1 FROM T WHERE inner.k = outer.k GROUP BY inner.k HAVING agg > threshold)`
///
/// This is semantically equivalent to:
/// `outer.k IN (SELECT inner.k FROM T GROUP BY inner.k HAVING agg > threshold)`
///
/// The correlation predicate (`inner.k = outer.k`) is extracted from the inner WHERE
/// and converted to the SemiJoin join condition. The remaining (non-correlated)
/// inner WHERE predicates are applied as a pre-aggregate filter. The inner tree is
/// wrapped in `Aggregate → Filter(HAVING) → Subquery`, matching the `parse_any_sublink`
/// GROUP BY treatment.
///
/// # Safety
/// Caller must ensure `sublink` points to a valid `pg_sys::SubLink`.
fn parse_exists_sublink(
    sublink: &pg_sys::SubLink,
    negated: bool,
    cte_ctx: &mut CteParseContext,
) -> Result<SublinkWrapper, PgTrickleError> {
    if sublink.subselect.is_null() {
        return Err(PgTrickleError::QueryParseError(
            "EXISTS subquery has NULL subselect".into(),
        ));
    }

    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let inner_select = pg_deref!(sublink.subselect as *const pg_sys::SelectStmt);

    // Parse the inner FROM clause into an OpTree
    let from_list = pg_list::<pg_sys::Node>(inner_select.fromClause);
    if from_list.is_empty() {
        return Err(PgTrickleError::QueryParseError(
            "EXISTS subquery must have a FROM clause".into(),
        ));
    }

    // INVARIANT: from_list is non-empty (error returned above), so head() cannot fail.
    // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
    let mut inner_tree = unsafe {
        parse_from_item(
            from_list.head().ok_or_else(|| {
                PgTrickleError::InternalError(
                    "EXISTS sublink from_list unexpectedly empty after non-empty check".into(),
                )
            })?,
            cte_ctx,
        )?
    };

    // Handle multiple FROM items (implicit cross joins in subquery)
    for i in 1..from_list.len() {
        if let Some(item) = from_list.get_ptr(i) {
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            let right = unsafe { parse_from_item(item, cte_ctx)? };
            inner_tree = OpTree::InnerJoin {
                condition: Expr::Literal("TRUE".into()),
                left: Box::new(inner_tree),
                right: Box::new(right),
            };
        }
    }

    // ── GROUP BY / HAVING handling ──────────────────────────────────
    //
    // When the inner SELECT has GROUP BY or HAVING, a flat SemiJoin on the
    // raw scan would lose the aggregation semantics entirely. We restructure
    // the inner tree to:
    //
    //   Subquery(
    //     Filter(HAVING, Aggregate(GROUP BY inner.k, ..., child=Scan(T))))
    //
    // and derive the SemiJoin join condition by extracting the correlation
    // predicate from the inner WHERE clause.
    //
    // A correlation predicate has the form `inner.col = outer.col` (or the
    // reverse), where `inner.col` refers to one of the inner FROM aliases
    // and `outer.col` refers to an alias from the enclosing query.
    //
    // Example transformation:
    //   EXISTS (SELECT 1 FROM eh_ord o
    //           WHERE o.cust_id = c.id
    //           GROUP BY o.cust_id
    //           HAVING SUM(o.amount) > 100)
    // →
    //   SemiJoin(cond  = c.id = __pgt_in_sub.cust_id,
    //            left  = Scan(eh_cust c),
    //            right = Subquery(Filter(HAVING sum > 100,
    //                      Aggregate(GROUP BY o.cust_id,
    //                        Scan(eh_ord o)))))
    let group_list = pg_list::<pg_sys::Node>(inner_select.groupClause);
    let has_group_by = !group_list.is_empty();
    let has_having = !inner_select.havingClause.is_null();

    if has_group_by || has_having {
        // Collect the inner FROM aliases to distinguish correlated references.
        let inner_aliases = collect_tree_source_aliases(&inner_tree);

        // Split the inner WHERE into:
        //   corr_pairs  — (outer_expr, inner_group_key_output_name)
        //   inner_filter — everything else (applied before aggregation)
        let (corr_pairs, inner_filter) = if !inner_select.whereClause.is_null() {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let where_expr = safe_node_to_expr(inner_select.whereClause)?;
            split_exists_correlation(&where_expr, &inner_aliases)
        } else {
            (Vec::new(), None)
        };

        // Apply the non-correlated inner WHERE as a pre-aggregate filter.
        if let Some(filter_expr) = inner_filter {
            inner_tree = OpTree::Filter {
                predicate: filter_expr,
                child: Box::new(inner_tree),
            };
        }

        // Parse GROUP BY expressions.
        let mut group_by = Vec::new();
        for node_ptr in group_list.iter_ptr() {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let expr = safe_node_to_expr(node_ptr)?;
            group_by.push(expr);
        }

        // Extract aggregates from the HAVING clause (SELECT 1 has no agg targets).
        let mut aggregates: Vec<AggExpr> = Vec::new();
        if has_having {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let having_expr = safe_node_to_expr(inner_select.havingClause)?;
            let having_aggs = extract_aggregates_from_expr(&having_expr, 0);
            aggregates.extend(having_aggs);
        }

        // Build the Aggregate node.
        inner_tree = OpTree::Aggregate {
            group_by,
            aggregates: aggregates.clone(),
            child: Box::new(inner_tree),
        };

        // Apply HAVING as Filter on top of Aggregate.
        if has_having {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let having_expr = safe_node_to_expr(inner_select.havingClause)?;
            let rewritten = rewrite_having_expr(&having_expr, &aggregates);
            inner_tree = OpTree::Filter {
                predicate: rewritten,
                child: Box::new(inner_tree),
            };
        }

        // Wrap in Subquery so the SemiJoin sees it as a derived table.
        let sub_alias = format!("__pgt_in_sub_{}", inner_tree.alias());
        inner_tree = OpTree::Subquery {
            alias: sub_alias.clone(),
            column_aliases: Vec::new(),
            child: Box::new(inner_tree),
        };

        // Build the SemiJoin condition from extracted correlation pairs.
        // Each pair produces: outer_expr = subquery_alias.inner_col_output_name.
        let condition = if !corr_pairs.is_empty() {
            let equalities: Vec<Expr> = corr_pairs
                .into_iter()
                .map(|(outer_expr, inner_col_name)| Expr::BinaryOp {
                    op: "=".to_string(),
                    left: Box::new(outer_expr),
                    right: Box::new(Expr::ColumnRef {
                        table_alias: Some(sub_alias.clone()),
                        column_name: inner_col_name,
                    }),
                })
                .collect();
            equalities
                .into_iter()
                .reduce(|acc, eq| Expr::BinaryOp {
                    op: "AND".to_string(),
                    left: Box::new(acc),
                    right: Box::new(eq),
                })
                .ok_or_else(|| {
                    PgTrickleError::InternalError(
                        "corr_pairs was non-empty but reduce produced None".into(),
                    )
                })?
        } else {
            // No correlation found — let the pre-aggregate filter handle
            // all filtering; the SemiJoin condition is a tautology.
            Expr::Literal("TRUE".into())
        };

        return Ok(SublinkWrapper {
            negated,
            condition,
            inner_tree,
        });
    }

    // ── Simple form: no GROUP BY / HAVING ───────────────────────────

    // The inner WHERE clause becomes the semi/anti-join condition.
    let condition = if inner_select.whereClause.is_null() {
        Expr::Literal("TRUE".into())
    } else {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        safe_node_to_expr(inner_select.whereClause)?
    };

    Ok(SublinkWrapper {
        negated,
        condition,
        inner_tree,
    })
}

/// Collect all base-table Scan aliases from an OpTree.
///
/// Used by `parse_exists_sublink` to identify which column references in an
/// EXISTS inner WHERE clause belong to the inner subquery vs. the outer query.
pub(crate) fn collect_tree_source_aliases(tree: &OpTree) -> Vec<String> {
    match tree {
        OpTree::Scan { alias, .. } | OpTree::CteScan { alias, .. } => vec![alias.clone()],
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. } => {
            let mut aliases = collect_tree_source_aliases(left);
            aliases.extend(collect_tree_source_aliases(right));
            aliases
        }
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Distinct { child, .. }
        | OpTree::Aggregate { child, .. } => collect_tree_source_aliases(child),
        OpTree::Subquery { alias, .. } => vec![alias.clone()],
        _ => vec![],
    }
}

/// Split an EXISTS inner WHERE expression into correlation predicates and
/// inner-only predicates.
///
/// Walks the conjuncts of `where_expr` (splitting on `AND`) and classifies
/// each conjunct:
///
/// - Correlation predicate: an equality `inner_alias.col = outer_ref` (or the
///   reverse) where `inner_alias` is in `inner_aliases` and the other side is
///   a column reference from an alias NOT in `inner_aliases` (i.e., an outer
///   reference). Each such predicate is extracted as `(outer_expr, inner_col_name)`.
///
/// - Inner-only predicate: everything else; left in the filter applied before
///   aggregation.
///
/// Returns `(corr_pairs, remaining_filter)`.
pub(crate) fn split_exists_correlation(
    where_expr: &Expr,
    inner_aliases: &[String],
) -> (Vec<(Expr, String)>, Option<Expr>) {
    match where_expr {
        Expr::BinaryOp { op, left, right } if op == "AND" => {
            let (mut lc, lr) = split_exists_correlation(left, inner_aliases);
            let (rc, rr) = split_exists_correlation(right, inner_aliases);
            lc.extend(rc);
            let remaining = match (lr, rr) {
                (Some(l), Some(r)) => Some(Expr::BinaryOp {
                    op: "AND".to_string(),
                    left: Box::new(l),
                    right: Box::new(r),
                }),
                (Some(l), None) | (None, Some(l)) => Some(l),
                (None, None) => None,
            };
            (lc, remaining)
        }
        _ => {
            if let Some(pair) = try_extract_exists_corr_pair(where_expr, inner_aliases) {
                (vec![pair], None)
            } else {
                (vec![], Some(where_expr.clone()))
            }
        }
    }
}

/// Try to extract `(outer_expr, inner_col_output_name)` from an equality predicate.
///
/// Succeeds when:
/// - The expression is `A = B`
/// - Exactly one of A, B is a `ColumnRef` with a qualifier in `inner_aliases`
/// - The other is a `ColumnRef` with a qualifier NOT in `inner_aliases` (outer ref)
///
/// Returns `None` for non-equality predicates or predicates where both / neither
/// side is an inner reference.
fn try_extract_exists_corr_pair(pred: &Expr, inner_aliases: &[String]) -> Option<(Expr, String)> {
    let Expr::BinaryOp { op, left, right } = pred else {
        return None;
    };
    if op != "=" {
        return None;
    }

    let left_qualifier = match left.as_ref() {
        Expr::ColumnRef {
            table_alias: Some(a),
            ..
        } => Some(a.as_str()),
        _ => None,
    };
    let right_qualifier = match right.as_ref() {
        Expr::ColumnRef {
            table_alias: Some(a),
            ..
        } => Some(a.as_str()),
        _ => None,
    };

    match (left_qualifier, right_qualifier) {
        (Some(la), Some(ra)) => {
            let left_is_inner = inner_aliases.iter().any(|a| a == la);
            let right_is_inner = inner_aliases.iter().any(|a| a == ra);
            if left_is_inner && !right_is_inner {
                // inner.col = outer.ref → outer_expr=right, inner_col=left.output_name()
                Some((*right.clone(), left.output_name()))
            } else if !left_is_inner && right_is_inner {
                // outer.ref = inner.col → outer_expr=left, inner_col=right.output_name()
                Some((*left.clone(), right.output_name()))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Qualify unqualified column references in an expression with the given
/// table alias.  Used for IN-subquery inner expressions to prevent ambiguity
/// when the inner SELECT target shares column names with the outer query
/// (e.g., both tables have an `id` column).  Without qualification the
/// join-condition rewriter resolves the bare name against the left (outer)
/// tree first, producing incorrect delta SQL.
fn qualify_inner_col_refs(expr: Expr, alias: &str) -> Expr {
    match expr {
        Expr::ColumnRef {
            table_alias: None,
            column_name,
        } => Expr::ColumnRef {
            table_alias: Some(alias.to_string()),
            column_name,
        },
        Expr::BinaryOp { op, left, right } => Expr::BinaryOp {
            op,
            left: Box::new(qualify_inner_col_refs(*left, alias)),
            right: Box::new(qualify_inner_col_refs(*right, alias)),
        },
        Expr::FuncCall { func_name, args } => Expr::FuncCall {
            func_name,
            args: args
                .into_iter()
                .map(|a| qualify_inner_col_refs(a, alias))
                .collect(),
        },
        other => other,
    }
}

/// Parse an ANY SubLink (IN / = ANY) into a SublinkWrapper.
///
/// `x IN (SELECT col FROM inner_table WHERE filter)`
/// is equivalent to:
/// `EXISTS (SELECT 1 FROM inner_table WHERE inner_table.col = x AND filter)`
///
/// When the inner SELECT has GROUP BY / HAVING, the inner tree is wrapped
/// in an Aggregate (+ HAVING Filter) so the SemiJoin only matches qualifying
/// groups:
/// `x IN (SELECT col FROM T GROUP BY col HAVING agg > threshold)`
/// → SemiJoin(cond = x = sub.col,
///            inner = Subquery(Filter(HAVING, Aggregate(GROUP BY col, child=T))))
///
/// # Safety
/// Caller must ensure `sublink` points to a valid `pg_sys::SubLink`.
fn parse_any_sublink(
    sublink: &pg_sys::SubLink,
    negated: bool,
    cte_ctx: &mut CteParseContext,
) -> Result<SublinkWrapper, PgTrickleError> {
    if sublink.subselect.is_null() {
        return Err(PgTrickleError::QueryParseError(
            "IN/ANY subquery has NULL subselect".into(),
        ));
    }

    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let inner_select = pg_deref!(sublink.subselect as *const pg_sys::SelectStmt);

    // Parse the inner FROM clause
    let from_list = pg_list::<pg_sys::Node>(inner_select.fromClause);
    if from_list.is_empty() {
        return Err(PgTrickleError::QueryParseError(
            "IN subquery must have a FROM clause".into(),
        ));
    }

    // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
    let mut inner_tree = unsafe {
        parse_from_item(
            from_list.head().ok_or_else(|| {
                PgTrickleError::InternalError(
                    "IN sublink from_list unexpectedly empty after non-empty check".into(),
                )
            })?,
            cte_ctx,
        )?
    };
    for i in 1..from_list.len() {
        if let Some(item) = from_list.get_ptr(i) {
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            let right = unsafe { parse_from_item(item, cte_ctx)? };
            inner_tree = OpTree::InnerJoin {
                condition: Expr::Literal("TRUE".into()),
                left: Box::new(inner_tree),
                right: Box::new(right),
            };
        }
    }

    // Extract the test expression (left-hand side of IN)
    let test_expr = if sublink.testexpr.is_null() {
        return Err(PgTrickleError::QueryParseError(
            "IN subquery has NULL test expression".into(),
        ));
    } else {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        safe_node_to_expr(sublink.testexpr)?
    };

    // ── M-5 (v0.55.0): Multi-column IN → EXISTS-equivalent SemiJoin ─
    //
    // `(a, b) IN (SELECT x, y FROM t)` is automatically rewritten to a
    // SemiJoin with condition `a=x AND b=y [AND inner_where]`, which is
    // semantically equivalent to EXISTS (SELECT 1 FROM t WHERE a=x AND b=y).
    // Previously this case returned an unsupported-feature error (G12-SQL-IN).

    // Extract the inner SELECT target list.
    let target_list = pg_list::<pg_sys::Node>(inner_select.targetList);
    if target_list.is_empty() {
        return Err(PgTrickleError::QueryParseError(
            "IN subquery SELECT list is empty".into(),
        ));
    }

    let is_row_test = matches!(test_expr, Expr::Raw(ref s) if s.starts_with("ROW("));
    let is_multi_col = target_list.len() > 1;

    if is_row_test || is_multi_col {
        // Extract left-side exprs from the RowExpr (if present).
        let left_exprs: Vec<Expr> =
            if let Some(rowexpr) = cast_node!(sublink.testexpr, T_RowExpr, pg_sys::RowExpr) {
                let fields = pg_list::<pg_sys::Node>(rowexpr.args);
                let mut exprs = Vec::with_capacity(fields.len());
                for n in fields.iter_ptr() {
                    // SAFETY: parse-tree pointer from raw_parser.
                    exprs.push(safe_node_to_expr(n)?);
                }
                exprs
            } else {
                vec![test_expr.clone()]
            };

        // Extract right-side exprs from the inner SELECT target list.
        let mut right_exprs: Vec<Expr> = Vec::with_capacity(target_list.len());
        for node_ptr in target_list.iter_ptr() {
            if let Some(rt) = cast_node!(node_ptr, T_ResTarget, pg_sys::ResTarget) {
                if rt.val.is_null() {
                    return Err(PgTrickleError::QueryParseError(
                        "multi-column IN: target column is NULL".into(),
                    ));
                }
                // SAFETY: parse-tree pointer from raw_parser.
                right_exprs.push(safe_node_to_expr(rt.val)?);
            } else {
                return Err(PgTrickleError::QueryParseError(
                    "multi-column IN: target is not a ResTarget".into(),
                ));
            }
        }

        if left_exprs.len() != right_exprs.len() {
            return Err(PgTrickleError::QueryParseError(format!(
                "multi-column IN (subquery): left side has {} column(s) but \
                 inner SELECT has {} column(s); counts must match.",
                left_exprs.len(),
                right_exprs.len(),
            )));
        }

        // Build: col1=tgt1 AND col2=tgt2 AND ... [AND inner_where].
        // COR-1: Save clones for the NULL-safety check applied after condition is built.
        let left_exprs_for_null_check: Vec<Expr> = left_exprs.clone();
        let right_exprs_for_null_check: Vec<Expr> = right_exprs.clone();
        let eq_chain = left_exprs
            .into_iter()
            .zip(right_exprs)
            .map(|(l, r)| Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(l),
                right: Box::new(r),
            })
            .reduce(|acc, eq| Expr::BinaryOp {
                op: "AND".to_string(),
                left: Box::new(acc),
                right: Box::new(eq),
            })
            .ok_or_else(|| {
                PgTrickleError::InternalError(
                    "multi-column IN: column list was empty after length check".into(),
                )
            })?;

        let condition = if inner_select.whereClause.is_null() {
            eq_chain
        } else {
            // SAFETY: parse-tree pointer from raw_parser.
            let inner_where = safe_node_to_expr(inner_select.whereClause)?;
            Expr::BinaryOp {
                op: "AND".to_string(),
                left: Box::new(eq_chain),
                right: Box::new(inner_where),
            }
        };

        // COR-1: NULL safety check for multi-column NOT IN.
        // AntiJoin semantics differ from SQL NOT IN when NULL is involved:
        // SQL NOT IN returns UNKNOWN (exclude row) when any comparison is NULL,
        // but AntiJoin keeps the row on UNKNOWN correlation predicates.
        // Skip the AntiJoin rewrite when any left-side or right-side element is
        // a NULL constant — fall back to subquery-based delta computation instead.
        if negated {
            let left_has_null_literal = left_exprs_for_null_check
                .iter()
                .any(|e| matches!(e, Expr::Raw(s) if s.trim().eq_ignore_ascii_case("null")));
            let right_has_null_literal = right_exprs_for_null_check
                .iter()
                .any(|e| matches!(e, Expr::Raw(s) if s.trim().eq_ignore_ascii_case("null")));
            if left_has_null_literal || right_has_null_literal {
                pgrx::notice!(
                    "pg_trickle: multi-column NOT IN with nullable elements cannot be \
                     rewritten to an anti-join; falling back to subquery-based delta computation."
                );
                return Err(PgTrickleError::UnsupportedOperator(
                    "multi-column NOT IN with nullable elements cannot be rewritten to an anti-join"
                        .into(),
                ));
            }
        }

        return Ok(SublinkWrapper {
            negated,
            condition,
            inner_tree,
        });
    }

    // ── Single-column IN (original path) ────────────────────────────
    let first_target = target_list.head().ok_or_else(|| {
        PgTrickleError::InternalError(
            "IN sublink target_list unexpectedly empty after non-empty check".into(),
        )
    })?;
    let inner_col_expr = if let Some(rt) = cast_node!(first_target, T_ResTarget, pg_sys::ResTarget)
    {
        if rt.val.is_null() {
            return Err(PgTrickleError::QueryParseError(
                "IN subquery target column is NULL".into(),
            ));
        }
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        safe_node_to_expr(rt.val)?
    } else {
        return Err(PgTrickleError::QueryParseError(
            "IN subquery target is not a ResTarget".into(),
        ));
    };

    // ── GROUP BY / HAVING handling ──────────────────────────────────
    //
    // When the inner SELECT has GROUP BY or HAVING, the flat SemiJoin
    // rewrite would lose the grouping semantics. Instead, wrap the inner
    // tree in Aggregate + Filter(HAVING) so only qualifying groups are
    // considered for the semi-join match.
    let group_list = pg_list::<pg_sys::Node>(inner_select.groupClause);
    let has_group_by = !group_list.is_empty();
    let has_having = !inner_select.havingClause.is_null();

    if has_group_by || has_having {
        // Apply inner WHERE as a Filter on the FROM tree
        if !inner_select.whereClause.is_null() {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let inner_where = safe_node_to_expr(inner_select.whereClause)?;
            inner_tree = OpTree::Filter {
                predicate: inner_where,
                child: Box::new(inner_tree),
            };
        }

        // Parse GROUP BY expressions
        let mut group_by = Vec::new();
        for node_ptr in group_list.iter_ptr() {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let expr = safe_node_to_expr(node_ptr)?;
            group_by.push(expr);
        }

        // Extract aggregates from the target list
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        let (mut aggregates, _non_agg_exprs) = unsafe { extract_aggregates(&target_list)? };

        // Extract additional aggregates from HAVING expression.
        // The HAVING clause may reference aggregates not present in the
        // SELECT list (e.g., `SELECT col FROM T GROUP BY col HAVING SUM(x) > 0`).
        if has_having {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let having_expr = safe_node_to_expr(inner_select.havingClause)?;
            let having_aggs = extract_aggregates_from_expr(&having_expr, aggregates.len());
            for ha in &having_aggs {
                // Avoid duplicates: only add if no existing aggregate matches
                let already_present = aggregates.iter().any(|a| {
                    a.function.sql_name() == ha.function.sql_name()
                        && a.argument.as_ref().map(|e| e.to_sql())
                            == ha.argument.as_ref().map(|e| e.to_sql())
                });
                if !already_present {
                    aggregates.push(ha.clone());
                }
            }
        }

        // Build Aggregate node
        inner_tree = OpTree::Aggregate {
            group_by,
            aggregates: aggregates.clone(),
            child: Box::new(inner_tree),
        };

        // Apply HAVING as Filter on top of Aggregate
        if has_having {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let having_expr = safe_node_to_expr(inner_select.havingClause)?;
            let rewritten = rewrite_having_expr(&having_expr, &aggregates);
            inner_tree = OpTree::Filter {
                predicate: rewritten,
                child: Box::new(inner_tree),
            };
        }

        // Wrap in Subquery so the SemiJoin treats it as a derived table
        let sub_alias = format!("__pgt_in_sub_{}", inner_tree.alias());
        inner_tree = OpTree::Subquery {
            alias: sub_alias,
            column_aliases: Vec::new(),
            child: Box::new(inner_tree),
        };

        // Build the equality condition using the output column name.
        // The inner_col_expr (from the SELECT list) is a group-by column
        // that passes through Aggregate → Filter → Subquery.
        // Qualify inner column refs with the Subquery alias to prevent
        // ambiguity when outer and inner tables share column names.
        let inner_col_qualified = qualify_inner_col_refs(inner_col_expr, inner_tree.alias());
        let equality = Expr::BinaryOp {
            op: "=".to_string(),
            left: Box::new(test_expr),
            right: Box::new(inner_col_qualified),
        };

        return Ok(SublinkWrapper {
            negated,
            condition: equality,
            inner_tree,
        });
    }

    // ── Simple case: no GROUP BY / HAVING ───────────────────────────

    // Qualify inner column refs with the inner tree's alias to prevent
    // ambiguity when outer and inner tables share column names (e.g.,
    // both have "id").  Without this, the join-condition rewriter
    // resolves bare "id" against the left (outer) tree first.
    let inner_col_qualified = qualify_inner_col_refs(inner_col_expr, inner_tree.alias());

    // Build the equality condition: test_expr = inner_col_expr
    let equality = Expr::BinaryOp {
        op: "=".to_string(),
        left: Box::new(test_expr),
        right: Box::new(inner_col_qualified),
    };

    // Combine with inner WHERE clause if present
    let condition = if inner_select.whereClause.is_null() {
        equality
    } else {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let inner_where = safe_node_to_expr(inner_select.whereClause)?;
        Expr::BinaryOp {
            op: "AND".to_string(),
            left: Box::new(equality),
            right: Box::new(inner_where),
        }
    };

    Ok(SublinkWrapper {
        negated,
        condition,
        inner_tree,
    })
}

/// Extract aggregate function calls from an expression tree.
///
/// Used to find aggregates in HAVING clauses that may not appear in the
/// SELECT target list. Each discovered aggregate is assigned a unique alias
/// `__pgt_having_{N}` where N starts from `start_idx`.
fn extract_aggregates_from_expr(expr: &Expr, start_idx: usize) -> Vec<AggExpr> {
    let mut result = Vec::new();
    extract_aggregates_from_expr_inner(expr, start_idx, &mut result);
    result
}

fn extract_aggregates_from_expr_inner(expr: &Expr, start_idx: usize, out: &mut Vec<AggExpr>) {
    match expr {
        Expr::FuncCall { func_name, args } => {
            let name_lower = func_name.to_lowercase();
            let agg_func = match name_lower.as_str() {
                "count" => {
                    // COUNT(*) from HAVING arrives as FuncCall { args: [Raw("*")] };
                    // treat it as CountStar with no argument (same as target list).
                    if args.is_empty()
                        || (args.len() == 1 && matches!(&args[0], Expr::Raw(s) if s == "*"))
                    {
                        Some(AggFunc::CountStar)
                    } else {
                        Some(AggFunc::Count)
                    }
                }
                "sum" => Some(AggFunc::Sum),
                "avg" => Some(AggFunc::Avg),
                "min" => Some(AggFunc::Min),
                "max" => Some(AggFunc::Max),
                _ => None,
            };
            if let Some(func) = agg_func {
                // CountStar has no argument (even if the parse produced [Raw("*")]).
                let argument = if matches!(func, AggFunc::CountStar) {
                    None
                } else {
                    args.first().cloned()
                };
                let alias = format!("__pgt_having_{}", start_idx + out.len());
                out.push(AggExpr {
                    function: func,
                    argument,
                    alias,
                    is_distinct: false,
                    second_arg: None,
                    filter: None,
                    order_within_group: None,
                    statistical_support: None,
                });
            } else {
                // Non-aggregate FuncCall: recurse into arguments
                for arg in args {
                    extract_aggregates_from_expr_inner(arg, start_idx, out);
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            extract_aggregates_from_expr_inner(left, start_idx, out);
            extract_aggregates_from_expr_inner(right, start_idx, out);
        }
        _ => {}
    }
}

/// Parse an ALL SubLink (`x op ALL (SELECT col FROM ...)`) into a SublinkWrapper.
///
/// `x op ALL (SELECT col FROM inner_table WHERE filter)` is rewritten as:
/// `NOT EXISTS (SELECT 1 FROM inner_table WHERE NOT (x op col) [AND filter])`
///
/// This produces an AntiJoin where the condition is the negated comparison.
/// If the ALL expression itself is negated (`NOT (x op ALL (...))`), the
/// double negation produces a SemiJoin with the negated operator.
///
/// # Safety
/// Caller must ensure `sublink` points to a valid `pg_sys::SubLink`.
fn parse_all_sublink(
    sublink: &pg_sys::SubLink,
    negated: bool,
    cte_ctx: &mut CteParseContext,
) -> Result<SublinkWrapper, PgTrickleError> {
    if sublink.subselect.is_null() {
        return Err(PgTrickleError::QueryParseError(
            "ALL subquery has NULL subselect".into(),
        ));
    }

    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let inner_select = pg_deref!(sublink.subselect as *const pg_sys::SelectStmt);

    // Parse the inner FROM clause
    let from_list = pg_list::<pg_sys::Node>(inner_select.fromClause);
    if from_list.is_empty() {
        return Err(PgTrickleError::QueryParseError(
            "ALL subquery must have a FROM clause".into(),
        ));
    }

    // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
    let mut inner_tree = unsafe {
        parse_from_item(
            from_list.head().ok_or_else(|| {
                PgTrickleError::InternalError(
                    "ALL sublink from_list unexpectedly empty after non-empty check".into(),
                )
            })?,
            cte_ctx,
        )?
    };
    for i in 1..from_list.len() {
        if let Some(item) = from_list.get_ptr(i) {
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            let right = unsafe { parse_from_item(item, cte_ctx)? };
            inner_tree = OpTree::InnerJoin {
                condition: Expr::Literal("TRUE".into()),
                left: Box::new(inner_tree),
                right: Box::new(right),
            };
        }
    }

    // Extract the test expression (left-hand side: `x` in `x op ALL (...)`)
    let test_expr = if sublink.testexpr.is_null() {
        return Err(PgTrickleError::QueryParseError(
            "ALL subquery has NULL test expression".into(),
        ));
    } else {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        safe_node_to_expr(sublink.testexpr)?
    };

    // Extract the inner SELECT target column
    let target_list = pg_list::<pg_sys::Node>(inner_select.targetList);
    if target_list.is_empty() {
        return Err(PgTrickleError::QueryParseError(
            "ALL subquery SELECT list is empty".into(),
        ));
    }

    let first_target = target_list.head().ok_or_else(|| {
        PgTrickleError::InternalError(
            "ALL sublink target_list unexpectedly empty after non-empty check".into(),
        )
    })?;
    let inner_col_expr = if let Some(rt) = cast_node!(first_target, T_ResTarget, pg_sys::ResTarget)
    {
        if rt.val.is_null() {
            return Err(PgTrickleError::QueryParseError(
                "ALL subquery target column is NULL".into(),
            ));
        }
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        safe_node_to_expr(rt.val)?
    } else {
        return Err(PgTrickleError::QueryParseError(
            "ALL subquery target is not a ResTarget".into(),
        ));
    };

    // Extract the comparison operator from operName.
    // For `x = ALL (...)`, operName is a list containing "=".
    // We use extract_func_name which handles pg_sys::List of String/Value nodes.
    let op_name = if sublink.operName.is_null() {
        "=".to_string() // default to equality
    } else {
        // SAFETY: Parse-tree node pointer from raw_parser; valid within current memory context.
        unsafe { extract_func_name(sublink.operName) }.unwrap_or_else(|_| "=".to_string())
    };

    // Build NULL-safe anti-join condition:
    // (inner_col IS NULL OR NOT (test_expr op inner_col))
    //
    // Using just `NOT (test_expr op inner_col)` is not NULL-safe: when
    // inner_col is NULL, `NOT (x op NULL)` evaluates to NULL (not TRUE),
    // so the EXISTS does not fire for that row, and the outer row is
    // incorrectly included. SQL semantics for ALL require that a NULL
    // in the subquery makes the whole ALL expression indeterminate (hence
    // the outer row should be excluded).
    let inner_col_sql = inner_col_expr.to_sql();
    let negated_cond = Expr::Raw(format!(
        "(({inner_col_sql}) IS NULL OR NOT ({} {op_name} {inner_col_sql}))",
        test_expr.to_sql()
    ));

    // Combine with inner WHERE clause if present
    let condition = if inner_select.whereClause.is_null() {
        negated_cond
    } else {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let inner_where = safe_node_to_expr(inner_select.whereClause)?;
        Expr::BinaryOp {
            op: "AND".to_string(),
            left: Box::new(negated_cond),
            right: Box::new(inner_where),
        }
    };

    // ALL → NOT EXISTS (AntiJoin). If the expression is negated
    // (NOT (x op ALL (...))), the double negation produces SemiJoin.
    Ok(SublinkWrapper {
        negated: !negated,
        condition,
        inner_tree,
    })
}

/// Parse a defining query string into an OpTree.
///
/// Uses `pg_sys::raw_parser()` to get the raw parse tree, then walks
/// the `SelectStmt` structure to build the OpTree. Column types and
/// table OIDs are resolved via catalog lookups.
///
/// Returns the legacy `OpTree` (without CTE registry). For full CTE
/// support, prefer [`parse_defining_query_full`].
pub fn parse_defining_query(query: &str) -> Result<OpTree, PgTrickleError> {
    // SAFETY: We're calling PostgreSQL C parser functions with valid inputs.
    // raw_parser and related functions are safe when called within
    // a PostgreSQL backend with a valid memory context.
    // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
    let result = unsafe { parse_defining_query_inner(query) }?;
    Ok(result.tree)
}

/// Parse a defining query string into a [`ParseResult`] (tree + CTE registry).
///
/// This is the preferred entry point — the returned `CteRegistry` enables
/// the diff engine to differentiate each CTE body only once even when the
/// CTE is referenced multiple times (Tier 2 optimization).
pub fn parse_defining_query_full(query: &str) -> Result<ParseResult, PgTrickleError> {
    // SAFETY: same invariants as parse_defining_query.
    unsafe { parse_defining_query_inner(query) }
}

#[cfg(any(not(test), feature = "pg_test"))]
struct AnalyzedAggCollectorCtx {
    by_location: *mut HashMap<i32, pg_sys::Oid>,
}

#[cfg(any(not(test), feature = "pg_test"))]
unsafe extern "C-unwind" fn analyzed_agg_walker(
    node: *mut pg_sys::Node,
    context: *mut std::ffi::c_void,
) -> bool {
    if node.is_null() {
        return false;
    }

    if unsafe { pgrx::is_a(node, pg_sys::NodeTag::T_Aggref) } {
        // SAFETY: node tag verified as T_Aggref.
        let aggref = unsafe { &*(node as *const pg_sys::Aggref) };
        if aggref.location >= 0 {
            // SAFETY: context is our AnalyzedAggCollectorCtx for the walk.
            let by_location =
                unsafe { &mut *((*(context as *mut AnalyzedAggCollectorCtx)).by_location) };
            by_location
                .entry(aggref.location)
                .or_insert(aggref.aggfnoid);
        }
        return false;
    }

    if unsafe { pgrx::is_a(node, pg_sys::NodeTag::T_Query) } {
        // SAFETY: node tag verified as T_Query.
        return unsafe {
            pg_sys::query_tree_walker_impl(
                node as *mut pg_sys::Query,
                Some(analyzed_agg_walker),
                context,
                0,
            )
        };
    }

    // SAFETY: expression_tree_walker handles all standard analyzed expression nodes.
    unsafe { pg_sys::expression_tree_walker_impl(node, Some(analyzed_agg_walker), context) }
}

#[cfg(any(not(test), feature = "pg_test"))]
fn collect_analyzed_aggregate_locations(
    query: &str,
) -> Result<HashMap<i32, pg_sys::Oid>, PgTrickleError> {
    use pgrx::PgList;
    use std::ffi::CString;
    use std::panic::AssertUnwindSafe;

    let c_sql = CString::new(query)
        .map_err(|e| PgTrickleError::QueryParseError(format!("Query contains null byte: {e}")))?;

    // PostgreSQL reports unresolved relations from parse_analyze_fixedparams
    // with ERROR, not a null return. Metadata is optional, so contain that
    // error and let AUTO use its normal FULL fallback.
    unsafe {
        pgrx::PgTryBuilder::new(AssertUnwindSafe(|| {
            // SAFETY: raw_parser + parse_analyze_fixedparams are called inside
            // a valid PostgreSQL backend; returned nodes live for the duration
            // of this closure.
            let raw_list =
                pg_sys::raw_parser(c_sql.as_ptr(), pg_sys::RawParseMode::RAW_PARSE_DEFAULT);
            let stmts = PgList::<pg_sys::RawStmt>::from_pg(raw_list);
            let raw_stmt = stmts.get_ptr(0).ok_or_else(|| {
                PgTrickleError::QueryParseError("Query produced no parse tree nodes".into())
            })?;
            let query_node = pg_sys::parse_analyze_fixedparams(
                raw_stmt,
                c_sql.as_ptr(),
                std::ptr::null(),
                0,
                std::ptr::null_mut(),
            );
            if query_node.is_null() {
                return Err(PgTrickleError::QueryParseError(
                    "Query analysis returned null".into(),
                ));
            }

            let mut by_location = HashMap::new();
            let mut ctx = AnalyzedAggCollectorCtx {
                by_location: &mut by_location as *mut _,
            };
            pg_sys::query_tree_walker_impl(
                query_node,
                Some(analyzed_agg_walker),
                &mut ctx as *mut AnalyzedAggCollectorCtx as *mut std::ffi::c_void,
                0,
            );
            Ok(by_location)
        }))
        .execute()
    }
}

#[cfg(any(not(test), feature = "pg_test"))]
fn load_transition_arg_type_oids(
    client: &pgrx::spi::SpiClient<'_>,
    aggfnoid: pg_sys::Oid,
) -> Result<Vec<u32>, PgTrickleError> {
    let sql = "SELECT COALESCE(array_agg(arg.arg_type_oid ORDER BY arg.ordinality), ARRAY[]::int4[]) \
               FROM pg_catalog.pg_aggregate a \
               JOIN pg_catalog.pg_proc transfn ON transfn.oid = a.aggtransfn \
               LEFT JOIN LATERAL ( \
                   SELECT arg_type_txt::int4 AS arg_type_oid, ordinality \
                   FROM unnest(string_to_array(transfn.proargtypes::text, ' ')) WITH ORDINALITY \
                        AS args(arg_type_txt, ordinality) \
               ) arg ON TRUE \
               WHERE a.aggfnoid = $1::oid \
               GROUP BY a.aggtransfn";
    let result = client.select(sql, None, &[aggfnoid.into()]).map_err(|e| {
        PgTrickleError::QueryParseError(format!("Failed to inspect aggregate signature: {e}"))
    })?;
    if result.is_empty() {
        return Ok(Vec::new());
    }
    let row = result.first();
    let arg_type_oids = row
        .get::<Vec<i32>>(1)
        .map_err(|e| {
            PgTrickleError::QueryParseError(format!("Failed to read aggregate signature: {e}"))
        })?
        .unwrap_or_default()
        .into_iter()
        .map(|oid| oid as u32)
        .collect();
    Ok(arg_type_oids)
}

#[cfg(any(not(test), feature = "pg_test"))]
fn load_type_sql_name(
    client: &pgrx::spi::SpiClient<'_>,
    type_oid: u32,
) -> Result<Option<String>, PgTrickleError> {
    let result = client
        .select(
            "SELECT format_type($1::oid, NULL)",
            None,
            &[(type_oid as i32).into()],
        )
        .map_err(|e| {
            PgTrickleError::QueryParseError(format!("Failed to inspect type name: {e}"))
        })?;
    if result.is_empty() {
        return Ok(None);
    }
    result
        .first()
        .get::<String>(1)
        .map_err(|e| PgTrickleError::QueryParseError(format!("Failed to read type name: {e}")))
}

fn annotate_tree_statistical_support(
    tree: &mut OpTree,
    aggfnoids_by_location: &HashMap<i32, pg_sys::Oid>,
    transition_arg_types_by_aggfnoid: &HashMap<pg_sys::Oid, Vec<u32>>,
    type_sql_by_oid: &HashMap<u32, String>,
) {
    match tree {
        OpTree::Aggregate {
            aggregates, child, ..
        } => {
            annotate_tree_statistical_support(
                child,
                aggfnoids_by_location,
                transition_arg_types_by_aggfnoid,
                type_sql_by_oid,
            );
            for agg in aggregates {
                let Some(support) = agg.statistical_support.as_mut() else {
                    continue;
                };
                if support.accumulator_type.is_some() {
                    continue;
                }
                let Some(location) = support.parse_location else {
                    continue;
                };
                let Some(aggfnoid) = aggfnoids_by_location.get(&location) else {
                    continue;
                };
                let Some(transition_arg_types) = transition_arg_types_by_aggfnoid.get(aggfnoid)
                else {
                    continue;
                };
                let Some(type_oid) =
                    resolve_statistical_accumulator_type_oid(&agg.function, transition_arg_types)
                else {
                    continue;
                };
                let Some(type_sql) = type_sql_by_oid.get(&type_oid) else {
                    continue;
                };
                support.accumulator_type = Some(ResolvedSqlType {
                    oid: type_oid,
                    sql: type_sql.clone(),
                });
            }
        }
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Subquery { child, .. }
        | OpTree::Window { child, .. }
        | OpTree::Distinct { child }
        | OpTree::LateralFunction { child, .. }
        | OpTree::LateralSubquery { child, .. } => annotate_tree_statistical_support(
            child,
            aggfnoids_by_location,
            transition_arg_types_by_aggfnoid,
            type_sql_by_oid,
        ),
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. }
        | OpTree::SemiJoin { left, right, .. }
        | OpTree::AntiJoin { left, right, .. }
        | OpTree::Intersect { left, right, .. }
        | OpTree::Except { left, right, .. } => {
            annotate_tree_statistical_support(
                left,
                aggfnoids_by_location,
                transition_arg_types_by_aggfnoid,
                type_sql_by_oid,
            );
            annotate_tree_statistical_support(
                right,
                aggfnoids_by_location,
                transition_arg_types_by_aggfnoid,
                type_sql_by_oid,
            );
        }
        OpTree::UnionAll { children } => {
            for child in children {
                annotate_tree_statistical_support(
                    child,
                    aggfnoids_by_location,
                    transition_arg_types_by_aggfnoid,
                    type_sql_by_oid,
                );
            }
        }
        OpTree::ScalarSubquery {
            subquery, child, ..
        } => {
            annotate_tree_statistical_support(
                subquery,
                aggfnoids_by_location,
                transition_arg_types_by_aggfnoid,
                type_sql_by_oid,
            );
            annotate_tree_statistical_support(
                child,
                aggfnoids_by_location,
                transition_arg_types_by_aggfnoid,
                type_sql_by_oid,
            );
        }
        OpTree::RecursiveCte {
            base, recursive, ..
        } => {
            annotate_tree_statistical_support(
                base,
                aggfnoids_by_location,
                transition_arg_types_by_aggfnoid,
                type_sql_by_oid,
            );
            annotate_tree_statistical_support(
                recursive,
                aggfnoids_by_location,
                transition_arg_types_by_aggfnoid,
                type_sql_by_oid,
            );
        }
        OpTree::Scan { .. }
        | OpTree::ConstantSelect { .. }
        | OpTree::CteScan { .. }
        | OpTree::RecursiveSelfRef { .. } => {}
    }
}

#[cfg(any(not(test), feature = "pg_test"))]
fn annotate_statistical_aggregate_support(
    query: &str,
    tree: &mut OpTree,
    cte_registry: &mut CteRegistry,
) -> Result<(), PgTrickleError> {
    if !tree_has_statistical_aggregate(tree)
        && !cte_registry
            .entries
            .iter()
            .any(|(_, cte_tree)| tree_has_statistical_aggregate(cte_tree))
    {
        return Ok(());
    }

    // Metadata is an admission aid, not a reason to reject an otherwise
    // parseable query.  If PostgreSQL cannot analyze the query in this
    // planning context, leave the support unresolved so AUTO falls back to
    // FULL and explicit incremental modes fail closed.
    let aggfnoids_by_location = match collect_analyzed_aggregate_locations(query) {
        Ok(locations) => locations,
        Err(_) => return Ok(()),
    };
    if aggfnoids_by_location.is_empty() {
        return Ok(());
    }

    let mut transition_arg_types_by_aggfnoid = HashMap::new();
    let mut type_sql_by_oid = HashMap::new();
    let unique_aggfnoids: HashSet<pg_sys::Oid> = aggfnoids_by_location.values().copied().collect();

    Spi::connect(|client| {
        let mut needed_type_oids = HashSet::new();
        for aggfnoid in &unique_aggfnoids {
            let arg_type_oids = load_transition_arg_type_oids(client, *aggfnoid)?;
            transition_arg_types_by_aggfnoid.insert(*aggfnoid, arg_type_oids);
        }

        for (_, cte_tree) in &cte_registry.entries {
            collect_needed_statistical_type_oids(
                cte_tree,
                &aggfnoids_by_location,
                &transition_arg_types_by_aggfnoid,
                &mut needed_type_oids,
            );
        }
        collect_needed_statistical_type_oids(
            tree,
            &aggfnoids_by_location,
            &transition_arg_types_by_aggfnoid,
            &mut needed_type_oids,
        );

        for type_oid in needed_type_oids {
            if let Some(type_sql) = load_type_sql_name(client, type_oid)? {
                type_sql_by_oid.insert(type_oid, type_sql);
            }
        }

        Ok::<(), PgTrickleError>(())
    })?;

    annotate_tree_statistical_support(
        tree,
        &aggfnoids_by_location,
        &transition_arg_types_by_aggfnoid,
        &type_sql_by_oid,
    );
    for (_, cte_tree) in &mut cte_registry.entries {
        annotate_tree_statistical_support(
            cte_tree,
            &aggfnoids_by_location,
            &transition_arg_types_by_aggfnoid,
            &type_sql_by_oid,
        );
    }

    Ok(())
}

fn tree_has_statistical_aggregate(tree: &OpTree) -> bool {
    match tree {
        OpTree::Aggregate {
            aggregates, child, ..
        } => {
            aggregates.iter().any(|agg| {
                !agg.is_distinct
                    && (agg.function.needs_sum_of_squares() || agg.function.needs_cross_products())
            }) || tree_has_statistical_aggregate(child)
        }
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Subquery { child, .. }
        | OpTree::Window { child, .. }
        | OpTree::Distinct { child }
        | OpTree::LateralFunction { child, .. }
        | OpTree::LateralSubquery { child, .. } => tree_has_statistical_aggregate(child),
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. }
        | OpTree::SemiJoin { left, right, .. }
        | OpTree::AntiJoin { left, right, .. }
        | OpTree::Intersect { left, right, .. }
        | OpTree::Except { left, right, .. } => {
            tree_has_statistical_aggregate(left) || tree_has_statistical_aggregate(right)
        }
        OpTree::UnionAll { children } => children.iter().any(tree_has_statistical_aggregate),
        OpTree::ScalarSubquery {
            subquery, child, ..
        } => tree_has_statistical_aggregate(subquery) || tree_has_statistical_aggregate(child),
        OpTree::RecursiveCte {
            base, recursive, ..
        } => tree_has_statistical_aggregate(base) || tree_has_statistical_aggregate(recursive),
        OpTree::Scan { .. }
        | OpTree::ConstantSelect { .. }
        | OpTree::CteScan { .. }
        | OpTree::RecursiveSelfRef { .. } => false,
    }
}

fn collect_needed_statistical_type_oids(
    tree: &OpTree,
    aggfnoids_by_location: &HashMap<i32, pg_sys::Oid>,
    transition_arg_types_by_aggfnoid: &HashMap<pg_sys::Oid, Vec<u32>>,
    out: &mut HashSet<u32>,
) {
    match tree {
        OpTree::Aggregate {
            aggregates, child, ..
        } => {
            collect_needed_statistical_type_oids(
                child,
                aggfnoids_by_location,
                transition_arg_types_by_aggfnoid,
                out,
            );
            for agg in aggregates {
                let Some(support) = agg.statistical_support.as_ref() else {
                    continue;
                };
                let Some(location) = support.parse_location else {
                    continue;
                };
                let Some(aggfnoid) = aggfnoids_by_location.get(&location) else {
                    continue;
                };
                let Some(transition_arg_types) = transition_arg_types_by_aggfnoid.get(aggfnoid)
                else {
                    continue;
                };
                if let Some(type_oid) =
                    resolve_statistical_accumulator_type_oid(&agg.function, transition_arg_types)
                {
                    out.insert(type_oid);
                }
            }
        }
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Subquery { child, .. }
        | OpTree::Window { child, .. }
        | OpTree::Distinct { child }
        | OpTree::LateralFunction { child, .. }
        | OpTree::LateralSubquery { child, .. } => collect_needed_statistical_type_oids(
            child,
            aggfnoids_by_location,
            transition_arg_types_by_aggfnoid,
            out,
        ),
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. }
        | OpTree::SemiJoin { left, right, .. }
        | OpTree::AntiJoin { left, right, .. }
        | OpTree::Intersect { left, right, .. }
        | OpTree::Except { left, right, .. } => {
            collect_needed_statistical_type_oids(
                left,
                aggfnoids_by_location,
                transition_arg_types_by_aggfnoid,
                out,
            );
            collect_needed_statistical_type_oids(
                right,
                aggfnoids_by_location,
                transition_arg_types_by_aggfnoid,
                out,
            );
        }
        OpTree::UnionAll { children } => {
            for child in children {
                collect_needed_statistical_type_oids(
                    child,
                    aggfnoids_by_location,
                    transition_arg_types_by_aggfnoid,
                    out,
                );
            }
        }
        OpTree::ScalarSubquery {
            subquery, child, ..
        } => {
            collect_needed_statistical_type_oids(
                subquery,
                aggfnoids_by_location,
                transition_arg_types_by_aggfnoid,
                out,
            );
            collect_needed_statistical_type_oids(
                child,
                aggfnoids_by_location,
                transition_arg_types_by_aggfnoid,
                out,
            );
        }
        OpTree::RecursiveCte {
            base, recursive, ..
        } => {
            collect_needed_statistical_type_oids(
                base,
                aggfnoids_by_location,
                transition_arg_types_by_aggfnoid,
                out,
            );
            collect_needed_statistical_type_oids(
                recursive,
                aggfnoids_by_location,
                transition_arg_types_by_aggfnoid,
                out,
            );
        }
        OpTree::Scan { .. }
        | OpTree::ConstantSelect { .. }
        | OpTree::CteScan { .. }
        | OpTree::RecursiveSelfRef { .. } => {}
    }
}

#[cfg(all(test, not(feature = "pg_test")))]
fn annotate_statistical_aggregate_support(
    _query: &str,
    _tree: &mut OpTree,
    _cte_registry: &mut CteRegistry,
) -> Result<(), PgTrickleError> {
    Ok(())
}

unsafe fn parse_defining_query_inner(query: &str) -> Result<ParseResult, PgTrickleError> {
    // Clear the per-parse warning accumulator so each invocation starts fresh.
    PARSE_ADVISORY_WARNINGS.with(|w| w.borrow_mut().clear());

    // PERF-2 (v0.30.0): Reject queries whose approximate parse-node count
    // would exceed pg_trickle.max_parse_nodes.  We estimate node count
    // conservatively as query_len / 4 (typical average node is ~4 chars
    // of SQL, e.g. ", 1" in an IN list).  This guard fires before the
    // OpTree builder allocates per-node structures, keeping peak memory bounded.
    // When max_parse_nodes == 0 (default changed to 0 for opt-in), the check is disabled.
    let max_nodes = crate::config::pg_trickle_max_parse_nodes();
    if max_nodes > 0 {
        let estimated_nodes = (query.len() / 4).max(1);
        if estimated_nodes > max_nodes {
            return Err(PgTrickleError::QueryTooComplex(format!(
                "Query rejected by pg_trickle.max_parse_nodes: \
                 estimated {estimated_nodes} parse nodes exceeds limit {max_nodes}. \
                 Raise pg_trickle.max_parse_nodes or simplify the query."
            )));
        }
    }

    let list = parse_query(query)?;
    if list.len() != 1 {
        return Err(PgTrickleError::QueryParseError(format!(
            "Expected 1 statement, got {}",
            list.len(),
        )));
    }

    let raw_stmt = list
        .head()
        .ok_or_else(|| PgTrickleError::QueryParseError("Empty parse list".into()))?;

    let stmt_ptr = pg_deref!(raw_stmt).stmt as *const pg_sys::SelectStmt;
    // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
    if !is_node_type!(stmt_ptr as *mut pg_sys::Node, T_SelectStmt) {
        return Err(PgTrickleError::QueryParseError(
            "Defining query must be a SELECT statement".into(),
        ));
    }

    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let select = pg_deref!(stmt_ptr);

    // ── Extract CTE definitions (if any) ─────────────────────────────
    let has_recursion = if !select.withClause.is_null() {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let wc = pg_deref!(select.withClause);
        wc.recursive
    } else {
        false
    };

    let (cte_map, cte_def_aliases, recursive_cte_stmts) = if !select.withClause.is_null() {
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        unsafe { extract_cte_map_with_recursive(select.withClause)? }
    } else {
        (HashMap::new(), HashMap::new(), Vec::new())
    };

    // Build a CTE parse context — this holds the raw SelectStmt pointers
    // *and* a mutable CteRegistry that gets populated as CTE references
    // are encountered during tree construction.
    let mut cte_ctx = CteParseContext::new(cte_map, cte_def_aliases);

    // Parse recursive CTEs into OpTree and register them.
    // This must happen before parsing the main SELECT so that references
    // to the recursive CTE name resolve to CteScan nodes.
    for (name, base_stmt, rec_stmt, def_cols, union_all) in &recursive_cte_stmts {
        // Parse the base case (non-recursive term).
        // In PostgreSQL 18, the larg/rarg of a UNION may themselves appear
        // as set-operation wrappers (e.g., when the base case is itself a
        // multi-arm UNION). Check and dispatch accordingly.
        //
        // Set `in_cte_anchor = true` so that `parse_select_stmt_inner`
        // accepts a constant SELECT without a FROM clause (e.g. `SELECT 1
        // AS id`) and returns an `OpTree::ConstantSelect` instead of an
        // error.
        cte_ctx.in_cte_anchor = true;
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        let base_tree = unsafe {
            let s = &**base_stmt;
            if s.op != pg_sys::SetOperation::SETOP_NONE {
                parse_set_operation(s, &mut cte_ctx)?
            } else {
                parse_select_stmt(s, "", &mut cte_ctx)?
            }
        };
        cte_ctx.in_cte_anchor = false;

        // Determine output columns: CTE def aliases > base case output
        let columns = if def_cols.is_empty() {
            base_tree.output_columns()
        } else {
            def_cols.clone()
        };

        // Set up self-ref tracking for parsing the recursive term
        cte_ctx.recursive_self_ref_name = Some(name.clone());
        cte_ctx.recursive_self_ref_columns = columns.clone();

        // Parse the recursive term — self-references become RecursiveSelfRef
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        let rec_tree = unsafe {
            let s = &**rec_stmt;
            if s.op != pg_sys::SetOperation::SETOP_NONE {
                parse_set_operation(s, &mut cte_ctx)?
            } else {
                parse_select_stmt(s, "", &mut cte_ctx)?
            }
        };

        // Clear self-ref tracking
        cte_ctx.recursive_self_ref_name = None;
        cte_ctx.recursive_self_ref_columns.clear();

        // Build the RecursiveCte node and register it
        let rec_cte = OpTree::RecursiveCte {
            alias: name.clone(),
            columns,
            base: Box::new(base_tree),
            recursive: Box::new(rec_tree),
            union_all: *union_all,
        };

        cte_ctx.register(name, rec_cte);
    }

    // Check for set operations — use the `op` field rather than larg/rarg nullness
    // because PG18 may leave larg/rarg non-null on non-union SelectStmt nodes.
    let mut tree = if select.op != pg_sys::SetOperation::SETOP_NONE {
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        unsafe { parse_set_operation(select, &mut cte_ctx)? }
    } else {
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        unsafe { parse_select_stmt(select, query, &mut cte_ctx)? }
    };

    annotate_statistical_aggregate_support(query, &mut tree, &mut cte_ctx.registry)?;

    // Prune Scan columns to only those referenced by the defining query,
    // reducing the number of new_*/old_* columns read from change buffers.
    tree.prune_scan_columns();
    for (_, cte_tree) in &mut cte_ctx.registry.entries {
        cte_tree.prune_scan_columns();
    }

    // Drain advisory warnings accumulated during this parse.
    let warnings = PARSE_ADVISORY_WARNINGS.with(|w| std::mem::take(&mut *w.borrow_mut()));

    Ok(ParseResult {
        tree,
        cte_registry: cte_ctx.registry,
        has_recursion,
        warnings,
    })
}

/// Parse a set-operation SELECT (UNION, UNION ALL, INTERSECT [ALL], EXCEPT [ALL]).
///
/// - `UNION ALL` produces `OpTree::UnionAll { children }`.
/// - `UNION` (deduplicated) produces `OpTree::Distinct { child: UnionAll }`.
/// - `INTERSECT [ALL]` produces `OpTree::Intersect { left, right, all }`.
/// - `EXCEPT [ALL]` produces `OpTree::Except { left, right, all }`.
///
/// Mixed UNION/UNION ALL trees are handled by respecting PostgreSQL's nested
/// `SetOperationStmt` tree structure: children with a different `all` flag
/// are parsed as separate set operations rather than flattened.
unsafe fn parse_set_operation(
    select: &pg_sys::SelectStmt,
    cte_ctx: &mut CteParseContext,
) -> Result<OpTree, PgTrickleError> {
    // G13-SD: Track recursion depth.
    let prev_depth = cte_ctx.descend()?;
    // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
    let result = unsafe { parse_set_operation_inner(select, cte_ctx) };
    cte_ctx.ascend(prev_depth);
    result
}

/// Inner implementation of `parse_set_operation` (after depth check).
unsafe fn parse_set_operation_inner(
    select: &pg_sys::SelectStmt,
    cte_ctx: &mut CteParseContext,
) -> Result<OpTree, PgTrickleError> {
    match select.op {
        pg_sys::SetOperation::SETOP_UNION => {
            let mut children = Vec::new();
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            unsafe { collect_union_children(select, select.all, &mut children, cte_ctx)? };

            if children.len() < 2 {
                return Err(PgTrickleError::QueryParseError(
                    "UNION / UNION ALL requires at least 2 children".into(),
                ));
            }

            let union_all = OpTree::UnionAll { children };

            // UNION (without ALL) = UNION ALL + DISTINCT deduplication
            if !select.all {
                Ok(OpTree::Distinct {
                    child: Box::new(union_all),
                })
            } else {
                Ok(union_all)
            }
        }
        pg_sys::SetOperation::SETOP_INTERSECT => {
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            let left = unsafe { parse_set_op_child(select.larg, cte_ctx)? };
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            let right = unsafe { parse_set_op_child(select.rarg, cte_ctx)? };
            Ok(OpTree::Intersect {
                left: Box::new(left),
                right: Box::new(right),
                all: select.all,
            })
        }
        pg_sys::SetOperation::SETOP_EXCEPT => {
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            let left = unsafe { parse_set_op_child(select.larg, cte_ctx)? };
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            let right = unsafe { parse_set_op_child(select.rarg, cte_ctx)? };
            Ok(OpTree::Except {
                left: Box::new(left),
                right: Box::new(right),
                all: select.all,
            })
        }
        _ => Err(PgTrickleError::UnsupportedOperator(format!(
            "Set operation {:?} not supported",
            select.op,
        ))),
    }
}

/// Parse a single child of a set operation (left or right branch).
///
/// The child may itself be a set operation node (e.g., `(A INTERSECT B) EXCEPT C`)
/// or a plain SELECT.
unsafe fn parse_set_op_child(
    child_ptr: *mut pg_sys::SelectStmt,
    cte_ctx: &mut CteParseContext,
) -> Result<OpTree, PgTrickleError> {
    if child_ptr.is_null() {
        return Err(PgTrickleError::QueryParseError(
            "Set operation branch is NULL".into(),
        ));
    }
    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let child = pg_deref!(child_ptr);
    if child.op != pg_sys::SetOperation::SETOP_NONE {
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        unsafe { parse_set_operation(child, cte_ctx) }
    } else {
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        unsafe { parse_select_stmt(child, "", cte_ctx) }
    }
}

/// Recursively collect UNION / UNION ALL child branches from a set-operation tree.
///
/// Uses the `op` field (not `larg`/`rarg` nullness) to detect set-operation
/// nodes, because PostgreSQL 18 may leave `larg`/`rarg` non-null on simple
/// SELECT nodes within CTE bodies.
///
/// `parent_all` is the `all` flag of the top-level node. When an intermediate
/// UNION node has a different `all` flag (mixed UNION / UNION ALL), it is
/// parsed as a separate set operation rather than flattened, preserving
/// PostgreSQL's nested `SetOperationStmt` tree structure.
unsafe fn collect_union_children(
    select: &pg_sys::SelectStmt,
    parent_all: bool,
    children: &mut Vec<OpTree>,
    cte_ctx: &mut CteParseContext,
) -> Result<(), PgTrickleError> {
    // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
    unsafe {
        if !select.larg.is_null() && !select.rarg.is_null() {
            let larg = &*select.larg;
            let rarg = &*select.rarg;

            if larg.op != pg_sys::SetOperation::SETOP_NONE {
                if larg.op == pg_sys::SetOperation::SETOP_UNION && larg.all == parent_all {
                    // Same UNION type — flatten into the same children list
                    collect_union_children(larg, parent_all, children, cte_ctx)?;
                } else {
                    // Different set operation or mixed UNION/UNION ALL —
                    // parse as a separate sub-tree (preserves nesting).
                    let tree = parse_set_operation(larg, cte_ctx)?;
                    children.push(tree);
                }
            } else {
                let tree = parse_select_stmt(larg, "", cte_ctx)?;
                children.push(tree);
            }

            if rarg.op != pg_sys::SetOperation::SETOP_NONE {
                if rarg.op == pg_sys::SetOperation::SETOP_UNION && rarg.all == parent_all {
                    // Same UNION type — flatten
                    collect_union_children(rarg, parent_all, children, cte_ctx)?;
                } else {
                    // Different set operation or mixed — parse as sub-tree
                    let tree = parse_set_operation(rarg, cte_ctx)?;
                    children.push(tree);
                }
            } else {
                let tree = parse_select_stmt(rarg, "", cte_ctx)?;
                children.push(tree);
            }
        }
        Ok(())
    }
}

/// Parse a simple (non-set-operation) SELECT statement into an OpTree.
///
/// Callers must ensure this is NOT a UNION/INTERSECT/EXCEPT node — check
/// `select.op == SETOP_NONE` before calling. Use [`parse_set_operation`] for
/// set-operation nodes.
///
/// Builds bottom-up: Scan → Filter → Join → Project → Aggregate → Distinct
unsafe fn parse_select_stmt(
    select: &pg_sys::SelectStmt,
    _full_query: &str,
    cte_ctx: &mut CteParseContext,
) -> Result<OpTree, PgTrickleError> {
    // G13-SD: Track recursion depth.
    let prev_depth = cte_ctx.descend()?;
    // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
    let result = unsafe { parse_select_stmt_inner(select, _full_query, cte_ctx) };
    cte_ctx.ascend(prev_depth);
    result
}

/// Inner implementation of `parse_select_stmt` (after depth check).
unsafe fn parse_select_stmt_inner(
    select: &pg_sys::SelectStmt,
    _full_query: &str,
    cte_ctx: &mut CteParseContext,
) -> Result<OpTree, PgTrickleError> {
    // ── Step 1: Parse FROM clause into Scan/Join tree ──────────────────
    let from_list = pg_list::<pg_sys::Node>(select.fromClause);
    if from_list.is_empty() {
        // Allow a constant SELECT (no FROM clause) when parsing the base case
        // of a WITH RECURSIVE anchor, e.g. `SELECT 1 AS id`.  These nodes
        // have no source tables and are represented as ConstantSelect so that
        // helper functions like generate_query_sql can embed them verbatim.
        if cte_ctx.in_cte_anchor {
            let sql = deparse_select_stmt_with_view_subs(select as *const _, &[])?;
            // SAFETY: `select.targetList` is a valid List pointer from the PG parser.
            let columns = unsafe { extract_target_list_column_names(select.targetList) };
            return Ok(OpTree::ConstantSelect { columns, sql });
        }
        return Err(PgTrickleError::QueryParseError(format!(
            "Defining query must have a FROM clause (op={}, all={}, larg_null={}, rarg_null={}, target_len={}, where_null={})",
            select.op,
            select.all,
            select.larg.is_null(),
            select.rarg.is_null(),
            pg_list::<pg_sys::Node>(select.targetList).len(),
            select.whereClause.is_null(),
        )));
    }

    // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
    let mut tree = unsafe {
        parse_from_item(
            from_list.head().ok_or_else(|| {
                PgTrickleError::InternalError(
                    "parse_select_stmt_inner from_list unexpectedly empty".into(),
                )
            })?,
            cte_ctx,
        )?
    };

    // Handle implicit cross-joins / LATERAL SRFs (multiple items in FROM)
    for i in 1..from_list.len() {
        if let Some(item) = from_list.get_ptr(i) {
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            let right = unsafe { parse_from_item(item, cte_ctx)? };
            // If the right side is a LateralFunction, attach the current tree
            // as its child (LATERAL dependency) instead of wrapping in a cross join.
            if let OpTree::LateralFunction {
                func_sql,
                alias,
                column_aliases,
                with_ordinality,
                ..
            } = right
            {
                tree = OpTree::LateralFunction {
                    func_sql,
                    alias,
                    column_aliases,
                    with_ordinality,
                    child: Box::new(tree),
                };
            } else if let OpTree::LateralSubquery {
                subquery_sql,
                alias,
                column_aliases,
                output_cols,
                is_left_join,
                subquery_source_oids,
                correlation_predicates,
                ..
            } = right
            {
                // LATERAL subquery in comma syntax: FROM t, LATERAL (SELECT ...)
                tree = OpTree::LateralSubquery {
                    subquery_sql,
                    alias,
                    column_aliases,
                    output_cols,
                    is_left_join,
                    subquery_source_oids,
                    correlation_predicates,
                    child: Box::new(tree),
                };
            } else {
                tree = OpTree::InnerJoin {
                    condition: Expr::Literal("TRUE".into()),
                    left: Box::new(tree),
                    right: Box::new(right),
                };
            }
        }
    }

    // ── Step 2: Parse WHERE clause (with SubLink extraction) ─────────
    // SubLinks (EXISTS, IN subquery) in the WHERE clause are extracted
    // and converted into SemiJoin/AntiJoin OpTree wrappers. The remaining
    // non-SubLink predicates become a Filter node.
    if !select.whereClause.is_null() {
        let (sublink_wrappers, remaining_predicate) =
            extract_where_sublinks(select.whereClause, cte_ctx)?;

        // Apply SubLink wrappers (SemiJoin/AntiJoin) bottom-up
        for wrapper in sublink_wrappers {
            if wrapper.negated {
                tree = OpTree::AntiJoin {
                    condition: wrapper.condition,
                    left: Box::new(tree),
                    right: Box::new(wrapper.inner_tree),
                };
            } else {
                tree = OpTree::SemiJoin {
                    condition: wrapper.condition,
                    left: Box::new(tree),
                    right: Box::new(wrapper.inner_tree),
                };
            }
        }

        // Apply remaining non-SubLink predicates as Filter
        if let Some(pred) = remaining_predicate {
            tree = OpTree::Filter {
                predicate: pred,
                child: Box::new(tree),
            };
        }

        // ── Predicate pushdown into cross joins ────────────────────
        // Comma-separated FROM items create InnerJoin(TRUE, ...) chains.
        // Promote eligible predicates from the Filter into appropriate
        // JOIN ON clauses, converting cross joins to equi-joins.
        //
        // Guard (a): predicates containing scalar subqueries (e.g.
        // TPC-H Q17 correlated subquery) are kept in the Filter and
        // never promoted — see `expr_contains_subquery()`.
        //
        // Note: Q07 previously showed revenue drift with pushdown
        // enabled — if it recurs, investigate the aggregate diff or
        // MERGE pipeline; the root cause is not in pushdown itself.
        tree = push_filter_into_cross_joins(tree)?;
    }

    // ── Step 3: Parse GROUP BY + aggregates ─────────────────────────────
    let group_list = pg_list::<pg_sys::Node>(select.groupClause);
    let target_list = pg_list::<pg_sys::Node>(select.targetList);

    // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
    let has_aggregates = unsafe { target_list_has_aggregates(&target_list) };
    let has_windows = target_list_has_windows(&target_list);

    if has_windows {
        // ── Window function path ───────────────────────────────────────
        // Extract window expressions and pass-through columns.
        let (window_exprs, pass_through) =
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            unsafe { extract_window_exprs(&target_list, select.windowClause)? };

        if window_exprs.is_empty() {
            return Err(PgTrickleError::QueryParseError(
                "Window function detected but extraction failed".into(),
            ));
        }

        // Determine the shared PARTITION BY for the DVM diff optimizer.
        // If all window functions share the same PARTITION BY, use it —
        // the diff engine can then recompute only affected partitions.
        // When PARTITION BY clauses differ, fall back to empty (un-partitioned)
        // which triggers full recomputation on any change (correct but
        // less targeted).
        let canonical_partition: Vec<String> = window_exprs[0]
            .partition_by
            .iter()
            .map(|e| e.to_sql())
            .collect();
        let all_same_partition = window_exprs[1..].iter().all(|wexpr| {
            let p: Vec<String> = wexpr.partition_by.iter().map(|e| e.to_sql()).collect();
            p == canonical_partition
        });

        let partition_by = if all_same_partition {
            window_exprs[0].partition_by.clone()
        } else {
            // Different PARTITION BY clauses → un-partitioned diff mode
            // (full recomputation on any change).
            vec![]
        };

        // If there's also a GROUP BY, build the Aggregate child first.
        if !group_list.is_empty() || has_aggregates {
            let mut group_by = Vec::new();
            for node_ptr in group_list.iter_ptr() {
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let expr = safe_node_to_expr(node_ptr)?;
                group_by.push(expr);
            }
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            let (aggregates, _non_agg_exprs) = unsafe { extract_aggregates(&target_list)? };
            tree = OpTree::Aggregate {
                group_by,
                aggregates,
                child: Box::new(tree),
            };
        }

        tree = OpTree::Window {
            window_exprs,
            partition_by,
            pass_through,
            child: Box::new(tree),
        };
    } else if !group_list.is_empty() || has_aggregates {
        let mut group_by = Vec::new();
        for node_ptr in group_list.iter_ptr() {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let expr = safe_node_to_expr(node_ptr)?;
            group_by.push(expr);
        }

        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        let (aggregates, non_agg_exprs) = unsafe { extract_aggregates(&target_list)? };
        let _ = non_agg_exprs;

        // ── Step 3a2: Check for SELECT alias renames on GROUP BY cols ───
        // When the SELECT list renames a GROUP BY column (e.g.,
        // `SELECT l_suppkey AS supplier_no, SUM(...) AS total_revenue
        //  GROUP BY l_suppkey`), the Aggregate's output uses the raw
        // expression name (l_suppkey), losing the alias. Detect renames
        // BEFORE moving group_by into the Aggregate.
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        let (target_exprs, target_aliases) = unsafe { parse_target_list(&target_list)? };

        let coalesced_sum_defaults: std::collections::HashMap<String, Expr> = target_exprs
            .iter()
            .zip(target_aliases.iter())
            .filter_map(|(expr, alias)| {
                let Expr::FuncCall { func_name, args } = expr else {
                    return None;
                };
                if !func_name.eq_ignore_ascii_case("coalesce") || args.len() < 2 {
                    return None;
                }
                let Expr::FuncCall {
                    func_name: inner_name,
                    ..
                } = &args[0]
                else {
                    return None;
                };
                if inner_name.eq_ignore_ascii_case("sum") {
                    Some((alias.clone(), args[1].clone()))
                } else {
                    None
                }
            })
            .collect();

        // Resolve ordinal GROUP BY references (e.g., GROUP BY 1 → first
        // target expression). PostgreSQL allows GROUP BY <n> to refer to
        // the Nth output column. Resolve these to the actual target
        // expressions now so that rename detection below can match them.
        for gb_expr in &mut group_by {
            if let Expr::Raw(s) = &gb_expr
                && let Ok(pos) = s.parse::<usize>()
                && pos >= 1
                && pos <= target_exprs.len()
            {
                *gb_expr = target_exprs[pos - 1].clone();
            }
        }

        let mut rename_map = std::collections::HashMap::<usize, String>::new();
        for (i, gb_expr) in group_by.iter().enumerate() {
            let gb_sql = gb_expr.to_sql();
            let gb_output = gb_expr.output_name();
            for (te, ta) in target_exprs.iter().zip(target_aliases.iter()) {
                if te.to_sql() == gb_sql && ta != &gb_output {
                    rename_map.insert(i, ta.clone());
                    break;
                }
            }
        }

        tree = OpTree::Aggregate {
            group_by,
            aggregates: aggregates.clone(),
            child: Box::new(tree),
        };

        // Add a Project wrapper for GROUP BY renames and for
        // COALESCE(SUM(...), default), so the aggregate node restores NULL
        // and the outer projection applies PostgreSQL's COALESCE semantics.
        let needs_projection = !rename_map.is_empty() || !coalesced_sum_defaults.is_empty();
        if needs_projection {
            let agg_output = tree.output_columns();
            let proj_exprs: Vec<Expr> = agg_output
                .iter()
                .map(|name| {
                    coalesced_sum_defaults.get(name).map_or_else(
                        || Expr::ColumnRef {
                            table_alias: None,
                            column_name: name.clone(),
                        },
                        |default| Expr::FuncCall {
                            func_name: "COALESCE".to_string(),
                            args: vec![
                                Expr::ColumnRef {
                                    table_alias: None,
                                    column_name: name.clone(),
                                },
                                default.clone(),
                            ],
                        },
                    )
                })
                .collect();
            let proj_aliases: Vec<String> = agg_output
                .iter()
                .enumerate()
                .map(|(i, name)| rename_map.get(&i).cloned().unwrap_or_else(|| name.clone()))
                .collect();
            tree = OpTree::Project {
                expressions: proj_exprs,
                aliases: proj_aliases,
                child: Box::new(tree),
            };
        }

        // ── Step 3b: Parse HAVING clause as Filter on top of Aggregate ──
        if !select.havingClause.is_null() {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let having_pred = safe_node_to_expr(select.havingClause)?;
            let rewritten = rewrite_having_expr(&having_pred, &aggregates);
            tree = OpTree::Filter {
                predicate: rewritten,
                child: Box::new(tree),
            };
        }
    } else {
        // ── Step 4: Parse target list as Project ───────────────────────
        if !select.havingClause.is_null() {
            return Err(PgTrickleError::QueryParseError(
                "HAVING clause requires GROUP BY or aggregate functions".into(),
            ));
        }
        if let Some(scalar_tree) =
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            unsafe {
                parse_appended_scalar_target_subqueries(&target_list, tree.clone(), cte_ctx)?
            }
        {
            tree = scalar_tree;
        } else {
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            let (expressions, aliases) = unsafe { parse_target_list(&target_list)? };
            if !is_star_only(&expressions) {
                tree = OpTree::Project {
                    expressions,
                    aliases,
                    child: Box::new(tree),
                };
            }
        }
    }

    // ── Step 5: Parse DISTINCT ──────────────────────────────────────────
    // PostgreSQL raw parser represents:
    //   SELECT DISTINCT           → distinctClause = list of NIL entries
    //   SELECT DISTINCT ON (expr) → distinctClause = list of real expression nodes
    //   No DISTINCT               → distinctClause = NULL
    //
    // We detect DISTINCT ON by checking whether any list entry is a
    // non-null node pointer.
    if !select.distinctClause.is_null() {
        let distinct_list = pg_list::<pg_sys::Node>(select.distinctClause);
        let has_real_exprs = distinct_list.iter_ptr().any(|ptr| !ptr.is_null());
        if has_real_exprs {
            // DISTINCT ON (expr, ...) — reject
            return Err(PgTrickleError::UnsupportedOperator(
                "DISTINCT ON is not supported in defining queries. \
                 Use plain DISTINCT or rewrite with window functions."
                    .into(),
            ));
        }
        tree = OpTree::Distinct {
            child: Box::new(tree),
        };
    }

    // ── Step 6: Handle ORDER BY ────────────────────────────────────────
    // ORDER BY is meaningless for stream table storage — row order is
    // undefined. We accept it silently (PostgreSQL does the same for
    // CREATE MATERIALIZED VIEW) and simply discard the sort clause.
    // No need to inspect `select.sortClause` — it is ignored.

    // ── Step 7: Reject LIMIT / OFFSET ──────────────────────────────────
    // TopK tables (ORDER BY + LIMIT) bypass the DVM pipeline entirely —
    // they never reach this point. This rejection handles the case where
    // LIMIT/OFFSET appears in a non-TopK context (e.g., LIMIT without
    // ORDER BY in DIFFERENTIAL mode, which reject_limit_offset should
    // have already caught at the API layer).
    if !select.limitCount.is_null() {
        return Err(PgTrickleError::UnsupportedOperator(
            "LIMIT is not supported in the DVM pipeline. \
             Use ORDER BY + LIMIT for TopK stream tables."
                .into(),
        ));
    }
    if !select.limitOffset.is_null() {
        return Err(PgTrickleError::UnsupportedOperator(
            "OFFSET is not supported in defining queries. \
             Stream tables materialize the full result set."
                .into(),
        ));
    }

    Ok(tree)
}

/// Parse simple appended scalar subqueries in the SELECT list.
///
/// Supported shape:
/// - Zero or more non-star target expressions first
/// - One or more bare scalar subquery targets last
///
/// Example:
/// `SELECT customer, amount, (SELECT val FROM cfg) AS tax_rate FROM orders`
///
/// This lowers the query to:
/// - a Project over the outer FROM tree for the non-scalar columns, then
/// - one ScalarSubquery wrapper per appended scalar target.
///
/// More complex target-list shapes (interleaved scalar targets, `*`, or
/// scalar subqueries nested inside larger expressions) fall back to the
/// generic Project path.
unsafe fn parse_appended_scalar_target_subqueries(
    target_list: &pgrx::PgList<pg_sys::Node>,
    mut tree: OpTree,
    cte_ctx: &mut CteParseContext,
) -> Result<Option<OpTree>, PgTrickleError> {
    struct ScalarTarget {
        alias: String,
        subquery: OpTree,
        source_oids: Vec<u32>,
    }

    let mut plain_exprs = Vec::new();
    let mut plain_aliases = Vec::new();
    let mut scalar_targets = Vec::new();
    let mut saw_scalar_target = false;

    for node_ptr in target_list.iter_ptr() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if node_ptr.is_null() || !is_node_type!(node_ptr, T_ResTarget) {
            continue;
        }

        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let rt = pg_deref!(node_ptr as *const pg_sys::ResTarget);
        if rt.val.is_null() {
            continue;
        }

        let alias =
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            unsafe { target_alias_for_res_target(rt, plain_exprs.len() + scalar_targets.len()) };

        if let Some((subquery, source_oids)) =
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            unsafe { parse_scalar_target_subquery(rt.val, cte_ctx)? }
        {
            scalar_targets.push(ScalarTarget {
                alias,
                subquery,
                source_oids,
            });
            saw_scalar_target = true;
            continue;
        }

        if saw_scalar_target {
            return Ok(None);
        }

        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = safe_node_to_expr(rt.val)?;
        if matches!(expr, Expr::Star { .. }) {
            return Ok(None);
        }

        plain_exprs.push(expr);
        plain_aliases.push(alias);
    }

    if scalar_targets.is_empty() || plain_exprs.is_empty() {
        return Ok(None);
    }

    tree = OpTree::Project {
        expressions: plain_exprs,
        aliases: plain_aliases,
        child: Box::new(tree),
    };

    for scalar in scalar_targets {
        tree = OpTree::ScalarSubquery {
            subquery: Box::new(scalar.subquery),
            alias: scalar.alias,
            subquery_source_oids: scalar.source_oids,
            child: Box::new(tree),
        };
    }

    Ok(Some(tree))
}

/// Parse a bare scalar subquery target into an OpTree.
///
/// Accepts both:
/// - raw parser `T_SubLink` nodes for `EXPR_SUBLINK`, and
/// - `Expr::Raw("(SELECT ...)")` fallback expressions produced by `node_to_expr()`.
unsafe fn parse_scalar_target_subquery(
    node: *mut pg_sys::Node,
    cte_ctx: &mut CteParseContext,
) -> Result<Option<(OpTree, Vec<u32>)>, PgTrickleError> {
    if let Some(sublink) = cast_node!(node, T_SubLink, pg_sys::SubLink) {
        if sublink.subLinkType != pg_sys::SubLinkType::EXPR_SUBLINK {
            return Ok(None);
        }
        if sublink.subselect.is_null()
            // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
            || !is_node_type!(sublink.subselect, T_SelectStmt)
        {
            return Err(PgTrickleError::QueryParseError(
                "Scalar subquery target must contain a SELECT".into(),
            ));
        }

        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let inner_select = pg_deref!(sublink.subselect as *const pg_sys::SelectStmt);
        // If the inner SELECT contains constructs that the DVM pipeline cannot
        // parse fully (e.g. LIMIT/OFFSET, correlated sub-expressions with OR),
        // fall back to `Ok(None)` so the caller's `node_to_expr` path deparsed
        // the sublink as an opaque `Expr::Raw`.  This is safe because:
        //   1. `extract_source_relations` uses the full PostgreSQL analyzer and
        //      still records every inner table reference for dependency tracking.
        //   2. The scheduler forces a FULL refresh whenever upstream stream-table
        //      sources have newer data, so the raw-SQL expression is only exercised
        //      for base-table-CDC differentials, where it is re-evaluated correctly
        //      for each affected row.
        let subquery = match if inner_select.op != pg_sys::SetOperation::SETOP_NONE {
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            unsafe { parse_set_operation(inner_select, cte_ctx) }
        } else {
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            unsafe { parse_select_stmt(inner_select, "", cte_ctx) }
        } {
            Ok(tree) => tree,
            Err(_) => return Ok(None),
        };

        let mut source_oids = subquery.source_oids();
        source_oids.sort_unstable();
        source_oids.dedup();
        return Ok(Some((subquery, source_oids)));
    }

    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
    let expr = safe_node_to_expr(node)?;
    let Expr::Raw(raw_sql) = expr else {
        return Ok(None);
    };

    let Some(inner_sql) = extract_bare_scalar_subquery_sql(&raw_sql) else {
        return Ok(None);
    };

    let inner_select = match parse_first_select(inner_sql.as_str())? {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        Some(s) => pg_deref!(s),
        None => {
            return Err(PgTrickleError::QueryParseError(
                "Scalar subquery target must parse to a SELECT".into(),
            ));
        }
    };
    // Same graceful fallback for the raw-SQL code path.
    let subquery = match if inner_select.op != pg_sys::SetOperation::SETOP_NONE {
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        unsafe { parse_set_operation(inner_select, cte_ctx) }
    } else {
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        unsafe { parse_select_stmt(inner_select, "", cte_ctx) }
    } {
        Ok(tree) => tree,
        Err(_) => return Ok(None),
    };

    let mut source_oids = subquery.source_oids();
    source_oids.sort_unstable();
    source_oids.dedup();
    Ok(Some((subquery, source_oids)))
}

fn extract_bare_scalar_subquery_sql(raw_sql: &str) -> Option<String> {
    let trimmed = raw_sql.trim();
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        return None;
    }

    let inner = trimmed[1..trimmed.len() - 1].trim();
    if inner.len() < 6 || !inner[..6].eq_ignore_ascii_case("SELECT") {
        return None;
    }

    Some(inner.to_string())
}

/// Parse a FROM clause item (RangeVar, JoinExpr, or RangeSubselect) into an OpTree.
unsafe fn parse_from_item(
    node: *mut pg_sys::Node,
    cte_ctx: &mut CteParseContext,
) -> Result<OpTree, PgTrickleError> {
    // G13-SD: Track recursion depth.
    let prev_depth = cte_ctx.descend()?;
    // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
    let result = unsafe { parse_from_item_inner(node, cte_ctx) };
    cte_ctx.ascend(prev_depth);
    result
}

/// Inner implementation of `parse_from_item` (after depth check).
unsafe fn parse_from_item_inner(
    node: *mut pg_sys::Node,
    cte_ctx: &mut CteParseContext,
) -> Result<OpTree, PgTrickleError> {
    if let Some(rv) = cast_node!(node, T_RangeVar, pg_sys::RangeVar) {
        // Explicit schema qualifier: use as-is.
        // Unqualified name: defer resolution to after the CTE check so we
        // don't do a search_path lookup for names that are CTE aliases.
        let explicit_schema: Option<String> = if rv.schemaname.is_null() {
            None
        } else {
            Some(
                pg_cstr_to_str(rv.schemaname)
                    .unwrap_or("public")
                    .to_string(),
            )
        };
        let table_name = pg_cstr_to_str(rv.relname)?.to_string();
        let alias = if !rv.alias.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let a = pg_deref!(rv.alias);
            pg_cstr_to_str(a.aliasname)
                .unwrap_or(&table_name)
                .to_string()
        } else {
            table_name.clone()
        };

        // Check if this name is a self-reference in a recursive CTE
        if rv.schemaname.is_null()
            && let Some(ref self_ref_name) = cte_ctx.recursive_self_ref_name
            && table_name == *self_ref_name
        {
            let self_alias = if !rv.alias.is_null() {
                // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
                let a = pg_deref!(rv.alias);
                pg_cstr_to_str(a.aliasname)
                    .unwrap_or(&table_name)
                    .to_string()
            } else {
                table_name.clone()
            };
            return Ok(OpTree::RecursiveSelfRef {
                cte_name: self_ref_name.clone(),
                alias: self_alias,
                columns: cte_ctx.recursive_self_ref_columns.clone(),
            });
        }

        // Check if this name references a CTE (schema-unqualified only)
        if rv.schemaname.is_null() && cte_ctx.is_cte(&table_name) {
            // Extract column aliases from the RangeVar's alias, if any
            let col_aliases = if !rv.alias.is_null() {
                // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
                let a = pg_deref!(rv.alias);
                extract_alias_colnames(a)?
            } else {
                Vec::new()
            };

            // Get CTE definition-level aliases: WITH x(a, b) AS (...)
            let def_aliases = cte_ctx
                .def_aliases
                .get(&table_name)
                .cloned()
                .unwrap_or_default();

            // Check if this CTE has already been parsed (reuse cte_id)
            let (cte_id, columns, body_clone) = if let Some(id) = cte_ctx.lookup_id(&table_name) {
                // Already parsed — reuse the existing entry
                let (_, body) = &cte_ctx.registry.entries[id];
                let cols = body.output_columns();
                let body_clone = body.clone();
                (id, cols, body_clone)
            } else {
                // First reference — parse the CTE body and register it.
                // Check `op` field to handle UNION ALL CTE bodies (e.g.,
                // non-recursive CTEs that happen to use UNION ALL).
                // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
                let cte_stmt = pg_deref!(cte_ctx.raw_map[&table_name]);
                let cte_tree = if cte_stmt.op != pg_sys::SetOperation::SETOP_NONE {
                    // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
                    unsafe { parse_set_operation(cte_stmt, cte_ctx)? }
                } else {
                    // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
                    unsafe { parse_select_stmt(cte_stmt, "", cte_ctx)? }
                };
                let cols = cte_tree.output_columns();
                let body_clone = cte_tree.clone();
                let id = cte_ctx.register(&table_name, cte_tree);
                (id, cols, body_clone)
            };

            return Ok(OpTree::CteScan {
                cte_id,
                cte_name: table_name,
                alias,
                columns,
                cte_def_aliases: def_aliases,
                column_aliases: col_aliases,
                body: Some(Box::new(body_clone)),
            });
        }

        // Resolve the schema: use the explicit qualifier if present, otherwise
        // look up the table via the session search_path.  This avoids the
        // previous bug where unqualified names were hardcoded to "public",
        // causing a cryptic SPI error for tables in other schemas.
        let schema_name = match explicit_schema {
            Some(s) => s,
            None => match resolve_rangevar_schema(rv)? {
                Some(s) => s,
                None => {
                    return Err(PgTrickleError::NotFound(format!(
                        "table '{}' not found in the current search_path. \
                         Use a schema-qualified name (e.g. \"my_schema\".\"{}\") \
                         or add the schema to the search_path.",
                        table_name, table_name
                    )));
                }
            },
        };

        let table_oid = resolve_table_oid(&schema_name, &table_name)?;
        let columns = resolve_columns(table_oid)?;
        let pk_columns = resolve_pk_columns(table_oid)?;

        Ok(OpTree::Scan {
            table_oid,
            table_name,
            schema: schema_name,
            columns,
            pk_columns,
            alias,
        })
    } else if let Some(join) = cast_node!(node, T_JoinExpr, pg_sys::JoinExpr) {
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        let left = unsafe { parse_from_item(join.larg, cte_ctx)? };
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        let right = unsafe { parse_from_item(join.rarg, cte_ctx)? };

        // If right side is a LateralSubquery, handle LATERAL join semantics
        if let OpTree::LateralSubquery {
            subquery_sql,
            alias,
            column_aliases,
            output_cols,
            subquery_source_oids,
            correlation_predicates,
            ..
        } = right
        {
            match join.jointype {
                pg_sys::JoinType::JOIN_INNER => {
                    return Ok(OpTree::LateralSubquery {
                        subquery_sql,
                        alias,
                        column_aliases,
                        output_cols,
                        is_left_join: false,
                        subquery_source_oids,
                        correlation_predicates,
                        child: Box::new(left),
                    });
                }
                pg_sys::JoinType::JOIN_LEFT => {
                    return Ok(OpTree::LateralSubquery {
                        subquery_sql,
                        alias,
                        column_aliases,
                        output_cols,
                        is_left_join: true,
                        subquery_source_oids,
                        correlation_predicates,
                        child: Box::new(left),
                    });
                }
                other => {
                    return Err(PgTrickleError::UnsupportedOperator(format!(
                        "LATERAL subqueries support only INNER JOIN and LEFT JOIN. \
                         RIGHT JOIN LATERAL and FULL JOIN LATERAL are rejected by \
                         PostgreSQL itself because the lateral reference on the right \
                         side creates a dependency that conflicts with RIGHT/FULL JOIN \
                         semantics (got join type {:?}).",
                        other,
                    )));
                }
            }
        }

        // If right side is a LateralFunction from explicit JOIN syntax
        if let OpTree::LateralFunction {
            func_sql,
            alias,
            column_aliases,
            with_ordinality,
            ..
        } = right
        {
            match join.jointype {
                pg_sys::JoinType::JOIN_INNER | pg_sys::JoinType::JOIN_LEFT => {
                    return Ok(OpTree::LateralFunction {
                        func_sql,
                        alias,
                        column_aliases,
                        with_ordinality,
                        child: Box::new(left),
                    });
                }
                other => {
                    return Err(PgTrickleError::UnsupportedOperator(format!(
                        "LATERAL set-returning functions support only INNER JOIN and LEFT JOIN. \
                         RIGHT JOIN LATERAL and FULL JOIN LATERAL are rejected by \
                         PostgreSQL itself because the lateral reference on the right \
                         side creates a dependency that conflicts with RIGHT/FULL JOIN \
                         semantics (got join type {:?}).",
                        other,
                    )));
                }
            }
        }

        // ── NATURAL JOIN: resolve common columns and synthesize equi-join ──
        // PostgreSQL's raw parse tree leaves `quals` NULL for NATURAL JOIN.
        // We resolve common column names from the already-parsed left/right
        // OpTree nodes and build an explicit equi-join condition.
        if join.isNatural {
            let left_cols = left.output_columns();
            let right_cols = right.output_columns();

            // Find common columns (order follows left side for determinism).
            let right_set: std::collections::HashSet<String> = right_cols.iter().cloned().collect();
            let common: Vec<String> = left_cols
                .iter()
                .filter(|c| right_set.contains(c.as_str()))
                .cloned()
                .collect();

            if common.is_empty() {
                return Err(PgTrickleError::QueryParseError(
                    "NATURAL JOIN has no common columns between left and right sides. \
                     Use an explicit JOIN ... ON condition instead."
                        .into(),
                ));
            }

            // F38: Warn about NATURAL JOIN column drift risk.
            // If columns are added to either table that happen to match a column
            // on the other side, the join semantics change silently. Explicit
            // JOIN ... ON is recommended for production stream tables.
            // Push to the per-parse accumulator; the top-level caller emits
            // this exactly once via ParseResult.warnings.
            push_parse_warning(format!(
                "pg_trickle: NATURAL JOIN resolved on columns [{}]. \
                 Adding a same-named column to either table will silently change \
                 join semantics. Consider using explicit JOIN ... ON instead.",
                common.join(", "),
            ));

            // Build condition: left.col1 = right.col1 AND left.col2 = right.col2 ...
            let left_alias = left.alias();
            let right_alias = right.alias();

            let parts: Vec<String> = common
                .iter()
                .map(|col| {
                    let lq = format!(
                        "\"{}\".\"{}\"",
                        left_alias.replace('"', "\"\""),
                        col.replace('"', "\"\""),
                    );
                    let rq = format!(
                        "\"{}\".\"{}\"",
                        right_alias.replace('"', "\"\""),
                        col.replace('"', "\"\""),
                    );
                    format!("{lq} = {rq}")
                })
                .collect();

            let condition = Expr::Raw(parts.join(" AND "));

            return match join.jointype {
                pg_sys::JoinType::JOIN_INNER => Ok(OpTree::InnerJoin {
                    condition,
                    left: Box::new(left),
                    right: Box::new(right),
                }),
                pg_sys::JoinType::JOIN_LEFT => Ok(OpTree::LeftJoin {
                    condition,
                    left: Box::new(left),
                    right: Box::new(right),
                }),
                pg_sys::JoinType::JOIN_RIGHT => Ok(OpTree::LeftJoin {
                    condition,
                    left: Box::new(right),
                    right: Box::new(left),
                }),
                pg_sys::JoinType::JOIN_FULL => Ok(OpTree::FullJoin {
                    condition,
                    left: Box::new(left),
                    right: Box::new(right),
                }),
                other => Err(PgTrickleError::UnsupportedOperator(format!(
                    "NATURAL JOIN type {other:?} not supported",
                ))),
            };
        }

        let condition = if join.quals.is_null() {
            Expr::Literal("TRUE".into())
        } else {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            safe_node_to_expr(join.quals)?
        };

        match join.jointype {
            pg_sys::JoinType::JOIN_INNER => Ok(OpTree::InnerJoin {
                condition,
                left: Box::new(left),
                right: Box::new(right),
            }),
            pg_sys::JoinType::JOIN_LEFT => Ok(OpTree::LeftJoin {
                condition,
                left: Box::new(left),
                right: Box::new(right),
            }),
            pg_sys::JoinType::JOIN_RIGHT => {
                // RIGHT JOIN(A, B) → LEFT JOIN(B, A) with swapped operands
                Ok(OpTree::LeftJoin {
                    condition,
                    left: Box::new(right),
                    right: Box::new(left),
                })
            }
            pg_sys::JoinType::JOIN_FULL => Ok(OpTree::FullJoin {
                condition,
                left: Box::new(left),
                right: Box::new(right),
            }),
            other => Err(PgTrickleError::UnsupportedOperator(format!(
                "Join type {other:?} not supported for differential mode",
            ))),
        }
    } else if let Some(sub) = cast_node!(node, T_RangeSubselect, pg_sys::RangeSubselect) {
        if sub.subquery.is_null() {
            return Err(PgTrickleError::QueryParseError(
                "RangeSubselect with NULL subquery".into(),
            ));
        }

        // The subquery must be a SelectStmt
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if !is_node_type!(sub.subquery, T_SelectStmt) {
            return Err(PgTrickleError::QueryParseError(
                "Subquery in FROM must be a SELECT statement".into(),
            ));
        }

        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let sub_stmt = pg_deref!(sub.subquery as *const pg_sys::SelectStmt);

        // Extract alias name
        let alias = if !sub.alias.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let a = pg_deref!(sub.alias);
            pg_cstr_to_str(a.aliasname)
                .unwrap_or("subquery")
                .to_string()
        } else {
            "subquery".to_string()
        };

        // Extract column aliases from the alias, if any
        let col_aliases = if !sub.alias.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let a = pg_deref!(sub.alias);
            extract_alias_colnames(a)?
        } else {
            Vec::new()
        };

        if sub.lateral {
            // ── LATERAL subquery: store as raw SQL ─────────────────────
            // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
            let subquery_sql = unsafe { deparse_select_stmt_to_sql(sub_stmt)? };

            // Extract output column names from the subquery's SELECT list
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            let output_cols = unsafe { extract_select_output_cols(sub_stmt.targetList)? };

            // Extract source table OIDs from the subquery's FROM clause
            let subquery_source_oids =
                // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
                unsafe { extract_select_source_oids(sub_stmt)? };

            // P2-6: Extract alias→OID mapping and correlation predicates
            // for scoped inner-change re-execution.
            let inner_alias_oids =
                // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
                unsafe { extract_from_alias_oids(sub_stmt.fromClause)? };
            let correlation_predicates =
                // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
                unsafe { extract_correlation_predicates(sub_stmt.whereClause, &inner_alias_oids) };

            // Return a LateralSubquery with a placeholder child.
            // The real child is attached in the FROM-list loop or JoinExpr handler.
            return Ok(OpTree::LateralSubquery {
                subquery_sql,
                alias,
                column_aliases: col_aliases,
                output_cols,
                is_left_join: false,
                subquery_source_oids,
                correlation_predicates,
                child: Box::new(OpTree::Scan {
                    table_oid: 0,
                    table_name: String::new(),
                    schema: String::new(),
                    columns: vec![],
                    pk_columns: vec![],
                    alias: String::new(),
                }),
            });
        }

        // ── Non-LATERAL subquery: existing code path ───────────────────
        // Check `op` to handle subqueries that are set operations
        let sub_tree = if sub_stmt.op != pg_sys::SetOperation::SETOP_NONE {
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            unsafe { parse_set_operation(sub_stmt, cte_ctx)? }
        } else {
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            unsafe { parse_select_stmt(sub_stmt, "", cte_ctx)? }
        };

        Ok(OpTree::Subquery {
            alias,
            column_aliases: col_aliases,
            child: Box::new(sub_tree),
        })
    } else if let Some(rf) = cast_node!(node, T_RangeFunction, pg_sys::RangeFunction) {
        // Extract the function call from rf.functions (a List of Lists).
        // Each element is a two-element List: [FuncCall, column_def_list].
        let func_list = pg_list::<pg_sys::Node>(rf.functions);
        if func_list.is_empty() {
            return Err(PgTrickleError::QueryParseError(
                "RangeFunction with no functions".into(),
            ));
        }

        // We support a single function. ROWS FROM(f1, f2, ...) is not supported.
        if rf.is_rowsfrom && func_list.len() > 1 {
            return Err(PgTrickleError::UnsupportedOperator(
                "ROWS FROM() with multiple functions is not supported. \
                 Use a single set-returning function in FROM instead."
                    .into(),
            ));
        }

        // The first element is a List node; its first element is the FuncCall.
        // SAFETY: func_list is non-empty, head is a List node containing the FuncCall.
        let inner_list_node = func_list.head().ok_or_else(|| {
            PgTrickleError::QueryParseError(
                "RangeFunction func_list head is None despite non-empty check".into(),
            )
        })?;
        let inner_list = pg_list::<pg_sys::Node>(inner_list_node as *mut pg_sys::List);
        if inner_list.is_empty() {
            return Err(PgTrickleError::QueryParseError(
                "RangeFunction inner list is empty".into(),
            ));
        }

        let func_node = inner_list.head().ok_or_else(|| {
            PgTrickleError::QueryParseError(
                "RangeFunction inner_list head is None despite non-empty check".into(),
            )
        })?;
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if !is_node_type!(func_node, T_FuncCall) {
            return Err(PgTrickleError::QueryParseError(
                "RangeFunction does not contain a FuncCall node".into(),
            ));
        }

        // Deparse the function call to SQL text.
        // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
        let func_sql = unsafe { deparse_func_call(func_node as *const pg_sys::FuncCall)? };

        // Extract alias name
        let alias = if !rf.alias.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let a = pg_deref!(rf.alias);
            pg_cstr_to_str(a.aliasname).unwrap_or("srf").to_string()
        } else {
            "srf".to_string()
        };

        // Extract column aliases (e.g., AS child(key, value))
        let column_aliases = if !rf.alias.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let a = pg_deref!(rf.alias);
            extract_alias_colnames(a)?
        } else {
            Vec::new()
        };

        let with_ordinality = rf.ordinality;

        // Return a LateralFunction with a placeholder child.
        // The real child is attached in the FROM-list loop in parse_select_stmt().
        Ok(OpTree::LateralFunction {
            func_sql,
            alias,
            column_aliases,
            with_ordinality,
            child: Box::new(OpTree::Scan {
                table_oid: 0,
                table_name: String::new(),
                schema: String::new(),
                columns: vec![],
                pk_columns: vec![],
                alias: String::new(),
            }),
        })
    } else if let Some(jt) = cast_node!(node, T_JsonTable, pg_sys::JsonTable) {
        // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
        let func_sql = unsafe { deparse_json_table(jt as *const pg_sys::JsonTable)? };

        // Extract alias
        let alias = if !jt.alias.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let a = pg_deref!(jt.alias);
            pg_cstr_to_str(a.aliasname).unwrap_or("jt").to_string()
        } else {
            "jt".to_string()
        };

        // Extract column aliases from the alias node
        let column_aliases = if !jt.alias.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let a = pg_deref!(jt.alias);
            extract_alias_colnames(a)?
        } else {
            Vec::new()
        };

        // JSON_TABLE is inherently lateral (references the left-hand table).
        // Model it as LateralFunction with a placeholder child that gets
        // replaced in the FROM-list loop of parse_select_stmt().
        Ok(OpTree::LateralFunction {
            func_sql,
            alias,
            column_aliases,
            with_ordinality: false,
            child: Box::new(OpTree::Scan {
                table_oid: 0,
                table_name: String::new(),
                schema: String::new(),
                columns: vec![],
                pk_columns: vec![],
                alias: String::new(),
            }),
        })
    // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
    } else if is_node_type!(node, T_RangeTableSample) {
        // TABLESAMPLE: SELECT * FROM t TABLESAMPLE BERNOULLI(10)
        // Stream tables materialize complete result sets; sampling at parse
        // time is not meaningful or supported.
        Err(PgTrickleError::UnsupportedOperator(
            "TABLESAMPLE is not supported in defining queries. \
             Stream tables materialize the complete result set; \
             use a WHERE condition with random() if sampling is needed."
                .into(),
        ))
    } else {
        Err(PgTrickleError::UnsupportedOperator(format!(
            "Unsupported FROM item node type: {:?}",
            pg_deref!(node).type_,
        )))
    }
}

/// Return type for [`extract_cte_map_with_recursive`]:
/// (non_recursive_map, def_aliases, recursive_cte_stmts).
///
/// `recursive_cte_stmts` is a Vec of `(name, base_stmt, recursive_stmt, def_col_aliases, union_all)`.
type RecursiveCteMapResult = (
    HashMap<String, *const pg_sys::SelectStmt>,
    HashMap<String, Vec<String>>,
    Vec<(
        String,
        *const pg_sys::SelectStmt,
        *const pg_sys::SelectStmt,
        Vec<String>,
        bool,
    )>,
);

/// Extract CTE definitions from a WithClause, handling both recursive and
/// non-recursive CTEs.
///
/// Non-recursive CTEs go into the `name→SelectStmt` map (as before).
/// Recursive CTEs are returned as `(name, base_stmt, recursive_stmt, def_cols, union_all)`
/// tuples for separate parsing into `OpTree::RecursiveCte`.
///
/// # Safety
/// Caller must ensure `with_clause` points to a valid `WithClause` node.
unsafe fn extract_cte_map_with_recursive(
    with_clause: *const pg_sys::WithClause,
) -> Result<RecursiveCteMapResult, PgTrickleError> {
    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let wc = pg_deref!(with_clause);
    let cte_list = pg_list::<pg_sys::Node>(wc.ctes);
    let mut map = HashMap::new();
    let mut def_aliases_map = HashMap::new();
    let mut recursive_stmts = Vec::new();

    for node_ptr in cte_list.iter_ptr() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if !is_node_type!(node_ptr, T_CommonTableExpr) {
            continue;
        }

        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let cte = pg_deref!(node_ptr as *const pg_sys::CommonTableExpr);

        // Extract CTE name
        let cte_name = pg_cstr_to_str(cte.ctename)?.to_string();

        // The CTE body is ctequery, which must be a SelectStmt
        if cte.ctequery.is_null() {
            return Err(PgTrickleError::QueryParseError(format!(
                "CTE '{cte_name}' has NULL body",
            )));
        }
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if !is_node_type!(cte.ctequery, T_SelectStmt) {
            return Err(PgTrickleError::QueryParseError(format!(
                "CTE '{cte_name}' body is not a SELECT statement",
            )));
        }

        // Extract column definition aliases: WITH x(a, b) AS (...)
        let col_aliases = extract_cte_def_colnames(cte)?;
        if !col_aliases.is_empty() {
            def_aliases_map.insert(cte_name.clone(), col_aliases.clone());
        }

        // FEAT-1 (v0.61.0): Detect SEARCH ... FIRST BY and CYCLE ... SET ... USING
        // clauses — these are not yet supported in DIFFERENTIAL mode.
        if !cte.search_clause.is_null() {
            return Err(PgTrickleError::UnsupportedOperator(format!(
                "SEARCH clause (SEARCH BREADTH/DEPTH FIRST BY) is not yet \
                 supported in differential mode (planned for v1.1.0). CTE: '{cte_name}'. \
                 Remove the SEARCH clause from the recursive CTE definition, or use \
                 REFRESH MODE FULL if the query requires it."
            )));
        }
        if !cte.cycle_clause.is_null() {
            return Err(PgTrickleError::UnsupportedOperator(format!(
                "CYCLE clause (CYCLE ... SET ... USING) is not yet \
                 supported in differential mode (planned for v1.1.0). CTE: '{cte_name}'. \
                 Remove the CYCLE clause from the recursive CTE definition, or use \
                 REFRESH MODE FULL if the query requires it."
            )));
        }

        // Detect recursive CTEs: In PG18's raw_parser output,
        // cte.cterecursive is NOT set (it's only populated by the
        // analyzer). We detect recursion by checking: (a) the WITH
        // clause has the RECURSIVE keyword (wc.recursive), AND
        // (b) the CTE body is a UNION or UNION ALL (op == SETOP_UNION).
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let body = pg_deref!(cte.ctequery as *const pg_sys::SelectStmt);
        let is_recursive =
            cte.cterecursive || (wc.recursive && body.op == pg_sys::SetOperation::SETOP_UNION);

        if is_recursive {
            // Recursive CTE — split UNION [ALL] into base + recursive terms
            if body.op != pg_sys::SetOperation::SETOP_UNION {
                return Err(PgTrickleError::QueryParseError(format!(
                    "Recursive CTE '{cte_name}' body must be a UNION or UNION ALL",
                )));
            }

            if body.larg.is_null() || body.rarg.is_null() {
                return Err(PgTrickleError::QueryParseError(format!(
                    "Recursive CTE '{cte_name}' UNION is missing left or right arm",
                )));
            }

            let base_stmt = body.larg as *const pg_sys::SelectStmt;
            let rec_stmt = body.rarg as *const pg_sys::SelectStmt;
            let union_all = body.all;

            recursive_stmts.push((cte_name, base_stmt, rec_stmt, col_aliases, union_all));
        } else {
            // Non-recursive CTE — add to normal map
            let stmt = cte.ctequery as *const pg_sys::SelectStmt;
            map.insert(cte_name, stmt);
        }
    }

    Ok((map, def_aliases_map, recursive_stmts))
}

/// Extract column aliases from a `CommonTableExpr.aliascolnames` list.
///
/// These are the column aliases on the CTE definition itself:
/// `WITH x(a, b) AS (SELECT id, name FROM ...)`.
///
/// Returns an empty `Vec` if no column aliases are specified.
pub(crate) fn extract_cte_def_colnames(
    cte: &pg_sys::CommonTableExpr,
) -> Result<Vec<String>, PgTrickleError> {
    let colnames = pg_list::<pg_sys::Node>(cte.aliascolnames);
    let mut names = Vec::new();
    for node_ptr in colnames.iter_ptr() {
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        let name = safe_node_to_string(node_ptr)?;
        names.push(name);
    }
    Ok(names)
}

/// Extract column aliases from an Alias node's colnames list.
///
/// Returns an empty Vec if the alias has no explicit column names
/// (i.e. `FROM x AS alias` without `(c1, c2, ...)`).
///
/// # Safety
/// Caller must ensure `alias` points to a valid `Alias` struct.
pub(crate) fn extract_alias_colnames(alias: &pg_sys::Alias) -> Result<Vec<String>, PgTrickleError> {
    let colnames = pg_list::<pg_sys::Node>(alias.colnames);
    let mut names = Vec::new();
    for node_ptr in colnames.iter_ptr() {
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        let name = safe_node_to_string(node_ptr)?;
        names.push(name);
    }
    Ok(names)
}

/// Resolve a table name to its OID via SPI.
fn resolve_table_oid(schema: &str, table: &str) -> Result<u32, PgTrickleError> {
    let sql = format!(
        "SELECT c.oid FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = '{}' AND c.relname = '{}'",
        schema.replace('\'', "''"),
        table.replace('\'', "''"),
    );

    Spi::connect(|client| {
        let mut result = client
            .select(&sql, None, &[])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        // Use iterator to safely handle empty result sets — calling
        // .first().get() on an empty SpiTupleTable throws "positioned
        // before the start or after the end" instead of a useful error.
        if let Some(row) = result.next() {
            let oid: Option<pg_sys::Oid> = row
                .get(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            return oid
                .map(|o| o.to_u32())
                .ok_or_else(|| PgTrickleError::NotFound(format!("{schema}.{table}")));
        }
        Err(PgTrickleError::NotFound(format!(
            "table '{schema}.{table}' not found in pg_class — \
             if the table is not in schema '{schema}', use a schema-qualified \
             name in the query (e.g. \"my_schema\".\"{}\")",
            table
        )))
    })
}

/// Resolve column metadata for a table via SPI.
fn resolve_columns(table_oid: u32) -> Result<Vec<Column>, PgTrickleError> {
    // Filter out internal __pgt_* columns (e.g. __pgt_row_id) that exist
    // on stream table storage tables. These are implementation details
    // and must not participate in row_id computation or delta queries.
    // Mirrors the filter in resolve_st_output_columns().
    //
    // Also filter out generated columns (attgenerated <> '').  PostgreSQL
    // GENERATED ALWAYS AS … STORED columns are excluded from change buffer
    // tables by resolve_source_column_defs() (which uses AND attgenerated = '').
    // If we include them here, the DVM would emit c."new_<col>" references
    // that do not exist in the change buffer, causing errors like
    // "column c.new_mid does not exist" at refresh time.
    let sql = format!(
        "SELECT attname::text, atttypid, attnotnull \
         FROM pg_attribute \
         WHERE attrelid = {} AND attnum > 0 AND NOT attisdropped \
           AND attname::text NOT LIKE '__pgt_%%' \
           AND attgenerated = '' \
         ORDER BY attnum",
        table_oid,
    );

    Spi::connect(|client| {
        let result = client
            .select(&sql, None, &[])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        let mut columns = Vec::new();
        for row in result {
            let name: String = row
                .get(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or_default();
            let type_oid: pg_sys::Oid = row
                .get(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(pg_sys::Oid::INVALID);
            let not_null: bool = row
                .get(3)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(false);
            columns.push(Column {
                name,
                type_oid: type_oid.to_u32(),
                is_nullable: !not_null,
            });
        }
        Ok(columns)
    })
}

/// Resolve primary key column names for a table via `pg_constraint`.
///
/// Returns columns in key order. Returns an empty Vec if no PK exists.
fn resolve_pk_columns(table_oid: u32) -> Result<Vec<String>, PgTrickleError> {
    let sql = format!(
        "SELECT a.attname::text \
         FROM pg_constraint c \
         JOIN pg_attribute a ON a.attrelid = c.conrelid \
           AND a.attnum = ANY(c.conkey) \
         WHERE c.conrelid = {} AND c.contype = 'p' \
         ORDER BY array_position(c.conkey, a.attnum)",
        table_oid,
    );

    Spi::connect(|client| {
        let result = client
            .select(&sql, None, &[])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        let mut pk_cols = Vec::new();
        for row in result {
            let name: String = row
                .get(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or_default();
            pk_cols.push(name);
        }
        Ok(pk_cols)
    })
}

/// Convert a pg_sys::Node to an Expr.
pub(crate) unsafe fn node_to_expr(node: *mut pg_sys::Node) -> Result<Expr, PgTrickleError> {
    if node.is_null() {
        return Err(PgTrickleError::QueryParseError(
            "NULL node in expression".into(),
        ));
    }

    if let Some(cref) = cast_node!(node, T_ColumnRef, pg_sys::ColumnRef) {
        let fields = pg_list::<pg_sys::Node>(cref.fields);

        match fields.len() {
            1 => {
                let field = fields.head().ok_or_else(|| {
                    PgTrickleError::InternalError(
                        "ColumnRef fields list has len=1 but head() returned None".into(),
                    )
                })?;
                // Bare `SELECT *` arrives as ColumnRef with a single A_Star field.
                // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
                if is_node_type!(field, T_A_Star) {
                    return Ok(Expr::Star { table_alias: None });
                }
                // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
                let col_name = safe_node_to_string(field)?;
                Ok(Expr::ColumnRef {
                    table_alias: None,
                    column_name: col_name,
                })
            }
            2 => {
                // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
                let table_alias = safe_node_to_string(fields.get_ptr(0).ok_or_else(|| {
                    PgTrickleError::InternalError("ColumnRef fields[0] is None".into())
                })?)?;
                let last = fields.get_ptr(1).ok_or_else(|| {
                    PgTrickleError::InternalError("ColumnRef fields[1] is None".into())
                })?;
                // `table.*` arrives as ColumnRef with fields [T_String, T_A_Star].
                // T_A_Star is not a T_String, so node_to_string falls back to
                // "node_T_A_Star" — check explicitly before calling it.
                // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
                if is_node_type!(last, T_A_Star) {
                    return Ok(Expr::Star {
                        table_alias: Some(table_alias),
                    });
                }
                // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
                let col_name = safe_node_to_string(last)?;
                Ok(Expr::ColumnRef {
                    table_alias: Some(table_alias),
                    column_name: col_name,
                })
            }
            3 => {
                // schema.table.column — drop the schema, use table.column
                // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
                let _schema = safe_node_to_string(fields.get_ptr(0).ok_or_else(|| {
                    PgTrickleError::InternalError("ColumnRef fields[0] is None".into())
                })?)?;
                // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
                let table_alias = safe_node_to_string(fields.get_ptr(1).ok_or_else(|| {
                    PgTrickleError::InternalError("ColumnRef fields[1] is None".into())
                })?)?;
                let last = fields.get_ptr(2).ok_or_else(|| {
                    PgTrickleError::InternalError("ColumnRef fields[2] is None".into())
                })?;
                // `schema.table.*` — same T_A_Star guard as the 2-field case.
                // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
                if is_node_type!(last, T_A_Star) {
                    return Ok(Expr::Star {
                        table_alias: Some(table_alias),
                    });
                }
                // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
                let col_name = safe_node_to_string(last)?;
                Ok(Expr::ColumnRef {
                    table_alias: Some(table_alias),
                    column_name: col_name,
                })
            }
            n => Err(PgTrickleError::QueryParseError(format!(
                "Unexpected ColumnRef with {n} fields",
            ))),
        }
    // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
    } else if is_node_type!(node, T_A_Const) {
        // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
        Ok(Expr::Raw(unsafe { deparse_node(node) }))
    } else if let Some(aexpr) = cast_node!(node, T_A_Expr, pg_sys::A_Expr) {
        match aexpr.kind {
            pg_sys::A_Expr_Kind::AEXPR_OP => {
                // SAFETY: Parse-tree node pointer from raw_parser; valid within current memory context.
                let op_name = unsafe { extract_operator_name(aexpr.name)? };
                // Handle unary prefix operators (e.g., -x) where lexpr is NULL
                if aexpr.lexpr.is_null() {
                    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                    let right = safe_node_to_expr(aexpr.rexpr)?;
                    return Ok(Expr::Raw(format!("{op_name}{}", right.to_sql())));
                }
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let left = safe_node_to_expr(aexpr.lexpr)?;
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let right = safe_node_to_expr(aexpr.rexpr)?;
                Ok(Expr::BinaryOp {
                    op: op_name,
                    left: Box::new(left),
                    right: Box::new(right),
                })
            }
            pg_sys::A_Expr_Kind::AEXPR_DISTINCT => {
                // IS DISTINCT FROM
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let left = safe_node_to_expr(aexpr.lexpr)?;
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let right = safe_node_to_expr(aexpr.rexpr)?;
                Ok(Expr::Raw(format!(
                    "{} IS DISTINCT FROM {}",
                    left.to_sql(),
                    right.to_sql()
                )))
            }
            pg_sys::A_Expr_Kind::AEXPR_NOT_DISTINCT => {
                // IS NOT DISTINCT FROM
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let left = safe_node_to_expr(aexpr.lexpr)?;
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let right = safe_node_to_expr(aexpr.rexpr)?;
                Ok(Expr::Raw(format!(
                    "{} IS NOT DISTINCT FROM {}",
                    left.to_sql(),
                    right.to_sql()
                )))
            }
            pg_sys::A_Expr_Kind::AEXPR_IN => {
                // x IN (v1, v2, v3)
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let left = safe_node_to_expr(aexpr.lexpr)?;
                let right_list = pg_list::<pg_sys::Node>(aexpr.rexpr as *mut _);
                let mut vals = Vec::new();
                for n in right_list.iter_ptr() {
                    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                    vals.push(safe_node_to_expr(n)?.to_sql());
                }
                // SAFETY: Parse-tree node pointer from raw_parser; valid within current memory context.
                let op_name = unsafe { extract_operator_name(aexpr.name) }
                    .unwrap_or_else(|_| "=".to_string());
                if op_name == "<>" {
                    Ok(Expr::Raw(format!(
                        "{} NOT IN ({})",
                        left.to_sql(),
                        vals.join(", ")
                    )))
                } else {
                    Ok(Expr::Raw(format!(
                        "{} IN ({})",
                        left.to_sql(),
                        vals.join(", ")
                    )))
                }
            }
            pg_sys::A_Expr_Kind::AEXPR_BETWEEN => {
                // x BETWEEN a AND b
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let tested = safe_node_to_expr(aexpr.lexpr)?;
                let bounds = pg_list::<pg_sys::Node>(aexpr.rexpr as *mut _);
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let low = safe_node_to_expr(bounds.get_ptr(0).ok_or_else(|| {
                    PgTrickleError::InternalError("BETWEEN bounds[0] is None".into())
                })?)?;
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let high = safe_node_to_expr(bounds.get_ptr(1).ok_or_else(|| {
                    PgTrickleError::InternalError("BETWEEN bounds[1] is None".into())
                })?)?;
                Ok(Expr::Raw(format!(
                    "{} BETWEEN {} AND {}",
                    tested.to_sql(),
                    low.to_sql(),
                    high.to_sql()
                )))
            }
            pg_sys::A_Expr_Kind::AEXPR_NOT_BETWEEN => {
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let tested = safe_node_to_expr(aexpr.lexpr)?;
                let bounds = pg_list::<pg_sys::Node>(aexpr.rexpr as *mut _);
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let low = safe_node_to_expr(bounds.get_ptr(0).ok_or_else(|| {
                    PgTrickleError::InternalError("NOT BETWEEN bounds[0] is None".into())
                })?)?;
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let high = safe_node_to_expr(bounds.get_ptr(1).ok_or_else(|| {
                    PgTrickleError::InternalError("NOT BETWEEN bounds[1] is None".into())
                })?)?;
                Ok(Expr::Raw(format!(
                    "{} NOT BETWEEN {} AND {}",
                    tested.to_sql(),
                    low.to_sql(),
                    high.to_sql()
                )))
            }
            pg_sys::A_Expr_Kind::AEXPR_BETWEEN_SYM => {
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let tested = safe_node_to_expr(aexpr.lexpr)?;
                let bounds = pg_list::<pg_sys::Node>(aexpr.rexpr as *mut _);
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let low = safe_node_to_expr(bounds.get_ptr(0).ok_or_else(|| {
                    PgTrickleError::InternalError("BETWEEN SYMMETRIC bounds[0] is None".into())
                })?)?;
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let high = safe_node_to_expr(bounds.get_ptr(1).ok_or_else(|| {
                    PgTrickleError::InternalError("BETWEEN SYMMETRIC bounds[1] is None".into())
                })?)?;
                Ok(Expr::Raw(format!(
                    "{} BETWEEN SYMMETRIC {} AND {}",
                    tested.to_sql(),
                    low.to_sql(),
                    high.to_sql()
                )))
            }
            pg_sys::A_Expr_Kind::AEXPR_NOT_BETWEEN_SYM => {
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let tested = safe_node_to_expr(aexpr.lexpr)?;
                let bounds = pg_list::<pg_sys::Node>(aexpr.rexpr as *mut _);
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let low = safe_node_to_expr(bounds.get_ptr(0).ok_or_else(|| {
                    PgTrickleError::InternalError("NOT BETWEEN SYMMETRIC bounds[0] is None".into())
                })?)?;
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let high = safe_node_to_expr(bounds.get_ptr(1).ok_or_else(|| {
                    PgTrickleError::InternalError("NOT BETWEEN SYMMETRIC bounds[1] is None".into())
                })?)?;
                Ok(Expr::Raw(format!(
                    "{} NOT BETWEEN SYMMETRIC {} AND {}",
                    tested.to_sql(),
                    low.to_sql(),
                    high.to_sql()
                )))
            }
            pg_sys::A_Expr_Kind::AEXPR_SIMILAR => {
                // SIMILAR TO
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let left = safe_node_to_expr(aexpr.lexpr)?;
                // rexpr for SIMILAR TO is a FuncCall wrapping the pattern
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let right = safe_node_to_expr(aexpr.rexpr)?;
                Ok(Expr::Raw(format!(
                    "{} SIMILAR TO {}",
                    left.to_sql(),
                    right.to_sql()
                )))
            }
            pg_sys::A_Expr_Kind::AEXPR_LIKE => {
                // [NOT] LIKE — name is "~~" (LIKE) or "!~~" (NOT LIKE).
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let left = safe_node_to_expr(aexpr.lexpr)?;
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let right = safe_node_to_expr(aexpr.rexpr)?;
                // SAFETY: Parse-tree node pointer from raw_parser; valid within current memory context.
                let op_name = unsafe { extract_operator_name(aexpr.name) }
                    .unwrap_or_else(|_| "~~".to_string());
                let kw = if op_name == "!~~" { "NOT LIKE" } else { "LIKE" };
                Ok(Expr::Raw(format!(
                    "{} {kw} {}",
                    left.to_sql(),
                    right.to_sql()
                )))
            }
            pg_sys::A_Expr_Kind::AEXPR_ILIKE => {
                // [NOT] ILIKE — name is "~~*" (ILIKE) or "!~~*" (NOT ILIKE).
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let left = safe_node_to_expr(aexpr.lexpr)?;
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let right = safe_node_to_expr(aexpr.rexpr)?;
                // SAFETY: Parse-tree node pointer from raw_parser; valid within current memory context.
                let op_name = unsafe { extract_operator_name(aexpr.name) }
                    .unwrap_or_else(|_| "~~*".to_string());
                let kw = if op_name == "!~~*" {
                    "NOT ILIKE"
                } else {
                    "ILIKE"
                };
                Ok(Expr::Raw(format!(
                    "{} {kw} {}",
                    left.to_sql(),
                    right.to_sql()
                )))
            }
            pg_sys::A_Expr_Kind::AEXPR_OP_ANY => {
                // expr op ANY(array)
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let left = safe_node_to_expr(aexpr.lexpr)?;
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let right = safe_node_to_expr(aexpr.rexpr)?;
                // SAFETY: Parse-tree node pointer from raw_parser; valid within current memory context.
                let op_name = unsafe { extract_operator_name(aexpr.name)? };
                Ok(Expr::Raw(format!(
                    "{} {op_name} ANY({})",
                    left.to_sql(),
                    right.to_sql()
                )))
            }
            pg_sys::A_Expr_Kind::AEXPR_OP_ALL => {
                // expr op ALL(array)
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let left = safe_node_to_expr(aexpr.lexpr)?;
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let right = safe_node_to_expr(aexpr.rexpr)?;
                // SAFETY: Parse-tree node pointer from raw_parser; valid within current memory context.
                let op_name = unsafe { extract_operator_name(aexpr.name)? };
                Ok(Expr::Raw(format!(
                    "{} {op_name} ALL({})",
                    left.to_sql(),
                    right.to_sql()
                )))
            }
            pg_sys::A_Expr_Kind::AEXPR_NULLIF => {
                // NULLIF(a, b) — evaluates to a when a <> b, else NULL.
                // Represented in the raw parse tree as AEXPR_NULLIF; handled
                // here so expressions like `NULLIF(col, '')::bigint` in the
                // SELECT list don't cause an unsupported-operator error.
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let left = safe_node_to_expr(aexpr.lexpr)?;
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let right = safe_node_to_expr(aexpr.rexpr)?;
                Ok(Expr::Raw(format!(
                    "NULLIF({}, {})",
                    left.to_sql(),
                    right.to_sql()
                )))
            }
            _other => Err(PgTrickleError::UnsupportedOperator(format!(
                "A_Expr kind {:?} is not supported in defining queries",
                _other,
            ))),
        }
    } else if let Some(bexpr) = cast_node!(node, T_BoolExpr, pg_sys::BoolExpr) {
        let args_list = pg_list::<pg_sys::Node>(bexpr.args);
        let mut args = Vec::new();
        for n in args_list.iter_ptr() {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            if let Ok(e) = safe_node_to_expr(n) {
                args.push(e);
            }
        }

        match bexpr.boolop {
            pg_sys::BoolExprType::AND_EXPR => {
                if args.len() < 2 {
                    return args.into_iter().next().ok_or_else(|| {
                        PgTrickleError::QueryParseError("Empty AND expression".into())
                    });
                }
                let mut result = args[0].clone();
                for arg in &args[1..] {
                    result = Expr::BinaryOp {
                        op: "AND".to_string(),
                        left: Box::new(result),
                        right: Box::new(arg.clone()),
                    };
                }
                Ok(result)
            }
            pg_sys::BoolExprType::OR_EXPR => {
                if args.len() < 2 {
                    return args.into_iter().next().ok_or_else(|| {
                        PgTrickleError::QueryParseError("Empty OR expression".into())
                    });
                }
                let mut result = args[0].clone();
                for arg in &args[1..] {
                    result = Expr::BinaryOp {
                        op: "OR".to_string(),
                        left: Box::new(result),
                        right: Box::new(arg.clone()),
                    };
                }
                Ok(result)
            }
            pg_sys::BoolExprType::NOT_EXPR => {
                let inner = args.into_iter().next().ok_or_else(|| {
                    PgTrickleError::QueryParseError("Empty NOT expression".into())
                })?;
                Ok(Expr::FuncCall {
                    func_name: "NOT".to_string(),
                    args: vec![inner],
                })
            }
            _ => Ok(Expr::Raw("/* unknown bool expr */".to_string())),
        }
    } else if let Some(fcall) = cast_node!(node, T_FuncCall, pg_sys::FuncCall) {
        // SAFETY: Parse-tree node pointer from raw_parser; valid within current memory context.
        let func_name = unsafe { extract_func_name(fcall.funcname)? };

        // Handle COUNT(*) and other agg_star calls.
        // PostgreSQL represents COUNT(*) as FuncCall { agg_star: true, args: [] }.
        // We must emit the "*" explicitly so to_sql() produces COUNT(*) not COUNT().
        let base_args_str = if fcall.agg_star {
            "*".to_string()
        } else {
            let args_list = pg_list::<pg_sys::Node>(fcall.args);
            let mut args = Vec::new();
            for n in args_list.iter_ptr() {
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                if let Ok(e) = safe_node_to_expr(n) {
                    args.push(e.to_sql());
                }
            }
            args.join(", ")
        };

        // Window functions: FuncCall with non-null .over → emit full OVER clause
        if !fcall.over.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let over = pg_deref!(fcall.over);
            let mut over_parts = Vec::new();

            // PARTITION BY
            if !over.partitionClause.is_null() {
                let parts_list = pg_list::<pg_sys::Node>(over.partitionClause);
                let mut pk = Vec::new();
                for p in parts_list.iter_ptr() {
                    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                    pk.push(safe_node_to_expr(p)?.to_sql());
                }
                over_parts.push(format!("PARTITION BY {}", pk.join(", ")));
            }

            // ORDER BY
            if !over.orderClause.is_null() {
                let sort_list = pg_list::<pg_sys::Node>(over.orderClause);
                // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
                let sort_sql = unsafe { deparse_sort_clause(&sort_list)? };
                over_parts.push(format!("ORDER BY {}", sort_sql));
            }

            // Frame clause (ROWS/RANGE/GROUPS BETWEEN ... AND ...)
            // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
            if let Some(frame_sql) = unsafe { deparse_window_frame(over) } {
                over_parts.push(frame_sql);
            }

            let over_sql = over_parts.join(" ");
            return Ok(Expr::Raw(format!(
                "{func_name}({base_args_str}) OVER ({over_sql})"
            )));
        }

        if fcall.agg_star {
            Ok(Expr::FuncCall {
                func_name,
                args: vec![Expr::Raw("*".to_string())],
            })
        } else {
            let args_list = pg_list::<pg_sys::Node>(fcall.args);
            let mut args = Vec::new();
            for n in args_list.iter_ptr() {
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                if let Ok(e) = safe_node_to_expr(n) {
                    args.push(e);
                }
            }
            Ok(Expr::FuncCall { func_name, args })
        }
    } else if let Some(tc) = cast_node!(node, T_TypeCast, pg_sys::TypeCast) {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let inner = safe_node_to_expr(tc.arg)?;
        // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
        let type_name = unsafe { deparse_typename(tc.typeName) };
        Ok(Expr::Raw(format!(
            "CAST({} AS {})",
            inner.to_sql(),
            type_name,
        )))
    } else if let Some(nt) = cast_node!(node, T_NullTest, pg_sys::NullTest) {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let arg = safe_node_to_expr(nt.arg as *mut pg_sys::Node)?;
        let op = if nt.nulltesttype == pg_sys::NullTestType::IS_NULL {
            "IS NULL"
        } else {
            "IS NOT NULL"
        };
        Ok(Expr::Raw(format!("{} {op}", arg.to_sql())))
    } else if let Some(case_expr) = cast_node!(node, T_CaseExpr, pg_sys::CaseExpr) {
        let mut sql = String::from("CASE");

        // Simple CASE: CASE <arg> WHEN ...
        if !case_expr.arg.is_null() {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let arg = safe_node_to_expr(case_expr.arg as *mut pg_sys::Node)?;
            sql.push_str(&format!(" {}", arg.to_sql()));
        }

        let when_list = pg_list::<pg_sys::Node>(case_expr.args);
        for w in when_list.iter_ptr() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let case_when = pg_deref!(w as *const pg_sys::CaseWhen);
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let cond = safe_node_to_expr(case_when.expr as *mut pg_sys::Node)?;
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let result = safe_node_to_expr(case_when.result as *mut pg_sys::Node)?;
            sql.push_str(&format!(" WHEN {} THEN {}", cond.to_sql(), result.to_sql()));
        }

        if !case_expr.defresult.is_null() {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let def = safe_node_to_expr(case_expr.defresult as *mut pg_sys::Node)?;
            sql.push_str(&format!(" ELSE {}", def.to_sql()));
        }

        sql.push_str(" END");
        Ok(Expr::Raw(sql))
    } else if let Some(coalesce) = cast_node!(node, T_CoalesceExpr, pg_sys::CoalesceExpr) {
        let args_list = pg_list::<pg_sys::Node>(coalesce.args);
        let mut args = Vec::new();
        for n in args_list.iter_ptr() {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            args.push(safe_node_to_expr(n)?);
        }
        Ok(Expr::FuncCall {
            func_name: "COALESCE".to_string(),
            args,
        })
    } else if let Some(nullif) = cast_node!(node, T_NullIfExpr, pg_sys::NullIfExpr) {
        let args_list = pg_list::<pg_sys::Node>(nullif.args);
        let mut args = Vec::new();
        for n in args_list.iter_ptr() {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            args.push(safe_node_to_expr(n)?);
        }
        Ok(Expr::FuncCall {
            func_name: "NULLIF".to_string(),
            args,
        })
    } else if let Some(mmexpr) = cast_node!(node, T_MinMaxExpr, pg_sys::MinMaxExpr) {
        let func_name = if mmexpr.op == pg_sys::MinMaxOp::IS_GREATEST {
            "GREATEST"
        } else {
            "LEAST"
        };
        let args_list = pg_list::<pg_sys::Node>(mmexpr.args);
        let mut args = Vec::new();
        for n in args_list.iter_ptr() {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            args.push(safe_node_to_expr(n)?);
        }
        Ok(Expr::FuncCall {
            func_name: func_name.to_string(),
            args,
        })
    } else if let Some(svf) = cast_node!(node, T_SQLValueFunction, pg_sys::SQLValueFunction) {
        let kw = sql_value_function_name(svf.op);
        Ok(Expr::Raw(kw.to_string()))
    } else if let Some(bt) = cast_node!(node, T_BooleanTest, pg_sys::BooleanTest) {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let arg = safe_node_to_expr(bt.arg as *mut pg_sys::Node)?;
        let test = match bt.booltesttype {
            pg_sys::BoolTestType::IS_TRUE => "IS TRUE",
            pg_sys::BoolTestType::IS_NOT_TRUE => "IS NOT TRUE",
            pg_sys::BoolTestType::IS_FALSE => "IS FALSE",
            pg_sys::BoolTestType::IS_NOT_FALSE => "IS NOT FALSE",
            pg_sys::BoolTestType::IS_UNKNOWN => "IS UNKNOWN",
            pg_sys::BoolTestType::IS_NOT_UNKNOWN => "IS NOT UNKNOWN",
            _ => "IS TRUE",
        };
        Ok(Expr::Raw(format!("{} {test}", arg.to_sql())))
    } else if let Some(sublink) = cast_node!(node, T_SubLink, pg_sys::SubLink) {
        match sublink.subLinkType {
            pg_sys::SubLinkType::EXPR_SUBLINK => {
                // Scalar subquery — reconstruct as Raw SQL
                // The subselect is a SelectStmt; deparse it back to SQL
                if sublink.subselect.is_null() {
                    return Err(PgTrickleError::QueryParseError(
                        "Scalar subquery has NULL subselect".into(),
                    ));
                }
                let inner_sql = deparse_select_to_sql(sublink.subselect)?;
                Ok(Expr::Raw(format!("({inner_sql})")))
            }
            pg_sys::SubLinkType::EXISTS_SUBLINK => {
                // EXISTS in an expression context (e.g., inside CASE WHEN)
                // Reconstruct as Raw SQL
                if sublink.subselect.is_null() {
                    return Err(PgTrickleError::QueryParseError(
                        "EXISTS subquery has NULL subselect".into(),
                    ));
                }
                let inner_sql = deparse_select_to_sql(sublink.subselect)?;
                Ok(Expr::Raw(format!("EXISTS ({inner_sql})")))
            }
            pg_sys::SubLinkType::ANY_SUBLINK => {
                // IN/ANY in an expression context
                if sublink.subselect.is_null() || sublink.testexpr.is_null() {
                    return Err(PgTrickleError::QueryParseError(
                        "IN/ANY subquery has NULL subselect or testexpr".into(),
                    ));
                }
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let test = safe_node_to_expr(sublink.testexpr)?;
                let inner_sql = deparse_select_to_sql(sublink.subselect)?;
                Ok(Expr::Raw(format!("{} IN ({inner_sql})", test.to_sql())))
            }
            _ => {
                let kind_desc = match sublink.subLinkType {
                    pg_sys::SubLinkType::ALL_SUBLINK => "ALL (subquery)",
                    _ => "subquery expression",
                };
                Err(PgTrickleError::UnsupportedOperator(format!(
                    "{kind_desc} is not supported in defining queries. \
                     Consider rewriting as a JOIN or LATERAL subquery.",
                )))
            }
        }
    } else if let Some(arrexpr) = cast_node!(node, T_ArrayExpr, pg_sys::ArrayExpr) {
        let elems = pg_list::<pg_sys::Node>(arrexpr.elements);
        let mut elem_sql = Vec::new();
        for n in elems.iter_ptr() {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            elem_sql.push(safe_node_to_expr(n)?.to_sql());
        }
        Ok(Expr::Raw(format!("ARRAY[{}]", elem_sql.join(", "))))
    } else if let Some(rowexpr) = cast_node!(node, T_RowExpr, pg_sys::RowExpr) {
        let fields = pg_list::<pg_sys::Node>(rowexpr.args);
        let mut field_sql = Vec::new();
        for n in fields.iter_ptr() {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            field_sql.push(safe_node_to_expr(n)?.to_sql());
        }
        Ok(Expr::Raw(format!("ROW({})", field_sql.join(", "))))
    } else if let Some(indir) = cast_node!(node, T_A_Indirection, pg_sys::A_Indirection) {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let base = safe_node_to_expr(indir.arg)?;
        let mut sql = base.to_sql();
        let indirection_list = pg_list::<pg_sys::Node>(indir.indirection);
        for ind_node in indirection_list.iter_ptr() {
            if let Some(indices) = cast_node!(ind_node, T_A_Indices, pg_sys::A_Indices) {
                if !indices.uidx.is_null() {
                    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                    let idx = safe_node_to_expr(indices.uidx)?;
                    // Parenthesize `sql` so that complex base expressions such as
                    // `array_agg(...) FILTER (WHERE ...)` deparse as
                    // `(array_agg(...) FILTER (WHERE ...))[1]` — the subscript
                    // operator binds tighter than FILTER/aggregate syntax, so
                    // omitting the parens produces invalid SQL.
                    sql = format!("({sql})[{}]", idx.to_sql());
                }
            } else if let Some(s) = cast_node!(ind_node, T_String, pg_sys::String) {
                let field_name = pg_cstr_to_str(s.sval).unwrap_or("");
                sql = format!("({sql}).{field_name}");
            // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
            } else if is_node_type!(ind_node, T_A_Star) {
                sql = format!("({sql}).*");
            }
        }
        Ok(Expr::Raw(sql))
    } else if let Some(cc) = cast_node!(node, T_CollateClause, pg_sys::CollateClause) {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let arg = safe_node_to_expr(cc.arg)?;
        // Extract collation name from the name list
        let coll_list = pg_list::<pg_sys::Node>(cc.collname);
        let mut coll_parts = Vec::new();
        for n in coll_list.iter_ptr() {
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            if let Ok(s) = safe_node_to_string(n) {
                coll_parts.push(s);
            }
        }
        let coll_name = if coll_parts.is_empty() {
            "\"C\"".to_string()
        } else {
            coll_parts
                .iter()
                .map(|p| format!("\"{}\"", p.replace('"', "\"\"")))
                .collect::<Vec<_>>()
                .join(".")
        };
        Ok(Expr::Raw(
            format!("{} COLLATE {}", arg.to_sql(), coll_name,),
        ))
    } else if let Some(jip) = cast_node!(node, T_JsonIsPredicate, pg_sys::JsonIsPredicate) {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let arg = safe_node_to_expr(jip.expr)?;
        let type_str = match jip.item_type {
            pg_sys::JsonValueType::JS_TYPE_OBJECT => "JSON OBJECT",
            pg_sys::JsonValueType::JS_TYPE_ARRAY => "JSON ARRAY",
            pg_sys::JsonValueType::JS_TYPE_SCALAR => "JSON SCALAR",
            _ => "JSON", // JS_TYPE_ANY
        };
        let unique = if jip.unique_keys {
            " WITH UNIQUE KEYS"
        } else {
            ""
        };
        Ok(Expr::Raw(format!("{} IS {type_str}{unique}", arg.to_sql())))
    } else if let Some(joc) =
        cast_node!(node, T_JsonObjectConstructor, pg_sys::JsonObjectConstructor)
    {
        let exprs = pg_list::<pg_sys::Node>(joc.exprs);
        let mut pairs = Vec::new();
        for n in exprs.iter_ptr() {
            if let Some(kv) = cast_node!(n, T_JsonKeyValue, pg_sys::JsonKeyValue) {
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let key = safe_node_to_expr(kv.key as *mut pg_sys::Node)?;
                let val = if !kv.value.is_null() {
                    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
                    let jve = pg_deref!(kv.value);
                    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                    safe_node_to_expr(jve.raw_expr as *mut pg_sys::Node)?
                } else {
                    Expr::Raw("NULL".to_string())
                };
                pairs.push(format!("{} : {}", key.to_sql(), val.to_sql()));
            }
        }
        let mut sql = format!("JSON_OBJECT({})", pairs.join(", "));
        if joc.absent_on_null {
            sql.push_str(" ABSENT ON NULL");
        }
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        unsafe { append_json_output(&mut sql, joc.output) };
        Ok(Expr::Raw(sql))
    } else if let Some(jac) = cast_node!(node, T_JsonArrayConstructor, pg_sys::JsonArrayConstructor)
    {
        let exprs = pg_list::<pg_sys::Node>(jac.exprs);
        let mut elems = Vec::new();
        for n in exprs.iter_ptr() {
            // Elements may be JsonValueExpr or plain exprs
            if let Some(jve) = cast_node!(n, T_JsonValueExpr, pg_sys::JsonValueExpr) {
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                elems.push(safe_node_to_expr(jve.raw_expr as *mut pg_sys::Node)?.to_sql());
            } else {
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                elems.push(safe_node_to_expr(n)?.to_sql());
            }
        }
        let mut sql = format!("JSON_ARRAY({})", elems.join(", "));
        if jac.absent_on_null {
            sql.push_str(" ABSENT ON NULL");
        }
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        unsafe { append_json_output(&mut sql, jac.output) };
        Ok(Expr::Raw(sql))
    } else if let Some(jaqc) = cast_node!(
        node,
        T_JsonArrayQueryConstructor,
        pg_sys::JsonArrayQueryConstructor
    ) {
        let inner_sql = if !jaqc.query.is_null() {
            deparse_select_to_sql(jaqc.query)?
        } else {
            "SELECT NULL".to_string()
        };
        let mut sql = format!("JSON_ARRAY({inner_sql})");
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        unsafe { append_json_output(&mut sql, jaqc.output) };
        Ok(Expr::Raw(sql))
    } else if let Some(jpe) = cast_node!(node, T_JsonParseExpr, pg_sys::JsonParseExpr) {
        let arg = if !jpe.expr.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let jve = pg_deref!(jpe.expr);
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            safe_node_to_expr(jve.raw_expr as *mut pg_sys::Node)?
        } else {
            Expr::Raw("NULL".to_string())
        };
        let mut sql = format!("JSON({})", arg.to_sql());
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        unsafe { append_json_output(&mut sql, jpe.output) };
        Ok(Expr::Raw(sql))
    } else if let Some(jse) = cast_node!(node, T_JsonScalarExpr, pg_sys::JsonScalarExpr) {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let arg = safe_node_to_expr(jse.expr as *mut pg_sys::Node)?;
        let mut sql = format!("JSON_SCALAR({})", arg.to_sql());
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        unsafe { append_json_output(&mut sql, jse.output) };
        Ok(Expr::Raw(sql))
    } else if let Some(jse) = cast_node!(node, T_JsonSerializeExpr, pg_sys::JsonSerializeExpr) {
        let arg = if !jse.expr.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let jve = pg_deref!(jse.expr);
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            safe_node_to_expr(jve.raw_expr as *mut pg_sys::Node)?
        } else {
            Expr::Raw("NULL".to_string())
        };
        let mut sql = format!("JSON_SERIALIZE({})", arg.to_sql());
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        unsafe { append_json_output(&mut sql, jse.output) };
        Ok(Expr::Raw(sql))
    } else if let Some(joa) = cast_node!(node, T_JsonObjectAgg, pg_sys::JsonObjectAgg) {
        let mut parts = Vec::new();
        if !joa.arg.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let kv = pg_deref!(joa.arg);
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let key = safe_node_to_expr(kv.key as *mut pg_sys::Node)?;
            let val = if !kv.value.is_null() {
                // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
                let jve = pg_deref!(kv.value);
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                safe_node_to_expr(jve.raw_expr as *mut pg_sys::Node)?
            } else {
                Expr::Raw("NULL".to_string())
            };
            parts.push(format!("{} : {}", key.to_sql(), val.to_sql()));
        }
        let mut inner = parts.join(", ");
        if joa.absent_on_null {
            inner.push_str(" ABSENT ON NULL");
        }
        if joa.unique {
            inner.push_str(" WITH UNIQUE KEYS");
        }
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        unsafe { append_json_agg_clauses(&mut inner, joa.constructor) };
        let sql = format!("JSON_OBJECTAGG({inner})");
        Ok(Expr::Raw(sql))
    } else if let Some(jaa) = cast_node!(node, T_JsonArrayAgg, pg_sys::JsonArrayAgg) {
        let arg = if !jaa.arg.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let jve = pg_deref!(jaa.arg);
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            safe_node_to_expr(jve.raw_expr as *mut pg_sys::Node)?
        } else {
            Expr::Raw("NULL".to_string())
        };
        let mut inner = arg.to_sql();
        if jaa.absent_on_null {
            inner.push_str(" ABSENT ON NULL");
        }
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        unsafe { append_json_agg_clauses(&mut inner, jaa.constructor) };
        let sql = format!("JSON_ARRAYAGG({inner})");
        Ok(Expr::Raw(sql))
    } else {
        // Catch-all: reject with clear error instead of silently producing broken SQL
        let tag = pg_deref!(node).type_;
        Err(PgTrickleError::UnsupportedOperator(format!(
            "Expression type {tag:?} is not supported in defining queries",
        )))
    }
}

/// Append a `RETURNING <type>` clause from a `JsonOutput` node to a SQL string.
///
/// # Safety
/// `output` must be null or point to a valid `JsonOutput` node.
unsafe fn append_json_output(sql: &mut String, output: *const pg_sys::JsonOutput) {
    if output.is_null() {
        return;
    }
    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let out = pg_deref!(output);
    if !out.typeName.is_null() {
        // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
        let type_name = unsafe { deparse_typename(out.typeName) };
        if !type_name.is_empty() && type_name != "json" && type_name != "jsonb" {
            sql.push_str(&format!(" RETURNING {type_name}"));
        }
    }
}

/// Append ORDER BY and FILTER clauses from a `JsonAggConstructor` to a SQL string.
///
/// # Safety
/// `constructor` must be null or point to a valid `JsonAggConstructor`.
unsafe fn append_json_agg_clauses(
    sql: &mut String,
    constructor: *const pg_sys::JsonAggConstructor,
) {
    if constructor.is_null() {
        return;
    }
    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let ctor = pg_deref!(constructor);

    // ORDER BY
    let order_list = pg_list::<pg_sys::Node>(ctor.agg_order);
    if !order_list.is_empty() {
        let mut order_parts = Vec::new();
        for n in order_list.iter_ptr() {
            if let Some(sb) = cast_node!(n, T_SortBy, pg_sys::SortBy)
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                && let Ok(expr) = safe_node_to_expr(sb.node)
            {
                let dir = match sb.sortby_dir {
                    pg_sys::SortByDir::SORTBY_DESC => " DESC",
                    pg_sys::SortByDir::SORTBY_ASC => " ASC",
                    _ => "",
                };
                let nulls = match sb.sortby_nulls {
                    pg_sys::SortByNulls::SORTBY_NULLS_FIRST => " NULLS FIRST",
                    pg_sys::SortByNulls::SORTBY_NULLS_LAST => " NULLS LAST",
                    _ => "",
                };
                order_parts.push(format!("{}{dir}{nulls}", expr.to_sql()));
            }
        }
        if !order_parts.is_empty() {
            sql.push_str(&format!(" ORDER BY {}", order_parts.join(", ")));
        }
    }

    // RETURNING
    // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
    unsafe { append_json_output(sql, ctor.output) };
}

/// Deparse a `T_JsonTable` FROM item to SQL text.
///
/// Produces: `JSON_TABLE(expr, 'path' COLUMNS (col_defs))`.
/// Does not include the alias — callers append `AS alias` as needed.
///
/// # Safety
/// `jt` must point to a valid `pg_sys::JsonTable` node.
unsafe fn deparse_json_table(jt: *const pg_sys::JsonTable) -> Result<String, PgTrickleError> {
    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let jt_ref = pg_deref!(jt);

    // Context item (the input expression)
    let context_sql = if !jt_ref.context_item.is_null() {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let jve = pg_deref!(jt_ref.context_item);
        if !jve.raw_expr.is_null() {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let expr = safe_node_to_expr(jve.raw_expr as *mut pg_sys::Node)?;
            expr.to_sql()
        } else {
            "NULL".to_string()
        }
    } else {
        "NULL".to_string()
    };

    // Path specification
    let path_sql = if !jt_ref.pathspec.is_null() {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let ps = pg_deref!(jt_ref.pathspec);
        if !ps.string.is_null() {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let expr = safe_node_to_expr(ps.string)?;
            expr.to_sql()
        } else {
            "'$'".to_string()
        }
    } else {
        "'$'".to_string()
    };

    // PASSING clause
    // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
    let passing_sql = unsafe { deparse_json_table_passing(jt_ref.passing)? };

    // COLUMNS clause
    // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
    let columns_sql = unsafe { deparse_json_table_columns(jt_ref.columns)? };

    // ON ERROR behavior
    // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
    let on_error_sql = unsafe { deparse_json_behavior(jt_ref.on_error, "ON ERROR") };

    let mut sql = format!("JSON_TABLE({context_sql}, {path_sql}");
    if !passing_sql.is_empty() {
        sql.push_str(&format!(" PASSING {passing_sql}"));
    }
    sql.push_str(&format!(" COLUMNS ({columns_sql})"));
    if !on_error_sql.is_empty() {
        sql.push_str(&format!(" {on_error_sql}"));
    }
    sql.push(')');

    Ok(sql)
}

/// Deparse the PASSING clause of JSON_TABLE.
///
/// # Safety
/// `passing` must be null or a valid pg_sys::List.
unsafe fn deparse_json_table_passing(passing: *mut pg_sys::List) -> Result<String, PgTrickleError> {
    if passing.is_null() {
        return Ok(String::new());
    }
    let list = pg_list::<pg_sys::Node>(passing);
    if list.is_empty() {
        return Ok(String::new());
    }
    // PASSING items are JsonArgument nodes: (val, name)
    // In the raw parse tree they appear as ResTarget-like structures.
    // Deparse each as "expr AS name".
    let mut parts = Vec::new();
    for node_ptr in list.iter_ptr() {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = safe_node_to_expr(node_ptr)?;
        parts.push(expr.to_sql());
    }
    Ok(parts.join(", "))
}

/// Deparse the COLUMNS clause of JSON_TABLE.
///
/// # Safety
/// `columns` must be null or a valid pg_sys::List of JsonTableColumn nodes.
unsafe fn deparse_json_table_columns(columns: *mut pg_sys::List) -> Result<String, PgTrickleError> {
    if columns.is_null() {
        return Ok(String::new());
    }
    let list = pg_list::<pg_sys::Node>(columns);
    let mut parts = Vec::new();
    for node_ptr in list.iter_ptr() {
        // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
        let col_sql = unsafe { deparse_json_table_column(node_ptr)? };
        parts.push(col_sql);
    }
    Ok(parts.join(", "))
}

/// Deparse a single JSON_TABLE column definition.
///
/// # Safety
/// `node` must point to a valid `pg_sys::JsonTableColumn`.
unsafe fn deparse_json_table_column(node: *mut pg_sys::Node) -> Result<String, PgTrickleError> {
    // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
    if !is_node_type!(node, T_JsonTableColumn) {
        return Err(PgTrickleError::QueryParseError(
            "Expected JsonTableColumn node".into(),
        ));
    }
    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let col = pg_deref!(node as *const pg_sys::JsonTableColumn);

    let name = if !col.name.is_null() {
        pg_cstr_to_str(col.name).unwrap_or("col").to_string()
    } else {
        "col".to_string()
    };

    match col.coltype {
        pg_sys::JsonTableColumnType::JTC_FOR_ORDINALITY => Ok(format!("{name} FOR ORDINALITY")),
        pg_sys::JsonTableColumnType::JTC_REGULAR => {
            // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
            let type_name = unsafe { deparse_typename(col.typeName) };
            let mut s = format!("{name} {type_name}");

            // FORMAT JSON
            if !col.format.is_null() {
                // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
                let fmt = pg_deref!(col.format);
                if fmt.format_type == pg_sys::JsonFormatType::JS_FORMAT_JSON {
                    s.push_str(" FORMAT JSON");
                }
            }

            // PATH 'spec'
            if !col.pathspec.is_null() {
                // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
                let ps = pg_deref!(col.pathspec);
                if !ps.string.is_null() {
                    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                    let path_expr = safe_node_to_expr(ps.string)?;
                    s.push_str(&format!(" PATH {}", path_expr.to_sql()));
                }
            }

            // WRAPPER
            match col.wrapper {
                pg_sys::JsonWrapper::JSW_CONDITIONAL => {
                    s.push_str(" WITH CONDITIONAL WRAPPER");
                }
                pg_sys::JsonWrapper::JSW_UNCONDITIONAL => {
                    s.push_str(" WITH UNCONDITIONAL WRAPPER");
                }
                _ => {}
            }

            // QUOTES
            if col.quotes == pg_sys::JsonQuotes::JS_QUOTES_OMIT {
                s.push_str(" OMIT QUOTES");
            }

            // ON EMPTY / ON ERROR
            // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
            let empty = unsafe { deparse_json_behavior(col.on_empty, "ON EMPTY") };
            if !empty.is_empty() {
                s.push_str(&format!(" {empty}"));
            }
            // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
            let error = unsafe { deparse_json_behavior(col.on_error, "ON ERROR") };
            if !error.is_empty() {
                s.push_str(&format!(" {error}"));
            }

            Ok(s)
        }
        pg_sys::JsonTableColumnType::JTC_EXISTS => {
            // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
            let type_name = unsafe { deparse_typename(col.typeName) };
            let mut s = format!("{name} {type_name} EXISTS");

            if !col.pathspec.is_null() {
                // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
                let ps = pg_deref!(col.pathspec);
                if !ps.string.is_null() {
                    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                    let path_expr = safe_node_to_expr(ps.string)?;
                    s.push_str(&format!(" PATH {}", path_expr.to_sql()));
                }
            }

            // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
            let error = unsafe { deparse_json_behavior(col.on_error, "ON ERROR") };
            if !error.is_empty() {
                s.push_str(&format!(" {error}"));
            }

            Ok(s)
        }
        pg_sys::JsonTableColumnType::JTC_FORMATTED => {
            // Formatted column: like regular but with FORMAT JSON
            // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
            let type_name = unsafe { deparse_typename(col.typeName) };
            let mut s = format!("{name} {type_name} FORMAT JSON");

            if !col.pathspec.is_null() {
                // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
                let ps = pg_deref!(col.pathspec);
                if !ps.string.is_null() {
                    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                    let path_expr = safe_node_to_expr(ps.string)?;
                    s.push_str(&format!(" PATH {}", path_expr.to_sql()));
                }
            }

            // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
            let empty = unsafe { deparse_json_behavior(col.on_empty, "ON EMPTY") };
            if !empty.is_empty() {
                s.push_str(&format!(" {empty}"));
            }
            // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
            let error = unsafe { deparse_json_behavior(col.on_error, "ON ERROR") };
            if !error.is_empty() {
                s.push_str(&format!(" {error}"));
            }

            Ok(s)
        }
        pg_sys::JsonTableColumnType::JTC_NESTED => {
            // NESTED PATH 'path' COLUMNS (...)
            let mut s = String::from("NESTED");
            if !col.pathspec.is_null() {
                // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
                let ps = pg_deref!(col.pathspec);
                if !ps.string.is_null() {
                    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                    let path_expr = safe_node_to_expr(ps.string)?;
                    s.push_str(&format!(" PATH {}", path_expr.to_sql()));
                }
            }
            // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
            let nested_cols = unsafe { deparse_json_table_columns(col.columns)? };
            s.push_str(&format!(" COLUMNS ({nested_cols})"));
            Ok(s)
        }
        _ => Err(PgTrickleError::QueryParseError(format!(
            "Unknown JSON_TABLE column type: {}",
            col.coltype,
        ))),
    }
}

/// Deparse a JSON behavior clause (ON EMPTY / ON ERROR).
///
/// Returns empty string if behavior is null or default.
///
/// # Safety
/// `behavior` must be null or point to a valid `pg_sys::JsonBehavior`.
unsafe fn deparse_json_behavior(behavior: *const pg_sys::JsonBehavior, suffix: &str) -> String {
    if behavior.is_null() {
        return String::new();
    }
    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let beh = pg_deref!(behavior);
    match beh.btype {
        pg_sys::JsonBehaviorType::JSON_BEHAVIOR_NULL => format!("NULL {suffix}"),
        pg_sys::JsonBehaviorType::JSON_BEHAVIOR_ERROR => format!("ERROR {suffix}"),
        pg_sys::JsonBehaviorType::JSON_BEHAVIOR_EMPTY => format!("EMPTY {suffix}"),
        pg_sys::JsonBehaviorType::JSON_BEHAVIOR_EMPTY_ARRAY => {
            format!("EMPTY ARRAY {suffix}")
        }
        pg_sys::JsonBehaviorType::JSON_BEHAVIOR_EMPTY_OBJECT => {
            format!("EMPTY OBJECT {suffix}")
        }
        pg_sys::JsonBehaviorType::JSON_BEHAVIOR_DEFAULT => {
            if !beh.expr.is_null() {
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                if let Ok(expr) = safe_node_to_expr(beh.expr) {
                    format!("DEFAULT {} {suffix}", expr.to_sql())
                } else {
                    String::new()
                }
            } else {
                String::new()
            }
        }
        pg_sys::JsonBehaviorType::JSON_BEHAVIOR_TRUE => format!("TRUE {suffix}"),
        pg_sys::JsonBehaviorType::JSON_BEHAVIOR_FALSE => format!("FALSE {suffix}"),
        pg_sys::JsonBehaviorType::JSON_BEHAVIOR_UNKNOWN => format!("UNKNOWN {suffix}"),
        _ => String::new(),
    }
}

/// Extract operator name from an A_Expr name list.
pub(crate) unsafe fn extract_operator_name(
    name_list: *mut pg_sys::List,
) -> Result<String, PgTrickleError> {
    let list = pg_list::<pg_sys::Node>(name_list);
    if let Some(node) = list.head() {
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        safe_node_to_string(node)
    } else {
        Ok("=".to_string())
    }
}

/// Extract function name from funcname list (possibly schema-qualified).
pub(crate) unsafe fn extract_func_name(
    name_list: *mut pg_sys::List,
) -> Result<String, PgTrickleError> {
    let list = pg_list::<pg_sys::Node>(name_list);
    let mut parts = Vec::new();
    for n in list.iter_ptr() {
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        if let Ok(s) = safe_node_to_string(n) {
            parts.push(s);
        }
    }
    Ok(parts.join("."))
}

/// Deparse a `FuncCall` node back to SQL text.
///
/// Reconstructs `func_name(arg1, arg2, ...)` from the parse tree.
/// This is used for set-returning functions in the FROM clause
/// where we need the complete function call as SQL text.
///
/// # Safety
/// Caller must ensure `fcall` points to a valid `pg_sys::FuncCall` node.
pub(crate) unsafe fn deparse_func_call(
    fcall: *const pg_sys::FuncCall,
) -> Result<String, PgTrickleError> {
    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let fcall_ref = pg_deref!(fcall);
    // SAFETY: Parse-tree node pointer from raw_parser; valid within current memory context.
    let func_name = unsafe { extract_func_name(fcall_ref.funcname)? };

    let args_list = pg_list::<pg_sys::Node>(fcall_ref.args);
    let mut arg_sqls = Vec::new();
    for n in args_list.iter_ptr() {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = safe_node_to_expr(n)?;
        arg_sqls.push(expr.to_sql());
    }

    Ok(format!("{}({})", func_name, arg_sqls.join(", ")))
}

// ═══════════════════════════════════════════════════════════════════════
// LATERAL subquery deparse infrastructure
// ═══════════════════════════════════════════════════════════════════════

/// Deparse a `SelectStmt` back to SQL text.
///
/// Used for LATERAL subquery bodies. Handles the common SQL constructs
/// that appear inside LATERAL subqueries: SELECT, FROM, WHERE, GROUP BY,
/// HAVING, ORDER BY, LIMIT, OFFSET, DISTINCT.
///
/// # Safety
/// Caller must ensure `stmt` points to a valid `pg_sys::SelectStmt`.
pub(crate) unsafe fn deparse_select_stmt_to_sql(
    stmt: *const pg_sys::SelectStmt,
) -> Result<String, PgTrickleError> {
    // SAFETY: caller guarantees stmt is valid.
    let s = pg_deref!(stmt);
    let mut parts = Vec::new();

    // DISTINCT
    let distinct_clause = pg_list::<pg_sys::Node>(s.distinctClause);
    let distinct_prefix = if !distinct_clause.is_empty() {
        "SELECT DISTINCT"
    } else {
        "SELECT"
    };

    // SELECT clause (target list)
    // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
    let targets = unsafe { deparse_target_list(s.targetList)? };
    parts.push(format!("{distinct_prefix} {targets}"));

    // FROM clause
    // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
    let from = unsafe { deparse_from_clause(s.fromClause)? };
    if !from.is_empty() {
        parts.push(format!("FROM {from}"));
    }

    // WHERE clause
    if !s.whereClause.is_null() {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = safe_node_to_expr(s.whereClause)?;
        parts.push(format!("WHERE {}", expr.to_sql()));
    }

    // GROUP BY clause
    let group_list = pg_list::<pg_sys::Node>(s.groupClause);
    if !group_list.is_empty() {
        let mut groups = Vec::new();
        for node_ptr in group_list.iter_ptr() {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let expr = safe_node_to_expr(node_ptr)?;
            groups.push(expr.to_sql());
        }
        parts.push(format!("GROUP BY {}", groups.join(", ")));
    }

    // HAVING clause
    if !s.havingClause.is_null() {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = safe_node_to_expr(s.havingClause)?;
        parts.push(format!("HAVING {}", expr.to_sql()));
    }

    // ORDER BY clause (SortBy nodes)
    let sort_list = pg_list::<pg_sys::Node>(s.sortClause);
    if !sort_list.is_empty() {
        // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
        let sorts = unsafe { deparse_sort_clause(&sort_list)? };
        parts.push(format!("ORDER BY {sorts}"));
    }

    // LIMIT
    if !s.limitCount.is_null() {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = safe_node_to_expr(s.limitCount)?;
        parts.push(format!("LIMIT {}", expr.to_sql()));
    }

    // OFFSET
    if !s.limitOffset.is_null() {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = safe_node_to_expr(s.limitOffset)?;
        parts.push(format!("OFFSET {}", expr.to_sql()));
    }

    Ok(parts.join(" "))
}

/// Deparse a target list (ResTarget nodes) into SQL text.
///
/// Produces: `expr1 AS alias1, expr2, expr3 AS alias3, ...`
///
/// # Safety
/// Caller must ensure `target_list` points to a valid `pg_sys::List`.
pub(crate) unsafe fn deparse_target_list(
    target_list: *mut pg_sys::List,
) -> Result<String, PgTrickleError> {
    let targets = pg_list::<pg_sys::Node>(target_list);
    let mut items = Vec::new();
    for node_ptr in targets.iter_ptr() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if !is_node_type!(node_ptr, T_ResTarget) {
            continue;
        }
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let rt = pg_deref!(node_ptr as *const pg_sys::ResTarget);
        if rt.val.is_null() {
            continue;
        }
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = safe_node_to_expr(rt.val)?;
        let expr_sql = expr.to_sql();
        if !rt.name.is_null() {
            let alias = pg_cstr_to_str(rt.name).unwrap_or("");
            items.push(format!("{expr_sql} AS {alias}"));
        } else {
            items.push(expr_sql);
        }
    }
    Ok(items.join(", "))
}

/// Extract column names from a target list.
///
/// For each `ResTarget`:
/// - If the target has an explicit alias (`AS col`), use that.
/// - Otherwise generate a positional name `_col_N` (1-based).
///
/// Used to derive output column names from a constant anchor SELECT.
///
/// # Safety
/// Caller must ensure `target_list` points to a valid `pg_sys::List`.
unsafe fn extract_target_list_column_names(target_list: *mut pg_sys::List) -> Vec<String> {
    let targets = pg_list::<pg_sys::Node>(target_list);
    let mut cols = Vec::new();
    for (i, node_ptr) in targets.iter_ptr().enumerate() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if !is_node_type!(node_ptr, T_ResTarget) {
            cols.push(format!("_col_{}", i + 1));
            continue;
        }
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let rt = pg_deref!(node_ptr as *const pg_sys::ResTarget);
        if !rt.name.is_null() {
            let alias = pg_cstr_to_str(rt.name).unwrap_or("").to_string();
            if alias.is_empty() {
                cols.push(format!("_col_{}", i + 1));
            } else {
                cols.push(alias);
            }
        } else {
            cols.push(format!("_col_{}", i + 1));
        }
    }
    cols
}

/// Deparse a FROM clause (list of FROM items) into SQL text.
///
/// # Safety
/// Caller must ensure `from_list` points to a valid `pg_sys::List`.
unsafe fn deparse_from_clause(from_list: *mut pg_sys::List) -> Result<String, PgTrickleError> {
    let list = pg_list::<pg_sys::Node>(from_list);
    let mut items = Vec::new();
    for node_ptr in list.iter_ptr() {
        // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
        let item = unsafe { deparse_from_item_to_sql(node_ptr)? };
        items.push(item);
    }
    Ok(items.join(", "))
}

/// Deparse a single FROM item (RangeVar, JoinExpr, RangeSubselect, RangeFunction) to SQL.
///
/// # Safety
/// Caller must ensure `node` points to a valid pg_sys::Node.
pub(crate) unsafe fn deparse_from_item_to_sql(
    node: *mut pg_sys::Node,
) -> Result<String, PgTrickleError> {
    if node.is_null() {
        return Err(PgTrickleError::QueryParseError(
            "NULL FROM item node".into(),
        ));
    }

    if let Some(rv) = cast_node!(node, T_RangeVar, pg_sys::RangeVar) {
        let mut name = String::new();
        if !rv.schemaname.is_null() {
            let schema = pg_cstr_to_str(rv.schemaname).unwrap_or("");
            name.push_str(schema);
            name.push('.');
        }
        if !rv.relname.is_null() {
            let rel = pg_cstr_to_str(rv.relname).unwrap_or("");
            name.push_str(rel);
        }
        if !rv.alias.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let a = pg_deref!(rv.alias);
            let alias_name = pg_cstr_to_str(a.aliasname).unwrap_or("");
            if alias_name != name {
                name.push_str(&format!(" {alias_name}"));
            }
        }
        Ok(name)
    } else if let Some(join) = cast_node!(node, T_JoinExpr, pg_sys::JoinExpr) {
        // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
        let left = unsafe { deparse_from_item_to_sql(join.larg)? };
        // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
        let right = unsafe { deparse_from_item_to_sql(join.rarg)? };
        let join_type = match join.jointype {
            pg_sys::JoinType::JOIN_INNER => "JOIN",
            pg_sys::JoinType::JOIN_LEFT => "LEFT JOIN",
            pg_sys::JoinType::JOIN_FULL => "FULL JOIN",
            pg_sys::JoinType::JOIN_RIGHT => "RIGHT JOIN",
            _ => "JOIN",
        };
        let condition = if join.quals.is_null() {
            "TRUE".to_string()
        } else {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let expr = safe_node_to_expr(join.quals)?;
            expr.to_sql()
        };
        Ok(format!("{left} {join_type} {right} ON {condition}"))
    } else if let Some(sub) = cast_node!(node, T_RangeSubselect, pg_sys::RangeSubselect) {
        if sub.subquery.is_null()
            // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
            || !is_node_type!(sub.subquery, T_SelectStmt)
        {
            return Err(PgTrickleError::QueryParseError(
                "RangeSubselect without valid SelectStmt".into(),
            ));
        }
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let sub_stmt = pg_deref!(sub.subquery as *const pg_sys::SelectStmt);
        // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
        let inner_sql = unsafe { deparse_select_stmt_to_sql(sub_stmt)? };
        let mut result = format!("({inner_sql})");
        if !sub.alias.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let a = pg_deref!(sub.alias);
            let alias_name = pg_cstr_to_str(a.aliasname).unwrap_or("");
            result.push_str(&format!(" AS {alias_name}"));
        }
        Ok(result)
    } else if let Some(rf) = cast_node!(node, T_RangeFunction, pg_sys::RangeFunction) {
        let func_list = pg_list::<pg_sys::Node>(rf.functions);
        if func_list.is_empty() {
            return Err(PgTrickleError::QueryParseError(
                "RangeFunction with no functions in deparse".into(),
            ));
        }
        let inner_list_node = func_list.head().ok_or_else(|| {
            PgTrickleError::QueryParseError(
                "RangeFunction func_list head is None in deparse".into(),
            )
        })?;
        let inner_list = pg_list::<pg_sys::Node>(inner_list_node as *mut pg_sys::List);
        if inner_list.is_empty() {
            return Err(PgTrickleError::QueryParseError(
                "RangeFunction inner list empty in deparse".into(),
            ));
        }
        let func_node = inner_list.head().ok_or_else(|| {
            PgTrickleError::QueryParseError(
                "RangeFunction inner_list head is None in deparse".into(),
            )
        })?;
        // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
        let func_sql = unsafe { deparse_func_call(func_node as *const pg_sys::FuncCall)? };
        let mut result = func_sql;
        if !rf.alias.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let a = pg_deref!(rf.alias);
            let alias_name = pg_cstr_to_str(a.aliasname).unwrap_or("");
            result.push_str(&format!(" AS {alias_name}"));
        }
        Ok(result)
    } else if let Some(jt) = cast_node!(node, T_JsonTable, pg_sys::JsonTable) {
        // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
        let mut result = unsafe { deparse_json_table(jt as *const pg_sys::JsonTable)? };
        if !jt.alias.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let a = pg_deref!(jt.alias);
            let alias_name = pg_cstr_to_str(a.aliasname).unwrap_or("");
            result.push_str(&format!(" AS {alias_name}"));
        }
        Ok(result)
    } else {
        // Fallback: deparse as generic expression
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = safe_node_to_expr(node)?;
        Ok(expr.to_sql())
    }
}

/// Deparse a sort clause (list of SortBy nodes) into SQL text.
///
/// # Safety
/// Caller must ensure all nodes in `sort_list` are valid `SortBy` nodes.
pub(crate) unsafe fn deparse_sort_clause(
    sort_list: &pgrx::PgList<pg_sys::Node>,
) -> Result<String, PgTrickleError> {
    let mut items = Vec::new();
    for node_ptr in sort_list.iter_ptr() {
        // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
        let sort_sql = unsafe { deparse_sort_by(node_ptr as *const pg_sys::SortBy)? };
        items.push(sort_sql);
    }
    Ok(items.join(", "))
}

/// Deparse a single SortBy node to SQL (e.g., `created_at DESC`).
///
/// # Safety
/// Caller must ensure `node` points to a valid `pg_sys::SortBy`.
unsafe fn deparse_sort_by(node: *const pg_sys::SortBy) -> Result<String, PgTrickleError> {
    // SAFETY: caller guarantees node is valid.
    let sb = pg_deref!(node);
    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
    let expr = safe_node_to_expr(sb.node)?;
    let dir = match sb.sortby_dir {
        pg_sys::SortByDir::SORTBY_ASC => " ASC",
        pg_sys::SortByDir::SORTBY_DESC => " DESC",
        _ => "",
    };
    let nulls = match sb.sortby_nulls {
        pg_sys::SortByNulls::SORTBY_NULLS_FIRST => " NULLS FIRST",
        pg_sys::SortByNulls::SORTBY_NULLS_LAST => " NULLS LAST",
        _ => "",
    };
    Ok(format!("{}{dir}{nulls}", expr.to_sql()))
}

/// Extract output column names from a SelectStmt's target list.
///
/// For each ResTarget:
/// - If it has an explicit alias (`AS name`), use that
/// - If it's a ColumnRef, use the column name
/// - Otherwise, generate a positional name (`column1`, `column2`, ...)
///
/// # Safety
/// Caller must ensure `target_list` points to a valid `pg_sys::List`.
unsafe fn extract_select_output_cols(
    target_list: *mut pg_sys::List,
) -> Result<Vec<String>, PgTrickleError> {
    let targets = pg_list::<pg_sys::Node>(target_list);
    let mut cols = Vec::new();
    for (i, node_ptr) in targets.iter_ptr().enumerate() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if !is_node_type!(node_ptr, T_ResTarget) {
            cols.push(format!("column{}", i + 1));
            continue;
        }
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let rt = pg_deref!(node_ptr as *const pg_sys::ResTarget);
        if !rt.name.is_null() {
            let name = pg_cstr_to_str(rt.name).unwrap_or("");
            cols.push(name.to_string());
        } else if !rt.val.is_null() {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            if let Ok(expr) = safe_node_to_expr(rt.val) {
                cols.push(expr.output_name());
            } else {
                cols.push(format!("column{}", i + 1));
            }
        } else {
            cols.push(format!("column{}", i + 1));
        }
    }
    Ok(cols)
}

/// Walk a LATERAL body collecting every base table OID it can reference.
///
/// Used to extract source OIDs from a LATERAL subquery body's FROM clause
/// and nested subqueries so that CDC triggers can be set up for every table
/// whose changes can affect the body.
///
/// # Safety
/// Caller must ensure `stmt` points to a valid raw `SelectStmt`.
unsafe fn extract_select_source_oids(
    stmt: &pg_sys::SelectStmt,
) -> Result<Vec<u32>, PgTrickleError> {
    let mut oids = Vec::new();
    unsafe { collect_from_oids(stmt.fromClause, &mut oids)? };

    // The FROM walker cannot see relations inside scalar/EXISTS/IN
    // subqueries embedded in SELECT, WHERE, HAVING, ORDER BY, or LIMIT
    // expressions.  Walk each expression tree as well.
    let mut walk_expr = |node: *mut pg_sys::Node| {
        if !node.is_null() {
            // SAFETY: `node` points into the raw parse tree owned by `stmt`.
            unsafe { collect_expression_source_oids(node, &mut oids) };
        }
    };
    for target_ptr in pg_list::<pg_sys::ResTarget>(stmt.targetList).iter_ptr() {
        if !target_ptr.is_null() {
            // SAFETY: target_ptr is a ResTarget from stmt.targetList.
            walk_expr(unsafe { (*target_ptr).val });
        }
    }
    walk_expr(stmt.whereClause);
    walk_expr(stmt.havingClause);
    walk_expr(stmt.limitOffset);
    walk_expr(stmt.limitCount);

    for sort_ptr in pg_list::<pg_sys::SortBy>(stmt.sortClause).iter_ptr() {
        if !sort_ptr.is_null() {
            // SAFETY: sort_ptr is a SortBy from stmt.sortClause.
            walk_expr(unsafe { (*sort_ptr).node });
        }
    }
    for group_ptr in pg_list::<pg_sys::Node>(stmt.groupClause).iter_ptr() {
        walk_expr(group_ptr);
    }
    for distinct_ptr in pg_list::<pg_sys::Node>(stmt.distinctClause).iter_ptr() {
        walk_expr(distinct_ptr);
    }

    if !stmt.withClause.is_null() {
        // SAFETY: withClause and its list are part of the same raw parse tree.
        let with_clause = unsafe { &*stmt.withClause };
        for cte_ptr in pg_list::<pg_sys::CommonTableExpr>(with_clause.ctes).iter_ptr() {
            if !cte_ptr.is_null() {
                // SAFETY: cte_ptr is a CommonTableExpr from with_clause.ctes.
                let cte_query = unsafe { (*cte_ptr).ctequery };
                if !cte_query.is_null()
                    // SAFETY: raw parser node tag is valid for this pointer.
                    && is_node_type!(cte_query, T_SelectStmt)
                {
                    // SAFETY: cte_query is a valid SelectStmt raw parse node.
                    unsafe {
                        oids.extend(extract_select_source_oids(
                            &*(cte_query as *const pg_sys::SelectStmt),
                        )?);
                    }
                }
            }
        }
    }

    Ok(oids)
}

/// Collect table OIDs from a FROM clause into an existing vector.
///
/// # Safety
/// Caller must ensure `from_list` points to a valid `pg_sys::List`.
unsafe fn collect_from_oids(
    from_list: *mut pg_sys::List,
    oids: &mut Vec<u32>,
) -> Result<(), PgTrickleError> {
    let list = pg_list::<pg_sys::Node>(from_list);
    for node_ptr in list.iter_ptr() {
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        unsafe { collect_from_item_oids(node_ptr, oids)? };
    }
    Ok(())
}

/// Walk a raw expression tree for relations referenced by nested subqueries.
///
/// # Safety
/// Caller must ensure `node` points to a valid raw parse-tree expression.
unsafe fn collect_expression_source_oids(node: *mut pg_sys::Node, oids: &mut Vec<u32>) {
    let mut ctx = RawSourceOidContext {
        oids: oids as *mut Vec<u32>,
    };
    // SAFETY: node is a valid raw parse-tree expression and the callback only
    // reads relation/subquery fields from nodes supplied by PostgreSQL.
    unsafe {
        pg_sys::raw_expression_tree_walker_impl(
            node,
            Some(raw_source_oid_walker),
            &mut ctx as *mut RawSourceOidContext as *mut std::ffi::c_void,
        );
    }
}

struct RawSourceOidContext {
    oids: *mut Vec<u32>,
}

/// Callback for `raw_expression_tree_walker_impl`.
///
/// Nested `SubLink` SELECTs are collected explicitly because the raw walker
/// does not recurse into `SelectStmt` nodes as ordinary expressions.
unsafe extern "C-unwind" fn raw_source_oid_walker(
    node: *mut pg_sys::Node,
    context: *mut std::ffi::c_void,
) -> bool {
    if node.is_null() {
        return false;
    }

    let oids = unsafe { &mut *((*(context as *mut RawSourceOidContext)).oids) };
    if is_node_type!(node, T_SubLink) {
        // SAFETY: node tag verified as SubLink.
        let sublink = unsafe { &*(node as *const pg_sys::SubLink) };
        if !sublink.subselect.is_null()
            // SAFETY: raw parser node tag is valid for this pointer.
            && is_node_type!(sublink.subselect, T_SelectStmt)
        {
            // SAFETY: subselect is a valid raw SelectStmt node.
            match unsafe {
                extract_select_source_oids(&*(sublink.subselect as *const pg_sys::SelectStmt))
            } {
                Ok(inner) => oids.extend(inner),
                Err(_) => oids.push(0),
            }
        }
    }

    false
}

/// Recursively collect table OIDs from a single FROM item.
///
/// # Safety
/// Caller must ensure `node` points to a valid pg_sys::Node.
unsafe fn collect_from_item_oids(
    node: *mut pg_sys::Node,
    oids: &mut Vec<u32>,
) -> Result<(), PgTrickleError> {
    if node.is_null() {
        return Ok(());
    }

    if let Some(rv) = cast_node!(node, T_RangeVar, pg_sys::RangeVar) {
        let table_name = if !rv.relname.is_null() {
            pg_cstr_to_str(rv.relname).unwrap_or("").to_string()
        } else {
            return Ok(());
        };
        let schema = if !rv.schemaname.is_null() {
            pg_cstr_to_str(rv.schemaname)
                .unwrap_or("public")
                .to_string()
        } else {
            "public".to_string()
        };

        // Resolve table OID via SPI
        let sql = format!(
            "SELECT c.oid::int4 FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relname = '{}' AND n.nspname = '{}'",
            table_name.replace('\'', "''"),
            schema.replace('\'', "''"),
        );
        let oid = pgrx::Spi::connect(|client| {
            let result = client
                .select(&sql, None, &[])
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            for row in result {
                if let Ok(Some(id)) = row.get::<i32>(1) {
                    return Ok(id as u32);
                }
            }
            Ok(0u32)
        })?;
        // Keep an explicit unresolved marker.  Treating an unresolvable
        // relation as "no dependency" can make inner changes invisible to
        // the LATERAL delta path; admission rejects the marker.
        oids.push(oid);
    } else if let Some(join) = cast_node!(node, T_JoinExpr, pg_sys::JoinExpr) {
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        unsafe { collect_from_item_oids(join.larg, oids)? };
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        unsafe { collect_from_item_oids(join.rarg, oids)? };
    } else if let Some(sub) = cast_node!(node, T_RangeSubselect, pg_sys::RangeSubselect)
        && !sub.subquery.is_null()
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        && is_node_type!(sub.subquery, T_SelectStmt)
    {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let sub_stmt = pg_deref!(sub.subquery as *const pg_sys::SelectStmt);
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        let inner_oids = unsafe { extract_select_source_oids(sub_stmt)? };
        oids.extend(inner_oids);
    }
    // RangeFunction: no table OIDs to extract (SRFs don't reference tables directly)
    Ok(())
}

/// Extract alias→OID pairs from a LATERAL subquery's FROM clause.
///
/// For each `RangeVar` in the FROM list, resolves the table OID and
/// collects the effective alias (explicit alias or table name).
///
/// # Safety
/// Caller must ensure `from_list` points to a valid `pg_sys::List`.
unsafe fn extract_from_alias_oids(
    from_list: *mut pg_sys::List,
) -> Result<Vec<(String, u32)>, PgTrickleError> {
    let list = pg_list::<pg_sys::Node>(from_list);
    let mut pairs = Vec::new();
    for node_ptr in list.iter_ptr() {
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        unsafe { collect_from_item_alias_oids(node_ptr, &mut pairs)? };
    }
    Ok(pairs)
}

/// Recursively collect (alias, OID) pairs from a FROM item.
///
/// # Safety
/// Caller must ensure `node` points to a valid `pg_sys::Node`.
unsafe fn collect_from_item_alias_oids(
    node: *mut pg_sys::Node,
    pairs: &mut Vec<(String, u32)>,
) -> Result<(), PgTrickleError> {
    if node.is_null() {
        return Ok(());
    }

    if let Some(rv) = cast_node!(node, T_RangeVar, pg_sys::RangeVar) {
        let table_name = if !rv.relname.is_null() {
            pg_cstr_to_str(rv.relname).unwrap_or("").to_string()
        } else {
            return Ok(());
        };

        // Effective alias: explicit alias > table name
        let alias = if !rv.alias.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let a = pg_deref!(rv.alias);
            pg_cstr_to_str(a.aliasname)
                .unwrap_or(&table_name)
                .to_string()
        } else {
            table_name.clone()
        };

        let schema = if !rv.schemaname.is_null() {
            pg_cstr_to_str(rv.schemaname)
                .unwrap_or("public")
                .to_string()
        } else {
            "public".to_string()
        };

        let sql = format!(
            "SELECT c.oid::int4 FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relname = '{}' AND n.nspname = '{}'",
            table_name.replace('\'', "''"),
            schema.replace('\'', "''"),
        );
        let oid = pgrx::Spi::connect(|client| {
            let result = client
                .select(&sql, None, &[])
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            for row in result {
                if let Ok(Some(id)) = row.get::<i32>(1) {
                    return Ok(id as u32);
                }
            }
            Ok(0u32)
        })?;
        pairs.push((alias, oid));
    } else if let Some(join) = cast_node!(node, T_JoinExpr, pg_sys::JoinExpr) {
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        unsafe { collect_from_item_alias_oids(join.larg, pairs)? };
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        unsafe { collect_from_item_alias_oids(join.rarg, pairs)? };
    }
    Ok(())
}

/// Extract correlation predicates from a LATERAL subquery's WHERE clause.
///
/// Walks the WHERE expression looking for equality comparisons of the form
/// `inner_alias.col = outer_ref.col` where `inner_alias` matches a FROM
/// item inside the subquery and the other side is an outer (correlation)
/// reference.
///
/// Only simple equalities (top-level or under AND) are extracted.
/// Complex predicates (OR, functions, casts) are skipped — the caller
/// falls back to full outer-table scanning.
///
/// # Safety
/// Caller must ensure `where_clause` points to a valid expression node.
unsafe fn extract_correlation_predicates(
    where_clause: *mut pg_sys::Node,
    inner_alias_oids: &[(String, u32)],
) -> Vec<CorrelationPredicate> {
    if where_clause.is_null() {
        return Vec::new();
    }
    let inner_aliases: Vec<&str> = inner_alias_oids.iter().map(|(a, _)| a.as_str()).collect();

    // Convert to Expr tree — best-effort, ignore errors.
    // SAFETY: caller guarantees `where_clause` is a valid expression node.
    let expr = match safe_node_to_expr(where_clause) {
        Ok(e) => e,
        Err(_) => return Vec::new(),
    };

    let mut predicates = Vec::new();
    collect_correlation_equalities(&expr, &inner_aliases, inner_alias_oids, &mut predicates);
    predicates
}

/// Recursively collect equality predicates from an expression tree.
pub(crate) fn collect_correlation_equalities(
    expr: &Expr,
    inner_aliases: &[&str],
    inner_alias_oids: &[(String, u32)],
    out: &mut Vec<CorrelationPredicate>,
) {
    match expr {
        Expr::BinaryOp { op, left, right } if op == "=" => {
            // Check if this is inner.col = outer.col or outer.col = inner.col
            if let Some(pred) =
                try_extract_correlation(left, right, inner_aliases, inner_alias_oids)
            {
                out.push(pred);
            } else if let Some(pred) =
                try_extract_correlation(right, left, inner_aliases, inner_alias_oids)
            {
                out.push(pred);
            }
        }
        Expr::BinaryOp { op, left, right } if op == "AND" => {
            collect_correlation_equalities(left, inner_aliases, inner_alias_oids, out);
            collect_correlation_equalities(right, inner_aliases, inner_alias_oids, out);
        }
        _ => {}
    }
}

/// Try to match `(inner_ref, outer_ref)` as a correlation predicate.
///
/// Returns `Some(CorrelationPredicate)` if `inner_ref` is a qualified column
/// reference whose alias matches an inner FROM item and `outer_ref` is a
/// qualified column reference whose alias does NOT match any inner FROM item.
fn try_extract_correlation(
    inner_ref: &Expr,
    outer_ref: &Expr,
    inner_aliases: &[&str],
    inner_alias_oids: &[(String, u32)],
) -> Option<CorrelationPredicate> {
    let (inner_alias, inner_col) = match inner_ref {
        Expr::ColumnRef {
            table_alias: Some(tbl),
            column_name,
        } if inner_aliases.contains(&tbl.as_str()) => (tbl.clone(), column_name.clone()),
        _ => return None,
    };

    let outer_col = match outer_ref {
        Expr::ColumnRef {
            table_alias: Some(tbl),
            column_name,
        } if !inner_aliases.contains(&tbl.as_str()) => column_name.clone(),
        // Unqualified column that's not in inner aliases — assume outer
        Expr::ColumnRef {
            table_alias: None,
            column_name,
        } => column_name.clone(),
        _ => return None,
    };

    // Resolve inner alias → OID
    let inner_oid = inner_alias_oids
        .iter()
        .find(|(a, _)| a == &inner_alias)
        .map(|(_, oid)| *oid)?;

    Some(CorrelationPredicate {
        outer_col,
        inner_alias,
        inner_col,
        inner_oid,
    })
}

/// Convert a String/Value node to a Rust String.
unsafe fn node_to_string(node: *mut pg_sys::Node) -> Result<String, PgTrickleError> {
    if node.is_null() {
        return Err(PgTrickleError::QueryParseError("NULL node".into()));
    }
    if let Some(s) = cast_node!(node, T_String, pg_sys::String) {
        Ok(pg_cstr_to_str(s.sval).unwrap_or("").to_string())
    } else {
        Ok(format!("node_{:?}", pg_deref!(node).type_))
    }
}

/// Safe wrapper for `node_to_string`.
///
/// Precondition: `node` must be a valid parse-tree pointer from `raw_parser()`,
/// live within the current PostgreSQL memory context.
fn safe_node_to_string(node: *mut pg_sys::Node) -> Result<String, PgTrickleError> {
    // SAFETY: parse-tree pointer from raw_parser(); valid for the current
    // memory context. Null is handled inside node_to_string.
    unsafe { node_to_string(node) }
}

/// Deparse a node back to SQL text (simplified fallback).
unsafe fn deparse_node(node: *mut pg_sys::Node) -> String {
    if node.is_null() {
        return "NULL".to_string();
    }
    if let Some(aconst) = cast_node!(node, T_A_Const, pg_sys::A_Const) {
        if aconst.isnull {
            return "NULL".to_string();
        }
        let val_ptr = &aconst.val as *const _ as *const pg_sys::Node;
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if is_node_type!(val_ptr as *mut pg_sys::Node, T_String) {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let s = pg_deref!(val_ptr as *const pg_sys::String);
            return format!("'{}'", pg_cstr_to_str(s.sval).unwrap_or(""));
        }
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if is_node_type!(val_ptr as *mut pg_sys::Node, T_Integer) {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let i = pg_deref!(val_ptr as *const pg_sys::Integer);
            return i.ival.to_string();
        }
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if is_node_type!(val_ptr as *mut pg_sys::Node, T_Float) {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let f = pg_deref!(val_ptr as *const pg_sys::Float);
            return pg_cstr_to_str(f.fval).unwrap_or("0").to_string();
        }
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if is_node_type!(val_ptr as *mut pg_sys::Node, T_Boolean) {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let b = pg_deref!(val_ptr as *const pg_sys::Boolean);
            return if b.boolval {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            };
        }
        "?".to_string()
    // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
    } else if is_node_type!(node, T_ColumnRef) {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        if let Ok(e) = safe_node_to_expr(node) {
            e.to_sql()
        } else {
            "?".to_string()
        }
    } else {
        format!("/* node {:?} */", pg_deref!(node).type_)
    }
}

/// Deparse a TypeName to SQL (simplified).
unsafe fn deparse_typename(tn: *mut pg_sys::TypeName) -> String {
    if tn.is_null() {
        return "unknown".to_string();
    }
    // SAFETY: PgList::from_pg is safe for both null and valid list pointers from the parser.
    let names = unsafe { pgrx::PgList::<pg_sys::Node>::from_pg((*tn).names) };
    let mut parts = Vec::new();
    for n in names.iter_ptr() {
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        if let Ok(s) = safe_node_to_string(n) {
            parts.push(s);
        }
    }
    parts.join(".")
}

/// Check if the target list contains any window function calls (FuncCall with non-null `over`),
/// including window functions nested inside CASE, COALESCE, function args, etc.
pub(crate) fn target_list_has_windows(target_list: &pgrx::PgList<pg_sys::Node>) -> bool {
    for node_ptr in target_list.iter_ptr() {
        if let Some(rt) = cast_node!(node_ptr, T_ResTarget, pg_sys::ResTarget)
            && !rt.val.is_null()
            // SAFETY: Node pointer from parse-tree; allocated by raw_parser.
            && unsafe { node_contains_window_func(rt.val) }
        {
            return true;
        }
    }
    false
}

/// Recursively check if a parse-tree node contains a window function call
/// (`T_FuncCall` with non-null `over` field).
///
/// Walks into CASE, COALESCE, NULLIF, GREATEST/LEAST, BoolExpr, A_Expr,
/// FuncCall args, NullTest, BooleanTest, TypeCast, A_Indirection, RowExpr,
/// and ArrayExpr to detect deeply nested window functions.
///
/// # Safety
/// Caller must ensure `node` points to a valid `pg_sys::Node`.
pub(crate) unsafe fn node_contains_window_func(node: *mut pg_sys::Node) -> bool {
    if node.is_null() {
        return false;
    }

    // Direct hit: FuncCall with OVER clause
    if let Some(fcall) = cast_node!(node, T_FuncCall, pg_sys::FuncCall) {
        if !fcall.over.is_null() {
            return true;
        }
        // Check function arguments recursively
        if !fcall.args.is_null() {
            let args = pg_list::<pg_sys::Node>(fcall.args);
            for arg in args.iter_ptr() {
                // SAFETY: Node pointer from parse-tree; allocated by raw_parser.
                if unsafe { node_contains_window_func(arg) } {
                    return true;
                }
            }
        }
        return false;
    }

    // CASE WHEN ... THEN ... ELSE ... END
    if let Some(case_expr) = cast_node!(node, T_CaseExpr, pg_sys::CaseExpr) {
        // Check the optional test expression (simple CASE)
        if !case_expr.arg.is_null()
            // SAFETY: Node pointer from parse-tree; allocated by raw_parser.
            && unsafe { node_contains_window_func(case_expr.arg as *mut pg_sys::Node) }
        {
            return true;
        }
        // Check each WHEN clause
        if !case_expr.args.is_null() {
            let whens = pg_list::<pg_sys::Node>(case_expr.args);
            for when_ptr in whens.iter_ptr() {
                if let Some(cw) = cast_node!(when_ptr, T_CaseWhen, pg_sys::CaseWhen) {
                    // SAFETY: Node pointer from parse-tree; allocated by raw_parser.
                    if unsafe { node_contains_window_func(cw.expr as *mut pg_sys::Node) } {
                        return true;
                    }
                    // SAFETY: Node pointer from parse-tree; allocated by raw_parser.
                    if unsafe { node_contains_window_func(cw.result as *mut pg_sys::Node) } {
                        return true;
                    }
                }
            }
        }
        // Check ELSE
        if !case_expr.defresult.is_null()
            // SAFETY: Node pointer from parse-tree; allocated by raw_parser.
            && unsafe { node_contains_window_func(case_expr.defresult as *mut pg_sys::Node) }
        {
            return true;
        }
        return false;
    }

    // COALESCE(a, b, ...)
    if let Some(coalesce) = cast_node!(node, T_CoalesceExpr, pg_sys::CoalesceExpr) {
        if !coalesce.args.is_null() {
            let args = pg_list::<pg_sys::Node>(coalesce.args);
            for arg in args.iter_ptr() {
                // SAFETY: Node pointer from parse-tree; allocated by raw_parser.
                if unsafe { node_contains_window_func(arg) } {
                    return true;
                }
            }
        }
        return false;
    }

    // NULLIF(a, b)
    if let Some(nullif) = cast_node!(node, T_NullIfExpr, pg_sys::NullIfExpr) {
        if !nullif.args.is_null() {
            let args = pg_list::<pg_sys::Node>(nullif.args);
            for arg in args.iter_ptr() {
                // SAFETY: Node pointer from parse-tree; allocated by raw_parser.
                if unsafe { node_contains_window_func(arg) } {
                    return true;
                }
            }
        }
        return false;
    }

    // GREATEST / LEAST
    if let Some(minmax) = cast_node!(node, T_MinMaxExpr, pg_sys::MinMaxExpr) {
        if !minmax.args.is_null() {
            let args = pg_list::<pg_sys::Node>(minmax.args);
            for arg in args.iter_ptr() {
                // SAFETY: Node pointer from parse-tree; allocated by raw_parser.
                if unsafe { node_contains_window_func(arg) } {
                    return true;
                }
            }
        }
        return false;
    }

    // AND / OR / NOT
    if let Some(bexpr) = cast_node!(node, T_BoolExpr, pg_sys::BoolExpr) {
        if !bexpr.args.is_null() {
            let args = pg_list::<pg_sys::Node>(bexpr.args);
            for arg in args.iter_ptr() {
                // SAFETY: Node pointer from parse-tree; allocated by raw_parser.
                if unsafe { node_contains_window_func(arg) } {
                    return true;
                }
            }
        }
        return false;
    }

    // Binary/unary ops: a + b, -a, a BETWEEN x AND y, etc.
    if let Some(aexpr) = cast_node!(node, T_A_Expr, pg_sys::A_Expr) {
        // SAFETY: Node pointer from parse-tree; allocated by raw_parser.
        if !aexpr.lexpr.is_null() && unsafe { node_contains_window_func(aexpr.lexpr) } {
            return true;
        }
        // SAFETY: Node pointer from parse-tree; allocated by raw_parser.
        if !aexpr.rexpr.is_null() && unsafe { node_contains_window_func(aexpr.rexpr) } {
            return true;
        }
        return false;
    }

    // CAST(x AS type)
    if let Some(tc) = cast_node!(node, T_TypeCast, pg_sys::TypeCast) {
        if !tc.arg.is_null() {
            // SAFETY: Node pointer from parse-tree; allocated by raw_parser.
            return unsafe { node_contains_window_func(tc.arg) };
        }
        return false;
    }

    // IS [NOT] NULL
    if let Some(nt) = cast_node!(node, T_NullTest, pg_sys::NullTest) {
        if !nt.arg.is_null() {
            // SAFETY: Node pointer from parse-tree; allocated by raw_parser.
            return unsafe { node_contains_window_func(nt.arg as *mut pg_sys::Node) };
        }
        return false;
    }

    // IS [NOT] TRUE / FALSE / UNKNOWN
    if let Some(bt) = cast_node!(node, T_BooleanTest, pg_sys::BooleanTest) {
        if !bt.arg.is_null() {
            // SAFETY: Node pointer from parse-tree; allocated by raw_parser.
            return unsafe { node_contains_window_func(bt.arg as *mut pg_sys::Node) };
        }
        return false;
    }

    // ARRAY[a, b, c]
    if let Some(arr) = cast_node!(node, T_ArrayExpr, pg_sys::ArrayExpr) {
        if !arr.elements.is_null() {
            let elems = pg_list::<pg_sys::Node>(arr.elements);
            for elem in elems.iter_ptr() {
                // SAFETY: Node pointer from parse-tree; allocated by raw_parser.
                if unsafe { node_contains_window_func(elem) } {
                    return true;
                }
            }
        }
        return false;
    }

    // ROW(a, b, c)
    if let Some(row) = cast_node!(node, T_RowExpr, pg_sys::RowExpr) {
        if !row.args.is_null() {
            let args = pg_list::<pg_sys::Node>(row.args);
            for arg in args.iter_ptr() {
                // SAFETY: Node pointer from parse-tree; allocated by raw_parser.
                if unsafe { node_contains_window_func(arg) } {
                    return true;
                }
            }
        }
        return false;
    }

    // SubLink — check testexpr and subselect target list
    if let Some(sub) = cast_node!(node, T_SubLink, pg_sys::SubLink) {
        // SAFETY: Node pointer from parse-tree; allocated by raw_parser.
        if !sub.testexpr.is_null() && unsafe { node_contains_window_func(sub.testexpr) } {
            return true;
        }
        return false;
    }

    // Leaf nodes: ColumnRef, A_Const, SQLValueFunction, etc — no children
    false
}

/// Recursively collect all FuncCall-with-OVER nodes inside an expression tree.
///
/// When a FuncCall-with-OVER is found it is pushed to `result` and recursion
/// stops (window functions nested inside window spec arguments are invalid SQL).
///
/// # Safety
/// Caller must ensure `node` points to a valid parse-tree Node.
pub(crate) unsafe fn collect_all_window_func_nodes(
    node: *mut pg_sys::Node,
    result: &mut Vec<*mut pg_sys::Node>,
) {
    if node.is_null() {
        return;
    }

    // FuncCall with OVER → window function: collect and stop descent
    if let Some(func) = cast_node!(node, T_FuncCall, pg_sys::FuncCall) {
        if !func.over.is_null() {
            result.push(node);
            return;
        }
        // Regular function call — recurse into args
        if !func.args.is_null() {
            let args = pg_list::<pg_sys::Node>(func.args);
            for arg in args.iter_ptr() {
                // SAFETY: Node pointer from parse-tree target list; allocated by raw_parser.
                unsafe { collect_all_window_func_nodes(arg, result) };
            }
        }
        return;
    }

    // A_Expr: binary/unary/subscript operators
    if let Some(expr) = cast_node!(node, T_A_Expr, pg_sys::A_Expr) {
        if !expr.lexpr.is_null() {
            // SAFETY: Node pointer from parse-tree target list; allocated by raw_parser.
            unsafe { collect_all_window_func_nodes(expr.lexpr, result) };
        }
        if !expr.rexpr.is_null() {
            // SAFETY: Node pointer from parse-tree target list; allocated by raw_parser.
            unsafe { collect_all_window_func_nodes(expr.rexpr, result) };
        }
        return;
    }

    // TypeCast: (expr)::type
    if let Some(tc) = cast_node!(node, T_TypeCast, pg_sys::TypeCast) {
        if !tc.arg.is_null() {
            // SAFETY: Node pointer from parse-tree target list; allocated by raw_parser.
            unsafe { collect_all_window_func_nodes(tc.arg, result) };
        }
        return;
    }

    // CaseExpr: CASE [arg] WHEN ... THEN ... ELSE ... END
    if let Some(case) = cast_node!(node, T_CaseExpr, pg_sys::CaseExpr) {
        if !case.arg.is_null() {
            // SAFETY: Node pointer from parse-tree target list; allocated by raw_parser.
            unsafe { collect_all_window_func_nodes(case.arg as *mut pg_sys::Node, result) };
        }
        if !case.args.is_null() {
            let when_list = pg_list::<pg_sys::Node>(case.args);
            for w in when_list.iter_ptr() {
                // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
                if !w.is_null() && is_node_type!(w, T_CaseWhen) {
                    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
                    let cw = pg_deref!(w as *const pg_sys::CaseWhen);
                    if !cw.expr.is_null() {
                        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
                        unsafe {
                            collect_all_window_func_nodes(cw.expr as *mut pg_sys::Node, result)
                        };
                    }
                    if !cw.result.is_null() {
                        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
                        unsafe {
                            collect_all_window_func_nodes(cw.result as *mut pg_sys::Node, result)
                        };
                    }
                }
            }
        }
        if !case.defresult.is_null() {
            // SAFETY: Node pointer from parse-tree target list; allocated by raw_parser.
            unsafe { collect_all_window_func_nodes(case.defresult as *mut pg_sys::Node, result) };
        }
        return;
    }

    // CoalesceExpr: COALESCE(a, b, ...)
    if let Some(coalesce) = cast_node!(node, T_CoalesceExpr, pg_sys::CoalesceExpr) {
        if !coalesce.args.is_null() {
            let args = pg_list::<pg_sys::Node>(coalesce.args);
            for arg in args.iter_ptr() {
                // SAFETY: Node pointer from parse-tree target list; allocated by raw_parser.
                unsafe { collect_all_window_func_nodes(arg, result) };
            }
        }
        return;
    }

    // NullIfExpr: NULLIF(a, b) — `args` list
    if let Some(nif) = cast_node!(node, T_NullIfExpr, pg_sys::NullIfExpr) {
        if !nif.args.is_null() {
            let args = pg_list::<pg_sys::Node>(nif.args);
            for arg in args.iter_ptr() {
                // SAFETY: Node pointer from parse-tree target list; allocated by raw_parser.
                unsafe { collect_all_window_func_nodes(arg, result) };
            }
        }
        return;
    }

    // MinMaxExpr: GREATEST / LEAST
    if let Some(mm) = cast_node!(node, T_MinMaxExpr, pg_sys::MinMaxExpr) {
        if !mm.args.is_null() {
            let args = pg_list::<pg_sys::Node>(mm.args);
            for arg in args.iter_ptr() {
                // SAFETY: Node pointer from parse-tree target list; allocated by raw_parser.
                unsafe { collect_all_window_func_nodes(arg, result) };
            }
        }
        return;
    }

    // BoolExpr: AND / OR / NOT
    if let Some(be) = cast_node!(node, T_BoolExpr, pg_sys::BoolExpr) {
        if !be.args.is_null() {
            let args = pg_list::<pg_sys::Node>(be.args);
            for arg in args.iter_ptr() {
                // SAFETY: Node pointer from parse-tree target list; allocated by raw_parser.
                unsafe { collect_all_window_func_nodes(arg, result) };
            }
        }
        return;
    }

    // NullTest: IS NULL / IS NOT NULL
    if let Some(nt) = cast_node!(node, T_NullTest, pg_sys::NullTest) {
        if !nt.arg.is_null() {
            // SAFETY: Node pointer from parse-tree target list; allocated by raw_parser.
            unsafe { collect_all_window_func_nodes(nt.arg as *mut pg_sys::Node, result) };
        }
        return;
    }

    // RowExpr: ROW(a, b, c)
    if let Some(row) = cast_node!(node, T_RowExpr, pg_sys::RowExpr)
        && !row.args.is_null()
    {
        let args = pg_list::<pg_sys::Node>(row.args);
        for arg in args.iter_ptr() {
            // SAFETY: Node pointer from parse-tree target list; allocated by raw_parser.
            unsafe { collect_all_window_func_nodes(arg, result) };
        }
    }

    // Leaf nodes (ColumnRef, A_Const, SQLValueFunction, etc.) — no children
}

/// Extraction result for window function parsing.
type WindowExtraction = (Vec<WindowExpr>, Vec<(Expr, String)>);

/// Extract window function expressions and pass-through columns from a target list.
///
/// Returns `(window_exprs, pass_through_cols)` where each pass-through column
/// is `(Expr, alias)`.
unsafe fn extract_window_exprs(
    target_list: &pgrx::PgList<pg_sys::Node>,
    window_clause: *mut pg_sys::List,
) -> Result<WindowExtraction, PgTrickleError> {
    let mut window_exprs = Vec::new();
    let mut pass_through = Vec::new();

    for node_ptr in target_list.iter_ptr() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if !is_node_type!(node_ptr, T_ResTarget) {
            continue;
        }
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let rt = pg_deref!(node_ptr as *const pg_sys::ResTarget);
        if rt.val.is_null() {
            continue;
        }

        // Check if this target is a FuncCall with OVER clause
        if let Some(fcall) = cast_node!(rt.val, T_FuncCall, pg_sys::FuncCall)
            && !fcall.over.is_null()
        {
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            let wexpr = unsafe { parse_window_func_call(fcall, rt, window_clause)? };
            window_exprs.push(wexpr);
            continue;
        }

        // Check if a window function is nested inside an expression (CASE, COALESCE, etc.)
        // We detect this but cannot extract it — reject with a clear error.
        // SAFETY: Node pointer from parse-tree; allocated by raw_parser.
        if unsafe { node_contains_window_func(rt.val) } {
            return Err(PgTrickleError::UnsupportedOperator(
                "Window functions nested inside expressions (CASE, COALESCE, arithmetic, etc.) \
                 are not supported in defining queries. Move the window function to a separate \
                 column, e.g.:\n  SELECT ROW_NUMBER() OVER (...) AS rn, ... FROM t\n\
                 Then wrap the stream table in a view to apply the expression."
                    .into(),
            ));
        }

        // Not a window function — pass-through column
        let alias = if !rt.name.is_null() {
            pg_cstr_to_str(rt.name).unwrap_or("?column?").to_string()
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        } else if let Ok(e) = safe_node_to_expr(rt.val) {
            match &e {
                Expr::ColumnRef { column_name, .. } => column_name.clone(),
                Expr::Star { .. } => "*".to_string(),
                _ => format!("col_{}", pass_through.len()),
            }
        } else {
            format!("col_{}", pass_through.len())
        };
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = safe_node_to_expr(rt.val)?;
        pass_through.push((expr, alias));
    }

    Ok((window_exprs, pass_through))
}

/// Parse a single FuncCall with OVER clause into a WindowExpr.
///
/// If the OVER clause references a named window (e.g., `OVER w`),
/// the definition is resolved from `window_clause` (the `WINDOW` clause).
unsafe fn parse_window_func_call(
    fcall: &pg_sys::FuncCall,
    rt: &pg_sys::ResTarget,
    window_clause: *mut pg_sys::List,
) -> Result<WindowExpr, PgTrickleError> {
    // Function name
    // SAFETY: Parse-tree node pointer from raw_parser; valid within current memory context.
    let func_name = unsafe { extract_func_name(fcall.funcname)? };

    // Function arguments
    let args_list = pg_list::<pg_sys::Node>(fcall.args);
    let mut args = Vec::new();
    for n in args_list.iter_ptr() {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        if let Ok(e) = safe_node_to_expr(n) {
            args.push(e);
        }
    }

    // Parse the WindowDef (OVER clause)
    // SAFETY: caller guarantees fcall.over is non-null
    let wdef = pg_deref!(fcall.over);

    // Resolve named window reference: OVER w → look up from WINDOW clause
    let resolved_wdef = if !wdef.refname.is_null() && !window_clause.is_null() {
        let ref_name = pg_cstr_to_str(wdef.refname).unwrap_or("");
        let wclause = pg_list::<pg_sys::Node>(window_clause);
        let mut found: Option<&pg_sys::WindowDef> = None;
        for n in wclause.iter_ptr() {
            if let Some(wd) = cast_node!(n, T_WindowDef, pg_sys::WindowDef)
                && !wd.name.is_null()
            {
                let wd_name = pg_cstr_to_str(wd.name).unwrap_or("");
                if wd_name == ref_name {
                    found = Some(wd);
                    break;
                }
            }
        }
        found
    } else {
        None
    };

    // Use resolved window definition for partition/order if the inline OVER is empty
    let effective_part_clause = if !wdef.partitionClause.is_null() {
        wdef.partitionClause
    } else if let Some(rwd) = resolved_wdef {
        rwd.partitionClause
    } else {
        std::ptr::null_mut()
    };

    let effective_ord_clause = if !wdef.orderClause.is_null() {
        wdef.orderClause
    } else if let Some(rwd) = resolved_wdef {
        rwd.orderClause
    } else {
        std::ptr::null_mut()
    };

    // Use resolved window for frame if the inline OVER doesn't specify one
    let effective_frame_wdef = if wdef.frameOptions as u32 & pg_sys::FRAMEOPTION_NONDEFAULT != 0 {
        wdef
    } else if let Some(rwd) = resolved_wdef {
        rwd
    } else {
        wdef
    };

    // PARTITION BY
    let part_list = pg_list::<pg_sys::Node>(effective_part_clause);
    let mut partition_by = Vec::new();
    for n in part_list.iter_ptr() {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        if let Ok(e) = safe_node_to_expr(n) {
            partition_by.push(e);
        }
    }

    // ORDER BY (list of SortBy nodes)
    let ord_list = pg_list::<pg_sys::Node>(effective_ord_clause);
    let mut order_by = Vec::new();
    for n in ord_list.iter_ptr() {
        if let Some(sb) = cast_node!(n, T_SortBy, pg_sys::SortBy) {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let expr = safe_node_to_expr(sb.node)?;
            let ascending = sb.sortby_dir != pg_sys::SortByDir::SORTBY_DESC;
            let nulls_first = match sb.sortby_nulls {
                pg_sys::SortByNulls::SORTBY_NULLS_FIRST => true,
                pg_sys::SortByNulls::SORTBY_NULLS_LAST => false,
                _ => !ascending, // default: NULLS FIRST for DESC, NULLS LAST for ASC
            };
            order_by.push(SortExpr {
                expr,
                ascending,
                nulls_first,
            });
        }
    }

    // Parse window frame clause
    // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
    let frame_clause = unsafe { deparse_window_frame(effective_frame_wdef) };

    // Alias
    let alias = if !rt.name.is_null() {
        pg_cstr_to_str(rt.name).unwrap_or(&func_name).to_string()
    } else {
        func_name.clone()
    };

    Ok(WindowExpr {
        func_name,
        args,
        partition_by,
        order_by,
        frame_clause,
        alias,
    })
}

/// Deparse a window frame clause from a WindowDef's frameOptions.
///
/// Returns `None` if the frame is the default, `Some(clause)` otherwise.
///
/// # Safety
/// `wdef` must point to a valid `pg_sys::WindowDef`.
unsafe fn deparse_window_frame(wdef: &pg_sys::WindowDef) -> Option<String> {
    let opts = wdef.frameOptions as u32;
    if opts & pg_sys::FRAMEOPTION_NONDEFAULT == 0 {
        return None;
    }

    let mode = if opts & pg_sys::FRAMEOPTION_ROWS != 0 {
        "ROWS"
    } else if opts & pg_sys::FRAMEOPTION_GROUPS != 0 {
        "GROUPS"
    } else {
        "RANGE"
    };

    // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
    let start = unsafe { deparse_frame_bound(opts, true, wdef.startOffset) };
    // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
    let end = unsafe { deparse_frame_bound(opts, false, wdef.endOffset) };

    let frame = if opts & pg_sys::FRAMEOPTION_BETWEEN != 0 {
        format!("{mode} BETWEEN {start} AND {end}")
    } else {
        format!("{mode} {start}")
    };

    let exclusion = if opts & pg_sys::FRAMEOPTION_EXCLUDE_CURRENT_ROW != 0 {
        " EXCLUDE CURRENT ROW"
    } else if opts & pg_sys::FRAMEOPTION_EXCLUDE_GROUP != 0 {
        " EXCLUDE GROUP"
    } else if opts & pg_sys::FRAMEOPTION_EXCLUDE_TIES != 0 {
        " EXCLUDE TIES"
    } else {
        ""
    };

    Some(format!("{frame}{exclusion}"))
}

/// Deparse a single frame bound (start or end).
///
/// # Safety
/// `offset` may be null; caller must ensure it's valid if non-null.
unsafe fn deparse_frame_bound(opts: u32, is_start: bool, offset: *mut pg_sys::Node) -> String {
    if is_start {
        if opts & pg_sys::FRAMEOPTION_START_UNBOUNDED_PRECEDING != 0 {
            return "UNBOUNDED PRECEDING".to_string();
        }
        if opts & pg_sys::FRAMEOPTION_START_CURRENT_ROW != 0 {
            return "CURRENT ROW".to_string();
        }
        if opts & pg_sys::FRAMEOPTION_START_OFFSET_PRECEDING != 0
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            && let Ok(e) = safe_node_to_expr(offset)
        {
            return format!("{} PRECEDING", e.to_sql());
        }
        if opts & pg_sys::FRAMEOPTION_START_OFFSET_FOLLOWING != 0
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            && let Ok(e) = safe_node_to_expr(offset)
        {
            return format!("{} FOLLOWING", e.to_sql());
        }
        if opts & pg_sys::FRAMEOPTION_START_UNBOUNDED_FOLLOWING != 0 {
            return "UNBOUNDED FOLLOWING".to_string();
        }
    } else {
        if opts & pg_sys::FRAMEOPTION_END_UNBOUNDED_FOLLOWING != 0 {
            return "UNBOUNDED FOLLOWING".to_string();
        }
        if opts & pg_sys::FRAMEOPTION_END_CURRENT_ROW != 0 {
            return "CURRENT ROW".to_string();
        }
        if opts & pg_sys::FRAMEOPTION_END_OFFSET_FOLLOWING != 0
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            && let Ok(e) = safe_node_to_expr(offset)
        {
            return format!("{} FOLLOWING", e.to_sql());
        }
        if opts & pg_sys::FRAMEOPTION_END_OFFSET_PRECEDING != 0
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            && let Ok(e) = safe_node_to_expr(offset)
        {
            return format!("{} PRECEDING", e.to_sql());
        }
        if opts & pg_sys::FRAMEOPTION_END_UNBOUNDED_PRECEDING != 0 {
            return "UNBOUNDED PRECEDING".to_string();
        }
    }
    "CURRENT ROW".to_string()
}

/// Check if the target list contains any aggregate function calls.
unsafe fn target_list_has_aggregates(target_list: &pgrx::PgList<pg_sys::Node>) -> bool {
    for node_ptr in target_list.iter_ptr() {
        if let Some(rt) = cast_node!(node_ptr, T_ResTarget, pg_sys::ResTarget)
            && !rt.val.is_null()
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            && unsafe { expr_contains_agg(rt.val) }
        {
            return true;
        }
    }
    false
}

/// Check if a single raw-parse-tree node is an aggregate function call.
///
/// Does NOT recurse — only checks the immediate node.
unsafe fn is_agg_node(node: *mut pg_sys::Node) -> bool {
    if node.is_null() {
        return false;
    }
    // SQL/JSON standard aggregates
    // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
    if is_node_type!(node, T_JsonObjectAgg)
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        || is_node_type!(node, T_JsonArrayAgg)
    {
        return true;
    }
    if let Some(fcall) = cast_node!(node, T_FuncCall, pg_sys::FuncCall) {
        // Window functions (FuncCall with OVER clause) are NOT aggregates,
        // even if they use aggregate function names like SUM, COUNT.
        if !fcall.over.is_null() {
            return false;
        }
        if fcall.agg_within_group
            || !fcall.agg_order.is_null()
            || fcall.agg_star
            || !fcall.agg_filter.is_null()
            || fcall.agg_distinct
        {
            return true;
        }
        // SAFETY: Parse-tree node pointer from raw_parser; valid within current memory context.
        if let Ok(name) = unsafe { extract_func_name(fcall.funcname) } {
            let name_lower = name.to_lowercase();
            if is_known_aggregate(&name_lower) {
                return true;
            }
        }
    }
    false
}

/// Walker callback for [`expr_contains_agg`].
///
/// Returns `true` (stop walking) when a node is detected as an aggregate,
/// `false` to continue. Recursion into children is handled by
/// `raw_expression_tree_walker_impl`.
///
/// # Safety
/// Called by PostgreSQL's `raw_expression_tree_walker_impl` with valid
/// raw parse tree node pointers.
#[cfg(not(test))]
// SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
unsafe extern "C-unwind" fn agg_check_walker(
    node: *mut pg_sys::Node,
    _context: *mut std::ffi::c_void,
) -> bool {
    if node.is_null() {
        return false;
    }
    // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
    if unsafe { is_agg_node(node) } {
        return true; // found aggregate — stop walking
    }
    // Continue recursion into children.
    // SAFETY: raw_expression_tree_walker_impl handles all raw parse tree
    // node types (A_Expr, CaseExpr, BoolExpr, FuncCall, TypeCast, etc.).
    // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
    unsafe {
        pg_sys::raw_expression_tree_walker_impl(node, Some(agg_check_walker), std::ptr::null_mut())
    }
}

/// Recursively check if a raw parse tree node contains an aggregate
/// function call, anywhere in its expression subtree.
///
/// Uses PostgreSQL's `raw_expression_tree_walker_impl` for correct
/// recursion into all node types (A_Expr, CaseExpr, BoolExpr, etc.).
#[cfg(not(test))]
unsafe fn expr_contains_agg(node: *mut pg_sys::Node) -> bool {
    if node.is_null() {
        return false;
    }
    // Check the node itself first.
    // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
    if unsafe { is_agg_node(node) } {
        return true;
    }
    // Recurse into children.
    // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
    unsafe {
        pg_sys::raw_expression_tree_walker_impl(node, Some(agg_check_walker), std::ptr::null_mut())
    }
}

#[cfg(test)]
unsafe fn expr_contains_agg(node: *mut pg_sys::Node) -> bool {
    if node.is_null() {
        return false;
    }
    // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
    unsafe { is_agg_node(node) }
}

/// Check if a function name is a user-defined aggregate via `pg_proc.prokind`.
///
/// Returns `true` if the function exists in `pg_proc` with `prokind = 'a'`
/// (aggregate function) but is NOT in our hardcoded `is_known_aggregate` list.
/// This detects custom aggregates created with `CREATE AGGREGATE`.
#[cfg(not(test))]
fn is_user_defined_aggregate(name: &str) -> bool {
    // Strip schema qualification if present (e.g., "myschema.my_agg" → "my_agg").
    let bare_name = name.rsplit('.').next().unwrap_or(name);

    Spi::connect(|client| {
        let result = client
            .select(
                "SELECT 1 FROM pg_catalog.pg_proc WHERE proname = $1 AND prokind = 'a'",
                None,
                &[bare_name.into()],
            )
            .ok()?;
        if !result.is_empty() { Some(true) } else { None }
    })
    .unwrap_or(false)
}

/// Test-only stub: SPI is unavailable in unit tests.
#[cfg(test)]
fn is_user_defined_aggregate(_name: &str) -> bool {
    false
}

/// Check if a function name is a known aggregate (built-in or common).
pub(crate) fn is_known_aggregate(name: &str) -> bool {
    matches!(
        name,
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "string_agg"
            | "array_agg"
            | "json_agg"
            | "jsonb_agg"
            | "json_object_agg"
            | "jsonb_object_agg"
            | "json_objectagg"
            | "json_arrayagg"
            | "bool_and"
            | "bool_or"
            | "every"
            | "bit_and"
            | "bit_or"
            | "bit_xor"
            | "xmlagg"
            | "stddev"
            | "stddev_pop"
            | "stddev_samp"
            | "variance"
            | "var_pop"
            | "var_samp"
            | "corr"
            | "covar_pop"
            | "covar_samp"
            | "regr_avgx"
            | "regr_avgy"
            | "regr_count"
            | "regr_intercept"
            | "regr_r2"
            | "regr_slope"
            | "regr_sxx"
            | "regr_sxy"
            | "regr_syy"
            | "percentile_cont"
            | "percentile_disc"
            | "mode"
            | "any_value"
            | "rank"
            | "dense_rank"
            | "percent_rank"
            | "cume_dist"
            // G5.6: Range aggregates — recognized but rejected for DIFFERENTIAL mode.
            | "range_agg"
            | "range_intersect_agg"
    )
}

/// Extract aggregates and non-aggregate expressions from a target list.
unsafe fn extract_aggregates(
    target_list: &pgrx::PgList<pg_sys::Node>,
) -> Result<(Vec<AggExpr>, Vec<Expr>), PgTrickleError> {
    let mut aggs = Vec::new();
    let mut non_aggs = Vec::new();

    for node_ptr in target_list.iter_ptr() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if !is_node_type!(node_ptr, T_ResTarget) {
            continue;
        }
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let rt = pg_deref!(node_ptr as *const pg_sys::ResTarget);
        if rt.val.is_null() {
            continue;
        }

        // Preserve a simple COALESCE(SUM(...), default) as an outer Project
        // over a real SUM aggregate.  Treating the whole expression as a
        // ComplexExpression loses SUM's auxiliary state and makes deletion
        // of the last non-NULL input indistinguishable from a zero.
        if let Some(coalesce) = cast_node!(rt.val, T_CoalesceExpr, pg_sys::CoalesceExpr) {
            let coalesce_args = pg_list::<pg_sys::Node>(coalesce.args);
            if coalesce_args.len() >= 2
                && let Some(sum_node) = coalesce_args.head()
                && let Some(sum_call) = cast_node!(sum_node, T_FuncCall, pg_sys::FuncCall)
            {
                let func_name = unsafe { extract_func_name(sum_call.funcname)? };
                if func_name.eq_ignore_ascii_case("sum") && sum_call.over.is_null() {
                    let alias = if !rt.name.is_null() {
                        pg_cstr_to_str(rt.name).unwrap_or(&func_name).to_string()
                    } else {
                        func_name.clone()
                    };
                    let args_list = pg_list::<pg_sys::Node>(sum_call.args);
                    let argument = args_list.head().and_then(|n| safe_node_to_expr(n).ok());
                    let filter = if !sum_call.agg_filter.is_null() {
                        Some(safe_node_to_expr(sum_call.agg_filter)?)
                    } else {
                        None
                    };
                    aggs.push(AggExpr {
                        function: AggFunc::Sum,
                        argument,
                        alias,
                        is_distinct: sum_call.agg_distinct,
                        second_arg: None,
                        filter,
                        order_within_group: None,
                        statistical_support: None,
                    });
                    continue;
                }
            }
        }

        if let Some(fcall) = cast_node!(rt.val, T_FuncCall, pg_sys::FuncCall) {
            // Window functions (FuncCall with OVER clause) are NOT aggregates.
            // Treat them as plain expressions — they'll be handled by the
            // window operator path in parse_select_stmt.
            if !fcall.over.is_null() {
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let expr = safe_node_to_expr(rt.val)?;
                non_aggs.push(expr);
                continue;
            }

            // SAFETY: Parse-tree node pointer from raw_parser; valid within current memory context.
            let func_name = unsafe { extract_func_name(fcall.funcname)? };
            let name_lower = func_name.to_lowercase();

            let alias = if !rt.name.is_null() {
                pg_cstr_to_str(rt.name).unwrap_or(&func_name).to_string()
            } else {
                func_name.clone()
            };

            if let Some(agg_func) = match name_lower.as_str() {
                "count" if fcall.agg_star => Some(AggFunc::CountStar),
                "count" => Some(AggFunc::Count),
                "sum" => Some(AggFunc::Sum),
                "avg" => Some(AggFunc::Avg),
                "min" => Some(AggFunc::Min),
                "max" => Some(AggFunc::Max),
                "bool_and" | "every" => Some(AggFunc::BoolAnd),
                "bool_or" => Some(AggFunc::BoolOr),
                "string_agg" => Some(AggFunc::StringAgg),
                "array_agg" => Some(AggFunc::ArrayAgg),
                "json_agg" => Some(AggFunc::JsonAgg),
                "jsonb_agg" => Some(AggFunc::JsonbAgg),
                "bit_and" => Some(AggFunc::BitAnd),
                "bit_or" => Some(AggFunc::BitOr),
                "bit_xor" => Some(AggFunc::BitXor),
                "json_object_agg" => Some(AggFunc::JsonObjectAgg),
                "jsonb_object_agg" => Some(AggFunc::JsonbObjectAgg),
                "stddev" | "stddev_samp" => Some(AggFunc::StddevSamp),
                "stddev_pop" => Some(AggFunc::StddevPop),
                "variance" | "var_samp" => Some(AggFunc::VarSamp),
                "var_pop" => Some(AggFunc::VarPop),
                "any_value" => Some(AggFunc::AnyValue),
                "mode" => Some(AggFunc::Mode),
                "percentile_cont" => Some(AggFunc::PercentileCont),
                "percentile_disc" => Some(AggFunc::PercentileDisc),
                "corr" => Some(AggFunc::Corr),
                "covar_pop" => Some(AggFunc::CovarPop),
                "covar_samp" => Some(AggFunc::CovarSamp),
                "regr_avgx" => Some(AggFunc::RegrAvgx),
                "regr_avgy" => Some(AggFunc::RegrAvgy),
                "regr_count" => Some(AggFunc::RegrCount),
                "regr_intercept" => Some(AggFunc::RegrIntercept),
                "regr_r2" => Some(AggFunc::RegrR2),
                "regr_slope" => Some(AggFunc::RegrSlope),
                "regr_sxx" => Some(AggFunc::RegrSxx),
                "regr_sxy" => Some(AggFunc::RegrSxy),
                "regr_syy" => Some(AggFunc::RegrSyy),
                "xmlagg" => Some(AggFunc::XmlAgg),
                "rank" => Some(AggFunc::HypRank),
                "dense_rank" => Some(AggFunc::HypDenseRank),
                "percent_rank" => Some(AggFunc::HypPercentRank),
                "cume_dist" => Some(AggFunc::HypCumeDist),
                _ => None,
            } {
                let args_list = pg_list::<pg_sys::Node>(fcall.args);
                let argument = args_list
                    .head()
                    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                    .and_then(|n| safe_node_to_expr(n).ok());

                // Extract optional second argument (e.g., STRING_AGG separator)
                let second_arg = if args_list.len() >= 2 {
                    args_list
                        .get_ptr(1)
                        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                        .and_then(|n| safe_node_to_expr(n).ok())
                } else {
                    None
                };

                // Parse optional FILTER (WHERE ...) clause
                let filter = if !fcall.agg_filter.is_null() {
                    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                    Some(safe_node_to_expr(fcall.agg_filter)?)
                } else {
                    None
                };

                // Parse optional WITHIN GROUP (ORDER BY ...) for ordered-set aggregates
                // and regular aggregate ORDER BY (e.g., STRING_AGG(val, ',' ORDER BY val)).
                // Both are stored in order_within_group; agg_to_rescan_sql distinguishes
                // between WITHIN GROUP and regular ORDER BY based on the AggFunc type.
                let order_within_group = if !fcall.agg_order.is_null() {
                    let ord_list = pg_list::<pg_sys::Node>(fcall.agg_order);
                    let mut sorts = Vec::new();
                    for n in ord_list.iter_ptr() {
                        if let Some(sb) = cast_node!(n, T_SortBy, pg_sys::SortBy) {
                            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                            let expr = safe_node_to_expr(sb.node)?;
                            let ascending = sb.sortby_dir != pg_sys::SortByDir::SORTBY_DESC;
                            let nulls_first = match sb.sortby_nulls {
                                pg_sys::SortByNulls::SORTBY_NULLS_FIRST => true,
                                pg_sys::SortByNulls::SORTBY_NULLS_LAST => false,
                                // default: NULLS FIRST for DESC, NULLS LAST for ASC
                                _ => !ascending,
                            };
                            sorts.push(SortExpr {
                                expr,
                                ascending,
                                nulls_first,
                            });
                        }
                    }
                    if sorts.is_empty() { None } else { Some(sorts) }
                } else {
                    None
                };
                let statistical_support =
                    if agg_func.needs_sum_of_squares() || agg_func.needs_cross_products() {
                        Some(StatisticalAggSupport::with_location(fcall.location))
                    } else {
                        None
                    };

                aggs.push(AggExpr {
                    function: agg_func,
                    argument,
                    alias,
                    is_distinct: fcall.agg_distinct,
                    second_arg,
                    filter,
                    order_within_group,
                    statistical_support,
                });
            } else if is_known_aggregate(&name_lower) {
                // Recognized as an aggregate but not supported for differential maintenance
                return Err(PgTrickleError::UnsupportedOperator(format!(
                    "Aggregate function {func_name}() is not supported in DIFFERENTIAL mode. \
                     Supported aggregates: COUNT, SUM, AVG, MIN, MAX, BOOL_AND, BOOL_OR, \
                     STRING_AGG, ARRAY_AGG, JSON_AGG, JSONB_AGG, BIT_AND, BIT_OR, BIT_XOR, \
                     JSON_OBJECT_AGG, JSONB_OBJECT_AGG, STDDEV, STDDEV_POP, STDDEV_SAMP, \
                     VARIANCE, VAR_POP, VAR_SAMP, ANY_VALUE, MODE, PERCENTILE_CONT, \
                     PERCENTILE_DISC, CORR, COVAR_POP, COVAR_SAMP, REGR_AVGX, REGR_AVGY, \
                     REGR_COUNT, REGR_INTERCEPT, REGR_R2, REGR_SLOPE, REGR_SXX, REGR_SXY, \
                     REGR_SYY, XMLAGG, RANK/DENSE_RANK/PERCENT_RANK/CUME_DIST WITHIN GROUP. \
                     Use FULL refresh mode instead.",
                )));
            } else if is_user_defined_aggregate(&name_lower) {
                // User-defined aggregate (CREATE AGGREGATE) — treat as group-rescan.
                // Build the complete call SQL (including ORDER BY and FILTER) so that
                // agg_to_rescan_sql can emit it verbatim and produce correct results.
                // Also populate `argument` and `filter` so agg_delta_exprs can apply
                // the FILTER predicate when counting insert/delete events.

                let args_list = pg_list::<pg_sys::Node>(fcall.args);
                let argument = args_list
                    .head()
                    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                    .and_then(|n| safe_node_to_expr(n).ok());

                // Build the argument SQL, respecting DISTINCT.
                let distinct_str = if fcall.agg_distinct { "DISTINCT " } else { "" };
                let base_args: Vec<String> = {
                    let mut v = Vec::new();
                    for n in args_list.iter_ptr() {
                        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                        if let Ok(e) = safe_node_to_expr(n) {
                            v.push(e.to_sql());
                        }
                    }
                    v
                };
                let args_sql = if base_args.is_empty() {
                    String::new()
                } else {
                    format!("{distinct_str}{}", base_args.join(", "))
                };

                // Parse ORDER BY inside the aggregate call (e.g., my_agg(val ORDER BY x)).
                let order_sql = if !fcall.agg_order.is_null() {
                    let ord_list = pg_list::<pg_sys::Node>(fcall.agg_order);
                    let mut sorts = Vec::new();
                    for n in ord_list.iter_ptr() {
                        if let Some(sb) = cast_node!(n, T_SortBy, pg_sys::SortBy) {
                            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                            let expr = safe_node_to_expr(sb.node)?;
                            let dir = if sb.sortby_dir == pg_sys::SortByDir::SORTBY_DESC {
                                " DESC"
                            } else {
                                ""
                            };
                            sorts.push(format!("{}{dir}", expr.to_sql()));
                        }
                    }
                    if sorts.is_empty() {
                        None
                    } else {
                        Some(sorts.join(", "))
                    }
                } else {
                    None
                };

                let call_sql = match &order_sql {
                    Some(ord) => format!("{func_name}({args_sql} ORDER BY {ord})"),
                    None => format!("{func_name}({args_sql})"),
                };

                // Parse FILTER (WHERE ...) clause — stored both in `filter` (for delta
                // tracking) and appended to `raw_sql` (for rescan correctness).
                let filter = if !fcall.agg_filter.is_null() {
                    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                    Some(safe_node_to_expr(fcall.agg_filter)?)
                } else {
                    None
                };

                let raw_sql = match &filter {
                    Some(f) => format!("{call_sql} FILTER (WHERE {})", f.to_sql()),
                    None => call_sql,
                };

                aggs.push(AggExpr {
                    function: AggFunc::UserDefined(raw_sql),
                    argument,
                    alias,
                    is_distinct: fcall.agg_distinct,
                    second_arg: None,
                    filter,
                    order_within_group: None,
                    statistical_support: None,
                });
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            } else if unsafe { expr_contains_agg(rt.val) } {
                // Non-aggregate function wrapping nested aggregate(s),
                // e.g. ROUND(STDDEV_POP(amount), 2). Treat as ComplexExpression
                // so the group-rescan path re-evaluates it correctly.
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let raw_expr = safe_node_to_expr(rt.val)?;
                let raw_sql = raw_expr.to_sql();

                let alias = if !rt.name.is_null() {
                    pg_cstr_to_str(rt.name).unwrap_or("complex_agg").to_string()
                } else {
                    format!("complex_agg_{}", aggs.len())
                };

                aggs.push(AggExpr {
                    function: AggFunc::ComplexExpression(raw_sql),
                    argument: None,
                    alias,
                    is_distinct: false,
                    second_arg: None,
                    filter: None,
                    order_within_group: None,
                    statistical_support: None,
                });
            } else {
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let expr = safe_node_to_expr(rt.val)?;
                non_aggs.push(expr);
            }
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        } else if is_node_type!(rt.val, T_JsonObjectAgg) {
            // ── F11: SQL/JSON standard JSON_OBJECTAGG(key: value ...) ──
            // This node type is separate from T_FuncCall, so we handle it
            // explicitly. Deparse to SQL and store as AggFunc::JsonObjectAggStd.
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let raw_expr = safe_node_to_expr(rt.val)?;
            let raw_sql = raw_expr.to_sql();

            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let joa = pg_deref!(rt.val as *const pg_sys::JsonObjectAgg);
            let alias = if !rt.name.is_null() {
                pg_cstr_to_str(rt.name)
                    .unwrap_or("json_objectagg")
                    .to_string()
            } else {
                "json_objectagg".to_string()
            };

            // Extract FILTER clause from the constructor (stored separately
            // so agg_delta_exprs can apply the filter to change tracking).
            let filter = if !joa.constructor.is_null() {
                // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
                let ctor = pg_deref!(joa.constructor);
                if !ctor.agg_filter.is_null() {
                    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                    Some(safe_node_to_expr(ctor.agg_filter)?)
                } else {
                    None
                }
            } else {
                None
            };

            aggs.push(AggExpr {
                function: AggFunc::JsonObjectAggStd(raw_sql),
                argument: None,
                alias,
                is_distinct: false,
                second_arg: None,
                filter,
                order_within_group: None,
                statistical_support: None,
            });
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        } else if is_node_type!(rt.val, T_JsonArrayAgg) {
            // ── F11: SQL/JSON standard JSON_ARRAYAGG(expr ...) ──
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let raw_expr = safe_node_to_expr(rt.val)?;
            let raw_sql = raw_expr.to_sql();

            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let jaa = pg_deref!(rt.val as *const pg_sys::JsonArrayAgg);
            let alias = if !rt.name.is_null() {
                pg_cstr_to_str(rt.name)
                    .unwrap_or("json_arrayagg")
                    .to_string()
            } else {
                "json_arrayagg".to_string()
            };

            let filter = if !jaa.constructor.is_null() {
                // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
                let ctor = pg_deref!(jaa.constructor);
                if !ctor.agg_filter.is_null() {
                    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                    Some(safe_node_to_expr(ctor.agg_filter)?)
                } else {
                    None
                }
            } else {
                None
            };

            aggs.push(AggExpr {
                function: AggFunc::JsonArrayAggStd(raw_sql),
                argument: None,
                alias,
                is_distinct: false,
                second_arg: None,
                filter,
                order_within_group: None,
                statistical_support: None,
            });
        } else {
            // Not a top-level aggregate function call.
            // Check if the expression CONTAINS nested aggregate calls
            // (e.g., `100 * SUM(a) / NULLIF(SUM(b), 0)`).
            // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
            if unsafe { expr_contains_agg(rt.val) } {
                // Deparse the entire expression to SQL for the rescan CTE.
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let raw_expr = safe_node_to_expr(rt.val)?;
                let raw_sql = raw_expr.to_sql();

                let alias = if !rt.name.is_null() {
                    pg_cstr_to_str(rt.name).unwrap_or("complex_agg").to_string()
                } else {
                    format!("complex_agg_{}", aggs.len())
                };

                aggs.push(AggExpr {
                    function: AggFunc::ComplexExpression(raw_sql),
                    argument: None,
                    alias,
                    is_distinct: false,
                    second_arg: None,
                    filter: None,
                    order_within_group: None,
                    statistical_support: None,
                });
            } else {
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let expr = safe_node_to_expr(rt.val)?;
                non_aggs.push(expr);
            }
        }
    }

    Ok((aggs, non_aggs))
}

/// Parse the target list (SELECT expressions) into Expr + alias pairs.
unsafe fn parse_target_list(
    target_list: &pgrx::PgList<pg_sys::Node>,
) -> Result<(Vec<Expr>, Vec<String>), PgTrickleError> {
    let mut expressions = Vec::new();
    let mut aliases = Vec::new();

    for node_ptr in target_list.iter_ptr() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if !is_node_type!(node_ptr, T_ResTarget) {
            continue;
        }
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let rt = pg_deref!(node_ptr as *const pg_sys::ResTarget);

        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        let alias = unsafe { target_alias_for_res_target(rt, expressions.len()) };

        if rt.val.is_null() {
            expressions.push(Expr::Star { table_alias: None });
            aliases.push("*".to_string());
        } else {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let expr = safe_node_to_expr(rt.val)?;
            expressions.push(expr);
            aliases.push(alias);
        }
    }

    Ok((expressions, aliases))
}

/// Determine the output alias for a SELECT target.
///
/// Matches the aliasing used by [`parse_target_list`]: explicit alias first,
/// then simple column name / `*`, else a synthetic `col_N` fallback.
unsafe fn target_alias_for_res_target(rt: &pg_sys::ResTarget, ordinal: usize) -> String {
    if !rt.name.is_null() {
        return pg_cstr_to_str(rt.name).unwrap_or("?column?").to_string();
    }

    if !rt.val.is_null()
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        && let Ok(expr) = safe_node_to_expr(rt.val)
    {
        return match &expr {
            Expr::ColumnRef { column_name, .. } => column_name.clone(),
            Expr::Star { .. } => "*".to_string(),
            _ => format!("col_{ordinal}"),
        };
    }

    format!("col_{ordinal}")
}

// ── Unit tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm::parser::types::{AggExpr, AggFunc};

    // Helper: build a minimal AggExpr for testing rewrite_having_expr.
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

    // Helper: build a minimal Scan node for OpTree tests.
    fn make_scan(alias: &str) -> OpTree {
        OpTree::Scan {
            table_oid: 0,
            table_name: alias.to_string(),
            schema: "public".to_string(),
            columns: vec![],
            pk_columns: vec![],
            alias: alias.to_string(),
        }
    }

    // ── extract_bare_scalar_subquery_sql ─────────────────────────────────

    #[test]
    fn test_extract_bare_scalar_subquery_sql_valid() {
        let result = extract_bare_scalar_subquery_sql("(SELECT 1)");
        assert_eq!(result, Some("SELECT 1".to_string()));
    }

    #[test]
    fn test_extract_bare_scalar_subquery_sql_with_whitespace() {
        let result = extract_bare_scalar_subquery_sql("  (  SELECT count(*) FROM t  )  ");
        assert_eq!(result, Some("SELECT count(*) FROM t".to_string()));
    }

    #[test]
    fn test_extract_bare_scalar_subquery_sql_no_parens() {
        assert!(extract_bare_scalar_subquery_sql("SELECT 1").is_none());
    }

    #[test]
    fn test_extract_bare_scalar_subquery_sql_only_open_paren() {
        assert!(extract_bare_scalar_subquery_sql("(SELECT 1").is_none());
    }

    #[test]
    fn test_extract_bare_scalar_subquery_sql_not_a_select() {
        // Parenthesised expression that doesn't start with SELECT.
        assert!(extract_bare_scalar_subquery_sql("(42)").is_none());
    }

    #[test]
    fn test_extract_bare_scalar_subquery_sql_case_insensitive() {
        // "select" (lowercase) should also be accepted.
        let result = extract_bare_scalar_subquery_sql("(select count(*) from t)");
        assert_eq!(result, Some("select count(*) from t".to_string()));
    }

    #[test]
    fn test_extract_bare_scalar_subquery_sql_empty() {
        assert!(extract_bare_scalar_subquery_sql("").is_none());
    }

    // ── is_known_aggregate ───────────────────────────────────────────────

    #[test]
    fn test_is_known_aggregate_basic_aggregates() {
        for name in ["count", "sum", "avg", "min", "max"] {
            assert!(is_known_aggregate(name), "{name} should be known");
        }
    }

    #[test]
    fn test_is_known_aggregate_string_aggregates() {
        assert!(is_known_aggregate("string_agg"));
        assert!(is_known_aggregate("array_agg"));
        assert!(is_known_aggregate("json_agg"));
        assert!(is_known_aggregate("jsonb_agg"));
    }

    #[test]
    fn test_is_known_aggregate_statistical() {
        assert!(is_known_aggregate("stddev"));
        assert!(is_known_aggregate("variance"));
        assert!(is_known_aggregate("corr"));
        assert!(is_known_aggregate("regr_slope"));
    }

    #[test]
    fn test_is_known_aggregate_ordered_set() {
        assert!(is_known_aggregate("percentile_cont"));
        assert!(is_known_aggregate("percentile_disc"));
        assert!(is_known_aggregate("mode"));
    }

    #[test]
    fn test_is_known_aggregate_range_aggregates() {
        assert!(is_known_aggregate("range_agg"));
        assert!(is_known_aggregate("range_intersect_agg"));
    }

    #[test]
    fn test_is_known_aggregate_unknown_function() {
        assert!(!is_known_aggregate("lower"));
        assert!(!is_known_aggregate("upper"));
        assert!(!is_known_aggregate("my_custom_agg"));
        assert!(!is_known_aggregate(""));
    }

    // ── is_star_only ─────────────────────────────────────────────────────

    #[test]
    fn test_is_star_only_single_star() {
        assert!(is_star_only(&[Expr::Star { table_alias: None }]));
    }

    #[test]
    fn test_is_star_only_qualified_star_is_not_star_only() {
        // `table.*` is not a bare `*`.
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
        // COUNT(*) in HAVING → rewritten to the target-list alias column.
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
        // A function not in the agg list is left as-is.
        let aggs = vec![make_count_star_agg("__pgt_cnt")];
        let expr = Expr::FuncCall {
            func_name: "lower".to_string(),
            args: vec![Expr::ColumnRef {
                table_alias: None,
                column_name: "name".to_string(),
            }],
        };
        let result = rewrite_having_expr(&expr, &aggs);
        // Should be unchanged.
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
        // BinaryOp containing a matching aggregate gets rewritten inside.
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

    // ── collect_tree_source_aliases ──────────────────────────────────────

    #[test]
    fn test_collect_tree_source_aliases_single_scan() {
        let tree = make_scan("orders");
        let aliases = collect_tree_source_aliases(&tree);
        assert_eq!(aliases, vec!["orders"]);
    }

    #[test]
    fn test_collect_tree_source_aliases_inner_join() {
        let tree = OpTree::InnerJoin {
            condition: Expr::Literal("TRUE".to_string()),
            left: Box::new(make_scan("orders")),
            right: Box::new(make_scan("customers")),
        };
        let mut aliases = collect_tree_source_aliases(&tree);
        aliases.sort();
        assert_eq!(aliases, vec!["customers", "orders"]);
    }

    #[test]
    fn test_collect_tree_source_aliases_filter_over_scan() {
        let tree = OpTree::Filter {
            predicate: Expr::Literal("TRUE".to_string()),
            child: Box::new(make_scan("orders")),
        };
        assert_eq!(collect_tree_source_aliases(&tree), vec!["orders"]);
    }

    #[test]
    fn test_collect_tree_source_aliases_subquery() {
        let tree = OpTree::Subquery {
            alias: "sub".to_string(),
            column_aliases: vec![],
            child: Box::new(make_scan("orders")),
        };
        assert_eq!(collect_tree_source_aliases(&tree), vec!["sub"]);
    }

    // ── split_exists_correlation ─────────────────────────────────────────

    #[test]
    fn test_split_exists_correlation_simple_equality() {
        // inner.id = outer.order_id → extracted as correlation pair.
        let inner_aliases = vec!["inner_t".to_string()];
        let expr = Expr::BinaryOp {
            op: "=".to_string(),
            left: Box::new(Expr::ColumnRef {
                table_alias: Some("inner_t".to_string()),
                column_name: "id".to_string(),
            }),
            right: Box::new(Expr::ColumnRef {
                table_alias: Some("outer_t".to_string()),
                column_name: "order_id".to_string(),
            }),
        };
        let (corr, remaining) = split_exists_correlation(&expr, &inner_aliases);
        assert_eq!(corr.len(), 1, "one correlation pair expected");
        assert!(remaining.is_none(), "no remaining predicates");
    }

    #[test]
    fn test_split_exists_correlation_non_correlation_kept_as_remaining() {
        // A predicate that references only the inner table stays in remaining.
        let inner_aliases = vec!["inner_t".to_string()];
        let expr = Expr::BinaryOp {
            op: ">".to_string(),
            left: Box::new(Expr::ColumnRef {
                table_alias: Some("inner_t".to_string()),
                column_name: "amount".to_string(),
            }),
            right: Box::new(Expr::Literal("0".to_string())),
        };
        let (corr, remaining) = split_exists_correlation(&expr, &inner_aliases);
        assert!(corr.is_empty(), "no correlation pairs expected");
        assert!(remaining.is_some(), "non-correlation predicate should stay");
    }

    #[test]
    fn test_split_exists_correlation_and_conjunction_splits() {
        // AND(corr, filter) → corr extracted, filter in remaining.
        let inner_aliases = vec!["inner_t".to_string()];
        let corr_pred = Expr::BinaryOp {
            op: "=".to_string(),
            left: Box::new(Expr::ColumnRef {
                table_alias: Some("inner_t".to_string()),
                column_name: "id".to_string(),
            }),
            right: Box::new(Expr::ColumnRef {
                table_alias: Some("outer_t".to_string()),
                column_name: "inner_id".to_string(),
            }),
        };
        let filter_pred = Expr::BinaryOp {
            op: ">".to_string(),
            left: Box::new(Expr::ColumnRef {
                table_alias: Some("inner_t".to_string()),
                column_name: "amount".to_string(),
            }),
            right: Box::new(Expr::Literal("0".to_string())),
        };
        let and_expr = Expr::BinaryOp {
            op: "AND".to_string(),
            left: Box::new(corr_pred),
            right: Box::new(filter_pred),
        };
        let (corr, remaining) = split_exists_correlation(&and_expr, &inner_aliases);
        assert_eq!(corr.len(), 1);
        assert!(remaining.is_some());
    }

    // ── M-5: multi-column IN → SemiJoin tests ────────────────────────────

    #[test]
    fn test_multi_column_in_builds_eq_chain() {
        // Verify that the multi-column path in parse_any_sublink produces
        // an AND-chained equality condition from RowExpr columns.
        // We test this indirectly through the Expr building logic.
        let left1 = Expr::ColumnRef {
            table_alias: None,
            column_name: "a".into(),
        };
        let left2 = Expr::ColumnRef {
            table_alias: None,
            column_name: "b".into(),
        };
        let right1 = Expr::ColumnRef {
            table_alias: Some("t".into()),
            column_name: "x".into(),
        };
        let right2 = Expr::ColumnRef {
            table_alias: Some("t".into()),
            column_name: "y".into(),
        };

        let pairs: Vec<(Expr, Expr)> = vec![(left1, right1), (left2, right2)];
        let eq_chain = pairs
            .into_iter()
            .map(|(l, r)| Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(l),
                right: Box::new(r),
            })
            .reduce(|acc, eq| Expr::BinaryOp {
                op: "AND".to_string(),
                left: Box::new(acc),
                right: Box::new(eq),
            })
            .unwrap();

        let sql = eq_chain.to_sql();
        // to_sql() emits double-quoted identifiers (e.g. "t"."x").
        assert!(
            sql.contains("a") && sql.contains("x"),
            "expected a and x in: {sql}"
        );
        assert!(
            sql.contains("b") && sql.contains("y"),
            "expected b and y in: {sql}"
        );
        assert!(sql.contains("AND"), "expected AND in: {sql}");
    }
}
