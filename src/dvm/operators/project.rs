//! Projection differentiation.
//!
//! ΔI(πE(Q)) = πE(ΔI(Q))
//!
//! Apply the same projection expressions to the child's delta.
//! Row ID and action columns are passed through unchanged, except
//! for join children where the row ID is recomputed from the projected
//! columns to match the full-refresh hash formula.

use std::collections::HashMap;
use std::fmt::Write as FmtWrite;

use crate::dvm::diff::{DiffContext, DiffResult, quote_ident};
use crate::dvm::operators::scan::build_hash_expr;
use crate::dvm::parser::{Expr, OpTree, join_pk_expr_indices, unwrap_transparent};
use crate::error::PgTrickleError;

/// Differentiate a Project node.
pub fn diff_project(ctx: &mut DiffContext, op: &OpTree) -> Result<DiffResult, PgTrickleError> {
    let OpTree::Project {
        expressions,
        aliases,
        child,
    } = op
    else {
        return Err(PgTrickleError::InternalError(
            "diff_project called on non-Project node".into(),
        ));
    };

    // When a Project renames columns (e.g., `r.name AS region`), the
    // child's output column names differ from the ST's column names
    // (which use Project aliases). Temporarily map st_user_columns to
    // the child's output names so that downstream operators (e.g.,
    // diff_aggregate's is_intermediate check) can match their output
    // columns against the "effective" ST columns at their level.
    //
    // Also build st_column_alias_map so that downstream operators can
    // translate their own column names back to the actual ST column names
    // (e.g., aggregate merge CTE needs st."region" not st."name").
    let saved_st_cols = ctx.st_user_columns.clone();
    let saved_alias_map = ctx.st_column_alias_map.clone();
    if let Some(ref st_cols) = saved_st_cols {
        let child_out = child.output_columns();
        // Map positionally: aliases[i] in st_cols → child_out[i]
        if child_out.len() == aliases.len() {
            let mut alias_map = std::collections::HashMap::new();
            let mapped: Vec<String> = st_cols
                .iter()
                .map(|st_col| {
                    if let Some(pos) = aliases.iter().position(|a| a == st_col) {
                        let child_name = child_out
                            .get(pos)
                            .cloned()
                            .unwrap_or_else(|| st_col.clone());
                        if child_name != *st_col {
                            alias_map.insert(child_name.clone(), st_col.clone());
                        }
                        child_name
                    } else {
                        st_col.clone()
                    }
                })
                .collect();
            ctx.st_user_columns = Some(mapped);
            if !alias_map.is_empty() {
                ctx.st_column_alias_map = Some(alias_map);
            }
        }
    }

    // Detect COALESCE(aggregate_col, default) wrappers in the Project and
    // inform diff_aggregate about the ELSE branch semantics.  The ST stores
    // the default for a wrapped SUM and NULL for a bare SUM when it empties.
    // P-3: agg_sum_coalesce_defaults is Option<HashMap> — only allocated on
    // first COALESCE detection (lazy allocation).
    let saved_coalesce_defaults = ctx.agg_sum_coalesce_defaults.take();
    for (expr, alias) in expressions.iter().zip(aliases.iter()) {
        if let crate::dvm::parser::Expr::FuncCall { func_name, args } = expr
            && func_name.eq_ignore_ascii_case("coalesce")
            && args.len() >= 2
            && let (
                crate::dvm::parser::Expr::ColumnRef { column_name, .. },
                crate::dvm::parser::Expr::Literal(default_val),
            ) = (&args[0], &args[1])
        {
            // The column being wrapped is a child output column.
            // Record the mapping: alias (ST col name) → default.
            let _ = alias; // alias matches the outer SELECT alias
            ctx.agg_sum_coalesce_defaults
                .get_or_insert_with(HashMap::new)
                .insert(column_name.clone(), default_val.clone());
        }
    }

    // Differentiate the child
    let child_result = ctx.diff_node(child)?;

    // Restore st_user_columns, alias map, and coalesce defaults
    ctx.st_user_columns = saved_st_cols;
    ctx.st_column_alias_map = saved_alias_map;
    ctx.agg_sum_coalesce_defaults = saved_coalesce_defaults;

    // The child CTE's column names. For join children, columns are
    // disambiguated as "table__col" (e.g., "l__id", "r__id").
    let child_cols = &child_result.columns;

    let cte_name = ctx.next_cte_name("project");

    // Build projected column expressions.
    // Qualified column references (e.g., l.id) need to be resolved to
    // the child CTE's disambiguated column names (e.g., "l__id") since
    // the child is a single CTE, not multiple tables.
    let proj_cols: Vec<String> = expressions
        .iter()
        .zip(aliases.iter())
        .map(|(expr, alias)| {
            let resolved_sql = resolve_expr_to_child(expr, child_cols);
            let alias_ident = quote_ident(alias);
            if resolved_sql == *alias {
                alias_ident
            } else {
                format!("{resolved_sql} AS {alias_ident}")
            }
        })
        .collect();

    // For join children, recompute __pgt_row_id from the projected columns.
    // The full refresh uses hash of all output columns for join queries,
    // so the delta must produce matching row IDs.
    //
    // For lateral function/subquery children, also recompute __pgt_row_id
    // from all projected columns. SRF expansions have no natural PK, so
    // the delta's row_id must match the full refresh's hash of the
    // projected output columns.
    //
    // We must reference the SOURCE column names (from child CTE), not the
    // aliases, since SQL doesn't allow referencing aliases defined in the
    // same SELECT clause.
    // Look through transparent wrappers (Filter, Subquery) to find the
    // underlying child node. Q15 has Project > Filter > InnerJoin — the
    // Filter must not prevent PK-based row_ids or lateral detection.
    let unwrapped = unwrap_transparent(child);
    let is_join_child = matches!(
        unwrapped,
        OpTree::InnerJoin { .. } | OpTree::LeftJoin { .. } | OpTree::FullJoin { .. }
    );
    let is_lateral_child = matches!(
        unwrapped,
        OpTree::LateralFunction { .. } | OpTree::LateralSubquery { .. }
    );
    let is_semijoin_child = matches!(unwrapped, OpTree::SemiJoin { .. } | OpTree::AntiJoin { .. });

    let row_id_select = if is_join_child {
        // Use PK-corresponding expressions for hashing — PK-based row_ids
        // are stable across value changes, avoiding hash mismatch when
        // both join sides change simultaneously.
        let pk_indices = join_pk_expr_indices(expressions, unwrapped);
        let hash_exprs: Vec<&Expr> = if pk_indices.is_empty() {
            // Fallback: hash all expressions (no PK info available)
            expressions.iter().collect()
        } else {
            pk_indices.iter().map(|&i| &expressions[i]).collect()
        };
        let hash_cols: Vec<String> = hash_exprs
            .iter()
            .map(|expr| resolve_expr_to_child(expr, child_cols))
            .collect();
        format!("{} AS __pgt_row_id", build_hash_expr(&hash_cols))
    } else if is_lateral_child || is_semijoin_child {
        // Hash all projected columns — matches row_id_key_columns()
        // which returns all aliases for lateral children and semi/anti-join
        // children.  For EXISTS/IN queries, this ensures the FULL refresh and
        // DIFFERENTIAL refresh produce identical __pgt_row_id values so that
        // MERGE can correctly DELETE rows that no longer satisfy the semi-join.
        let hash_cols: Vec<String> = expressions
            .iter()
            .map(|expr| resolve_expr_to_child(expr, child_cols))
            .collect();
        format!("{} AS __pgt_row_id", build_hash_expr(&hash_cols))
    } else {
        // Recompute __pgt_row_id from the projected key columns to ensure
        // consistency with the FULL refresh hash formula.
        //
        // For regular table sources with PK, this produces the same value
        // as the change buffer's pk_hash (both hash the same PK columns).
        // For ST sources without PK, the change buffer's pk_hash is the
        // upstream ST's row_id (a different formula), so recomputing here
        // ensures the MERGE ON clause can match rows correctly.
        match op.row_id_key_columns() {
            Some(key_cols) if !key_cols.is_empty() => {
                let hash_cols: Vec<String> = key_cols
                    .iter()
                    .filter_map(|col| {
                        aliases
                            .iter()
                            .position(|a| a == col)
                            .map(|pos| resolve_expr_to_child(&expressions[pos], child_cols))
                    })
                    .collect();
                if hash_cols.is_empty() || hash_cols.len() != key_cols.len() {
                    // Not all key columns could be resolved — fall back
                    "__pgt_row_id".to_string()
                } else if hash_cols.len() == 1 {
                    format!(
                        "pgtrickle.pg_trickle_hash({}::TEXT) AS __pgt_row_id",
                        hash_cols[0]
                    )
                } else {
                    let items: Vec<String> =
                        hash_cols.iter().map(|e| format!("({e})::TEXT")).collect();
                    format!(
                        "{} AS __pgt_row_id",
                        crate::hash::build_composite_hash_expr(&items)
                    )
                }
            }
            _ => "__pgt_row_id".to_string(),
        }
    };

    // SF-6: Forward dual-count columns (__pgt_count_l, __pgt_count_r) when
    // the child result includes them (EXCEPT/INTERSECT operators). Without
    // this, the MERGE step cannot update the per-branch multiplicity counts
    // and rows become permanently stale.
    let has_count_l = child_result.columns.contains(&"__pgt_count_l".to_string());
    let has_count_r = child_result.columns.contains(&"__pgt_count_r".to_string());

    // SF-7: Forward aggregate auxiliary columns (__pgt_count, __pgt_aux_*)
    // when the child result includes them (Aggregate operator). Without
    // this, the MERGE step inserts new aggregate rows with __pgt_count = 0
    // (the column default), corrupting the group count used for subsequent
    // differential refreshes.
    let aux_cols: Vec<&String> = child_result
        .columns
        .iter()
        .filter(|c| {
            *c == "__pgt_count" || c.starts_with("__pgt_aux_") || c.starts_with("__pgt_nonnull_")
        })
        .collect();

    // PERF-2: Pre-size the extra_cols buffer to avoid repeated reallocations.
    // Each column entry is ~20 chars on average; reserve based on count.
    let extra_cap = if has_count_l && has_count_r { 40 } else { 0 } + aux_cols.len() * 24;
    let mut extra_cols_select = String::with_capacity(extra_cap);
    if has_count_l && has_count_r {
        extra_cols_select.push_str(", \"__pgt_count_l\", \"__pgt_count_r\"");
    }
    for col in &aux_cols {
        // SAFETY: write! on String never fails.
        let _ = write!(extra_cols_select, ", \"{}\"", col.replace('"', "\"\""));
    }

    // PERF-2: Pre-size the final SQL buffer: row_id + action + all projected
    // columns + extra columns + FROM clause. Eliminates the intermediate
    // String allocation that `format!` would otherwise create.
    let proj_cols_str = proj_cols.join(", ");
    let child_cte = &child_result.cte_name;
    let sql_cap = row_id_select.len()
        + 20 // ", __pgt_action, "
        + proj_cols_str.len()
        + extra_cols_select.len()
        + 8  // "\nFROM "
        + child_cte.len();
    let mut sql = String::with_capacity(sql_cap);
    let _ = write!(
        sql,
        "SELECT {row_id_select}, __pgt_action, {proj_cols_str}{extra_cols_select}\nFROM {child_cte}",
    );

    ctx.add_cte(cte_name.clone(), sql);

    let mut output_cols: Vec<String> = aliases.clone();
    if has_count_l && has_count_r {
        output_cols.push("__pgt_count_l".to_string());
        output_cols.push("__pgt_count_r".to_string());
    }
    for col in &aux_cols {
        output_cols.push((*col).clone());
    }

    Ok(DiffResult {
        cte_name,
        columns: output_cols,
        is_deduplicated: child_result.is_deduplicated,
        has_key_changed: child_result.has_key_changed,
    })
}

/// Resolve an expression's column references to match the child CTE's
/// column names.
///
/// When the child is a join CTE, columns are disambiguated as
/// `"table__col"`. A qualified reference like `ColumnRef("l", "id")`
/// becomes `"l__id"` if that name exists in `child_cols`. Unqualified
/// references and non-ColumnRef expressions pass through unchanged.
fn resolve_expr_to_child(expr: &Expr, child_cols: &[String]) -> String {
    #[allow(clippy::match_same_arms)]
    match expr {
        Expr::ColumnRef {
            table_alias: Some(tbl),
            column_name,
        } => {
            // Check if the child has a disambiguated column "tbl__col"
            let disambiguated = format!("{tbl}__{column_name}");
            if child_cols.contains(&disambiguated) {
                quote_ident(&disambiguated)
            } else {
                // Try nested join prefix: *__tbl__col
                let nested_suffix = format!("__{tbl}__{column_name}");
                let nested_match = child_cols.iter().find(|c| c.ends_with(&nested_suffix));
                if let Some(found) = nested_match {
                    quote_ident(found)
                } else if child_cols.contains(column_name) {
                    // No disambiguation needed — column name is unique
                    quote_ident(column_name)
                } else if child_cols.contains(tbl) && !child_cols.contains(column_name) {
                    // The "table" is actually a column in the child CTE — this
                    // happens with LATERAL SRF aliases (e.g., `e.value` where
                    // `e` is a single-column SRF output stored as column "e"
                    // in the child CTE). Resolve to the column name directly;
                    // for SRFs like jsonb_array_elements, `"e"` already holds
                    // the unwrapped value.
                    quote_ident(tbl)
                } else {
                    // Fallback: use original qualified form
                    expr.to_sql()
                }
            }
        }
        Expr::ColumnRef {
            table_alias: None,
            column_name,
        } => {
            if child_cols.contains(column_name) {
                quote_ident(column_name)
            } else {
                // Suffix match: find column ending in __column_name
                let suffix = format!("__{column_name}");
                let matches: Vec<&String> =
                    child_cols.iter().filter(|c| c.ends_with(&suffix)).collect();
                if matches.len() == 1 {
                    quote_ident(matches[0])
                } else {
                    expr.to_sql()
                }
            }
        }
        Expr::BinaryOp { op, left, right } => {
            let l = resolve_expr_to_child(left, child_cols);
            let r = resolve_expr_to_child(right, child_cols);
            format!("({l} {op} {r})")
        }
        Expr::FuncCall { func_name, args } => {
            let resolved_args: Vec<String> = args
                .iter()
                .map(|a| resolve_expr_to_child(a, child_cols))
                .collect();
            format!("{}({})", func_name, resolved_args.join(", "))
        }
        Expr::Raw(sql) => {
            // Best-effort: replace column refs in raw SQL
            crate::dvm::operators::filter::replace_column_refs_in_raw(sql, child_cols)
        }
        _ => expr.to_sql(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm::operators::test_helpers::*;

    #[test]
    fn test_diff_project_basic_columns() {
        let mut ctx = test_ctx();
        let child = scan(1, "t", "public", "t", &["id", "name", "amount"]);
        let tree = project(
            vec![colref("id"), colref("name")],
            vec!["id", "name"],
            child,
        );
        let result = diff_project(&mut ctx, &tree).unwrap();
        assert_eq!(result.columns, vec!["id", "name"]);
    }

    #[test]
    fn test_diff_project_passthrough_row_id() {
        let mut ctx = test_ctx();
        let child = scan(1, "t", "public", "t", &["id", "val"]);
        let tree = project(vec![colref("val")], vec!["val"], child);
        let result = diff_project(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Non-join child: __pgt_row_id passed through directly
        assert_sql_contains(&sql, "__pgt_row_id");
    }

    #[test]
    fn test_diff_project_alias_rename() {
        let mut ctx = test_ctx();
        let child = scan(1, "t", "public", "t", &["id", "amount"]);
        let tree = project(vec![colref("amount")], vec!["total"], child);
        let result = diff_project(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        assert_eq!(result.columns, vec!["total"]);
        assert_sql_contains(&sql, "\"total\"");
    }

    #[test]
    fn test_diff_project_preserves_dedup_flag() {
        let mut ctx = test_ctx();
        ctx.merge_safe_dedup = true;
        let child = scan_with_pk(1, "t", "public", "t", &["id", "val"], &["id"]);
        let tree = project(vec![colref("val")], vec!["val"], child);
        let result = diff_project(&mut ctx, &tree).unwrap();
        assert!(result.is_deduplicated);
    }

    #[test]
    fn test_diff_project_error_on_non_project_node() {
        let mut ctx = test_ctx();
        let tree = scan(1, "t", "public", "t", &["id"]);
        let result = diff_project(&mut ctx, &tree);
        assert!(result.is_err());
    }

    // ── resolve_expr_to_child tests ─────────────────────────────────

    #[test]
    fn test_resolve_expr_unqualified_column() {
        let child_cols = vec!["id".to_string(), "name".to_string()];
        let result = resolve_expr_to_child(&colref("id"), &child_cols);
        assert_eq!(result, "\"id\"");
    }

    #[test]
    fn test_resolve_expr_qualified_column_disambiguated() {
        let child_cols = vec!["l__id".to_string(), "r__id".to_string()];
        let result = resolve_expr_to_child(&qcolref("l", "id"), &child_cols);
        assert_eq!(result, "\"l__id\"");
    }

    #[test]
    fn test_resolve_expr_qualified_column_not_disambiguated() {
        let child_cols = vec!["id".to_string(), "name".to_string()];
        let result = resolve_expr_to_child(&qcolref("t", "id"), &child_cols);
        assert_eq!(result, "\"id\"");
    }

    #[test]
    fn test_resolve_expr_binary_op() {
        let child_cols = vec!["a".to_string(), "b".to_string()];
        let expr = binop("+", colref("a"), colref("b"));
        let result = resolve_expr_to_child(&expr, &child_cols);
        assert!(result.contains("\"a\""));
        assert!(result.contains("+"));
        assert!(result.contains("\"b\""));
    }

    #[test]
    fn test_resolve_expr_func_call() {
        let child_cols = vec!["x".to_string()];
        let expr = Expr::FuncCall {
            func_name: "upper".to_string(),
            args: vec![colref("x")],
        };
        let result = resolve_expr_to_child(&expr, &child_cols);
        assert_eq!(result, "upper(\"x\")");
    }

    // ── SF-6: Dual-count column forwarding ──────────────────────────

    #[test]
    fn test_diff_project_forwards_dual_count_columns() {
        // Project over Except: the count columns must be forwarded.
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["name"]);
        let right = scan(2, "b", "public", "b", &["name"]);
        let except_node = except(left, right, false);
        let tree = project(vec![colref("name")], vec!["name"], except_node);
        let result = diff_project(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Output columns must include the dual-count columns
        assert!(
            result.columns.contains(&"__pgt_count_l".to_string()),
            "Project over Except should forward __pgt_count_l: {:?}",
            result.columns
        );
        assert!(
            result.columns.contains(&"__pgt_count_r".to_string()),
            "Project over Except should forward __pgt_count_r: {:?}",
            result.columns
        );

        // The SQL CTE should SELECT the count columns
        assert_sql_contains(&sql, "__pgt_count_l");
        assert_sql_contains(&sql, "__pgt_count_r");
    }

    #[test]
    fn test_diff_project_no_count_columns_for_normal_child() {
        // Project over Scan: no count columns should appear.
        let mut ctx = test_ctx();
        let child = scan(1, "t", "public", "t", &["id", "name"]);
        let tree = project(
            vec![colref("id"), colref("name")],
            vec!["id", "name"],
            child,
        );
        let result = diff_project(&mut ctx, &tree).unwrap();

        assert_eq!(result.columns, vec!["id", "name"]);
        assert!(!result.columns.contains(&"__pgt_count_l".to_string()));
    }

    // ── SF-7: Aggregate auxiliary column forwarding ──────────────────

    #[test]
    fn test_diff_project_forwards_pgt_count_from_aggregate() {
        // Project over Aggregate: __pgt_count must be forwarded.
        let mut ctx = test_ctx_with_st("public", "my_st");
        ctx.st_user_columns = Some(vec!["region".to_string(), "cnt".to_string()]);
        ctx.st_has_pgt_count = true;

        let child = scan(1, "orders", "public", "o", &["id", "region"]);
        let agg = aggregate(vec![colref("region")], vec![count_star("cnt")], child);
        let tree = project(
            vec![colref("region"), colref("cnt")],
            vec!["region", "cnt"],
            agg,
        );
        let result = diff_project(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Output columns must include __pgt_count
        assert!(
            result.columns.contains(&"__pgt_count".to_string()),
            "Project over Aggregate should forward __pgt_count: {:?}",
            result.columns
        );

        // The SQL CTE should SELECT __pgt_count
        assert_sql_contains(&sql, "__pgt_count");
    }
}
