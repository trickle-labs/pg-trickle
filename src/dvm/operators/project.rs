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
use crate::dvm::operators::join_common::{has_source_alias, snapshot_join_column_name};
use crate::dvm::parser::{Expr, OpTree, join_pk_expr_indices, unwrap_transparent};
use crate::dvm::row_identity_domain;
use crate::dvm::schema::{ColumnProvenance, RelationColumn, RelationSchema};
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
    let differential_child = prune_unreferenced_false_left_joins(child, expressions);

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
        let child_out = differential_child.output_columns();
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
                            alias_map.insert(format!("__pgt_group_{pos}"), st_col.clone());
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
    let child_result = ctx.diff_node(differential_child)?;

    // Restore st_user_columns, alias map, and coalesce defaults
    ctx.st_user_columns = saved_st_cols;
    ctx.st_column_alias_map = saved_alias_map;
    ctx.agg_sum_coalesce_defaults = saved_coalesce_defaults;

    // The child CTE's column names. For join children, columns are
    // disambiguated as "table__col" (e.g., "l__id", "r__id").
    let child_cols = &child_result.columns;

    let cte_name = ctx.next_cte_name("project");

    // Look through transparent wrappers once for both column resolution and
    // row-identity selection.
    let unwrapped = unwrap_transparent(child);
    let resolution_child = unwrap_transparent(differential_child);
    let positional_columns = positional_project_columns(expressions, child_cols);
    let resolve_project_expr = |index: usize, expr: &Expr| {
        positional_columns
            .as_ref()
            .and_then(|columns| columns.get(index))
            .map_or_else(
                || resolve_expr_to_child_in_tree(expr, child_cols, resolution_child),
                |column| quote_ident(column),
            )
    };

    // Build projected column expressions.
    // Qualified column references (e.g., l.id) need to be resolved to
    // the child CTE's disambiguated column names (e.g., "l__id") since
    // the child is a single CTE, not multiple tables.
    let proj_cols: Vec<String> = expressions
        .iter()
        .zip(aliases.iter())
        .enumerate()
        .map(|(index, (expr, alias))| {
            let resolved_sql = resolve_project_expr(index, expr);
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
        let hash_indices: Vec<usize> = if pk_indices.is_empty() {
            // Fallback: hash all expressions (no PK info available)
            (0..expressions.len()).collect()
        } else {
            pk_indices
        };
        let hash_cols: Vec<String> = hash_indices
            .iter()
            .map(|&index| resolve_project_expr(index, &expressions[index]))
            .collect();
        let row_id_hash = if hash_cols.len() == 1 {
            format!(
                "pgtrickle.encode_row_id_v2('JOIN_KEY', ROW({}))",
                hash_cols[0]
            )
        } else {
            crate::dvm::operators::scan::build_hash_expr_for_domain("JOIN_KEY", &hash_cols)
        };
        format!("{row_id_hash} AS __pgt_row_id")
    } else if is_lateral_child || is_semijoin_child {
        // Hash all projected columns — matches row_id_key_columns()
        // which returns all aliases for lateral children and semi/anti-join
        // children.  For EXISTS/IN queries, this ensures the FULL refresh and
        // DIFFERENTIAL refresh produce identical __pgt_row_id values so that
        // MERGE can correctly DELETE rows that no longer satisfy the semi-join.
        let hash_cols: Vec<String> = expressions
            .iter()
            .enumerate()
            .map(|(index, expr)| resolve_project_expr(index, expr))
            .collect();
        format!(
            "{} AS __pgt_row_id",
            crate::dvm::operators::scan::build_hash_expr_for_domain(
                row_identity_domain(unwrapped),
                &hash_cols,
            )
        )
    } else {
        // Recompute __pgt_row_id from the projected key columns to ensure
        // consistency with the FULL refresh hash formula.
        //
        // For regular table sources with PK, this produces the same value
        // as the change buffer's row identity (both encode the same PK columns).
        // For ST sources without PK, the change buffer's row identity is the
        // upstream ST's row_id (a different formula), so recomputing here
        // ensures the MERGE ON clause can match rows correctly.
        match op.row_id_key_columns() {
            Some(key_cols) if !key_cols.is_empty() => {
                let hash_cols: Vec<String> = projection_key_indices(aliases, &key_cols)
                    .into_iter()
                    .flatten()
                    .map(|pos| resolve_project_expr(pos, &expressions[pos]))
                    .collect();
                if hash_cols.is_empty() || hash_cols.len() != key_cols.len() {
                    // Not all key columns could be resolved — fall back
                    "__pgt_row_id".to_string()
                } else {
                    format!(
                        "{} AS __pgt_row_id",
                        crate::dvm::operators::scan::build_hash_expr_for_domain(
                            row_identity_domain(op),
                            &hash_cols,
                        )
                    )
                }
            }
            Some(_) => "__pgt_row_id".to_string(),
            None => {
                // FULL refresh falls back to a complete visible-row identity
                // when a Project hides its child's key. Recompute that same
                // identity here instead of forwarding an incompatible child ID.
                let hash_cols: Vec<String> = expressions
                    .iter()
                    .enumerate()
                    .map(|(index, expr)| resolve_project_expr(index, expr))
                    .collect();
                format!(
                    "{} AS __pgt_row_id",
                    crate::dvm::operators::scan::build_hash_expr_for_domain(
                        "SYNTHETIC",
                        &hash_cols,
                    )
                )
            }
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
    let projects_target_row = ctx.is_top_level_diff_node()
        && ctx.st_has_pgt_count
        && ctx
            .st_user_columns
            .as_ref()
            .is_some_and(|columns| columns == aliases);
    let aux_cols: Vec<&String> = if projects_target_row {
        child_result
            .columns
            .iter()
            .filter(|c| {
                *c == "__pgt_count"
                    || c.starts_with("__pgt_aux_")
                    || c.starts_with("__pgt_nonnull_")
            })
            .collect()
    } else {
        Vec::new()
    };

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
    let from_source = positional_columns.as_ref().map_or_else(
        || child_cte.clone(),
        |columns| {
            let mut aliases = vec!["__pgt_row_id".to_string(), "__pgt_action".to_string()];
            aliases.extend(columns.iter().cloned());
            aliases.extend(child_cols.iter().skip(columns.len()).cloned());
            format!(
                "{child_cte} AS __pgt_project_input({})",
                aliases
                    .iter()
                    .map(|alias| quote_ident(alias))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        },
    );
    let sql_cap = row_id_select.len()
        + 20 // ", __pgt_action, "
        + proj_cols_str.len()
        + extra_cols_select.len()
        + 8  // "\nFROM "
        + from_source.len();
    let mut sql = String::with_capacity(sql_cap);
    let _ = write!(
        sql,
        "SELECT {row_id_select}, __pgt_action, {proj_cols_str}{extra_cols_select}\nFROM {from_source}",
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

    let mut schema = RelationSchema::from_names(aliases);
    for (index, ((expr, alias), output_column)) in expressions
        .iter()
        .zip(aliases)
        .zip(&mut schema.0)
        .enumerate()
    {
        if positional_columns.is_some()
            && let Some(source_column) = child_result.schema.0.get(index)
        {
            *output_column = source_column.clone();
            output_column.name = alias.clone();
            continue;
        }
        let Expr::ColumnRef {
            table_alias,
            column_name,
        } = expr
        else {
            continue;
        };
        let Some(source_name) = resolve_column_name_to_child(
            table_alias.as_deref(),
            column_name,
            child_cols,
            Some(resolution_child),
        ) else {
            continue;
        };
        let Some(source_column) = child_result
            .schema
            .0
            .iter()
            .find(|column| column.name == source_name)
        else {
            continue;
        };
        *output_column = source_column.clone();
        output_column.name = alias.clone();
    }
    for extra_name in output_cols.iter().skip(aliases.len()) {
        let mut column = child_result
            .schema
            .0
            .iter()
            .find(|column| column.name == *extra_name)
            .cloned()
            .unwrap_or_else(|| RelationColumn {
                name: extra_name.clone(),
                type_oid: 0,
                typmod: -1,
                nullable: true,
                provenance: ColumnProvenance::Internal,
            });
        column.name = extra_name.clone();
        schema.0.push(column);
    }

    Ok(DiffResult {
        cte_name,
        columns: output_cols.clone(),
        schema,
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
fn prune_unreferenced_false_left_joins<'a>(child: &'a OpTree, expressions: &[Expr]) -> &'a OpTree {
    let mut current = child;
    while let OpTree::LeftJoin {
        condition,
        left,
        right,
    } = current
    {
        if !is_literal_false(condition)
            || expressions
                .iter()
                .any(|expr| expression_may_reference_tree(expr, right))
        {
            break;
        }
        current = left;
    }
    current
}

fn is_literal_false(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(value) | Expr::Raw(value)
            if value.trim().eq_ignore_ascii_case("false")
    )
}

fn expression_may_reference_tree(expr: &Expr, tree: &OpTree) -> bool {
    match expr {
        Expr::ColumnRef {
            table_alias: Some(alias),
            ..
        }
        | Expr::Star {
            table_alias: Some(alias),
        } => has_source_alias(tree, alias),
        Expr::BinaryOp { left, right, .. } => {
            expression_may_reference_tree(left, tree) || expression_may_reference_tree(right, tree)
        }
        Expr::FuncCall { args, .. } => args
            .iter()
            .any(|arg| expression_may_reference_tree(arg, tree)),
        Expr::Literal(_) => false,
        Expr::Raw(sql) => raw_expression_may_reference_tree(sql, tree),
        // Unqualified columns and unqualified stars cannot be attributed
        // safely, so retain the join.
        Expr::ColumnRef {
            table_alias: None, ..
        }
        | Expr::Star { table_alias: None } => true,
    }
}

fn raw_expression_may_reference_tree(sql: &str, tree: &OpTree) -> bool {
    let mut has_right_qualified_ref = false;
    let _ =
        crate::dvm::operators::filter::replace_qualified_column_refs_with(sql, |table_alias, _| {
            if has_source_alias(tree, table_alias) {
                has_right_qualified_ref = true;
            }
            None
        });
    if has_right_qualified_ref {
        return true;
    }

    let right_columns = tree.output_columns();
    raw_has_unsafe_shape_or_unqualified_column(sql, &right_columns, tree)
}

fn raw_has_unsafe_shape_or_unqualified_column(
    sql: &str,
    columns: &[String],
    tree: &OpTree,
) -> bool {
    let chars = sql.chars().collect::<Vec<_>>();
    let mut index = 0;
    let mut in_string = false;
    while index < chars.len() {
        if chars[index] == '\'' {
            if in_string && index + 1 < chars.len() && chars[index + 1] == '\'' {
                index += 2;
                continue;
            }
            in_string = !in_string;
            index += 1;
            continue;
        }
        if in_string {
            index += 1;
            continue;
        }
        let start = index;
        let identifier = if chars[index] == '"' {
            index += 1;
            let mut identifier = String::new();
            while index < chars.len() {
                if chars[index] == '"' {
                    if index + 1 < chars.len() && chars[index + 1] == '"' {
                        identifier.push('"');
                        index += 2;
                    } else {
                        index += 1;
                        break;
                    }
                } else {
                    identifier.push(chars[index]);
                    index += 1;
                }
            }
            Some(identifier)
        } else if chars[index].is_ascii_alphabetic() || chars[index] == '_' {
            index += 1;
            while index < chars.len()
                && (chars[index].is_ascii_alphanumeric() || chars[index] == '_')
            {
                index += 1;
            }
            Some(chars[start..index].iter().collect::<String>())
        } else {
            index += 1;
            None
        };
        let Some(identifier) = identifier else {
            continue;
        };

        let previous = chars[..start]
            .iter()
            .rev()
            .find(|character| !character.is_whitespace());
        let next_position = chars[index..]
            .iter()
            .position(|character| !character.is_whitespace())
            .map(|offset| index + offset);
        let next = next_position.map(|position| &chars[position]);
        let has_right_qualified_star = next_position.is_some_and(|dot_position| {
            chars[dot_position] == '.'
                && has_source_alias(tree, &identifier)
                && chars[dot_position + 1..]
                    .iter()
                    .find(|character| !character.is_whitespace())
                    == Some(&'*')
        });
        if has_right_qualified_star {
            return true;
        }
        let is_qualified = previous == Some(&'.') || next == Some(&'.');
        if !is_qualified && columns.iter().any(|column| column == &identifier) {
            return true;
        }
    }
    false
}

fn positional_project_columns(expressions: &[Expr], child_cols: &[String]) -> Option<Vec<String>> {
    if expressions.len() > child_cols.len() {
        return None;
    }
    let projected_child_cols = &child_cols[..expressions.len()];
    let has_duplicates = projected_child_cols
        .iter()
        .enumerate()
        .any(|(index, column)| projected_child_cols[..index].contains(column));
    if !has_duplicates
        || !expressions
            .iter()
            .zip(projected_child_cols)
            .all(|(expr, child_column)| {
                matches!(
                    expr,
                    Expr::ColumnRef {
                        table_alias: None,
                        column_name,
                    } if column_name == child_column
                )
            })
    {
        return None;
    }

    Some(
        (0..expressions.len())
            .map(|index| format!("__pgt_project_col_{index}"))
            .collect(),
    )
}

fn projection_key_indices(aliases: &[String], key_columns: &[String]) -> Option<Vec<usize>> {
    let mut used = vec![false; aliases.len()];
    key_columns
        .iter()
        .map(|key| {
            let index = aliases
                .iter()
                .enumerate()
                .find_map(|(index, alias)| (!used[index] && alias == key).then_some(index))?;
            used[index] = true;
            Some(index)
        })
        .collect()
}

fn resolve_expr_to_child(expr: &Expr, child_cols: &[String]) -> String {
    resolve_expr_to_child_inner(expr, child_cols, None)
}

fn resolve_expr_to_child_in_tree(expr: &Expr, child_cols: &[String], child: &OpTree) -> String {
    resolve_expr_to_child_inner(expr, child_cols, Some(child))
}

fn resolve_expr_to_child_inner(
    expr: &Expr,
    child_cols: &[String],
    child: Option<&OpTree>,
) -> String {
    #[allow(clippy::match_same_arms)]
    match expr {
        Expr::ColumnRef {
            table_alias,
            column_name,
        } => resolve_column_name_to_child(table_alias.as_deref(), column_name, child_cols, child)
            .map_or_else(|| expr.to_sql(), |name| quote_ident(&name)),
        Expr::BinaryOp { op, left, right } => {
            let l = resolve_expr_to_child_inner(left, child_cols, child);
            let r = resolve_expr_to_child_inner(right, child_cols, child);
            format!("({l} {op} {r})")
        }
        Expr::FuncCall { func_name, args } => {
            let resolved_args: Vec<String> = args
                .iter()
                .map(|a| resolve_expr_to_child_inner(a, child_cols, child))
                .collect();
            format!("{}({})", func_name, resolved_args.join(", "))
        }
        Expr::Raw(sql) => {
            // Casts and other parser expressions may be represented as Raw
            // SQL. Resolve their qualified references through the operator
            // tree before applying the generic flattened-column rewrite.
            let tree_resolved = child.map_or_else(
                || sql.clone(),
                |tree| {
                    crate::dvm::operators::filter::replace_qualified_column_refs_with(
                        sql,
                        |table_alias, column_name| {
                            resolve_source_column(tree, table_alias, column_name)
                                .filter(|resolved| child_cols.contains(resolved))
                                .map(|resolved| quote_ident(&resolved))
                        },
                    )
                },
            );
            crate::dvm::operators::filter::replace_column_refs_in_raw(&tree_resolved, child_cols)
        }
        _ => expr.to_sql(),
    }
}

fn resolve_column_name_to_child(
    table_alias: Option<&str>,
    column_name: &str,
    child_cols: &[String],
    child: Option<&OpTree>,
) -> Option<String> {
    if let Some(table_alias) = table_alias {
        let disambiguated = snapshot_join_column_name(table_alias, column_name);
        if child_cols.contains(&disambiguated) {
            return Some(disambiguated);
        }

        let nested_suffix = format!("__{table_alias}__{column_name}");
        if let Some(found) = child_cols
            .iter()
            .find(|column| column.ends_with(&nested_suffix))
        {
            return Some(found.clone());
        }

        if let Some(found) = child
            .and_then(|tree| resolve_source_column(tree, table_alias, column_name))
            .filter(|resolved| child_cols.contains(resolved))
        {
            return Some(found);
        }

        if child_cols.contains(&column_name.to_string()) {
            return Some(column_name.to_string());
        }

        // A single-column SRF may expose its table alias as the flattened
        // child column, for example `e.value` stored as column `e`.
        if child_cols.contains(&table_alias.to_string())
            && !child_cols.contains(&column_name.to_string())
        {
            return Some(table_alias.to_string());
        }

        return None;
    }

    if child_cols.contains(&column_name.to_string()) {
        return Some(column_name.to_string());
    }

    let suffix = format!("__{column_name}");
    let mut matches = child_cols.iter().filter(|column| column.ends_with(&suffix));
    let found = matches.next()?;
    matches.next().is_none().then(|| found.clone())
}

/// Map an original table alias to its column name in a differentiated child.
/// Join nodes prefix each child's output with that child's plan alias. A
/// LATERAL function flattens its outer child's columns under the function
/// alias, so `r.id` becomes `n__id` when `r CROSS JOIN LATERAL ... n` is a
/// join child.
fn resolve_source_column(op: &OpTree, table_alias: &str, column_name: &str) -> Option<String> {
    match op {
        OpTree::Scan { alias, .. } | OpTree::CteScan { alias, .. } if alias == table_alias => {
            Some(column_name.to_string())
        }
        OpTree::LateralFunction { alias, child, .. } => {
            if alias == table_alias
                || resolve_source_column(child, table_alias, column_name).is_some()
            {
                Some(column_name.to_string())
            } else {
                None
            }
        }
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. } => {
            if let Some(column) = resolve_source_column(left, table_alias, column_name) {
                Some(snapshot_join_column_name(left.alias(), &column))
            } else {
                resolve_source_column(right, table_alias, column_name)
                    .map(|column| snapshot_join_column_name(right.alias(), &column))
            }
        }
        OpTree::Filter { child, .. } => resolve_source_column(child, table_alias, column_name),
        OpTree::Subquery {
            alias,
            column_aliases,
            child,
        } => {
            if alias == table_alias
                && (column_aliases.is_empty() || column_aliases.iter().any(|c| c == column_name))
            {
                Some(column_name.to_string())
            } else {
                resolve_source_column(child, table_alias, column_name)
            }
        }
        OpTree::Project {
            expressions,
            aliases,
            ..
        } => expressions
            .iter()
            .position(|expr| {
                matches!(
                    expr,
                    Expr::ColumnRef {
                        table_alias: Some(alias),
                        column_name: name,
                    } if alias == table_alias && name == column_name
                )
            })
            .and_then(|position| aliases.get(position).cloned()),
        _ => None,
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
    fn test_diff_project_computed_mdm_source_key_is_per_source_row() {
        let mut ctx = test_ctx();
        let child = scan_with_pk(
            1,
            "mdm_crm",
            "public",
            "mdm_crm",
            &["id", "name", "deleted"],
            &["id"],
        );
        let tree = project(
            vec![
                Expr::Raw("'crm'::text".into()),
                Expr::FuncCall {
                    func_name: "pgtrickle.encode_row_id_v2".into(),
                    args: vec![
                        Expr::Raw("'MDM_SOURCE_KEY_V1'".into()),
                        Expr::Raw("ROW((SELECT entity_id FROM identity_map), id)".into()),
                    ],
                },
                colref("name"),
            ],
            vec!["source_name", "source_record_key", "name"],
            child,
        );

        let result = diff_project(&mut ctx, &tree).unwrap();
        let sql = ctx.cte_sql(&result.cte_name).unwrap();
        let row_id = sql.split(" AS __pgt_row_id").next().unwrap_or(sql);

        assert!(row_id.contains("encode_row_id_v2('SCAN_KEY'"), "{row_id}");
        assert!(
            row_id.contains("encode_row_id_v2('MDM_SOURCE_KEY_V1'"),
            "{row_id}"
        );
        assert!(
            row_id.contains("ROW((SELECT entity_id FROM identity_map), id)"),
            "{row_id}"
        );
        assert!(!row_id.contains("'crm'::text"), "{row_id}");
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
    fn test_diff_project_over_table_srf_matches_full_refresh_identity() {
        let mut ctx = test_ctx_with_st("public", "normalized");
        let child = OpTree::LateralFunction {
            func_sql: "normalize_text(r.raw_value)".into(),
            alias: "n".into(),
            column_aliases: vec![],
            declared_columns: vec![
                crate::dvm::parser::Column {
                    name: "canonical_value".into(),
                    type_oid: 25,
                    is_nullable: true,
                },
                crate::dvm::parser::Column {
                    name: "normalized_state".into(),
                    type_oid: 25,
                    is_nullable: true,
                },
            ],
            with_ordinality: false,
            child: Box::new(scan_with_pk(
                1,
                "records",
                "public",
                "r",
                &["id", "source_record_id", "raw_value"],
                &["id"],
            )),
        };
        let tree = project(
            vec![
                qcolref("r", "id"),
                qcolref("r", "source_record_id"),
                qcolref("n", "canonical_value"),
                qcolref("n", "normalized_state"),
            ],
            vec![
                "id",
                "source_record_id",
                "canonical_value",
                "normalized_state",
            ],
            child,
        );

        // FULL refresh obtains this same visible-column key from the plan.
        assert_eq!(
            tree.row_id_key_columns(),
            Some(vec![
                "id".to_string(),
                "source_record_id".to_string(),
                "canonical_value".to_string(),
                "normalized_state".to_string(),
            ])
        );

        let result = diff_project(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);
        assert_sql_contains(&sql, "encode_row_id_v2('SCAN_KEY'");
        for column in [
            "id",
            "source_record_id",
            "canonical_value",
            "normalized_state",
        ] {
            assert_sql_contains(&sql, &quote_ident(column));
        }
    }

    #[test]
    fn test_diff_project_resolves_outer_alias_through_lateral_join_chain() {
        let lateral = OpTree::LateralFunction {
            func_sql: "normalize_text(r.raw_value)".into(),
            alias: "n".into(),
            column_aliases: vec!["state".into(), "normalized".into()],
            declared_columns: vec![],
            with_ordinality: false,
            child: Box::new(scan(
                1,
                "records",
                "public",
                "r",
                &["source_record_key", "name", "row_changed_at", "raw_value"],
            )),
        };
        let with_records = inner_join(
            eq_cond("r", "source_record_key", "sr", "source_record_key"),
            lateral,
            scan(
                2,
                "source_records",
                "public",
                "sr",
                &[
                    "source_record_key",
                    "source_record_id",
                    "source_identity_id",
                ],
            ),
        );
        let with_identity = inner_join(
            eq_cond("sr", "source_identity_id", "si", "source_identity_id"),
            with_records,
            scan(
                3,
                "source_identity_map",
                "public",
                "si",
                &["source_identity_id", "source_name"],
            ),
        );
        let tree = project(
            vec![
                qcolref("r", "source_record_key"),
                qcolref("sr", "source_record_id"),
                qcolref("n", "state"),
                Expr::Raw("CAST(\"r\".\"name\" AS text)".into()),
                Expr::Raw("CAST(\"r\".\"row_changed_at\" AS timestamptz)".into()),
            ],
            vec![
                "source_record_key",
                "source_record_id",
                "state",
                "raw_value",
                "row_changed_at",
            ],
            with_identity,
        );
        let mut ctx = test_ctx_with_st("public", "normalized");

        let result = diff_project(&mut ctx, &tree).unwrap();
        let project_sql = ctx.cte_sql(&result.cte_name).unwrap();

        assert_sql_contains(project_sql, "\"join__n__source_record_key\"");
        assert_sql_contains(project_sql, "\"join__sr__source_record_id\"");
        assert_sql_contains(project_sql, "\"join__n__state\"");
        assert_sql_contains(project_sql, "CAST(\"join__n__name\" AS text)");
        assert_sql_contains(
            project_sql,
            "CAST(\"join__n__row_changed_at\" AS timestamptz)",
        );
        assert!(!project_sql.contains("\"r\"."), "{project_sql}");
    }

    #[test]
    fn test_diff_project_resolves_subquery_alias_through_deep_left_join_chain() {
        let mut child = subquery(
            "evidence",
            vec![],
            scan(
                1,
                "evidence_branch",
                "public",
                "branch",
                &[
                    "left_source_record_id",
                    "right_source_record_id",
                    "comparator",
                    "comparator_version",
                    "left_value_digest",
                ],
            ),
        );
        for index in 0..18 {
            let alias = format!("dependency_{index}");
            child = left_join(
                Expr::Raw("FALSE".into()),
                child,
                scan(100 + index, &alias, "public", &alias, &["dependency_value"]),
            );
        }
        let expressions = vec![
            qcolref("evidence", "left_source_record_id"),
            qcolref("evidence", "right_source_record_id"),
            qcolref("evidence", "comparator"),
            qcolref("evidence", "comparator_version"),
            Expr::Raw(
                "CASE WHEN mdm_graph.normalized_levenshtein_score(\"evidence\".\"comparator\", \"evidence\".\"comparator\", (SELECT COALESCE(((\"d\".\"expanded_definition\" -> 'limits') ->> 'max_comparator_work')::bigint, 1000000::bigint) + COUNT(*) OVER () * 0 FROM mdm_graph.definition_limits AS d WHERE \"d\".\"entity_name\" = 'person'::text)) > 0 THEN mdm_graph.evidence_digest(\"evidence\".\"left_source_record_id\") ELSE \"evidence\".\"left_value_digest\" END"
                    .into(),
            ),
        ];
        let aliases = vec![
            "left_source_record_id",
            "right_source_record_id",
            "comparator",
            "comparator_version",
            "left_value_digest",
        ];
        let tree = project(expressions, aliases, child);

        let mut ctx = test_ctx();
        let result = diff_project(&mut ctx, &tree).expect("deep project must diff");
        let full_sql = ctx.build_with_query(&result.cte_name);
        let project_sql = ctx
            .cte_sql(&result.cte_name)
            .expect("project CTE must exist");
        assert!(!project_sql.contains("\"evidence\"."), "{project_sql}");
        for column in [
            "left_source_record_id",
            "right_source_record_id",
            "comparator",
            "comparator_version",
            "left_value_digest",
        ] {
            let quoted = quote_ident(column);
            assert!(
                project_sql.matches(&quoted).count() >= 2,
                "missing {column} in {project_sql}"
            );
        }
        assert!(!full_sql.contains("dependency_0"), "{full_sql}");
        assert!(!full_sql.contains("dependency_17"), "{full_sql}");
        assert!(
            full_sql.len() < 20_000,
            "SQL grew to {} bytes",
            full_sql.len()
        );
    }

    #[test]
    fn test_false_left_join_pruning_keeps_raw_right_references() {
        let dependency = scan(
            2,
            "dependency",
            "public",
            "dependency_0",
            &["dependency_value"],
        );
        assert!(raw_expression_may_reference_tree(
            "CASE WHEN \"dependency_0\".\"dependency_value\" IS NULL THEN 'x' ELSE 'y' END",
            &dependency,
        ));
        assert!(raw_expression_may_reference_tree(
            "COALESCE(dependency_value, 'x')",
            &dependency,
        ));
        assert!(!raw_expression_may_reference_tree(
            "EXISTS (SELECT 1 FROM elsewhere)",
            &dependency,
        ));
        assert!(!raw_expression_may_reference_tree(
            "(SELECT COUNT(*) FROM elsewhere) + 2 * 3",
            &dependency,
        ));
        assert!(raw_expression_may_reference_tree(
            "EXISTS (SELECT 1 FROM elsewhere WHERE \"dependency_0\".\"dependency_value\" IS NOT NULL)",
            &dependency,
        ));
        assert!(raw_expression_may_reference_tree(
            "ROW(\"dependency_0\".*)",
            &dependency,
        ));
        assert!(!raw_expression_may_reference_tree(
            "CASE WHEN \"evidence\".\"state\" = 'value' THEN mdm_graph.evidence_digest(\"evidence\".\"normalized\") END",
            &dependency,
        ));
    }

    #[test]
    fn test_diff_project_preserves_qualified_types_through_three_way_join() {
        let blocks = |oid, table: &str, alias: &str| OpTree::Scan {
            table_oid: oid,
            table_name: table.into(),
            schema: "public".into(),
            columns: vec![
                crate::dvm::parser::Column {
                    name: "source_record_id".into(),
                    type_oid: 2950,
                    is_nullable: false,
                },
                crate::dvm::parser::Column {
                    name: "channel_id".into(),
                    type_oid: 25,
                    is_nullable: false,
                },
                crate::dvm::parser::Column {
                    name: "block_key".into(),
                    type_oid: 17,
                    is_nullable: false,
                },
                crate::dvm::parser::Column {
                    name: "source_sort_key".into(),
                    type_oid: 17,
                    is_nullable: false,
                },
            ],
            pk_columns: vec![],
            alias: alias.into(),
        };
        let stats = OpTree::Scan {
            table_oid: 2,
            table_name: "block_stats".into(),
            schema: "public".into(),
            columns: vec![
                crate::dvm::parser::Column {
                    name: "channel_id".into(),
                    type_oid: 25,
                    is_nullable: false,
                },
                crate::dvm::parser::Column {
                    name: "block_key".into(),
                    type_oid: 17,
                    is_nullable: false,
                },
            ],
            pk_columns: vec![],
            alias: "s".into(),
        };
        let left_with_stats = inner_join(
            eq_cond("l", "channel_id", "s", "channel_id"),
            blocks(1, "blocks_left", "l"),
            stats,
        );
        let candidate_pairs = inner_join(
            eq_cond("l", "channel_id", "r", "channel_id"),
            left_with_stats,
            blocks(3, "blocks_right", "r"),
        );
        let tree = project(
            vec![
                qcolref("l", "source_record_id"),
                qcolref("r", "source_record_id"),
                qcolref("l", "source_sort_key"),
                qcolref("r", "source_sort_key"),
            ],
            vec![
                "left_source_record_id",
                "right_source_record_id",
                "left_sort_key",
                "right_sort_key",
            ],
            candidate_pairs,
        );
        let mut ctx = test_ctx();

        let result = diff_project(&mut ctx, &tree).unwrap();

        assert_eq!(result.schema.names(), result.columns);
        assert_eq!(
            result
                .schema
                .0
                .iter()
                .map(|column| column.type_oid)
                .collect::<Vec<_>>(),
            vec![2950, 2950, 17, 17]
        );
    }

    #[test]
    fn test_diff_project_reads_duplicate_child_columns_positionally() {
        let duplicate_child = project(
            vec![colref("left_id"), colref("right_id")],
            vec!["source_record_id", "source_record_id"],
            scan(1, "pairs", "public", "p", &["left_id", "right_id"]),
        );
        let tree = project(
            vec![colref("source_record_id"), colref("source_record_id")],
            vec!["left_source_record_id", "right_source_record_id"],
            duplicate_child,
        );
        let mut ctx = test_ctx();

        assert_eq!(
            tree.row_id_key_columns(),
            Some(vec![
                "left_source_record_id".to_string(),
                "right_source_record_id".to_string(),
            ])
        );
        let result = diff_project(&mut ctx, &tree).unwrap();
        let sql = ctx.cte_sql(&result.cte_name).unwrap();

        assert!(sql.contains("AS __pgt_project_input("), "{sql}");
        assert!(
            sql.contains("\"__pgt_project_col_0\" AS \"left_source_record_id\""),
            "{sql}"
        );
        assert!(
            sql.contains("\"__pgt_project_col_1\" AS \"right_source_record_id\""),
            "{sql}"
        );
        assert!(
            !sql.contains("SELECT \"source_record_id\" AS \"left_source_record_id\""),
            "{sql}"
        );
        assert!(
            sql.contains("ROW((\"__pgt_project_col_0\"), (\"__pgt_project_col_1\"))"),
            "{sql}"
        );
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

    #[test]
    fn test_diff_project_does_not_leak_aggregate_count_into_intermediate_shape() {
        let mut ctx = test_ctx();
        let child = scan(1, "pairs", "public", "p", &["left_id", "right_id"]);
        let agg = aggregate(vec![colref("left_id"), colref("right_id")], vec![], child);
        let tree = project(
            vec![colref("left_id"), colref("right_id")],
            vec!["left_source_record_id", "right_source_record_id"],
            agg,
        );

        let result = diff_project(&mut ctx, &tree).unwrap();
        let sql = ctx.cte_sql(&result.cte_name).unwrap();

        assert_eq!(
            result.columns,
            vec!["left_source_record_id", "right_source_record_id"]
        );
        assert!(!sql.contains("__pgt_count"), "{sql}");
    }
}
