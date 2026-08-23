//! GROUP BY aggregate differentiation.
//!
//! Uses auxiliary counters (count, sum) stored alongside user data
//! to maintain aggregate values incrementally.
//!
//! For each group, computes the net change from inserts/deletes in the child
//! delta, then merges with existing ST state to determine if the group:
//! - Appears (new_count > 0, was 0) → INSERT
//! - Vanishes (new_count ≤ 0, was > 0) → DELETE
//! - Changes value → UPDATE (emitted as DELETE + INSERT pair)

use crate::dvm::diff::{DeltaSource, DiffContext, DiffResult, quote_ident};
use crate::dvm::operators::scan::build_hash_expr;
use crate::dvm::parser::{AggExpr, AggFunc, CteRegistry, Expr, OpTree};
use crate::error::PgTrickleError;

/// Resolve a column reference expression against child CTE column names.
///
/// For a qualified `ColumnRef("c", "region")`, checks if `c__region`
/// exists in child_cols (from join disambiguation). Falls back to
/// the unqualified name, then to the original expression.
///
/// Returns an UNQUOTED column name — callers handle quoting.
fn resolve_col_for_child(expr: &Expr, child_cols: &[String]) -> String {
    match expr {
        Expr::ColumnRef {
            table_alias: Some(tbl),
            column_name,
        } => {
            // Direct disambiguated: tbl__col
            let disambiguated = format!("{tbl}__{column_name}");
            if child_cols.contains(&disambiguated) {
                return disambiguated;
            }
            // Nested join prefix: *__tbl__col
            let nested_suffix = format!("__{tbl}__{column_name}");
            for c in child_cols {
                if c.ends_with(&nested_suffix) {
                    return c.clone();
                }
            }
            // Exact match on column name alone
            if child_cols.contains(column_name) {
                return column_name.clone();
            }
            column_name.clone()
        }
        Expr::ColumnRef {
            table_alias: None,
            column_name,
        } => {
            // Exact match
            if child_cols.contains(column_name) {
                return column_name.clone();
            }
            // Suffix match: find column ending in __column_name
            let suffix = format!("__{column_name}");
            let matches: Vec<&String> =
                child_cols.iter().filter(|c| c.ends_with(&suffix)).collect();
            if matches.len() == 1 {
                return matches[0].clone();
            }
            column_name.clone()
        }
        _ => expr.strip_qualifier().to_sql(),
    }
}

/// Resolve a group-by expression for the child CTE's column names.
///
/// Uses `resolve_expr_for_child` so that compound expressions such as
/// `COALESCE(l.dept, r.dept)` are recursed into, producing
/// `COALESCE(l__dept, r__dept)` rather than the incorrect
/// `COALESCE(dept, dept)` that `resolve_col_for_child` would emit.
fn resolve_group_col(expr: &Expr, child_cols: &[String]) -> String {
    resolve_expr_for_child(expr, child_cols)
}

/// Return a SQL fragment that can be safely embedded in a SELECT list or
/// GROUP BY clause.
///
/// Simple column names (no parentheses) are double-quoted as identifiers.
/// Compound SQL expressions (containing `(`) are returned as-is — quoting
/// them as identifiers would cause PostgreSQL to look for a column whose
/// name is literally the expression string.
fn col_ref_or_sql_expr(s: &str) -> String {
    if s.contains('(') {
        s.to_string()
    } else {
        quote_ident(s)
    }
}

/// Resolve an entire expression tree against child CTE column names.
///
/// Recursively rewrites `ColumnRef` nodes using `resolve_col_for_child`
/// and rebuilds the surrounding expression in SQL syntax. Used for FILTER
/// clauses where the predicate may contain arbitrary expressions.
fn resolve_expr_for_child(expr: &Expr, child_cols: &[String]) -> String {
    match expr {
        Expr::ColumnRef { .. } => resolve_col_for_child(expr, child_cols),
        Expr::BinaryOp { op, left, right } => {
            format!(
                "({} {op} {})",
                resolve_expr_for_child(left, child_cols),
                resolve_expr_for_child(right, child_cols),
            )
        }
        Expr::FuncCall { func_name, args } => {
            let resolved_args: Vec<String> = args
                .iter()
                .map(|a| resolve_expr_for_child(a, child_cols))
                .collect();
            format!("{func_name}({})", resolved_args.join(", "))
        }
        Expr::Raw(sql) => {
            // Best-effort: replace column refs in raw SQL
            crate::dvm::operators::filter::replace_column_refs_in_raw(sql, child_cols)
        }
        _ => expr.to_sql(),
    }
}

// ── Group-rescan helpers ────────────────────────────────────────────

/// Reconstruct the FROM clause SQL from a child OpTree.
///
/// Returns the SQL fragment for `FROM ...` suitable for the rescan CTE.
/// Returns `None` for complex children (CTEs, subqueries, unions) that
/// cannot be reconstructed reliably.
///
/// The `registry` is used to resolve [`OpTree::CteScan`] nodes: instead of
/// emitting the CTE alias as a bare table reference (which would fail at
/// SQL execution time for chained CTEs that are not real relations), we
/// recursively resolve the CTE body and emit its underlying source.
fn child_to_from_sql(child: &OpTree, registry: &CteRegistry) -> Option<String> {
    match child {
        OpTree::Scan {
            schema,
            table_name,
            alias,
            columns,
            ..
        } => {
            let columns = columns
                .iter()
                .map(|column| quote_ident(&column.name))
                .collect::<Vec<_>>()
                .join(", ");
            Some(format!(
                "(SELECT {columns} FROM {}.{}) AS {}",
                quote_ident(schema),
                quote_ident(table_name),
                quote_ident(alias),
            ))
        }
        OpTree::Filter { predicate, child } => {
            let inner = child_to_from_sql(child, registry)?;
            Some(format!("{inner} WHERE {}", predicate.to_sql()))
        }
        OpTree::InnerJoin {
            condition,
            left,
            right,
        } => {
            let l = child_to_from_sql(left, registry)?;
            let r = child_to_from_sql(right, registry)?;
            Some(format!("{l} INNER JOIN {r} ON {}", condition.to_sql()))
        }
        OpTree::LeftJoin {
            condition,
            left,
            right,
        } => {
            let l = child_to_from_sql(left, registry)?;
            let r = child_to_from_sql(right, registry)?;
            Some(format!("{l} LEFT JOIN {r} ON {}", condition.to_sql()))
        }
        OpTree::FullJoin {
            condition,
            left,
            right,
        } => {
            let l = child_to_from_sql(left, registry)?;
            let r = child_to_from_sql(right, registry)?;
            Some(format!("{l} FULL JOIN {r} ON {}", condition.to_sql()))
        }
        OpTree::Project {
            expressions,
            aliases,
            child,
        } => {
            // Wrap the child's FROM in a subquery that applies the
            // projection, so aggregate expressions and GROUP BY clauses
            // that reference the Project's output aliases (e.g., `o_year`
            // from `EXTRACT(year FROM o_orderdate) AS o_year`) resolve
            // correctly in the rescan CTE.
            let inner = child_to_from_sql(child, registry)?;
            let select_items: Vec<String> = expressions
                .iter()
                .zip(aliases.iter())
                .map(|(expr, alias)| {
                    let sql = expr.to_sql();
                    if sql == *alias {
                        quote_ident(alias)
                    } else {
                        format!("{} AS {}", sql, quote_ident(alias))
                    }
                })
                .collect();
            Some(format!(
                "(SELECT {} FROM {}) __pgt_proj",
                select_items.join(", "),
                inner,
            ))
        }
        OpTree::CteScan { cte_id, .. } => {
            // Resolve the CTE body via the registry and recursively determine
            // the FROM clause. This handles chained CTEs where the aggregate's
            // child is a CTE alias (e.g., `FROM raw`) that is not a real
            // relation — using the alias directly would cause a PostgreSQL
            // "relation does not exist" error in the rescan CTE.
            registry
                .get(*cte_id)
                .and_then(|(_, body)| child_to_from_sql(body, registry))
        }
        OpTree::Subquery {
            child,
            alias,
            column_aliases,
        } => {
            // Only recurse into Subquery when the child is an Aggregate.
            // An Aggregate child produces a complete subquery expression
            // (SELECT ... GROUP BY ...) that can be aliased and used as FROM.
            //
            // For other child types (Project, Filter over joins, etc.),
            // return None so callers fall back to the defining-query approach.
            // This is important because Project nodes rename columns with
            // aliases (e.g., `extract(year from o_orderdate) AS o_year`)
            // that are lost when child_to_from_sql recurses through them.
            match child.as_ref() {
                OpTree::Aggregate { .. } => {
                    let inner = child_to_from_sql(child, registry)?;
                    if column_aliases.is_empty() {
                        Some(format!("{inner} AS {}", quote_ident(alias)))
                    } else {
                        // Apply positional column aliases using PostgreSQL's
                        // AS alias(col1, col2) syntax to match the delta's
                        // renamed columns from diff_subquery.
                        let col_list: Vec<String> =
                            column_aliases.iter().map(|a| quote_ident(a)).collect();
                        Some(format!(
                            "{inner} AS {}({})",
                            quote_ident(alias),
                            col_list.join(", ")
                        ))
                    }
                }
                _ => None,
            }
        }
        OpTree::Aggregate {
            group_by,
            aggregates,
            child,
            ..
        } => {
            let inner_from = child_to_from_sql(child, registry)?;
            let mut selects = Vec::new();
            for expr in group_by {
                selects.push(expr.to_sql());
            }
            // NOTE: do NOT include COUNT(*) AS __pgt_count here.
            // The intermediate aggregate delta output_cols excludes __pgt_count
            // (it's internal bookkeeping), so the child_to_from_sql must match.
            // Including it here would cause column count mismatches in the
            // EXCEPT ALL between SELECT * FROM this subquery and the delta CTE.
            for agg in aggregates {
                selects.push(format!(
                    "{} AS {}",
                    agg_to_rescan_sql(agg),
                    quote_ident(&agg.alias),
                ));
            }
            let gb = if group_by.is_empty() {
                String::new()
            } else {
                let cols: Vec<String> = group_by.iter().map(|e| e.to_sql()).collect();
                format!(" GROUP BY {}", cols.join(", "))
            };
            Some(format!(
                "(SELECT {} FROM {inner_from}{})",
                selects.join(", "),
                gb,
            ))
        }
        _ => None,
    }
}

/// Reconstruct an aggregate function call as SQL text for the rescan CTE.
///
/// Handles regular aggregates (`BIT_AND(flags)`), aggregates with DISTINCT,
/// FILTER clauses, second arguments (`STRING_AGG(name, ', ')`), and
/// ordered-set aggregates (`MODE() WITHIN GROUP (ORDER BY amount)`).
///
/// SQL/JSON standard aggregates (`JSON_OBJECTAGG`, `JSON_ARRAYAGG`) carry
/// their full deparsed SQL in the `AggFunc` variant and are returned directly.
pub fn agg_to_rescan_sql(agg: &AggExpr) -> String {
    // SQL/JSON standard aggregates: use the stored raw SQL directly since
    // their special syntax (key: value, ABSENT ON NULL, etc.) cannot be
    // reconstructed from function name + arguments.
    match &agg.function {
        AggFunc::JsonObjectAggStd(raw) | AggFunc::JsonArrayAggStd(raw) => {
            let filter = match &agg.filter {
                Some(f) => format!(" FILTER (WHERE {})", f.to_sql()),
                None => String::new(),
            };
            return format!("{raw}{filter}");
        }
        AggFunc::ComplexExpression(raw) | AggFunc::UserDefined(raw) => {
            // Complex expression / user-defined aggregate: the stored raw SQL
            // already contains the full call. The rescan CTE evaluates it
            // against source data directly.
            return raw.clone();
        }
        _ => {}
    }

    let func_name = agg.function.sql_name();
    let distinct_str = if agg.is_distinct { "DISTINCT " } else { "" };

    // Build argument list
    let mut arg_parts = Vec::new();
    if matches!(agg.function, AggFunc::CountStar) {
        arg_parts.push("*".to_string());
    } else if let Some(ref arg) = agg.argument {
        arg_parts.push(format!("{distinct_str}{}", arg.to_sql()));
    }
    if let Some(ref second) = agg.second_arg {
        arg_parts.push(second.to_sql());
    }

    let args_sql = arg_parts.join(", ");

    // Ordered-set aggregates (MODE, PERCENTILE_*) use WITHIN GROUP (ORDER BY ...).
    // Regular aggregates (STRING_AGG, ARRAY_AGG, etc.) use ORDER BY inside parens.
    let is_ordered_set = matches!(
        agg.function,
        AggFunc::Mode
            | AggFunc::PercentileCont
            | AggFunc::PercentileDisc
            | AggFunc::HypRank
            | AggFunc::HypDenseRank
            | AggFunc::HypPercentRank
            | AggFunc::HypCumeDist
    );

    let order_sql = match &agg.order_within_group {
        Some(sorts) if !sorts.is_empty() => {
            let sort_list: Vec<String> = sorts
                .iter()
                .map(|s| {
                    let dir = if s.ascending { "" } else { " DESC" };
                    let nulls = if s.ascending {
                        if s.nulls_first { " NULLS FIRST" } else { "" }
                    } else if s.nulls_first {
                        ""
                    } else {
                        " NULLS LAST"
                    };
                    format!("{}{dir}{nulls}", s.expr.to_sql())
                })
                .collect();
            Some(sort_list.join(", "))
        }
        _ => None,
    };

    let (base, within_group) = match order_sql {
        Some(ref order) if is_ordered_set => (
            format!("{func_name}({args_sql})"),
            format!(" WITHIN GROUP (ORDER BY {order})"),
        ),
        Some(ref order) => (
            format!("{func_name}({args_sql} ORDER BY {order})"),
            String::new(),
        ),
        None => (format!("{func_name}({args_sql})"), String::new()),
    };

    // FILTER clause
    let filter = match &agg.filter {
        Some(f) => format!(" FILTER (WHERE {})", f.to_sql()),
        None => String::new(),
    };

    format!("{base}{within_group}{filter}")
}

// ── Intermediate aggregate delta (subquery-in-FROM) ─────────────────

/// Returns true if the aggregate's old value can be computed algebraically
/// from new value + delta counts: `old = new - ins + del`.
///
/// COUNT, COUNT_STAR, and SUM are algebraically invertible.
/// AVG is algebraic via auxiliary columns (SUM + COUNT) — handled separately.
/// MIN/MAX/AVG and group-rescan aggregates require a full rescan of old data.
///
/// DI-8: SUM(CASE WHEN …) is NOT algebraically invertible because the CASE
/// condition may reference non-group-key columns that change via UPDATE.
/// The algebraic formula evaluates both 'D' and 'I' sides using the post-change
/// column values, causing the 'D' side to miscount when the CASE condition
/// references a mutated column. Falls back to GROUP_RESCAN which correctly
/// reads pre-change state via EXCEPT ALL. This band-aid can be removed once
/// DI-2 (aggregate UPDATE-split using old_* columns) lands.
fn is_algebraically_invertible(agg: &AggExpr) -> bool {
    if agg.is_distinct {
        return false;
    }

    // A42-11: DI-8: SUM(CASE WHEN …) can drift when the CASE condition
    // references mutable non-group-key columns — fall back to GROUP_RESCAN
    // (EXCEPT ALL). Detection is now AST-level so it handles CASE expressions
    // wrapped in casts, type coercions, or function calls (e.g.
    // SUM(CAST(CASE WHEN x > 1 THEN y ELSE 0 END AS numeric))).
    if matches!(agg.function, AggFunc::Sum) && agg.argument.as_ref().is_some_and(expr_contains_case)
    {
        return false;
    }

    matches!(
        agg.function,
        AggFunc::CountStar | AggFunc::Count | AggFunc::Sum
    )
}

/// A42-11: Return true if the expression tree contains a CASE expression at
/// any level — including CASE wrapped in casts, function calls, or binary ops.
///
/// Since CASE expressions reach us as `Expr::Raw(s)` (the parser does not yet
/// have a dedicated `Expr::Case` variant), we identify them by checking
/// whether the normalized raw text starts with "CASE". We recurse into
/// `FuncCall` and `BinaryOp` nodes to catch wrapped CASE expressions.
fn expr_contains_case(expr: &Expr) -> bool {
    match expr {
        Expr::Raw(s) => s.trim_start().to_uppercase().starts_with("CASE"),
        Expr::FuncCall { args, .. } => args.iter().any(expr_contains_case),
        Expr::BinaryOp { left, right, .. } => expr_contains_case(left) || expr_contains_case(right),
        Expr::ColumnRef { .. } | Expr::Literal(_) | Expr::Star { .. } => false,
    }
}

/// Returns true if the aggregate uses auxiliary columns for algebraic
/// maintenance (AVG). These track running SUM and COUNT on the ST so
/// `new_avg = (old_sum + Δsum) / (old_count + Δcount)` is exact.
fn is_algebraic_via_aux(agg: &AggExpr) -> bool {
    if agg.is_distinct {
        return false;
    }
    agg.function.is_algebraic_via_aux()
}

/// Build delta CTEs for an intermediate aggregate (one whose group-by
/// columns do NOT exist in the stream table).
///
/// Instead of LEFT JOINing to the stream table (which doesn't have the
/// intermediate columns), this builds:
///
/// **Algebraic path** (when all aggregates are COUNT/SUM):
/// 1. A "new rescan" CTE: re-aggregates affected groups from current data.
/// 2. A final CTE: emits 'D' with old values computed algebraically
///    (`old = new - ins + del` from the already-computed delta_cte) and
///    'I' with new values from the rescan.
///
/// **EXCEPT ALL path** (when MIN/MAX/group-rescan aggregates are present):
/// 1. A "new rescan" CTE: re-aggregates affected groups from current data.
/// 2. An "old rescan" CTE: re-aggregates affected groups from old data
///    (current data minus child delta inserts, plus child delta deletes).
/// 3. A final CTE: emits 'D' for old rows and 'I' for new rows.
///
/// The algebraic path is preferred because it:
/// - Avoids a second `diff_node(child)` call (no duplicate CTEs)
/// - Eliminates EXCEPT ALL (no column-matching or materialization issues)
/// - Uses the already-computed delta values from the standard aggregate path
///
/// The parent operator receives D/I pairs and processes them as normal
/// delta events.
fn build_intermediate_agg_delta(
    ctx: &mut DiffContext,
    child: &OpTree,
    group_by: &[Expr],
    group_output: &[String],
    aggregates: &[AggExpr],
    delta_cte: &str,
) -> Result<DiffResult, PgTrickleError> {
    let source_from = child_to_from_sql(child, &ctx.cte_registry);

    // We need the child's source SQL for rescanning. If we can't reconstruct
    // it, fall back to the defining query approach.
    let from_sql = match source_from {
        Some(sql) => sql,
        None => {
            // Fallback: if we have the defining query, wrap it. Otherwise error.
            return Err(PgTrickleError::InternalError(
                "Cannot build intermediate aggregate delta: \
                 child FROM clause cannot be reconstructed"
                    .into(),
            ));
        }
    };

    // Build the rescan SELECT list: group columns + all aggregates
    let mut rescan_selects = Vec::new();
    for (expr, output) in group_by.iter().zip(group_output.iter()) {
        let expr_sql = expr.to_sql();
        let qt_output = quote_ident(output);
        if expr_sql == *output && !expr_sql.contains('(') {
            rescan_selects.push(qt_output);
        } else {
            rescan_selects.push(format!("{expr_sql} AS {qt_output}"));
        }
    }
    // COUNT(*) to track the group's row count (__pgt_count)
    rescan_selects.push("COUNT(*) AS __pgt_count".to_string());
    for agg in aggregates {
        rescan_selects.push(format!(
            "{} AS {}",
            agg_to_rescan_sql(agg),
            quote_ident(&agg.alias),
        ));
    }

    // GROUP BY clause
    let group_by_sql = if group_by.is_empty() {
        String::new()
    } else {
        let gb: Vec<String> = group_by.iter().map(|e| e.to_sql()).collect();
        format!("\nGROUP BY {}", gb.join(", "))
    };

    // Group filter: only rescan affected groups
    let group_filter = if group_output.is_empty() {
        String::new()
    } else if group_output.len() == 1 {
        let col = &group_output[0];
        format!("{col} IN (SELECT {} FROM {delta_cte})", quote_ident(col),)
    } else {
        let corr: Vec<String> = group_output
            .iter()
            .map(|c| format!("{c} IS NOT DISTINCT FROM __pgt_d2.{}", quote_ident(c)))
            .collect();
        format!(
            "EXISTS (SELECT 1 FROM {delta_cte} __pgt_d2 WHERE {})",
            corr.join(" AND "),
        )
    };

    // Determine WHERE/AND connector based on existing WHERE in from_sql.
    // Only check for WHERE at the outer level — if from_sql is a subquery
    // (starts with '('), any WHERE inside is internal to the subquery.
    let has_outer_where = from_sql.contains(" WHERE ") && !from_sql.starts_with('(');
    let where_connector = if group_filter.is_empty() {
        String::new()
    } else if has_outer_where {
        format!("\n  AND {group_filter}")
    } else {
        format!("\nWHERE {group_filter}")
    };

    // ── New rescan CTE: aggregate on current (post-change) data ─────
    let new_rescan_cte = ctx.next_cte_name("agg_new");
    let new_rescan_sql = format!(
        "SELECT {selects}\nFROM {from_sql}{where_connector}{group_by}",
        selects = rescan_selects.join(",\n       "),
        group_by = group_by_sql,
    );
    ctx.add_cte(new_rescan_cte.clone(), new_rescan_sql);

    // Build output columns — exclude __pgt_count since this is an intermediate
    // aggregate. __pgt_count is internal bookkeeping for the top-level aggregate's
    // MERGE with the stream table. Including it in intermediate output causes
    // parent operators (e.g., InnerJoin) to propagate it into column references
    // against snapshots that don't have it (leading to "column __pgt_count
    // does not exist" errors).
    let mut output_cols = Vec::new();
    output_cols.extend(group_output.iter().cloned());
    for agg in aggregates {
        output_cols.push(agg.alias.clone());
    }

    // Check if all aggregates are algebraically invertible.
    // If so, we can compute old = new - ins + del using the already-computed
    // delta_cte, avoiding a second diff_node(child) call and EXCEPT ALL.
    let all_algebraic = aggregates.iter().all(is_algebraically_invertible);

    if all_algebraic {
        // ── Algebraic path: old = new − ins + del ───────────────────
        //
        // D events use the delta_cte (which has __ins_count, __del_count,
        // __ins_{alias}, __del_{alias} per group) LEFT JOINed with
        // new_rescan to compute old values algebraically.
        // I events come directly from new_rescan.
        //
        // This avoids a second diff_node(child) call (no duplicate CTEs)
        // and eliminates the EXCEPT ALL (no column-matching issues).

        let final_cte = ctx.next_cte_name("agg_final");

        // Row ID: hash ALL output columns (group + aggregates) so the
        // row_id matches the initial load's content hash.  This is
        // necessary for CTE-wrapped aggregates where the intermediate
        // aggregate's row_id flows directly to the MERGE.
        //
        // For D events, the aggregate values are the OLD (pre-change)
        // values computed algebraically;  for I events, they are the
        // NEW values from the rescan CTE.  A value change thus produces
        // different row_ids for D and I, which is correct: the D event
        // deletes the old ST row (matched by old content hash), and the
        // I event inserts a new ST row (with new content hash).
        let old_agg_hash_exprs: Vec<String> = aggregates
            .iter()
            .map(|agg| {
                let alias = &agg.alias;
                let ins_col = format!("__ins_{alias}");
                let del_col = format!("__del_{alias}");
                format!(
                    "(COALESCE(n.{a}, 0) - COALESCE(d.{i}, 0) + COALESCE(d.{d}, 0))::TEXT",
                    a = quote_ident(alias),
                    i = quote_ident(&ins_col),
                    d = quote_ident(&del_col),
                )
            })
            .collect();
        let new_agg_hash_exprs: Vec<String> = aggregates
            .iter()
            .map(|a| format!("n.{}::TEXT", quote_ident(&a.alias)))
            .collect();

        let group_hash_d: Vec<String> = group_output
            .iter()
            .map(|c| format!("d.{}::TEXT", quote_ident(c)))
            .chain(old_agg_hash_exprs)
            .collect();
        let group_hash_n: Vec<String> = group_output
            .iter()
            .map(|c| format!("n.{}::TEXT", quote_ident(c)))
            .chain(new_agg_hash_exprs)
            .collect();
        let row_id_d = if group_hash_d.is_empty() {
            "pgtrickle.pg_trickle_hash('__singleton_group')".to_string()
        } else {
            build_hash_expr(&group_hash_d)
        };
        let row_id_n = if group_hash_n.is_empty() {
            "pgtrickle.pg_trickle_hash('__singleton_group')".to_string()
        } else {
            build_hash_expr(&group_hash_n)
        };

        // D event group columns from delta_cte
        let d_group_refs: Vec<String> = group_output
            .iter()
            .map(|c| format!("d.{}", quote_ident(c)))
            .collect();
        let n_group_refs: Vec<String> = group_output
            .iter()
            .map(|c| format!("n.{}", quote_ident(c)))
            .collect();

        // Algebraic old expressions: old_X = COALESCE(new_X, 0) - ins_X + del_X
        let old_pgt_count = "COALESCE(n.\"__pgt_count\", 0) \
                             - COALESCE(d.\"__ins_count\", 0) \
                             + COALESCE(d.\"__del_count\", 0)";
        let old_agg_exprs: Vec<String> = aggregates
            .iter()
            .map(|agg| {
                let alias = &agg.alias;
                let ins_col = format!("__ins_{alias}");
                let del_col = format!("__del_{alias}");
                format!(
                    "COALESCE(n.{a}, 0) - COALESCE(d.{i}, 0) + COALESCE(d.{d}, 0) AS {a}",
                    a = quote_ident(alias),
                    i = quote_ident(&ins_col),
                    d = quote_ident(&del_col),
                )
            })
            .collect();

        let new_agg_refs: Vec<String> = aggregates
            .iter()
            .map(|a| format!("n.{}", quote_ident(&a.alias)))
            .collect();

        // JOIN condition between delta_cte and new_rescan on group columns
        let join_cond = if group_output.is_empty() {
            "TRUE".to_string()
        } else {
            group_output
                .iter()
                .map(|c| format!("d.{q} IS NOT DISTINCT FROM n.{q}", q = quote_ident(c),))
                .collect::<Vec<_>>()
                .join(" AND ")
        };

        // Format column sections
        let extra_d_groups = if d_group_refs.is_empty() {
            String::new()
        } else {
            format!("{}, ", d_group_refs.join(", "))
        };
        let extra_n_groups = if n_group_refs.is_empty() {
            String::new()
        } else {
            format!("{}, ", n_group_refs.join(", "))
        };
        let extra_old_aggs = if old_agg_exprs.is_empty() {
            String::new()
        } else {
            format!(",\n       {}", old_agg_exprs.join(",\n       "))
        };
        let extra_new_aggs = if new_agg_refs.is_empty() {
            String::new()
        } else {
            format!(",\n       {}", new_agg_refs.join(",\n       "))
        };

        let final_sql = format!(
            "\
-- D events: old state (algebraic: old = new - ins + del)
SELECT {row_id_d} AS __pgt_row_id,
       'D'::TEXT AS __pgt_action,
       {extra_d_groups}{old_pgt_count} AS __pgt_count{extra_old_aggs}
FROM {delta_cte} d
LEFT JOIN {new_rescan_cte} n ON {join_cond}

UNION ALL

-- I events: new state of affected groups
SELECT {row_id_n} AS __pgt_row_id,
       'I'::TEXT AS __pgt_action,
       {extra_n_groups}n.__pgt_count{extra_new_aggs}
FROM {new_rescan_cte} n",
        );

        ctx.add_cte(final_cte.clone(), final_sql);

        Ok(DiffResult {
            cte_name: final_cte,
            columns: output_cols,
            is_deduplicated: false,
            has_key_changed: false,
        })
    } else {
        // ── EXCEPT ALL path: rescan old data ────────────────────────
        //
        // Old data = current data - delta inserts + delta deletes.
        // We use EXCEPT ALL / UNION ALL which works positionally (by column
        // position, not by name). The source side uses SELECT * from the
        // original FROM clause, while the delta side uses the child delta
        // CTE columns. Positional matching handles name differences.

        // Get the child's delta CTE columns (these are what diff_node
        // produced for the child). We need data columns only (excluding
        // __pgt_action, __pgt_row_id).
        let child_result = ctx.diff_node(child)?;
        let delta_data_cols: Vec<String> = child_result
            .columns
            .iter()
            .map(|c| quote_ident(c))
            .collect();
        let delta_col_list = delta_data_cols.join(", ");

        // Use the same alias for the old_rescan wrapper as the original
        // FROM expression so qualified column refs resolve correctly.
        let old_rescan_alias = if from_sql.starts_with('(') {
            if let Some(as_pos) = from_sql.rfind(" AS ") {
                from_sql[as_pos + 4..].trim_matches('"').to_string()
            } else {
                "__pgt_old".to_string()
            }
        } else {
            "__pgt_old".to_string()
        };

        let old_rescan_cte = ctx.next_cte_name("agg_old");
        let old_rescan_sql = format!(
            "SELECT {selects}\nFROM (\
             SELECT * FROM {from_sql}{where_connector} \
             EXCEPT ALL \
             SELECT {delta_col_list} FROM {child_delta} WHERE __pgt_action = 'I' \
             UNION ALL \
             SELECT {delta_col_list} FROM {child_delta} WHERE __pgt_action = 'D'\
             ) {old_alias}{group_by}",
            selects = rescan_selects.join(",\n       "),
            child_delta = child_result.cte_name,
            old_alias = quote_ident(&old_rescan_alias),
            group_by = group_by_sql,
        );
        ctx.add_cte(old_rescan_cte.clone(), old_rescan_sql);

        // ── Final CTE: emit D/I pairs ───────────────────────────────
        let final_cte = ctx.next_cte_name("agg_final");

        let group_hash_n: Vec<String> = group_output
            .iter()
            .map(|c| format!("n.{}::TEXT", quote_ident(c)))
            .collect();
        let group_hash_o: Vec<String> = group_output
            .iter()
            .map(|c| format!("o.{}::TEXT", quote_ident(c)))
            .collect();
        let row_id_new = if group_hash_n.is_empty() {
            "pgtrickle.pg_trickle_hash('__singleton_group')".to_string()
        } else {
            build_hash_expr(&group_hash_n)
        };
        let row_id_old = if group_hash_o.is_empty() {
            "pgtrickle.pg_trickle_hash('__singleton_group')".to_string()
        } else {
            build_hash_expr(&group_hash_o)
        };

        let new_group_refs = group_output
            .iter()
            .map(|c| format!("n.{}", quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");
        let old_group_refs = group_output
            .iter()
            .map(|c| format!("o.{}", quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");

        let new_agg_refs: Vec<String> = aggregates
            .iter()
            .map(|a| format!("n.{}", quote_ident(&a.alias)))
            .collect();
        let old_agg_refs: Vec<String> = aggregates
            .iter()
            .map(|a| format!("o.{}", quote_ident(&a.alias)))
            .collect();

        let extra_new_groups = if new_group_refs.is_empty() {
            String::new()
        } else {
            format!("{new_group_refs}, ")
        };
        let extra_old_groups = if old_group_refs.is_empty() {
            String::new()
        } else {
            format!("{old_group_refs}, ")
        };
        let extra_new_aggs = if new_agg_refs.is_empty() {
            String::new()
        } else {
            format!(",\n       {}", new_agg_refs.join(",\n       "))
        };
        let extra_old_aggs = if old_agg_refs.is_empty() {
            String::new()
        } else {
            format!(",\n       {}", old_agg_refs.join(",\n       "))
        };

        let final_sql = format!(
            "\
-- D events: old state of affected groups
SELECT {row_id_old} AS __pgt_row_id,
       'D'::TEXT AS __pgt_action,
       {extra_old_groups}o.__pgt_count{extra_old_aggs}
FROM {old_rescan_cte} o

UNION ALL

-- I events: new state of affected groups
SELECT {row_id_new} AS __pgt_row_id,
       'I'::TEXT AS __pgt_action,
       {extra_new_groups}n.__pgt_count{extra_new_aggs}
FROM {new_rescan_cte} n",
        );

        ctx.add_cte(final_cte.clone(), final_sql);

        Ok(DiffResult {
            cte_name: final_cte,
            columns: output_cols,
            is_deduplicated: false,
            has_key_changed: false,
        })
    }
}

/// Build a rescan CTE that re-aggregates affected groups from the source
/// table. Used for group-rescan aggregates (BIT_AND, STRING_AGG, etc.)
/// and MIN/MAX (semi-algebraic: needs rescan when extremum is deleted).
///
/// The CTE selects from the original source tables (reconstructed from
/// the child OpTree or wrapped from the defining query), filters to only
/// the groups that had changes (via semi-join to the delta CTE), and
/// re-aggregates those groups.
///
/// Returns `Some(cte_name)` if a rescan CTE was created, `None` otherwise.
fn build_rescan_cte(
    ctx: &mut DiffContext,
    child: &OpTree,
    group_by: &[Expr],
    group_output: &[String],
    aggregates: &[AggExpr],
    delta_cte: &str,
    force_all_aggs: bool,
) -> Option<String> {
    // Include group-rescan aggregates AND MIN/MAX (which need rescan
    // when the old extremum is deleted).
    // When `force_all_aggs` is true (HAVING context), include ALL aggregates
    // so that the merge CTE can use the correct full aggregate value for
    // groups that were absent from the ST (below the HAVING threshold).
    let rescan_aggs: Vec<&AggExpr> = if force_all_aggs {
        aggregates.iter().collect()
    } else {
        aggregates
            .iter()
            .filter(|a| {
                a.is_distinct
                    || a.function.is_group_rescan()
                    || matches!(a.function, AggFunc::Min | AggFunc::Max)
                // P2-2: SUM over a FULL JOIN child no longer needs a rescan CTE.
                // The __pgt_aux_nonnull_* auxiliary column provides algebraic
                // NULL-transition correction without rescanning source data.
            })
            .collect()
    };
    if rescan_aggs.is_empty() {
        return None;
    }

    let rescan_cte = ctx.next_cte_name("agg_rescan");
    let source_from = child_to_from_sql(child, &ctx.cte_registry);
    let can_rescan_sum_nonnull = source_from.is_some();

    // Build SELECT list: group columns + rescan aggregate calls
    let mut selects = Vec::new();
    for (expr, output) in group_by.iter().zip(group_output.iter()) {
        let expr_sql = expr.to_sql();
        let qt_output = quote_ident(output);
        // When `expr_sql == *output` AND the expression is a simple
        // identifier (no parentheses), SELECT the column directly.
        // For compound expressions like `COALESCE(l.dept, r.dept)`,
        // `to_sql()` equals `output_name()` (both fall back to to_sql).
        // In that case we must use `expr AS alias` form, because
        // `qt_output` alone would be treated as an identifier lookup
        // ("`COALESCE(...)`" does not exist as a column), not a function call.
        if expr_sql == *output && !expr_sql.contains('(') {
            selects.push(qt_output);
        } else {
            selects.push(format!("{expr_sql} AS {qt_output}"));
        }
    }
    // When rescanning for HAVING threshold crossings, also compute the
    // correct row count so the ST's __pgt_count is set correctly for
    // groups entering the view for the first time.
    if force_all_aggs {
        selects.push("COUNT(*) AS __pgt_count".to_string());
    }
    for agg in &rescan_aggs {
        selects.push(format!(
            "{} AS {}",
            agg_to_rescan_sql(agg),
            quote_ident(&agg.alias),
        ));
        if force_all_aggs
            && matches!(agg.function, AggFunc::Sum)
            && !agg.is_distinct
            && can_rescan_sum_nonnull
        {
            selects.push(format!(
                "{} AS {}",
                agg_nonnull_count_rescan_sql(agg),
                quote_ident(&format!("__pgt_nonnull_{}", agg.alias)),
            ));
        }
    }

    // Build GROUP BY clause
    let group_by_sql = if group_by.is_empty() {
        String::new()
    } else {
        let gb: Vec<String> = group_by.iter().map(|e| e.to_sql()).collect();
        format!("\nGROUP BY {}", gb.join(", "))
    };

    // Try to reconstruct the FROM clause from the child OpTree
    let rescan_sql = if let Some(from_sql) = source_from {
        // Direct source reconstruction: more efficient since we can push
        // the group filter into the WHERE clause before aggregation.
        let group_filter = if group_output.is_empty() {
            String::new()
        } else if group_output.len() == 1 {
            // Use EXISTS with IS NOT DISTINCT FROM instead of IN to
            // correctly match NULL group-key values (NULL IN (...) → NULL).
            let col = &group_output[0];
            format!(
                "EXISTS (SELECT 1 FROM {delta_cte} __pgt_d2 WHERE {col} IS NOT DISTINCT FROM __pgt_d2.{})",
                quote_ident(col),
            )
        } else {
            // Multi-column group key: use EXISTS with IS NOT DISTINCT FROM
            // to correctly handle NULL group-key values.
            let corr: Vec<String> = group_output
                .iter()
                .map(|c| format!("{c} IS NOT DISTINCT FROM __pgt_d2.{}", quote_ident(c),))
                .collect();
            format!(
                "EXISTS (SELECT 1 FROM {delta_cte} __pgt_d2 WHERE {})",
                corr.join(" AND "),
            )
        };

        // If the child is a Filter, the FROM already includes WHERE.
        // We need to use AND instead of WHERE for the group filter.
        // Only check for WHERE at the outer level — if from_sql is a
        // subquery (starts with '('), any WHERE inside is internal.
        let has_outer_where = from_sql.contains(" WHERE ") && !from_sql.starts_with('(');
        if group_filter.is_empty() {
            format!(
                "SELECT {selects}\nFROM {from_sql}{group_by}",
                selects = selects.join(",\n       "),
                group_by = group_by_sql,
            )
        } else if has_outer_where {
            format!(
                "SELECT {selects}\nFROM {from_sql}\n  AND {group_filter}{group_by}",
                selects = selects.join(",\n       "),
                group_by = group_by_sql,
            )
        } else {
            format!(
                "SELECT {selects}\nFROM {from_sql}\nWHERE {group_filter}{group_by}",
                selects = selects.join(",\n       "),
                group_by = group_by_sql,
            )
        }
    } else {
        let defining_query = ctx.defining_query.as_ref()?;
        // Fallback: wrap the defining query as a subquery and filter to
        // affected groups. Less efficient (re-aggregates all groups then
        // filters) but correct for any child OpTree shape.
        //
        // The defining_query output columns use the SELECT aliases (e.g., "k"),
        // while the delta CTE columns use the raw GROUP BY output_name()
        // (e.g., `COALESCE("ab"."k", "c"."k")`). Translate via alias_map
        // so `__pgt_dq` is referenced with the correct defining_query column name.
        let dq_col_name = |delta_col: &str| -> String {
            if let Some(ref map) = ctx.st_column_alias_map {
                map.get(delta_col)
                    .cloned()
                    .unwrap_or_else(|| delta_col.to_string())
            } else {
                delta_col.to_string()
            }
        };

        let where_clause = if group_output.is_empty() {
            String::new()
        } else if group_output.len() == 1 {
            // Use EXISTS with IS NOT DISTINCT FROM instead of IN to
            // correctly match NULL group-key values (NULL IN (...) → NULL).
            let delta_col = &group_output[0];
            let dq_col = dq_col_name(delta_col);
            format!(
                "\nWHERE EXISTS (SELECT 1 FROM {delta_cte} __pgt_d2 WHERE __pgt_dq.{dqc} IS NOT DISTINCT FROM __pgt_d2.{d2c})",
                dqc = quote_ident(&dq_col),
                d2c = quote_ident(delta_col),
            )
        } else {
            let corr: Vec<String> = group_output
                .iter()
                .map(|delta_col| {
                    let dq_col = dq_col_name(delta_col);
                    format!(
                        "__pgt_dq.{dqc} IS NOT DISTINCT FROM __pgt_d2.{d2c}",
                        dqc = quote_ident(&dq_col),
                        d2c = quote_ident(delta_col),
                    )
                })
                .collect();
            format!(
                "\nWHERE EXISTS (SELECT 1 FROM {delta_cte} __pgt_d2 WHERE {})",
                corr.join(" AND "),
            )
        };

        // Select only the rescan aggregate columns from the defining query,
        // using defining_query column names (SELECT aliases) for __pgt_dq.
        // Alias the output back to the delta CTE column name so that the
        // merge CTE's rescan join condition `r.{col} = d.{col}` works
        // correctly when the delta column name differs from the ST alias.
        let dq_selects: Vec<String> = group_output
            .iter()
            .map(|delta_col| {
                let dq_col = dq_col_name(delta_col);
                if dq_col == *delta_col {
                    format!("__pgt_dq.{}", quote_ident(delta_col))
                } else {
                    // The defining_query output uses the ST alias (dq_col),
                    // but downstream merge CTEs expect the delta column name.
                    format!(
                        "__pgt_dq.{} AS {}",
                        quote_ident(&dq_col),
                        quote_ident(delta_col),
                    )
                }
            })
            .chain(
                rescan_aggs
                    .iter()
                    .map(|a| format!("__pgt_dq.{}", quote_ident(&a.alias))),
            )
            .collect();

        format!(
            "SELECT {selects}\nFROM ({defining_query}) __pgt_dq{where_clause}",
            selects = dq_selects.join(", "),
        )
    };

    ctx.add_cte(rescan_cte.clone(), rescan_sql);
    Some(rescan_cte)
}

// ── P5: Direct aggregate bypass helpers ─────────────────────────────

fn agg_nonnull_count_rescan_sql(agg: &AggExpr) -> String {
    let argument = agg
        .argument
        .as_ref()
        .map(Expr::to_sql)
        .unwrap_or_else(|| "*".to_string());
    let filter = agg
        .filter
        .as_ref()
        .map(|expr| format!(" FILTER (WHERE {})", expr.to_sql()))
        .unwrap_or_default();
    format!("COUNT({argument}){filter}")
}

/// Check if a Scan → Aggregate tree qualifies for the P5 direct bypass.
///
/// Requirements:
/// - Child is a direct `OpTree::Scan` (no intervening Filter/Project/Join)
/// - All aggregates are decomposable (SUM, COUNT, CountStar, AVG — not MIN/MAX)
/// - No DISTINCT aggregates
/// - All aggregate arguments are simple `ColumnRef` (or `None` for COUNT(*))
/// - All group-by expressions are simple `ColumnRef`
fn is_direct_agg_eligible(
    child: &OpTree,
    group_by: &[Expr],
    aggregates: &[AggExpr],
    ctx: &DiffContext,
) -> bool {
    // ST-ST-4: The P5 direct aggregate bypass reads directly from the
    // change buffer table using old_*/new_* columns.  ST change buffers
    // have a different schema (no old_*, no UPDATE actions), so disable
    // the bypass for ST sources — fall back to the standard scan pipeline.
    let table_oid = match child {
        OpTree::Scan { table_oid, .. } => *table_oid,
        _ => return false,
    };
    if ctx.st_source_pgt_ids.contains_key(&table_oid) {
        return false;
    }
    for agg in aggregates {
        // P5 only supports decomposable algebraic aggregates without FILTER.
        // AVG requires auxiliary column tracking (not P5-compatible),
        // MIN/MAX and group-rescan aggregates also excluded.
        if matches!(agg.function, AggFunc::Min | AggFunc::Max)
            || agg.function.is_group_rescan()
            || agg.function.is_algebraic_via_aux()
        {
            return false;
        }
        if agg.is_distinct {
            return false;
        }
        if agg.filter.is_some() {
            return false;
        }
        if let Some(arg) = &agg.argument {
            match arg {
                Expr::ColumnRef { .. } => {
                    // Simple column ref — always P5-eligible.
                }
                Expr::Raw(case_sql)
                    if matches!(agg.function, AggFunc::Sum) && expr_contains_case(arg) =>
                {
                    // A44-2 (v0.43.0): SUM(CASE ...) is P5-eligible when the
                    // CASE expression references only simple column identifiers
                    // that are present in the CDC change buffer for this table.
                    // Verified here (with CDC column access) rather than deferred
                    // to generate_direct_agg_delta, so an empty-column fallback
                    // never produces invalid SQL.
                    let cdc_cols = match child {
                        OpTree::Scan { table_oid, .. } => ctx.source_cdc_columns.get(table_oid),
                        _ => return false,
                    };
                    if let Some(cols) = cdc_cols {
                        let extracted = extract_case_columns(case_sql, cols);
                        if extracted.is_empty() {
                            return false;
                        }
                        // At least one column found — P5-eligible.
                    } else {
                        // No CDC column metadata — cannot verify; fall back to
                        // the standard scan path (GROUP_RESCAN).
                        return false;
                    }
                }
                _ => {
                    return false;
                }
            }
        }
    }
    for expr in group_by {
        if !matches!(expr, Expr::ColumnRef { .. }) {
            return false;
        }
    }
    true
}

// ── A44-2 (v0.43.0): DI-2 CASE expression helpers ─────────────────────────

/// A44-2: Extract simple identifier (column) names from a CASE SQL expression.
///
/// Returns the list of identifiers that are NOT SQL keywords and are likely
/// column references. Used to include the necessary columns in the LATERAL
/// VALUES construction for the DI-2 UPDATE split path.
///
/// Only returns identifiers that appear in `available_cols` to avoid
/// including SQL constants, operators, and keywords.
fn extract_case_columns<'a>(case_sql: &str, available_cols: &'a [String]) -> Vec<&'a String> {
    // Collect word tokens from the CASE expression (alphanumeric + underscore).
    // Exclude common SQL keywords.
    const SQL_KEYWORDS: &[&str] = &[
        "CASE", "WHEN", "THEN", "ELSE", "END", "AND", "OR", "NOT", "IN", "IS", "NULL", "TRUE",
        "FALSE", "BETWEEN", "LIKE", "ILIKE", "CAST", "AS", "ON", "AT", "TIME", "ZONE",
    ];
    let upper = case_sql.to_uppercase();
    let words: Vec<&str> = upper
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|s| !s.is_empty())
        .filter(|s| !SQL_KEYWORDS.contains(s))
        .filter(|s| !s.chars().all(|c| c.is_ascii_digit()))
        .collect();

    available_cols
        .iter()
        .filter(|col| words.contains(&col.to_uppercase().as_str()))
        .collect()
}

/// A44-2: Rewrite a CASE SQL expression so all column references are
/// prefixed with `v."val_"`, making it suitable for the LATERAL VALUES path.
///
/// For each column in `cols`, replaces the bare identifier with the
/// Rewrite a CASE SQL expression to use direct CB column references `c."col"`.
///
/// A44-10 (D+I schema): Previously used `v."val_{col}"` LATERAL VALUES aliases;
/// with D+I flat schema we reference `c.{col}` directly since the CB already
/// has separate I-rows and D-rows with flat column values.
///
/// Uses word-boundary replacement to avoid partial matches
/// (e.g. `status` vs `order_status`).
fn rewrite_case_for_lateral(case_sql: &str, cols: &[&String]) -> String {
    let mut result = case_sql.to_string();
    for col in cols {
        // A44-10: direct CB column reference instead of LATERAL VALUES alias
        let replacement = format!("c.{}", quote_ident(col));
        // Word-boundary replacement: only replace `col` when it's a standalone word.
        // We match it as a word (preceded and followed by non-word chars or start/end).
        let col_upper = col.to_uppercase();
        let result_upper = result.to_uppercase();
        // Find all occurrences of the column name as a complete word in the SQL.
        let mut out = String::with_capacity(result.len());
        let mut pos = 0usize;
        while pos < result_upper.len() {
            if let Some(idx) = result_upper[pos..].find(col_upper.as_str()) {
                let abs_idx = pos + idx;
                let before_ok = abs_idx == 0
                    || !result_upper.as_bytes()[abs_idx - 1].is_ascii_alphanumeric()
                        && result_upper.as_bytes()[abs_idx - 1] != b'_';
                let after_ok = abs_idx + col_upper.len() >= result_upper.len()
                    || (!result_upper.as_bytes()[abs_idx + col_upper.len()]
                        .is_ascii_alphanumeric()
                        && result_upper.as_bytes()[abs_idx + col_upper.len()] != b'_');
                if before_ok && after_ok {
                    out.push_str(&result[pos..abs_idx]);
                    out.push_str(&replacement);
                    pos = abs_idx + col.len();
                } else {
                    out.push_str(&result[pos..abs_idx + 1]);
                    pos = abs_idx + 1;
                }
            } else {
                out.push_str(&result[pos..]);
                break;
            }
        }
        result = out;
    }
    result
}

/// P5 + P7 — Generate a direct aggregate delta CTE from the change buffer.
///
/// Instead of differentiating the child Scan (which would go through the full
/// scan delta pipeline with window functions), reads directly from the typed
/// change buffer table. Group-by keys and aggregate arguments are referenced
/// as `c."new_{col}"` / `c."old_{col}"` — typed columns that are already
/// available from the P7 typed change buffer.
///
/// For UPDATE rows, the LATERAL VALUES expansion splits each change into
/// an INSERT side (from `new_*` columns) and a DELETE side (from `old_*`
/// columns), correctly handling group-key changes.
///
/// A44-2 (v0.43.0): SUM(CASE WHEN ...) is handled via the DI-2 UPDATE split:
/// the CASE expression is rewritten to reference LATERAL VALUES aliases, and
/// all referenced column names are included in the LATERAL Values construction
/// (with old_* for D-rows and new_* for I-rows). This makes SUM(CASE) O(Δ)
/// rather than O(group_size) via GROUP_RESCAN.
///
/// Returns `(delta_cte_name, group_output_names)`.
fn generate_direct_agg_delta(
    ctx: &mut DiffContext,
    scan: &OpTree,
    group_by: &[Expr],
    aggregates: &[AggExpr],
) -> Result<(String, Vec<String>), PgTrickleError> {
    let OpTree::Scan { table_oid, .. } = scan else {
        return Err(PgTrickleError::InternalError(
            "generate_direct_agg_delta called on non-Scan".into(),
        ));
    };

    let buf_name = ctx
        .source_buffer_names
        .get(table_oid)
        .cloned()
        .unwrap_or_else(|| format!("changes_{table_oid}"));
    let change_table = format!("{}.{}", quote_ident(&ctx.change_buffer_schema), buf_name);
    let prev_lsn = ctx.get_prev_lsn(*table_oid);
    let new_lsn = ctx.get_new_lsn(*table_oid);

    // Collect group-by column names
    let group_output: Vec<String> = group_by.iter().map(|e| e.output_name()).collect();

    // A44-2 (v0.43.0): Gather CDC columns for DI-2 CASE-expression rewrite.
    let cdc_cols_for_case = ctx
        .source_cdc_columns
        .get(table_oid)
        .cloned()
        .unwrap_or_default();

    // Collect unique aggregate argument column names.
    // A44-2: For SUM(CASE ...) aggregates, extract the column names referenced
    // inside the CASE expression and include them individually in arg_cols, then
    // record the rewritten CASE for use in the SELECT expressions.
    let mut arg_cols: Vec<String> = Vec::new();
    // Maps aggregate alias -> rewritten CASE expression using v.val_{col} refs.
    let mut case_rewrites: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    // Track whether any aggregate has a CASE argument (was used by V-row optimization,
    // now only kept for case_rewrites population).
    let mut _has_case_aggregate = false;

    for agg in aggregates {
        if let Some(arg) = &agg.argument {
            match arg {
                Expr::ColumnRef { .. } => {
                    let name = arg.output_name();
                    if !arg_cols.contains(&name) {
                        arg_cols.push(name);
                    }
                }
                Expr::Raw(case_sql)
                    if matches!(agg.function, AggFunc::Sum) && expr_contains_case(arg) =>
                {
                    // A44-2: DI-2 UPDATE split for SUM(CASE ...).
                    // Extract column refs from the CASE expression and
                    // rewrite it to use LATERAL VALUES aliases.
                    let case_cols = extract_case_columns(case_sql, &cdc_cols_for_case);
                    if !case_cols.is_empty() {
                        for col in &case_cols {
                            if !arg_cols.contains(*col) {
                                arg_cols.push((*col).clone());
                            }
                        }
                        let rewritten = rewrite_case_for_lateral(case_sql, &case_cols);
                        case_rewrites.insert(agg.alias.clone(), rewritten);
                        _has_case_aggregate = true;
                    } else {
                        // is_direct_agg_eligible guards this path: if it returned
                        // true, CDC columns were verified to be non-empty.  If we
                        // somehow reach here with no columns extracted, it is an
                        // internal invariant violation.
                        return Err(PgTrickleError::InternalError(format!(
                            "A44-2: generate_direct_agg_delta reached with CASE aggregate \
                             '{}' but no CASE columns could be extracted — CDC column \
                             metadata missing. This should have been caught by \
                             is_direct_agg_eligible.",
                            agg.alias
                        )));
                    }
                }
                _ => {
                    let name = arg.output_name();
                    if !arg_cols.contains(&name) {
                        arg_cols.push(name);
                    }
                }
            }
        }
    }

    // ── A44-10 (D+I schema): Build SELECT expressions directly from CB ──
    //
    // With D+I, the CB has separate I-rows (action='I') and D-rows (action='D'),
    // each with flat column values. No LATERAL VALUES needed — just filter on
    // c.action directly. This eliminates the 5-CTE UNION ALL scan pipeline and
    // the 'V' (value-only) net-correction optimization (which is now unnecessary
    // since I/D rows naturally cancel in COUNT and aggregate correctly in SUM).
    //
    // The LATERAL VALUES structure is kept for A44-2 (SUM(CASE ...)) compatibility
    // since the rewritten CASE expression references v.val_{col} aliases. For
    // simple column-ref aggregates, we use direct c.action-based filtering.
    //
    // A44-2 CASE rewrite: the case_rewrites map has rewritten CASE expressions
    // using v.val_{col} references. These still need LATERAL VALUES.
    // For non-CASE aggregates, use direct c.{col} references.
    let has_value_only = false; // V-row optimization dropped in D+I schema.

    // ── Build SELECT expressions ──────────────────────────────────────
    let delta_cte = ctx.next_cte_name("agg_delta");
    let mut select_exprs = Vec::new();

    // Group columns: c."region" AS "region"
    // A44-10: direct CB reference — same value on both I and D rows within a group
    for name in &group_output {
        select_exprs.push(format!("c.{} AS {}", quote_ident(name), quote_ident(name),));
    }

    // __ins_count, __del_count (total row counts per group)
    select_exprs
        .push("SUM(CASE WHEN c.action = 'I' THEN 1 ELSE 0 END)::bigint AS __ins_count".to_string());
    select_exprs
        .push("SUM(CASE WHEN c.action = 'D' THEN 1 ELSE 0 END)::bigint AS __del_count".to_string());

    // Per-aggregate delta expressions
    for agg in aggregates {
        // A44-2: If this aggregate has a rewritten CASE expression, use it
        // directly with I/D action filtering instead of the generic helper.
        let (ins_expr, del_expr) = if let Some(rewritten) = case_rewrites.get(&agg.alias) {
            // DI-2 UPDATE split for SUM(CASE ...): the rewritten CASE references
            // c."col" CB column aliases (after A44-10 update of rewrite_case_for_lateral).
            let ins = format!("SUM(CASE WHEN c.action = 'I' THEN ({rewritten}) ELSE 0 END)");
            let del = format!("SUM(CASE WHEN c.action = 'D' THEN ({rewritten}) ELSE 0 END)");
            (ins, del)
        } else {
            direct_agg_delta_exprs(agg, has_value_only)
        };
        select_exprs.push(format!(
            "{ins_expr} AS {}",
            quote_ident(&format!("__ins_{}", agg.alias)),
        ));
        select_exprs.push(format!(
            "{del_expr} AS {}",
            quote_ident(&format!("__del_{}", agg.alias)),
        ));

        // SUM needs a qualifying non-NULL count so the algebraic merge can
        // restore PostgreSQL's NULL result when the last value disappears.
        if matches!(agg.function, AggFunc::Sum) && !agg.is_distinct {
            let argument = case_rewrites
                .get(&agg.alias)
                .cloned()
                .unwrap_or_else(|| direct_agg_argument_expr(agg));
            let (ins_nonnull, del_nonnull) = direct_nonnull_delta_exprs(&argument);
            select_exprs.push(format!(
                "{ins_nonnull} AS {}",
                quote_ident(&format!("__ins_nonnull_{}", agg.alias)),
            ));
            select_exprs.push(format!(
                "{del_nonnull} AS {}",
                quote_ident(&format!("__del_nonnull_{}", agg.alias)),
            ));
        }
    }

    // GROUP BY — use c."col" directly (A44-10: no v.grp_* aliases needed)
    let group_by_clause = if group_output.is_empty() {
        String::new()
    } else {
        let refs: Vec<String> = group_output
            .iter()
            .map(|name| format!("c.{}", quote_ident(name)))
            .collect();
        format!("\nGROUP BY {}", refs.join(", "))
    };

    // A44-10 (D+I schema): Simple WHERE — all CB rows are already I or D.
    // No LATERAL VALUES needed. UNION ALL approach is replaced by direct
    // c.action filtering in the SELECT expressions.
    let where_clause = format!(
        "WHERE c.lsn > '{prev_lsn}'::pg_lsn AND c.lsn <= '{new_lsn}'::pg_lsn\n  \
         AND c.action IN ('I', 'D')"
    );

    let delta_sql = format!(
        "\
SELECT {selects}
FROM {change_table} c
{where_clause}{group_by}",
        selects = select_exprs.join(",\n       "),
        group_by = group_by_clause,
    );

    ctx.add_cte(delta_cte.clone(), delta_sql);

    Ok((delta_cte, group_output))
}

/// Generate per-aggregate delta expressions for the P5 direct bypass CTE.
///
/// A44-10 (D+I schema): References flat `c."col"` columns and `c.action`
/// directly. No LATERAL VALUES needed — D-rows have old values, I-rows
/// have new values. The `has_value_only` parameter is always false with
/// D+I schema (V-row optimization dropped).
fn direct_agg_delta_exprs(agg: &AggExpr, _has_value_only: bool) -> (String, String) {
    if agg.is_distinct {
        unreachable!("P5 bypass does not support DISTINCT aggregates")
    }

    match &agg.function {
        AggFunc::CountStar => (
            "SUM(CASE WHEN c.action = 'I' THEN 1 ELSE 0 END)::bigint".to_string(),
            "SUM(CASE WHEN c.action = 'D' THEN 1 ELSE 0 END)::bigint".to_string(),
        ),
        AggFunc::Count => {
            // A44-10: flat column reference — c."col" for both I and D rows
            let col = agg
                .argument
                .as_ref()
                .map(|e| format!("c.{}", quote_ident(&e.output_name())))
                .unwrap_or_else(|| "1".to_string());
            (
                format!(
                    "SUM(CASE WHEN c.action = 'I' AND {col} IS NOT NULL THEN 1 ELSE 0 END)::bigint"
                ),
                format!(
                    "SUM(CASE WHEN c.action = 'D' AND {col} IS NOT NULL THEN 1 ELSE 0 END)::bigint"
                ),
            )
        }
        AggFunc::Sum => {
            // A44-10: flat column reference — c."col" for both I and D rows.
            // D-rows carry old values (−1), I-rows carry new values (+1).
            let col = agg
                .argument
                .as_ref()
                .map(|e| format!("c.{}", quote_ident(&e.output_name())))
                .unwrap_or_else(|| "0".to_string());
            (
                format!("SUM(CASE WHEN c.action = 'I' THEN {col} ELSE 0 END)"),
                format!("SUM(CASE WHEN c.action = 'D' THEN {col} ELSE 0 END)"),
            )
        }
        AggFunc::Min | AggFunc::Max => {
            // Should never be reached — eligibility check excludes MIN/MAX
            unreachable!("P5 bypass does not support MIN/MAX aggregates")
        }
        _ => {
            // Group-rescan aggregates should also never reach P5 bypass
            unreachable!("P5 bypass does not support group-rescan aggregates")
        }
    }
}

fn direct_agg_argument_expr(agg: &AggExpr) -> String {
    agg.argument
        .as_ref()
        .map(|expr| match expr {
            Expr::ColumnRef { .. } => format!("c.{}", quote_ident(&expr.output_name())),
            _ => expr.to_sql(),
        })
        .unwrap_or_else(|| "1".to_string())
}

fn direct_nonnull_delta_exprs(argument: &str) -> (String, String) {
    (
        format!(
            "SUM(CASE WHEN c.action = 'I' AND ({argument}) IS NOT NULL THEN 1 ELSE 0 END)::bigint"
        ),
        format!(
            "SUM(CASE WHEN c.action = 'D' AND ({argument}) IS NOT NULL THEN 1 ELSE 0 END)::bigint"
        ),
    )
}

/// Differentiate an Aggregate node.
pub fn diff_aggregate(ctx: &mut DiffContext, op: &OpTree) -> Result<DiffResult, PgTrickleError> {
    let OpTree::Aggregate {
        group_by,
        aggregates,
        child,
    } = op
    else {
        return Err(PgTrickleError::InternalError(
            "diff_aggregate called on non-Aggregate node".into(),
        ));
    };

    // ── CTE 1: Choose between P5 direct bypass or standard path ────────
    //
    // P5: For Scan → Aggregate trees where all aggregates are decomposable
    // (SUM/COUNT/AVG), extract only the needed group-by keys and aggregate
    // argument columns directly from the change buffer via JSONB '->>'
    // instead of deserializing ALL columns with jsonb_populate_record.
    //
    // The P5 bypass reads directly from the change buffer table, so it is
    // only applicable for ChangeBuffer delta sources. For TransitionTable
    // (IMMEDIATE mode), we always use the standard path which correctly
    // reads from the trigger transition temp tables via diff_scan.
    let use_p5 = matches!(ctx.delta_source, DeltaSource::ChangeBuffer)
        && is_direct_agg_eligible(child, group_by, aggregates, ctx);
    let (delta_cte, group_output) = if use_p5 {
        generate_direct_agg_delta(ctx, child, group_by, aggregates)?
    } else {
        // ── Standard path: differentiate child first ───────────────────
        let child_result = ctx.diff_node(child)?;

        // Resolve group-by expressions against child CTE's column names.
        let child_cols = &child_result.columns;
        let group_resolved: Vec<String> = group_by
            .iter()
            .map(|e| resolve_group_col(e, child_cols))
            .collect();
        let group_output: Vec<String> = group_by.iter().map(|e| e.output_name()).collect();

        let group_by_clause = if group_resolved.is_empty() {
            String::new()
        } else {
            let gb_cols: Vec<String> = group_resolved
                .iter()
                .map(|c| col_ref_or_sql_expr(c))
                .collect();
            format!("\nGROUP BY {}", gb_cols.join(", "))
        };

        let delta_cte = ctx.next_cte_name("agg_delta");
        let mut delta_selects = Vec::new();

        // Group by columns — alias to output name for consistent downstream refs.
        // When the output name is a SQL expression (contains '('), always add an
        // explicit alias so the delta CTE column is named by the full expression
        // as a quoted identifier. Without this guard, `split_part(x, y, z)` would
        // be implicitly named `split_part` by PostgreSQL, causing the merge CTE's
        // `d."split_part(x, y, z)"` reference to fail with "column does not exist".
        // build_rescan_cte uses the same `&& !expr_sql.contains('(')` guard.
        for (resolved, output) in group_resolved.iter().zip(group_output.iter()) {
            if resolved == output && !output.contains('(') {
                delta_selects.push(col_ref_or_sql_expr(resolved));
            } else {
                delta_selects.push(format!(
                    "{} AS {}",
                    col_ref_or_sql_expr(resolved),
                    quote_ident(output)
                ));
            }
        }

        // Always track insert/delete counts
        delta_selects
            .push("SUM(CASE WHEN __pgt_action = 'I' THEN 1 ELSE 0 END) AS __ins_count".to_string());
        delta_selects
            .push("SUM(CASE WHEN __pgt_action = 'D' THEN 1 ELSE 0 END) AS __del_count".to_string());

        // Per-aggregate tracking
        for agg in aggregates {
            let (ins_expr, del_expr) = agg_delta_exprs(agg, child_cols);
            let alias_i = format!("__ins_{}", agg.alias);
            let alias_d = format!("__del_{}", agg.alias);
            delta_selects.push(format!("{ins_expr} AS {}", quote_ident(&alias_i)));
            delta_selects.push(format!("{del_expr} AS {}", quote_ident(&alias_d)));

            // AVG/STDDEV/VAR algebraic path: also track non-NULL counts for
            // the argument so the merge can update __pgt_aux_count_{alias}.
            if is_algebraic_via_aux(agg) {
                let (ins_cnt, del_cnt) = agg_avg_count_delta_exprs(agg, child_cols);
                let cnt_alias_i = format!("__ins_count_{}", agg.alias);
                let cnt_alias_d = format!("__del_count_{}", agg.alias);
                delta_selects.push(format!("{ins_cnt} AS {}", quote_ident(&cnt_alias_i)));
                delta_selects.push(format!("{del_cnt} AS {}", quote_ident(&cnt_alias_d)));
            }

            // STDDEV/VAR: also track SUM(arg²) for sum-of-squares auxiliary.
            if agg.function.needs_sum_of_squares() && !agg.is_distinct {
                let (ins_sum2, del_sum2) = agg_sum2_delta_exprs(agg, child_cols);
                let sum2_alias_i = format!("__ins_sum2_{}", agg.alias);
                let sum2_alias_d = format!("__del_sum2_{}", agg.alias);
                delta_selects.push(format!("{ins_sum2} AS {}", quote_ident(&sum2_alias_i)));
                delta_selects.push(format!("{del_sum2} AS {}", quote_ident(&sum2_alias_d)));
            }

            // P3-2: CORR/COVAR/REGR_* — track cross-product auxiliary deltas.
            if agg.function.needs_cross_products() && !agg.is_distinct {
                let (ins_exprs, del_exprs) = agg_covar_delta_exprs(agg, child_cols);
                for (suffix, ins_expr, del_expr) in &[
                    ("sumx", &ins_exprs.0, &del_exprs.0),
                    ("sumy", &ins_exprs.1, &del_exprs.1),
                    ("sumxy", &ins_exprs.2, &del_exprs.2),
                    ("sumx2", &ins_exprs.3, &del_exprs.3),
                    ("sumy2", &ins_exprs.4, &del_exprs.4),
                ] {
                    let i_alias = format!("__ins_{suffix}_{}", agg.alias);
                    let d_alias = format!("__del_{suffix}_{}", agg.alias);
                    delta_selects.push(format!("{ins_expr} AS {}", quote_ident(&i_alias)));
                    delta_selects.push(format!("{del_expr} AS {}", quote_ident(&d_alias)));
                }
            }

            // Track nonnull-count deltas so SUM can distinguish NULL from zero
            // without a full-group rescan.
            if matches!(agg.function, AggFunc::Sum) && !agg.is_distinct {
                let (ins_nonnull, del_nonnull) = agg_nonnull_delta_exprs(agg, child_cols);
                let nonnull_alias_i = format!("__ins_nonnull_{}", agg.alias);
                let nonnull_alias_d = format!("__del_nonnull_{}", agg.alias);
                delta_selects.push(format!(
                    "{ins_nonnull} AS {}",
                    quote_ident(&nonnull_alias_i)
                ));
                delta_selects.push(format!(
                    "{del_nonnull} AS {}",
                    quote_ident(&nonnull_alias_d)
                ));
            }
        }

        let delta_sql = format!(
            "SELECT {selects}\nFROM {child_cte}{group_by}",
            selects = delta_selects.join(",\n       "),
            child_cte = child_result.cte_name,
            group_by = group_by_clause,
        );
        ctx.add_cte(delta_cte.clone(), delta_sql);

        (delta_cte, group_output)
    };

    // ── Detect intermediate aggregate ───────────────────────────────
    //
    // An intermediate aggregate is one whose output columns do NOT exist
    // in the stream table (e.g., an inner GROUP BY in a subquery-in-FROM,
    // or a global aggregate like MAX inside a scalar subquery).
    // For such aggregates, we cannot LEFT JOIN to the stream table because
    // the ST doesn't have the intermediate columns.  Instead, we build an
    // "old snapshot" CTE by re-aggregating the child's old data (current
    // data minus child delta inserts, plus child delta deletes).
    //
    // Also intermediate when the ST does NOT have `__pgt_count` — e.g.,
    // an aggregate inside a CTE body where the top-level tree is
    // Filter(CteScan{...}).  The aggregate's group/value columns match
    // the ST's user columns, but `__pgt_count` was never added because
    // `needs_pgt_count()` returns false for the top-level CteScan.
    let is_intermediate = if let Some(ref st_cols) = ctx.st_user_columns {
        if !ctx.st_has_pgt_count {
            // ST has no __pgt_count → aggregate merge cannot read st.__pgt_count
            true
        } else if !group_output.is_empty() {
            // Grouped aggregate: check if any group column is missing from ST
            group_output.iter().any(|g| !st_cols.contains(g))
        } else if !aggregates.is_empty() {
            // Global aggregate (no GROUP BY): check if aggregate output
            // columns exist in the stream table. If not, this is an
            // intermediate aggregate (e.g., MAX inside a scalar subquery).
            aggregates.iter().any(|a| !st_cols.contains(&a.alias))
        } else {
            false
        }
    } else {
        false
    };

    if is_intermediate {
        return build_intermediate_agg_delta(
            ctx,
            child,
            group_by,
            &group_output,
            aggregates,
            &delta_cte,
        );
    }

    // ── Rescan CTE: re-aggregate affected groups for group-rescan aggs ──
    // Also forced when under a HAVING filter (`ctx.having_filter = true`) so
    // that groups crossing the threshold upward (absent from the ST) receive
    // the correct full aggregate value rather than just the per-cycle delta.
    let use_having_rescan = ctx.having_filter;
    let rescan_cte = build_rescan_cte(
        ctx,
        child,
        group_by,
        &group_output,
        aggregates,
        &delta_cte,
        use_having_rescan,
    );
    let has_rescan = rescan_cte.is_some();

    // ── CTE 2: Merge with existing ST state to classify actions ────────
    let merge_cte = ctx.next_cte_name("agg_merge");

    let st_table = ctx.st_qualified_name.as_deref().unwrap_or("/* st_table */");

    // When a parent Project renames columns (e.g., `name AS region`),
    // the aggregate's group_output names don't match the ST column names.
    // Use the alias map to translate group/agg column names back to the
    // actual ST column names for `st.{col}` references.
    let st_col_name = |col: &str| -> String {
        if let Some(ref map) = ctx.st_column_alias_map {
            map.get(col).cloned().unwrap_or_else(|| col.to_string())
        } else {
            col.to_string()
        }
    };

    // Row ID from group-by columns (using output names)
    let group_hash_exprs: Vec<String> = group_output
        .iter()
        .map(|c| format!("d.{}::TEXT", quote_ident(c)))
        .collect();
    let row_id_expr = if group_hash_exprs.is_empty() {
        "pgtrickle.pg_trickle_hash('__singleton_group')".to_string()
    } else {
        build_hash_expr(&group_hash_exprs)
    };

    let mut merge_selects = Vec::new();
    // Use the existing row ID from the target when the group already exists
    // (LEFT JOIN hit), otherwise compute a hash from the group-by columns.
    // This ensures the DVM-generated row IDs are compatible with however the
    // initial FULL refresh assigned IDs (ROW_NUMBER vs hash), preventing
    // duplicate-key violations when the aggregate fast-path (B-1) or MERGE
    // tries to UPDATE/DELETE by __pgt_row_id.
    merge_selects.push(format!(
        "COALESCE(st.__pgt_row_id, {row_id_expr}) AS __pgt_row_id"
    ));

    // Group columns
    for col in &group_output {
        merge_selects.push(format!("d.{}", quote_ident(col)));
    }

    // New count = old + inserts - deletes
    // COALESCE guards: for global aggregates (no GROUP BY) the agg_delta
    // CTE always produces exactly one row.  When **all** child-delta rows
    // are filtered out the SUM(CASE …) expressions evaluate over an empty
    // set and return NULL, not 0.  Wrapping in COALESCE(…, 0) keeps the
    // arithmetic safe.
    //
    // HAVING rescan override: for groups not yet in the ST (below the HAVING
    // threshold), the algebraic formula `0 + ins - del` only accounts for the
    // current-cycle delta, missing pre-existing rows.  Use the rescan CTE's
    // COUNT(*) instead when the group is new.
    let new_count_expr = if use_having_rescan {
        "CASE WHEN st.__pgt_count IS NULL \
         THEN COALESCE(r.__pgt_count, 0) \
         ELSE COALESCE(st.__pgt_count, 0) + COALESCE(d.__ins_count, 0) - COALESCE(d.__del_count, 0) END"
            .to_string()
    } else {
        "COALESCE(st.__pgt_count, 0) + COALESCE(d.__ins_count, 0) - COALESCE(d.__del_count, 0)"
            .to_string()
    };
    merge_selects.push(format!("{new_count_expr} AS new_count"));
    merge_selects.push("COALESCE(st.__pgt_count, 0) AS old_count".to_string());

    // Per-aggregate new values + old values for G-S1 change detection
    for agg in aggregates {
        // Every non-DISTINCT SUM uses nonnull-count aux for algebraic
        // NULL-transition correction. SUM is never routed through rescan.
        let agg_has_nonnull_aux = matches!(agg.function, AggFunc::Sum) && !agg.is_distinct;
        let agg_has_rescan = if matches!(agg.function, AggFunc::Sum) && !agg.is_distinct {
            // SUM is always algebraic — never use the rescan path
            false
        } else {
            has_rescan
        };
        let new_val_expr = agg_merge_expr_mapped(
            agg,
            agg_has_rescan,
            use_having_rescan,
            agg_has_nonnull_aux,
            &st_col_name(&agg.alias),
            ctx.agg_sum_coalesce_defaults
                .as_ref()
                .and_then(|m| m.get(&agg.alias))
                .map(|s| s.as_str()),
        );
        merge_selects.push(format!(
            "{new_val_expr} AS {}",
            quote_ident(&format!("new_{}", agg.alias)),
        ));
        // Keep old value alongside so the final CTE can skip unchanged U rows.
        // st.{alias} is NULL for brand-new groups (LEFT JOIN miss), which is
        // correct: IS DISTINCT FROM will see old=NULL vs new=<value> → changed.
        merge_selects.push(format!(
            "st.{} AS {}",
            quote_ident(&st_col_name(&agg.alias)),
            quote_ident(&format!("old_{}", agg.alias)),
        ));

        // AVG algebraic path: include auxiliary column updates in the merge CTE.
        // These flow through to the final CTE and ultimately to the MERGE SET.
        if is_algebraic_via_aux(agg) {
            let st_alias = st_col_name(&agg.alias);
            let aux_sum = quote_ident(&format!("__pgt_aux_sum_{st_alias}"));
            let aux_cnt = quote_ident(&format!("__pgt_aux_count_{st_alias}"));
            let ins_sum = quote_ident(&format!("__ins_{}", agg.alias));
            let del_sum = quote_ident(&format!("__del_{}", agg.alias));
            let ins_cnt = quote_ident(&format!("__ins_count_{}", agg.alias));
            let del_cnt = quote_ident(&format!("__del_count_{}", agg.alias));

            merge_selects.push(format!(
                "COALESCE(st.{aux_sum}, 0) + COALESCE(d.{ins_sum}, 0) - COALESCE(d.{del_sum}, 0) AS {}",
                quote_ident(&format!("new___pgt_aux_sum_{}", agg.alias)),
            ));
            merge_selects.push(format!(
                "COALESCE(st.{aux_cnt}, 0) + COALESCE(d.{ins_cnt}, 0) - COALESCE(d.{del_cnt}, 0) AS {}",
                quote_ident(&format!("new___pgt_aux_count_{}", agg.alias)),
            ));
        }

        // STDDEV/VAR: also update the sum-of-squares auxiliary column.
        if agg.function.needs_sum_of_squares() && !agg.is_distinct {
            let st_alias = st_col_name(&agg.alias);
            let aux_sum2 = quote_ident(&format!("__pgt_aux_sum2_{st_alias}"));
            let ins_sum2 = quote_ident(&format!("__ins_sum2_{}", agg.alias));
            let del_sum2 = quote_ident(&format!("__del_sum2_{}", agg.alias));

            merge_selects.push(format!(
                "COALESCE(st.{aux_sum2}, 0) + COALESCE(d.{ins_sum2}, 0) - COALESCE(d.{del_sum2}, 0) AS {}",
                quote_ident(&format!("new___pgt_aux_sum2_{}", agg.alias)),
            ));
        }

        // P3-2: CORR/COVAR/REGR_* — update cross-product auxiliary columns.
        if agg.function.needs_cross_products() && !agg.is_distinct {
            let st_alias = st_col_name(&agg.alias);
            for suffix in &["sumx", "sumy", "sumxy", "sumx2", "sumy2"] {
                let aux = quote_ident(&format!("__pgt_aux_{suffix}_{st_alias}"));
                let ins = quote_ident(&format!("__ins_{suffix}_{}", agg.alias));
                let del = quote_ident(&format!("__del_{suffix}_{}", agg.alias));
                merge_selects.push(format!(
                    "COALESCE(st.{aux}, 0) + COALESCE(d.{ins}, 0) - COALESCE(d.{del}, 0) AS {}",
                    quote_ident(&format!("new___pgt_aux_{suffix}_{}", agg.alias)),
                ));
            }
        }

        // Update the SUM nonnull-count auxiliary column.
        if agg_has_nonnull_aux {
            let st_alias = st_col_name(&agg.alias);
            let aux_nonnull = quote_ident(&format!("__pgt_aux_nonnull_{st_alias}"));
            let ins_nonnull = quote_ident(&format!("__ins_nonnull_{}", agg.alias));
            let del_nonnull = quote_ident(&format!("__del_nonnull_{}", agg.alias));
            let algebraic = format!(
                "COALESCE(st.{aux_nonnull}, 0) + COALESCE(d.{ins_nonnull}, 0) - COALESCE(d.{del_nonnull}, 0)"
            );
            let new_nonnull = if use_having_rescan {
                format!(
                    "CASE WHEN st.__pgt_count IS NULL \
                     THEN COALESCE(r.{}, 0) \
                     ELSE {algebraic} END",
                    quote_ident(&format!("__pgt_nonnull_{}", agg.alias)),
                )
            } else {
                algebraic
            };
            merge_selects.push(format!(
                "{new_nonnull} AS {}",
                quote_ident(&format!("new___pgt_aux_nonnull_{}", agg.alias)),
            ));
        }
    }

    // ── Scalar-aggregate guard ───────────────────────────────────
    //
    // PostgreSQL scalar aggregates (no GROUP BY) always return exactly
    // one row: `SELECT SUM(x) FROM empty_table` → 1 row (NULL).  The
    // singleton ST row must **never** be deleted, so we omit the 'D'
    // classification for scalar aggregates and emit 'U' instead.
    let is_scalar_agg = group_by.is_empty();

    // Action classification (same COALESCE guards as new_count)
    let action_case = if is_scalar_agg {
        "\
CASE
    WHEN st.__pgt_count IS NULL AND (COALESCE(d.__ins_count, 0) - COALESCE(d.__del_count, 0)) > 0 THEN 'I'
    ELSE 'U'
END AS __pgt_meta_action"
            .to_string()
    } else {
        "\
CASE
    WHEN st.__pgt_count IS NULL AND (COALESCE(d.__ins_count, 0) - COALESCE(d.__del_count, 0)) > 0 THEN 'I'
    WHEN COALESCE(st.__pgt_count, 0) + COALESCE(d.__ins_count, 0) - COALESCE(d.__del_count, 0) <= 0 THEN 'D'
    ELSE 'U'
END AS __pgt_meta_action"
            .to_string()
    };
    merge_selects.push(action_case);

    // Join condition on group-by columns
    let join_cond = if group_output.is_empty() {
        "TRUE".to_string()
    } else {
        group_output
            .iter()
            .map(|c| {
                let st_c = st_col_name(c);
                format!(
                    "st.{st_qc} IS NOT DISTINCT FROM d.{d_qc}",
                    st_qc = quote_ident(&st_c),
                    d_qc = quote_ident(c),
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    };

    // Optional LEFT JOIN to the rescan CTE for group-rescan aggregates
    let rescan_join = if let Some(ref rc) = rescan_cte {
        let rescan_join_cond = if group_output.is_empty() {
            "TRUE".to_string()
        } else {
            group_output
                .iter()
                .map(|c| format!("r.{qc} IS NOT DISTINCT FROM d.{qc}", qc = quote_ident(c)))
                .collect::<Vec<_>>()
                .join(" AND ")
        };
        format!("\nLEFT JOIN {rc} r ON {rescan_join_cond}")
    } else {
        String::new()
    };

    let merge_sql = format!(
        "SELECT {selects}\nFROM {delta_cte} d\nLEFT JOIN {st_table} st ON {join_cond}{rescan_join}",
        selects = merge_selects.join(",\n       "),
    );
    ctx.add_cte(merge_cte.clone(), merge_sql);

    // ── CTE 3: Single-row-per-group emit ────────────────────────────
    //
    // Each group produces exactly ONE delta row. MERGE handles all
    // three cases correctly with a single action per __pgt_row_id:
    //
    //   I row  → action='I', new values  → NOT MATCHED → INSERT
    //   D row  → action='D', old values  → MATCHED     → DELETE
    //   U row  → action='I', new values  → MATCHED     → UPDATE SET
    //
    // Because the output is already 1-row-per-group-key, the downstream
    // DISTINCT ON sort in the MERGE USING clause can be skipped entirely
    // (is_deduplicated = true).
    let final_cte = ctx.next_cte_name("agg_final");

    // Build output column list
    let mut output_cols = Vec::new();
    output_cols.extend(group_output.iter().cloned());
    output_cols.push("__pgt_count".to_string());
    for agg in aggregates {
        output_cols.push(agg.alias.clone());
        // AVG/STDDEV/VAR auxiliary columns flow through to the MERGE
        if is_algebraic_via_aux(agg) {
            output_cols.push(format!("__pgt_aux_sum_{}", agg.alias));
            output_cols.push(format!("__pgt_aux_count_{}", agg.alias));
        }
        // STDDEV/VAR: also propagate sum-of-squares
        if agg.function.needs_sum_of_squares() && !agg.is_distinct {
            output_cols.push(format!("__pgt_aux_sum2_{}", agg.alias));
        }
        // P3-2: CORR/COVAR/REGR_* — propagate cross-product auxiliary columns
        if agg.function.needs_cross_products() && !agg.is_distinct {
            for suffix in &["sumx", "sumy", "sumxy", "sumx2", "sumy2"] {
                output_cols.push(format!("__pgt_aux_{suffix}_{}", agg.alias));
            }
        }
        // SUM — propagate nonnull-count auxiliary column
        if matches!(agg.function, AggFunc::Sum) && !agg.is_distinct {
            output_cols.push(format!("__pgt_aux_nonnull_{}", agg.alias));
        }
    }

    let group_col_refs = group_output
        .iter()
        .map(|c| format!("m.{}", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(", ");

    let extra_group = if group_col_refs.is_empty() {
        String::new()
    } else {
        format!("{group_col_refs}, ")
    };

    // Build CASE expressions: D → old values, I/U → new values
    let count_case = "CASE WHEN m.__pgt_meta_action = 'D' THEN m.old_count ELSE m.new_count END";

    let mut agg_cases: Vec<String> = Vec::new();
    for agg in aggregates {
        let new_col = quote_ident(&format!("new_{}", agg.alias));
        let old_col = quote_ident(&format!("old_{}", agg.alias));
        // For scalar aggregates, SUM/AVG (and similar nullable aggs) must return
        // NULL — not 0 — when new_count drops to 0, matching PostgreSQL's
        // `SELECT SUM(x) FROM empty_table` → NULL semantics.  COUNT(*) and
        // COUNT(col) correctly yield 0 from the count arithmetic, so they
        // don't need this override.
        let needs_null_on_empty = is_scalar_agg
            && matches!(
                agg.function,
                AggFunc::Sum
                    | AggFunc::Min
                    | AggFunc::Max
                    | AggFunc::Avg
                    | AggFunc::StddevPop
                    | AggFunc::StddevSamp
                    | AggFunc::VarPop
                    | AggFunc::VarSamp
            );
        if needs_null_on_empty {
            agg_cases.push(format!(
                "CASE WHEN m.__pgt_meta_action = 'D' THEN m.{old_col} \
                 WHEN m.new_count <= 0 THEN NULL \
                 ELSE m.{new_col} END AS {}",
                quote_ident(&agg.alias),
            ));
        } else {
            agg_cases.push(format!(
                "CASE WHEN m.__pgt_meta_action = 'D' THEN m.{old_col} ELSE m.{new_col} END AS {}",
                quote_ident(&agg.alias),
            ));
        }

        // AVG auxiliary columns: propagate the algebraically computed values
        if is_algebraic_via_aux(agg) {
            let new_aux_sum = quote_ident(&format!("new___pgt_aux_sum_{}", agg.alias));
            let new_aux_cnt = quote_ident(&format!("new___pgt_aux_count_{}", agg.alias));
            let aux_sum_alias = quote_ident(&format!("__pgt_aux_sum_{}", agg.alias));
            let aux_cnt_alias = quote_ident(&format!("__pgt_aux_count_{}", agg.alias));

            // D events: emit 0 (the group is being deleted)
            // I/U events: emit the algebraically computed new values
            agg_cases.push(format!(
                "CASE WHEN m.__pgt_meta_action = 'D' THEN 0 ELSE m.{new_aux_sum} END AS {aux_sum_alias}",
            ));
            agg_cases.push(format!(
                "CASE WHEN m.__pgt_meta_action = 'D' THEN 0 ELSE m.{new_aux_cnt} END AS {aux_cnt_alias}",
            ));
        }

        // STDDEV/VAR: propagate the sum-of-squares auxiliary column
        if agg.function.needs_sum_of_squares() && !agg.is_distinct {
            let new_aux_sum2 = quote_ident(&format!("new___pgt_aux_sum2_{}", agg.alias));
            let aux_sum2_alias = quote_ident(&format!("__pgt_aux_sum2_{}", agg.alias));

            agg_cases.push(format!(
                "CASE WHEN m.__pgt_meta_action = 'D' THEN 0 ELSE m.{new_aux_sum2} END AS {aux_sum2_alias}",
            ));
        }

        // P3-2: CORR/COVAR/REGR_* — propagate cross-product auxiliary columns
        if agg.function.needs_cross_products() && !agg.is_distinct {
            for suffix in &["sumx", "sumy", "sumxy", "sumx2", "sumy2"] {
                let new_col = quote_ident(&format!("new___pgt_aux_{suffix}_{}", agg.alias));
                let out_alias = quote_ident(&format!("__pgt_aux_{suffix}_{}", agg.alias));
                agg_cases.push(format!(
                    "CASE WHEN m.__pgt_meta_action = 'D' THEN 0 ELSE m.{new_col} END AS {out_alias}",
                ));
            }
        }

        // Propagate the SUM nonnull-count auxiliary column.
        // D events reset to 0 (group deleted). I/U events use the algebraically
        // maintained new value.
        if matches!(agg.function, AggFunc::Sum) && !agg.is_distinct {
            let new_aux_nonnull = quote_ident(&format!("new___pgt_aux_nonnull_{}", agg.alias));
            let aux_nonnull_alias = quote_ident(&format!("__pgt_aux_nonnull_{}", agg.alias));

            agg_cases.push(format!(
                "CASE WHEN m.__pgt_meta_action = 'D' THEN 0 ELSE m.{new_aux_nonnull} END AS {aux_nonnull_alias}",
            ));
        }
    }

    let extra_agg_cases = if agg_cases.is_empty() {
        String::new()
    } else {
        format!(",\n       {}", agg_cases.join(",\n       "))
    };

    // ── G-S1: Build change-detection guard for UPDATE rows ──────────
    //
    // When a source row changes a non-aggregated column, the group count
    // and all aggregate values stay identical — emitting a row would
    // be a no-op MERGE. Skip U rows entirely when nothing changed.
    //
    // Guard: (new_count IS DISTINCT FROM old_count)
    //     OR (new_agg1 IS DISTINCT FROM old_agg1)
    //     OR ...
    //
    // For IS DISTINCT FROM, NULL vs NULL = same, NULL vs value = different.
    // This is correct for brand-new groups (old = NULL, new = <value>).
    let mut change_checks = vec!["m.new_count IS DISTINCT FROM m.old_count".to_string()];
    for agg in aggregates {
        change_checks.push(format!(
            "m.{new}::text IS DISTINCT FROM m.{old}::text",
            new = quote_ident(&format!("new_{}", agg.alias)),
            old = quote_ident(&format!("old_{}", agg.alias)),
        ));
    }
    let change_guard = change_checks.join("\n          OR ");

    let final_sql = format!(
        "\
SELECT m.__pgt_row_id,
       CASE WHEN m.__pgt_meta_action = 'D' THEN 'D' ELSE 'I' END AS __pgt_action,
       {extra_group}{count_case} AS __pgt_count{extra_agg_cases}
FROM {merge_cte} m
WHERE m.__pgt_meta_action IN ('I', 'D')
   OR (m.__pgt_meta_action = 'U'
       AND ({change_guard}))",
    );

    ctx.add_cte(final_cte.clone(), final_sql);

    Ok(DiffResult {
        cte_name: final_cte,
        columns: output_cols,
        is_deduplicated: true,
        has_key_changed: false,
    })
}

/// Generate SUM expressions for INSERT/DELETE tracking in the delta CTE.
///
/// Aggregate arguments are stripped of table qualifiers because the child
/// CTE (e.g. join delta) outputs unqualified column names.
///
/// When a FILTER clause is present, its condition is added as an additional
/// AND guard in all CASE WHEN expressions, so only rows passing the filter
/// contribute to the aggregate delta.
pub fn agg_delta_exprs(agg: &AggExpr, child_cols: &[String]) -> (String, String) {
    if agg.is_distinct {
        return (
            "SUM(CASE WHEN __pgt_action = 'I' THEN 1 ELSE 0 END)".to_string(),
            "SUM(CASE WHEN __pgt_action = 'D' THEN 1 ELSE 0 END)".to_string(),
        );
    }

    let filter_sql = agg
        .filter
        .as_ref()
        .map(|f| resolve_expr_for_child(f, child_cols));
    let filter_and = filter_sql
        .as_ref()
        .map(|f| format!(" AND {f}"))
        .unwrap_or_default();

    match &agg.function {
        AggFunc::CountStar => (
            format!("SUM(CASE WHEN __pgt_action = 'I'{filter_and} THEN 1 ELSE 0 END)"),
            format!("SUM(CASE WHEN __pgt_action = 'D'{filter_and} THEN 1 ELSE 0 END)"),
        ),
        AggFunc::Count => {
            let col = agg
                .argument
                .as_ref()
                .map(|e| resolve_expr_for_child(e, child_cols))
                .unwrap_or("*".into());
            (
                format!(
                    "SUM(CASE WHEN __pgt_action = 'I' AND {col} IS NOT NULL{filter_and} THEN 1 ELSE 0 END)"
                ),
                format!(
                    "SUM(CASE WHEN __pgt_action = 'D' AND {col} IS NOT NULL{filter_and} THEN 1 ELSE 0 END)"
                ),
            )
        }
        AggFunc::Sum => {
            let col = agg
                .argument
                .as_ref()
                .map(|e| resolve_expr_for_child(e, child_cols))
                .unwrap_or("0".into());
            (
                format!("SUM(CASE WHEN __pgt_action = 'I'{filter_and} THEN {col} ELSE 0 END)"),
                format!("SUM(CASE WHEN __pgt_action = 'D'{filter_and} THEN {col} ELSE 0 END)"),
            )
        }
        // AVG: track SUM of argument values in __ins_/__del_ columns.
        // A separate call to agg_delta_count_exprs produces the non-NULL
        // count columns (__ins_count_/__del_count_) needed by the merge.
        AggFunc::Avg => {
            let col = agg
                .argument
                .as_ref()
                .map(|e| resolve_expr_for_child(e, child_cols))
                .unwrap_or("0".into());
            (
                format!("SUM(CASE WHEN __pgt_action = 'I'{filter_and} THEN {col} ELSE 0 END)"),
                format!("SUM(CASE WHEN __pgt_action = 'D'{filter_and} THEN {col} ELSE 0 END)"),
            )
        }
        // STDDEV/VAR: track SUM of argument values (same as AVG). Separate
        // calls produce the non-NULL count and sum-of-squares columns.
        AggFunc::StddevPop | AggFunc::StddevSamp | AggFunc::VarPop | AggFunc::VarSamp => {
            let col = agg
                .argument
                .as_ref()
                .map(|e| resolve_expr_for_child(e, child_cols))
                .unwrap_or("0".into());
            (
                format!("SUM(CASE WHEN __pgt_action = 'I'{filter_and} THEN {col} ELSE 0 END)"),
                format!("SUM(CASE WHEN __pgt_action = 'D'{filter_and} THEN {col} ELSE 0 END)"),
            )
        }
        // P3-2: CORR/COVAR/REGR_* — the main delta tracks SUM(Y) for the shared
        // sum/count auxiliary path. Cross-product columns (sumx, sumy, sumxy, etc.)
        // are tracked separately by agg_covar_delta_exprs.
        AggFunc::Corr
        | AggFunc::CovarPop
        | AggFunc::CovarSamp
        | AggFunc::RegrAvgx
        | AggFunc::RegrAvgy
        | AggFunc::RegrCount
        | AggFunc::RegrIntercept
        | AggFunc::RegrR2
        | AggFunc::RegrSlope
        | AggFunc::RegrSxx
        | AggFunc::RegrSxy
        | AggFunc::RegrSyy => {
            let y_col = agg
                .argument
                .as_ref()
                .map(|e| resolve_expr_for_child(e, child_cols))
                .unwrap_or("0".into());
            let x_col = agg
                .second_arg
                .as_ref()
                .map(|e| resolve_expr_for_child(e, child_cols))
                .unwrap_or("0".into());
            // Only count non-null pairs (both X and Y non-null)
            let guard = format!("{x_col} IS NOT NULL AND {y_col} IS NOT NULL");
            (
                format!(
                    "SUM(CASE WHEN __pgt_action = 'I' AND {guard}{filter_and} THEN ({y_col}) ELSE 0 END)"
                ),
                format!(
                    "SUM(CASE WHEN __pgt_action = 'D' AND {guard}{filter_and} THEN ({y_col}) ELSE 0 END)"
                ),
            )
        }
        AggFunc::Min | AggFunc::Max => {
            let col = agg
                .argument
                .as_ref()
                .map(|e| resolve_expr_for_child(e, child_cols))
                .unwrap_or("NULL".into());
            let func = agg.function.sql_name();
            (
                format!("{func}(CASE WHEN __pgt_action = 'I'{filter_and} THEN {col} END)"),
                format!("{func}(CASE WHEN __pgt_action = 'D'{filter_and} THEN {col} END)"),
            )
        }
        // Group-rescan aggregates: track insertions/deletions as simple counts.
        // Any change to a group triggers a NULL sentinel in the merge, causing
        // the MERGE layer to re-aggregate the entire group.
        _ if agg.function.is_group_rescan() => {
            let col = agg
                .argument
                .as_ref()
                .map(|e| resolve_expr_for_child(e, child_cols))
                .unwrap_or("1".into());
            // We only need to detect "any change happened" — counting suffices.
            (
                format!(
                    "SUM(CASE WHEN __pgt_action = 'I'{filter_and} AND {col} IS NOT NULL THEN 1 ELSE 0 END)"
                ),
                format!(
                    "SUM(CASE WHEN __pgt_action = 'D'{filter_and} AND {col} IS NOT NULL THEN 1 ELSE 0 END)"
                ),
            )
        }
        _ => unreachable!("unexpected AggFunc variant in agg_delta_exprs"),
    }
}

/// Generate non-NULL COUNT expressions for AVG auxiliary count tracking.
///
/// Returns `(ins_count_expr, del_count_expr)` — counts of non-NULL argument
/// values in the insert and delete sides of the delta. AVG needs these to
/// maintain its `__pgt_aux_count_{alias}` column algebraically.
///
/// For CORR/COVAR/REGR_* aggregates: counts non-null PAIRS (both X and Y
/// must be non-NULL) per the SQL standard for regression function semantics.
fn agg_avg_count_delta_exprs(agg: &AggExpr, child_cols: &[String]) -> (String, String) {
    let filter_sql = agg
        .filter
        .as_ref()
        .map(|f| resolve_expr_for_child(f, child_cols));
    let filter_and = filter_sql
        .as_ref()
        .map(|f| format!(" AND {f}"))
        .unwrap_or_default();

    // For regression aggregates, both X and Y must be non-null.
    let non_null_guard = if agg.function.needs_cross_products() {
        let y_col = agg
            .argument
            .as_ref()
            .map(|e| resolve_expr_for_child(e, child_cols))
            .unwrap_or_else(|| "0".into());
        let x_col = agg
            .second_arg
            .as_ref()
            .map(|e| resolve_expr_for_child(e, child_cols))
            .unwrap_or_else(|| "0".into());
        format!("{x_col} IS NOT NULL AND {y_col} IS NOT NULL")
    } else {
        let col = agg
            .argument
            .as_ref()
            .map(|e| resolve_expr_for_child(e, child_cols))
            .unwrap_or_else(|| "1".into());
        format!("{col} IS NOT NULL")
    };

    (
        format!(
            "SUM(CASE WHEN __pgt_action = 'I' AND {non_null_guard}{filter_and} THEN 1 ELSE 0 END)"
        ),
        format!(
            "SUM(CASE WHEN __pgt_action = 'D' AND {non_null_guard}{filter_and} THEN 1 ELSE 0 END)"
        ),
    )
}

/// Generate sum-of-squares delta expressions for STDDEV/VAR algebraic tracking.
///
/// Returns `(ins_sum2_expr, del_sum2_expr)` — sums of `arg * arg` for the
/// insert and delete sides. STDDEV/VAR needs these to maintain the
/// `__pgt_aux_sum2_{alias}` column algebraically.
fn agg_sum2_delta_exprs(agg: &AggExpr, child_cols: &[String]) -> (String, String) {
    let filter_sql = agg
        .filter
        .as_ref()
        .map(|f| resolve_expr_for_child(f, child_cols));
    let filter_and = filter_sql
        .as_ref()
        .map(|f| format!(" AND {f}"))
        .unwrap_or_default();

    let col = agg
        .argument
        .as_ref()
        .map(|e| resolve_expr_for_child(e, child_cols))
        .unwrap_or("1".into());
    let cast_type = agg
        .statistical_accumulator_type()
        .map(|ty| ty.sql.as_str())
        .unwrap_or("numeric");
    (
        format!(
            "SUM(CASE WHEN __pgt_action = 'I'{filter_and} \
             THEN ({col})::{cast_type} * ({col})::{cast_type} ELSE 0 END)"
        ),
        format!(
            "SUM(CASE WHEN __pgt_action = 'D'{filter_and} \
             THEN ({col})::{cast_type} * ({col})::{cast_type} ELSE 0 END)"
        ),
    )
}

/// Generate non-NULL COUNT expressions for SUM nonnull-count auxiliary tracking (P2-2).
///
/// Returns `(ins_nonnull_expr, del_nonnull_expr)` — counts of non-NULL argument
/// values in the insert and delete sides of the delta. This enables
/// NULL-transition correction without a full-group rescan.
fn agg_nonnull_delta_exprs(agg: &AggExpr, child_cols: &[String]) -> (String, String) {
    let filter_sql = agg
        .filter
        .as_ref()
        .map(|f| resolve_expr_for_child(f, child_cols));
    let filter_and = filter_sql
        .as_ref()
        .map(|f| format!(" AND {f}"))
        .unwrap_or_default();

    let col = agg
        .argument
        .as_ref()
        .map(|e| resolve_expr_for_child(e, child_cols))
        .unwrap_or_else(|| "1".to_string());
    (
        format!(
            "SUM(CASE WHEN __pgt_action = 'I' AND {col} IS NOT NULL{filter_and} THEN 1 ELSE 0 END)::bigint"
        ),
        format!(
            "SUM(CASE WHEN __pgt_action = 'D' AND {col} IS NOT NULL{filter_and} THEN 1 ELSE 0 END)::bigint"
        ),
    )
}

/// Generate cross-product delta expressions for CORR/COVAR/REGR_* (P3-2).
///
/// Returns `(insert_exprs, delete_exprs)` where each is a tuple of 5 SQL
/// expressions: `(sum_x, sum_y, sum_xy, sum_x2, sum_y2)`.
///
/// Regression aggregates use `(Y, X)` argument order per the SQL standard:
/// - `argument` = Y (dependent variable)
/// - `second_arg` = X (independent variable)
///
/// Five-element tuple for cross-product accumulator expressions (sumx, sumy, sumxy, sumx2, sumy2).
type CovarDeltaTuple = (String, String, String, String, String);

/// Pairs where BOTH X and Y are NULL are excluded (matching PostgreSQL's
/// REGR_COUNT semantics which only counts non-null pairs).
fn agg_covar_delta_exprs(
    agg: &AggExpr,
    child_cols: &[String],
) -> (CovarDeltaTuple, CovarDeltaTuple) {
    let filter_sql = agg
        .filter
        .as_ref()
        .map(|f| resolve_expr_for_child(f, child_cols));
    let filter_and = filter_sql
        .as_ref()
        .map(|f| format!(" AND {f}"))
        .unwrap_or_default();

    let y_col = agg
        .argument
        .as_ref()
        .map(|e| resolve_expr_for_child(e, child_cols))
        .unwrap_or_else(|| "0".into());
    let x_col = agg
        .second_arg
        .as_ref()
        .map(|e| resolve_expr_for_child(e, child_cols))
        .unwrap_or_else(|| "0".into());
    let cast_type = agg
        .statistical_accumulator_type()
        .map(|ty| ty.sql.as_str())
        .unwrap_or("numeric");

    // Only count rows where BOTH x and y are non-NULL (SQL standard for regression)
    let non_null_guard = format!("{x_col} IS NOT NULL AND {y_col} IS NOT NULL");

    let ins = (
        format!(
            "SUM(CASE WHEN __pgt_action = 'I' AND {non_null_guard}{filter_and} THEN ({x_col}) ELSE 0 END)"
        ),
        format!(
            "SUM(CASE WHEN __pgt_action = 'I' AND {non_null_guard}{filter_and} THEN ({y_col}) ELSE 0 END)"
        ),
        format!(
            "SUM(CASE WHEN __pgt_action = 'I' AND {non_null_guard}{filter_and} \
         THEN ({x_col})::{cast_type} * ({y_col})::{cast_type} ELSE 0 END)"
        ),
        format!(
            "SUM(CASE WHEN __pgt_action = 'I' AND {non_null_guard}{filter_and} \
         THEN ({x_col})::{cast_type} * ({x_col})::{cast_type} ELSE 0 END)"
        ),
        format!(
            "SUM(CASE WHEN __pgt_action = 'I' AND {non_null_guard}{filter_and} \
         THEN ({y_col})::{cast_type} * ({y_col})::{cast_type} ELSE 0 END)"
        ),
    );
    let del = (
        format!(
            "SUM(CASE WHEN __pgt_action = 'D' AND {non_null_guard}{filter_and} THEN ({x_col}) ELSE 0 END)"
        ),
        format!(
            "SUM(CASE WHEN __pgt_action = 'D' AND {non_null_guard}{filter_and} THEN ({y_col}) ELSE 0 END)"
        ),
        format!(
            "SUM(CASE WHEN __pgt_action = 'D' AND {non_null_guard}{filter_and} \
         THEN ({x_col})::{cast_type} * ({y_col})::{cast_type} ELSE 0 END)"
        ),
        format!(
            "SUM(CASE WHEN __pgt_action = 'D' AND {non_null_guard}{filter_and} \
         THEN ({x_col})::{cast_type} * ({x_col})::{cast_type} ELSE 0 END)"
        ),
        format!(
            "SUM(CASE WHEN __pgt_action = 'D' AND {non_null_guard}{filter_and} \
         THEN ({y_col})::{cast_type} * ({y_col})::{cast_type} ELSE 0 END)"
        ),
    );
    (ins, del)
}

/// Generate the merge expression for computing the new aggregate value.
///
/// When `has_rescan` is true, group-rescan aggregates reference the rescan
/// CTE (`r.{alias}`) instead of producing a NULL sentinel. This correctly
/// re-aggregates the affected group from source data.
///
/// When no column aliasing is in effect, use [`agg_merge_expr`] which
/// defaults `st_col` to `agg.alias`.
fn agg_merge_expr_mapped(
    agg: &AggExpr,
    has_rescan: bool,
    having_rescan: bool,
    has_nonnull_aux: bool,
    st_col: &str,
    _else_default: Option<&str>,
) -> String {
    let alias = &agg.alias;
    let qt = quote_ident(st_col);
    let r_qt = quote_ident(alias);

    if agg.is_distinct {
        let ins = quote_ident(&format!("__ins_{alias}"));
        let del = quote_ident(&format!("__del_{alias}"));
        if has_rescan {
            return format!(
                "CASE WHEN COALESCE(d.{ins}, 0) > 0 OR COALESCE(d.{del}, 0) > 0 \
                 THEN r.{r_qt} \
                 ELSE st.{qt} END"
            );
        }

        return format!(
            "CASE WHEN COALESCE(d.{ins}, 0) > 0 OR COALESCE(d.{del}, 0) > 0 \
             THEN NULL \
             ELSE st.{qt} END"
        );
    }

    match &agg.function {
        AggFunc::CountStar | AggFunc::Count => {
            let ins = quote_ident(&format!("__ins_{alias}"));
            let del = quote_ident(&format!("__del_{alias}"));
            if having_rescan {
                // For groups not yet in the ST (below HAVING threshold), use the
                // full rescan count so the stored value is correct from the start.
                format!(
                    "CASE WHEN st.{qt} IS NULL \
                     THEN COALESCE(r.{r_qt}, 0) \
                     ELSE COALESCE(st.{qt}, 0) + COALESCE(d.{ins}, 0) - COALESCE(d.{del}, 0) END",
                )
            } else {
                format!("COALESCE(st.{qt}, 0) + COALESCE(d.{ins}, 0) - COALESCE(d.{del}, 0)",)
            }
        }
        AggFunc::Sum => {
            let ins = quote_ident(&format!("__ins_{alias}"));
            let del = quote_ident(&format!("__del_{alias}"));
            if has_nonnull_aux {
                // P2-2: Nonnull-count auxiliary available — use algebraic
                // NULL-transition correction.
                //
                // new_nonnull = old_nonnull + ins_nonnull - del_nonnull
                //   > 0 → at least one non-NULL argument value remains in the
                //          group: the algebraic SUM formula is safe.
                //   = 0 → all argument values in the group are now NULL.
                //          Restore NULL here; an outer Project applies
                //          COALESCE, if the defining query requested it.
                let aux_nonnull = quote_ident(&format!("__pgt_aux_nonnull_{st_col}"));
                let ins_nonnull = quote_ident(&format!("__ins_nonnull_{alias}"));
                let del_nonnull = quote_ident(&format!("__del_nonnull_{alias}"));
                let algebraic =
                    format!("COALESCE(st.{qt}, 0) + COALESCE(d.{ins}, 0) - COALESCE(d.{del}, 0)");
                let value = if having_rescan {
                    format!("CASE WHEN st.__pgt_count IS NULL THEN r.{r_qt} ELSE {algebraic} END")
                } else {
                    algebraic
                };
                format!(
                    "CASE \
                     WHEN (COALESCE(st.{aux_nonnull}, 0) + COALESCE(d.{ins_nonnull}, 0) - COALESCE(d.{del_nonnull}, 0)) > 0 \
                     THEN {value} \
                     ELSE NULL \
                     END",
                )
            } else if has_rescan {
                // Rescan CTE available (HAVING context):
                // use the re-aggregated value for any changed group.
                format!(
                    "CASE WHEN COALESCE(d.{ins}, 0) > 0 OR COALESCE(d.{del}, 0) > 0 \
                     THEN r.{r_qt} \
                     ELSE st.{qt} END",
                )
            } else if having_rescan {
                // Safety net (having_rescan should always imply has_rescan,
                // but guard against edge cases).
                format!(
                    "CASE WHEN st.{qt} IS NULL \
                     THEN COALESCE(r.{r_qt}, 0) \
                     ELSE COALESCE(st.{qt}, 0) + COALESCE(d.{ins}, 0) - COALESCE(d.{del}, 0) END",
                )
            } else {
                format!("COALESCE(st.{qt}, 0) + COALESCE(d.{ins}, 0) - COALESCE(d.{del}, 0)",)
            }
        }
        // AVG: algebraic via auxiliary SUM/COUNT columns on the ST.
        // new_aux_sum  = old_aux_sum + ins_sum - del_sum
        // new_aux_count = old_aux_count + ins_count - del_count
        // new_avg = new_aux_sum / NULLIF(new_aux_count, 0)
        //
        // The actual AVG user column is computed from the auxiliary columns
        // in the final CTE.  This merge expression computes the new AVG
        // value directly from the algebraically-maintained auxiliaries.
        AggFunc::Avg => {
            let ins_sum = quote_ident(&format!("__ins_{alias}"));
            let del_sum = quote_ident(&format!("__del_{alias}"));
            let ins_cnt = quote_ident(&format!("__ins_count_{alias}"));
            let del_cnt = quote_ident(&format!("__del_count_{alias}"));
            let aux_sum_col = quote_ident(&format!("__pgt_aux_sum_{st_col}"));
            let aux_count_col = quote_ident(&format!("__pgt_aux_count_{st_col}"));

            // Compute new AVG = new_sum / NULLIF(new_count, 0)
            // new_sum = old_aux_sum + ins_sum - del_sum
            // new_count = old_aux_count + ins_count - del_count
            format!(
                "(COALESCE(st.{aux_sum_col}, 0) + COALESCE(d.{ins_sum}, 0) - COALESCE(d.{del_sum}, 0)) \
                 / NULLIF(COALESCE(st.{aux_count_col}, 0) + COALESCE(d.{ins_cnt}, 0) - COALESCE(d.{del_cnt}, 0), 0)",
            )
        }
        // STDDEV/VAR: algebraic via sum, sum-of-squares, and count auxiliaries.
        //   n = old_count + ins_count - del_count
        //   s = old_sum + ins_sum - del_sum
        //   s2 = old_sum2 + ins_sum2 - del_sum2
        //   VAR_POP  = GREATEST(0, (n*s2 - s*s) / (n*n))
        //   VAR_SAMP = GREATEST(0, (n*s2 - s*s) / (n*(n-1)))
        //   STDDEV_* = SQRT(VAR_*)
        AggFunc::StddevPop | AggFunc::StddevSamp | AggFunc::VarPop | AggFunc::VarSamp => {
            let ins_sum = quote_ident(&format!("__ins_{alias}"));
            let del_sum = quote_ident(&format!("__del_{alias}"));
            let ins_cnt = quote_ident(&format!("__ins_count_{alias}"));
            let del_cnt = quote_ident(&format!("__del_count_{alias}"));
            let ins_sum2 = quote_ident(&format!("__ins_sum2_{alias}"));
            let del_sum2 = quote_ident(&format!("__del_sum2_{alias}"));
            let aux_sum_col = quote_ident(&format!("__pgt_aux_sum_{st_col}"));
            let aux_count_col = quote_ident(&format!("__pgt_aux_count_{st_col}"));
            let aux_sum2_col = quote_ident(&format!("__pgt_aux_sum2_{st_col}"));

            let n = format!(
                "(COALESCE(st.{aux_count_col}, 0) + COALESCE(d.{ins_cnt}, 0) - COALESCE(d.{del_cnt}, 0))"
            );
            let s = format!(
                "(COALESCE(st.{aux_sum_col}, 0) + COALESCE(d.{ins_sum}, 0) - COALESCE(d.{del_sum}, 0))"
            );
            let s2 = format!(
                "(COALESCE(st.{aux_sum2_col}, 0) + COALESCE(d.{ins_sum2}, 0) - COALESCE(d.{del_sum2}, 0))"
            );
            let numer = format!("({n} * {s2} - {s} * {s})");

            let (denom, null_guard, wrap_sqrt) = match &agg.function {
                AggFunc::VarPop => (format!("{n} * {n}"), format!("{n} = 0"), false),
                AggFunc::VarSamp => (format!("{n} * ({n} - 1)"), format!("{n} <= 1"), false),
                AggFunc::StddevPop => (format!("{n} * {n}"), format!("{n} = 0"), true),
                AggFunc::StddevSamp => (format!("{n} * ({n} - 1)"), format!("{n} <= 1"), true),
                _ => unreachable!(),
            };

            let var_expr = format!("GREATEST(0, {numer} / NULLIF({denom}, 0))");
            let body = if wrap_sqrt {
                format!("SQRT({var_expr})")
            } else {
                var_expr
            };

            format!("CASE WHEN {null_guard} THEN NULL ELSE {body} END")
        }
        // P3-2: CORR/COVAR/REGR_* — algebraic via cross-product auxiliaries.
        //   n   = old_count + ins_count - del_count
        //   sx  = old_sumx + ins_sumx - del_sumx
        //   sy  = old_sumy + ins_sumy - del_sumy
        //   sxy = old_sumxy + ins_sumxy - del_sumxy
        //   sx2 = old_sumx2 + ins_sumx2 - del_sumx2
        //   sy2 = old_sumy2 + ins_sumy2 - del_sumy2
        AggFunc::Corr
        | AggFunc::CovarPop
        | AggFunc::CovarSamp
        | AggFunc::RegrAvgx
        | AggFunc::RegrAvgy
        | AggFunc::RegrCount
        | AggFunc::RegrIntercept
        | AggFunc::RegrR2
        | AggFunc::RegrSlope
        | AggFunc::RegrSxx
        | AggFunc::RegrSxy
        | AggFunc::RegrSyy => {
            let ins_cnt = quote_ident(&format!("__ins_count_{alias}"));
            let del_cnt = quote_ident(&format!("__del_count_{alias}"));
            let aux_count = quote_ident(&format!("__pgt_aux_count_{st_col}"));

            // Helper macro for cross-product auxiliary columns
            let cv = |suffix: &str| -> (String, String, String) {
                let ins = quote_ident(&format!("__ins_{suffix}_{alias}"));
                let del = quote_ident(&format!("__del_{suffix}_{alias}"));
                let aux = quote_ident(&format!("__pgt_aux_{suffix}_{st_col}"));
                (ins, del, aux)
            };
            let (ins_sx, del_sx, aux_sx) = cv("sumx");
            let (ins_sy, del_sy, aux_sy) = cv("sumy");
            let (ins_sxy, del_sxy, aux_sxy) = cv("sumxy");
            let (ins_sx2, del_sx2, aux_sx2) = cv("sumx2");
            let (ins_sy2, del_sy2, aux_sy2) = cv("sumy2");

            let n = format!(
                "(COALESCE(st.{aux_count}, 0) + COALESCE(d.{ins_cnt}, 0) - COALESCE(d.{del_cnt}, 0))"
            );
            let sx = format!(
                "(COALESCE(st.{aux_sx}, 0) + COALESCE(d.{ins_sx}, 0) - COALESCE(d.{del_sx}, 0))"
            );
            let sy = format!(
                "(COALESCE(st.{aux_sy}, 0) + COALESCE(d.{ins_sy}, 0) - COALESCE(d.{del_sy}, 0))"
            );
            let sxy = format!(
                "(COALESCE(st.{aux_sxy}, 0) + COALESCE(d.{ins_sxy}, 0) - COALESCE(d.{del_sxy}, 0))"
            );
            let sx2 = format!(
                "(COALESCE(st.{aux_sx2}, 0) + COALESCE(d.{ins_sx2}, 0) - COALESCE(d.{del_sx2}, 0))"
            );
            let sy2 = format!(
                "(COALESCE(st.{aux_sy2}, 0) + COALESCE(d.{ins_sy2}, 0) - COALESCE(d.{del_sy2}, 0))"
            );

            match &agg.function {
                AggFunc::RegrCount => {
                    // REGR_COUNT = n (number of non-null pairs)
                    format!("CASE WHEN {n} <= 0 THEN 0 ELSE {n} END")
                }
                AggFunc::RegrAvgx => {
                    // REGR_AVGX = SUM(X) / n
                    format!("CASE WHEN {n} = 0 THEN NULL ELSE {sx} / {n} END")
                }
                AggFunc::RegrAvgy => {
                    // REGR_AVGY = SUM(Y) / n
                    format!("CASE WHEN {n} = 0 THEN NULL ELSE {sy} / {n} END")
                }
                AggFunc::CovarPop => {
                    // COVAR_POP = (n*SXY - SX*SY) / (n*n)
                    let numer = format!("({n} * {sxy} - {sx} * {sy})");
                    let denom = format!("{n} * {n}");
                    format!("CASE WHEN {n} = 0 THEN NULL ELSE {numer} / NULLIF({denom}, 0) END")
                }
                AggFunc::CovarSamp => {
                    // COVAR_SAMP = (n*SXY - SX*SY) / (n*(n-1))
                    let numer = format!("({n} * {sxy} - {sx} * {sy})");
                    let denom = format!("{n} * ({n} - 1)");
                    format!("CASE WHEN {n} <= 1 THEN NULL ELSE {numer} / NULLIF({denom}, 0) END")
                }
                AggFunc::RegrSxx => {
                    // REGR_SXX = SUM(X²) - SUM(X)²/n = n*SX2 - SX*SX  (over n)
                    // Actually: REGR_SXX = SUM((X - avg_x)²) = SX2 - SX²/n
                    format!("CASE WHEN {n} = 0 THEN NULL ELSE {sx2} - {sx} * {sx} / {n} END")
                }
                AggFunc::RegrSyy => {
                    // REGR_SYY = SUM((Y - avg_y)²) = SY2 - SY²/n
                    format!("CASE WHEN {n} = 0 THEN NULL ELSE {sy2} - {sy} * {sy} / {n} END")
                }
                AggFunc::RegrSxy => {
                    // REGR_SXY = SUM((X - avg_x)*(Y - avg_y)) = SXY - SX*SY/n
                    format!("CASE WHEN {n} = 0 THEN NULL ELSE {sxy} - {sx} * {sy} / {n} END")
                }
                AggFunc::RegrSlope => {
                    // REGR_SLOPE = REGR_SXY / REGR_SXX
                    let sxy_expr = format!("({sxy} - {sx} * {sy} / {n})");
                    let sxx_expr = format!("({sx2} - {sx} * {sx} / {n})");
                    format!(
                        "CASE WHEN {n} = 0 OR {sxx_expr} = 0 THEN NULL ELSE {sxy_expr} / {sxx_expr} END"
                    )
                }
                AggFunc::RegrIntercept => {
                    // REGR_INTERCEPT = AVG(Y) - REGR_SLOPE * AVG(X)
                    let avg_y = format!("{sy} / {n}");
                    let avg_x = format!("{sx} / {n}");
                    let sxy_expr = format!("({sxy} - {sx} * {sy} / {n})");
                    let sxx_expr = format!("({sx2} - {sx} * {sx} / {n})");
                    let slope = format!("{sxy_expr} / NULLIF({sxx_expr}, 0)");
                    format!(
                        "CASE WHEN {n} = 0 OR {sxx_expr} = 0 THEN NULL ELSE {avg_y} - {slope} * {avg_x} END"
                    )
                }
                AggFunc::RegrR2 => {
                    // REGR_R2 = REGR_SXY² / (REGR_SXX * REGR_SYY)
                    // When SXX = 0 and SYY = 0 → 1.0 (all points identical)
                    // When SXX = 0 or SYY = 0 → NULL
                    let sxy_expr = format!("({sxy} - {sx} * {sy} / {n})");
                    let sxx_expr = format!("({sx2} - {sx} * {sx} / {n})");
                    let syy_expr = format!("({sy2} - {sy} * {sy} / {n})");
                    format!(
                        "CASE WHEN {n} = 0 THEN NULL \
                         WHEN {sxx_expr} = 0 AND {syy_expr} = 0 THEN 1.0 \
                         WHEN {sxx_expr} = 0 OR {syy_expr} = 0 THEN NULL \
                         ELSE ({sxy_expr}) * ({sxy_expr}) / ({sxx_expr} * {syy_expr}) END"
                    )
                }
                AggFunc::Corr => {
                    // CORR = REGR_SXY / SQRT(REGR_SXX * REGR_SYY)
                    let sxy_expr = format!("({sxy} - {sx} * {sy} / {n})");
                    let sxx_expr = format!("({sx2} - {sx} * {sx} / {n})");
                    let syy_expr = format!("({sy2} - {sy} * {sy} / {n})");
                    format!(
                        "CASE WHEN {n} = 0 THEN NULL \
                         WHEN {sxx_expr} = 0 OR {syy_expr} = 0 THEN NULL \
                         ELSE {sxy_expr} / SQRT(ABS({sxx_expr} * {syy_expr})) END"
                    )
                }
                _ => unreachable!(),
            }
        }
        AggFunc::Min | AggFunc::Max => {
            // MIN/MAX merge with group-rescan fallback.
            //
            // Case 1: The old extremum was NOT deleted (or there was no old value).
            //   → Use LEAST/GREATEST(old_value, new_inserts) = simple algebraic merge.
            //
            // Case 2: The old extremum WAS deleted.
            //   → The new extremum might be entirely different. When a rescan CTE
            //     is available (has_rescan=true), use the rescanned value from
            //     source data. Otherwise fall back to just the insert extremum
            //     (which may be NULL if there were no inserts).
            //
            // The "was deleted" check: d.__del_{alias} IS NOT NULL AND
            //   d.__del_{alias} = st.{alias} (the deleted extremum equals the stored one).
            let func = if matches!(agg.function, AggFunc::Min) {
                "LEAST"
            } else {
                "GREATEST"
            };
            let ins = quote_ident(&format!("__ins_{alias}"));
            let del = quote_ident(&format!("__del_{alias}"));
            if has_rescan {
                format!(
                    "CASE WHEN d.{del} IS NOT NULL AND d.{del} = st.{qt} \
                     THEN r.{r_qt} \
                     ELSE {func}(st.{qt}, d.{ins}) END"
                )
            } else {
                format!(
                    "CASE WHEN d.{del} IS NOT NULL AND d.{del} = st.{qt} \
                     THEN d.{ins} \
                     ELSE {func}(st.{qt}, d.{ins}) END"
                )
            }
        }
        // Group-rescan aggregates: use rescan CTE value when available,
        // or fall back to NULL sentinel.
        //
        // With rescan CTE: when any row in the group changed (ins or del > 0),
        // use the re-aggregated value from the rescan CTE (`r.{alias}`).
        //
        // Without rescan CTE (legacy fallback): return NULL sentinel.
        _ if agg.function.is_group_rescan() => {
            let ins = quote_ident(&format!("__ins_{alias}"));
            let del = quote_ident(&format!("__del_{alias}"));
            if has_rescan {
                format!(
                    "CASE WHEN COALESCE(d.{ins}, 0) > 0 OR COALESCE(d.{del}, 0) > 0 \
                     THEN r.{r_qt} \
                     ELSE st.{qt} END"
                )
            } else {
                format!(
                    "CASE WHEN COALESCE(d.{ins}, 0) > 0 OR COALESCE(d.{del}, 0) > 0 \
                     THEN NULL \
                     ELSE st.{qt} END"
                )
            }
        }
        _ => unreachable!("unexpected AggFunc variant in agg_merge_expr"),
    }
}

/// Convenience wrapper: uses the aggregate's own alias as the ST column name.
///
/// Equivalent to `agg_merge_expr_mapped(agg, has_rescan, false, false, &agg.alias, None)`.
pub fn agg_merge_expr(agg: &AggExpr, has_rescan: bool) -> String {
    agg_merge_expr_mapped(agg, has_rescan, false, false, &agg.alias, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm::operators::test_helpers::*;

    /// Wrapper for tests: calls is_direct_agg_eligible with an empty ctx
    /// (no ST sources), matching the old 3-argument signature.
    fn is_direct_agg_eligible(child: &OpTree, group_by: &[Expr], aggs: &[AggExpr]) -> bool {
        let ctx = crate::dvm::diff::DiffContext::new_standalone(
            crate::version::Frontier::default(),
            crate::version::Frontier::default(),
        );
        super::is_direct_agg_eligible(child, group_by, aggs, &ctx)
    }

    // ── is_direct_agg_eligible tests ────────────────────────────────

    #[test]
    fn test_eligible_scan_count_star() {
        let child = scan(1, "t", "public", "t", &["id", "region"]);
        let group_by = vec![colref("region")];
        let aggs = vec![count_star("cnt")];
        assert!(is_direct_agg_eligible(&child, &group_by, &aggs));
    }

    #[test]
    fn test_eligible_scan_sum_and_count() {
        let child = scan(1, "t", "public", "t", &["id", "region", "amount"]);
        let group_by = vec![colref("region")];
        let aggs = vec![sum_col("amount", "total"), count_star("cnt")];
        assert!(is_direct_agg_eligible(&child, &group_by, &aggs));
    }

    #[test]
    fn test_ineligible_non_scan_child() {
        let child = filter(
            binop(">", colref("id"), lit("0")),
            scan(1, "t", "public", "t", &["id", "region"]),
        );
        let group_by = vec![colref("region")];
        let aggs = vec![count_star("cnt")];
        assert!(!is_direct_agg_eligible(&child, &group_by, &aggs));
    }

    #[test]
    fn test_ineligible_min_max() {
        let child = scan(1, "t", "public", "t", &["id", "region", "amount"]);
        let group_by = vec![colref("region")];
        let aggs = vec![min_col("amount", "min_amt")];
        assert!(!is_direct_agg_eligible(&child, &group_by, &aggs));
    }

    #[test]
    fn test_ineligible_distinct_aggregate() {
        let child = scan(1, "t", "public", "t", &["id", "region"]);
        let group_by = vec![colref("region")];
        let aggs = vec![AggExpr {
            function: AggFunc::Count,
            argument: Some(colref("id")),
            alias: "cnt".to_string(),
            is_distinct: true,
            filter: None,
            second_arg: None,
            order_within_group: None,
            statistical_support: None,
        }];
        assert!(!is_direct_agg_eligible(&child, &group_by, &aggs));
    }

    #[test]
    fn test_eligible_no_group_by() {
        let child = scan(1, "t", "public", "t", &["amount"]);
        let group_by = vec![];
        let aggs = vec![sum_col("amount", "total")];
        assert!(is_direct_agg_eligible(&child, &group_by, &aggs));
    }

    // ── diff_aggregate integration tests ────────────────────────────

    #[test]
    fn test_diff_aggregate_count_star_with_group_by() {
        let mut ctx = test_ctx_with_st("public", "my_st");
        let child = scan(1, "orders", "public", "o", &["id", "region", "amount"]);
        let tree = aggregate(vec![colref("region")], vec![count_star("cnt")], child);
        let result = diff_aggregate(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Output: group cols + __pgt_count + aggregate aliases
        assert!(result.columns.contains(&"region".to_string()));
        assert!(result.columns.contains(&"__pgt_count".to_string()));
        assert!(result.columns.contains(&"cnt".to_string()));

        // Should use direct bypass (P5) since it's Scan → Aggregate with COUNT(*)
        // A44-10 (D+I schema): P5 uses c.action directly, no LATERAL VALUES
        assert_sql_contains(&sql, "changes_1");
        assert_sql_contains(&sql, "c.action = 'I'");
        assert_sql_contains(&sql, "c.action = 'D'");
    }

    #[test]
    fn test_diff_aggregate_sum_with_group_by() {
        let mut ctx = test_ctx_with_st("public", "my_st");
        let child = scan(1, "orders", "public", "o", &["id", "region", "amount"]);
        let tree = aggregate(
            vec![colref("region")],
            vec![sum_col("amount", "total")],
            child,
        );
        let result = diff_aggregate(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        assert!(result.columns.contains(&"total".to_string()));
        // A44-10 (D+I schema): P5 direct bypass uses c.action filtering, no LATERAL VALUES
        assert_sql_contains(&sql, "c.action = 'I'");
        assert_sql_contains(&sql, "c.action = 'D'");
        assert_sql_not_contains(&sql, "LATERAL");
    }

    #[test]
    fn test_diff_aggregate_no_group_by() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["amount"]);
        let tree = aggregate(
            vec![],
            vec![sum_col("amount", "total"), count_star("cnt")],
            child,
        );
        let result = diff_aggregate(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // No GROUP BY → singleton group hash
        assert_sql_contains(&sql, "__singleton_group");
        assert!(result.is_deduplicated);
    }

    #[test]
    fn test_diff_aggregate_standard_path_when_child_is_filter() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = filter(
            binop(">", colref("amount"), lit("0")),
            scan(1, "t", "public", "t", &["id", "region", "amount"]),
        );
        let tree = aggregate(vec![colref("region")], vec![count_star("cnt")], child);
        let result = diff_aggregate(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Should NOT use P5 direct bypass when child is not a direct Scan
        assert_sql_not_contains(&sql, "LATERAL");
        // Should use standard path with __pgt_action
        assert_sql_contains(&sql, "__pgt_action");
    }

    #[test]
    fn test_diff_aggregate_func_call_group_by_aliased_in_delta_cte() {
        // Regression test: GROUP BY with a function call expression (e.g.,
        // split_part(full_path, ' > ', 2)) must emit an explicit alias in the
        // delta CTE so the merge CTE can reference d."split_part(full_path, ' > ', 2)".
        // Without the fix, the column was implicitly named `split_part` by
        // PostgreSQL, causing "column d.split_part(full_path, ' > ', 2) does not exist".
        // Mirrors the department_report query in the getting-started tutorial.
        let func_expr = Expr::FuncCall {
            func_name: "split_part".to_string(),
            args: vec![
                colref("full_path"),
                Expr::Literal("' > '".to_string()),
                Expr::Literal("2".to_string()),
            ],
        };
        let mut ctx = test_ctx_with_st("public", "department_report");
        let child = filter(
            binop(">=", colref("depth"), lit("1")),
            scan(
                1,
                "department_stats",
                "public",
                "ds",
                &["full_path", "depth", "headcount", "total_salary"],
            ),
        );
        let tree = aggregate(
            vec![func_expr],
            vec![sum_col("headcount", "total_headcount")],
            child,
        );
        let result = diff_aggregate(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // The delta CTE must explicitly alias the function call expression so
        // it can be referenced by its full name in the merge CTE.
        // Check for: split_part(...) AS "split_part(full_path, ' > ', 2)"
        assert_sql_contains(&sql, r#"AS "split_part(full_path, ' > ', 2)""#);
        // The merge CTE must reference it with the quoted identifier.
        assert_sql_contains(&sql, r#"d."split_part(full_path, ' > ', 2)""#);
    }

    #[test]
    fn test_diff_aggregate_is_deduplicated() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["id", "region"]);
        let tree = aggregate(vec![colref("region")], vec![count_star("cnt")], child);
        let result = diff_aggregate(&mut ctx, &tree).unwrap();
        // Aggregates produce exactly one row per group → deduplicated
        assert!(result.is_deduplicated);
    }

    #[test]
    fn test_diff_aggregate_error_on_non_aggregate_node() {
        let mut ctx = test_ctx_with_st("public", "st");
        let tree = scan(1, "t", "public", "t", &["id"]);
        let result = diff_aggregate(&mut ctx, &tree);
        assert!(result.is_err());
    }

    #[test]
    fn test_diff_aggregate_change_detection_guard() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["region", "amount"]);
        let tree = aggregate(
            vec![colref("region")],
            vec![sum_col("amount", "total")],
            child,
        );
        let result = diff_aggregate(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // G-S1: change detection guard should be present
        assert_sql_contains(&sql, "IS DISTINCT FROM");
    }

    // ── agg_delta_exprs tests ───────────────────────────────────────

    #[test]
    fn test_agg_delta_exprs_count_star() {
        let agg = count_star("cnt");
        let (ins, del) = agg_delta_exprs(&agg, &[]);
        assert!(ins.contains("'I'"));
        assert!(del.contains("'D'"));
    }

    #[test]
    fn test_agg_delta_exprs_sum() {
        let agg = sum_col("amount", "total");
        let child_cols = vec!["amount".to_string()];
        let (ins, del) = agg_delta_exprs(&agg, &child_cols);
        assert!(ins.contains("amount"));
        assert!(del.contains("amount"));
    }

    #[test]
    fn test_agg_delta_exprs_count_col() {
        let agg = count_col("name", "name_count");
        let child_cols = vec!["name".to_string()];
        let (ins, del) = agg_delta_exprs(&agg, &child_cols);
        assert!(ins.contains("IS NOT NULL"));
        assert!(del.contains("IS NOT NULL"));
    }

    #[test]
    fn test_agg_delta_exprs_count_distinct_uses_change_sentinel() {
        let agg = AggExpr {
            function: AggFunc::Count,
            argument: Some(colref("name")),
            alias: "uniq".to_string(),
            is_distinct: true,
            second_arg: None,
            filter: None,
            order_within_group: None,
            statistical_support: None,
        };
        let child_cols = vec!["name".to_string()];
        let (ins, del) = agg_delta_exprs(&agg, &child_cols);
        assert_eq!(ins, "SUM(CASE WHEN __pgt_action = 'I' THEN 1 ELSE 0 END)");
        assert_eq!(del, "SUM(CASE WHEN __pgt_action = 'D' THEN 1 ELSE 0 END)");
    }

    // ── AVG algebraic path tests ────────────────────────────────────

    #[test]
    fn test_agg_delta_exprs_avg_tracks_sum() {
        // AVG delta tracking should use SUM of argument values (like SUM does)
        let agg = avg_col("score", "avg_score");
        let child_cols = vec!["score".to_string()];
        let (ins, del) = agg_delta_exprs(&agg, &child_cols);
        assert!(
            ins.contains("SUM") && ins.contains("score"),
            "AVG delta ins should SUM the argument: {ins}"
        );
        assert!(
            del.contains("SUM") && del.contains("score"),
            "AVG delta del should SUM the argument: {del}"
        );
    }

    #[test]
    fn test_agg_avg_count_delta_exprs() {
        // The companion count expressions should count non-NULL values
        let agg = avg_col("score", "avg_score");
        let child_cols = vec!["score".to_string()];
        let (ins_cnt, del_cnt) = agg_avg_count_delta_exprs(&agg, &child_cols);
        assert!(
            ins_cnt.contains("IS NOT NULL") && ins_cnt.contains("'I'"),
            "AVG count delta ins should check non-NULL: {ins_cnt}"
        );
        assert!(
            del_cnt.contains("IS NOT NULL") && del_cnt.contains("'D'"),
            "AVG count delta del should check non-NULL: {del_cnt}"
        );
    }

    #[test]
    fn test_is_algebraic_via_aux_avg() {
        let agg = avg_col("score", "avg_score");
        assert!(
            is_algebraic_via_aux(&agg),
            "AVG should be algebraic via auxiliary columns"
        );
    }

    // ── DI-8: SUM(CASE WHEN …) drift fix tests ─────────────────────

    #[test]
    fn test_sum_case_when_not_algebraically_invertible() {
        let agg = AggExpr {
            function: AggFunc::Sum,
            argument: Some(Expr::Raw(
                "CASE WHEN o_orderpriority IN ('1-URGENT','2-HIGH') THEN 1 ELSE 0 END".into(),
            )),
            alias: "high_line_count".to_string(),
            is_distinct: false,
            filter: None,
            second_arg: None,
            order_within_group: None,
            statistical_support: None,
        };
        assert!(
            !is_algebraically_invertible(&agg),
            "SUM(CASE WHEN …) should NOT be algebraically invertible (DI-8)"
        );
    }

    #[test]
    fn test_sum_plain_column_still_algebraically_invertible() {
        let agg = sum_col("amount", "total");
        assert!(
            is_algebraically_invertible(&agg),
            "SUM(col) should remain algebraically invertible"
        );
    }

    #[test]
    fn test_sum_case_when_with_leading_whitespace() {
        let agg = AggExpr {
            function: AggFunc::Sum,
            argument: Some(Expr::Raw(
                "  CASE WHEN status = 'A' THEN qty ELSE 0 END".into(),
            )),
            alias: "active_qty".to_string(),
            is_distinct: false,
            filter: None,
            second_arg: None,
            order_within_group: None,
            statistical_support: None,
        };
        assert!(
            !is_algebraically_invertible(&agg),
            "SUM(CASE …) with leading whitespace should still be caught (DI-8)"
        );
    }

    #[test]
    fn test_count_star_still_algebraically_invertible() {
        let agg = count_star("cnt");
        assert!(
            is_algebraically_invertible(&agg),
            "COUNT(*) should remain algebraically invertible"
        );
    }

    #[test]
    fn test_is_algebraic_via_aux_not_for_count() {
        let agg = count_star("cnt");
        assert!(
            !is_algebraic_via_aux(&agg),
            "COUNT(*) should not be algebraic-via-aux"
        );
    }

    #[test]
    fn test_is_algebraic_via_aux_not_for_distinct_avg() {
        let agg = AggExpr {
            function: AggFunc::Avg,
            argument: Some(colref("score")),
            alias: "avg_score".to_string(),
            is_distinct: true,
            filter: None,
            second_arg: None,
            order_within_group: None,
            statistical_support: None,
        };
        assert!(
            !is_algebraic_via_aux(&agg),
            "DISTINCT AVG should not use algebraic path"
        );
    }

    #[test]
    fn test_diff_aggregate_avg_with_group_by() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "orders", "public", "o", &["id", "region", "amount"]);
        let tree = aggregate(
            vec![colref("region")],
            vec![avg_col("amount", "avg_amt")],
            child,
        );
        let result = diff_aggregate(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Should include auxiliary columns in output
        assert!(
            result
                .columns
                .contains(&"__pgt_aux_sum_avg_amt".to_string()),
            "output should include __pgt_aux_sum_avg_amt: {:?}",
            result.columns
        );
        assert!(
            result
                .columns
                .contains(&"__pgt_aux_count_avg_amt".to_string()),
            "output should include __pgt_aux_count_avg_amt: {:?}",
            result.columns
        );
        // Should reference the auxiliary column merge formula
        assert_sql_contains(&sql, "__pgt_aux_sum_avg_amt");
        assert_sql_contains(&sql, "__pgt_aux_count_avg_amt");
        // Should track SUM/COUNT delta for AVG
        assert_sql_contains(&sql, "__ins_count_avg_amt");
        assert_sql_contains(&sql, "__del_count_avg_amt");
    }

    #[test]
    fn test_diff_aggregate_avg_no_group_by() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["amount"]);
        let tree = aggregate(vec![], vec![avg_col("amount", "avg_amt")], child);
        let result = diff_aggregate(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Scalar AVG should use singleton group hash
        assert_sql_contains(&sql, "__singleton_group");
        // Should still have auxiliary columns
        assert!(
            result
                .columns
                .contains(&"__pgt_aux_sum_avg_amt".to_string())
        );
        assert!(
            result
                .columns
                .contains(&"__pgt_aux_count_avg_amt".to_string())
        );
    }

    #[test]
    fn test_diff_aggregate_mixed_sum_avg() {
        // Test a query with both SUM and AVG to ensure they coexist
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "orders", "public", "o", &["region", "amount"]);
        let tree = aggregate(
            vec![colref("region")],
            vec![sum_col("amount", "total"), avg_col("amount", "avg_amt")],
            child,
        );
        let result = diff_aggregate(&mut ctx, &tree).unwrap();

        // SUM should be in output without aux columns
        assert!(result.columns.contains(&"total".to_string()));
        // AVG should have aux columns
        assert!(result.columns.contains(&"avg_amt".to_string()));
        assert!(
            result
                .columns
                .contains(&"__pgt_aux_sum_avg_amt".to_string())
        );
        assert!(
            result
                .columns
                .contains(&"__pgt_aux_count_avg_amt".to_string())
        );
    }

    // ── agg_merge_expr tests ────────────────────────────────────────

    #[test]
    fn test_agg_merge_expr_count_star() {
        let agg = count_star("cnt");
        let result = agg_merge_expr(&agg, false);
        assert!(result.contains("COALESCE(st.\"cnt\", 0)"));
        assert!(result.contains("__ins_cnt"));
        assert!(result.contains("__del_cnt"));
    }

    #[test]
    fn test_agg_merge_expr_sum() {
        let agg = sum_col("amount", "total");
        let result = agg_merge_expr(&agg, false);
        assert!(result.contains("COALESCE(st.\"total\", 0)"));
        assert!(result.contains("COALESCE(d.\"__ins_total\", 0)"));
    }

    #[test]
    fn test_agg_merge_expr_avg() {
        // AVG is algebraic via auxiliary columns: the merge expression
        // computes new_avg = (aux_sum + ins - del) / NULLIF(aux_count + ins - del, 0)
        let agg = avg_col("score", "avg_score");
        let result = agg_merge_expr(&agg, false);
        assert!(
            result.contains("__pgt_aux_sum_avg_score"),
            "AVG should reference __pgt_aux_sum: {result}"
        );
        assert!(
            result.contains("__pgt_aux_count_avg_score"),
            "AVG should reference __pgt_aux_count: {result}"
        );
        assert!(
            result.contains("NULLIF"),
            "AVG should use NULLIF to guard division by zero: {result}"
        );
    }

    #[test]
    fn test_agg_merge_expr_avg_algebraic_formula() {
        // Verify the full algebraic formula structure
        let agg = avg_col("amount", "avg_amt");
        let result = agg_merge_expr(&agg, false);
        // Should have: (COALESCE(st.aux_sum, 0) + COALESCE(d.ins, 0) - COALESCE(d.del, 0))
        //            / NULLIF(COALESCE(st.aux_count, 0) + COALESCE(d.ins_count, 0) - COALESCE(d.del_count, 0), 0)
        assert!(
            result.contains("__ins_avg_amt"),
            "should reference ins delta: {result}"
        );
        assert!(
            result.contains("__del_avg_amt"),
            "should reference del delta: {result}"
        );
        assert!(
            result.contains("__ins_count_avg_amt"),
            "should reference ins count delta: {result}"
        );
        assert!(
            result.contains("__del_count_avg_amt"),
            "should reference del count delta: {result}"
        );
    }

    // ── MIN/MAX merge expression tests ──────────────────────────────

    #[test]
    fn test_agg_merge_expr_min() {
        let agg = AggExpr {
            function: AggFunc::Min,
            argument: Some(colref("val")),
            alias: "min_val".to_string(),
            is_distinct: false,
            filter: None,
            second_arg: None,
            order_within_group: None,
            statistical_support: None,
        };
        let result = agg_merge_expr(&agg, false);
        // Should use LEAST for MIN
        assert!(
            result.contains("LEAST"),
            "MIN merge should use LEAST: {result}"
        );
        // Should check if deleted extremum matches stored value
        assert!(
            result.contains("__del_min_val"),
            "should reference __del_min_val: {result}"
        );
        assert!(
            result.contains("__ins_min_val"),
            "should reference __ins_min_val: {result}"
        );
        // Should have a CASE expression
        assert!(
            result.contains("CASE WHEN"),
            "should use CASE WHEN: {result}"
        );
    }

    #[test]
    fn test_agg_merge_expr_max() {
        let agg = AggExpr {
            function: AggFunc::Max,
            argument: Some(colref("val")),
            alias: "max_val".to_string(),
            is_distinct: false,
            filter: None,
            second_arg: None,
            order_within_group: None,
            statistical_support: None,
        };
        let result = agg_merge_expr(&agg, false);
        // Should use GREATEST for MAX
        assert!(
            result.contains("GREATEST"),
            "MAX merge should use GREATEST: {result}"
        );
        assert!(
            result.contains("__del_max_val"),
            "should reference __del_max_val: {result}"
        );
        assert!(
            result.contains("__ins_max_val"),
            "should reference __ins_max_val: {result}"
        );
    }

    // ── MIN/MAX delta expression tests ──────────────────────────────

    #[test]
    fn test_agg_delta_exprs_min() {
        let agg = AggExpr {
            function: AggFunc::Min,
            argument: Some(colref("val")),
            alias: "min_val".to_string(),
            is_distinct: false,
            filter: None,
            second_arg: None,
            order_within_group: None,
            statistical_support: None,
        };
        let child_cols = vec!["val".to_string()];
        let (ins, del) = agg_delta_exprs(&agg, &child_cols);
        // MIN of inserted values
        assert!(
            ins.contains("MIN") && ins.contains("'I'"),
            "MIN delta ins should use MIN: {ins}"
        );
        // MIN of deleted values
        assert!(
            del.contains("MIN") && del.contains("'D'"),
            "MIN delta del should use MIN: {del}"
        );
    }

    #[test]
    fn test_agg_delta_exprs_max() {
        let agg = AggExpr {
            function: AggFunc::Max,
            argument: Some(colref("val")),
            alias: "max_val".to_string(),
            is_distinct: false,
            filter: None,
            second_arg: None,
            order_within_group: None,
            statistical_support: None,
        };
        let child_cols = vec!["val".to_string()];
        let (ins, del) = agg_delta_exprs(&agg, &child_cols);
        // MAX of inserted values
        assert!(
            ins.contains("MAX") && ins.contains("'I'"),
            "MAX delta ins should use MAX: {ins}"
        );
        // MAX of deleted values
        assert!(
            del.contains("MAX") && del.contains("'D'"),
            "MAX delta del should use MAX: {del}"
        );
    }

    // ── MIN/MAX diff_aggregate integration tests ────────────────────

    #[test]
    fn test_diff_aggregate_min_with_group_by() {
        let mut ctx = test_ctx_with_st("public", "st");
        let agg = OpTree::Aggregate {
            group_by: vec![colref("dept")],
            aggregates: vec![AggExpr {
                function: AggFunc::Min,
                argument: Some(colref("salary")),
                alias: "min_salary".to_string(),
                is_distinct: false,
                filter: None,
                second_arg: None,
                order_within_group: None,
                statistical_support: None,
            }],
            child: Box::new(scan(1, "employees", "public", "e", &["dept", "salary"])),
        };
        let result = diff_aggregate(&mut ctx, &agg);
        assert!(
            result.is_ok(),
            "MIN aggregate should diff successfully: {result:?}"
        );
        let dr = result.unwrap();
        let sql = ctx.build_with_query(&dr.cte_name);
        assert!(
            sql.contains("LEAST") || sql.contains("MIN"),
            "MIN aggregate diff should reference LEAST or MIN: {sql}",
        );
    }

    #[test]
    fn test_diff_aggregate_max_with_group_by() {
        let mut ctx = test_ctx_with_st("public", "st");
        let agg = OpTree::Aggregate {
            group_by: vec![colref("dept")],
            aggregates: vec![AggExpr {
                function: AggFunc::Max,
                argument: Some(colref("salary")),
                alias: "max_salary".to_string(),
                is_distinct: false,
                filter: None,
                second_arg: None,
                order_within_group: None,
                statistical_support: None,
            }],
            child: Box::new(scan(1, "employees", "public", "e", &["dept", "salary"])),
        };
        let result = diff_aggregate(&mut ctx, &agg);
        assert!(
            result.is_ok(),
            "MAX aggregate should diff successfully: {result:?}"
        );
        let dr = result.unwrap();
        let sql = ctx.build_with_query(&dr.cte_name);
        assert!(
            sql.contains("GREATEST") || sql.contains("MAX"),
            "MAX aggregate diff should reference GREATEST or MAX: {sql}",
        );
    }

    // ── B5: FILTER clause tests ──────────────────────────────────────

    #[test]
    fn test_agg_delta_exprs_count_star_with_filter() {
        let agg = with_filter(
            count_star("cnt"),
            binop("=", colref("status"), lit("'active'")),
        );
        let (ins, del) = agg_delta_exprs(&agg, &[]);
        assert!(
            ins.contains("__pgt_action = 'I'"),
            "Insert expr should check action: {ins}",
        );
        assert!(
            ins.contains("(status = 'active')"),
            "Insert expr should contain filter: {ins}",
        );
        assert!(
            del.contains("__pgt_action = 'D'"),
            "Delete expr should check action: {del}",
        );
        assert!(
            del.contains("(status = 'active')"),
            "Delete expr should contain filter: {del}",
        );
    }

    #[test]
    fn test_agg_delta_exprs_sum_with_filter() {
        let agg = with_filter(
            sum_col("amount", "total"),
            binop(">", colref("amount"), lit("0")),
        );
        let (ins, _del) = agg_delta_exprs(&agg, &["amount".to_string()]);
        assert!(
            ins.contains("(amount > 0)"),
            "Insert expr should contain filter: {ins}",
        );
        assert!(
            ins.contains("THEN amount"),
            "Insert expr should reference amount column: {ins}",
        );
    }

    #[test]
    fn test_agg_delta_exprs_count_no_filter() {
        let agg = count_star("cnt");
        let (ins, del) = agg_delta_exprs(&agg, &[]);
        assert!(
            !ins.contains("AND"),
            "Without filter, no extra AND should appear: {ins}",
        );
        assert!(
            !del.contains("AND"),
            "Without filter, no extra AND should appear: {del}",
        );
    }

    #[test]
    fn test_agg_delta_exprs_min_with_filter() {
        let agg = with_filter(
            min_col("val", "min_val"),
            binop("=", colref("active"), lit("true")),
        );
        let (ins, _del) = agg_delta_exprs(&agg, &["val".to_string(), "active".to_string()]);
        assert!(
            ins.contains("(active = true)"),
            "MIN insert delta should contain filter: {ins}",
        );
        assert!(ins.contains("MIN("), "Should use MIN function: {ins}");
    }

    #[test]
    fn test_diff_aggregate_with_filter() {
        let mut ctx = test_ctx_with_st("public", "st");
        let agg = OpTree::Aggregate {
            group_by: vec![colref("dept")],
            aggregates: vec![with_filter(
                count_star("active_cnt"),
                binop("=", colref("status"), lit("'active'")),
            )],
            child: Box::new(scan(1, "employees", "public", "e", &["dept", "status"])),
        };
        let result = diff_aggregate(&mut ctx, &agg);
        assert!(result.is_ok(), "FILTER aggregate should diff: {result:?}");
        let dr = result.unwrap();
        let sql = ctx.build_with_query(&dr.cte_name);
        assert!(
            sql.contains("(status = 'active')"),
            "Generated SQL should contain FILTER condition: {sql}",
        );
    }

    #[test]
    fn test_filter_disqualifies_p5_bypass() {
        let child = scan(1, "t", "public", "t", &["id", "region", "status"]);
        let group_by = vec![colref("region")];
        let aggs = vec![with_filter(
            count_star("cnt"),
            binop("=", colref("status"), lit("'active'")),
        )];
        assert!(
            !is_direct_agg_eligible(&child, &group_by, &aggs),
            "Filtered aggregates should not be eligible for P5 bypass",
        );
    }

    // ── B4: Group-rescan aggregate tests ─────────────────────────────

    #[test]
    fn test_is_group_rescan() {
        assert!(!AggFunc::Count.is_group_rescan());
        assert!(!AggFunc::Sum.is_group_rescan());
        // AVG is now algebraic via auxiliary columns, not group-rescan
        assert!(!AggFunc::Avg.is_group_rescan());
        assert!(AggFunc::Avg.is_algebraic_via_aux());
        assert!(!AggFunc::Min.is_group_rescan());
        assert!(!AggFunc::Max.is_group_rescan());
        assert!(AggFunc::BoolAnd.is_group_rescan());
        assert!(AggFunc::BoolOr.is_group_rescan());
        assert!(AggFunc::StringAgg.is_group_rescan());
        assert!(AggFunc::ArrayAgg.is_group_rescan());
        assert!(AggFunc::JsonAgg.is_group_rescan());
        assert!(AggFunc::JsonbAgg.is_group_rescan());
        assert!(AggFunc::BitAnd.is_group_rescan());
        assert!(AggFunc::BitOr.is_group_rescan());
        assert!(AggFunc::BitXor.is_group_rescan());
        assert!(AggFunc::JsonObjectAgg.is_group_rescan());
        assert!(AggFunc::JsonbObjectAgg.is_group_rescan());
        assert!(AggFunc::JsonObjectAggStd("JSON_OBJECTAGG(k : v)".into()).is_group_rescan());
        assert!(AggFunc::JsonArrayAggStd("JSON_ARRAYAGG(x)".into()).is_group_rescan());
        // STDDEV/VAR are now algebraic via auxiliary columns, not group-rescan
        assert!(!AggFunc::StddevPop.is_group_rescan());
        assert!(!AggFunc::StddevSamp.is_group_rescan());
        assert!(!AggFunc::VarPop.is_group_rescan());
        assert!(!AggFunc::VarSamp.is_group_rescan());
        assert!(AggFunc::StddevPop.is_algebraic_via_aux());
        assert!(AggFunc::StddevSamp.is_algebraic_via_aux());
        assert!(AggFunc::VarPop.is_algebraic_via_aux());
        assert!(AggFunc::VarSamp.is_algebraic_via_aux());
        assert!(AggFunc::StddevPop.needs_sum_of_squares());
        assert!(AggFunc::StddevSamp.needs_sum_of_squares());
        assert!(AggFunc::VarPop.needs_sum_of_squares());
        assert!(AggFunc::VarSamp.needs_sum_of_squares());
        assert!(AggFunc::Mode.is_group_rescan());
        assert!(AggFunc::PercentileCont.is_group_rescan());
        assert!(AggFunc::PercentileDisc.is_group_rescan());
        assert!(AggFunc::XmlAgg.is_group_rescan());
        assert!(AggFunc::HypRank.is_group_rescan());
        assert!(AggFunc::HypDenseRank.is_group_rescan());
        assert!(AggFunc::HypPercentRank.is_group_rescan());
        assert!(AggFunc::HypCumeDist.is_group_rescan());
    }

    #[test]
    fn test_agg_to_rescan_sql_json_objectagg_std() {
        let agg = AggExpr {
            function: AggFunc::JsonObjectAggStd(
                "JSON_OBJECTAGG(name : value ABSENT ON NULL)".into(),
            ),
            argument: None,
            alias: "obj".to_string(),
            is_distinct: false,
            second_arg: None,
            filter: None,
            order_within_group: None,
            statistical_support: None,
        };
        assert_eq!(
            agg_to_rescan_sql(&agg),
            "JSON_OBJECTAGG(name : value ABSENT ON NULL)"
        );
    }

    #[test]
    fn test_agg_to_rescan_sql_json_arrayagg_std() {
        let agg = AggExpr {
            function: AggFunc::JsonArrayAggStd("JSON_ARRAYAGG(x ORDER BY x)".into()),
            argument: None,
            alias: "arr".to_string(),
            is_distinct: false,
            second_arg: None,
            filter: None,
            order_within_group: None,
            statistical_support: None,
        };
        assert_eq!(agg_to_rescan_sql(&agg), "JSON_ARRAYAGG(x ORDER BY x)");
    }

    #[test]
    fn test_agg_to_rescan_sql_json_arrayagg_std_with_filter() {
        let agg = AggExpr {
            function: AggFunc::JsonArrayAggStd("JSON_ARRAYAGG(x)".into()),
            argument: None,
            alias: "arr".to_string(),
            is_distinct: false,
            second_arg: None,
            filter: Some(Expr::Raw("x > 0".into())),
            order_within_group: None,
            statistical_support: None,
        };
        assert_eq!(
            agg_to_rescan_sql(&agg),
            "JSON_ARRAYAGG(x) FILTER (WHERE x > 0)"
        );
    }

    #[test]
    fn test_uda_classified_as_group_rescan() {
        let uda = AggFunc::UserDefined("my_custom_agg(x)".into());
        assert!(uda.is_group_rescan());
        assert_eq!(uda.sql_name(), "USER_DEFINED");
    }

    #[test]
    fn test_uda_rescan_sql_rendering() {
        let agg = AggExpr {
            function: AggFunc::UserDefined("my_custom_agg(x)".into()),
            argument: None,
            alias: "custom".to_string(),
            is_distinct: false,
            second_arg: None,
            filter: None,
            order_within_group: None,
            statistical_support: None,
        };
        assert_eq!(agg_to_rescan_sql(&agg), "my_custom_agg(x)");
    }

    #[test]
    fn test_uda_rescan_sql_schema_qualified() {
        let agg = AggExpr {
            function: AggFunc::UserDefined("myschema.my_agg(x, y)".into()),
            argument: None,
            alias: "result".to_string(),
            is_distinct: false,
            second_arg: None,
            filter: None,
            order_within_group: None,
            statistical_support: None,
        };
        assert_eq!(agg_to_rescan_sql(&agg), "myschema.my_agg(x, y)");
    }

    #[test]
    fn test_uda_rescan_sql_with_filter_clause() {
        // UDA with FILTER: the raw SQL is stored directly, FILTER handled
        // by the UserDefined variant returning the raw SQL as-is.
        let agg = AggExpr {
            function: AggFunc::UserDefined("my_agg(x) FILTER (WHERE x > 0)".into()),
            argument: None,
            alias: "filtered".to_string(),
            is_distinct: false,
            second_arg: None,
            filter: None,
            order_within_group: None,
            statistical_support: None,
        };
        assert_eq!(agg_to_rescan_sql(&agg), "my_agg(x) FILTER (WHERE x > 0)");
    }

    #[test]
    fn test_uda_rescan_sql_with_order_by() {
        let agg = AggExpr {
            function: AggFunc::UserDefined("my_agg(x ORDER BY y)".into()),
            argument: None,
            alias: "ordered".to_string(),
            is_distinct: false,
            second_arg: None,
            filter: None,
            order_within_group: None,
            statistical_support: None,
        };
        assert_eq!(agg_to_rescan_sql(&agg), "my_agg(x ORDER BY y)");
    }

    #[test]
    fn test_agg_delta_exprs_bool_and() {
        let agg = bool_and_col("active", "all_active");
        let (ins, _del) = agg_delta_exprs(&agg, &["active".to_string()]);
        assert!(
            ins.contains("__pgt_action = 'I'"),
            "BOOL_AND insert delta should check action: {ins}",
        );
        assert!(
            ins.contains("active IS NOT NULL"),
            "BOOL_AND insert delta should check NOT NULL: {ins}",
        );
    }

    #[test]
    fn test_agg_delta_exprs_bool_or() {
        let agg = bool_or_col("active", "any_active");
        let (_ins, del) = agg_delta_exprs(&agg, &["active".to_string()]);
        assert!(
            del.contains("__pgt_action = 'D'"),
            "BOOL_OR delete delta should check action: {del}",
        );
    }

    #[test]
    fn test_agg_delta_exprs_string_agg() {
        let agg = string_agg_col("name", "', '", "members");
        let (ins, _del) = agg_delta_exprs(&agg, &["name".to_string()]);
        assert!(
            ins.contains("name IS NOT NULL"),
            "STRING_AGG insert delta should check NOT NULL: {ins}",
        );
    }

    #[test]
    fn test_agg_delta_exprs_array_agg() {
        let agg = array_agg_col("val", "vals");
        let (ins, _del) = agg_delta_exprs(&agg, &["val".to_string()]);
        assert!(
            ins.contains("__pgt_action = 'I'"),
            "ARRAY_AGG insert delta should check action: {ins}",
        );
    }

    #[test]
    fn test_agg_merge_expr_bool_and_rescan() {
        let agg = bool_and_col("active", "all_active");
        let merge = agg_merge_expr(&agg, false);
        assert!(
            merge.contains("THEN NULL"),
            "BOOL_AND merge should return NULL sentinel on change: {merge}",
        );
        assert!(
            merge.contains("d.\"__ins_all_active\""),
            "BOOL_AND merge should reference ins counter: {merge}",
        );
    }

    #[test]
    fn test_agg_merge_expr_string_agg_rescan() {
        let agg = string_agg_col("name", "', '", "members");
        let merge = agg_merge_expr(&agg, false);
        assert!(
            merge.contains("THEN NULL"),
            "STRING_AGG merge should return NULL sentinel: {merge}",
        );
        assert!(
            merge.contains("st.\"members\""),
            "STRING_AGG merge should reference old value: {merge}",
        );
    }

    #[test]
    fn test_agg_merge_expr_array_agg_rescan() {
        let agg = array_agg_col("val", "vals");
        let merge = agg_merge_expr(&agg, false);
        assert!(
            merge.contains("THEN NULL"),
            "ARRAY_AGG merge should return NULL sentinel: {merge}",
        );
    }

    #[test]
    fn test_rescan_agg_disqualifies_p5_bypass() {
        let child = scan(1, "t", "public", "t", &["id", "region", "name"]);
        let group_by = vec![colref("region")];
        let aggs = vec![string_agg_col("name", "', '", "members")];
        assert!(
            !is_direct_agg_eligible(&child, &group_by, &aggs),
            "Group-rescan aggregates should not be eligible for P5 bypass",
        );
    }

    #[test]
    fn test_diff_aggregate_bool_and() {
        let mut ctx = test_ctx_with_st("public", "st");
        let agg = OpTree::Aggregate {
            group_by: vec![colref("dept")],
            aggregates: vec![bool_and_col("active", "all_active")],
            child: Box::new(scan(1, "employees", "public", "e", &["dept", "active"])),
        };
        let result = diff_aggregate(&mut ctx, &agg);
        assert!(result.is_ok(), "BOOL_AND aggregate should diff: {result:?}",);
        let dr = result.unwrap();
        assert!(
            dr.columns.contains(&"all_active".to_string()),
            "Output should include BOOL_AND alias: {:?}",
            dr.columns,
        );
    }

    #[test]
    fn test_diff_aggregate_string_agg() {
        let mut ctx = test_ctx_with_st("public", "st");
        let agg = OpTree::Aggregate {
            group_by: vec![colref("dept")],
            aggregates: vec![string_agg_col("name", "', '", "members")],
            child: Box::new(scan(1, "employees", "public", "e", &["dept", "name"])),
        };
        let result = diff_aggregate(&mut ctx, &agg);
        assert!(
            result.is_ok(),
            "STRING_AGG aggregate should diff: {result:?}",
        );
        let dr = result.unwrap();
        let sql = ctx.build_with_query(&dr.cte_name);
        assert!(
            sql.contains("agg_rescan"),
            "STRING_AGG diff should generate rescan CTE: {sql}",
        );
    }

    #[test]
    fn test_diff_aggregate_array_agg() {
        let mut ctx = test_ctx_with_st("public", "st");
        let agg = OpTree::Aggregate {
            group_by: vec![colref("dept")],
            aggregates: vec![array_agg_col("val", "vals")],
            child: Box::new(scan(1, "t", "public", "t", &["dept", "val"])),
        };
        let result = diff_aggregate(&mut ctx, &agg);
        assert!(
            result.is_ok(),
            "ARRAY_AGG aggregate should diff: {result:?}",
        );
    }

    #[test]
    fn test_diff_aggregate_bool_or() {
        let mut ctx = test_ctx_with_st("public", "st");
        let agg = OpTree::Aggregate {
            group_by: vec![colref("region")],
            aggregates: vec![bool_or_col("has_flag", "any_flag")],
            child: Box::new(scan(1, "t", "public", "t", &["region", "has_flag"])),
        };
        let result = diff_aggregate(&mut ctx, &agg);
        assert!(result.is_ok(), "BOOL_OR should diff: {result:?}");
    }

    #[test]
    fn test_diff_aggregate_mixed_algebraic_and_rescan() {
        let mut ctx = test_ctx_with_st("public", "st");
        let agg = OpTree::Aggregate {
            group_by: vec![colref("dept")],
            aggregates: vec![
                count_star("cnt"),
                sum_col("amount", "total"),
                string_agg_col("name", "', '", "members"),
            ],
            child: Box::new(scan(
                1,
                "employees",
                "public",
                "e",
                &["dept", "amount", "name"],
            )),
        };
        let result = diff_aggregate(&mut ctx, &agg);
        assert!(
            result.is_ok(),
            "Mixed algebraic+rescan should diff: {result:?}",
        );
        let dr = result.unwrap();
        let sql = ctx.build_with_query(&dr.cte_name);
        // Algebraic aggregates use addition
        assert!(
            sql.contains("COALESCE(d.__ins_count, 0) - COALESCE(d.__del_count, 0)"),
            "COUNT should use algebraic merge: {sql}",
        );
        // Rescan aggregates use NULL sentinel
        assert!(
            sql.contains("agg_rescan"),
            "STRING_AGG should generate rescan CTE: {sql}",
        );
    }

    #[test]
    fn test_agg_delta_exprs_bool_and_with_filter() {
        let agg = with_filter(
            bool_and_col("active", "all_active"),
            binop("=", colref("dept"), lit("'eng'")),
        );
        let (ins, _del) = agg_delta_exprs(&agg, &["active".to_string(), "dept".to_string()]);
        assert!(
            ins.contains("(dept = 'eng')"),
            "Filtered BOOL_AND should include filter: {ins}",
        );
    }

    // ── resolve_expr_for_child tests ─────────────────────────────────

    #[test]
    fn test_resolve_expr_for_child_simple_column() {
        let expr = colref("amount");
        let result = resolve_expr_for_child(&expr, &["amount".to_string()]);
        assert_eq!(result, "amount");
    }

    #[test]
    fn test_resolve_expr_for_child_qualified_column_with_disambiguation() {
        let expr = qcolref("o", "amount");
        let result =
            resolve_expr_for_child(&expr, &["o__amount".to_string(), "c__name".to_string()]);
        assert_eq!(result, "o__amount");
    }

    #[test]
    fn test_resolve_expr_for_child_binary_op() {
        let expr = binop("=", colref("status"), lit("'active'"));
        let result = resolve_expr_for_child(&expr, &["status".to_string()]);
        assert_eq!(result, "(status = 'active')");
    }

    #[test]
    fn test_resolve_expr_for_child_nested_binary_op() {
        let expr = binop(
            "AND",
            binop("=", colref("status"), lit("'active'")),
            binop(">", colref("amount"), lit("0")),
        );
        let result = resolve_expr_for_child(&expr, &["status".to_string(), "amount".to_string()]);
        assert!(
            result.contains("status = 'active'"),
            "Should resolve left: {result}"
        );
        assert!(
            result.contains("amount > 0"),
            "Should resolve right: {result}"
        );
    }

    // ── BIT_AND / BIT_OR / BIT_XOR tests ────────────────────────────

    #[test]
    fn test_agg_delta_exprs_bit_and() {
        let agg = bit_and_col("flags", "all_flags");
        let (ins, del) = agg_delta_exprs(&agg, &["flags".to_string()]);
        assert!(
            ins.contains("__pgt_action = 'I'"),
            "BIT_AND insert delta should check action: {ins}",
        );
        assert!(
            ins.contains("flags IS NOT NULL"),
            "BIT_AND insert delta should check NOT NULL: {ins}",
        );
        assert!(
            del.contains("__pgt_action = 'D'"),
            "BIT_AND delete delta should check action: {del}",
        );
    }

    #[test]
    fn test_agg_delta_exprs_bit_or() {
        let agg = bit_or_col("flags", "any_flags");
        let (ins, _del) = agg_delta_exprs(&agg, &["flags".to_string()]);
        assert!(
            ins.contains("flags IS NOT NULL"),
            "BIT_OR insert delta should check NOT NULL: {ins}",
        );
    }

    #[test]
    fn test_agg_delta_exprs_bit_xor() {
        let agg = bit_xor_col("flags", "xor_flags");
        let (ins, del) = agg_delta_exprs(&agg, &["flags".to_string()]);
        assert!(
            ins.contains("__pgt_action = 'I'"),
            "BIT_XOR insert delta should check action: {ins}",
        );
        assert!(
            del.contains("__pgt_action = 'D'"),
            "BIT_XOR delete delta should check action: {del}",
        );
    }

    #[test]
    fn test_agg_merge_expr_bit_and_rescan() {
        let agg = bit_and_col("flags", "all_flags");
        let merge = agg_merge_expr(&agg, false);
        assert!(
            merge.contains("THEN NULL"),
            "BIT_AND merge should return NULL sentinel on change: {merge}",
        );
    }

    #[test]
    fn test_agg_merge_expr_bit_or_rescan() {
        let agg = bit_or_col("flags", "any_flags");
        let merge = agg_merge_expr(&agg, false);
        assert!(
            merge.contains("THEN NULL"),
            "BIT_OR merge should return NULL sentinel on change: {merge}",
        );
    }

    #[test]
    fn test_agg_merge_expr_bit_xor_rescan() {
        let agg = bit_xor_col("flags", "xor_flags");
        let merge = agg_merge_expr(&agg, false);
        assert!(
            merge.contains("THEN NULL"),
            "BIT_XOR merge should return NULL sentinel on change: {merge}",
        );
    }

    #[test]
    fn test_diff_aggregate_bit_and() {
        let mut ctx = test_ctx_with_st("public", "st");
        let agg = OpTree::Aggregate {
            group_by: vec![colref("dept")],
            aggregates: vec![bit_and_col("flags", "all_flags")],
            child: Box::new(scan(1, "t", "public", "t", &["dept", "flags"])),
        };
        let result = diff_aggregate(&mut ctx, &agg);
        assert!(result.is_ok(), "BIT_AND aggregate should diff: {result:?}",);
        let dr = result.unwrap();
        assert!(
            dr.columns.contains(&"all_flags".to_string()),
            "Output should include BIT_AND alias: {:?}",
            dr.columns,
        );
    }

    #[test]
    fn test_diff_aggregate_bit_or() {
        let mut ctx = test_ctx_with_st("public", "st");
        let agg = OpTree::Aggregate {
            group_by: vec![colref("dept")],
            aggregates: vec![bit_or_col("flags", "any_flags")],
            child: Box::new(scan(1, "t", "public", "t", &["dept", "flags"])),
        };
        let result = diff_aggregate(&mut ctx, &agg);
        assert!(result.is_ok(), "BIT_OR aggregate should diff: {result:?}",);
    }

    #[test]
    fn test_diff_aggregate_bit_xor() {
        let mut ctx = test_ctx_with_st("public", "st");
        let agg = OpTree::Aggregate {
            group_by: vec![colref("dept")],
            aggregates: vec![bit_xor_col("flags", "xor_flags")],
            child: Box::new(scan(1, "t", "public", "t", &["dept", "flags"])),
        };
        let result = diff_aggregate(&mut ctx, &agg);
        assert!(result.is_ok(), "BIT_XOR aggregate should diff: {result:?}",);
        let dr = result.unwrap();
        let sql = ctx.build_with_query(&dr.cte_name);
        assert!(
            sql.contains("agg_rescan"),
            "BIT_XOR diff should generate rescan CTE: {sql}",
        );
    }

    #[test]
    fn test_rescan_agg_bit_and_disqualifies_p5_bypass() {
        let child = scan(1, "t", "public", "t", &["id", "dept", "flags"]);
        let group_by = vec![colref("dept")];
        let aggs = vec![bit_and_col("flags", "all_flags")];
        assert!(
            !is_direct_agg_eligible(&child, &group_by, &aggs),
            "BIT_AND should not be eligible for P5 bypass",
        );
    }

    // ── JSON_OBJECT_AGG / JSONB_OBJECT_AGG tests ────────────────────

    #[test]
    fn test_agg_delta_exprs_json_object_agg() {
        let agg = json_object_agg_col("name", "value", "obj");
        let (ins, del) = agg_delta_exprs(&agg, &["name".to_string(), "value".to_string()]);
        assert!(
            ins.contains("__pgt_action = 'I'"),
            "JSON_OBJECT_AGG insert delta should check action: {ins}",
        );
        assert!(
            ins.contains("name IS NOT NULL"),
            "JSON_OBJECT_AGG insert delta should check NOT NULL: {ins}",
        );
        assert!(
            del.contains("__pgt_action = 'D'"),
            "JSON_OBJECT_AGG delete delta should check action: {del}",
        );
    }

    #[test]
    fn test_agg_delta_exprs_jsonb_object_agg() {
        let agg = jsonb_object_agg_col("key", "val", "obj");
        let (ins, _del) = agg_delta_exprs(&agg, &["key".to_string(), "val".to_string()]);
        assert!(
            ins.contains("key IS NOT NULL"),
            "JSONB_OBJECT_AGG insert delta should check NOT NULL on key arg: {ins}",
        );
    }

    #[test]
    fn test_agg_merge_expr_json_object_agg_rescan() {
        let agg = json_object_agg_col("name", "value", "obj");
        let merge = agg_merge_expr(&agg, false);
        assert!(
            merge.contains("THEN NULL"),
            "JSON_OBJECT_AGG merge should return NULL sentinel: {merge}",
        );
    }

    #[test]
    fn test_agg_merge_expr_jsonb_object_agg_rescan() {
        let agg = jsonb_object_agg_col("key", "val", "obj");
        let merge = agg_merge_expr(&agg, false);
        assert!(
            merge.contains("THEN NULL"),
            "JSONB_OBJECT_AGG merge should return NULL sentinel: {merge}",
        );
    }

    #[test]
    fn test_diff_aggregate_json_object_agg() {
        let mut ctx = test_ctx_with_st("public", "st");
        let agg = OpTree::Aggregate {
            group_by: vec![colref("dept")],
            aggregates: vec![json_object_agg_col("name", "value", "obj")],
            child: Box::new(scan(1, "t", "public", "t", &["dept", "name", "value"])),
        };
        let result = diff_aggregate(&mut ctx, &agg);
        assert!(
            result.is_ok(),
            "JSON_OBJECT_AGG aggregate should diff: {result:?}",
        );
        let dr = result.unwrap();
        assert!(
            dr.columns.contains(&"obj".to_string()),
            "Output should include JSON_OBJECT_AGG alias: {:?}",
            dr.columns,
        );
    }

    #[test]
    fn test_diff_aggregate_jsonb_object_agg() {
        let mut ctx = test_ctx_with_st("public", "st");
        let agg = OpTree::Aggregate {
            group_by: vec![colref("dept")],
            aggregates: vec![jsonb_object_agg_col("key", "val", "obj")],
            child: Box::new(scan(1, "t", "public", "t", &["dept", "key", "val"])),
        };
        let result = diff_aggregate(&mut ctx, &agg);
        assert!(
            result.is_ok(),
            "JSONB_OBJECT_AGG aggregate should diff: {result:?}",
        );
        let dr = result.unwrap();
        let sql = ctx.build_with_query(&dr.cte_name);
        assert!(
            sql.contains("agg_rescan"),
            "JSONB_OBJECT_AGG diff should generate rescan CTE: {sql}",
        );
    }

    #[test]
    fn test_rescan_agg_json_object_agg_disqualifies_p5_bypass() {
        let child = scan(1, "t", "public", "t", &["id", "dept", "name", "value"]);
        let group_by = vec![colref("dept")];
        let aggs = vec![json_object_agg_col("name", "value", "obj")];
        assert!(
            !is_direct_agg_eligible(&child, &group_by, &aggs),
            "JSON_OBJECT_AGG should not be eligible for P5 bypass",
        );
    }

    #[test]
    fn test_diff_aggregate_mixed_with_bitwise() {
        let mut ctx = test_ctx_with_st("public", "st");
        let agg = OpTree::Aggregate {
            group_by: vec![colref("dept")],
            aggregates: vec![count_star("cnt"), bit_or_col("perms", "combined_perms")],
            child: Box::new(scan(1, "t", "public", "t", &["dept", "perms"])),
        };
        let result = diff_aggregate(&mut ctx, &agg);
        assert!(
            result.is_ok(),
            "Mixed COUNT + BIT_OR should diff: {result:?}",
        );
        let dr = result.unwrap();
        let sql = ctx.build_with_query(&dr.cte_name);
        // COUNT uses algebraic merge
        assert!(
            sql.contains("COALESCE(d.__ins_count, 0) - COALESCE(d.__del_count, 0)"),
            "COUNT should use algebraic merge: {sql}",
        );
        // BIT_OR uses rescan sentinel
        assert!(
            sql.contains("agg_rescan"),
            "BIT_OR should generate rescan CTE: {sql}",
        );
    }

    // ── Statistical aggregate tests ─────────────────────────────────────

    #[test]
    fn test_agg_delta_exprs_stddev_pop() {
        let agg = stddev_pop_col("amount", "sd_pop");
        let (ins, del) = agg_delta_exprs(&agg, &["amount".to_string()]);
        assert!(
            ins.contains("__pgt_action = 'I'"),
            "STDDEV_POP insert delta should check action: {ins}",
        );
        assert!(
            del.contains("__pgt_action = 'D'"),
            "STDDEV_POP delete delta should check action: {del}",
        );
    }

    #[test]
    fn test_agg_delta_exprs_stddev_samp() {
        let agg = stddev_samp_col("amount", "sd_samp");
        let (ins, del) = agg_delta_exprs(&agg, &["amount".to_string()]);
        // STDDEV_SAMP is now algebraic: tracks SUM(amount) not NULL count
        assert!(
            ins.contains("__pgt_action = 'I'") && ins.contains("amount"),
            "STDDEV_SAMP insert delta should track SUM of argument: {ins}",
        );
        assert!(
            del.contains("__pgt_action = 'D'") && del.contains("amount"),
            "STDDEV_SAMP delete delta should track SUM of argument: {del}",
        );
    }

    #[test]
    fn test_agg_delta_exprs_var_pop() {
        let agg = var_pop_col("amount", "v_pop");
        let (ins, del) = agg_delta_exprs(&agg, &["amount".to_string()]);
        assert!(
            ins.contains("__pgt_action = 'I'"),
            "VAR_POP insert delta should check action: {ins}",
        );
        assert!(
            del.contains("__pgt_action = 'D'"),
            "VAR_POP delete delta should check action: {del}",
        );
    }

    #[test]
    fn test_agg_delta_exprs_var_samp() {
        let agg = var_samp_col("amount", "v_samp");
        let (ins, del) = agg_delta_exprs(&agg, &["amount".to_string()]);
        // VAR_SAMP is now algebraic: tracks SUM(amount) not NULL count
        assert!(
            ins.contains("__pgt_action = 'I'") && ins.contains("amount"),
            "VAR_SAMP insert delta should track SUM of argument: {ins}",
        );
        assert!(
            del.contains("__pgt_action = 'D'") && del.contains("amount"),
            "VAR_SAMP delete delta should track SUM of argument: {del}",
        );
    }

    #[test]
    fn test_agg_merge_expr_stddev_pop_rescan() {
        let agg = stddev_pop_col("amount", "sd_pop");
        let merge = agg_merge_expr(&agg, false);
        // STDDEV_POP is now algebraic — uses SQRT(GREATEST(0, ...))
        assert!(
            merge.contains("SQRT") && merge.contains("GREATEST"),
            "STDDEV_POP merge should use algebraic SQRT formula: {merge}",
        );
        assert!(
            merge.contains("__pgt_aux_sum2_"),
            "STDDEV_POP merge should reference sum2 auxiliary column: {merge}",
        );
    }

    #[test]
    fn test_agg_merge_expr_stddev_samp_rescan() {
        let agg = stddev_samp_col("amount", "sd_samp");
        let merge = agg_merge_expr(&agg, false);
        // STDDEV_SAMP is now algebraic — uses SQRT(GREATEST(0, ...))
        assert!(
            merge.contains("SQRT") && merge.contains("GREATEST"),
            "STDDEV_SAMP merge should use algebraic SQRT formula: {merge}",
        );
        // SAMP variant uses n*(n-1) denominator
        assert!(
            merge.contains("- 1"),
            "STDDEV_SAMP merge should use (n-1) denominator: {merge}",
        );
    }

    #[test]
    fn test_agg_merge_expr_var_pop_rescan() {
        let agg = var_pop_col("amount", "v_pop");
        let merge = agg_merge_expr(&agg, false);
        // VAR_POP is now algebraic — uses GREATEST(0, ...)
        assert!(
            merge.contains("GREATEST") && !merge.contains("SQRT"),
            "VAR_POP merge should use algebraic formula without SQRT: {merge}",
        );
    }

    #[test]
    fn test_agg_merge_expr_var_samp_rescan() {
        let agg = var_samp_col("amount", "v_samp");
        let merge = agg_merge_expr(&agg, false);
        // VAR_SAMP is now algebraic — uses GREATEST(0, ...)
        assert!(
            merge.contains("GREATEST") && !merge.contains("SQRT"),
            "VAR_SAMP merge should use algebraic formula without SQRT: {merge}",
        );
        // SAMP variant uses n*(n-1) denominator
        assert!(
            merge.contains("- 1"),
            "VAR_SAMP merge should use (n-1) denominator: {merge}",
        );
    }

    #[test]
    fn test_statistical_delta_products_cast_operands_to_resolved_type() {
        let agg = AggExpr {
            function: AggFunc::Corr,
            argument: Some(colref("y")),
            alias: "corr".into(),
            is_distinct: false,
            filter: None,
            second_arg: Some(colref("x")),
            order_within_group: None,
            statistical_support: Some(crate::dvm::parser::StatisticalAggSupport::with_accumulator(
                crate::dvm::parser::PG_FLOAT8_TYPE_OID,
                "double precision",
            )),
        };
        let (insert, delete) = agg_covar_delta_exprs(&agg, &["x".into(), "y".into()]);
        for expr in [insert.2, insert.3, insert.4, delete.2, delete.3, delete.4] {
            assert!(
                expr.contains("::double precision *"),
                "cross-product operand must be cast before multiplication: {expr}"
            );
        }
    }

    #[test]
    fn test_statistical_sum2_casts_operands_to_resolved_type() {
        let agg = AggExpr {
            function: AggFunc::StddevPop,
            argument: Some(colref("amount")),
            alias: "sd".into(),
            is_distinct: false,
            filter: None,
            second_arg: None,
            order_within_group: None,
            statistical_support: Some(crate::dvm::parser::StatisticalAggSupport::with_accumulator(
                crate::dvm::parser::PG_NUMERIC_TYPE_OID,
                "numeric",
            )),
        };
        let (insert, delete) = agg_sum2_delta_exprs(&agg, &["amount".into()]);
        for expr in [insert, delete] {
            assert!(
                expr.contains("::numeric *"),
                "sum-of-squares operand must be cast before multiplication: {expr}"
            );
        }
    }

    #[test]
    fn test_diff_aggregate_stddev_pop() {
        let mut ctx = test_ctx_with_st("public", "st");
        let agg = OpTree::Aggregate {
            group_by: vec![colref("dept")],
            aggregates: vec![stddev_pop_col("amount", "sd_pop")],
            child: Box::new(scan(1, "t", "public", "t", &["dept", "amount"])),
        };
        let result = diff_aggregate(&mut ctx, &agg);
        assert!(result.is_ok(), "STDDEV_POP should diff: {result:?}");
        let dr = result.unwrap();
        let sql = ctx.build_with_query(&dr.cte_name);
        // STDDEV_POP is algebraic — no rescan CTE, uses sum/sum2/count auxiliaries
        assert!(
            !sql.contains("agg_rescan"),
            "STDDEV_POP should NOT generate rescan CTE (algebraic): {sql}",
        );
        assert!(
            sql.contains("__ins_sum2_"),
            "STDDEV_POP delta should track sum-of-squares: {sql}",
        );
        assert!(
            sql.contains("SQRT"),
            "STDDEV_POP merge should use SQRT formula: {sql}",
        );
    }

    #[test]
    fn test_diff_aggregate_stddev_samp() {
        let mut ctx = test_ctx_with_st("public", "st");
        let agg = OpTree::Aggregate {
            group_by: vec![colref("dept")],
            aggregates: vec![stddev_samp_col("amount", "sd_samp")],
            child: Box::new(scan(1, "t", "public", "t", &["dept", "amount"])),
        };
        let result = diff_aggregate(&mut ctx, &agg);
        assert!(result.is_ok(), "STDDEV_SAMP should diff: {result:?}");
        let dr = result.unwrap();
        let sql = ctx.build_with_query(&dr.cte_name);
        assert!(
            !sql.contains("agg_rescan"),
            "STDDEV_SAMP should NOT generate rescan CTE (algebraic): {sql}",
        );
        assert!(
            sql.contains("__ins_sum2_"),
            "STDDEV_SAMP delta should track sum-of-squares: {sql}",
        );
    }

    #[test]
    fn test_diff_aggregate_var_pop() {
        let mut ctx = test_ctx_with_st("public", "st");
        let agg = OpTree::Aggregate {
            group_by: vec![colref("dept")],
            aggregates: vec![var_pop_col("amount", "v_pop")],
            child: Box::new(scan(1, "t", "public", "t", &["dept", "amount"])),
        };
        let result = diff_aggregate(&mut ctx, &agg);
        assert!(result.is_ok(), "VAR_POP should diff: {result:?}");
        let dr = result.unwrap();
        let sql = ctx.build_with_query(&dr.cte_name);
        assert!(
            !sql.contains("agg_rescan"),
            "VAR_POP should NOT generate rescan CTE (algebraic): {sql}",
        );
        assert!(
            sql.contains("GREATEST"),
            "VAR_POP merge should use GREATEST formula: {sql}",
        );
    }

    #[test]
    fn test_diff_aggregate_var_samp() {
        let mut ctx = test_ctx_with_st("public", "st");
        let agg = OpTree::Aggregate {
            group_by: vec![colref("dept")],
            aggregates: vec![var_samp_col("amount", "v_samp")],
            child: Box::new(scan(1, "t", "public", "t", &["dept", "amount"])),
        };
        let result = diff_aggregate(&mut ctx, &agg);
        assert!(result.is_ok(), "VAR_SAMP should diff: {result:?}");
        let dr = result.unwrap();
        let sql = ctx.build_with_query(&dr.cte_name);
        assert!(
            !sql.contains("agg_rescan"),
            "VAR_SAMP should NOT generate rescan CTE (algebraic): {sql}",
        );
    }

    #[test]
    fn test_rescan_agg_stddev_disqualifies_p5_bypass() {
        let child = scan(1, "t", "public", "t", &["id", "dept", "amount"]);
        let group_by = vec![colref("dept")];
        let aggs = vec![stddev_samp_col("amount", "sd")];
        assert!(
            !is_direct_agg_eligible(&child, &group_by, &aggs),
            "STDDEV_SAMP should not be eligible for P5 bypass",
        );
    }

    #[test]
    fn test_rescan_agg_variance_disqualifies_p5_bypass() {
        let child = scan(1, "t", "public", "t", &["id", "dept", "amount"]);
        let group_by = vec![colref("dept")];
        let aggs = vec![var_samp_col("amount", "v")];
        assert!(
            !is_direct_agg_eligible(&child, &group_by, &aggs),
            "VAR_SAMP should not be eligible for P5 bypass",
        );
    }

    #[test]
    fn test_diff_aggregate_mixed_with_statistical() {
        let mut ctx = test_ctx_with_st("public", "st");
        let agg = OpTree::Aggregate {
            group_by: vec![colref("dept")],
            aggregates: vec![
                count_star("cnt"),
                sum_col("amount", "total"),
                stddev_pop_col("amount", "sd_pop"),
            ],
            child: Box::new(scan(1, "t", "public", "t", &["dept", "amount"])),
        };
        let result = diff_aggregate(&mut ctx, &agg);
        assert!(
            result.is_ok(),
            "Mixed COUNT + SUM + STDDEV_POP should diff: {result:?}",
        );
        let dr = result.unwrap();
        let sql = ctx.build_with_query(&dr.cte_name);
        // COUNT uses algebraic merge
        assert!(
            sql.contains("COALESCE(d.__ins_count, 0) - COALESCE(d.__del_count, 0)"),
            "COUNT should use algebraic merge: {sql}",
        );
        // STDDEV_POP is now algebraic — no rescan needed
        assert!(
            !sql.contains("agg_rescan"),
            "All aggregates are algebraic, no rescan CTE needed: {sql}",
        );
        assert!(
            sql.contains("SQRT"),
            "STDDEV_POP should use algebraic SQRT formula: {sql}",
        );
    }

    // ── Ordered-set aggregate tests (MODE, PERCENTILE_CONT, PERCENTILE_DISC) ──

    #[test]
    fn test_is_group_rescan_ordered_set() {
        assert!(AggFunc::Mode.is_group_rescan());
        assert!(AggFunc::PercentileCont.is_group_rescan());
        assert!(AggFunc::PercentileDisc.is_group_rescan());
        assert!(AggFunc::XmlAgg.is_group_rescan());
        assert!(AggFunc::HypRank.is_group_rescan());
        assert!(AggFunc::HypDenseRank.is_group_rescan());
        assert!(AggFunc::HypPercentRank.is_group_rescan());
        assert!(AggFunc::HypCumeDist.is_group_rescan());
    }

    #[test]
    fn test_agg_func_sql_names_ordered_set() {
        assert_eq!(AggFunc::Mode.sql_name(), "MODE");
        assert_eq!(AggFunc::PercentileCont.sql_name(), "PERCENTILE_CONT");
        assert_eq!(AggFunc::PercentileDisc.sql_name(), "PERCENTILE_DISC");
        assert_eq!(AggFunc::XmlAgg.sql_name(), "XMLAGG");
        assert_eq!(AggFunc::HypRank.sql_name(), "RANK");
        assert_eq!(AggFunc::HypDenseRank.sql_name(), "DENSE_RANK");
        assert_eq!(AggFunc::HypPercentRank.sql_name(), "PERCENT_RANK");
        assert_eq!(AggFunc::HypCumeDist.sql_name(), "CUME_DIST");
    }

    #[test]
    fn test_agg_merge_expr_mode_rescan() {
        let agg = mode_col("category", "most_common");
        let merge = agg_merge_expr(&agg, false);
        assert!(
            merge.contains("THEN NULL"),
            "MODE merge should return NULL sentinel on change: {merge}",
        );
        assert!(
            merge.contains("d.\"__ins_most_common\""),
            "MODE merge should reference ins counter: {merge}",
        );
    }

    #[test]
    fn test_agg_merge_expr_percentile_cont_rescan() {
        let agg = percentile_cont_col("0.5", "amount", "median_amount");
        let merge = agg_merge_expr(&agg, false);
        assert!(
            merge.contains("THEN NULL"),
            "PERCENTILE_CONT merge should return NULL sentinel on change: {merge}",
        );
        assert!(
            merge.contains("st.\"median_amount\""),
            "PERCENTILE_CONT merge should reference old value: {merge}",
        );
    }

    #[test]
    fn test_agg_merge_expr_percentile_disc_rescan() {
        let agg = percentile_disc_col("0.75", "score", "p75_score");
        let merge = agg_merge_expr(&agg, false);
        assert!(
            merge.contains("THEN NULL"),
            "PERCENTILE_DISC merge should return NULL sentinel on change: {merge}",
        );
        assert!(
            merge.contains("st.\"p75_score\""),
            "PERCENTILE_DISC merge should reference old value: {merge}",
        );
    }

    #[test]
    fn test_agg_delta_exprs_mode() {
        let agg = mode_col("category", "most_common");
        let (ins, del) = agg_delta_exprs(&agg, &["category".to_string()]);
        assert!(
            ins.contains("__pgt_action = 'I'"),
            "MODE insert delta should check action: {ins}",
        );
        assert!(
            del.contains("__pgt_action = 'D'"),
            "MODE delete delta should check action: {del}",
        );
    }

    #[test]
    fn test_agg_delta_exprs_percentile_cont() {
        let agg = percentile_cont_col("0.5", "amount", "median_amount");
        let (ins, _del) = agg_delta_exprs(&agg, &["amount".to_string()]);
        assert!(
            ins.contains("__pgt_action = 'I'"),
            "PERCENTILE_CONT insert delta should check action: {ins}",
        );
    }

    #[test]
    fn test_agg_delta_exprs_percentile_disc() {
        let agg = percentile_disc_col("0.75", "score", "p75_score");
        let (ins, _del) = agg_delta_exprs(&agg, &["score".to_string()]);
        assert!(
            ins.contains("__pgt_action = 'I'"),
            "PERCENTILE_DISC insert delta should check action: {ins}",
        );
    }

    #[test]
    fn test_diff_aggregate_mode() {
        let mut ctx = test_ctx_with_st("public", "st");
        let agg = OpTree::Aggregate {
            group_by: vec![colref("region")],
            aggregates: vec![mode_col("category", "most_common")],
            child: Box::new(scan(1, "products", "public", "p", &["region", "category"])),
        };
        let result = diff_aggregate(&mut ctx, &agg);
        assert!(result.is_ok(), "MODE aggregate should diff: {result:?}");
        let dr = result.unwrap();
        assert!(
            dr.columns.contains(&"most_common".to_string()),
            "Output should include MODE alias: {:?}",
            dr.columns,
        );
        let sql = ctx.build_with_query(&dr.cte_name);
        assert!(
            sql.contains("agg_rescan"),
            "MODE diff should generate rescan CTE: {sql}",
        );
    }

    #[test]
    fn test_diff_aggregate_percentile_cont() {
        let mut ctx = test_ctx_with_st("public", "st");
        let agg = OpTree::Aggregate {
            group_by: vec![colref("dept")],
            aggregates: vec![percentile_cont_col("0.5", "salary", "median_salary")],
            child: Box::new(scan(1, "employees", "public", "e", &["dept", "salary"])),
        };
        let result = diff_aggregate(&mut ctx, &agg);
        assert!(
            result.is_ok(),
            "PERCENTILE_CONT aggregate should diff: {result:?}",
        );
        let dr = result.unwrap();
        assert!(
            dr.columns.contains(&"median_salary".to_string()),
            "Output should include PERCENTILE_CONT alias: {:?}",
            dr.columns,
        );
        let sql = ctx.build_with_query(&dr.cte_name);
        assert!(
            sql.contains("agg_rescan"),
            "PERCENTILE_CONT diff should generate rescan CTE: {sql}",
        );
    }

    #[test]
    fn test_diff_aggregate_percentile_disc() {
        let mut ctx = test_ctx_with_st("public", "st");
        let agg = OpTree::Aggregate {
            group_by: vec![colref("dept")],
            aggregates: vec![percentile_disc_col("0.75", "score", "p75")],
            child: Box::new(scan(1, "t", "public", "t", &["dept", "score"])),
        };
        let result = diff_aggregate(&mut ctx, &agg);
        assert!(
            result.is_ok(),
            "PERCENTILE_DISC aggregate should diff: {result:?}",
        );
        let dr = result.unwrap();
        assert!(
            dr.columns.contains(&"p75".to_string()),
            "Output should include PERCENTILE_DISC alias: {:?}",
            dr.columns,
        );
    }

    #[test]
    fn test_rescan_agg_mode_disqualifies_p5_bypass() {
        let child = scan(1, "t", "public", "t", &["id", "region", "category"]);
        let group_by = vec![colref("region")];
        let aggs = vec![mode_col("category", "most_common")];
        assert!(
            !is_direct_agg_eligible(&child, &group_by, &aggs),
            "MODE should not be eligible for P5 bypass",
        );
    }

    #[test]
    fn test_rescan_agg_percentile_cont_disqualifies_p5_bypass() {
        let child = scan(1, "t", "public", "t", &["id", "dept", "salary"]);
        let group_by = vec![colref("dept")];
        let aggs = vec![percentile_cont_col("0.5", "salary", "median")];
        assert!(
            !is_direct_agg_eligible(&child, &group_by, &aggs),
            "PERCENTILE_CONT should not be eligible for P5 bypass",
        );
    }

    #[test]
    fn test_rescan_agg_percentile_disc_disqualifies_p5_bypass() {
        let child = scan(1, "t", "public", "t", &["id", "dept", "score"]);
        let group_by = vec![colref("dept")];
        let aggs = vec![percentile_disc_col("0.75", "score", "p75")];
        assert!(
            !is_direct_agg_eligible(&child, &group_by, &aggs),
            "PERCENTILE_DISC should not be eligible for P5 bypass",
        );
    }

    #[test]
    fn test_diff_aggregate_mixed_with_ordered_set() {
        let mut ctx = test_ctx_with_st("public", "st");
        let agg = OpTree::Aggregate {
            group_by: vec![colref("dept")],
            aggregates: vec![
                count_star("cnt"),
                sum_col("salary", "total"),
                percentile_cont_col("0.5", "salary", "median_salary"),
            ],
            child: Box::new(scan(1, "employees", "public", "e", &["dept", "salary"])),
        };
        let result = diff_aggregate(&mut ctx, &agg);
        assert!(
            result.is_ok(),
            "Mixed COUNT + SUM + PERCENTILE_CONT should diff: {result:?}",
        );
        let dr = result.unwrap();
        let sql = ctx.build_with_query(&dr.cte_name);
        // COUNT uses algebraic merge
        assert!(
            sql.contains("COALESCE(d.__ins_count, 0) - COALESCE(d.__del_count, 0)"),
            "COUNT should use algebraic merge: {sql}",
        );
        // PERCENTILE_CONT uses rescan sentinel
        assert!(
            sql.contains("agg_rescan"),
            "PERCENTILE_CONT should generate rescan CTE: {sql}",
        );
    }

    #[test]
    fn test_diff_aggregate_mode_with_filter() {
        let agg = with_filter(
            mode_col("category", "most_common"),
            binop("=", colref("active"), lit("true")),
        );
        let (ins, _del) = agg_delta_exprs(&agg, &["category".to_string(), "active".to_string()]);
        assert!(
            ins.contains("(active = true)"),
            "Filtered MODE should include filter: {ins}",
        );
    }

    #[test]
    fn test_diff_aggregate_percentile_cont_with_filter() {
        let agg = with_filter(
            percentile_cont_col("0.5", "amount", "median"),
            binop(">", colref("amount"), lit("0")),
        );
        let (ins, _del) = agg_delta_exprs(&agg, &["amount".to_string()]);
        assert!(
            ins.contains("(amount > 0)"),
            "Filtered PERCENTILE_CONT should include filter: {ins}",
        );
    }

    // ── A-1: Verify rescan CTE does not leak into algebraic aggregate SQL ─────

    #[test]
    fn test_no_rescan_cte_for_sum() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["region", "amount"]);
        let tree = aggregate(
            vec![colref("region")],
            vec![sum_col("amount", "total")],
            child,
        );
        let result = diff_aggregate(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);
        assert_sql_not_contains(&sql, "agg_rescan");
    }

    #[test]
    fn test_no_rescan_cte_for_count_star() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["region", "val"]);
        let tree = aggregate(vec![colref("region")], vec![count_star("cnt")], child);
        let result = diff_aggregate(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);
        assert_sql_not_contains(&sql, "agg_rescan");
    }

    #[test]
    fn test_no_rescan_cte_for_avg() {
        // AVG is now algebraic via auxiliary columns — no rescan needed
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["region", "amount"]);
        let tree = aggregate(
            vec![colref("region")],
            vec![avg_col("amount", "avg_amt")],
            child,
        );
        let result = diff_aggregate(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);
        assert_sql_not_contains(&sql, "agg_rescan");
    }

    #[test]
    fn test_no_rescan_cte_for_sum_count_avg_combined() {
        // All three (SUM, COUNT, AVG) are algebraic — no rescan needed
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["region", "amount"]);
        let tree = aggregate(
            vec![colref("region")],
            vec![
                sum_col("amount", "total"),
                count_star("cnt"),
                avg_col("amount", "avg_amt"),
            ],
            child,
        );
        let result = diff_aggregate(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);
        assert_sql_not_contains(&sql, "agg_rescan");
    }

    #[test]
    fn test_rescan_cte_for_min_max() {
        // MIN/MAX now include a rescan CTE so that when the old extremum
        // is deleted, the correct new extremum is rescanned from source.
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["region", "amount"]);
        let tree = aggregate(
            vec![colref("region")],
            vec![min_col("amount", "min_amt"), max_col("amount", "max_amt")],
            child,
        );
        let result = diff_aggregate(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);
        assert_sql_contains(&sql, "agg_rescan");
        // Rescan fallback in merge: when extremum deleted, use r.{col}
        assert_sql_contains(&sql, "THEN r.\"min_amt\"");
        assert_sql_contains(&sql, "THEN r.\"max_amt\"");
    }

    #[test]
    fn test_rescan_cte_only_for_group_rescan_aggregates() {
        // Mixed: SUM (algebraic) + BIT_AND (group-rescan)
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["region", "flags", "amount"]);
        let tree = aggregate(
            vec![colref("region")],
            vec![
                sum_col("amount", "total"),
                bit_and_col("flags", "all_flags"),
            ],
            child,
        );
        let result = diff_aggregate(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);
        // Rescan CTE should be present because BIT_AND requires it
        assert_sql_contains(&sql, "agg_rescan");
        // But the algebraic SUM should still use algebraic merge (COALESCE + ins - del)
        assert_sql_contains(
            &sql,
            "COALESCE(d.\"__ins_total\", 0) - COALESCE(d.\"__del_total\", 0)",
        );
    }

    // ── SUM NULL-transition correction ─────────────────────────────────

    #[test]
    fn test_p2_2_sum_full_join_uses_nonnull_aux_not_rescan() {
        // P2-2: SUM above a FULL OUTER JOIN should use the __pgt_aux_nonnull_*
        // auxiliary column for algebraic NULL-transition correction instead of
        // a full-group rescan CTE.
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "l", "public", "l", &["id", "amount"]);
        let right = scan(2, "r", "public", "r", &["id", "value"]);
        let join_child = full_join(
            binop("=", qcolref("l", "id"), qcolref("r", "id")),
            left,
            right,
        );
        let tree = aggregate(
            vec![qcolref("l", "id")],
            vec![sum_col("value", "total")],
            join_child,
        );

        let result = diff_aggregate(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Must NOT produce a rescan CTE (P2-2: nonnull-count replaces rescan)
        assert!(
            !sql.contains("agg_rescan"),
            "P2-2: SUM above FULL JOIN should not use rescan CTE, got:\n{sql}"
        );

        // Must track nonnull delta in the delta CTE
        assert_sql_contains(&sql, "__ins_nonnull_total");
        assert_sql_contains(&sql, "__del_nonnull_total");

        // Merge CTE must update the nonnull-count auxiliary column
        assert_sql_contains(&sql, "new___pgt_aux_nonnull_total");

        // The merge expression for SUM must use the CASE … nonnull > 0 form
        assert_sql_contains(&sql, "__pgt_aux_nonnull_total");

        // Output columns must include the nonnull auxiliary
        assert!(
            result
                .columns
                .contains(&"__pgt_aux_nonnull_total".to_string()),
            "output_cols should include __pgt_aux_nonnull_total: {:?}",
            result.columns
        );
    }

    #[test]
    fn test_sum_plain_scan_uses_nonnull_aux() {
        // Every non-DISTINCT SUM needs the nonnull count, including a plain
        // scan, so the last non-NULL deletion restores SUM(NULL).
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["region", "amount"]);
        let tree = aggregate(
            vec![colref("region")],
            vec![sum_col("amount", "total")],
            child,
        );

        let result = diff_aggregate(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // No rescan CTE — plain scan, purely algebraic
        assert!(
            !sql.contains("agg_rescan"),
            "SUM above plain scan should not use rescan CTE:\n{sql}"
        );

        assert_sql_contains(&sql, "__ins_nonnull_total");
        assert_sql_contains(&sql, "__del_nonnull_total");

        // Standard algebraic expression
        assert_sql_contains(
            &sql,
            "COALESCE(d.\"__ins_total\", 0) - COALESCE(d.\"__del_total\", 0)",
        );

        assert!(
            result
                .columns
                .contains(&"__pgt_aux_nonnull_total".to_string())
        );
    }

    #[test]
    fn test_sum_filter_tracks_only_qualifying_nonnull_values() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["region", "amount", "active"]);
        let agg = AggExpr {
            function: AggFunc::Sum,
            argument: Some(colref("amount")),
            alias: "total".into(),
            is_distinct: false,
            filter: Some(Expr::ColumnRef {
                table_alias: None,
                column_name: "active".into(),
            }),
            second_arg: None,
            order_within_group: None,
            statistical_support: None,
        };
        let tree = aggregate(vec![colref("region")], vec![agg], child);
        let result = diff_aggregate(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        assert!(sql.contains("amount IS NOT NULL AND active"));
        assert!(sql.contains("__pgt_aux_nonnull_total"));
    }

    #[test]
    fn test_coalesce_sum_restores_null_before_projection() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["region", "amount"]);
        let agg = aggregate(
            vec![colref("region")],
            vec![sum_col("amount", "total")],
            child,
        );
        let tree = project(
            vec![
                colref("region"),
                Expr::FuncCall {
                    func_name: "COALESCE".into(),
                    args: vec![colref("total"), Expr::Literal("0".into())],
                },
            ],
            vec!["region", "total"],
            agg,
        );

        let result = crate::dvm::operators::project::diff_project(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);
        assert!(sql.contains("ELSE NULL"));
        assert!(sql.contains("COALESCE"));
    }

    #[test]
    fn test_p2_2_sum_full_join_correct_null_transition_formula() {
        // Verify the CASE expression used for SUM over a FULL JOIN:
        //   CASE WHEN (__pgt_aux_nonnull_total + ins_nonnull - del_nonnull) > 0
        //        THEN COALESCE(st.total, 0) + ins - del
        //        ELSE NULL   -- for bare SUM: return NULL when all values are NULL
        //   END
        //
        // For bare SUM (no COALESCE wrapper at the Project level), when all
        // argument values are NULL (nonnull = 0), the result should be NULL.
        // COALESCE-wrapped SUM queries use the algebraic formula in the ELSE
        // branch (communicated via ctx.agg_sum_coalesce_defaults by diff_project).
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "l", "public", "l", &["id", "score"]);
        let right = scan(2, "r", "public", "r", &["id", "score"]);
        let join_child = full_join(
            binop("=", qcolref("l", "id"), qcolref("r", "id")),
            left,
            right,
        );
        let tree = aggregate(
            vec![qcolref("l", "id")],
            vec![sum_col("score", "sum_score")],
            join_child,
        );

        let result = diff_aggregate(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // The nonnull-guard CASE expression must be present
        assert_sql_contains(&sql, "__pgt_aux_nonnull_sum_score");

        // For bare SUM (no COALESCE wrapper), ELSE must return NULL so the ST
        // stores NULL when all argument values become NULL.
        assert_sql_contains(&sql, "ELSE NULL");
        assert!(
            !sql.contains("ELSE COALESCE("),
            "P2-2 bare SUM ELSE branch should return NULL, not COALESCE formula,\
             got:\n{sql}"
        );

        // Delta CTE nonnull tracking
        assert_sql_contains(&sql, "__ins_nonnull_sum_score");
        assert_sql_contains(&sql, "__del_nonnull_sum_score");
    }

    #[test]
    fn test_diff_aggregate_mixed_count_sum_complex_expression() {
        // Simulates: SELECT dept, COUNT(*) AS cnt, SUM(amount) AS total,
        //            ROUND(ROUND(STDDEV_POP(amount), 4), 4) AS sd
        //            FROM mstat_src GROUP BY dept
        // The ComplexExpression is a group-rescan aggregate; COUNT* and SUM are algebraic.
        let mut ctx = test_ctx_with_st("public", "mstat_st");
        let child = scan(
            1,
            "mstat_src",
            "public",
            "mstat_src",
            &["id", "dept", "amount"],
        );

        let complex_sd = AggExpr {
            function: AggFunc::ComplexExpression(
                "round(round(stddev_pop(amount), 4), 4)".to_string(),
            ),
            argument: None,
            alias: "sd".to_string(),
            is_distinct: false,
            filter: None,
            second_arg: None,
            order_within_group: None,
            statistical_support: None,
        };

        let tree = aggregate(
            vec![colref("dept")],
            vec![count_star("cnt"), sum_col("amount", "total"), complex_sd],
            child,
        );

        let result = diff_aggregate(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Rescan CTE should be present because ComplexExpression requires it
        assert_sql_contains(&sql, "agg_rescan");

        // ComplexExpression delta tracking should count insertions
        assert_sql_contains(&sql, "__ins_sd");
        assert_sql_contains(&sql, "__del_sd");

        // The rescan CTE should compute the ComplexExpression from source
        assert_sql_contains(&sql, "round(round(stddev_pop(amount), 4), 4)");

        // COUNT* should use algebraic merge
        assert_sql_contains(
            &sql,
            "COALESCE(d.\"__ins_cnt\", 0) - COALESCE(d.\"__del_cnt\", 0)",
        );

        // SUM should use algebraic merge (no full-join child)
        assert_sql_contains(
            &sql,
            "COALESCE(d.\"__ins_total\", 0) - COALESCE(d.\"__del_total\", 0)",
        );

        // ComplexExpression should use rescan CTE value when changed
        assert_sql_contains(&sql, "THEN r.\"sd\"");

        // Output columns should include cnt, total, sd but NOT aux columns
        assert!(result.columns.contains(&"cnt".to_string()));
        assert!(result.columns.contains(&"total".to_string()));
        assert!(result.columns.contains(&"sd".to_string()));
        assert!(
            !result.columns.contains(&"__pgt_aux_sum_sd".to_string()),
            "ComplexExpression should not create aux sum columns"
        );
        assert!(
            !result.columns.contains(&"__pgt_aux_count_sd".to_string()),
            "ComplexExpression should not create aux count columns"
        );
        assert!(
            !result.columns.contains(&"__pgt_aux_sum2_sd".to_string()),
            "ComplexExpression should not create aux sum2 columns"
        );

        // Print SQL for debugging (will only show when test fails or with --nocapture)
        eprintln!("=== Mixed COUNT* + SUM + ComplexExpression SQL ===\n{sql}");
    }

    // ── child_to_from_sql tests (SF-4) ──────────────────────────────

    #[test]
    fn test_child_to_from_sql_scan_projects_pruned_columns() {
        let child = scan(
            1,
            "events",
            "public",
            "e",
            &["id", "group_id", "created_at"],
        );

        let sql = child_to_from_sql(&child, &CteRegistry::default()).unwrap();

        assert_eq!(
            sql,
            "(SELECT \"id\", \"group_id\", \"created_at\" FROM \"public\".\"events\") AS \"e\""
        );
    }

    #[test]
    fn test_child_to_from_sql_project_over_scan() {
        // Project { EXTRACT(year FROM o_orderdate) AS o_year, o_totalprice }
        // over Scan should produce a subquery with the projected columns.
        let child = project(
            vec![
                Expr::FuncCall {
                    func_name: "extract_year".to_string(),
                    args: vec![colref("o_orderdate")],
                },
                colref("o_totalprice"),
            ],
            vec!["o_year", "o_totalprice"],
            scan(
                1,
                "orders",
                "public",
                "orders",
                &["o_orderdate", "o_totalprice"],
            ),
        );

        let result = child_to_from_sql(&child, &CteRegistry::default());
        assert!(result.is_some(), "Project over Scan should return Some");
        let sql = result.unwrap();
        // Should be a subquery wrapping the scan
        assert!(
            sql.starts_with('('),
            "Should be wrapped in a subquery: {sql}"
        );
        assert!(
            sql.contains("__pgt_proj"),
            "Should have __pgt_proj alias: {sql}"
        );
        assert!(
            sql.contains("AS \"o_year\""),
            "Should alias the expression: {sql}"
        );
        assert!(
            sql.contains("\"o_totalprice\""),
            "Should include pass-through column: {sql}"
        );
    }

    #[test]
    fn test_child_to_from_sql_project_over_non_agg_subquery_returns_none() {
        // Project over Subquery { child: Scan } — Subquery returns None
        // for non-Aggregate children, so Project should also return None.
        let child = project(
            vec![colref("id")],
            vec!["id"],
            OpTree::Subquery {
                child: Box::new(scan(1, "t", "public", "t", &["id"])),
                alias: "sub".to_string(),
                column_aliases: vec![],
            },
        );

        let result = child_to_from_sql(&child, &CteRegistry::default());
        assert!(
            result.is_none(),
            "Project over non-Aggregate Subquery should return None"
        );
    }

    #[test]
    fn test_child_to_from_sql_cte_scan_resolves_via_registry() {
        // CteScan whose body is a Scan should resolve to the underlying
        // table's FROM clause, not the CTE alias. This covers the
        // chained-CTE case: WITH raw AS (SELECT ... FROM metrics), averages
        // AS (SELECT ... FROM raw GROUP BY ...) — where `raw` is not a real
        // relation and must be resolved through the registry.
        let metrics_scan = scan(
            1,
            "metrics",
            "public",
            "metrics",
            &["id", "sensor", "reading"],
        );
        let registry = CteRegistry {
            entries: vec![("raw".to_string(), metrics_scan)],
        };

        let cte_child = cte_scan(
            0,
            "raw",
            "raw",
            vec!["id", "sensor", "reading"],
            vec![],
            vec![],
        );
        let result = child_to_from_sql(&cte_child, &registry);

        assert!(result.is_some(), "CteScan over Scan should resolve to Some");
        let sql = result.unwrap();
        assert!(
            sql.contains("metrics"),
            "Should resolve to the underlying table, not the CTE alias: {sql}"
        );
        assert!(
            !sql.contains("\"raw\""),
            "Should NOT emit the CTE alias as a bare table reference: {sql}"
        );
    }

    #[test]
    fn test_child_to_from_sql_cte_scan_unresolvable_returns_none() {
        // CteScan with a cte_id that is not in the registry should return None
        // so callers fall back to the defining-query approach.
        let registry = CteRegistry::default(); // empty
        let cte_child = cte_scan(0, "missing", "missing", vec!["id"], vec![], vec![]);
        let result = child_to_from_sql(&cte_child, &registry);
        assert!(
            result.is_none(),
            "CteScan with missing registry entry should return None"
        );
    }

    // ── A-2: Value-only UPDATE optimization tests ───────────────────

    #[test]
    fn test_direct_agg_delta_exprs_sum_without_value_only() {
        // A44-10 (D+I schema): flat c.action filtering, no LATERAL VALUES
        let agg = sum_col("amount", "total");
        let (ins, del) = direct_agg_delta_exprs(&agg, false);
        assert!(
            ins.contains("c.action = 'I'"),
            "SUM insert expr should use c.action = 'I': {ins}"
        );
        assert!(
            ins.contains("c.\"amount\""),
            "SUM insert expr should reference flat c.\"amount\": {ins}"
        );
        assert!(
            del.contains("c.action = 'D'"),
            "SUM delete expr should use c.action = 'D': {del}"
        );
    }

    #[test]
    fn test_direct_agg_delta_exprs_sum_with_value_only() {
        // A44-10 (D+I schema): has_value_only is always false; V-row optimization
        // dropped. Behaviour is same as without value-only — c.action filtering.
        let agg = sum_col("amount", "total");
        let (ins, del) = direct_agg_delta_exprs(&agg, true);
        assert!(
            ins.contains("c.action = 'I'"),
            "SUM insert expr should use c.action = 'I' (D+I schema): {ins}"
        );
        assert!(
            del.contains("c.action = 'D'"),
            "SUM delete expr should use c.action = 'D' (D+I schema): {del}"
        );
        // V-row optimization is dropped in D+I schema
        assert!(
            !ins.contains("'V'"),
            "SUM insert expr should NOT include 'V' in D+I schema: {ins}"
        );
    }

    #[test]
    fn test_direct_agg_delta_exprs_count_star_ignores_value_only() {
        // COUNT(*) should not include 'V' even with has_value_only=true,
        // because value-only updates don't change row counts.
        let agg = count_star("cnt");
        let (ins, del) = direct_agg_delta_exprs(&agg, true);
        assert!(
            !ins.contains("'V'"),
            "COUNT(*) insert should not include 'V': {ins}"
        );
        assert!(
            !del.contains("'V'"),
            "COUNT(*) delete should not include 'V': {del}"
        );
    }

    #[test]
    fn test_direct_agg_delta_exprs_count_col_ignores_value_only() {
        let agg = AggExpr {
            function: AggFunc::Count,
            argument: Some(colref("id")),
            alias: "cnt".to_string(),
            is_distinct: false,
            filter: None,
            second_arg: None,
            order_within_group: None,
            statistical_support: None,
        };
        let (ins, del) = direct_agg_delta_exprs(&agg, true);
        assert!(
            !ins.contains("'V'"),
            "COUNT(col) insert should not include 'V': {ins}"
        );
        assert!(
            !del.contains("'V'"),
            "COUNT(col) delete should not include 'V': {del}"
        );
    }

    #[test]
    fn test_a2_p5_value_only_generates_v_row() {
        // A44-10 (D+I schema): V-row optimization dropped. generate_direct_agg_delta
        // should produce direct c.action filtering even when key columns are set.
        let child = scan(1, "t", "public", "t", &["id", "region", "amount"]);
        let group_by = vec![colref("region")];
        let aggs = vec![sum_col("amount", "total")];
        let mut ctx = crate::dvm::diff::DiffContext::new_standalone(
            crate::version::Frontier::default(),
            crate::version::Frontier::default(),
        );
        ctx.source_key_columns.insert(1, vec!["region".to_string()]);
        ctx.source_cdc_columns.insert(
            1,
            vec!["id".to_string(), "region".to_string(), "amount".to_string()],
        );
        let result = generate_direct_agg_delta(&mut ctx, &child, &group_by, &aggs);
        assert!(result.is_ok());
        let (cte_name, _group_out) = result.unwrap();
        let sql = ctx.cte_sql(&cte_name).expect("CTE should exist");
        // D+I schema: no LATERAL VALUES, no 'V' rows, use c.action directly
        assert!(
            !sql.contains("'V'"),
            "P5 SQL should NOT contain 'V' in D+I schema: {sql}"
        );
        assert!(
            sql.contains("c.action = 'I'"),
            "P5 SQL should use c.action = 'I' for insert side: {sql}"
        );
        assert!(
            sql.contains("c.action = 'D'"),
            "P5 SQL should use c.action = 'D' for delete side: {sql}"
        );
    }

    #[test]
    fn test_a2_p5_no_value_only_without_key_columns() {
        // Without key columns set, the standard P5 path should be used (no 'V').
        let child = scan(1, "t", "public", "t", &["id", "region", "amount"]);
        let group_by = vec![colref("region")];
        let aggs = vec![sum_col("amount", "total")];
        let mut ctx = crate::dvm::diff::DiffContext::new_standalone(
            crate::version::Frontier::default(),
            crate::version::Frontier::default(),
        );
        // No source_key_columns or source_cdc_columns set.
        let result = generate_direct_agg_delta(&mut ctx, &child, &group_by, &aggs);
        assert!(result.is_ok());
        let (cte_name, _group_out) = result.unwrap();
        let sql = ctx.cte_sql(&cte_name).expect("CTE should exist");
        assert!(
            !sql.contains("'V'"),
            "P5 SQL should NOT contain 'V' without key columns: {sql}"
        );
    }

    #[test]
    fn test_a2_p5_all_keys_no_value_only() {
        // When all columns are key columns, compute_varbit_key_cols_mask
        // returns None → no value-only optimization.
        let child = scan(1, "t", "public", "t", &["id", "region"]);
        let group_by = vec![colref("id"), colref("region")];
        let aggs = vec![count_star("cnt")];
        let mut ctx = crate::dvm::diff::DiffContext::new_standalone(
            crate::version::Frontier::default(),
            crate::version::Frontier::default(),
        );
        ctx.source_key_columns
            .insert(1, vec!["id".to_string(), "region".to_string()]);
        ctx.source_cdc_columns
            .insert(1, vec!["id".to_string(), "region".to_string()]);
        let result = generate_direct_agg_delta(&mut ctx, &child, &group_by, &aggs);
        assert!(result.is_ok());
        let (cte_name, _) = result.unwrap();
        let sql = ctx.cte_sql(&cte_name).expect("CTE should exist");
        assert!(
            !sql.contains("'V'"),
            "All columns are keys — no value-only optimization: {sql}"
        );
    }

    // ── A44-2 (v0.43.0): DI-2 CASE expression tests ─────────────────

    #[test]
    fn test_a44_2_extract_case_columns_simple() {
        let case_sql = "CASE WHEN status = 'active' THEN amount ELSE 0 END";
        let cdc_cols = vec![
            "id".to_string(),
            "status".to_string(),
            "amount".to_string(),
            "region".to_string(),
        ];
        let extracted = extract_case_columns(case_sql, &cdc_cols);
        let names: Vec<&str> = extracted.iter().map(|s| s.as_str()).collect();
        assert!(
            names.contains(&"status"),
            "should extract 'status': {names:?}"
        );
        assert!(
            names.contains(&"amount"),
            "should extract 'amount': {names:?}"
        );
        assert!(!names.contains(&"id"), "should not extract 'id': {names:?}");
    }

    #[test]
    fn test_a44_2_extract_case_columns_no_match() {
        let case_sql = "CASE WHEN 1 = 1 THEN 42 ELSE 0 END";
        let cdc_cols = vec!["status".to_string(), "amount".to_string()];
        let extracted = extract_case_columns(case_sql, &cdc_cols);
        assert!(
            extracted.is_empty(),
            "No columns should be extracted from all-literal CASE: {extracted:?}"
        );
    }

    #[test]
    fn test_a44_2_rewrite_case_for_lateral_simple() {
        // A44-10 (D+I schema): rewrite_case_for_lateral now produces c."col" direct
        // references instead of v."val_col" LATERAL VALUES aliases.
        let case_sql = "CASE WHEN status = 'active' THEN amount ELSE 0 END";
        let cols: Vec<String> = vec!["status".to_string(), "amount".to_string()];
        let col_refs: Vec<&String> = cols.iter().collect();
        let rewritten = rewrite_case_for_lateral(case_sql, &col_refs);
        assert!(
            rewritten.contains(r#"c."status""#),
            "status should be rewritten to c.\"status\": {rewritten}"
        );
        assert!(
            rewritten.contains(r#"c."amount""#),
            "amount should be rewritten to c.\"amount\": {rewritten}"
        );
        assert!(
            !rewritten.contains("= status"),
            "bare 'status' should be gone: {rewritten}"
        );
    }

    #[test]
    fn test_a44_2_sum_case_eligible_for_p5_with_cdc_cols() {
        // SUM(CASE ...) should be P5-eligible when CDC columns are set.
        let child = scan(1, "t", "public", "t", &["id", "status", "amount", "region"]);
        let group_by = vec![colref("region")];
        let aggs = vec![AggExpr {
            function: AggFunc::Sum,
            argument: Some(Expr::Raw(
                "CASE WHEN status = 'active' THEN amount ELSE 0 END".to_string(),
            )),
            alias: "active_total".to_string(),
            is_distinct: false,
            filter: None,
            second_arg: None,
            order_within_group: None,
            statistical_support: None,
        }];
        let mut ctx = crate::dvm::diff::DiffContext::new_standalone(
            crate::version::Frontier::default(),
            crate::version::Frontier::default(),
        );
        ctx.source_cdc_columns.insert(
            1,
            vec![
                "id".to_string(),
                "status".to_string(),
                "amount".to_string(),
                "region".to_string(),
            ],
        );
        assert!(
            super::is_direct_agg_eligible(&child, &group_by, &aggs, &ctx),
            "SUM(CASE ...) with CDC cols should be P5-eligible"
        );
    }

    #[test]
    fn test_a44_2_sum_case_not_eligible_without_cdc_cols() {
        // SUM(CASE ...) should NOT be P5-eligible when CDC columns are missing.
        let child = scan(1, "t", "public", "t", &["id", "status", "amount"]);
        let group_by = vec![colref("region")];
        let aggs = vec![AggExpr {
            function: AggFunc::Sum,
            argument: Some(Expr::Raw(
                "CASE WHEN status = 'active' THEN amount ELSE 0 END".to_string(),
            )),
            alias: "active_total".to_string(),
            is_distinct: false,
            filter: None,
            second_arg: None,
            order_within_group: None,
            statistical_support: None,
        }];
        // Use local wrapper (empty ctx — no CDC cols).
        assert!(
            !is_direct_agg_eligible(&child, &group_by, &aggs),
            "SUM(CASE ...) without CDC cols should NOT be P5-eligible"
        );
    }

    #[test]
    fn test_a44_2_sum_case_di2_generates_correct_sql() {
        // The P5 path for SUM(CASE ...) should produce SQL with:
        // 1. LATERAL VALUES containing old_*/new_* for each CASE column
        // 2. A rewritten CASE expression using v.val_* references
        // 3. No 'V' (value-only) row (disabled for CASE aggregates)
        let child = scan(1, "t", "public", "t", &["id", "status", "amount", "region"]);
        let group_by = vec![colref("region")];
        let aggs = vec![AggExpr {
            function: AggFunc::Sum,
            argument: Some(Expr::Raw(
                "CASE WHEN status = 'active' THEN amount ELSE 0 END".to_string(),
            )),
            alias: "active_total".to_string(),
            is_distinct: false,
            filter: None,
            second_arg: None,
            order_within_group: None,
            statistical_support: None,
        }];
        let mut ctx = crate::dvm::diff::DiffContext::new_standalone(
            crate::version::Frontier::default(),
            crate::version::Frontier::default(),
        );
        ctx.source_cdc_columns.insert(
            1,
            vec![
                "id".to_string(),
                "status".to_string(),
                "amount".to_string(),
                "region".to_string(),
            ],
        );
        // Key columns set to trigger potential V-row (but should be disabled for CASE).
        ctx.source_key_columns.insert(1, vec!["id".to_string()]);

        let result = generate_direct_agg_delta(&mut ctx, &child, &group_by, &aggs);
        assert!(
            result.is_ok(),
            "SUM(CASE ...) P5 should succeed: {result:?}"
        );
        let (cte_name, _group_out) = result.unwrap();
        let sql = ctx.cte_sql(&cte_name).expect("CTE should exist");

        // A44-10 (D+I schema): rewritten CASE uses c."col" references.
        // No LATERAL VALUES, no v.side — use c.action directly.
        assert!(
            sql.contains(r#"c."status""#),
            "SQL should contain c.\"status\" reference: {sql}"
        );
        assert!(
            sql.contains(r#"c."amount""#),
            "SQL should contain c.\"amount\" reference: {sql}"
        );
        // V-row should not be present in D+I schema.
        assert!(
            !sql.contains("'V'"),
            "V-row should be absent in D+I schema: {sql}"
        );
        // Should use c.action = 'I'/'D' directly.
        assert!(
            sql.contains("c.action = 'I'"),
            "SQL should filter on c.action = 'I': {sql}"
        );
        assert!(
            sql.contains("c.action = 'D'"),
            "SQL should filter on c.action = 'D': {sql}"
        );
    }
}
