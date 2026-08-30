//! Shared helpers for join differentiation operators.
//!
//! Provides snapshot SQL generation and condition rewriting that handle
//! both simple (Scan) and nested (join-of-join) children correctly.
//!
//! ## Nested join handling
//!
//! When a join child is itself a join (e.g., `(A ⋈ B) ⋈ C`), two things
//! differ from the simple binary case:
//!
//! 1. **Snapshot**: The "current state" of the left/right child is not
//!    a plain table reference but a subquery: `(SELECT a."id" AS "a__id",
//!    ... FROM a JOIN b ON ...)`.
//!
//! 2. **Condition rewriting**: The join condition references original
//!    table aliases (e.g., `o.prod_id = p.id`). For nested children,
//!    these aliases are *inside* the snapshot subquery and must be
//!    translated to disambiguated column names (e.g., `l."o__prod_id"`).

use std::collections::{HashMap, HashSet};

use crate::dvm::diff::quote_ident;
use crate::dvm::operators::aggregate::agg_to_rescan_sql;
use crate::dvm::parser::{Expr, OpTree};

// ── Snapshot SQL generation ─────────────────────────────────────────────

/// Build a SQL expression for the current snapshot of an operator subtree.
///
/// For `Scan` nodes, returns the quoted `"schema"."table"` reference.
/// For join nodes, returns a parenthesized subquery with disambiguated
/// column names matching the diff engine's output format.
///
/// Used in join delta formulas where one side of the join must reference
/// the current full state of the other side.
pub fn build_snapshot_sql(op: &OpTree) -> String {
    match op {
        OpTree::Scan {
            schema, table_name, ..
        } => {
            format!(
                "\"{}\".\"{}\"",
                schema.replace('"', "\"\""),
                table_name.replace('"', "\"\""),
            )
        }
        OpTree::CteScan {
            alias,
            body,
            columns,
            cte_def_aliases,
            column_aliases,
            ..
        } => {
            // Delegate to the body's snapshot when available (always in
            // production; `None` only in unit-test stubs that never call
            // this function).  This prevents the old bug where the CTE alias
            // (e.g. "p") was emitted as a bare relation name, causing
            // "relation 'p' does not exist" errors at refresh time.
            let Some(body) = body else {
                // Unit-test fallback — alias is not a real relation but we
                // cannot do better without a body.
                return quote_ident(alias);
            };

            let body_snap = build_snapshot_sql(body);
            let body_cols = body.output_columns();

            // Determine the CTE's effective output column names.
            let effective_cols: Vec<&str> = if !column_aliases.is_empty() {
                column_aliases.iter().map(|s| s.as_str()).collect()
            } else if !cte_def_aliases.is_empty() {
                cte_def_aliases.iter().map(|s| s.as_str()).collect()
            } else {
                columns.iter().map(|s| s.as_str()).collect()
            };

            // When body columns differ from effective CTE columns (due to
            // CTE-level column aliases), wrap in a renaming SELECT.
            if body_cols
                .iter()
                .zip(effective_cols.iter())
                .any(|(b, e)| b.as_str() != *e)
                || body_cols.len() != effective_cols.len()
            {
                let selects: Vec<String> = body_cols
                    .iter()
                    .zip(effective_cols.iter())
                    .map(|(src, dst)| {
                        if src.as_str() == *dst {
                            quote_ident(src)
                        } else {
                            format!("{} AS {}", quote_ident(src), quote_ident(dst))
                        }
                    })
                    .collect();
                format!(
                    "(SELECT {} FROM {} {})",
                    selects.join(", "),
                    body_snap,
                    quote_ident(body.alias()),
                )
            } else {
                body_snap
            }
        }
        OpTree::InnerJoin {
            condition,
            left,
            right,
        } => build_join_snapshot("JOIN", condition, left, right),
        OpTree::LeftJoin {
            condition,
            left,
            right,
        } => build_join_snapshot("LEFT JOIN", condition, left, right),
        OpTree::FullJoin {
            condition,
            left,
            right,
        } => build_join_snapshot("FULL JOIN", condition, left, right),
        OpTree::Filter { predicate, child } => {
            let child_snap = build_snapshot_sql(child);
            if matches!(child.as_ref(), OpTree::Scan { .. }) {
                let alias = child.alias();
                format!(
                    "(SELECT * FROM {} {} WHERE {})",
                    child_snap,
                    quote_ident(alias),
                    predicate.to_sql()
                )
            } else if matches!(child.as_ref(), OpTree::Aggregate { .. }) {
                // Filter on top of Aggregate = HAVING clause.
                // The child_snap is a `(SELECT ... GROUP BY ...)` subquery.
                // Wrap in an outer SELECT to apply the HAVING predicate.
                format!(
                    "(SELECT * FROM {} __having_sub WHERE {})",
                    child_snap,
                    predicate.to_sql()
                )
            } else {
                // For non-Scan children (e.g. Filter over Join), the filter
                // is applied by diff_filter in the diff pipeline. The
                // snapshot represents the unfiltered child state.
                child_snap
            }
        }
        OpTree::Project {
            expressions,
            aliases,
            child,
        } => {
            // A Project renames/transforms columns. The snapshot must preserve
            // these aliases so that downstream join conditions can reference
            // the projected column names (e.g., `__pgt_scalar_1` from a
            // scalar subquery CROSS JOIN rewrite).
            //
            // When the child is a join, inline its FROM clause to keep the
            // original table aliases (e.g., "a", "b") accessible so that
            // project expressions like COALESCE(a.k, b.k) can reference them.
            // Without this, wrapping the join snapshot as "full_join" hides
            // the inner aliases, causing "missing FROM-clause entry" errors.
            let selects: Vec<String> = expressions
                .iter()
                .zip(aliases.iter())
                .map(|(expr, alias)| {
                    let expr_sql = expr.to_sql();
                    let alias_ident = quote_ident(alias);
                    if expr_sql == *alias {
                        alias_ident
                    } else {
                        format!("{expr_sql} AS {alias_ident}")
                    }
                })
                .collect();
            if let Some(from_clause) = build_snapshot_inline_from_for_join(child) {
                format!("(SELECT {} FROM {})", selects.join(", "), from_clause)
            } else {
                let inner = build_snapshot_sql(child);
                let child_alias = child.alias();
                format!(
                    "(SELECT {} FROM {} {})",
                    selects.join(", "),
                    inner,
                    quote_ident(child_alias),
                )
            }
        }
        OpTree::Subquery {
            column_aliases,
            child,
            ..
        } => {
            if column_aliases.is_empty() {
                build_snapshot_sql(child)
            } else {
                // Subquery with column aliases (e.g., `(...) AS v("c1", "c2")`).
                // Wrap the child snapshot in a SELECT that renames columns
                // positionally to match the aliases.
                let inner = build_snapshot_sql(child);
                let child_alias = child.alias();
                // Use positional references (ordinal) to rename
                let selects: Vec<String> = column_aliases
                    .iter()
                    .enumerate()
                    .map(|(i, alias)| {
                        // Reference by position: column number i+1
                        // We use a subquery wrapper so we can rename by ordinal
                        format!("__sub.col{} AS {}", i + 1, quote_ident(alias))
                    })
                    .collect();
                // Wrap inner snapshot with ordinal column names
                let child_cols = child.output_columns();
                let inner_selects: Vec<String> = child_cols
                    .iter()
                    .enumerate()
                    .map(|(i, c)| format!("{} AS col{}", quote_ident(c), i + 1))
                    .collect();
                format!(
                    "(SELECT {} FROM (SELECT {} FROM {} {}) __sub)",
                    selects.join(", "),
                    inner_selects.join(", "),
                    inner,
                    quote_ident(child_alias),
                )
            }
        }
        OpTree::Aggregate {
            group_by,
            aggregates,
            child,
        } => {
            let inner = build_snapshot_sql(child);
            let child_alias = child.alias();
            let mut selects = Vec::new();
            for expr in group_by {
                selects.push(expr.to_sql());
            }
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
            format!(
                "(SELECT {} FROM {} {}{})",
                selects.join(", "),
                inner,
                quote_ident(child_alias),
                gb,
            )
        }
        OpTree::SemiJoin {
            condition,
            left,
            right,
        } => {
            let left_snap = build_snapshot_sql(left);
            let right_snap = build_snapshot_sql(right);
            // Use safe non-reserved aliases to avoid Expr::to_sql() emitting
            // unquoted reserved words like "join" (from InnerJoin.alias()).
            let left_alias = "__pgt_sl";
            let right_alias = "__pgt_sr";
            let cond = rewrite_join_condition(condition, left, left_alias, right, right_alias);
            format!(
                "(SELECT {la}.* FROM {left_snap} {la} WHERE EXISTS \
                 (SELECT 1 FROM {right_snap} {ra} WHERE {cond}))",
                la = quote_ident(left_alias),
                ra = quote_ident(right_alias),
            )
        }
        OpTree::AntiJoin {
            condition,
            left,
            right,
        } => {
            let left_snap = build_snapshot_sql(left);
            let right_snap = build_snapshot_sql(right);
            let left_alias = "__pgt_al";
            let right_alias = "__pgt_ar";
            let cond = rewrite_join_condition(condition, left, left_alias, right, right_alias);
            format!(
                "(SELECT {la}.* FROM {left_snap} {la} WHERE NOT EXISTS \
                 (SELECT 1 FROM {right_snap} {ra} WHERE {cond}))",
                la = quote_ident(left_alias),
                ra = quote_ident(right_alias),
            )
        }
        OpTree::Window {
            window_exprs,
            pass_through,
            child,
            ..
        } => {
            let inner = build_snapshot_sql(child);
            let child_alias = child.alias();
            let mut selects: Vec<String> = pass_through
                .iter()
                .map(|(_, alias)| quote_ident(alias))
                .collect();
            for w in window_exprs {
                selects.push(format!("{} AS {}", w.to_sql(), quote_ident(&w.alias)));
            }
            format!(
                "(SELECT {} FROM {} {})",
                selects.join(", "),
                inner,
                quote_ident(child_alias),
            )
        }
        _ => {
            // This node type cannot appear as a direct join child in a snapshot.
            // Raise a PostgreSQL-level error so the user gets a clear message
            // instead of a SQL syntax error from an injected comment.
            pgrx::error!(
                "pg_trickle: operator '{}' is not supported as a direct join source \
                 in DIFFERENTIAL/IMMEDIATE mode; rewrite the defining query to \
                 place this subquery in a named CTE or derived table.",
                op.node_kind()
            );
        }
    }
}

/// Build an inline FROM clause for a join node using `build_snapshot_sql` for each child.
///
/// Returns the raw FROM clause string (e.g., `<left_snap> "a" FULL JOIN <right_snap> "b" ON ...`)
/// *without* a wrapping `(SELECT ... FROM ...)`. This preserves the original table aliases (`a`, `b`)
/// so that `Project` expressions like `COALESCE(a.k, b.k)` can reference them directly.
fn build_snapshot_inline_join_from(
    join_type: &str,
    condition: &Expr,
    left: &OpTree,
    right: &OpTree,
) -> String {
    let left_alias = left.alias();
    let right_alias = right.alias();
    let left_snap = build_snapshot_sql(left);
    let right_snap = build_snapshot_sql(right);
    let cond_sql = rewrite_join_condition(condition, left, left_alias, right, right_alias);
    format!(
        "{} {} {} {} {} ON {}",
        left_snap,
        quote_ident(left_alias),
        join_type,
        right_snap,
        quote_ident(right_alias),
        cond_sql,
    )
}

/// When a `Project` node sits directly over a join, return an inline FROM clause that
/// keeps the original join table aliases accessible.
///
/// This prevents the "missing FROM-clause entry for table 'a'" error that arises when
/// a Project wraps a join snapshot subquery (aliased as `"full_join"` etc.) — inside
/// that subquery the original aliases `a`, `b` are no longer visible.
///
/// Returns `Some(from_clause)` for direct join children; `None` otherwise.
fn build_snapshot_inline_from_for_join(op: &OpTree) -> Option<String> {
    match op {
        OpTree::InnerJoin {
            condition,
            left,
            right,
        } => Some(build_snapshot_inline_join_from(
            "JOIN", condition, left, right,
        )),
        OpTree::LeftJoin {
            condition,
            left,
            right,
        } => Some(build_snapshot_inline_join_from(
            "LEFT JOIN",
            condition,
            left,
            right,
        )),
        OpTree::FullJoin {
            condition,
            left,
            right,
        } => Some(build_snapshot_inline_join_from(
            "FULL JOIN",
            condition,
            left,
            right,
        )),
        // No recursion through Filter (would lose the predicate) or other wrappers.
        _ => None,
    }
}

/// Build a snapshot subquery for a join node.
///
/// Produces a parenthesized SELECT with disambiguated column names:
/// ```sql
/// (SELECT l."id" AS "l__id", ..., r."id" AS "r__id", ...
///  FROM left_snap l JOIN right_snap r ON condition)
/// ```
fn build_join_snapshot(join_type: &str, condition: &Expr, left: &OpTree, right: &OpTree) -> String {
    let left_snap = build_snapshot_sql(left);
    let right_snap = build_snapshot_sql(right);
    let left_alias = left.alias();
    let right_alias = right.alias();

    // Use snapshot_output_columns instead of output_columns to get the
    // correct disambiguated names that nested join snapshots produce.
    // For Scan children, these are the same as output_columns().
    // For join children, output_columns() returns raw names (c_custkey)
    // but the snapshot subquery aliases them as customer__c_custkey.
    let left_cols = snapshot_output_columns(left);
    let right_cols = snapshot_output_columns(right);

    let mut select_parts = Vec::new();
    for c in &left_cols {
        select_parts.push(format!(
            "{}.{} AS {}",
            quote_ident(left_alias),
            quote_ident(c),
            quote_ident(&format!("{left_alias}__{c}"))
        ));
    }
    for c in &right_cols {
        select_parts.push(format!(
            "{}.{} AS {}",
            quote_ident(right_alias),
            quote_ident(c),
            quote_ident(&format!("{right_alias}__{c}"))
        ));
    }

    // Rewrite condition for snapshot: use child aliases directly
    let cond_sql = rewrite_join_condition(condition, left, left_alias, right, right_alias);

    format!(
        "(SELECT {} FROM {} {} {} {} {} ON {})",
        select_parts.join(", "),
        left_snap,
        quote_ident(left_alias),
        join_type,
        right_snap,
        quote_ident(right_alias),
        cond_sql
    )
}

// ── DI-2: NOT EXISTS anti-join for pre-change snapshot ──────────────────

/// Build the pk_hash expression for a Scan node matching the CDC trigger's
/// hash computation.
///
/// For single- and multi-column keys, emits the typed V2 `SCAN_KEY` identity.
/// For keyless tables: uses all columns as hash input.
fn build_pk_hash_expr(
    alias: &str,
    columns: &[crate::dvm::parser::Column],
    pk_columns: &[String],
) -> String {
    let hash_cols: Vec<&str> = if pk_columns.is_empty() {
        columns.iter().map(|c| c.name.as_str()).collect()
    } else {
        pk_columns.iter().map(|s| s.as_str()).collect()
    };
    let hash_args: Vec<String> = hash_cols
        .iter()
        .map(|c| format!("{}.{}", quote_ident(alias), quote_ident(c)))
        .collect();
    crate::hash::build_row_identity_expr(
        if pk_columns.is_empty() {
            "KEYLESS_ROW"
        } else {
            "SCAN_KEY"
        },
        &hash_args,
    )
}

/// DI-2: Build a pre-change snapshot for any non-join child operator.
///
/// For `Scan` nodes, generates a NOT EXISTS anti-join against the delta
/// CTE's `__pgt_row_id` (pk_hash match), which replaces the expensive
/// EXCEPT ALL that required sorting/hashing the full base table.
///
/// When a Scan's `table_oid` is in `fallback_oids`, the NOT EXISTS
/// optimisation is bypassed in favour of EXCEPT ALL. This per-leaf
/// conditional fallback (DI-2) is activated when the delta exceeds
/// `max_delta_fraction` for that specific source table, making the
/// hash-based EXCEPT ALL more efficient than indexed NOT EXISTS.
///
/// For non-`Scan` children (Filter, Subquery, Aggregate, etc.), falls
/// back to the traditional EXCEPT ALL approach.
pub fn build_leaf_snapshot_sql(
    op: &OpTree,
    delta_cte: &str,
    data_cols: &[String],
    fallback_oids: &HashSet<u32>,
    st_source_pgt_ids: &HashMap<u32, i64>,
) -> String {
    let col_list: String = data_cols
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");

    match op {
        OpTree::Scan {
            schema,
            table_name,
            alias,
            columns,
            pk_columns,
            table_oid,
        } => {
            // DI-2 per-leaf fallback: when this source's delta fraction
            // exceeds max_delta_fraction, emit EXCEPT ALL instead of the
            // index-based NOT EXISTS anti-join.
            if fallback_oids.contains(table_oid) {
                let table = build_snapshot_sql(op);
                format!(
                    "(SELECT {col_list} FROM {table} {alias_q} \
                     EXCEPT ALL \
                     SELECT {col_list} FROM {delta_cte} WHERE __pgt_action = 'I' \
                     UNION ALL \
                     SELECT {col_list} FROM {delta_cte} WHERE __pgt_action = 'D')",
                    alias_q = quote_ident(alias),
                )
            } else {
                let pk_hash_expr = if st_source_pgt_ids.contains_key(table_oid) {
                    format!("{}.{}", quote_ident(alias), quote_ident("__pgt_row_id"))
                } else {
                    build_pk_hash_expr(alias, columns, pk_columns)
                };
                format!(
                    "(SELECT {col_list} FROM \"{schema}\".\"{table_name}\" {alias_q} \
                     WHERE NOT EXISTS (\
                       SELECT 1 FROM {delta_cte} __pgt_d \
                       WHERE pgtrickle.row_probe_v1(__pgt_d.__pgt_row_id) = pgtrickle.row_probe_v1({pk_hash_expr}) \
                         AND __pgt_d.__pgt_row_id = {pk_hash_expr}\
                     ) \
                     UNION ALL \
                     SELECT {col_list} FROM {delta_cte} WHERE __pgt_action = 'D')",
                    schema = schema.replace('"', "\"\""),
                    table_name = table_name.replace('"', "\"\""),
                    alias_q = quote_ident(alias),
                )
            }
        }
        _ => {
            // Fallback: EXCEPT ALL for non-Scan children (Filter, Subquery, etc.)
            let table = build_snapshot_sql(op);
            let alias = op.alias();
            format!(
                "(SELECT {col_list} FROM {table} {alias_q} \
                 EXCEPT ALL \
                 SELECT {col_list} FROM {delta_cte} WHERE __pgt_action = 'I' \
                 UNION ALL \
                 SELECT {col_list} FROM {delta_cte} WHERE __pgt_action = 'D')",
                alias_q = quote_ident(alias),
            )
        }
    }
}

// ── EC01B-1: Per-leaf CTE-based pre-change snapshot ─────────────────────

/// Build a SQL expression for the pre-change (L₀/R₀) snapshot of an
/// operator subtree using per-leaf CTE-based snapshots.
///
/// Instead of the expensive approach of materializing the entire join
/// result and applying `EXCEPT ALL` against the combined join delta, this
/// function decomposes the snapshot into per-leaf pre-change states that
/// are individually cheap to compute, then re-joins them.
///
/// **For Scan leaves**: uses NOT EXISTS anti-join (DI-2) for fast pk-hash
///   matching against the delta CTE.
///
/// **For join nodes**: recursively builds pre-change children and joins
///   them with the original condition and column disambiguation.
///
/// **Semantic equivalence**: For INNER/LEFT/FULL JOINs, the join of
/// per-leaf pre-change states is identical to the pre-change state of
/// the join. This avoids materializing the full join result (which can
/// spill GBs of temp files for deep join trees at any scale factor).
///
/// `scan_delta_ctes` maps each Scan alias to its delta CTE name, as
/// populated by `diff_scan` during the diff traversal.
///
/// DI-2: For Scan leaves, uses NOT EXISTS anti-join against the delta
/// CTE's `__pgt_row_id` (pk_hash) instead of the expensive EXCEPT ALL.
/// When a Scan's `table_oid` is in `fallback_oids`, EXCEPT ALL is used
/// instead (per-leaf conditional fallback).
pub fn build_pre_change_snapshot_sql(
    op: &OpTree,
    scan_delta_ctes: &HashMap<String, String>,
    fallback_oids: &HashSet<u32>,
    st_source_pgt_ids: &HashMap<u32, i64>,
) -> String {
    match op {
        OpTree::Scan { alias, columns, .. } => {
            if let Some(delta_cte) = scan_delta_ctes.get(alias.as_str()) {
                // DI-2: Delegate to build_leaf_snapshot_sql which uses
                // NOT EXISTS for Scan nodes.
                let data_cols: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
                build_leaf_snapshot_sql(op, delta_cte, &data_cols, fallback_oids, st_source_pgt_ids)
            } else {
                // No delta for this scan — fall back to current state
                build_snapshot_sql(op)
            }
        }
        OpTree::InnerJoin {
            condition,
            left,
            right,
        } => build_pre_change_join_snapshot(
            "JOIN",
            condition,
            left,
            right,
            scan_delta_ctes,
            fallback_oids,
            st_source_pgt_ids,
        ),
        OpTree::LeftJoin {
            condition,
            left,
            right,
        } => build_pre_change_join_snapshot(
            "LEFT JOIN",
            condition,
            left,
            right,
            scan_delta_ctes,
            fallback_oids,
            st_source_pgt_ids,
        ),
        OpTree::FullJoin {
            condition,
            left,
            right,
        } => build_pre_change_join_snapshot(
            "FULL JOIN",
            condition,
            left,
            right,
            scan_delta_ctes,
            fallback_oids,
            st_source_pgt_ids,
        ),
        OpTree::Filter { child, predicate } => {
            let child_snap = build_pre_change_snapshot_sql(
                child,
                scan_delta_ctes,
                fallback_oids,
                st_source_pgt_ids,
            );
            if matches!(child.as_ref(), OpTree::Scan { .. }) {
                let alias = child.alias();
                format!(
                    "(SELECT * FROM {} {} WHERE {})",
                    child_snap,
                    quote_ident(alias),
                    predicate.to_sql()
                )
            } else {
                child_snap
            }
        }
        OpTree::Project {
            expressions,
            aliases,
            child,
        } => {
            // When the child is a join, inline its FROM clause to preserve the
            // original table aliases (e.g., "a", "b") so that project expressions
            // like COALESCE(a.k, b.k) can reference them directly. Without this,
            // wrapping the join snapshot as a subquery (e.g., aliased "full_join")
            // hides the inner aliases, causing "missing FROM-clause entry" errors.
            let selects: Vec<String> = expressions
                .iter()
                .zip(aliases.iter())
                .map(|(expr, alias)| {
                    let expr_sql = expr.to_sql();
                    let alias_ident = quote_ident(alias);
                    if expr_sql == *alias {
                        alias_ident
                    } else {
                        format!("{expr_sql} AS {alias_ident}")
                    }
                })
                .collect();
            if let Some(from_clause) = build_pre_change_inline_from_for_join(
                child,
                scan_delta_ctes,
                fallback_oids,
                st_source_pgt_ids,
            ) {
                format!("(SELECT {} FROM {})", selects.join(", "), from_clause)
            } else {
                let inner = build_pre_change_snapshot_sql(
                    child,
                    scan_delta_ctes,
                    fallback_oids,
                    st_source_pgt_ids,
                );
                let child_alias = child.alias();
                format!(
                    "(SELECT {} FROM {} {})",
                    selects.join(", "),
                    inner,
                    quote_ident(child_alias),
                )
            }
        }
        OpTree::Subquery {
            column_aliases,
            child,
            ..
        } => {
            if column_aliases.is_empty() {
                build_pre_change_snapshot_sql(
                    child,
                    scan_delta_ctes,
                    fallback_oids,
                    st_source_pgt_ids,
                )
            } else {
                // Mirror the ordinal-renaming logic from build_snapshot_sql:
                // wrap the child in a SELECT that renames columns positionally
                // to match the subquery's column aliases.
                let inner = build_pre_change_snapshot_sql(
                    child,
                    scan_delta_ctes,
                    fallback_oids,
                    st_source_pgt_ids,
                );
                let child_alias = child.alias();
                let child_cols = child.output_columns();
                let inner_selects: Vec<String> = child_cols
                    .iter()
                    .enumerate()
                    .map(|(i, c)| format!("{} AS col{}", quote_ident(c), i + 1))
                    .collect();
                let selects: Vec<String> = column_aliases
                    .iter()
                    .enumerate()
                    .map(|(i, alias)| format!("__sub.col{} AS {}", i + 1, quote_ident(alias)))
                    .collect();
                format!(
                    "(SELECT {} FROM (SELECT {} FROM {} {}) __sub)",
                    selects.join(", "),
                    inner_selects.join(", "),
                    inner,
                    quote_ident(child_alias),
                )
            }
        }
        OpTree::Aggregate {
            group_by,
            aggregates,
            child,
        } => {
            let inner = build_pre_change_snapshot_sql(
                child,
                scan_delta_ctes,
                fallback_oids,
                st_source_pgt_ids,
            );
            let child_alias = child.alias();
            let mut selects = Vec::new();
            for expr in group_by {
                selects.push(expr.to_sql());
            }
            for agg in aggregates {
                selects.push(format!(
                    "{} AS {}",
                    agg_to_rescan_sql(agg),
                    quote_ident(&agg.alias),
                ));
            }
            let group_by_sql = if group_by.is_empty() {
                String::new()
            } else {
                format!(
                    " GROUP BY {}",
                    group_by
                        .iter()
                        .map(Expr::to_sql)
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            };
            format!(
                "(SELECT {} FROM {} {}{})",
                selects.join(", "),
                inner,
                quote_ident(child_alias),
                group_by_sql,
            )
        }
        OpTree::CteScan {
            alias,
            columns,
            cte_def_aliases,
            column_aliases,
            body,
            ..
        } => {
            let Some(body) = body else {
                return build_snapshot_sql(op);
            };

            // CteScan keeps a clone of its body from before the registry's
            // column-pruning pass. Prune that clone before building its
            // snapshot so its leaf delta CTEs expose the same columns.
            let mut body = body.as_ref().clone();
            body.prune_scan_columns();
            let body_snap = build_pre_change_snapshot_sql(
                &body,
                scan_delta_ctes,
                fallback_oids,
                st_source_pgt_ids,
            );
            let body_cols = body.output_columns();
            let effective_cols: Vec<&str> = if !column_aliases.is_empty() {
                column_aliases.iter().map(String::as_str).collect()
            } else if !cte_def_aliases.is_empty() {
                cte_def_aliases.iter().map(String::as_str).collect()
            } else {
                columns.iter().map(String::as_str).collect()
            };

            if body_cols
                .iter()
                .zip(effective_cols.iter())
                .any(|(body_col, effective_col)| body_col != effective_col)
                || body_cols.len() != effective_cols.len()
            {
                let selects = body_cols
                    .iter()
                    .zip(effective_cols.iter())
                    .map(|(src, dst)| {
                        if src == dst {
                            quote_ident(src)
                        } else {
                            format!("{} AS {}", quote_ident(src), quote_ident(dst))
                        }
                    })
                    .collect::<Vec<_>>();
                format!(
                    "(SELECT {} FROM {} {})",
                    selects.join(", "),
                    body_snap,
                    quote_ident(alias),
                )
            } else {
                body_snap
            }
        }
        // Unsupported nodes still use the current snapshot. SnapshotPlan
        // keeps them off the per-leaf path unless exact reconstruction is
        // added here.
        _ => build_snapshot_sql(op),
    }
}

/// Build an inline FROM clause for a join node using `build_pre_change_snapshot_sql` for each child.
///
/// Mirrors [`build_snapshot_inline_join_from`] but uses the pre-change snapshots so that
/// original table aliases remain accessible when a `Project` node sits directly over the join.
fn build_pre_change_inline_join_from(
    join_type: &str,
    condition: &Expr,
    left: &OpTree,
    right: &OpTree,
    scan_delta_ctes: &HashMap<String, String>,
    fallback_oids: &HashSet<u32>,
    st_source_pgt_ids: &HashMap<u32, i64>,
) -> String {
    let left_alias = left.alias();
    let right_alias = right.alias();
    let left_snap =
        build_pre_change_snapshot_sql(left, scan_delta_ctes, fallback_oids, st_source_pgt_ids);
    let right_snap =
        build_pre_change_snapshot_sql(right, scan_delta_ctes, fallback_oids, st_source_pgt_ids);
    let cond_sql = rewrite_join_condition(condition, left, left_alias, right, right_alias);
    format!(
        "{} {} {} {} {} ON {}",
        left_snap,
        quote_ident(left_alias),
        join_type,
        right_snap,
        quote_ident(right_alias),
        cond_sql,
    )
}

/// Pre-change variant of [`build_snapshot_inline_from_for_join`].
fn build_pre_change_inline_from_for_join(
    op: &OpTree,
    scan_delta_ctes: &HashMap<String, String>,
    fallback_oids: &HashSet<u32>,
    st_source_pgt_ids: &HashMap<u32, i64>,
) -> Option<String> {
    match op {
        OpTree::InnerJoin {
            condition,
            left,
            right,
        } => Some(build_pre_change_inline_join_from(
            "JOIN",
            condition,
            left,
            right,
            scan_delta_ctes,
            fallback_oids,
            st_source_pgt_ids,
        )),
        OpTree::LeftJoin {
            condition,
            left,
            right,
        } => Some(build_pre_change_inline_join_from(
            "LEFT JOIN",
            condition,
            left,
            right,
            scan_delta_ctes,
            fallback_oids,
            st_source_pgt_ids,
        )),
        OpTree::FullJoin {
            condition,
            left,
            right,
        } => Some(build_pre_change_inline_join_from(
            "FULL JOIN",
            condition,
            left,
            right,
            scan_delta_ctes,
            fallback_oids,
            st_source_pgt_ids,
        )),
        _ => None,
    }
}

/// Build a pre-change join snapshot with per-leaf CTE-based sub-snapshots.
///
/// Analogous to [`build_join_snapshot`] but recursively applies per-leaf
/// pre-change snapshots instead of current-state snapshots.
fn build_pre_change_join_snapshot(
    join_type: &str,
    condition: &Expr,
    left: &OpTree,
    right: &OpTree,
    scan_delta_ctes: &HashMap<String, String>,
    fallback_oids: &HashSet<u32>,
    st_source_pgt_ids: &HashMap<u32, i64>,
) -> String {
    let left_snap =
        build_pre_change_snapshot_sql(left, scan_delta_ctes, fallback_oids, st_source_pgt_ids);
    let right_snap =
        build_pre_change_snapshot_sql(right, scan_delta_ctes, fallback_oids, st_source_pgt_ids);
    let left_alias = left.alias();
    let right_alias = right.alias();

    let left_cols = snapshot_output_columns(left);
    let right_cols = snapshot_output_columns(right);

    let mut select_parts = Vec::new();
    for c in &left_cols {
        select_parts.push(format!(
            "{}.{} AS {}",
            quote_ident(left_alias),
            quote_ident(c),
            quote_ident(&format!("{left_alias}__{c}"))
        ));
    }
    for c in &right_cols {
        select_parts.push(format!(
            "{}.{} AS {}",
            quote_ident(right_alias),
            quote_ident(c),
            quote_ident(&format!("{right_alias}__{c}"))
        ));
    }

    let cond_sql = rewrite_join_condition(condition, left, left_alias, right, right_alias);

    format!(
        "(SELECT {} FROM {} {} {} {} {} ON {})",
        select_parts.join(", "),
        left_snap,
        quote_ident(left_alias),
        join_type,
        right_snap,
        quote_ident(right_alias),
        cond_sql
    )
}

/// Return the column names as they appear in a snapshot subquery built by
/// [`build_snapshot_sql`].
///
/// For `Scan` nodes, snapshot columns are the same as `output_columns()`.
/// For join nodes, the snapshot subquery disambiguates names with the child
/// alias prefix (e.g., `customer__c_custkey`), so the returned names must
/// match that format. Using `output_columns()` directly for joins would
/// return the raw un-prefixed names, causing "column X does not exist"
/// errors when a higher-level join references the inner snapshot.
fn snapshot_output_columns(op: &OpTree) -> Vec<String> {
    match op {
        OpTree::Scan { .. } => op.output_columns(),
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. } => {
            let left_prefix = left.alias();
            let right_prefix = right.alias();
            let mut cols = Vec::new();
            for c in snapshot_output_columns(left) {
                cols.push(format!("{left_prefix}__{c}"));
            }
            for c in snapshot_output_columns(right) {
                cols.push(format!("{right_prefix}__{c}"));
            }
            cols
        }
        OpTree::Filter { child, .. } => snapshot_output_columns(child),
        OpTree::Project { aliases, .. } => {
            // Project snapshot uses the project's alias names
            aliases.clone()
        }
        OpTree::Subquery {
            column_aliases,
            child,
            ..
        } => {
            if column_aliases.is_empty() {
                snapshot_output_columns(child)
            } else {
                column_aliases.clone()
            }
        }
        _ => op.output_columns(),
    }
}

// ── Condition rewriting ─────────────────────────────────────────────────

/// Rewrite a join condition for use in a delta or snapshot query.
///
/// Replaces original table alias references with the provided new aliases,
/// handling nested joins by disambiguating column names with the original
/// table alias prefix when the source table is inside a nested join child.
///
/// For a simple case (Scan child), `o.cust_id` → `dl."cust_id"`.
/// For a nested case (Join child), `o.cust_id` → `dl."o__cust_id"`.
pub fn rewrite_join_condition(
    condition: &Expr,
    left: &OpTree,
    new_left: &str,
    right: &OpTree,
    new_right: &str,
) -> String {
    rewrite_expr_for_join(condition, left, new_left, right, new_right).to_sql()
}

/// Recursively rewrite an expression for join delta/snapshot usage.
fn rewrite_expr_for_join(
    expr: &Expr,
    left: &OpTree,
    new_left: &str,
    right: &OpTree,
    new_right: &str,
) -> Expr {
    match expr {
        Expr::ColumnRef {
            table_alias: Some(alias),
            column_name,
        } => {
            // Prefer a direct child alias over a same-named alias nested
            // inside the other child.  Nested joins commonly reuse `l`/`r`
            // aliases, but SQL scope gives the outer child alias precedence.
            if has_source_alias(right, alias) && is_simple_source(right, alias) {
                Expr::ColumnRef {
                    table_alias: Some(new_right.to_string()),
                    column_name: column_name.clone(),
                }
            } else if has_source_alias(left, alias) && is_simple_source(left, alias) {
                Expr::ColumnRef {
                    table_alias: Some(new_left.to_string()),
                    column_name: column_name.clone(),
                }
            } else if has_source_alias(left, alias) {
                if let Some(disambiguated) = resolve_disambiguated_column(left, alias, column_name)
                {
                    // Deep disambiguation: trace through nested joins
                    Expr::ColumnRef {
                        table_alias: Some(new_left.to_string()),
                        column_name: disambiguated,
                    }
                } else {
                    // Fallback: single-level disambiguation
                    Expr::ColumnRef {
                        table_alias: Some(new_left.to_string()),
                        column_name: format!("{alias}__{column_name}"),
                    }
                }
            } else if has_source_alias(right, alias) {
                if let Some(disambiguated) = resolve_disambiguated_column(right, alias, column_name)
                {
                    Expr::ColumnRef {
                        table_alias: Some(new_right.to_string()),
                        column_name: disambiguated,
                    }
                } else {
                    Expr::ColumnRef {
                        table_alias: Some(new_right.to_string()),
                        column_name: format!("{alias}__{column_name}"),
                    }
                }
            } else {
                // Alias not found in either child — pass through unchanged
                expr.clone()
            }
        }
        Expr::ColumnRef {
            table_alias: None,
            column_name,
        } => {
            // Unqualified column ref — resolve against left/right children
            // to find the source Scan and disambiguate correctly.
            if let Some(source_alias) = find_column_source(left, column_name) {
                if is_simple_source(left, &source_alias) {
                    Expr::ColumnRef {
                        table_alias: Some(new_left.to_string()),
                        column_name: column_name.clone(),
                    }
                } else if let Some(disambiguated) =
                    resolve_disambiguated_column(left, &source_alias, column_name)
                {
                    Expr::ColumnRef {
                        table_alias: Some(new_left.to_string()),
                        column_name: disambiguated,
                    }
                } else {
                    expr.clone()
                }
            } else if let Some(source_alias) = find_column_source(right, column_name) {
                if is_simple_source(right, &source_alias) {
                    Expr::ColumnRef {
                        table_alias: Some(new_right.to_string()),
                        column_name: column_name.clone(),
                    }
                } else if let Some(disambiguated) =
                    resolve_disambiguated_column(right, &source_alias, column_name)
                {
                    Expr::ColumnRef {
                        table_alias: Some(new_right.to_string()),
                        column_name: disambiguated,
                    }
                } else {
                    expr.clone()
                }
            } else {
                // Column not found in either child — pass through
                expr.clone()
            }
        }
        Expr::BinaryOp {
            op,
            left: l,
            right: r,
        } => Expr::BinaryOp {
            op: op.clone(),
            left: Box::new(rewrite_expr_for_join(l, left, new_left, right, new_right)),
            right: Box::new(rewrite_expr_for_join(r, left, new_left, right, new_right)),
        },
        Expr::FuncCall { func_name, args } => Expr::FuncCall {
            func_name: func_name.clone(),
            args: args
                .iter()
                .map(|a| rewrite_expr_for_join(a, left, new_left, right, new_right))
                .collect(),
        },
        Expr::Star { table_alias } => {
            // Rewrite star expressions: table.* → new_alias.*
            if let Some(alias) = table_alias {
                if has_source_alias(left, alias) {
                    Expr::Star {
                        table_alias: Some(new_left.to_string()),
                    }
                } else if has_source_alias(right, alias) {
                    Expr::Star {
                        table_alias: Some(new_right.to_string()),
                    }
                } else {
                    expr.clone()
                }
            } else {
                expr.clone()
            }
        }
        // Literals and Raw SQL without column references — pass through
        Expr::Literal(_) => expr.clone(),
        Expr::Raw(sql) => {
            // Best-effort: rewrite qualified column references in raw SQL
            // text. For each source alias in left/right children, replace
            // `alias."col"` and `alias.col` patterns with the new alias.
            let mut result = sql.clone();
            let all_aliases = collect_source_aliases(left)
                .into_iter()
                .chain(collect_source_aliases(right));
            for alias in all_aliases {
                let (new_alias, is_simple) = if has_source_alias(left, &alias) {
                    (new_left, is_simple_source(left, &alias))
                } else {
                    (new_right, is_simple_source(right, &alias))
                };

                if is_simple {
                    // Simple: replace alias.col → new_alias.col
                    // Match both alias."col" and alias.col patterns.
                    // Also handle quoted form: "alias"."col" → "new_alias"."col"
                    // (Expr::ColumnRef::to_sql() emits double-quoted identifiers)
                    let quoted_pattern = format!("\"{}\".", alias.replace('"', "\"\""));
                    let quoted_replacement = format!("\"{}\".", new_alias.replace('"', "\"\""));
                    result = result.replace(&quoted_pattern, &quoted_replacement);
                    let pattern = format!("{}.", alias);
                    let replacement = format!("{}.", new_alias);
                    result = result.replace(&pattern, &replacement);
                } else {
                    // Nested: alias.col → new_alias."alias__col"
                    // This is harder in raw SQL — we do a conservative
                    // pattern replacement for alias."col" → new_alias."alias__col"
                    // and alias.col → new_alias."alias__col"
                    // Also handle quoted form "alias"."col"
                    let quoted_prefix = format!("\"{}\".", alias.replace('"', "\"\""));
                    if result.contains(&quoted_prefix) {
                        result = rewrite_raw_quoted_alias_refs(&result, &alias, new_alias);
                    }
                    let dot_prefix = format!("{}.", alias);
                    if result.contains(&dot_prefix) {
                        // Replace qualified references carefully
                        result = rewrite_raw_alias_refs(&result, &alias, new_alias);
                    }
                }
            }
            Expr::Raw(result)
        }
    }
}

/// Collect all source table aliases from an OpTree.
fn collect_source_aliases(op: &OpTree) -> Vec<String> {
    match op {
        OpTree::Scan { alias, .. } => vec![alias.clone()],
        OpTree::Subquery { alias, .. } => vec![alias.clone()],
        OpTree::CteScan { alias, .. } => vec![alias.clone()],
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. }
        | OpTree::SemiJoin { left, right, .. }
        | OpTree::AntiJoin { left, right, .. } => {
            let mut aliases = collect_source_aliases(left);
            aliases.extend(collect_source_aliases(right));
            aliases
        }
        OpTree::Intersect { left, right, .. } | OpTree::Except { left, right, .. } => {
            let mut aliases = collect_source_aliases(left);
            aliases.extend(collect_source_aliases(right));
            aliases
        }
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Aggregate { child, .. }
        | OpTree::Distinct { child, .. } => collect_source_aliases(child),
        OpTree::UnionAll { children } => children.iter().flat_map(collect_source_aliases).collect(),
        // Window, LateralFunction, LateralSubquery, RecursiveCte,
        // RecursiveSelfRef, etc. — these rarely appear as direct join
        // children, but return empty to be safe.
        _ => vec![],
    }
}

/// Rewrite `alias.col` and `alias."col"` patterns in raw SQL text
/// to `new_alias."alias__col"` for nested join disambiguation.
fn rewrite_raw_alias_refs(sql: &str, old_alias: &str, new_alias: &str) -> String {
    let prefix = format!("{}.", old_alias);
    let mut result = String::with_capacity(sql.len());
    let mut remaining = sql;

    while let Some(pos) = remaining.find(&prefix) {
        // Copy everything before the match
        result.push_str(&remaining[..pos]);
        remaining = &remaining[pos + prefix.len()..];

        // Extract the column name after the dot
        let col_name = if remaining.starts_with('"') {
            // Quoted identifier: alias."col_name"
            if let Some(end) = remaining[1..].find('"') {
                let name = &remaining[1..1 + end];
                remaining = &remaining[2 + end..];
                name.to_string()
            } else {
                // Unterminated quote — pass through as-is
                result.push_str(&prefix);
                continue;
            }
        } else {
            // Unquoted identifier: read until non-identifier char
            let end = remaining
                .find(|c: char| !c.is_alphanumeric() && c != '_')
                .unwrap_or(remaining.len());
            if end == 0 {
                // Nothing after the dot — pass through
                result.push_str(&prefix);
                continue;
            }
            let name = &remaining[..end];
            remaining = &remaining[end..];
            name.to_string()
        };

        // Emit the disambiguated reference: new_alias."old_alias__col"
        result.push_str(&format!(
            "{}.\"{}__{}\"",
            new_alias,
            old_alias.replace('"', "\"\""),
            col_name.replace('"', "\"\""),
        ));
    }

    // Append the rest
    result.push_str(remaining);
    result
}

/// Rewrite `"alias"."col"` patterns (double-quoted form from `Expr::to_sql()`)
/// to `new_alias."alias__col"` for nested join disambiguation.
fn rewrite_raw_quoted_alias_refs(sql: &str, bare_alias: &str, new_alias: &str) -> String {
    let prefix = format!("\"{}\".", bare_alias.replace('"', "\"\""));
    let mut result = String::with_capacity(sql.len());
    let mut remaining = sql;

    while let Some(pos) = remaining.find(&prefix) {
        result.push_str(&remaining[..pos]);
        remaining = &remaining[pos + prefix.len()..];

        // After "alias"., expect a quoted column name "col"
        let col_name = if remaining.starts_with('"') {
            if let Some(end) = remaining[1..].find('"') {
                let name = &remaining[1..1 + end];
                remaining = &remaining[2 + end..];
                name.to_string()
            } else {
                result.push_str(&prefix);
                continue;
            }
        } else {
            // Unquoted identifier after quoted alias
            let end = remaining
                .find(|c: char| !c.is_alphanumeric() && c != '_')
                .unwrap_or(remaining.len());
            if end == 0 {
                result.push_str(&prefix);
                continue;
            }
            let name = &remaining[..end];
            remaining = &remaining[end..];
            name.to_string()
        };

        // Emit: new_alias."bare_alias__col"
        result.push_str(&format!(
            "{}.\"{}__{}\"",
            new_alias,
            bare_alias.replace('"', "\"\""),
            col_name.replace('"', "\"\""),
        ));
    }

    result.push_str(remaining);
    result
}

/// Check if an OpTree contains a source table with the given alias.
///
/// Descends into join children, filters, projects, and subqueries to
/// find whether a specific table alias is accessible from this subtree.
pub fn has_source_alias(op: &OpTree, alias: &str) -> bool {
    match op {
        OpTree::Scan { alias: a, .. } => a == alias,
        OpTree::CteScan { alias: a, .. } => a == alias,
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. }
        | OpTree::SemiJoin { left, right, .. }
        | OpTree::AntiJoin { left, right, .. } => {
            has_source_alias(left, alias) || has_source_alias(right, alias)
        }
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Aggregate { child, .. }
        | OpTree::Distinct { child, .. } => has_source_alias(child, alias),
        OpTree::Subquery {
            alias: sub_alias,
            child,
            ..
        } => {
            // A Subquery introduces a named scope (e.g., `(SELECT ...) AS revenue0`).
            // Its own alias is a valid source alias for column references.
            sub_alias == alias || has_source_alias(child, alias)
        }
        _ => false,
    }
}

/// Find which source Scan a bare (unqualified) column name belongs to.
///
/// Searches the OpTree for Scan nodes whose columns include `column_name`.
/// Returns the alias of the first matching Scan, or `None` if no Scan has
/// that column.
fn find_column_source(op: &OpTree, column_name: &str) -> Option<String> {
    match op {
        OpTree::Scan { alias, columns, .. } => {
            if columns.iter().any(|c| c.name == column_name) {
                Some(alias.clone())
            } else {
                None
            }
        }
        OpTree::CteScan {
            alias,
            columns,
            cte_def_aliases,
            column_aliases,
            ..
        } => {
            let visible_cols = if !column_aliases.is_empty() {
                column_aliases
            } else if !cte_def_aliases.is_empty() {
                cte_def_aliases
            } else {
                columns
            };
            if visible_cols.iter().any(|c| c == column_name) {
                Some(alias.clone())
            } else {
                None
            }
        }
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. }
        | OpTree::SemiJoin { left, right, .. }
        | OpTree::AntiJoin { left, right, .. } => {
            find_column_source(left, column_name).or_else(|| find_column_source(right, column_name))
        }
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Subquery { child, .. }
        | OpTree::Aggregate { child, .. }
        | OpTree::Distinct { child, .. } => find_column_source(child, column_name),
        _ => None,
    }
}

/// Resolve a table-qualified column reference to its fully disambiguated
/// name in the context of a specific OpTree.
///
/// For deeply nested joins (e.g., `((supplier ⋈ l1) ⋈ orders) ⋈ nation`),
/// a column like `l1.l_orderkey` is disambiguated through multiple levels:
/// `l1__l_orderkey` → `join__l1__l_orderkey` → `join__join__l1__l_orderkey`.
///
/// This function recursively traces the nesting to produce the correct
/// fully-qualified column name.
fn resolve_disambiguated_column(
    op: &OpTree,
    table_alias: &str,
    column_name: &str,
) -> Option<String> {
    match op {
        OpTree::Scan { alias, .. } if alias == table_alias => {
            // Found the target scan — return alias__column_name
            Some(format!("{alias}__{column_name}"))
        }
        OpTree::CteScan { alias, .. } if alias == table_alias => Some(column_name.to_string()),
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. } => {
            if has_source_alias(left, table_alias) {
                if is_simple_source(left, table_alias) {
                    // The table IS the left child scan — single level
                    Some(format!("{table_alias}__{column_name}"))
                } else {
                    // Nested — recurse and add the left child's alias prefix
                    let inner = resolve_disambiguated_column(left, table_alias, column_name)?;
                    Some(format!("{}__{inner}", left.alias()))
                }
            } else if has_source_alias(right, table_alias) {
                if is_simple_source(right, table_alias) {
                    Some(format!("{table_alias}__{column_name}"))
                } else {
                    let inner = resolve_disambiguated_column(right, table_alias, column_name)?;
                    Some(format!("{}__{inner}", right.alias()))
                }
            } else {
                None
            }
        }
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Subquery { child, .. } => {
            resolve_disambiguated_column(child, table_alias, column_name)
        }
        // SemiJoin/AntiJoin output only left-side columns. The right
        // side is used for the EXISTS check and doesn't contribute to
        // the output. Recurse into the left child only.
        OpTree::SemiJoin { left, .. } | OpTree::AntiJoin { left, .. } => {
            resolve_disambiguated_column(left, table_alias, column_name)
        }
        _ => None,
    }
}

/// Check if a table alias is directly accessible (no column disambiguation needed).
///
/// Returns `true` if the alias corresponds to a `Scan` that IS the node
/// or is wrapped only by transparent operators (Filter, Project, Subquery).
/// Returns `false` if the alias is inside a nested join, meaning columns
/// are prefixed with the original table alias.
pub fn is_simple_source(op: &OpTree, alias: &str) -> bool {
    match op {
        OpTree::Scan { alias: a, .. } => a == alias,
        OpTree::CteScan { alias: a, .. } => a == alias,
        OpTree::Filter { child, .. } | OpTree::Project { child, .. } => {
            is_simple_source(child, alias)
        }
        OpTree::Subquery {
            alias: sub_alias,
            child,
            ..
        } => {
            // If the subquery's own alias matches, this IS the atomic source.
            // Columns are directly accessible without disambiguation (e.g.,
            // a derived table `(SELECT ...) AS revenue0` — columns are
            // accessed as `revenue0.col`, not `revenue0__col`).
            if sub_alias == alias {
                true
            } else {
                is_simple_source(child, alias)
            }
        }
        // SemiJoin/AntiJoin pass through the left child's columns
        OpTree::SemiJoin { left, .. } | OpTree::AntiJoin { left, .. } => {
            is_simple_source(left, alias)
        }
        // For joins, the alias is inside the join — needs disambiguation
        _ => false,
    }
}

/// Check if a child is a "simple source" (Scan or transparent wrapper over Scan).
///
/// Used to determine if semi-join optimization can be applied — the
/// optimization requires filtering a plain table, not a complex subquery.
pub fn is_simple_child(op: &OpTree) -> bool {
    match op {
        OpTree::Scan { .. } => true,
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Subquery { child, .. } => is_simple_child(child),
        _ => false,
    }
}

/// Build per-column `alias."col"::TEXT` expressions for the base-table
/// side of a join, suitable for inclusion in a flat
/// `pg_trickle_hash_multi(ARRAY[...])` call.
///
/// For `Scan` nodes this uses the PK (non-nullable) columns; for
/// non-Scan children it falls back to `row_to_json(alias)::text`.
pub fn build_base_table_key_exprs(op: &OpTree, alias: &str) -> Vec<String> {
    match op {
        OpTree::Scan { columns, .. } => {
            let non_nullable: Vec<&str> = columns
                .iter()
                .filter(|c| !c.is_nullable)
                .map(|c| c.name.as_str())
                .collect();

            let key_cols: Vec<&str> = if non_nullable.is_empty() {
                columns.iter().map(|c| c.name.as_str()).collect()
            } else {
                non_nullable
            };

            key_cols
                .iter()
                .map(|c| format!("{alias}.{}", quote_ident(c)))
                .collect()
        }
        _ => {
            // Non-Scan child: fall back to row_to_json
            vec![format!("row_to_json({alias})::text")]
        }
    }
}

/// Extract equi-join key pairs from a condition, with column references
/// rewritten for the given aliases using the same disambiguation logic
/// as [`rewrite_join_condition`].
///
/// Returns `(left_col_sql, right_col_sql)` pairs suitable for building
/// `WHERE left_col IN (SELECT DISTINCT right_col FROM delta)` filters
/// that pre-filter a snapshot scan to only rows matching the delta.
///
/// Falls back gracefully: if the condition is too complex (OR, functions,
/// non-equality operators), returns an empty vec and the optimization is
/// skipped.
pub fn extract_equijoin_keys_aliased(
    condition: &Expr,
    left: &OpTree,
    left_alias: &str,
    right: &OpTree,
    right_alias: &str,
) -> Vec<(String, String)> {
    let mut keys = Vec::new();
    collect_aliased_keys(condition, left, left_alias, right, right_alias, &mut keys);
    keys
}

/// Recursively collect equi-join key pairs with alias rewriting.
fn collect_aliased_keys(
    expr: &Expr,
    left: &OpTree,
    left_alias: &str,
    right: &OpTree,
    right_alias: &str,
    keys: &mut Vec<(String, String)>,
) {
    match expr {
        Expr::BinaryOp {
            op,
            left: l_expr,
            right: r_expr,
        } if op == "=" => {
            let l_rewritten =
                rewrite_expr_for_join(l_expr, left, left_alias, right, right_alias).to_sql();
            let r_rewritten =
                rewrite_expr_for_join(r_expr, left, left_alias, right, right_alias).to_sql();
            keys.push((l_rewritten, r_rewritten));
        }
        Expr::BinaryOp {
            op,
            left: l_expr,
            right: r_expr,
        } if op.eq_ignore_ascii_case("AND") => {
            collect_aliased_keys(l_expr, left, left_alias, right, right_alias, keys);
            collect_aliased_keys(r_expr, left, left_alias, right, right_alias, keys);
        }
        _ => {
            // Non-equality / non-AND: skip.
        }
    }
}

/// Returns true if `op` is (or wraps) a join node, indicating a nested join
/// child for which the EXCEPT ALL approach may interact badly with
/// SemiJoin R_old (causing Q21-type regressions). Subquery/Aggregate
/// children are **not** considered join children — they can safely use the
/// pre-change snapshot.
///
/// Snapshot strategy selection is owned by [`crate::dvm::snapshot::SnapshotPlan`].
pub fn is_join_child(op: &OpTree) -> bool {
    match op {
        OpTree::InnerJoin { .. } | OpTree::LeftJoin { .. } | OpTree::FullJoin { .. } => true,
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Subquery { child, .. } => is_join_child(child),
        _ => false,
    }
}

/// Count the number of Scan (base table) nodes in a join subtree.
/// Used by nested-join correction terms and diagnostics.
pub fn join_scan_count(op: &OpTree) -> usize {
    match op {
        OpTree::Scan { .. } => 1,
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. } => join_scan_count(left) + join_scan_count(right),
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Subquery { child, .. } => join_scan_count(child),
        _ => 0, // Non-join nodes (Aggregate, SemiJoin, etc.) — stop counting
    }
}

/// Count the total number of Scan nodes in the operator tree, traversing
/// through ALL node types including Aggregate, Window, Distinct, etc.
/// Used by the DI-11 planner hints to detect deep-join queries.
pub fn total_scan_count(op: &OpTree) -> usize {
    match op {
        OpTree::Scan { .. } => 1,
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. } => total_scan_count(left) + total_scan_count(right),
        OpTree::SemiJoin { left, right, .. } | OpTree::AntiJoin { left, right, .. } => {
            total_scan_count(left) + total_scan_count(right)
        }
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Subquery { child, .. } => total_scan_count(child),
        OpTree::Aggregate { child, .. }
        | OpTree::Window { child, .. }
        | OpTree::Distinct { child, .. } => total_scan_count(child),
        OpTree::UnionAll { children, .. } => children.iter().map(total_scan_count).sum(),
        OpTree::CteScan { body, .. } => body.as_ref().map_or(0, |b| total_scan_count(b)),
        OpTree::RecursiveCte {
            base, recursive, ..
        } => total_scan_count(base) + total_scan_count(recursive),
        OpTree::RecursiveSelfRef { .. } => 0,
        // Intersect, Except, LateralFunction, Values, etc.
        _ => 0,
    }
}

/// EC-01 / EC-01b: Returns `true` when the operator tree contains any
/// join-shaped node (InnerJoin, LeftJoin, FullJoin, SemiJoin, AntiJoin)
/// at any depth.
///
/// This is the gating predicate for `cleanup_cross_cycle_phantoms`: only
/// join-bearing queries can leave stale `__pgt_row_id` values in the
/// stream table after a partial delta application, so non-join queries
/// can skip the reconciliation pass entirely.
///
/// Comma-joins (`FROM a, b WHERE a.x = b.y`) are normalised to `InnerJoin`
/// nodes by the parser, so this helper covers them too.
pub fn tree_contains_join(op: &OpTree) -> bool {
    match op {
        OpTree::InnerJoin { .. }
        | OpTree::LeftJoin { .. }
        | OpTree::FullJoin { .. }
        | OpTree::SemiJoin { .. }
        | OpTree::AntiJoin { .. } => true,
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Subquery { child, .. }
        | OpTree::Aggregate { child, .. }
        | OpTree::Window { child, .. }
        | OpTree::Distinct { child, .. } => tree_contains_join(child),
        OpTree::UnionAll { children, .. } => children.iter().any(tree_contains_join),
        OpTree::Intersect { left, right, .. } | OpTree::Except { left, right, .. } => {
            tree_contains_join(left) || tree_contains_join(right)
        }
        OpTree::CteScan { body, .. } => body.as_ref().is_some_and(|b| tree_contains_join(b)),
        OpTree::RecursiveCte {
            base, recursive, ..
        } => tree_contains_join(base) || tree_contains_join(recursive),
        OpTree::ScalarSubquery {
            subquery, child, ..
        } => tree_contains_join(subquery) || tree_contains_join(child),
        OpTree::LateralFunction { child, .. } | OpTree::LateralSubquery { child, .. } => {
            tree_contains_join(child)
        }
        OpTree::Scan { .. } | OpTree::RecursiveSelfRef { .. } => false,
        // Values and any future variant — conservatively no join.
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm::operators::test_helpers::*;
    use crate::dvm::snapshot::SnapshotPlan;

    fn use_pre_change_snapshot(child: &OpTree, inside_semijoin: bool, _threshold: usize) -> bool {
        SnapshotPlan::for_tree_in_context(child, inside_semijoin).uses_pre_change()
    }

    // ── build_snapshot_sql tests ────────────────────────────────

    #[test]
    fn test_snapshot_scan() {
        let node = scan(1, "orders", "public", "o", &["id", "cust_id"]);
        let snap = build_snapshot_sql(&node);
        assert_eq!(snap, "\"public\".\"orders\"");
    }

    #[test]
    fn test_snapshot_inner_join() {
        let left = scan(1, "orders", "public", "o", &["id", "cust_id"]);
        let right = scan(2, "customers", "public", "c", &["id", "name"]);
        let cond = eq_cond("o", "cust_id", "c", "id");
        let node = inner_join(cond, left, right);
        let snap = build_snapshot_sql(&node);

        // Should be a subquery with disambiguated column names
        assert!(snap.starts_with('('));
        assert!(snap.contains("\"o__id\""));
        assert!(snap.contains("\"o__cust_id\""));
        assert!(snap.contains("\"c__id\""));
        assert!(snap.contains("\"c__name\""));
        assert!(snap.contains("JOIN"));
    }

    #[test]
    fn test_snapshot_left_join() {
        let left = scan(1, "a", "public", "a", &["id"]);
        let right = scan(2, "b", "public", "b", &["id"]);
        let cond = eq_cond("a", "id", "b", "id");
        let node = left_join(cond, left, right);
        let snap = build_snapshot_sql(&node);

        assert!(snap.contains("LEFT JOIN"));
    }

    #[test]
    fn test_snapshot_nested_join() {
        // (orders o JOIN customers c) JOIN products p
        let o = scan(1, "orders", "public", "o", &["id", "cust_id", "prod_id"]);
        let c = scan(2, "customers", "public", "c", &["id", "name"]);
        let inner = inner_join(eq_cond("o", "cust_id", "c", "id"), o, c);
        let p = scan(3, "products", "public", "p", &["id", "price"]);
        let outer = inner_join(eq_cond("o", "prod_id", "p", "id"), inner, p);

        let snap = build_snapshot_sql(&outer);
        // Outer join should reference a subquery for the inner join
        assert!(snap.contains("\"join\""));
        assert!(snap.contains("\"p\""));
    }

    #[test]
    fn test_snapshot_filter_over_scan() {
        let child = scan(1, "t", "public", "t", &["id", "status"]);
        let node = filter(binop("=", qcolref("t", "status"), lit("'active'")), child);
        let snap = build_snapshot_sql(&node);
        assert!(snap.contains("SELECT *"));
        assert!(snap.contains("WHERE"));
    }

    // ── has_source_alias tests ──────────────────────────────────

    #[test]
    fn test_has_source_alias_scan() {
        let node = scan(1, "orders", "public", "o", &["id"]);
        assert!(has_source_alias(&node, "o"));
        assert!(!has_source_alias(&node, "x"));
    }

    #[test]
    fn test_has_source_alias_nested_join() {
        let o = scan(1, "orders", "public", "o", &["id"]);
        let c = scan(2, "customers", "public", "c", &["id"]);
        let node = inner_join(eq_cond("o", "id", "c", "id"), o, c);
        assert!(has_source_alias(&node, "o"));
        assert!(has_source_alias(&node, "c"));
        assert!(!has_source_alias(&node, "x"));
    }

    // ── is_simple_source tests ──────────────────────────────────

    #[test]
    fn test_is_simple_source_scan() {
        let node = scan(1, "t", "public", "t", &["id"]);
        assert!(is_simple_source(&node, "t"));
    }

    #[test]
    fn test_is_simple_source_filter_over_scan() {
        let node = filter(lit("TRUE"), scan(1, "t", "public", "t", &["id"]));
        assert!(is_simple_source(&node, "t"));
    }

    #[test]
    fn test_is_simple_source_nested_join() {
        let o = scan(1, "orders", "public", "o", &["id"]);
        let c = scan(2, "customers", "public", "c", &["id"]);
        let node = inner_join(eq_cond("o", "id", "c", "id"), o, c);
        // "o" is inside a join → not simple
        assert!(!is_simple_source(&node, "o"));
        assert!(!is_simple_source(&node, "c"));
    }

    // ── rewrite_join_condition tests ────────────────────────────

    #[test]
    fn test_rewrite_simple_condition() {
        let o = scan(1, "orders", "public", "o", &["id", "cust_id"]);
        let c = scan(2, "customers", "public", "c", &["id"]);
        let cond = eq_cond("o", "cust_id", "c", "id");

        let rewritten = rewrite_join_condition(&cond, &o, "dl", &c, "r");
        assert!(rewritten.contains("\"dl\"."));
        assert!(rewritten.contains("\"r\"."));
    }

    #[test]
    fn test_rewrite_nested_condition() {
        // Outer join: (orders ⋈ customers) ⋈ products
        // Condition: o.prod_id = p.id
        let o = scan(1, "orders", "public", "o", &["id", "prod_id"]);
        let c = scan(2, "customers", "public", "c", &["id"]);
        let inner = inner_join(eq_cond("o", "id", "c", "id"), o, c);
        let p = scan(3, "products", "public", "p", &["id"]);

        let cond = eq_cond("o", "prod_id", "p", "id");
        let rewritten = rewrite_join_condition(&cond, &inner, "dl", &p, "r");

        // "o" is inside the inner join → disambiguated to "o__prod_id"
        assert!(
            rewritten.contains("o__prod_id"),
            "expected o__prod_id, got: {rewritten}"
        );
        // "p" is a simple Scan → plain "id"
        assert!(rewritten.contains("\"r\"."));
    }

    #[test]
    fn test_rewrite_prefers_outer_right_alias_on_collision() {
        let left_inner = inner_join(
            eq_cond("l", "id", "r", "id"),
            scan(1, "left", "public", "l", &["id", "grp"]),
            scan(2, "right_inner", "public", "r", &["id", "grp"]),
        );
        let right_outer = scan(3, "right_outer", "public", "r", &["grp"]);

        let rewritten =
            rewrite_join_condition(&qcolref("r", "grp"), &left_inner, "dl", &right_outer, "dr");

        assert_eq!(rewritten, "\"dr\".\"grp\"");
    }

    #[test]
    fn test_rewrite_both_sides_nested() {
        let a = scan(1, "a", "public", "a", &["id"]);
        let b = scan(2, "b", "public", "b", &["id"]);
        let left = inner_join(eq_cond("a", "id", "b", "id"), a, b);

        let c = scan(3, "c", "public", "c", &["id"]);
        let d = scan(4, "d", "public", "d", &["id"]);
        let right = inner_join(eq_cond("c", "id", "d", "id"), c, d);

        let cond = eq_cond("a", "id", "c", "id");
        let rewritten = rewrite_join_condition(&cond, &left, "dl", &right, "r");
        assert!(
            rewritten.contains("a__id"),
            "expected a__id, got: {rewritten}"
        );
        assert!(
            rewritten.contains("c__id"),
            "expected c__id, got: {rewritten}"
        );
    }

    // ── is_simple_child tests ───────────────────────────────────

    #[test]
    fn test_is_simple_child_scan() {
        assert!(is_simple_child(&scan(1, "t", "public", "t", &["id"])));
    }

    #[test]
    fn test_is_simple_child_filter_over_scan() {
        let node = filter(lit("TRUE"), scan(1, "t", "public", "t", &["id"]));
        assert!(is_simple_child(&node));
    }

    #[test]
    fn test_is_simple_child_join() {
        let o = scan(1, "a", "public", "a", &["id"]);
        let c = scan(2, "b", "public", "b", &["id"]);
        let node = inner_join(eq_cond("a", "id", "b", "id"), o, c);
        assert!(!is_simple_child(&node));
    }

    // ── build_base_table_key_exprs tests ────────────────────────

    #[test]
    fn test_key_exprs_scan_non_nullable() {
        let node = scan_not_null(1, "orders", "public", "o", &["id", "name"]);
        let exprs = build_base_table_key_exprs(&node, "r");
        assert!(exprs.iter().any(|e| e.contains("r.\"id\"")));
    }

    #[test]
    fn test_key_exprs_non_scan_fallback() {
        let o = scan(1, "a", "public", "a", &["id"]);
        let c = scan(2, "b", "public", "b", &["id"]);
        let node = inner_join(eq_cond("a", "id", "b", "id"), o, c);
        let exprs = build_base_table_key_exprs(&node, "l");
        assert_eq!(exprs, vec!["row_to_json(l)::text"]);
    }

    // ── Raw SQL alias rewriting (quoted identifiers) ────────────

    #[test]
    fn test_rewrite_raw_with_quoted_aliases() {
        // Simulates the Expr::Raw produced by parse_all_sublink:
        // Expr::ColumnRef::to_sql() emits "alias"."col" (double-quoted).
        let left = scan(1, "products", "public", "p", &["id", "price"]);
        let right = scan(2, "competitors", "public", "c", &["price"]);
        let raw_cond =
            Expr::Raw(r#"(("c"."price") IS NULL OR NOT ("p"."price" < "c"."price"))"#.to_string());
        let rewritten = rewrite_join_condition(&raw_cond, &left, "dl", &right, "r");
        // "p" → "dl", "c" → "r"
        assert!(
            rewritten.contains("\"dl\".\"price\""),
            "expected dl.price, got: {rewritten}"
        );
        assert!(
            rewritten.contains("\"r\".\"price\""),
            "expected r.price, got: {rewritten}"
        );
        assert!(
            !rewritten.contains("\"p\"."),
            "should not contain old alias p, got: {rewritten}"
        );
        assert!(
            !rewritten.contains("\"c\"."),
            "should not contain old alias c, got: {rewritten}"
        );
    }

    #[test]
    fn test_rewrite_raw_with_mixed_quoted_and_unquoted() {
        let left = scan(1, "t1", "public", "a", &["id"]);
        let right = scan(2, "t2", "public", "b", &["val"]);
        // Mix of quoted and unquoted references
        let raw = Expr::Raw(r#"("a"."id" = b.val AND a.id > 0)"#.to_string());
        let rewritten = rewrite_join_condition(&raw, &left, "dl", &right, "r");
        assert!(
            !rewritten.contains("\"a\"."),
            "should not contain old alias a (quoted), got: {rewritten}"
        );
        assert!(
            !rewritten.contains("b."),
            "should not contain old alias b, got: {rewritten}"
        );
    }

    // ── SF-5: EC-01 boundary tests for use_pre_change_snapshot ──────

    #[test]
    fn test_pre_change_snapshot_simple_scan() {
        let child = scan(1, "t", "public", "t", &["id"]);
        assert!(
            use_pre_change_snapshot(&child, false, 999),
            "Simple scan should use pre-change snapshot"
        );
    }

    #[test]
    fn test_pre_change_snapshot_2_scan_join() {
        let a = scan(1, "a", "public", "a", &["id"]);
        let b = scan(2, "b", "public", "b", &["id"]);
        let j = inner_join(eq_cond("a", "id", "b", "id"), a, b);
        assert!(
            use_pre_change_snapshot(&j, false, 999),
            "2-scan join should use pre-change snapshot (EC-01 applies)"
        );
    }

    #[test]
    fn test_pre_change_snapshot_3_scan_join_uses_per_leaf_cte() {
        // EC01B-1: the ≤2-scan threshold was removed.  All non-semijoin join
        // subtrees (including ≥3 scans) now use the per-leaf CTE-based
        // pre-change snapshot strategy.
        let a = scan(1, "a", "public", "a", &["id"]);
        let b = scan(2, "b", "public", "b", &["id"]);
        let c = scan(3, "c", "public", "c", &["id"]);
        let j_ab = inner_join(eq_cond("a", "id", "b", "id"), a, b);
        let j_abc = inner_join(eq_cond("a", "id", "c", "id"), j_ab, c);
        assert!(
            use_pre_change_snapshot(&j_abc, false, 999),
            "3-scan join should use per-leaf pre-change snapshot (EC01B-1)"
        );
    }

    #[test]
    fn test_pre_change_snapshot_semijoin_forces_fallback() {
        let a = scan(1, "a", "public", "a", &["id"]);
        let b = scan(2, "b", "public", "b", &["id"]);
        let j = inner_join(eq_cond("a", "id", "b", "id"), a, b);
        // inside_semijoin=true forces fallback regardless of scan count
        assert!(
            !use_pre_change_snapshot(&j, true, 999),
            "Inside semijoin should fall back even for 2-scan join"
        );
    }

    #[test]
    fn test_pre_change_snapshot_non_join_child() {
        // Aggregate/Subquery children are not join children → safe for snapshot
        let child = aggregate(
            vec![colref("region")],
            vec![count_star("cnt")],
            scan(1, "t", "public", "t", &["id", "region"]),
        );
        assert!(
            use_pre_change_snapshot(&child, false, 999),
            "Non-join child (Aggregate) should use pre-change snapshot"
        );
    }

    #[test]
    fn test_pre_change_snapshot_join_with_missing_cte_body_falls_back() {
        let parent = scan(1, "parent", "public", "p", &["id"]);
        let aggregate_cte = cte_scan(0, "agg", "a", vec!["parent_id", "count"], vec![], vec![]);
        let child = left_join(eq_cond("p", "id", "a", "parent_id"), parent, aggregate_cte);

        assert!(
            !use_pre_change_snapshot(&child, false, 999),
            "A join containing a CTE scan cannot reconstruct an exact pre-change snapshot"
        );
    }

    // ── G17-EC01B: ≥3-scan join subtrees now use per-leaf CTE-based
    //    pre-change snapshots (EC01B-1, v0.12.0)
    //
    // These tests assert that `use_pre_change_snapshot` returns true for
    // pure InnerJoin/LeftJoin chains of any depth (no SemiJoin/AntiJoin).
    // The EC01B-1 per-leaf CTE strategy enables correct pre-change
    // snapshots without the temp_file_limit spillover that the old
    // full-snapshot EXCEPT ALL caused.

    #[test]
    fn test_ec01b_three_way_join_uses_pre_change_snapshot() {
        // a ⋈ b ⋈ c → 3 scan nodes → now uses per-leaf pre-change snapshot
        let a = scan(1, "a", "public", "a", &["id", "b_id"]);
        let b = scan(2, "b", "public", "b", &["id", "c_id"]);
        let c = scan(3, "c", "public", "c", &["id"]);
        let inner = inner_join(eq_cond("b", "c_id", "c", "id"), b, c);
        let outer = inner_join(eq_cond("a", "b_id", "b", "id"), a, inner);
        assert_eq!(join_scan_count(&outer), 3);
        assert!(
            use_pre_change_snapshot(&outer, false, 999),
            "≥3-scan join subtree should use per-leaf pre-change snapshot (EC01B-1)"
        );
    }

    #[test]
    fn test_ec01b_four_way_join_uses_pre_change_snapshot() {
        // a ⋈ b ⋈ c ⋈ d → 4 scan nodes
        let a = scan(1, "a", "public", "a", &["id"]);
        let b = scan(2, "b", "public", "b", &["id"]);
        let c = scan(3, "c", "public", "c", &["id"]);
        let d = scan(4, "d", "public", "d", &["id"]);
        let bc = inner_join(eq_cond("b", "id", "c", "id"), b, c);
        let bcd = inner_join(eq_cond("c", "id", "d", "id"), bc, d);
        let abcd = inner_join(eq_cond("a", "id", "b", "id"), a, bcd);
        assert_eq!(join_scan_count(&abcd), 4);
        assert!(
            use_pre_change_snapshot(&abcd, false, 999),
            "4-scan join subtree should use per-leaf pre-change snapshot (EC01B-1)"
        );
    }

    #[test]
    fn test_ec01b_right_subtree_three_scans_uses_pre_change_snapshot() {
        // For a join (left ⋈ right) where right has 3 scans, the right
        // side now uses per-leaf pre-change snapshot.
        let r1 = scan(2, "r1", "public", "r1", &["id"]);
        let r2 = scan(3, "r2", "public", "r2", &["id"]);
        let r3 = scan(4, "r3", "public", "r3", &["id"]);
        let right_inner = inner_join(eq_cond("r1", "id", "r2", "id"), r1, r2);
        let right_outer = inner_join(eq_cond("r2", "id", "r3", "id"), right_inner, r3);
        assert_eq!(join_scan_count(&right_outer), 3);
        assert!(
            use_pre_change_snapshot(&right_outer, false, 999),
            "Right subtree with 3 scans should use per-leaf pre-change snapshot (EC01B-1)"
        );
    }

    #[test]
    fn test_ec01b_boundary_two_scan_join_allows_pre_change_snapshot() {
        // 2-scan join → IS allowed (the boundary is > 2)
        let a = scan(1, "a", "public", "a", &["id"]);
        let b = scan(2, "b", "public", "b", &["id"]);
        let j = inner_join(eq_cond("a", "id", "b", "id"), a, b);
        assert_eq!(join_scan_count(&j), 2);
        assert!(
            use_pre_change_snapshot(&j, false, 999),
            "2-scan join should allow pre-change snapshot"
        );
    }

    // ── SnapshotPlan replaces the former depth heuristic ──────────

    #[test]
    fn test_snapshot_plan_keeps_deep_pure_join_per_leaf() {
        let a = scan(1, "a", "public", "a", &["id"]);
        let b = scan(2, "b", "public", "b", &["id"]);
        let c = scan(3, "c", "public", "c", &["id"]);
        let d = scan(4, "d", "public", "d", &["id"]);
        let ab = inner_join(eq_cond("a", "id", "b", "id"), a, b);
        let abc = inner_join(eq_cond("b", "id", "c", "id"), ab, c);
        let abcd = inner_join(eq_cond("c", "id", "d", "id"), abc, d);
        assert_eq!(join_scan_count(&abcd), 4);
        assert!(
            use_pre_change_snapshot(&abcd, false, 4),
            "SnapshotPlan selects per-leaf reconstruction for pure joins"
        );
    }

    #[test]
    fn test_snapshot_plan_keeps_three_scan_join_per_leaf() {
        let a = scan(1, "a", "public", "a", &["id"]);
        let b = scan(2, "b", "public", "b", &["id"]);
        let c = scan(3, "c", "public", "c", &["id"]);
        let ab = inner_join(eq_cond("a", "id", "b", "id"), a, b);
        let abc = inner_join(eq_cond("b", "id", "c", "id"), ab, c);
        assert_eq!(join_scan_count(&abc), 3);
        assert!(
            use_pre_change_snapshot(&abc, false, 4),
            "3-scan join at threshold=4 should keep L₀"
        );
    }

    #[test]
    fn test_snapshot_plan_keeps_scan_combined() {
        let child = scan(1, "t", "public", "t", &["id"]);
        assert!(
            use_pre_change_snapshot(&child, false, 1),
            "Simple scan should always use L₀ even at threshold=1"
        );
    }

    // ── DI-2: NOT EXISTS anti-join snapshot tests ───────────────────

    #[test]
    fn test_di2_leaf_snapshot_scan_single_pk() {
        // DI-2: Scan with single-column PK uses NOT EXISTS + pg_trickle_hash.
        let op = scan_with_pk(1, "orders", "public", "o", &["id", "amount"], &["id"]);
        let no_fallback = HashSet::new();
        let sql = build_leaf_snapshot_sql(
            &op,
            "scan_o",
            &["id".into(), "amount".into()],
            &no_fallback,
            &HashMap::new(),
        );
        assert!(
            sql.contains("NOT EXISTS"),
            "DI-2: single-PK Scan should use NOT EXISTS\n{sql}"
        );
        assert!(
            sql.contains("pgtrickle.encode_row_id_v2('SCAN_KEY'"),
            "DI-2: single-PK should use V2 identity\n{sql}"
        );
        assert!(
            sql.contains("__pgt_action = 'D'"),
            "DI-2: should UNION ALL with delete rows\n{sql}"
        );
        // Should NOT contain EXCEPT ALL
        assert!(
            !sql.contains("EXCEPT ALL"),
            "DI-2: Scan should NOT use EXCEPT ALL\n{sql}"
        );
    }

    #[test]
    fn test_di2_leaf_snapshot_scan_multi_pk() {
        // DI-2: Scan with multi-column PK uses pg_trickle_hash_multi.
        let op = scan_with_pk(1, "t", "public", "t", &["a", "b", "val"], &["a", "b"]);
        let no_fallback = HashSet::new();
        let sql = build_leaf_snapshot_sql(
            &op,
            "scan_t",
            &["a".into(), "b".into(), "val".into()],
            &no_fallback,
            &HashMap::new(),
        );
        assert!(
            sql.contains("pgtrickle.encode_row_id_v2('SCAN_KEY'"),
            "DI-2: multi-PK should use V2 identity\n{sql}"
        );
        assert!(
            sql.contains(r#""t"."a""#) && sql.contains(r#""t"."b""#),
            "DI-2: identity should reference both PK columns\n{sql}"
        );
    }

    #[test]
    fn test_di2_leaf_snapshot_scan_keyless() {
        // DI-2: Keyless Scan uses all columns for hash.
        let op = scan(1, "t", "public", "t", &["x", "y"]);
        let no_fallback = HashSet::new();
        let sql = build_leaf_snapshot_sql(
            &op,
            "scan_t",
            &["x".into(), "y".into()],
            &no_fallback,
            &HashMap::new(),
        );
        assert!(
            sql.contains("NOT EXISTS"),
            "DI-2: keyless Scan should still use NOT EXISTS\n{sql}"
        );
        assert!(
            sql.contains(r#""t"."x""#) && sql.contains(r#""t"."y""#),
            "DI-2: keyless identity should include all columns\n{sql}"
        );
    }

    #[test]
    fn test_di2_leaf_snapshot_non_scan_falls_back() {
        // DI-2: Non-Scan child (Aggregate) falls back to EXCEPT ALL.
        let child = aggregate(
            vec![colref("region")],
            vec![count_star("cnt")],
            scan(1, "t", "public", "t", &["id", "region"]),
        );
        let no_fallback = HashSet::new();
        let sql = build_leaf_snapshot_sql(
            &child,
            "agg_delta",
            &["region".into(), "cnt".into()],
            &no_fallback,
            &HashMap::new(),
        );
        assert!(
            sql.contains("EXCEPT ALL"),
            "DI-2: non-Scan child should fall back to EXCEPT ALL\n{sql}"
        );
        assert!(
            !sql.contains("NOT EXISTS"),
            "DI-2: non-Scan child should NOT use NOT EXISTS\n{sql}"
        );
    }

    #[test]
    fn test_di2_per_leaf_fallback_scan_uses_except_all() {
        // DI-2 per-leaf fallback: when source OID is in fallback set,
        // Scan uses EXCEPT ALL instead of NOT EXISTS.
        let op = scan_with_pk(42, "orders", "public", "o", &["id", "amount"], &["id"]);
        let mut fallback = HashSet::new();
        fallback.insert(42u32);
        let sql = build_leaf_snapshot_sql(
            &op,
            "scan_o",
            &["id".into(), "amount".into()],
            &fallback,
            &HashMap::new(),
        );
        assert!(
            sql.contains("EXCEPT ALL"),
            "DI-2 fallback: Scan with OID in fallback set should use EXCEPT ALL\n{sql}"
        );
        assert!(
            !sql.contains("NOT EXISTS"),
            "DI-2 fallback: Scan with OID in fallback set should NOT use NOT EXISTS\n{sql}"
        );
        assert!(
            sql.contains("__pgt_action = 'I'") && sql.contains("__pgt_action = 'D'"),
            "DI-2 fallback: should have both I and D branches\n{sql}"
        );
    }

    #[test]
    fn test_di2_per_leaf_fallback_other_oid_keeps_not_exists() {
        // DI-2 per-leaf fallback: unrelated OID in fallback set does not
        // affect this Scan.
        let op = scan_with_pk(42, "orders", "public", "o", &["id", "amount"], &["id"]);
        let mut fallback = HashSet::new();
        fallback.insert(99u32); // different OID
        let sql = build_leaf_snapshot_sql(
            &op,
            "scan_o",
            &["id".into(), "amount".into()],
            &fallback,
            &HashMap::new(),
        );
        assert!(
            sql.contains("NOT EXISTS"),
            "DI-2: Scan with different OID should keep NOT EXISTS\n{sql}"
        );
        assert!(
            !sql.contains("EXCEPT ALL"),
            "DI-2: Scan with different OID should NOT use EXCEPT ALL\n{sql}"
        );
    }
}
