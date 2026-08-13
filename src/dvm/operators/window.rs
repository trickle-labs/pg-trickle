//! Window function differentiation via partition-based recomputation.
//!
//! Strategy: For each partition that has *any* changed rows (inserts,
//! updates, or deletes in the child delta), recompute the window
//! function for the entire partition. This avoids tracking complex
//! window state incrementally.
//!
//! CTE chain:
//! 1. Child delta (from recursive diff_node)
//! 2. Changed partition keys (DISTINCT partition_by cols from delta)
//! 3. Old ST rows for changed partitions (emitted as 'D' actions)
//! 4. Reconstruct current input for changed partitions from ST + delta
//! 5. Recompute window function on current input (emitted as 'I' actions)
//! 6. Combine deletes + inserts into final delta

use crate::dvm::diff::{DiffContext, DiffResult, col_list, prefixed_col_list, quote_ident};
use crate::dvm::parser::{AggFunc, OpTree, WindowExpr};
use crate::error::PgTrickleError;

/// Build a mapping from aggregate SQL expressions (uppercased) to their
/// output aliases.
///
/// When a Window sits on top of an Aggregate (e.g., `RANK() OVER (ORDER BY SUM(val))`),
/// the window function's ORDER BY references aggregate expressions that no longer exist
/// in the pre-aggregated current_input CTE. This mapping allows the window diff to
/// rewrite `SUM(val)` → `dept_total` (the aggregate's output alias) so the
/// recomputed CTE references the correct columns.
fn build_agg_alias_map(child: &OpTree) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    // Look through transparent wrappers to find the Aggregate node
    let node = match child {
        OpTree::Filter { child, .. } | OpTree::Subquery { child, .. } => child.as_ref(),
        other => other,
    };
    if let OpTree::Aggregate { aggregates, .. } = node {
        for agg in aggregates {
            // Build the SQL form of this aggregate: e.g., SUM(val).
            // Strip table qualifiers from arguments so the key matches even
            // when the argument is written as `SUM(oi.quantity)` in the
            // source query.  The lookup side (render_window_sql) also strips
            // qualifiers, so both sides are normalised.
            let agg_sql = match &agg.function {
                AggFunc::CountStar => "COUNT(*)".to_string(),
                _ => {
                    let arg_sql = agg
                        .argument
                        .as_ref()
                        .map_or(String::new(), |e| e.strip_qualifier().to_sql());
                    format!("{}({})", agg.function.sql_name(), arg_sql)
                }
            };
            // Use uppercase key for case-insensitive matching
            map.insert(agg_sql.to_uppercase(), agg.alias.clone());
        }
    }
    map
}

/// Render a window function expression back to SQL, replacing aggregate
/// expressions in ORDER BY / PARTITION BY with their aliases from the
/// aggregate child. This is needed when the Window sits on top of an
/// Aggregate (e.g., `RANK() OVER (ORDER BY SUM(val))` becomes
/// `RANK() OVER (ORDER BY "dept_total")`).
///
/// The `agg_map` keys are uppercased for case-insensitive matching.
fn render_window_sql(
    w: &WindowExpr,
    agg_map: &std::collections::HashMap<String, String>,
) -> String {
    // Always render manually (no early return for agg_map.is_empty()) so that
    // we can strip table qualifiers from every expression.  Within the
    // recomputed-input CTE all columns are referenced by their bare output
    // names (the pass-through aliases), never by the original table-qualified
    // form from the source query.

    /// Resolve an expression against the aggregate alias map (case-insensitive).
    fn resolve(sql: &str, m: &std::collections::HashMap<String, String>) -> String {
        m.get(&sql.to_uppercase())
            .map_or_else(|| sql.to_string(), |alias| quote_ident(alias))
    }

    let args_sql = if w.args.is_empty() {
        String::new()
    } else {
        w.args
            .iter()
            .map(|a| resolve(&a.strip_qualifier().to_sql(), agg_map))
            .collect::<Vec<_>>()
            .join(", ")
    };

    let mut over_parts = Vec::new();
    if !w.partition_by.is_empty() {
        let pb = w
            .partition_by
            .iter()
            .map(|e| resolve(&e.strip_qualifier().to_sql(), agg_map))
            .collect::<Vec<_>>()
            .join(", ");
        over_parts.push(format!("PARTITION BY {pb}"));
    }
    if !w.order_by.is_empty() {
        let ob = w
            .order_by
            .iter()
            .map(|s| {
                let resolved = resolve(&s.expr.strip_qualifier().to_sql(), agg_map);
                let dir = if s.ascending { "ASC" } else { "DESC" };
                let nulls = if s.ascending {
                    if s.nulls_first { " NULLS FIRST" } else { "" }
                } else if s.nulls_first {
                    ""
                } else {
                    " NULLS LAST"
                };
                format!("{resolved} {dir}{nulls}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        over_parts.push(format!("ORDER BY {ob}"));
    }
    if let Some(ref frame) = w.frame_clause {
        over_parts.push(frame.clone());
    }

    let over_clause = over_parts.join(" ");
    format!("{}({args_sql}) OVER ({over_clause})", w.func_name)
}

/// Differentiate a Window node.
pub fn diff_window(ctx: &mut DiffContext, op: &OpTree) -> Result<DiffResult, PgTrickleError> {
    let OpTree::Window {
        window_exprs,
        partition_by,
        pass_through,
        child,
    } = op
    else {
        return Err(PgTrickleError::InternalError(
            "diff_window called on non-Window node".into(),
        ));
    };

    // ── Differentiate child to get the delta ───────────────────────────
    let child_result = ctx.diff_node(child)?;

    let st_table = ctx
        .st_qualified_name
        .clone()
        .unwrap_or_else(|| "/* st_table */".to_string());

    // Column lists
    let pt_aliases: Vec<String> = pass_through.iter().map(|(_, a)| a.clone()).collect();
    let wf_aliases: Vec<String> = window_exprs.iter().map(|w| w.alias.clone()).collect();

    // Detect auxiliary columns from the child result (e.g., __pgt_count
    // from an Aggregate child) that need to be passed through the window
    // diff so the MERGE can update them in the storage table.
    // Only include internal __pgt_* columns, not regular child columns.
    let aux_cols: Vec<String> = child_result
        .columns
        .iter()
        .filter(|c| c.starts_with("__pgt_") && !pt_aliases.contains(c) && !wf_aliases.contains(c))
        .cloned()
        .collect();

    let mut all_output_cols = pt_aliases.clone();
    all_output_cols.extend(wf_aliases.iter().cloned());
    all_output_cols.extend(aux_cols.iter().cloned());

    // Determine which output columns actually exist in the ST storage
    // table. When a Window is wrapped by an outer Project that strips
    // some columns (e.g., DISTINCT ON rewrite strips __pgt_rn), the
    // window function aliases won't exist in the ST. We must use NULL
    // for those columns when reading old ST rows.
    let st_stored_cols: Option<Vec<String>> = ctx.st_user_columns.clone();

    // Use output_name() (bare column name, no table qualifier) so that
    // col_list() / quote_ident() don't wrap an already-qualified expression
    // like `"p"."category"` in another layer of quotes.  Within all the
    // generated CTEs the column is referenced by its unqualified output
    // name (i.e. the pass-through alias), never by the original table-qualified
    // form from the source query.
    let partition_cols: Vec<String> = partition_by.iter().map(|e| e.output_name()).collect();

    // ── CTE 1: Find changed partition keys ─────────────────────────────
    let changed_parts_cte = ctx.next_cte_name("win_parts");
    if partition_cols.is_empty() {
        // Un-partitioned: any change means recompute everything.
        // Emit a single dummy row to trigger recomputation.
        let parts_sql = format!(
            "SELECT 1 AS __pgt_dummy\nFROM {child} LIMIT 1",
            child = child_result.cte_name,
        );
        ctx.add_cte(changed_parts_cte.clone(), parts_sql);
    } else {
        let distinct_cols = col_list(&partition_cols);
        let parts_sql = format!(
            "SELECT DISTINCT {distinct_cols}\nFROM {child}",
            child = child_result.cte_name,
        );
        ctx.add_cte(changed_parts_cte.clone(), parts_sql);
    }

    // ── join condition: st partition cols = cp partition cols ───────────
    let partition_join_st_cp = if partition_cols.is_empty() {
        "TRUE".to_string()
    } else {
        partition_cols
            .iter()
            .map(|c| {
                let qc = quote_ident(c);
                format!("st.{qc} IS NOT DISTINCT FROM cp.{qc}")
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    };

    // ── CTE 2: Old ST rows for changed partitions (DELETE actions) ─────
    let old_rows_cte = ctx.next_cte_name("win_old");
    // Only SELECT columns from the ST that actually exist there. For
    // window aliases stripped by an outer Project (e.g., __pgt_rn from
    // DISTINCT ON rewrite), emit a typed NULL instead of st."col" to
    // avoid both "column does not exist" errors and UNION ALL type
    // mismatches (untyped NULL defaults to text, but window functions
    // return bigint / numeric / double precision).
    let wf_return_types: std::collections::HashMap<String, &str> = window_exprs
        .iter()
        .map(|w| {
            let ret_type = match w.func_name.to_lowercase().as_str() {
                "row_number" | "rank" | "dense_rank" | "ntile" | "count" => "bigint",
                "percent_rank" | "cume_dist" => "double precision",
                _ => "bigint", // safe default for most window functions
            };
            (w.alias.clone(), ret_type)
        })
        .collect();
    let all_cols_st = all_output_cols
        .iter()
        .map(|c| {
            // Auxiliary columns from the child diff (e.g. __pgt_count from
            // an Aggregate) always exist in the ST storage table (they are
            // added during CREATE/ALTER). Read them directly instead of
            // checking st_stored_cols, which only reflects user-visible
            // output_columns() and omits internal __pgt_* columns.
            if aux_cols.contains(c) {
                return format!("st.{}", quote_ident(c));
            }
            let col_exists_in_st = st_stored_cols
                .as_ref()
                .map(|cols| cols.contains(c))
                .unwrap_or(true);
            if col_exists_in_st {
                format!("st.{}", quote_ident(c))
            } else {
                let cast_type = wf_return_types.get(c).copied().unwrap_or("bigint");
                format!("NULL::{cast_type} AS {}", quote_ident(c))
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    let old_rows_sql = format!(
        "SELECT st.\"__pgt_row_id\", {all_cols_st}\n\
         FROM {st_table} st\n\
         WHERE EXISTS (\n\
         SELECT 1 FROM {changed_parts_cte} cp WHERE {partition_join_st_cp}\n\
         )",
    );
    ctx.add_cte(old_rows_cte.clone(), old_rows_sql);

    // ── CTE 3: Reconstruct current input for changed partitions ────────
    // Current input = (old ST rows NOT deleted by delta) UNION ALL (delta inserts)
    let current_input_cte = ctx.next_cte_name("win_input");

    let pt_cols_old = prefixed_col_list("o", &pt_aliases);
    let pt_cols_delta = prefixed_col_list("d", &pt_aliases);

    // For the surviving rows, we need partition cols from the old rows too
    let partition_join_delta_cp = if partition_cols.is_empty() {
        "TRUE".to_string()
    } else {
        partition_cols
            .iter()
            .map(|c| {
                let qc = quote_ident(c);
                format!("d.{qc} IS NOT DISTINCT FROM cp.{qc}")
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    };

    let current_input_sql = {
        // Build aux column selections for surviving old rows and delta inserts
        let aux_old = if aux_cols.is_empty() {
            String::new()
        } else {
            let parts: Vec<String> = aux_cols
                .iter()
                .map(|c| format!(", o.{}", quote_ident(c)))
                .collect();
            parts.join("")
        };
        let aux_delta = if aux_cols.is_empty() {
            String::new()
        } else {
            let parts: Vec<String> = aux_cols
                .iter()
                .map(|c| format!(", d.{}", quote_ident(c)))
                .collect();
            parts.join("")
        };

        // Match surviving old rows against child delta rows using the most
        // stable child key we can recover. This is critical for Window over
        // Aggregate: aggregate updates arrive as replacement rows keyed by the
        // GROUP BY columns, not necessarily as full-row DELETE+INSERT pairs.
        // When key columns are available, ANY child delta row for the same key
        // should replace the previous input row.
        let child_key_cols = child
            .row_id_key_columns()
            .filter(|keys| !keys.is_empty() && keys.iter().all(|key| pt_aliases.contains(key)));
        let key_match_cond = child_key_cols.as_ref().map(|keys| {
            keys.iter()
                .map(|c| {
                    let qc = quote_ident(c);
                    format!("d2.{qc} IS NOT DISTINCT FROM o.{qc}")
                })
                .collect::<Vec<_>>()
                .join(" AND ")
        });

        // Fallback: match surviving old rows against explicit child DELETEs
        // using full pass-through columns, not __pgt_row_id. The ST stores row
        // IDs based on row_to_json+row_number from initial population, while
        // many child operators use different row-id formulas.
        let delete_match_cond = if pt_aliases.is_empty() {
            "TRUE".to_string()
        } else {
            pt_aliases
                .iter()
                .map(|c| {
                    let qc = quote_ident(c);
                    format!("d2.{qc} IS NOT DISTINCT FROM o.{qc}")
                })
                .collect::<Vec<_>>()
                .join(" AND ")
        };

        if let Some(key_cond) = key_match_cond {
            // Key-based matching: keys are unique, simple NOT EXISTS is safe.
            format!(
                "-- Surviving old rows (key-matched exclusion)\n\
                 SELECT {pt_cols_old}{aux_old}\n\
                 FROM {old_rows_cte} o\n\
                 WHERE NOT EXISTS (\n\
                     SELECT 1 FROM {child_delta} d2\n\
                     WHERE {key_cond}\n\
                 )\n\
                 UNION ALL\n\
                 -- Newly inserted rows\n\
                 SELECT {pt_cols_delta}{aux_delta}\n\
                 FROM {child_delta} d\n\
                 WHERE d.\"__pgt_action\" = 'I'\n\
                 AND EXISTS (\n\
                     SELECT 1 FROM {changed_parts_cte} cp WHERE {partition_join_delta_cp}\n\
                 )",
                child_delta = child_result.cte_name,
            )
        } else {
            // Content-based matching with counted exclusion.
            // When multiple old rows share the same pass-through values
            // (e.g., tied salaries in a DENSE_RANK window), a naive NOT EXISTS
            // would remove ALL matching rows instead of just the deleted ones.
            // ROW_NUMBER pairing ensures exactly one old row is excluded per
            // delta DELETE with the same content.
            let rn_partition_old = if pt_aliases.is_empty() {
                String::new()
            } else {
                format!(
                    "PARTITION BY {} ",
                    pt_aliases
                        .iter()
                        .map(|c| format!("o.{}", quote_ident(c)))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            };
            let rn_partition_d2 = if pt_aliases.is_empty() {
                String::new()
            } else {
                format!(
                    "PARTITION BY {} ",
                    pt_aliases
                        .iter()
                        .map(|c| format!("_xd.{}", quote_ident(c)))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            };
            let pt_cols_xd = prefixed_col_list("_xd", &pt_aliases);
            let aux_xd = if aux_cols.is_empty() {
                String::new()
            } else {
                aux_cols
                    .iter()
                    .map(|c| format!(", _xd.{}", quote_ident(c)))
                    .collect::<Vec<String>>()
                    .join("")
            };
            format!(
                "-- Surviving old rows (counted exclusion for duplicate content)\n\
                 SELECT {pt_cols_old}{aux_old}\n\
                 FROM (\n\
                     SELECT {pt_cols_old}{aux_old},\n\
                            ROW_NUMBER() OVER ({rn_partition_old}ORDER BY o.\"__pgt_row_id\") AS __pgt_xrn\n\
                     FROM {old_rows_cte} o\n\
                 ) o\n\
                 WHERE NOT EXISTS (\n\
                     SELECT 1 FROM (\n\
                         SELECT {pt_cols_xd}{aux_xd},\n\
                                ROW_NUMBER() OVER ({rn_partition_d2}ORDER BY _xd.\"__pgt_row_id\") AS __pgt_xrn\n\
                         FROM {child_delta} _xd\n\
                         WHERE _xd.\"__pgt_action\" = 'D'\n\
                     ) d2\n\
                     WHERE {delete_match_cond} AND d2.__pgt_xrn = o.__pgt_xrn\n\
                 )\n\
                 UNION ALL\n\
                 -- Newly inserted rows\n\
                 SELECT {pt_cols_delta}{aux_delta}\n\
                 FROM {child_delta} d\n\
                 WHERE d.\"__pgt_action\" = 'I'\n\
                 AND EXISTS (\n\
                     SELECT 1 FROM {changed_parts_cte} cp WHERE {partition_join_delta_cp}\n\
                 )",
                child_delta = child_result.cte_name,
            )
        }
    };
    ctx.add_cte(current_input_cte.clone(), current_input_sql);

    // ── CTE 4: Recompute window functions on current input ─────────────
    let recomputed_cte = ctx.next_cte_name("win_recomp");

    // Build aggregate alias map for Window-over-Aggregate queries.
    // When the child is an Aggregate, ORDER BY expressions like SUM(val)
    // must be rewritten to their output aliases (e.g., "dept_total").
    let agg_map = build_agg_alias_map(child);

    let window_func_selects: Vec<String> = window_exprs
        .iter()
        .map(|w| {
            format!(
                "{} AS {}",
                render_window_sql(w, &agg_map),
                quote_ident(&w.alias)
            )
        })
        .collect();

    // Compute unique row_ids using row_to_json + row_number, matching the
    // initial population formula. This guarantees uniqueness even when
    // pass-through columns have duplicates (e.g., DENSE_RANK ties).
    // The window diff deletes ALL old rows in changed partitions and
    // inserts ALL recomputed rows, so row_ids don't need to match between
    // old and new — they just need to be unique.

    let pt_cols_ci = prefixed_col_list("ci", &pt_aliases);

    // Include aux columns in recomputed output
    let aux_ci = if aux_cols.is_empty() {
        String::new()
    } else {
        let parts: Vec<String> = aux_cols
            .iter()
            .map(|c| format!(",\n               ci.{}", quote_ident(c)))
            .collect();
        parts.join("")
    };

    let recomputed_sql = format!(
        "SELECT pgtrickle.pg_trickle_hash(\
               row_to_json(w)::text || '/' || row_number() OVER ()::text\
         ) AS \"__pgt_row_id\",\n\
               {all_cols_w}{aux_w}\n\
         FROM (\n\
               SELECT {pt_cols_ci},\n\
                      {wf_selects}{aux_ci}\n\
               FROM {current_input_cte} ci\n\
         ) w",
        all_cols_w = all_output_cols
            .iter()
            .filter(|c| !aux_cols.contains(c))
            .map(|c| format!("w.{}", quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", "),
        aux_w = if aux_cols.is_empty() {
            String::new()
        } else {
            aux_cols
                .iter()
                .map(|c| format!(", w.{}", quote_ident(c)))
                .collect::<Vec<String>>()
                .join("")
        },
        wf_selects = window_func_selects.join(",\n       "),
    );
    ctx.add_cte(recomputed_cte.clone(), recomputed_sql);

    // ── CTE 5: Final delta — DELETE old + INSERT recomputed ────────────
    let final_cte = ctx.next_cte_name("win_final");

    let all_cols_name = col_list(&all_output_cols);

    let final_sql = format!(
        "-- Insert recomputed window results (listed first so that\n\
         -- PostgreSQL infers correct column types for the UNION ALL;\n\
         -- old_rows may contain NULL placeholders for window columns\n\
         -- stripped by an outer Project, which default to type text)\n\
         SELECT \"__pgt_row_id\", 'I' AS \"__pgt_action\", {all_cols_name}\n\
         FROM {recomputed_cte}\n\
         UNION ALL\n\
         -- Delete old window results for changed partitions\n\
         SELECT \"__pgt_row_id\", 'D' AS \"__pgt_action\", {all_cols_name}\n\
         FROM {old_rows_cte}",
    );
    ctx.add_cte(final_cte.clone(), final_sql);

    Ok(DiffResult {
        cte_name: final_cte,
        columns: all_output_cols,
        is_deduplicated: false,
        has_key_changed: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm::operators::test_helpers::*;

    #[test]
    fn test_diff_window_basic() {
        let mut ctx = test_ctx_with_st("public", "my_st");
        let child = scan(1, "orders", "public", "o", &["id", "region", "amount"]);
        let wf = window_expr(
            "ROW_NUMBER",
            vec![],
            vec![colref("region")],
            vec![sort_asc(colref("amount"))],
            "rn",
        );
        let tree = window(
            vec![wf],
            vec![colref("region")],
            vec![
                (colref("id"), "id".to_string()),
                (colref("region"), "region".to_string()),
                (colref("amount"), "amount".to_string()),
            ],
            child,
        );
        let result = diff_window(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Output should include pass-through + window alias
        assert!(result.columns.contains(&"id".to_string()));
        assert!(result.columns.contains(&"region".to_string()));
        assert!(result.columns.contains(&"amount".to_string()));
        assert!(result.columns.contains(&"rn".to_string()));

        // Should have the CTE chain: changed parts, old rows, input, recompute, final
        assert_sql_contains(&sql, "DELETE");
        assert_sql_contains(&sql, "INSERT");
    }

    #[test]
    fn test_diff_window_changed_partition_detection() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["id", "grp", "val"]);
        let wf = window_expr(
            "SUM",
            vec![colref("val")],
            vec![colref("grp")],
            vec![],
            "running_sum",
        );
        let tree = window(
            vec![wf],
            vec![colref("grp")],
            vec![
                (colref("id"), "id".to_string()),
                (colref("grp"), "grp".to_string()),
                (colref("val"), "val".to_string()),
            ],
            child,
        );
        let result = diff_window(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Should detect changed partitions via DISTINCT partition keys
        assert_sql_contains(&sql, "DISTINCT");
    }

    #[test]
    fn test_diff_window_unpartitioned() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["id", "val"]);
        let wf = window_expr(
            "ROW_NUMBER",
            vec![],
            vec![],
            vec![sort_asc(colref("val"))],
            "rn",
        );
        let tree = window(
            vec![wf],
            vec![], // no partition_by
            vec![
                (colref("id"), "id".to_string()),
                (colref("val"), "val".to_string()),
            ],
            child,
        );
        let result = diff_window(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Un-partitioned: any change → recompute all
        assert_sql_contains(&sql, "LIMIT 1");
    }

    #[test]
    fn test_diff_window_not_deduplicated() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["id", "val"]);
        let wf = window_expr(
            "ROW_NUMBER",
            vec![],
            vec![],
            vec![sort_asc(colref("val"))],
            "rn",
        );
        let tree = window(
            vec![wf],
            vec![],
            vec![
                (colref("id"), "id".to_string()),
                (colref("val"), "val".to_string()),
            ],
            child,
        );
        let result = diff_window(&mut ctx, &tree).unwrap();
        assert!(!result.is_deduplicated);
    }

    #[test]
    fn test_diff_window_error_on_non_window_node() {
        let mut ctx = test_ctx_with_st("public", "st");
        let tree = scan(1, "t", "public", "t", &["id"]);
        let result = diff_window(&mut ctx, &tree);
        assert!(result.is_err());
    }

    /// Regression test: table-qualified column refs in PARTITION BY (e.g.
    /// `PARTITION BY p.category`) must NOT produce double-quoted identifiers
    /// like `""p"."category""` in the generated SQL.  The column should be
    /// referenced by its bare output name (`"category"`) throughout all CTEs.
    #[test]
    fn test_diff_window_qualified_partition_columns() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "products", "public", "p", &["name", "category", "price"]);
        let wf = window_expr(
            "RANK",
            vec![],
            // Partition by a table-qualified column ref: p.category
            vec![qcolref("p", "category")],
            vec![sort_asc(qcolref("p", "price"))],
            "category_rank",
        );
        let tree = window(
            vec![wf],
            // Shared partition_by also uses the qualified form
            vec![qcolref("p", "category")],
            vec![
                (qcolref("p", "name"), "name".to_string()),
                (qcolref("p", "category"), "category".to_string()),
                (qcolref("p", "price"), "price".to_string()),
            ],
            child,
        );
        let result = diff_window(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // The generated SQL must NOT contain double-quoted identifiers of
        // the form `""p"."category""` — that is the symptom of the bug.
        assert!(
            !sql.contains(r#"""p"."category"""#),
            "SQL contains double-quoted qualified ref (bug regression): {sql}"
        );

        // The column should appear as the bare identifier "category"
        assert!(
            sql.contains("\"category\"") || sql.contains("category"),
            "Expected bare 'category' column reference in SQL: {sql}"
        );

        // Standard shape checks
        assert!(result.columns.contains(&"category_rank".to_string()));
        assert_sql_contains(&sql, "DISTINCT");
        assert_sql_contains(&sql, "DELETE");
        assert_sql_contains(&sql, "INSERT");
    }
}
