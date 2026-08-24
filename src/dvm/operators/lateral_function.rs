//! LATERAL set-returning function differentiation via row-scoped recomputation.
//!
//! Strategy: When source rows change, re-expand the SRF only for affected
//! rows. This is analogous to the window function operator's partition-based
//! recomputation, but the "partition key" is the source row's PK.
//!
//! CTE chain:
//! 1. Child delta (from recursive diff_node on the LATERAL dependency)
//! 2. Re-expand the SRF for deleted/updated source rows (emitted as 'D' actions)
//! 3. Re-expand the SRF for inserted/updated source rows (emitted as 'I' actions)
//! 4. Combine deletes + inserts into final delta
//!
//! Row identity: `hash(child_row_columns || '/' || srf_result)` — content-based.
//! This is stable as long as the same source row produces the same expanded values.

use crate::dvm::diff::{DiffContext, DiffResult, col_list, quote_ident};
use crate::dvm::operators::scan::build_hash_expr;
use crate::dvm::parser::{OpTree, lateral_function_output_columns};
use crate::error::PgTrickleError;

/// Differentiate a LateralFunction node via row-scoped recomputation.
///
/// For each source row that changed (INSERT/UPDATE/DELETE), delete old
/// SRF expansions from the ST and re-expand the SRF for the new version
/// of the source row.
pub fn diff_lateral_function(
    ctx: &mut DiffContext,
    op: &OpTree,
) -> Result<DiffResult, PgTrickleError> {
    let OpTree::LateralFunction {
        func_sql,
        alias,
        column_aliases,
        with_ordinality,
        child,
    } = op
    else {
        return Err(PgTrickleError::InternalError(
            "diff_lateral_function called on non-LateralFunction node".into(),
        ));
    };

    // ── Differentiate child to get the source delta ────────────────────
    let child_result = ctx.diff_node(child)?;

    // Column names from the child (source table columns)
    let child_cols = &child_result.columns;

    // Use the outer table's original alias for the changed-sources CTE
    // so that the func_sql's column references (e.g. `d.data` in
    // `jsonb_array_elements(d.data)`) resolve naturally.
    let outer_alias = child.alias().to_string();

    // SRF result column names
    let srf_cols = lateral_function_output_columns(func_sql, alias, column_aliases);

    let mut srf_cols_with_ord = srf_cols.clone();
    if *with_ordinality {
        srf_cols_with_ord.push("ordinality".to_string());
    }

    // All output columns = child columns + SRF columns (+ optional ordinality)
    let mut all_output_cols: Vec<String> = child_cols.clone();
    all_output_cols.extend(srf_cols_with_ord.iter().cloned());

    // ── CTE 1: Find source rows that changed ───────────────────────────
    let changed_sources_cte = ctx.next_cte_name("lat_changed");
    let changed_sources_sql = format!(
        "SELECT DISTINCT \"__pgt_row_id\", \"__pgt_action\", {child_col_list}\n\
         FROM {child_delta}",
        child_col_list = col_list(child_cols),
        child_delta = child_result.cte_name,
    );
    ctx.add_cte(changed_sources_cte.clone(), changed_sources_sql);

    // ── Build shared SRF components ────────────────────────────────────

    // Always name the SRF output columns explicitly so upstream operators can
    // resolve references like `e.value` and `kv.key` against the delta CTE.
    let srf_col_refs: Vec<String> = srf_cols_with_ord
        .iter()
        .map(|c| format!("{}.{}", quote_ident(alias), quote_ident(c)))
        .collect();
    let srf_col_refs_str = srf_col_refs.join(", ");

    let outer_alias_q = quote_ident(&outer_alias);
    let child_col_refs: Vec<String> = child_cols
        .iter()
        .map(|c| format!("{outer_alias_q}.{}", quote_ident(c)))
        .collect();
    let child_col_refs_str = child_col_refs.join(", ");

    // Build hash expression for the row ID: hash all output columns.
    let hash_exprs: Vec<String> = child_cols
        .iter()
        .map(|c| format!("{outer_alias_q}.{}::TEXT", quote_ident(c)))
        .chain(
            srf_cols_with_ord
                .iter()
                .map(|c| format!("{}.{}::TEXT", quote_ident(alias), quote_ident(c)))
                .collect::<Vec<_>>(),
        )
        .collect();
    let row_id_expr = build_hash_expr(&hash_exprs);

    // Build the LATERAL SRF clause with optional WITH ORDINALITY
    let ordinality_clause = if *with_ordinality {
        " WITH ORDINALITY"
    } else {
        ""
    };

    let col_alias_list = srf_cols_with_ord
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let srf_alias_clause = format!(
        "{}{ordinality_clause} ({col_alias_list})",
        quote_ident(alias),
    );

    // ── CTE 2: Re-expand SRF for deleted/updated source rows (DELETE) ──
    // Instead of reading from the stream table (which may not have
    // intermediate columns like source-table-only columns), re-expand
    // the SRF using the old values from the child delta (action = 'D').
    // This produces the old expanded rows that should be removed.
    let old_rows_cte = ctx.next_cte_name("lat_old");
    let old_rows_sql = format!(
        "SELECT {row_id_expr} AS \"__pgt_row_id\",\n\
                {child_col_refs_str},\n\
                {srf_col_refs_str}\n\
         FROM {changed_sources_cte} AS {outer_alias_q},\n\
              LATERAL {func_sql} AS {srf_alias_clause}\n\
         WHERE {outer_alias_q}.\"__pgt_action\" = 'D'",
    );
    ctx.add_cte(old_rows_cte.clone(), old_rows_sql);

    // ── CTE 3: Re-expand SRF for inserted/updated source rows (INSERT) ─
    // For source rows with action 'I' (insert), expand the SRF on the new
    // row values. UPDATE actions in the child delta appear as D+I pairs,
    // so CTE 2 handles the delete side and this CTE handles the insert side.
    let expand_cte = ctx.next_cte_name("lat_expand");
    let expand_sql = format!(
        "SELECT {row_id_expr} AS \"__pgt_row_id\",\n\
                {child_col_refs_str},\n\
                {srf_col_refs_str}\n\
         FROM {changed_sources_cte} AS {outer_alias_q},\n\
              LATERAL {func_sql} AS {srf_alias_clause}\n\
         WHERE {outer_alias_q}.\"__pgt_action\" = 'I'",
    );
    ctx.add_cte(expand_cte.clone(), expand_sql);

    // ── CTE 4: Final delta — DELETE old + INSERT new ───────────────────
    let final_cte = ctx.next_cte_name("lat_final");

    let all_cols_name = col_list(&all_output_cols);

    let final_sql = format!(
        "-- Delete old SRF expansions for changed source rows\n\
         SELECT \"__pgt_row_id\", 'D' AS \"__pgt_action\", {all_cols_name}\n\
         FROM {old_rows_cte}\n\
         UNION ALL\n\
         -- Insert re-expanded SRF results for new/updated source rows\n\
         SELECT \"__pgt_row_id\", 'I' AS \"__pgt_action\", {all_cols_name}\n\
         FROM {expand_cte}",
    );
    ctx.add_cte(final_cte.clone(), final_sql);

    Ok(DiffResult {
        cte_name: final_cte,
        columns: all_output_cols.clone(),
        schema: child_result
            .schema
            .concat(&crate::dvm::schema::RelationSchema::from_names(
                &srf_cols_with_ord,
            )),
        is_deduplicated: false,
        has_key_changed: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm::operators::test_helpers::*;

    /// Build a LateralFunction node for tests.
    fn lateral_func(
        func_sql: &str,
        alias: &str,
        col_aliases: Vec<&str>,
        with_ordinality: bool,
        child: OpTree,
    ) -> OpTree {
        OpTree::LateralFunction {
            func_sql: func_sql.to_string(),
            alias: alias.to_string(),
            column_aliases: col_aliases.into_iter().map(|c| c.to_string()).collect(),
            with_ordinality,
            child: Box::new(child),
        }
    }

    #[test]
    fn test_diff_lateral_function_basic() {
        let mut ctx = test_ctx_with_st("public", "my_st");
        let child = scan(1, "parent", "public", "p", &["id", "data"]);
        let tree = lateral_func(
            "jsonb_array_elements(p.data->'children')",
            "child",
            vec!["value"],
            false,
            child,
        );
        let result = diff_lateral_function(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Output should include child columns + SRF columns
        assert!(result.columns.contains(&"id".to_string()));
        assert!(result.columns.contains(&"data".to_string()));
        assert!(result.columns.contains(&"value".to_string()));

        // Should have the CTE chain
        assert_sql_contains(&sql, "lat_changed");
        assert_sql_contains(&sql, "lat_old");
        assert_sql_contains(&sql, "lat_expand");
        assert_sql_contains(&sql, "lat_final");
    }

    #[test]
    fn test_diff_lateral_function_with_ordinality() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["id", "arr"]);
        let tree = lateral_func(
            "jsonb_array_elements(t.arr)",
            "elem",
            vec!["value"],
            true,
            child,
        );
        let result = diff_lateral_function(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Output should include ordinality column
        assert!(result.columns.contains(&"ordinality".to_string()));
        assert_sql_contains(&sql, "WITH ORDINALITY");
    }

    #[test]
    fn test_diff_lateral_function_no_column_aliases() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["id", "tags"]);
        let tree = lateral_func("unnest(t.tags)", "tag", vec![], false, child);
        let result = diff_lateral_function(&mut ctx, &tree).unwrap();

        // When no column aliases, the alias name becomes the column
        assert!(result.columns.contains(&"tag".to_string()));
    }

    #[test]
    fn test_diff_lateral_function_multi_column_srf() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["id", "props"]);
        let tree = lateral_func(
            "jsonb_each(t.props)",
            "kv",
            vec!["key", "value"],
            false,
            child,
        );
        let result = diff_lateral_function(&mut ctx, &tree).unwrap();

        // Should have both key and value columns
        assert!(result.columns.contains(&"key".to_string()));
        assert!(result.columns.contains(&"value".to_string()));
    }

    #[test]
    fn test_diff_lateral_function_infers_jsonb_each_columns() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["id", "props"]);
        let tree = lateral_func("jsonb_each(t.props)", "kv", vec![], false, child);
        let result = diff_lateral_function(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        assert!(result.columns.contains(&"key".to_string()));
        assert!(result.columns.contains(&"value".to_string()));
        assert_sql_contains(&sql, "AS \"kv\" (\"key\", \"value\")");
    }

    #[test]
    fn test_diff_lateral_function_infers_jsonb_array_value_column() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["id", "data"]);
        let tree = lateral_func("jsonb_array_elements(t.data)", "e", vec![], false, child);
        let result = diff_lateral_function(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        assert!(result.columns.contains(&"value".to_string()));
        assert!(!result.columns.contains(&"e".to_string()));
        assert_sql_contains(&sql, "AS \"e\" (\"value\")");
    }

    #[test]
    fn test_diff_lateral_function_old_rows_reexpands_with_delete_action() {
        let mut ctx = test_ctx_with_st("public", "my_st");
        let child = scan(1, "parent", "public", "p", &["id", "data"]);
        let tree = lateral_func(
            "jsonb_array_elements(p.data)",
            "elem",
            vec!["value"],
            false,
            child,
        );
        let result = diff_lateral_function(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Old rows CTE should re-expand SRF using deleted/old source rows
        assert_sql_contains(&sql, "__pgt_action\" = 'D'");
        // Should use LATERAL for the re-expansion and the child's original alias
        assert_sql_contains(&sql, "LATERAL jsonb_array_elements");
    }

    #[test]
    fn test_diff_lateral_function_expand_filters_inserts() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["id", "data"]);
        let tree = lateral_func(
            "jsonb_array_elements(t.data)",
            "elem",
            vec!["value"],
            false,
            child,
        );
        let result = diff_lateral_function(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // The expand CTE should only process INSERT actions
        assert_sql_contains(&sql, "__pgt_action\" = 'I'");
    }

    #[test]
    fn test_diff_lateral_function_uses_lateral_keyword() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["id", "data"]);
        let tree = lateral_func(
            "jsonb_array_elements(t.data)",
            "elem",
            vec!["value"],
            false,
            child,
        );
        let result = diff_lateral_function(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // The expand CTE must use LATERAL keyword
        assert_sql_contains(&sql, "LATERAL jsonb_array_elements");
    }

    #[test]
    fn test_diff_lateral_function_not_deduplicated() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["id", "arr"]);
        let tree = lateral_func(
            "jsonb_array_elements(t.arr)",
            "elem",
            vec!["value"],
            false,
            child,
        );
        let result = diff_lateral_function(&mut ctx, &tree).unwrap();
        assert!(!result.is_deduplicated);
    }

    #[test]
    fn test_diff_lateral_function_error_on_non_lateral_node() {
        let mut ctx = test_ctx_with_st("public", "st");
        let tree = scan(1, "t", "public", "t", &["id"]);
        let result = diff_lateral_function(&mut ctx, &tree);
        assert!(result.is_err());
    }

    #[test]
    fn test_diff_lateral_function_hash_includes_all_columns() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["id", "data"]);
        let tree = lateral_func(
            "jsonb_array_elements(t.data)",
            "elem",
            vec!["value"],
            false,
            child,
        );
        let result = diff_lateral_function(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Row ID hash should include both child and SRF columns
        assert_sql_contains(&sql, "pg_trickle_hash");
    }

    #[test]
    fn test_lateral_function_output_columns_with_aliases() {
        let child = scan(1, "t", "public", "t", &["id", "data"]);
        let tree = lateral_func(
            "jsonb_array_elements(t.data)",
            "elem",
            vec!["value"],
            false,
            child,
        );
        assert_eq!(tree.output_columns(), vec!["id", "data", "value"],);
    }

    #[test]
    fn test_lateral_function_output_columns_no_aliases() {
        let child = scan(1, "t", "public", "t", &["id", "tags"]);
        let tree = lateral_func("unnest(t.tags)", "tag", vec![], false, child);
        assert_eq!(tree.output_columns(), vec!["id", "tags", "tag"]);
    }

    #[test]
    fn test_lateral_function_output_columns_infer_jsonb_each_defaults() {
        let child = scan(1, "t", "public", "t", &["id", "props"]);
        let tree = lateral_func("jsonb_each(t.props)", "kv", vec![], false, child);
        assert_eq!(tree.output_columns(), vec!["id", "props", "key", "value"]);
    }

    #[test]
    fn test_lateral_function_output_columns_infer_jsonb_array_defaults() {
        let child = scan(1, "t", "public", "t", &["id", "data"]);
        let tree = lateral_func("jsonb_array_elements(t.data)", "e", vec![], false, child);
        assert_eq!(tree.output_columns(), vec!["id", "data", "value"]);
    }

    #[test]
    fn test_lateral_function_output_columns_with_ordinality() {
        let child = scan(1, "t", "public", "t", &["id", "data"]);
        let tree = lateral_func(
            "jsonb_array_elements(t.data)",
            "elem",
            vec!["value"],
            true,
            child,
        );
        assert_eq!(
            tree.output_columns(),
            vec!["id", "data", "value", "ordinality"],
        );
    }

    #[test]
    fn test_lateral_function_source_oids() {
        let child = scan(42, "t", "public", "t", &["id", "data"]);
        let tree = lateral_func(
            "jsonb_array_elements(t.data)",
            "elem",
            vec!["value"],
            false,
            child,
        );
        assert_eq!(tree.source_oids(), vec![42]);
    }

    #[test]
    fn test_lateral_function_alias() {
        let child = scan(1, "t", "public", "t", &["id"]);
        let tree = lateral_func("unnest(t.tags)", "my_alias", vec![], false, child);
        assert_eq!(tree.alias(), "my_alias");
    }

    #[test]
    fn test_lateral_function_node_kind() {
        let child = scan(1, "t", "public", "t", &["id"]);
        let tree = lateral_func("unnest(t.tags)", "tag", vec![], false, child);
        assert_eq!(tree.node_kind(), "lateral function");
    }
}
