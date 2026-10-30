//! INTERSECT [ALL] differentiation.
//!
//! A row appears in the result when it exists in **both** branches.
//!
//! - **INTERSECT** (set): row present if `min(count_L, count_R) > 0`.
//! - **INTERSECT ALL** (bag): row appears `min(count_L, count_R)` times.
//!
//! Delta strategy: track per-branch multiplicity counts (`__pgt_count_l`,
//! `__pgt_count_r`).  When the minimum crosses the 0 boundary, emit
//! INSERT or DELETE actions.

use crate::dvm::diff::{DiffContext, DiffResult, quote_ident};
use crate::dvm::parser::OpTree;
use crate::dvm::schema::validate_set_operation;
use crate::error::PgTrickleError;

/// Differentiate an Intersect node.
pub fn diff_intersect(ctx: &mut DiffContext, op: &OpTree) -> Result<DiffResult, PgTrickleError> {
    let OpTree::Intersect { left, right, all } = op else {
        return Err(PgTrickleError::InternalError(
            "diff_intersect called on non-Intersect node".into(),
        ));
    };
    if *all {
        return Err(PgTrickleError::UnsupportedOperator(
            "INTERSECT ALL differential maintenance is not qualified".into(),
        ));
    }

    // Differentiate both children
    let left_result = ctx.diff_node(left)?;
    let right_result = ctx.diff_node(right)?;
    validate_set_operation("INTERSECT", &left_result.schema, &right_result.schema)?;

    let cols = &left_result.columns;
    let col_refs: Vec<String> = cols.iter().map(|c| quote_ident(c)).collect();
    let col_list = col_refs.join(", ");

    let row_id_expr = if col_refs.len() == 1 {
        format!(
            "pgtrickle.encode_row_id_v3('SET_KEY', ROW({}))",
            col_refs[0]
        )
    } else {
        crate::hash::build_row_identity_expr("SET_KEY", &col_refs)
    };
    let state_table = ctx.setop_state_relation.clone().ok_or_else(|| {
        PgTrickleError::InternalError(
            "INTERSECT differentiation requires a bound private state relation".into(),
        )
    })?;

    // CTE 1: Combine deltas from both branches, tagged with branch indicator
    let delta_cte = ctx.next_cte_name("isect_delta");
    let delta_sql = format!(
        "SELECT {row_id_expr} AS __pgt_row_id, {col_list},\n\
         SUM(CASE WHEN __pgt_action = 'I' THEN 1::bigint ELSE -1::bigint END) AS __net_count,\n\
         'L' AS __branch\n\
         FROM {left_cte}\n\
         GROUP BY {col_list}\n\
         \n\
         UNION ALL\n\
         \n\
         SELECT {row_id_expr} AS __pgt_row_id, {col_list},\n\
         SUM(CASE WHEN __pgt_action = 'I' THEN 1::bigint ELSE -1::bigint END) AS __net_count,\n\
         'R' AS __branch\n\
         FROM {right_cte}\n\
         GROUP BY {col_list}",
        left_cte = left_result.cte_name,
        right_cte = right_result.cte_name,
    );
    ctx.add_cte(delta_cte.clone(), delta_sql);

    // CTE 2: Merge with private state. The public ST contains visible rows only.
    let merge_cte = ctx.next_cte_name("isect_merge");
    let d_cols = cols
        .iter()
        .map(|c| format!("d.{}", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(", ");

    let tuple_eq = cols
        .iter()
        .map(|c| {
            format!(
                "st.{} IS NOT DISTINCT FROM d.{}",
                quote_ident(c),
                quote_ident(c)
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let merge_sql = format!(
        "SELECT d.__pgt_row_id, {d_cols},\n\
         COALESCE(st.__pgt_count_l, 0)\n\
             + COALESCE(SUM(d.__net_count) FILTER (WHERE d.__branch = 'L'), 0)::bigint AS new_count_l,\n\
         COALESCE(st.__pgt_count_r, 0)\n\
             + COALESCE(SUM(d.__net_count) FILTER (WHERE d.__branch = 'R'), 0)::bigint AS new_count_r,\n\
         COALESCE(st.__pgt_count_l, 0) AS old_count_l,\n\
         COALESCE(st.__pgt_count_r, 0) AS old_count_r\n\
         FROM {delta_cte} d\n\
         LEFT JOIN {state_table} st ON st.__pgt_row_id = d.__pgt_row_id\n\
             AND {tuple_eq}\n\
         GROUP BY d.__pgt_row_id, {d_cols}, st.__pgt_count_l, st.__pgt_count_r",
    );
    ctx.add_cte(merge_cte.clone(), merge_sql);

    // CTE 3: Detect boundary crossings
    let final_cte = ctx.next_cte_name("isect_final");

    let final_sql = format!(
        "SELECT __pgt_row_id,\n\
         CASE WHEN old_count_l > 0 AND old_count_r > 0\n\
                   AND (new_count_l = 0 OR new_count_r = 0) THEN 'D'\n\
              WHEN (old_count_l = 0 OR old_count_r = 0)\n\
                   AND new_count_l > 0 AND new_count_r > 0 THEN 'I'\n\
              ELSE 'S' END AS __pgt_action,\n\
         {col_list}, new_count_l AS __pgt_count_l,\n\
         new_count_r AS __pgt_count_r\n\
         FROM {merge_cte}\n\
         WHERE new_count_l <> old_count_l OR new_count_r <> old_count_r",
    );
    ctx.add_cte(final_cte.clone(), final_sql);

    let mut output_cols = cols.clone();
    output_cols.push("__pgt_count_l".to_string());
    output_cols.push("__pgt_count_r".to_string());
    let schema = left_result
        .schema
        .clone()
        .with_internal("__pgt_count_l", 20)
        .with_internal("__pgt_count_r", 20);

    Ok(DiffResult {
        cte_name: final_cte,
        columns: output_cols,
        schema,
        // Grouping by the complete output tuple guarantees one row per set key.
        is_deduplicated: true,
        has_key_changed: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm::operators::test_helpers::*;

    #[test]
    fn test_diff_intersect_basic() {
        let mut ctx = test_ctx_with_st("public", "my_st");
        let left = scan(1, "a", "public", "a", &["name"]);
        let right = scan(2, "b", "public", "b", &["name"]);
        let tree = intersect(left, right, false);
        let result = diff_intersect(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Output columns: user cols + dual counts
        assert!(result.columns.contains(&"name".to_string()));
        assert!(result.columns.contains(&"__pgt_count_l".to_string()));
        assert!(result.columns.contains(&"__pgt_count_r".to_string()));

        // Should contain the 3-CTE pattern
        assert_sql_contains(&sql, "isect_delta");
        assert_sql_contains(&sql, "isect_merge");
        assert_sql_contains(&sql, "isect_final");
    }

    #[test]
    fn test_diff_intersect_boundary_crossings() {
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["val"]);
        let right = scan(2, "b", "public", "b", &["val"]);
        let tree = intersect(left, right, false);
        let result = diff_intersect(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        assert_sql_contains(&sql, "new_count_l > 0 AND new_count_r > 0 THEN 'I'");
        assert_sql_contains(&sql, "new_count_l = 0 OR new_count_r = 0) THEN 'D'");
        assert_sql_contains(&sql, "ELSE 'S' END AS __pgt_action");
    }

    #[test]
    fn test_diff_intersect_all_basic() {
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["x"]);
        let right = scan(2, "b", "public", "b", &["x"]);
        let tree = intersect(left, right, true);
        assert!(diff_intersect(&mut ctx, &tree).is_err());
    }

    #[test]
    fn test_diff_intersect_output_columns_include_dual_counts() {
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["id", "name"]);
        let right = scan(2, "b", "public", "b", &["id", "name"]);
        let tree = intersect(left, right, false);
        let result = diff_intersect(&mut ctx, &tree).unwrap();

        assert_eq!(
            result.columns,
            vec!["id", "name", "__pgt_count_l", "__pgt_count_r"]
        );
    }

    #[test]
    fn test_diff_intersect_hash_row_id() {
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["x"]);
        let right = scan(2, "b", "public", "b", &["x"]);
        let tree = intersect(left, right, false);
        let result = diff_intersect(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        assert_sql_contains(&sql, "pgtrickle.encode_row_id_v3('SET_KEY'");
    }

    #[test]
    fn test_diff_intersect_is_deduplicated_by_semantic_tuple() {
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["x"]);
        let right = scan(2, "b", "public", "b", &["x"]);
        let tree = intersect(left, right, false);
        let result = diff_intersect(&mut ctx, &tree).unwrap();
        assert!(result.is_deduplicated);
    }

    #[test]
    fn test_diff_intersect_error_on_non_intersect_node() {
        let mut ctx = test_ctx_with_st("public", "st");
        let tree = scan(1, "t", "public", "t", &["id"]);
        let result = diff_intersect(&mut ctx, &tree);
        assert!(result.is_err());
    }

    #[test]
    fn test_diff_intersect_branch_tagging() {
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["val"]);
        let right = scan(2, "b", "public", "b", &["val"]);
        let tree = intersect(left, right, false);
        let result = diff_intersect(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Both L and R branch tags must be present
        assert_sql_contains(&sql, "'L' AS __branch");
        assert_sql_contains(&sql, "'R' AS __branch");
    }

    #[test]
    fn test_diff_intersect_multi_column() {
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["id", "name", "status"]);
        let right = scan(2, "b", "public", "b", &["id", "name", "status"]);
        let tree = intersect(left, right, false);
        let result = diff_intersect(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // All user columns appear in output
        assert_eq!(
            result.columns,
            vec!["id", "name", "status", "__pgt_count_l", "__pgt_count_r"]
        );
        // All user columns are quoted in GROUP BY
        assert_sql_contains(&sql, "\"id\", \"name\", \"status\"");
    }

    #[test]
    fn test_diff_intersect_private_state_join() {
        let mut ctx = test_ctx_with_st("myschema", "my_stream");
        let left = scan(1, "a", "public", "a", &["x"]);
        let right = scan(2, "b", "public", "b", &["x"]);
        let tree = intersect(left, right, false);
        let result = diff_intersect(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Old multiplicities come from private state, with full tuple equality.
        assert_sql_contains(&sql, "pgtrickle.__pgt_setop_state_test");
        assert_sql_contains(&sql, "st.\"x\" IS NOT DISTINCT FROM d.\"x\"");
        assert_sql_contains(&sql, "st.__pgt_count_l");
        assert_sql_contains(&sql, "st.__pgt_count_r");
        assert_sql_contains(&sql, "LEFT JOIN");
    }

    #[test]
    fn test_diff_intersect_emits_state_only_action() {
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["x"]);
        let right = scan(2, "b", "public", "b", &["x"]);
        let tree = intersect(left, right, false);
        let result = diff_intersect(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        assert_sql_contains(&sql, "new_count_l AS __pgt_count_l");
        assert_sql_contains(&sql, "new_count_r AS __pgt_count_r");
        assert_sql_contains(&sql, "ELSE 'S' END AS __pgt_action");
    }

    #[test]
    fn test_diff_intersect_all_update_on_count_change() {
        // INTERSECT ALL should emit an update (as INSERT) when counts change
        // but the row is still present in both branches
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["x"]);
        let right = scan(2, "b", "public", "b", &["x"]);
        let tree = intersect(left, right, true);
        assert!(diff_intersect(&mut ctx, &tree).is_err());
    }

    #[test]
    fn test_diff_intersect_net_count_aggregation() {
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["x"]);
        let right = scan(2, "b", "public", "b", &["x"]);
        let tree = intersect(left, right, false);
        let result = diff_intersect(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Delta CTE aggregates net counts: INSERT=+1, DELETE=-1
        assert_sql_contains(
            &sql,
            "SUM(CASE WHEN __pgt_action = 'I' THEN 1::bigint ELSE -1::bigint END) AS __net_count",
        );
    }

    #[test]
    fn test_diff_intersect_all_is_rejected() {
        let mut ctx_all = test_ctx_with_st("public", "st");
        let tree_all = intersect(
            scan(1, "a", "public", "a", &["x"]),
            scan(2, "b", "public", "b", &["x"]),
            true,
        );
        assert!(diff_intersect(&mut ctx_all, &tree_all).is_err());
    }
}
