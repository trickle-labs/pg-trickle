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
use crate::dvm::operators::scan::build_hash_expr_for_domain;
use crate::dvm::parser::OpTree;
use crate::dvm::schema::validate_set_operation;
use crate::error::PgTrickleError;

/// Differentiate an Intersect node.
pub fn diff_intersect(ctx: &mut DiffContext, op: &OpTree) -> Result<DiffResult, PgTrickleError> {
    let OpTree::Intersect {
        left, right, all, ..
    } = op
    else {
        return Err(PgTrickleError::InternalError(
            "diff_intersect called on non-Intersect node".into(),
        ));
    };

    // Differentiate both children
    let left_result = ctx.diff_node(left)?;
    let right_result = ctx.diff_node(right)?;
    validate_set_operation("INTERSECT", &left_result.schema, &right_result.schema)?;

    let cols = &left_result.columns;
    let col_refs: Vec<String> = cols.iter().map(|c| quote_ident(c)).collect();
    let col_list = col_refs.join(", ");

    // Hash all columns for the row_id
    let hash_exprs: Vec<String> = cols.iter().map(|c| quote_ident(c)).collect();
    let row_id_expr = build_hash_expr_for_domain("SET_KEY", &hash_exprs);

    let st_table = ctx
        .st_qualified_name
        .clone()
        .unwrap_or_else(|| "/* st_table */".to_string());

    // CTE 1: Combine deltas from both branches, tagged with branch indicator
    let delta_cte = ctx.next_cte_name("isect_delta");
    let delta_sql = format!(
        "SELECT {row_id_expr} AS __pgt_row_id, {col_list},\n\
         SUM(CASE WHEN __pgt_action = 'I' THEN 1 ELSE -1 END) AS __net_count,\n\
         'L' AS __branch\n\
         FROM {left_cte}\n\
         GROUP BY {col_list}\n\
         \n\
         UNION ALL\n\
         \n\
         SELECT {row_id_expr} AS __pgt_row_id, {col_list},\n\
         SUM(CASE WHEN __pgt_action = 'I' THEN 1 ELSE -1 END) AS __net_count,\n\
         'R' AS __branch\n\
         FROM {right_cte}\n\
         GROUP BY {col_list}",
        left_cte = left_result.cte_name,
        right_cte = right_result.cte_name,
    );
    ctx.add_cte(delta_cte.clone(), delta_sql);

    // CTE 2: Merge with ST storage to compute old and new per-branch counts
    let merge_cte = ctx.next_cte_name("isect_merge");
    let d_cols = cols
        .iter()
        .map(|c| format!("d.{}", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(", ");

    let merge_sql = format!(
        "SELECT d.__pgt_row_id, {d_cols},\n\
         COALESCE(st.__pgt_count_l, 0)\n\
             + SUM(CASE WHEN d.__branch = 'L' THEN d.__net_count ELSE 0 END) AS new_count_l,\n\
         COALESCE(st.__pgt_count_r, 0)\n\
             + SUM(CASE WHEN d.__branch = 'R' THEN d.__net_count ELSE 0 END) AS new_count_r,\n\
         COALESCE(st.__pgt_count_l, 0) AS old_count_l,\n\
         COALESCE(st.__pgt_count_r, 0) AS old_count_r\n\
         FROM {delta_cte} d\n\
         LEFT JOIN {st_table} st ON pgtrickle.row_probe_v1(st.__pgt_row_id) = pgtrickle.row_probe_v1(d.__pgt_row_id)\n\
             AND st.__pgt_row_id = d.__pgt_row_id\n\
         GROUP BY d.__pgt_row_id, {d_cols}, st.__pgt_count_l, st.__pgt_count_r",
    );
    ctx.add_cte(merge_cte.clone(), merge_sql);

    // CTE 3: Detect boundary crossings
    let final_cte = ctx.next_cte_name("isect_final");

    let final_sql = if *all {
        // INTERSECT ALL: emit rows based on LEAST(count_l, count_r) changes
        format!(
            "\
-- Row appears: min was 0, now positive
SELECT __pgt_row_id, 'I' AS __pgt_action,
       {col_list}, new_count_l AS __pgt_count_l, new_count_r AS __pgt_count_r
FROM {merge_cte}
WHERE LEAST(old_count_l, old_count_r) <= 0
  AND LEAST(new_count_l, new_count_r) > 0

UNION ALL

-- Row vanishes: min was positive, now 0
SELECT __pgt_row_id, 'D' AS __pgt_action,
       {col_list}, 0 AS __pgt_count_l, 0 AS __pgt_count_r
FROM {merge_cte}
WHERE LEAST(old_count_l, old_count_r) > 0
  AND LEAST(new_count_l, new_count_r) <= 0

UNION ALL

-- Counts changed but row still present
SELECT __pgt_row_id, 'I' AS __pgt_action,
       {col_list}, new_count_l AS __pgt_count_l, new_count_r AS __pgt_count_r
FROM {merge_cte}
WHERE LEAST(old_count_l, old_count_r) > 0
  AND LEAST(new_count_l, new_count_r) > 0
  AND (new_count_l != old_count_l OR new_count_r != old_count_r)",
        )
    } else {
        // INTERSECT (set): emit rows when min crosses the 0 boundary
        format!(
            "\
-- Row appears: was absent (min <= 0), now present (min > 0)
SELECT __pgt_row_id, 'I' AS __pgt_action,
       {col_list}, new_count_l AS __pgt_count_l, new_count_r AS __pgt_count_r
FROM {merge_cte}
WHERE LEAST(old_count_l, old_count_r) <= 0
  AND LEAST(new_count_l, new_count_r) > 0

UNION ALL

-- Row vanishes: was present (min > 0), now absent (min <= 0)
SELECT __pgt_row_id, 'D' AS __pgt_action,
       {col_list}, 0 AS __pgt_count_l, 0 AS __pgt_count_r
FROM {merge_cte}
WHERE LEAST(old_count_l, old_count_r) > 0
  AND LEAST(new_count_l, new_count_r) <= 0

UNION ALL

-- Counts changed but row still present (update stored counts)
SELECT __pgt_row_id, 'I' AS __pgt_action,
       {col_list}, new_count_l AS __pgt_count_l, new_count_r AS __pgt_count_r
FROM {merge_cte}
WHERE LEAST(old_count_l, old_count_r) > 0
  AND LEAST(new_count_l, new_count_r) > 0
  AND (new_count_l != old_count_l OR new_count_r != old_count_r)",
        )
    };
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
        is_deduplicated: false,
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

        // INSERT when min crosses 0→positive
        assert_sql_contains(&sql, "LEAST(old_count_l, old_count_r) <= 0");
        assert_sql_contains(&sql, "LEAST(new_count_l, new_count_r) > 0");
        // DELETE when min crosses positive→0
        assert_sql_contains(&sql, "LEAST(old_count_l, old_count_r) > 0");
        assert_sql_contains(&sql, "LEAST(new_count_l, new_count_r) <= 0");
    }

    #[test]
    fn test_diff_intersect_all_basic() {
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["x"]);
        let right = scan(2, "b", "public", "b", &["x"]);
        let tree = intersect(left, right, true);
        let result = diff_intersect(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // INTERSECT ALL uses LEAST like INTERSECT
        assert_sql_contains(&sql, "LEAST(old_count_l, old_count_r)");
        assert_sql_contains(&sql, "LEAST(new_count_l, new_count_r)");
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

        assert_sql_contains(&sql, "pgtrickle.encode_row_id_v2('SET_KEY'");
    }

    #[test]
    fn test_diff_intersect_not_deduplicated() {
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["x"]);
        let right = scan(2, "b", "public", "b", &["x"]);
        let tree = intersect(left, right, false);
        let result = diff_intersect(&mut ctx, &tree).unwrap();
        assert!(!result.is_deduplicated);
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
    fn test_diff_intersect_storage_table_join() {
        let mut ctx = test_ctx_with_st("myschema", "my_stream");
        let left = scan(1, "a", "public", "a", &["x"]);
        let right = scan(2, "b", "public", "b", &["x"]);
        let tree = intersect(left, right, false);
        let result = diff_intersect(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Merge CTE should join against the storage table
        assert_sql_contains(&sql, "\"myschema\".\"my_stream\"");
        assert_sql_contains(&sql, "st.__pgt_count_l");
        assert_sql_contains(&sql, "st.__pgt_count_r");
        assert_sql_contains(&sql, "LEFT JOIN");
    }

    #[test]
    fn test_diff_intersect_delete_action_zeros_counts() {
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["x"]);
        let right = scan(2, "b", "public", "b", &["x"]);
        let tree = intersect(left, right, false);
        let result = diff_intersect(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // DELETE rows should emit 0 for both counts
        assert_sql_contains(&sql, "'D' AS __pgt_action");
        assert_sql_contains(&sql, "0 AS __pgt_count_l, 0 AS __pgt_count_r");
    }

    #[test]
    fn test_diff_intersect_all_update_on_count_change() {
        // INTERSECT ALL should emit an update (as INSERT) when counts change
        // but the row is still present in both branches
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["x"]);
        let right = scan(2, "b", "public", "b", &["x"]);
        let tree = intersect(left, right, true);
        let result = diff_intersect(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // The "still present" arm checks for count changes
        assert_sql_contains(
            &sql,
            "new_count_l != old_count_l OR new_count_r != old_count_r",
        );
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
            "SUM(CASE WHEN __pgt_action = 'I' THEN 1 ELSE -1 END) AS __net_count",
        );
    }

    #[test]
    fn test_diff_intersect_set_and_all_produce_same_structure() {
        // Both INTERSECT and INTERSECT ALL use LEAST-based boundary detection
        let mut ctx_set = test_ctx_with_st("public", "st");
        let tree_set = intersect(
            scan(1, "a", "public", "a", &["x"]),
            scan(2, "b", "public", "b", &["x"]),
            false,
        );
        let r_set = diff_intersect(&mut ctx_set, &tree_set).unwrap();
        let sql_set = ctx_set.build_with_query(&r_set.cte_name);

        let mut ctx_all = test_ctx_with_st("public", "st");
        let tree_all = intersect(
            scan(1, "a", "public", "a", &["x"]),
            scan(2, "b", "public", "b", &["x"]),
            true,
        );
        let r_all = diff_intersect(&mut ctx_all, &tree_all).unwrap();
        let sql_all = ctx_all.build_with_query(&r_all.cte_name);

        // Both should have the same CTE structure
        assert_sql_contains(&sql_set, "isect_delta");
        assert_sql_contains(&sql_set, "isect_merge");
        assert_sql_contains(&sql_set, "isect_final");
        assert_sql_contains(&sql_all, "isect_delta");
        assert_sql_contains(&sql_all, "isect_merge");
        assert_sql_contains(&sql_all, "isect_final");

        // Both use LEAST (same effective-count function)
        assert_sql_contains(&sql_set, "LEAST(old_count_l, old_count_r)");
        assert_sql_contains(&sql_all, "LEAST(old_count_l, old_count_r)");

        // Output columns are identical
        assert_eq!(r_set.columns, r_all.columns);
    }
}
