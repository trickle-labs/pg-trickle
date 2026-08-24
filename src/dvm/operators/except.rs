//! EXCEPT [ALL] differentiation.
//!
//! A row appears in the result when it exists in the left branch but
//! **not** (or with fewer multiplicity in) the right branch.
//!
//! - **EXCEPT** (set): row present if `count_L > 0 AND count_R = 0`.
//! - **EXCEPT ALL** (bag): row appears `GREATEST(0, count_L - count_R)` times.
//!
//! Delta strategy: same dual-count tracking as INTERSECT, but the
//! effective-count function is `GREATEST(0, count_L - count_R)`.

use crate::dvm::diff::{DiffContext, DiffResult, quote_ident};
use crate::dvm::operators::scan::build_hash_expr;
use crate::dvm::parser::OpTree;
use crate::dvm::schema::validate_set_operation;
use crate::error::PgTrickleError;

/// Differentiate an Except node.
pub fn diff_except(ctx: &mut DiffContext, op: &OpTree) -> Result<DiffResult, PgTrickleError> {
    let OpTree::Except {
        left, right, all, ..
    } = op
    else {
        return Err(PgTrickleError::InternalError(
            "diff_except called on non-Except node".into(),
        ));
    };

    // Differentiate both children
    let left_result = ctx.diff_node(left)?;
    let right_result = ctx.diff_node(right)?;
    validate_set_operation("EXCEPT", &left_result.schema, &right_result.schema)?;

    let cols = &left_result.columns;
    let col_refs: Vec<String> = cols.iter().map(|c| quote_ident(c)).collect();
    let col_list = col_refs.join(", ");

    // Hash all columns for the row_id
    let hash_exprs: Vec<String> = cols
        .iter()
        .map(|c| format!("{}::TEXT", quote_ident(c)))
        .collect();
    let row_id_expr = build_hash_expr(&hash_exprs);

    let st_table = ctx
        .st_qualified_name
        .clone()
        .unwrap_or_else(|| "/* st_table */".to_string());

    // CTE 1: Combine deltas from both branches, tagged with branch indicator
    let delta_cte = ctx.next_cte_name("exct_delta");
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
    let merge_cte = ctx.next_cte_name("exct_merge");
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
         LEFT JOIN {st_table} st ON st.__pgt_row_id = d.__pgt_row_id\n\
         GROUP BY d.__pgt_row_id, {d_cols}, st.__pgt_count_l, st.__pgt_count_r",
    );
    ctx.add_cte(merge_cte.clone(), merge_sql);

    // CTE 3: Detect boundary crossings.
    //
    // IMPORTANT: We never emit 'D' (DELETE) for EXCEPT/EXCEPT ALL.
    // Instead, when a row becomes invisible (effective count drops to 0),
    // we emit 'I' (UPDATE) with the new counts, keeping the row in the
    // ST.  This preserves the per-branch multiplicity counts so that
    // future deltas can correctly restore the row when the boundary
    // crosses back.  Deleting the row would lose count information,
    // causing stale state on subsequent refreshes.
    let final_cte = ctx.next_cte_name("exct_final");

    let final_sql = if *all {
        // EXCEPT ALL: effective count = GREATEST(0, count_L - count_R)
        format!(
            "\
-- Row appears: effective count was 0, now positive
SELECT __pgt_row_id, 'I' AS __pgt_action,
       {col_list}, new_count_l AS __pgt_count_l, new_count_r AS __pgt_count_r
FROM {merge_cte}
WHERE GREATEST(0, old_count_l - old_count_r) <= 0
  AND GREATEST(0, new_count_l - new_count_r) > 0

UNION ALL

-- Row becomes invisible: keep with updated counts (never delete)
SELECT __pgt_row_id, 'I' AS __pgt_action,
       {col_list}, new_count_l AS __pgt_count_l, new_count_r AS __pgt_count_r
FROM {merge_cte}
WHERE GREATEST(0, old_count_l - old_count_r) > 0
  AND GREATEST(0, new_count_l - new_count_r) <= 0

UNION ALL

-- Counts changed but row still present
SELECT __pgt_row_id, 'I' AS __pgt_action,
       {col_list}, new_count_l AS __pgt_count_l, new_count_r AS __pgt_count_r
FROM {merge_cte}
WHERE GREATEST(0, old_count_l - old_count_r) > 0
  AND GREATEST(0, new_count_l - new_count_r) > 0
  AND (new_count_l != old_count_l OR new_count_r != old_count_r)

UNION ALL

-- New row not yet visible but needs count tracking for future cycles.
-- Without this, a row simultaneously added to both branches is never
-- inserted into the ST, so later removals from the right branch
-- cannot restore it.
SELECT __pgt_row_id, 'I' AS __pgt_action,
       {col_list}, new_count_l AS __pgt_count_l, new_count_r AS __pgt_count_r
FROM {merge_cte}
WHERE old_count_l = 0 AND old_count_r = 0
  AND GREATEST(0, new_count_l - new_count_r) <= 0
  AND (new_count_l > 0 OR new_count_r > 0)",
        )
    } else {
        // EXCEPT (set): row present iff count_L > 0 AND count_R = 0.
        // This is true set-difference semantics: a value appears in the
        // result when it exists in the left branch and does NOT exist in
        // the right branch, regardless of multiplicities.
        format!(
            "\
-- Row appears: was absent (not in L or was in R), now in L and not in R
SELECT __pgt_row_id, 'I' AS __pgt_action,
       {col_list}, new_count_l AS __pgt_count_l, new_count_r AS __pgt_count_r
FROM {merge_cte}
WHERE NOT (old_count_l > 0 AND old_count_r = 0)
  AND (new_count_l > 0 AND new_count_r = 0)

UNION ALL

-- Row becomes invisible: keep with updated counts (never delete)
SELECT __pgt_row_id, 'I' AS __pgt_action,
       {col_list}, new_count_l AS __pgt_count_l, new_count_r AS __pgt_count_r
FROM {merge_cte}
WHERE (old_count_l > 0 AND old_count_r = 0)
  AND NOT (new_count_l > 0 AND new_count_r = 0)

UNION ALL

-- Counts changed but row still present (update stored counts)
SELECT __pgt_row_id, 'I' AS __pgt_action,
       {col_list}, new_count_l AS __pgt_count_l, new_count_r AS __pgt_count_r
FROM {merge_cte}
WHERE (old_count_l > 0 AND old_count_r = 0)
  AND (new_count_l > 0 AND new_count_r = 0)
  AND (new_count_l != old_count_l OR new_count_r != old_count_r)

UNION ALL

-- New row not yet visible but needs count tracking for future cycles.
-- When a value is simultaneously added to both branches, it is invisible
-- (count_r > 0) but the ST must still track both counts so that a later
-- DELETE from the right branch can make the row visible.
SELECT __pgt_row_id, 'I' AS __pgt_action,
       {col_list}, new_count_l AS __pgt_count_l, new_count_r AS __pgt_count_r
FROM {merge_cte}
WHERE old_count_l = 0 AND old_count_r = 0
  AND NOT (new_count_l > 0 AND new_count_r = 0)
  AND (new_count_l > 0 OR new_count_r > 0)",
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
    fn test_diff_except_basic() {
        let mut ctx = test_ctx_with_st("public", "my_st");
        let left = scan(1, "a", "public", "a", &["name"]);
        let right = scan(2, "b", "public", "b", &["name"]);
        let tree = except(left, right, false);
        let result = diff_except(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Output columns: user cols + dual counts
        assert!(result.columns.contains(&"name".to_string()));
        assert!(result.columns.contains(&"__pgt_count_l".to_string()));
        assert!(result.columns.contains(&"__pgt_count_r".to_string()));

        // Should contain the 3-CTE pattern
        assert_sql_contains(&sql, "exct_delta");
        assert_sql_contains(&sql, "exct_merge");
        assert_sql_contains(&sql, "exct_final");
    }

    #[test]
    fn test_diff_except_boundary_crossings() {
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["val"]);
        let right = scan(2, "b", "public", "b", &["val"]);
        let tree = except(left, right, false);
        let result = diff_except(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // EXCEPT (set) uses count_L > 0 AND count_R = 0 boundary
        assert_sql_contains(&sql, "old_count_l > 0 AND old_count_r = 0");
        assert_sql_contains(&sql, "new_count_l > 0 AND new_count_r = 0");
    }

    #[test]
    fn test_diff_except_all_basic() {
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["x"]);
        let right = scan(2, "b", "public", "b", &["x"]);
        let tree = except(left, right, true);
        let result = diff_except(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // EXCEPT ALL uses GREATEST like EXCEPT
        assert_sql_contains(&sql, "GREATEST(0, old_count_l - old_count_r)");
        assert_sql_contains(&sql, "GREATEST(0, new_count_l - new_count_r)");
    }

    #[test]
    fn test_diff_except_output_columns_include_dual_counts() {
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["id", "name"]);
        let right = scan(2, "b", "public", "b", &["id", "name"]);
        let tree = except(left, right, false);
        let result = diff_except(&mut ctx, &tree).unwrap();

        assert_eq!(
            result.columns,
            vec!["id", "name", "__pgt_count_l", "__pgt_count_r"]
        );
    }

    #[test]
    fn test_diff_except_is_not_commutative() {
        // EXCEPT is L - R, so swapping branches should produce different SQL
        let mut ctx1 = test_ctx_with_st("public", "st");
        let tree1 = except(
            scan(1, "a", "public", "a", &["x"]),
            scan(2, "b", "public", "b", &["x"]),
            false,
        );
        let r1 = diff_except(&mut ctx1, &tree1).unwrap();
        let sql1 = ctx1.build_with_query(&r1.cte_name);

        let mut ctx2 = test_ctx_with_st("public", "st");
        let tree2 = except(
            scan(2, "b", "public", "b", &["x"]),
            scan(1, "a", "public", "a", &["x"]),
            false,
        );
        let r2 = diff_except(&mut ctx2, &tree2).unwrap();
        let sql2 = ctx2.build_with_query(&r2.cte_name);

        // The left-branch CTE should differ (different tables)
        // Both should still have boundary-crossing logic
        assert_sql_contains(&sql1, "old_count_l > 0");
        assert_sql_contains(&sql2, "old_count_l > 0");
        // They should not be identical — different scan ordering
        assert_ne!(sql1, sql2);
    }

    #[test]
    fn test_diff_except_hash_row_id() {
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["x"]);
        let right = scan(2, "b", "public", "b", &["x"]);
        let tree = except(left, right, false);
        let result = diff_except(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        assert_sql_contains(&sql, "pgtrickle.pg_trickle_hash");
    }

    #[test]
    fn test_diff_except_not_deduplicated() {
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["x"]);
        let right = scan(2, "b", "public", "b", &["x"]);
        let tree = except(left, right, false);
        let result = diff_except(&mut ctx, &tree).unwrap();
        assert!(!result.is_deduplicated);
    }

    #[test]
    fn test_diff_except_error_on_non_except_node() {
        let mut ctx = test_ctx_with_st("public", "st");
        let tree = scan(1, "t", "public", "t", &["id"]);
        let result = diff_except(&mut ctx, &tree);
        assert!(result.is_err());
    }

    #[test]
    fn test_diff_except_branch_tagging() {
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["val"]);
        let right = scan(2, "b", "public", "b", &["val"]);
        let tree = except(left, right, false);
        let result = diff_except(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        assert_sql_contains(&sql, "'L' AS __branch");
        assert_sql_contains(&sql, "'R' AS __branch");
    }

    #[test]
    fn test_diff_except_multi_column() {
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["id", "name", "status"]);
        let right = scan(2, "b", "public", "b", &["id", "name", "status"]);
        let tree = except(left, right, false);
        let result = diff_except(&mut ctx, &tree).unwrap();
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
    fn test_diff_except_storage_table_join() {
        let mut ctx = test_ctx_with_st("myschema", "my_stream");
        let left = scan(1, "a", "public", "a", &["x"]);
        let right = scan(2, "b", "public", "b", &["x"]);
        let tree = except(left, right, false);
        let result = diff_except(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Merge CTE should join against the storage table
        assert_sql_contains(&sql, "\"myschema\".\"my_stream\"");
        assert_sql_contains(&sql, "st.__pgt_count_l");
        assert_sql_contains(&sql, "st.__pgt_count_r");
        assert_sql_contains(&sql, "LEFT JOIN");
    }

    #[test]
    fn test_diff_except_invisible_rows_keep_counts() {
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["x"]);
        let right = scan(2, "b", "public", "b", &["x"]);
        let tree = except(left, right, false);
        let result = diff_except(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Invisible rows keep their counts (never zero, never delete)
        // All actions are 'I' — no 'D' emitted
        assert_sql_contains(&sql, "'I' AS __pgt_action");
        // new counts are preserved even for invisible rows
        assert_sql_contains(
            &sql,
            "new_count_l AS __pgt_count_l, new_count_r AS __pgt_count_r",
        );
    }

    #[test]
    fn test_diff_except_all_update_on_count_change() {
        // EXCEPT ALL should emit an update (as INSERT) when counts change
        // but the row is still in the effective result
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["x"]);
        let right = scan(2, "b", "public", "b", &["x"]);
        let tree = except(left, right, true);
        let result = diff_except(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        assert_sql_contains(
            &sql,
            "new_count_l != old_count_l OR new_count_r != old_count_r",
        );
    }

    #[test]
    fn test_diff_except_net_count_aggregation() {
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "a", "public", "a", &["x"]);
        let right = scan(2, "b", "public", "b", &["x"]);
        let tree = except(left, right, false);
        let result = diff_except(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Delta CTE aggregates net counts: INSERT=+1, DELETE=-1
        assert_sql_contains(
            &sql,
            "SUM(CASE WHEN __pgt_action = 'I' THEN 1 ELSE -1 END) AS __net_count",
        );
    }

    #[test]
    fn test_diff_except_set_uses_exact_boundary_all_uses_greatest() {
        // Set EXCEPT uses count_L > 0 AND count_R = 0 boundary detection,
        // while EXCEPT ALL uses GREATEST(0, L-R) for bag semantics.
        let mut ctx_set = test_ctx_with_st("public", "st");
        let tree_set = except(
            scan(1, "a", "public", "a", &["x"]),
            scan(2, "b", "public", "b", &["x"]),
            false,
        );
        let r_set = diff_except(&mut ctx_set, &tree_set).unwrap();
        let sql_set = ctx_set.build_with_query(&r_set.cte_name);

        let mut ctx_all = test_ctx_with_st("public", "st");
        let tree_all = except(
            scan(1, "a", "public", "a", &["x"]),
            scan(2, "b", "public", "b", &["x"]),
            true,
        );
        let r_all = diff_except(&mut ctx_all, &tree_all).unwrap();
        let sql_all = ctx_all.build_with_query(&r_all.cte_name);

        // Set EXCEPT: uses exact count_L > 0 AND count_R = 0
        assert_sql_contains(&sql_set, "old_count_l > 0 AND old_count_r = 0");
        assert_sql_contains(&sql_set, "new_count_l > 0 AND new_count_r = 0");

        // EXCEPT ALL: uses GREATEST
        assert_sql_contains(&sql_all, "GREATEST(0, old_count_l - old_count_r)");
        assert_sql_contains(&sql_all, "GREATEST(0, new_count_l - new_count_r)");

        // Output columns are identical
        assert_eq!(r_set.columns, r_all.columns);
    }

    #[test]
    fn test_diff_except_left_branch_is_positive() {
        // Verify the left branch contributes positively (L count increases
        // the effective result) and right branch contributes negatively
        let mut ctx = test_ctx_with_st("public", "st");
        let left = scan(1, "orders", "public", "orders", &["id"]);
        let right = scan(2, "cancelled", "public", "cancelled", &["id"]);
        let tree = except(left, right, false);
        let result = diff_except(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // L branch feeds the positive side, R branch the negative side
        // in the set-difference boundary: count_L > 0 AND count_R = 0
        assert_sql_contains(&sql, "new_count_l");
        assert_sql_contains(&sql, "new_count_r");
        assert_sql_contains(&sql, "old_count_l");
        assert_sql_contains(&sql, "old_count_r");
    }
}
