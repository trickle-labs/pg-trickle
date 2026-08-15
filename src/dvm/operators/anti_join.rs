//! Anti-join differentiation (NOT EXISTS / NOT IN subquery).
//!
//! Δ(L ▷ R) = Part1 ∪ Part2
//!
//! Part 1 — left-side changes:
//!   New/deleted left rows that have NO match in current right.
//!   ```sql
//!   SELECT ... FROM delta_left dl
//!   WHERE NOT EXISTS (SELECT 1 FROM right_snapshot r WHERE condition)
//!   ```
//!
//! Part 2 — right-side changes:
//!   Left rows whose anti-join status flips due to right changes.
//!   A left row's status changes when it goes from having no match in R
//!   to having a match (DELETE from anti-join output), or vice versa (INSERT).
//!
//!   For each left row correlated with any delta_right row:
//!   - If NOT EXISTS in R_current AND EXISTS in R_old → INSERT (regained)
//!   - If EXISTS in R_current AND NOT EXISTS in R_old → DELETE (lost)

use crate::dvm::diff::{DiffContext, DiffResult, quote_ident};
use crate::dvm::operators::join_common::{
    build_snapshot_sql, extract_equijoin_keys_aliased, rewrite_join_condition,
};
use crate::dvm::parser::OpTree;
use crate::error::PgTrickleError;

/// Differentiate an AntiJoin node.
pub fn diff_anti_join(ctx: &mut DiffContext, op: &OpTree) -> Result<DiffResult, PgTrickleError> {
    let OpTree::AntiJoin {
        condition,
        left,
        right,
    } = op
    else {
        return Err(PgTrickleError::InternalError(
            "diff_anti_join called on non-AntiJoin node".into(),
        ));
    };

    // Differentiate both children.
    // Set inside_semijoin flag so inner joins within this subtree use L₁
    // (post-change snapshot) instead of L₀ via EXCEPT ALL, avoiding the
    // Q21-type numwait regression.
    let saved_inside_semijoin = ctx.inside_semijoin;
    ctx.inside_semijoin = true;
    let left_result = ctx.diff_node(left)?;
    let right_result = ctx.diff_node(right)?;
    ctx.inside_semijoin = saved_inside_semijoin;

    let right_table = build_snapshot_sql(right);

    // Rewrite join condition aliases for each part
    let cond_part1 = rewrite_join_condition(condition, left, "dl", right, "r");
    let cond_part1_old = rewrite_join_condition(condition, left, "dl", right, "r_old");
    let cond_part2_new = rewrite_join_condition(condition, left, "l", right, "r");
    let cond_part2_dr = rewrite_join_condition(condition, left, "l", right, "dr");
    let cond_part2_old = rewrite_join_condition(condition, left, "l", right, "r_old");

    let left_cols = &left_result.columns;

    // Anti-join only outputs left-side columns
    let output_cols: Vec<String> = left_cols.to_vec();

    let dl_col_refs: Vec<String> = left_cols
        .iter()
        .map(|c| format!("dl.{}", quote_ident(c)))
        .collect();

    let l_col_refs: Vec<String> = left_cols
        .iter()
        .map(|c| format!("l.{}", quote_ident(c)))
        .collect();

    // Row ID: passthrough from left side
    let hash_part1 = "dl.__pgt_row_id".to_string();
    // For Part 2: hash left row using pg_trickle_hash
    let hash_part2 = {
        let key_exprs: Vec<String> = left_cols
            .iter()
            .map(|c| format!("l.{}::TEXT", quote_ident(c)))
            .collect();
        crate::hash::build_composite_hash_expr(&key_exprs)
    };

    // Build R_old snapshot (same approach as semi_join)
    let right_cols = &right_result.columns;
    // Filter out internal metadata columns (__pgt_count) — see semi_join.rs
    let right_user_cols: Vec<&String> = right_cols.iter().filter(|c| *c != "__pgt_count").collect();
    let right_col_list: String = right_user_cols
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let right_alias = right.alias();

    // Materialize R_old as a CTE — same optimization as semi_join.rs.
    // Prevents re-evaluation of the EXCEPT ALL / UNION ALL per EXISTS check.
    //
    // DI-6: Push equi-join key filter into R_old's base table scan.
    // Same approach as semi_join.rs — restricts R_old to rows whose join
    // key appears in delta_left or delta_right, reducing materialization
    // volume for high-cardinality right tables.
    let r_old_equi_keys = extract_equijoin_keys_aliased(condition, left, "dl", right, right_alias);
    let r_old_key_filter = {
        let valid_keys: Vec<_> = r_old_equi_keys
            .iter()
            .filter(|(lk, rk)| {
                lk.starts_with("dl.") && rk.starts_with(&format!("\"{}\".", right_alias))
            })
            .collect();
        if valid_keys.is_empty() {
            String::new()
        } else {
            let filters: Vec<String> = valid_keys
                .iter()
                .map(|(left_key, right_key)| {
                    format!(
                        "{right_key} IN (\
                         SELECT {left_key} FROM {delta_left} dl \
                         UNION ALL \
                         SELECT {right_key} FROM {delta_right} dr)",
                        delta_left = left_result.cte_name,
                        delta_right = right_result.cte_name,
                    )
                })
                .collect();
            format!(" WHERE {}", filters.join(" AND "))
        }
    };

    let r_old_cte_name = ctx.next_cte_name("r_old");
    let r_old_sql = format!(
        "SELECT {right_col_list} FROM {right_table} {right_alias}{r_old_key_filter} \
         EXCEPT ALL \
         SELECT {right_col_list} FROM {delta_right} WHERE __pgt_action = 'I' \
         UNION ALL \
         SELECT {right_col_list} FROM {delta_right} WHERE __pgt_action = 'D'",
        delta_right = right_result.cte_name,
        right_alias = quote_ident(right_alias),
    );
    ctx.add_materialized_cte(r_old_cte_name.clone(), r_old_sql);

    // ── Delta-key pre-filtering for Part 2 ──────────────────────────
    // Same approach as semi_join.rs: extract equi-join keys to build a
    // WHERE ... IN (SELECT DISTINCT ... FROM delta_right) filter on the
    // left snapshot, reducing the Part 2 scan from O(|L|) to O(|ΔR|).
    let equi_keys_raw = extract_equijoin_keys_aliased(condition, left, "__pgt_pre", right, "dr");
    let equi_keys: Vec<_> = equi_keys_raw
        .into_iter()
        .filter(|(lk, rk)| lk.contains("__pgt_pre") && rk.starts_with("dr."))
        .collect();
    let left_snapshot_raw = build_snapshot_sql(left);
    let left_snapshot_filtered = if equi_keys.is_empty() {
        left_snapshot_raw
    } else {
        let filters: Vec<String> = equi_keys
            .iter()
            .map(|(left_key, right_key)| {
                format!(
                    "{left_key} IN (SELECT DISTINCT {right_key} FROM {} dr)",
                    right_result.cte_name
                )
            })
            .collect();
        format!(
            "(SELECT * FROM {left_snapshot_raw} \"__pgt_pre\" WHERE {filters})",
            filters = filters.join(" AND "),
        )
    };

    let cte_name = ctx.next_cte_name("anti_join");

    let sql = format!(
        "\
-- Part 1: delta_left rows filtered by anti-join
-- INSERT: new left row has NO match in R_current  → emit INSERT
-- DELETE: old left row had NO match in R_old       → emit DELETE
-- For DELETEs we check R_old (pre-change state) because the matching
-- right rows may also have been deleted in the same mutation cycle.
SELECT {hash_part1} AS __pgt_row_id,
       dl.__pgt_action,
       {dl_cols}
FROM {delta_left} dl
WHERE CASE WHEN dl.__pgt_action = 'D'
           THEN NOT EXISTS (SELECT 1 FROM {r_old_cte} r_old WHERE {cond_part1_old})
           ELSE NOT EXISTS (SELECT 1 FROM {right_table} r WHERE {cond_part1})
      END

UNION ALL

-- Part 2: left rows whose anti-join status changed due to right-side delta
-- Emit 'I' if row now has no match in R_current but had a match in R_old
-- Emit 'D' if row had no match in R_old but now has a match in R_current
-- Left snapshot is pre-filtered by delta-right join keys for performance.
SELECT {hash_part2} AS __pgt_row_id,
       CASE WHEN NOT EXISTS (SELECT 1 FROM {right_table} r WHERE {cond_part2_new})
            THEN 'I' ELSE 'D'
       END AS __pgt_action,
       {l_cols}
FROM {left_snapshot} l
WHERE EXISTS (SELECT 1 FROM {delta_right} dr WHERE {cond_part2_dr})
  AND (EXISTS (SELECT 1 FROM {right_table} r WHERE {cond_part2_new})
       <> EXISTS (SELECT 1 FROM {r_old_cte} r_old WHERE {cond_part2_old}))",
        dl_cols = dl_col_refs.join(", "),
        l_cols = l_col_refs.join(", "),
        delta_left = left_result.cte_name,
        delta_right = right_result.cte_name,
        left_snapshot = left_snapshot_filtered,
        right_table = right_table,
        r_old_cte = r_old_cte_name,
        cond_part1_old = cond_part1_old,
    );

    ctx.add_cte(cte_name.clone(), sql);

    Ok(DiffResult {
        cte_name,
        columns: output_cols,
        is_deduplicated: false,
        has_key_changed: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm::operators::test_helpers::*;
    use proptest::prelude::*;

    #[test]
    fn test_diff_anti_join_basic() {
        let mut ctx = test_ctx();
        let left = scan(1, "orders", "public", "o", &["id", "cust_id", "amount"]);
        let right = scan(2, "returns", "public", "r", &["order_id", "reason"]);
        let cond = eq_cond("o", "id", "r", "order_id");
        let tree = OpTree::AntiJoin {
            condition: cond,
            left: Box::new(left),
            right: Box::new(right),
        };
        let result = diff_anti_join(&mut ctx, &tree).unwrap();

        // Anti-join outputs only left-side columns
        assert_eq!(result.columns, vec!["id", "cust_id", "amount"]);
        assert!(!result.is_deduplicated);
    }

    #[test]
    fn test_diff_anti_join_sql_contains_not_exists() {
        let mut ctx = test_ctx();
        let left = scan(1, "orders", "public", "o", &["id", "amount"]);
        let right = scan(2, "returns", "public", "ret", &["order_id"]);
        let cond = eq_cond("o", "id", "ret", "order_id");
        let tree = OpTree::AntiJoin {
            condition: cond,
            left: Box::new(left),
            right: Box::new(right),
        };
        let result = diff_anti_join(&mut ctx, &tree).unwrap();

        let sql = ctx.build_with_query(&result.cte_name);
        assert!(
            sql.contains("NOT EXISTS"),
            "SQL should contain NOT EXISTS check"
        );
        assert!(sql.contains("Part 1"), "SQL should have Part 1 comment");
        assert!(sql.contains("Part 2"), "SQL should have Part 2 comment");
        assert!(sql.contains("UNION ALL"), "SQL should UNION ALL both parts");
    }

    #[test]
    fn test_diff_anti_join_wrong_node_type() {
        let mut ctx = test_ctx();
        let scan_node = scan(1, "t", "public", "t", &["id"]);
        let result = diff_anti_join(&mut ctx, &scan_node);
        assert!(result.is_err());
    }

    /// Nested anti-join: (A ANTI JOIN (B ANTI JOIN C)).
    /// Outer anti-join must still emit only A's columns.
    #[test]
    fn test_diff_anti_join_nested() {
        let mut ctx = test_ctx();
        let a = scan(1, "orders", "public", "o", &["order_id", "customer_id"]);
        let b = scan(2, "blacklist", "public", "bl", &["order_id", "reason"]);
        let c = scan(3, "exemptions", "public", "ex", &["reason", "level"]);

        // Inner: B ANTI JOIN C on bl.reason = ex.reason
        let inner = OpTree::AntiJoin {
            condition: eq_cond("bl", "reason", "ex", "reason"),
            left: Box::new(b),
            right: Box::new(c),
        };

        // Outer: A ANTI JOIN (B ▷ C) on o.order_id = bl.order_id
        let outer = OpTree::AntiJoin {
            condition: eq_cond("o", "order_id", "bl", "order_id"),
            left: Box::new(a),
            right: Box::new(inner),
        };

        let result = diff_anti_join(&mut ctx, &outer).unwrap();

        // Outer anti-join outputs only A's columns
        assert_eq!(result.columns, vec!["order_id", "customer_id"]);

        let sql = ctx.build_with_query(&result.cte_name);
        assert!(
            sql.contains("NOT EXISTS"),
            "nested anti-join SQL must contain NOT EXISTS"
        );
    }

    /// Multi-column condition for anti-join.
    #[test]
    fn test_diff_anti_join_multi_column_condition() {
        let mut ctx = test_ctx();
        let left = scan(1, "shipments", "public", "s", &["order_id", "wh_id", "qty"]);
        let right = scan(2, "holds", "public", "h", &["order_id", "wh_id", "reason"]);

        let cond = binop(
            "AND",
            eq_cond("s", "order_id", "h", "order_id"),
            eq_cond("s", "wh_id", "h", "wh_id"),
        );

        let tree = OpTree::AntiJoin {
            condition: cond,
            left: Box::new(left),
            right: Box::new(right),
        };

        let result = diff_anti_join(&mut ctx, &tree).unwrap();

        assert_eq!(result.columns, vec!["order_id", "wh_id", "qty"]);
        let sql = ctx.build_with_query(&result.cte_name);
        assert!(
            sql.contains("NOT EXISTS"),
            "multi-col anti-join needs NOT EXISTS"
        );
    }

    /// Anti-join with a filter on the right child (e.g. NOT IN active returns).
    #[test]
    fn test_diff_anti_join_with_right_filter() {
        let mut ctx = test_ctx();
        let left = scan(1, "orders", "public", "o", &["id", "value"]);
        let base_right = scan(2, "returns", "public", "r", &["order_id", "status"]);
        let filtered_right = filter(
            binop("=", qcolref("r", "status"), lit("'active'")),
            base_right,
        );
        let cond = eq_cond("o", "id", "r", "order_id");

        let tree = OpTree::AntiJoin {
            condition: cond,
            left: Box::new(left),
            right: Box::new(filtered_right),
        };

        let result = diff_anti_join(&mut ctx, &tree).unwrap();

        assert_eq!(result.columns, vec!["id", "value"]);
        let sql = ctx.build_with_query(&result.cte_name);
        assert!(
            sql.contains("NOT EXISTS"),
            "filtered anti-join needs NOT EXISTS"
        );
    }

    /// Anti-join output must contain only left columns — right columns must
    /// never appear in the output SELECT list.
    #[test]
    fn test_diff_anti_join_output_columns_match_left_only() {
        let mut ctx = test_ctx();
        let left = scan(1, "a", "public", "a", &["x", "y"]);
        let right = scan(2, "b", "public", "b", &["x", "p", "q", "r", "s"]);
        let cond = eq_cond("a", "x", "b", "x");

        let tree = OpTree::AntiJoin {
            condition: cond,
            left: Box::new(left),
            right: Box::new(right),
        };

        let result = diff_anti_join(&mut ctx, &tree).unwrap();

        assert_eq!(result.columns, vec!["x", "y"]);
        let sql = ctx.build_with_query(&result.cte_name);
        // Right-only columns must not be in the main SELECT output
        assert!(
            !sql.contains("dl.\"p\""),
            "right column p must not be in output"
        );
        assert!(
            !sql.contains("l.\"q\""),
            "right column q must not be in output"
        );
    }

    /// R_old CTE must be materialized to prevent redundant re-evaluation.
    #[test]
    fn test_diff_anti_join_r_old_cte_materialized() {
        let mut ctx = test_ctx();
        let left = scan(1, "orders", "public", "o", &["id", "cust_id"]);
        let right = scan(2, "cancelled", "public", "c", &["order_id", "ts"]);
        let cond = eq_cond("o", "id", "c", "order_id");

        let tree = OpTree::AntiJoin {
            condition: cond,
            left: Box::new(left),
            right: Box::new(right),
        };

        let result = diff_anti_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        assert!(
            sql.contains("MATERIALIZED"),
            "R_old CTE should be MATERIALIZED for performance"
        );
        assert!(
            sql.contains("EXCEPT ALL"),
            "R_old reconstruction requires EXCEPT ALL"
        );
    }

    /// Semi-join and anti-join semantics are complementary: given the same
    /// inputs, a semi-join emits rows WITH a match and an anti-join emits rows
    /// WITHOUT. Verify the SQL structure reflects this (EXISTS vs NOT EXISTS).
    #[test]
    fn test_semi_and_anti_join_are_complementary() {
        use crate::dvm::operators::semi_join::diff_semi_join;

        let make_tree = |is_anti: bool| {
            let left = scan(1, "orders", "public", "o", &["id", "cust"]);
            let right = scan(2, "returns", "public", "r", &["order_id"]);
            let cond = eq_cond("o", "id", "r", "order_id");
            if is_anti {
                OpTree::AntiJoin {
                    condition: cond,
                    left: Box::new(left),
                    right: Box::new(right),
                }
            } else {
                OpTree::SemiJoin {
                    condition: cond,
                    left: Box::new(left),
                    right: Box::new(right),
                }
            }
        };

        let mut ctx_anti = test_ctx();
        let anti_result = diff_anti_join(&mut ctx_anti, &make_tree(true)).unwrap();
        let anti_sql = ctx_anti.build_with_query(&anti_result.cte_name);

        let mut ctx_semi = test_ctx();
        let semi_result = diff_semi_join(&mut ctx_semi, &make_tree(false)).unwrap();
        let semi_sql = ctx_semi.build_with_query(&semi_result.cte_name);

        // Both output same columns (left side only)
        assert_eq!(anti_result.columns, semi_result.columns);
        // Anti uses NOT EXISTS, semi uses EXISTS (without NOT)
        assert!(anti_sql.contains("NOT EXISTS"));
        // Semi SQL should have EXISTS but the Part 1 filter should NOT have NOT EXISTS
        assert!(semi_sql.contains("EXISTS"));
        assert!(
            !semi_sql.contains("NOT EXISTS"),
            "semi-join Part 1 must use EXISTS, not NOT EXISTS"
        );
    }

    // ── P2-4 property tests ───────────────────────────────────────────────

    proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(200))]

        /// Anti-join output columns equal the left columns exactly, for any
        /// combination of column names on both sides.
        #[test]
        fn prop_diff_anti_join_output_cols_equal_left_cols(
            left_cols in proptest::collection::vec("[a-z]{2,6}", 1..4usize),
            right_cols in proptest::collection::vec("[a-z]{2,6}", 1..4usize),
        ) {
            let lcols: Vec<String> = left_cols.iter().enumerate()
                .map(|(i, c)| format!("lc{i}_{c}")).collect();
            let rcols: Vec<String> = right_cols.iter().enumerate()
                .map(|(i, c)| format!("rc{i}_{c}")).collect();
            let lrefs: Vec<&str> = lcols.iter().map(|s| s.as_str()).collect();
            let rrefs: Vec<&str> = rcols.iter().map(|s| s.as_str()).collect();

            let left  = scan(1, "left_t",  "public", "l", &lrefs);
            let right = scan(2, "right_t", "public", "r", &rrefs);
            let cond  = eq_cond("l", &lcols[0], "r", &rcols[0]);
            let tree  = OpTree::AntiJoin {
                condition: cond,
                left: Box::new(left),
                right: Box::new(right),
            };

            let mut ctx = test_ctx();
            let result  = diff_anti_join(&mut ctx, &tree);
            prop_assert!(result.is_ok(), "diff_anti_join must not fail: {result:?}");
            prop_assert_eq!(
                result.unwrap().columns,
                lcols,
                "anti-join must output only left columns"
            );
        }

        /// Anti-join SQL always contains NOT EXISTS (negation filter).
        #[test]
        fn prop_diff_anti_join_sql_contains_not_exists(
            left_cols in proptest::collection::vec("[a-z]{2,6}", 1..3usize),
            right_cols in proptest::collection::vec("[a-z]{2,6}", 1..3usize),
        ) {
            let lcols: Vec<String> = left_cols.iter().enumerate()
                .map(|(i, c)| format!("lc{i}_{c}")).collect();
            let rcols: Vec<String> = right_cols.iter().enumerate()
                .map(|(i, c)| format!("rc{i}_{c}")).collect();
            let lrefs: Vec<&str> = lcols.iter().map(|s| s.as_str()).collect();
            let rrefs: Vec<&str> = rcols.iter().map(|s| s.as_str()).collect();

            let left  = scan(1, "left_t",  "public", "l", &lrefs);
            let right = scan(2, "right_t", "public", "r", &rrefs);
            let cond  = eq_cond("l", &lcols[0], "r", &rcols[0]);
            let tree  = OpTree::AntiJoin {
                condition: cond,
                left: Box::new(left),
                right: Box::new(right),
            };

            let mut ctx = test_ctx();
            let result  = diff_anti_join(&mut ctx, &tree).unwrap();
            let sql     = ctx.build_with_query(&result.cte_name);
            prop_assert!(sql.contains("NOT EXISTS"),
                "anti-join SQL must contain NOT EXISTS");
        }
    }
}
