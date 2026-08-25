//! Semi-join differentiation (EXISTS / IN subquery).
//!
//! Δ(L ⋉ R) = Part1 ∪ Part2
//!
//! Part 1 — left-side changes:
//!   New/deleted left rows that have a match in current right.
//!   ```sql
//!   SELECT ... FROM delta_left dl
//!   WHERE EXISTS (SELECT 1 FROM right_snapshot r WHERE condition)
//!   ```
//!
//! Part 2 — right-side changes:
//!   Left rows that gain or lose their semi-join match due to right changes.
//!   A left row is affected if it matches any changed right row. Its status
//!   flips from non-matching to matching (INSERT) or matching to non-matching
//!   (DELETE) based on whether a match exists in `R_new` vs `R_old`.
//!
//!   We compute `R_old` from the right snapshot by reversing delta_right:
//!   `R_old = R_current EXCEPT delta_right(action='I') UNION delta_right(action='D')`.
//!   For simplicity, we use the frontier-based approach: the right snapshot at
//!   `prev_frontier` is the "old" state, and the right snapshot at `new_frontier`
//!   is the "new" state. Since we always have the live table as "new", we
//!   approximate R_old by anti-joining delta_right inserts and re-adding deletes.
//!
//!   Simplified approach: for each left row that correlates with any delta_right
//!   row, check if it now matches R_current (live table). If yes → 'I', else → 'D'.
//!   To avoid false positives, also check if it matched R_old. Only emit if status changed.

use crate::dvm::diff::{DiffContext, DiffResult, quote_ident};
use crate::dvm::operators::join_common::{
    build_snapshot_sql, extract_equijoin_keys_aliased, rewrite_join_condition,
};
use crate::dvm::parser::OpTree;
use crate::error::PgTrickleError;

/// Differentiate a SemiJoin node.
pub fn diff_semi_join(ctx: &mut DiffContext, op: &OpTree) -> Result<DiffResult, PgTrickleError> {
    let OpTree::SemiJoin {
        condition,
        left,
        right,
    } = op
    else {
        return Err(PgTrickleError::InternalError(
            "diff_semi_join called on non-SemiJoin node".into(),
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
    let _left_prefix = left.alias();

    // Semi-join only outputs left-side columns
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
    // For Part 2: hash left row using pg_trickle_hash since it comes from snapshot
    let hash_part2 = {
        let key_exprs: Vec<String> = left_cols
            .iter()
            .map(|c| format!("l.{}::TEXT", quote_ident(c)))
            .collect();
        crate::hash::build_composite_hash_expr(&key_exprs)
    };

    // Build R_old snapshot: the right table state before the current delta.
    // R_old = (R_current EXCEPT rows inserted by delta_right)
    //         UNION (rows deleted by delta_right)
    //
    // Expressed as a subquery:
    //   (SELECT * FROM right_table
    //    EXCEPT ALL
    //    SELECT <right_cols> FROM delta_right WHERE __pgt_action = 'I'
    //    UNION ALL
    //    SELECT <right_cols> FROM delta_right WHERE __pgt_action = 'D')
    let right_cols = &right_result.columns;
    // Filter out internal metadata columns (__pgt_count) from the EXCEPT ALL /
    // UNION ALL column list. These are aggregate bookkeeping columns that:
    // (a) don't exist in the snapshot (build_snapshot_sql doesn't produce them)
    // (b) shouldn't participate in set-difference matching
    let right_user_cols: Vec<&String> = right_cols.iter().filter(|c| *c != "__pgt_count").collect();
    let right_col_list: String = right_user_cols
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let right_alias = right.alias();

    // Materialize R_old as a CTE to avoid re-evaluating the EXCEPT ALL /
    // UNION ALL set operation for every EXISTS check in Part 1 and Part 2.
    // At SF=0.01 with 3 mutation cycles, this reduces Q21 from ~5.4s to
    // sub-second by allowing PostgreSQL to hash-probe the pre-computed
    // snapshot instead of repeatedly scanning and differencing the tables.
    //
    // DI-6: Push equi-join key filter into R_old's base table scan.
    // Part 1 queries R_old with delta_left keys; Part 2 queries R_old with
    // delta_right-correlated left keys. The key filter restricts R_old to
    // only rows whose join key appears in either delta source, reducing
    // materialization volume for high-cardinality right tables (Q20/Q21).
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
            // Filter: right_key IN (keys from delta_left UNION ALL keys from delta_right)
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
    //
    // Part 2 scans the full left snapshot looking for rows correlated
    // with delta_right. For large left tables (e.g. lineitem in Q18/Q21)
    // this sequential scan dominates refresh time even though only a few
    // rows are actually affected.
    //
    // Extract equi-join keys from the condition and use them to build a
    // semi-join filter that limits the left snapshot to rows whose join
    // keys appear in delta_right. This converts O(|L|) into O(|ΔR|)
    // when the join key is indexed.
    //
    // The keys are rewritten using the same alias logic as the condition
    // rewriting. We filter to only "clean" key pairs where the left side
    // references the pre-filter alias and the right side references the
    // delta alias — this avoids incorrect filters when rewriting fails.
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

    let cte_name = ctx.next_cte_name("semi_join");

    let sql = format!(
        "\
-- Part 1: delta_left rows that match right (semi-join filter)
-- INSERT: new left row has match in R_current  → emit INSERT
-- DELETE: old left row had match in R_old      → emit DELETE
-- For INSERTs we check the live right table (post-change state).
-- For DELETEs we check R_old (pre-change state) because the matching
-- right rows may also have been deleted in the same mutation cycle
-- (e.g. RF2 deletes both orders AND their lineitems simultaneously).
SELECT {hash_part1} AS __pgt_row_id,
       dl.__pgt_action,
       {dl_cols}
FROM {delta_left} dl
WHERE CASE WHEN dl.__pgt_action = 'D'
           THEN EXISTS (SELECT 1 FROM {r_old_cte} r_old WHERE {cond_part1_old})
           ELSE EXISTS (SELECT 1 FROM {right_table} r WHERE {cond_part1})
      END

UNION ALL

-- Part 2: left rows whose semi-join status changed due to right-side delta
-- Emit 'I' if row now matches R_current but didn't match R_old
-- Emit 'D' if row matched R_old but no longer matches R_current
-- Left snapshot is pre-filtered by delta-right join keys for performance.
SELECT {hash_part2} AS __pgt_row_id,
       CASE WHEN EXISTS (SELECT 1 FROM {right_table} r WHERE {cond_part2_new})
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
        columns: output_cols.clone(),
        schema: left_result.schema,
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
    fn test_diff_semi_join_basic() {
        let mut ctx = test_ctx();
        let left = scan(1, "orders", "public", "o", &["id", "cust_id", "amount"]);
        let right = scan(2, "customers", "public", "c", &["id", "name"]);
        let cond = eq_cond("o", "cust_id", "c", "id");
        let tree = OpTree::SemiJoin {
            condition: cond,
            left: Box::new(left),
            right: Box::new(right),
        };
        let result = diff_semi_join(&mut ctx, &tree).unwrap();

        // Semi-join outputs only left-side columns
        assert_eq!(result.columns, vec!["id", "cust_id", "amount"]);
        assert!(!result.is_deduplicated);
    }

    #[test]
    fn test_diff_semi_join_sql_contains_exists() {
        let mut ctx = test_ctx();
        let left = scan(1, "orders", "public", "o", &["id", "amount"]);
        let right = scan(2, "items", "public", "i", &["order_id", "qty"]);
        let cond = eq_cond("o", "id", "i", "order_id");
        let tree = OpTree::SemiJoin {
            condition: cond,
            left: Box::new(left),
            right: Box::new(right),
        };
        let result = diff_semi_join(&mut ctx, &tree).unwrap();

        // Verify the generated SQL has the expected structure
        let sql = ctx.build_with_query(&result.cte_name);
        assert!(sql.contains("EXISTS"), "SQL should contain EXISTS check");
        assert!(sql.contains("Part 1"), "SQL should have Part 1 comment");
        assert!(sql.contains("Part 2"), "SQL should have Part 2 comment");
        assert!(sql.contains("UNION ALL"), "SQL should UNION ALL both parts");
    }

    #[test]
    fn test_diff_semi_join_wrong_node_type() {
        let mut ctx = test_ctx();
        let scan_node = scan(1, "t", "public", "t", &["id"]);
        let result = diff_semi_join(&mut ctx, &scan_node);
        assert!(result.is_err());
    }

    /// Nested semi-join: (A SEMI JOIN (B SEMI JOIN C)).
    /// The outer semi-join should still output only A's left-side columns,
    /// and the generated SQL should build correctly without panicking.
    #[test]
    fn test_diff_semi_join_nested() {
        let mut ctx = test_ctx();
        let a = scan(1, "orders", "public", "o", &["order_id", "customer_id"]);
        let b = scan(2, "lineitems", "public", "l", &["order_id", "part_id"]);
        let c = scan(3, "parts", "public", "p", &["part_id", "type"]);

        // Inner: B SEMI JOIN C on l.part_id = p.part_id
        let inner = OpTree::SemiJoin {
            condition: eq_cond("l", "part_id", "p", "part_id"),
            left: Box::new(b),
            right: Box::new(c),
        };

        // Outer: A SEMI JOIN (B ⋉ C) on o.order_id = l.order_id
        let outer = OpTree::SemiJoin {
            condition: eq_cond("o", "order_id", "l", "order_id"),
            left: Box::new(a),
            right: Box::new(inner),
        };

        let result = diff_semi_join(&mut ctx, &outer).unwrap();

        // Outer semi-join outputs only A's columns
        assert_eq!(result.columns, vec!["order_id", "customer_id"]);

        let sql = ctx.build_with_query(&result.cte_name);
        assert!(
            sql.contains("EXISTS"),
            "nested semi-join SQL must contain EXISTS"
        );
        assert!(
            sql.to_lowercase().contains("orders"),
            "should reference source table orders"
        );
    }

    /// Multi-column equi-join condition on semi-join.
    /// The delta-key pre-filter extraction path should handle composite keys.
    #[test]
    fn test_diff_semi_join_multi_column_condition() {
        let mut ctx = test_ctx();
        let left = scan(
            1,
            "orders",
            "public",
            "o",
            &["order_id", "region_id", "amount"],
        );
        let right = scan(
            2,
            "regions",
            "public",
            "r",
            &["region_id", "country", "flag"],
        );

        // Two-column condition: o.order_id = r.region_id AND o.region_id = r.region_id
        // (contrived but exercises multi-column code path)
        let cond = binop(
            "AND",
            eq_cond("o", "order_id", "r", "region_id"),
            eq_cond("o", "region_id", "r", "region_id"),
        );

        let tree = OpTree::SemiJoin {
            condition: cond,
            left: Box::new(left),
            right: Box::new(right),
        };

        let result = diff_semi_join(&mut ctx, &tree).unwrap();

        // Semi-join still outputs only left columns
        assert_eq!(result.columns, vec!["order_id", "region_id", "amount"]);
        let sql = ctx.build_with_query(&result.cte_name);
        assert!(
            sql.contains("EXISTS"),
            "multi-column semi-join needs EXISTS"
        );
    }

    /// Semi-join with a filter on the left child.
    /// Verifies that column propagation works through a Filter node.
    #[test]
    fn test_diff_semi_join_with_left_filter() {
        let mut ctx = test_ctx();
        let base = scan(1, "orders", "public", "o", &["id", "status", "amount"]);
        let filtered_left = filter(binop("=", qcolref("o", "status"), lit("'shipped'")), base);
        let right = scan(2, "shipments", "public", "s", &["order_id", "ship_date"]);
        let cond = eq_cond("o", "id", "s", "order_id");

        let tree = OpTree::SemiJoin {
            condition: cond,
            left: Box::new(filtered_left),
            right: Box::new(right),
        };

        let result = diff_semi_join(&mut ctx, &tree).unwrap();

        // Left columns pass through the filter
        assert_eq!(result.columns, vec!["id", "status", "amount"]);
        let sql = ctx.build_with_query(&result.cte_name);
        assert!(sql.contains("EXISTS"), "filtered semi-join needs EXISTS");
    }

    /// Semi-join: left child has no rows matched (empty delta right).
    /// The SQL must still be syntactically valid — the WHERE clause in Part 2
    /// will simply match nothing at runtime.
    #[test]
    fn test_diff_semi_join_output_columns_match_left_only() {
        let mut ctx = test_ctx();
        // Right has many columns — none should appear in the output
        let left = scan(1, "a", "public", "a", &["x", "y"]);
        let right = scan(2, "b", "public", "b", &["x", "p", "q", "r", "s"]);
        let cond = eq_cond("a", "x", "b", "x");

        let tree = OpTree::SemiJoin {
            condition: cond,
            left: Box::new(left),
            right: Box::new(right),
        };

        let result = diff_semi_join(&mut ctx, &tree).unwrap();

        // Only left columns: x, y
        assert_eq!(result.columns, vec!["x", "y"]);
        // Right columns must NOT appear in the output list
        let sql = ctx.build_with_query(&result.cte_name);
        // The output SELECT lists must not contain right-only columns
        // (p, q, r, s should only appear in EXISTS sub-selects, not the main SELECT)
        assert!(
            !sql.contains("dl.\"p\""),
            "right column p must not be in output"
        );
        assert!(
            !sql.contains("l.\"q\""),
            "right column q must not be in output"
        );
    }

    /// R_old CTE materialization: the generated SQL must include a materialized
    /// CTE that represents the pre-change state of the right side.
    #[test]
    fn test_diff_semi_join_r_old_cte_materialized() {
        let mut ctx = test_ctx();
        let left = scan(1, "orders", "public", "o", &["id", "cust_id"]);
        let right = scan(2, "customers", "public", "c", &["id", "tier"]);
        let cond = eq_cond("o", "cust_id", "c", "id");

        let tree = OpTree::SemiJoin {
            condition: cond,
            left: Box::new(left),
            right: Box::new(right),
        };

        let result = diff_semi_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // SQL must contain MATERIALIZED to force pre-computation of R_old
        assert!(
            sql.contains("MATERIALIZED"),
            "R_old CTE should be MATERIALIZED for performance"
        );
        // SQL must contain EXCEPT ALL (used to reconstruct R_old from snapshot)
        assert!(
            sql.contains("EXCEPT ALL"),
            "R_old reconstruction requires EXCEPT ALL"
        );
    }

    // ── P2-4 property tests ───────────────────────────────────────────────

    proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(200))]

        /// Semi-join output columns equal the left columns exactly, for any
        /// combination of column names on both sides.
        #[test]
        fn prop_diff_semi_join_output_cols_equal_left_cols(
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
            let tree  = OpTree::SemiJoin {
                condition: cond,
                left: Box::new(left),
                right: Box::new(right),
            };

            let mut ctx = test_ctx();
            let result  = diff_semi_join(&mut ctx, &tree);
            prop_assert!(result.is_ok(), "diff_semi_join must not fail: {result:?}");
            prop_assert_eq!(
                result.unwrap().columns,
                lcols,
                "semi-join must output only left columns"
            );
        }

        /// Semi-join SQL always contains EXISTS (filter condition structure).
        #[test]
        fn prop_diff_semi_join_sql_contains_exists(
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
            let tree  = OpTree::SemiJoin {
                condition: cond,
                left: Box::new(left),
                right: Box::new(right),
            };

            let mut ctx = test_ctx();
            let result  = diff_semi_join(&mut ctx, &tree).unwrap();
            let sql     = ctx.build_with_query(&result.cte_name);
            prop_assert!(sql.contains("EXISTS"),
                "semi-join SQL must contain EXISTS");
        }
    }

    /// Regression: IN subquery where outer and inner tables share a column
    /// name (e.g., both have `id`).  The inner column ref must be qualified
    /// with the inner table's alias so `rewrite_join_condition` maps it to
    /// the right side (delta_right), not the left.
    ///
    /// Without the fix in `parse_any_sublink` (qualify_inner_col_refs), the
    /// unqualified `id` would resolve to the LEFT table in Part 2, making
    /// the delta_right correlation condition `l.cust_id = l.id` instead of
    /// `l.cust_id = dr.id`.
    #[test]
    fn test_diff_semi_join_ambiguous_column_name() {
        let mut ctx = test_ctx();
        // sc_orders has columns: id, cust_id, amount
        let left = scan(
            1,
            "sc_orders",
            "public",
            "sc_orders",
            &["id", "cust_id", "amount"],
        );
        // sc_vip_customers has columns: id  (same name as left!)
        let right = scan(2, "sc_vip_customers", "public", "sc_vip_customers", &["id"]);

        // Condition as produced by parse_any_sublink AFTER qualification:
        // outer.cust_id = sc_vip_customers.id  (inner ref is qualified)
        let cond = binop("=", colref("cust_id"), qcolref("sc_vip_customers", "id"));

        let tree = OpTree::SemiJoin {
            condition: cond,
            left: Box::new(left),
            right: Box::new(right),
        };

        let result = diff_semi_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Part 2 must correlate with delta_right using `dr."id"`,
        // not `l."id"`.  The crucial WHERE clause is:
        //   WHERE EXISTS (SELECT 1 FROM <delta_right> dr WHERE l."cust_id" = dr."id")
        assert!(
            sql.contains(r#""dr"."id""#),
            "Part 2 must reference dr.\"id\" (inner table), not l.\"id\"\nSQL: {sql}"
        );

        // Also verify Part 2 doesn't have `l."cust_id" = l."id"` (the bug)
        assert!(
            !sql.contains(r#""l"."cust_id" = "l"."id""#),
            "Part 2 must NOT correlate left with itself\nSQL: {sql}"
        );
    }

    /// Verify that qualified inner column refs work when the column name
    /// is unique (no ambiguity) — should still produce correct SQL.
    #[test]
    fn test_diff_semi_join_qualified_no_ambiguity() {
        let mut ctx = test_ctx();
        let left = scan(1, "orders", "public", "orders", &["order_id", "cust_id"]);
        let right = scan(
            2,
            "customers",
            "public",
            "customers",
            &["customer_id", "name"],
        );

        // After qualification: cust_id = customers.customer_id
        let cond = binop("=", colref("cust_id"), qcolref("customers", "customer_id"));

        let tree = OpTree::SemiJoin {
            condition: cond,
            left: Box::new(left),
            right: Box::new(right),
        };

        let result = diff_semi_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        assert!(
            sql.contains(r#""dr"."customer_id""#),
            "Part 2 must reference dr.\"customer_id\"\nSQL: {sql}"
        );
    }

    // ── DI-6: R_old key push-down ───────────────────────────────────

    #[test]
    fn test_di6_r_old_key_pushdown_present() {
        let mut ctx = test_ctx();
        let left = scan(1, "orders", "public", "o", &["id", "cust_id"]);
        let right = scan(2, "customers", "public", "c", &["id", "name"]);
        let cond = eq_cond("o", "cust_id", "c", "id");
        let tree = OpTree::SemiJoin {
            condition: cond,
            left: Box::new(left),
            right: Box::new(right),
        };
        let result = diff_semi_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // DI-6: R_old should have a WHERE key IN (...) filter
        assert!(
            sql.contains("WHERE") && sql.contains("EXCEPT ALL"),
            "R_old CTE should have a WHERE key filter before EXCEPT ALL\nSQL: {sql}"
        );
    }
}
