//! Scalar subquery differentiation.
//!
//! Handles `SELECT (SELECT agg(...) FROM inner_src) AS alias, ... FROM outer_src`.
//!
//! The scalar subquery produces a single value that is effectively cross-joined
//! to every row from the outer child. When the inner source changes, ALL output
//! rows change (the scalar value is different). When the outer source changes,
//! only the changed rows are affected (the scalar value stays the same).
//!
//! ## Delta strategy (DBSP cross-product formula)
//!
//! ```text
//! Δ(C × S) = (ΔC × S₁) + (C₀ × ΔS)
//! ```
//!
//! **Part 1 — outer child changes** (`ΔC × S₁`):
//!   Pass through delta_outer rows, appending the current scalar value (`S₁`).
//!
//! **Part 2 — inner source changes** (`C₀ × ΔS`):
//!   For each row in the pre-change outer child (`C₀`), emit DELETE with the
//!   old scalar value (`S₀`) and INSERT with the new scalar value (`S₁`).
//!
//! Using `C₀` (pre-change) instead of `C₁` (post-change) in Part 2 is
//! critical when the outer child and inner subquery share a common source
//! table (e.g., Q15: both revenue0 and MAX(total_revenue) depend on
//! lineitem). Using `C₁` would double-count `ΔC × ΔS` overlap rows and
//! miss DELETE rows whose column values changed between cycles.
//!
//! `C₀` is reconstructed as:
//! `C_current EXCEPT ALL Δ_inserts UNION ALL Δ_deletes`
//!
//! Optimization: if only the outer source changed and the inner source is
//! stable, Part 2 is skipped entirely. The diff engine already handles this
//! because diff_node on a stable subtree produces zero delta rows.

use crate::dvm::diff::{DiffContext, DiffResult, quote_ident};
use crate::dvm::operators::join_common::build_snapshot_sql;
use crate::dvm::operators::scan::build_hash_expr_for_domain;
use crate::dvm::parser::OpTree;
use crate::error::PgTrickleError;

/// Differentiate a ScalarSubquery node.
pub fn diff_scalar_subquery(
    ctx: &mut DiffContext,
    op: &OpTree,
) -> Result<DiffResult, PgTrickleError> {
    let OpTree::ScalarSubquery {
        subquery,
        alias,
        child,
        ..
    } = op
    else {
        return Err(PgTrickleError::InternalError(
            "diff_scalar_subquery called on non-ScalarSubquery node".into(),
        ));
    };

    // Differentiate both the outer child and the inner subquery
    let child_result = ctx.diff_node(child)?;
    let subquery_result = ctx.diff_node(subquery)?;

    let child_cols = &child_result.columns;
    let child_table = build_snapshot_sql(child);

    // Output columns: child columns + scalar alias
    let mut output_cols = child_cols.clone();
    output_cols.push(alias.clone());

    let dc_col_refs: Vec<String> = child_cols
        .iter()
        .map(|c| format!("dc.{}", quote_ident(c)))
        .collect();

    let cs_col_refs: Vec<String> = child_cols
        .iter()
        .map(|c| format!("cs.{}", quote_ident(c)))
        .collect();

    // Build the scalar subquery SQL that computes the current value
    // The subquery is an aggregate, so we reconstruct it from the subquery's snapshot
    let subquery_snapshot = build_snapshot_sql(subquery);
    let subquery_alias = subquery.alias();
    let subquery_cols = &subquery_result.columns;
    if subquery_cols.is_empty() {
        return Err(PgTrickleError::UnsupportedOperator(format!(
            "scalar subquery '{}' produced no output columns; \
             unable to differentiate — wrap it in a CTE or derived table",
            alias
        )));
    }
    let scalar_col = subquery_cols[0].clone();

    let scalar_sql = format!(
        "(SELECT {sq_alias}.{scalar_col} FROM {subquery_snapshot} {sq_alias})",
        scalar_col = quote_ident(&scalar_col),
        sq_alias = quote_ident(subquery_alias),
    );

    let cte_name = ctx.next_cte_name("scalar_sub");

    // Part 1: outer child delta with current scalar value appended
    // Part 2: if inner subquery changed, all outer rows with new vs old scalar
    //
    // For Part 2, we need both old and new scalar values.
    // Old scalar = computed from R_old (before changes).
    // New scalar = computed from R_current (after changes).
    // We emit a DELETE for each outer row (old scalar) and INSERT (new scalar).
    //
    // Build R_old for the scalar subquery
    let sq_col_list: String = subquery_cols
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");

    let scalar_old_sql = {
        let r_old_snapshot = format!(
            "(SELECT {sq_col_list} FROM {subquery_snapshot} {sq_alias} \
             EXCEPT ALL \
             SELECT {sq_col_list} FROM {delta_sq} WHERE __pgt_action = 'I' \
             UNION ALL \
             SELECT {sq_col_list} FROM {delta_sq} WHERE __pgt_action = 'D')",
            sq_alias = quote_ident(subquery_alias),
            delta_sq = subquery_result.cte_name,
        );
        format!(
            "(SELECT {scalar_col} FROM {r_old_snapshot} sq_old)",
            scalar_col = quote_ident(&scalar_col),
        )
    };

    // Hash for child rows
    let hash_child = {
        let key_exprs: Vec<String> = child_cols
            .iter()
            .map(|c| format!("cs.{}", quote_ident(c)))
            .collect();
        build_hash_expr_for_domain("SCAN_KEY", &key_exprs)
    };

    let hash_delta_child = {
        let key_exprs: Vec<String> = child_cols
            .iter()
            .map(|c| format!("dc.{}", quote_ident(c)))
            .collect();
        build_hash_expr_for_domain("SCAN_KEY", &key_exprs)
    };

    // ── Pre-change outer child snapshot (C₀) for Part 2 ────────────
    //
    // DBSP: Δ(C × S) = (ΔC × S₁) + (C₀ × ΔS)
    //
    // C₀ = C_current EXCEPT ALL ΔC_inserts UNION ALL ΔC_deletes
    //
    // Using C₀ instead of C₁ in Part 2 prevents double-counting when
    // the outer child and scalar subquery share a common source table
    // (e.g., Q15 where revenue0 and MAX(total_revenue) both depend on
    // lineitem). Without C₀, Part 2 DELETE uses current child values
    // that no longer match the old scalar, causing missed DELETEs.
    let child_data_cols: String = child_cols
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");

    // P3-3: Gate C₀ computation behind inner delta existence check.
    // Add a helper CTE evaluated once that checks if the inner subquery
    // delta has any rows. Use it to guard the full outer snapshot scan
    // so the EXCEPT ALL is skipped entirely when the inner source is stable.
    let gate_cte = ctx.next_cte_name("sq_gate");
    let gate_sql = format!(
        "SELECT EXISTS (SELECT 1 FROM {delta_sq}) AS has_changes",
        delta_sq = subquery_result.cte_name,
    );
    ctx.add_cte(gate_cte.clone(), gate_sql);

    let child_old_snapshot = format!(
        "(SELECT {child_data_cols} FROM {child_table} __cs_inner \
         WHERE (SELECT has_changes FROM {gate_cte}) \
         EXCEPT ALL \
         SELECT {child_data_cols} FROM {delta_child} WHERE __pgt_action = 'I' \
         UNION ALL \
         SELECT {child_data_cols} FROM {delta_child} WHERE __pgt_action = 'D')",
        delta_child = child_result.cte_name,
    );

    let sql = format!(
        "\
-- Part 1: outer child delta rows with current scalar value (ΔC × S₁)
SELECT {hash_delta_child} AS __pgt_row_id,
       dc.__pgt_action,
       {dc_cols},
       {scalar_sql} AS {alias_ident}
FROM {delta_child} dc

UNION ALL

-- Part 2: pre-change outer rows with old scalar (C₀ × S₀ → DELETE)
SELECT {hash_child} AS __pgt_row_id,
       'D' AS __pgt_action,
       {cs_cols},
       {scalar_old_sql} AS {alias_ident}
FROM {child_old_snapshot} cs
WHERE (SELECT has_changes FROM {gate_cte})
  AND {scalar_sql} IS DISTINCT FROM {scalar_old_sql}

UNION ALL

-- Part 2b: pre-change outer rows with new scalar (C₀ × S₁ → INSERT)
SELECT {hash_child} AS __pgt_row_id,
       'I' AS __pgt_action,
       {cs_cols},
       {scalar_sql} AS {alias_ident}
FROM {child_old_snapshot} cs
WHERE (SELECT has_changes FROM {gate_cte})
  AND {scalar_sql} IS DISTINCT FROM {scalar_old_sql}",
        dc_cols = dc_col_refs.join(", "),
        cs_cols = cs_col_refs.join(", "),
        alias_ident = quote_ident(alias),
        hash_delta_child = hash_delta_child,
        delta_child = child_result.cte_name,
    );

    ctx.add_cte(cte_name.clone(), sql);

    Ok(DiffResult {
        cte_name,
        columns: output_cols.clone(),
        schema: child_result
            .schema
            .concat(&crate::dvm::schema::RelationSchema::from_names(
                std::slice::from_ref(alias),
            )),
        is_deduplicated: false,
        has_key_changed: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm::operators::test_helpers::*;

    #[test]
    fn test_diff_scalar_subquery_basic() {
        let mut ctx = test_ctx();
        let outer = scan(1, "orders", "public", "o", &["id", "amount"]);
        let inner = scan(2, "config", "public", "c", &["tax_rate"]);
        let tree = OpTree::ScalarSubquery {
            subquery: Box::new(inner),
            alias: "current_tax".to_string(),
            subquery_source_oids: vec![2],
            child: Box::new(outer),
        };
        let result = diff_scalar_subquery(&mut ctx, &tree).unwrap();

        // Output should include child columns + scalar alias
        assert_eq!(result.columns, vec!["id", "amount", "current_tax"]);
        assert!(!result.is_deduplicated);
    }

    #[test]
    fn test_diff_scalar_subquery_sql_structure() {
        let mut ctx = test_ctx();
        let outer = scan(1, "orders", "public", "o", &["id"]);
        let inner = scan(2, "stats", "public", "s", &["avg_val"]);
        let tree = OpTree::ScalarSubquery {
            subquery: Box::new(inner),
            alias: "global_avg".to_string(),
            subquery_source_oids: vec![2],
            child: Box::new(outer),
        };
        let result = diff_scalar_subquery(&mut ctx, &tree).unwrap();

        let sql = ctx.build_with_query(&result.cte_name);
        assert!(sql.contains("Part 1"), "SQL should have Part 1");
        assert!(sql.contains("Part 2"), "SQL should have Part 2");
        assert!(
            sql.contains("IS DISTINCT FROM"),
            "Part 2 should check for scalar value change"
        );
        assert!(
            !sql.contains("LIMIT 1"),
            "scalar snapshots must preserve PostgreSQL cardinality errors"
        );
        assert!(sql.contains("encode_row_id_v2('SCAN_KEY'"));
        assert!(!sql.contains("encode_row_id_v2('JOIN_KEY'"));
    }

    #[test]
    fn test_diff_scalar_subquery_uses_pre_change_snapshot() {
        // Part 2 must use C₀ (pre-change outer child snapshot) via EXCEPT ALL
        // to avoid double-counting when outer and inner share a source table.
        let mut ctx = test_ctx();
        let outer = scan(1, "lineitem", "public", "l", &["id", "amount"]);
        let inner = scan(2, "lineitem", "public", "l2", &["total"]);
        let tree = OpTree::ScalarSubquery {
            subquery: Box::new(inner),
            alias: "max_total".to_string(),
            subquery_source_oids: vec![2],
            child: Box::new(outer),
        };
        let result = diff_scalar_subquery(&mut ctx, &tree).unwrap();

        let sql = ctx.build_with_query(&result.cte_name);
        // Part 2 should use EXCEPT ALL to reconstruct C₀
        assert!(
            sql.contains("EXCEPT ALL"),
            "Part 2 should use EXCEPT ALL for pre-change outer snapshot:\n{sql}"
        );
        // The EXCEPT ALL should reference the child's delta CTE
        assert!(
            sql.contains("__pgt_action = 'I'"),
            "EXCEPT ALL should subtract inserts:\n{sql}"
        );
        assert!(
            sql.contains("__pgt_action = 'D'"),
            "UNION ALL should add back deletes:\n{sql}"
        );
    }

    #[test]
    fn test_diff_scalar_subquery_wrong_node_type() {
        let mut ctx = test_ctx();
        let scan_node = scan(1, "t", "public", "t", &["id"]);
        let result = diff_scalar_subquery(&mut ctx, &scan_node);
        assert!(result.is_err());
    }
}
