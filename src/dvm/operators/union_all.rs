//! UNION ALL differentiation.
//!
//! ΔI(Q₁ ∪ Q₂) = ΔI(Q₁) ∪ ΔI(Q₂)
//!
//! Straightforward — just UNION ALL the deltas of each child.
//! Row IDs are prefixed with a child index to prevent collisions.

use crate::dvm::diff::{DiffContext, DiffResult, quote_ident};
use crate::dvm::parser::OpTree;
use crate::dvm::schema::validate_set_operation;
use crate::error::PgTrickleError;

/// Differentiate a UnionAll node.
pub fn diff_union_all(ctx: &mut DiffContext, op: &OpTree) -> Result<DiffResult, PgTrickleError> {
    let OpTree::UnionAll { children } = op else {
        return Err(PgTrickleError::InternalError(
            "diff_union_all called on non-UnionAll node".into(),
        ));
    };

    if children.is_empty() {
        return Err(PgTrickleError::QueryParseError(
            "UNION ALL with no children".into(),
        ));
    }

    // Differentiate each child
    let mut child_results = Vec::new();
    for child in children {
        child_results.push(ctx.diff_node(child)?);
    }

    // Use the first child's columns as the output schema
    let output_cols = child_results[0].columns.clone();
    for child in child_results.iter().skip(1) {
        validate_set_operation("UNION ALL", &child_results[0].schema, &child.schema)?;
    }
    let col_refs: Vec<String> = output_cols.iter().map(|c| quote_ident(c)).collect();
    let col_list = col_refs.join(", ");

    let cte_name = ctx.next_cte_name("union");

    // Build UNION ALL of all child deltas with prefixed row IDs
    let parts: Vec<String> = child_results
        .iter()
        .enumerate()
        .map(|(i, result)| {
            let row_id_expr = crate::hash::build_composite_hash_expr(&[
                format!("'{}'::TEXT", i + 1),
                "__pgt_row_id::TEXT".to_string(),
            ]);
            format!(
                "SELECT {row_id_expr} \
                 AS __pgt_row_id,\n\
                 __pgt_action, {col_list}\n\
                 FROM {cte}",
                row_id_expr = row_id_expr,
                cte = result.cte_name,
            )
        })
        .collect();

    let sql = parts.join("\n\nUNION ALL\n\n");
    ctx.add_cte(cte_name.clone(), sql);

    Ok(DiffResult {
        cte_name,
        columns: output_cols,
        schema: child_results[0].schema.clone(),
        is_deduplicated: false,
        has_key_changed: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm::operators::test_helpers::*;

    #[test]
    fn test_diff_union_all_two_children() {
        let mut ctx = test_ctx();
        let left = scan(1, "t1", "public", "t1", &["id", "val"]);
        let right = scan(2, "t2", "public", "t2", &["id", "val"]);
        let tree = union_all(vec![left, right]);
        let result = diff_union_all(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        assert_eq!(result.columns, vec!["id", "val"]);
        assert_sql_contains(&sql, "UNION ALL");
        // Each child gets a prefixed row_id
        assert_sql_contains(&sql, "pgtrickle.pg_trickle_hash_multi");
    }

    #[test]
    fn test_diff_union_all_three_children() {
        let mut ctx = test_ctx();
        let c1 = scan(1, "a", "public", "a", &["x"]);
        let c2 = scan(2, "b", "public", "b", &["x"]);
        let c3 = scan(3, "c", "public", "c", &["x"]);
        let tree = union_all(vec![c1, c2, c3]);
        let result = diff_union_all(&mut ctx, &tree).unwrap();

        // Should produce 3 UNION ALL branches (2 "UNION ALL" separators)
        let sql = ctx.build_with_query(&result.cte_name);
        let count = sql.matches("UNION ALL").count();
        // Each of 3 scan nodes has UNION ALL internally + the final union
        assert!(count >= 2, "expected at least 2 UNION ALL, got {count}");
    }

    #[test]
    fn test_diff_union_all_empty_children_error() {
        let mut ctx = test_ctx();
        let tree = OpTree::UnionAll { children: vec![] };
        let result = diff_union_all(&mut ctx, &tree);
        assert!(result.is_err());
    }

    #[test]
    fn test_diff_union_all_not_deduplicated() {
        let mut ctx = test_ctx();
        let c1 = scan(1, "a", "public", "a", &["x"]);
        let c2 = scan(2, "b", "public", "b", &["x"]);
        let tree = union_all(vec![c1, c2]);
        let result = diff_union_all(&mut ctx, &tree).unwrap();
        assert!(!result.is_deduplicated);
    }

    #[test]
    fn test_diff_union_all_error_on_non_union_node() {
        let mut ctx = test_ctx();
        let tree = scan(1, "t", "public", "t", &["id"]);
        let result = diff_union_all(&mut ctx, &tree);
        assert!(result.is_err());
    }

    #[test]
    fn test_diff_union_all_rejects_mismatched_aliases_before_union_sql() {
        let mut ctx = test_ctx();
        let tree = union_all(vec![
            scan(1, "left_t", "public", "l", &["id"]),
            scan(2, "right_t", "public", "r", &["other_id"]),
        ]);
        let error = diff_union_all(&mut ctx, &tree).unwrap_err();
        assert!(error.to_string().contains("alias mismatch"));
    }
}
