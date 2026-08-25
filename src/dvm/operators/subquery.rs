//! Subquery differentiation.
//!
//! ΔI(Subquery(alias, col_aliases, Q)) = rename_columns(ΔI(Q))
//!
//! A subquery wrapper is transparent for differentiation. The child's
//! delta is computed first, then columns are optionally renamed to
//! match the subquery's column aliases.
//!
//! This handles both:
//! - Inlined CTEs: `WITH x AS (SELECT ...) SELECT ... FROM x`
//! - Explicit subqueries: `SELECT ... FROM (SELECT ...) AS x(c1, c2)`

use crate::dvm::diff::{DiffContext, DiffResult, quote_ident};
use crate::dvm::parser::OpTree;
use crate::error::PgTrickleError;

/// Differentiate a Subquery node.
///
/// Delegates to the child's differentiation. If column aliases are
/// specified, wraps the result in a renaming CTE.
pub fn diff_subquery(ctx: &mut DiffContext, op: &OpTree) -> Result<DiffResult, PgTrickleError> {
    let OpTree::Subquery {
        alias,
        column_aliases,
        child,
    } = op
    else {
        return Err(PgTrickleError::InternalError(
            "diff_subquery called on non-Subquery node".into(),
        ));
    };

    // Differentiate the child subtree
    let child_result = ctx.diff_node(child)?;

    // If no column aliases, the subquery is fully transparent — just
    // return the child's diff result directly.
    if column_aliases.is_empty() {
        return Ok(child_result);
    }

    // Column aliases present — generate a renaming CTE.
    // Map child output columns → alias names.
    let child_cols = &child_result.columns;
    let rename_exprs: Vec<String> = child_cols
        .iter()
        .zip(column_aliases.iter())
        .map(|(src, dst)| {
            let src_ident = quote_ident(src);
            let dst_ident = quote_ident(dst);
            if src == dst {
                dst_ident
            } else {
                format!("{src_ident} AS {dst_ident}")
            }
        })
        .collect();

    let cte_name = ctx.next_cte_name(&format!("subq_{alias}"));

    let sql = format!(
        "SELECT __pgt_row_id, __pgt_action, {cols}\n\
         FROM {child_cte}",
        cols = rename_exprs.join(", "),
        child_cte = child_result.cte_name,
    );

    ctx.add_cte(cte_name.clone(), sql);

    Ok(DiffResult {
        cte_name,
        columns: column_aliases.clone(),
        schema: child_result.schema.renamed(column_aliases),
        is_deduplicated: child_result.is_deduplicated,
        has_key_changed: child_result.has_key_changed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm::operators::test_helpers::*;

    #[test]
    fn test_diff_subquery_no_aliases_transparent() {
        let mut ctx = test_ctx();
        let child = scan(1, "t", "public", "t", &["id", "name"]);
        let tree = subquery("sq", vec![], child);
        let result = diff_subquery(&mut ctx, &tree).unwrap();

        // No column aliases → transparent passthrough, keeps child columns
        assert_eq!(result.columns, vec!["id", "name"]);
    }

    #[test]
    fn test_diff_subquery_with_aliases_generates_rename_cte() {
        let mut ctx = test_ctx();
        let child = scan(1, "t", "public", "t", &["id", "name"]);
        let tree = subquery("sq", vec!["a", "b"], child);
        let result = diff_subquery(&mut ctx, &tree).unwrap();

        assert_eq!(result.columns, vec!["a", "b"]);
        let sql = ctx.build_with_query(&result.cte_name);
        assert_sql_contains(&sql, "\"id\" AS \"a\"");
        assert_sql_contains(&sql, "\"name\" AS \"b\"");
    }

    #[test]
    fn test_diff_subquery_preserves_dedup_flag() {
        let mut ctx = test_ctx();
        ctx.merge_safe_dedup = true;
        let child = scan_with_pk(1, "t", "public", "t", &["id"], &["id"]);
        let tree = subquery("sq", vec!["x"], child);
        let result = diff_subquery(&mut ctx, &tree).unwrap();
        assert!(result.is_deduplicated);
    }

    #[test]
    fn test_diff_subquery_error_on_non_subquery_node() {
        let mut ctx = test_ctx();
        let tree = scan(1, "t", "public", "t", &["id"]);
        let result = diff_subquery(&mut ctx, &tree);
        assert!(result.is_err());
    }
}
