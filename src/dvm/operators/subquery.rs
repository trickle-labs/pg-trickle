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
use crate::dvm::parser::{OpTree, apply_column_aliases};
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
    let output_columns = apply_column_aliases(&child_result.columns, column_aliases);
    let rename_exprs: Vec<String> = child_result
        .columns
        .iter()
        .zip(&output_columns)
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
    let mut projections = vec!["__pgt_row_id".to_string(), "__pgt_action".to_string()];
    if child_result.has_key_changed {
        projections.push("__pgt_key_changed".to_string());
    }
    projections.extend(rename_exprs);
    let sql = format!(
        "SELECT {}\nFROM {}",
        projections.join(", "),
        child_result.cte_name,
    );

    ctx.add_cte(cte_name.clone(), sql);

    Ok(DiffResult {
        cte_name,
        columns: output_columns.clone(),
        schema: child_result.schema.renamed(&output_columns),
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
    fn test_diff_subquery_partial_alias_preserves_columns_and_key_changed() {
        let mut ctx = test_ctx();
        ctx.source_cdc_columns_mut()
            .insert(1, vec!["id".into(), "name".into(), "qty".into()]);
        ctx.source_key_columns_mut().insert(1, vec!["id".into()]);
        let child = scan_with_pk(1, "t", "public", "t", &["id", "name", "qty"], &["id"]);
        let tree = subquery("sq", vec!["renamed_id"], child);
        let result = diff_subquery(&mut ctx, &tree).unwrap();

        assert_eq!(result.columns, vec!["renamed_id", "name", "qty"]);
        assert!(result.has_key_changed);
        let sql = ctx.build_with_query(&result.cte_name);
        assert_sql_contains(
            &sql,
            "__pgt_action, __pgt_key_changed, \"id\" AS \"renamed_id\", \"name\", \"qty\"",
        );
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
