//! CTE scan differentiation (Tier 2 — shared delta for multi-reference CTEs).
//!
//! When the parser encounters a CTE referenced multiple times, each reference
//! produces an `OpTree::CteScan` with the same `cte_id`. The diff engine
//! differentiates the CTE body *once* and caches the result; subsequent
//! references reuse the cached `DiffResult` (pointing to the same system CTE).
//!
//! If the CteScan has column aliases (`FROM cte AS alias(c1, c2)`), a thin
//! renaming CTE is emitted on top of the cached delta output.

use crate::dvm::diff::{DiffContext, DiffResult, quote_ident};
use crate::dvm::operators::scan::build_hash_expr;
use crate::dvm::parser::OpTree;
use crate::error::PgTrickleError;

/// Differentiate a CteScan node.
///
/// 1. Look up `cte_id` in the delta cache — if cached, reuse.
/// 2. Otherwise, retrieve the CTE body from the registry, differentiate
///    it, cache the result.
/// 3. If column aliases are present, wrap the result in a renaming CTE.
pub fn diff_cte_scan(ctx: &mut DiffContext, op: &OpTree) -> Result<DiffResult, PgTrickleError> {
    let OpTree::CteScan {
        cte_id,
        cte_name,
        alias,
        columns: _,
        cte_def_aliases,
        column_aliases,
        body: _,
    } = op
    else {
        return Err(PgTrickleError::InternalError(
            "diff_cte_scan called on non-CteScan node".into(),
        ));
    };

    // Step 1 & 2: get-or-compute the delta for this CTE body
    let base_result = if let Some(cached) = ctx.get_cte_delta(*cte_id) {
        cached.clone()
    } else {
        // Retrieve the CTE body from the registry
        let (_, body) = ctx
            .cte_registry
            .get(*cte_id)
            .ok_or_else(|| {
                PgTrickleError::InternalError(format!(
                    "CTE '{cte_name}' (id={cte_id}) not found in registry"
                ))
            })?
            .clone();

        // Differentiate the body
        let result = ctx.diff_node(&body)?;

        // Cache for subsequent references
        ctx.set_cte_delta(*cte_id, result.clone());
        result
    };

    // Step 3: determine effective output columns.
    // Priority: column_aliases (FROM reference) > cte_def_aliases (CTE definition) > body columns
    let effective_cols = if !column_aliases.is_empty() {
        column_aliases.clone()
    } else if !cte_def_aliases.is_empty() {
        cte_def_aliases.clone()
    } else {
        base_result.columns.clone()
    };

    // Hash the source expressions before their positional aliases are applied.
    // The values are identical to the CTE's visible columns, while referring
    // to select-list aliases here would make the generated SQL invalid.
    let row_id_expr = build_hash_expr(
        &base_result
            .columns
            .iter()
            .map(|col| format!("{}::TEXT", quote_ident(col)))
            .collect::<Vec<_>>(),
    );

    // Build a wrapper CTE that applies visible column names positionally and
    // recomputes __pgt_row_id to match the CteScan initial population.
    let select_exprs: Vec<String> = base_result
        .columns
        .iter()
        .zip(effective_cols.iter())
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

    let cte_name_str = ctx.next_cte_name(&format!("ctescan_{alias}"));

    let sql = format!(
        "SELECT {row_id_expr} AS __pgt_row_id, __pgt_action, {cols}\n\
         FROM {child_cte}",
        cols = select_exprs.join(", "),
        child_cte = base_result.cte_name,
    );

    ctx.add_cte(cte_name_str.clone(), sql);

    Ok(DiffResult {
        cte_name: cte_name_str,
        columns: effective_cols.clone(),
        schema: base_result.schema.renamed(&effective_cols),
        is_deduplicated: base_result.is_deduplicated,
        has_key_changed: base_result.has_key_changed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm::operators::test_helpers::*;
    use crate::dvm::parser::CteRegistry;

    fn ctx_with_cte_registry(entries: Vec<(&str, OpTree)>) -> DiffContext {
        let registry = CteRegistry {
            entries: entries
                .into_iter()
                .map(|(name, tree)| (name.to_string(), tree))
                .collect(),
        };
        test_ctx().with_cte_registry(registry)
    }

    #[test]
    fn test_diff_cte_scan_basic() {
        let body = scan(1, "t", "public", "t", &["id", "name"]);
        let mut ctx = ctx_with_cte_registry(vec![("my_cte", body)]);
        let tree = cte_scan(0, "my_cte", "mc", vec!["id", "name"], vec![], vec![]);
        let result = diff_cte_scan(&mut ctx, &tree).unwrap();

        assert_eq!(result.columns, vec!["id", "name"]);
    }

    #[test]
    fn test_diff_cte_scan_caches_result() {
        let body = scan(1, "t", "public", "t", &["id", "name"]);
        let mut ctx = ctx_with_cte_registry(vec![("my_cte", body)]);

        // First scan
        let tree1 = cte_scan(0, "my_cte", "mc1", vec!["id", "name"], vec![], vec![]);
        let result1 = diff_cte_scan(&mut ctx, &tree1).unwrap();

        // Second scan of same CTE should reuse cached body delta
        let tree2 = cte_scan(0, "my_cte", "mc2", vec!["id", "name"], vec![], vec![]);
        let result2 = diff_cte_scan(&mut ctx, &tree2).unwrap();

        // Each reference gets its own wrapper CTE but the same output columns
        assert_eq!(result1.columns, result2.columns);
    }

    #[test]
    fn test_diff_cte_scan_with_column_aliases() {
        let body = scan(1, "t", "public", "t", &["id", "name"]);
        let mut ctx = ctx_with_cte_registry(vec![("my_cte", body)]);
        let tree = cte_scan(0, "my_cte", "mc", vec!["a", "b"], vec![], vec!["a", "b"]);
        let result = diff_cte_scan(&mut ctx, &tree).unwrap();

        // Columns should be renamed
        assert_eq!(result.columns, vec!["a", "b"]);
        let sql = ctx.build_with_query(&result.cte_name);
        assert_sql_contains(&sql, "ARRAY[(\"id\"::TEXT)::TEXT, (\"name\"::TEXT)::TEXT]");
        assert_sql_contains(&sql, "\"id\" AS \"a\"");
        assert_sql_contains(&sql, "\"name\" AS \"b\"");
    }

    #[test]
    fn test_diff_cte_scan_with_def_aliases() {
        let body = scan(1, "t", "public", "t", &["id", "name"]);
        let mut ctx = ctx_with_cte_registry(vec![("my_cte", body)]);
        let tree = cte_scan(0, "my_cte", "mc", vec!["x", "y"], vec!["x", "y"], vec![]);
        let result = diff_cte_scan(&mut ctx, &tree).unwrap();

        // Should use cte_def_aliases when column_aliases is empty
        assert_eq!(result.columns, vec!["x", "y"]);
    }

    #[test]
    fn test_diff_cte_scan_error_missing_cte() {
        let mut ctx = test_ctx(); // no CTE registry entries
        let tree = cte_scan(99, "nonexistent", "x", vec!["id"], vec![], vec![]);
        let result = diff_cte_scan(&mut ctx, &tree);
        assert!(result.is_err());
    }

    #[test]
    fn test_diff_cte_scan_error_on_non_cte_scan_node() {
        let mut ctx = test_ctx();
        let tree = scan(1, "t", "public", "t", &["id"]);
        let result = diff_cte_scan(&mut ctx, &tree);
        assert!(result.is_err());
    }
}
