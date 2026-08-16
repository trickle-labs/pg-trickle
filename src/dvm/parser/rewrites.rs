//! SQL query rewrite passes.
//!
//! Each rewrite function takes a SQL string, parses it via `pg_sys::raw_parser`,
//! transforms the AST, and deparses back to SQL. Rewrites include view inlining,
//! GROUPING SETS expansion, scalar subquery lifting, De Morgan normalization,
//! window function splitting, and predicate pushdown.

use super::*;
use crate::error::PgTrickleError;

// ── View inlining auto-rewrite ──────────────────────────────────────────

/// Auto-rewrite pass #0: Replace view references with inline subqueries.
///
/// For each `RangeVar` in the FROM clause that resolves to a PostgreSQL
/// view (`relkind = 'v'`), replaces it with `(view_definition) AS alias`.
/// Handles nested views by iterating until no views remain (fixpoint).
///
/// Materialized views (`relkind = 'm'`) are **not** inlined — their
/// semantics differ (stale snapshot vs live query). They are left as-is
/// for later rejection or acceptance depending on refresh mode.
///
/// Foreign tables (`relkind = 'f'`) are also left as-is.
///
/// Returns the original query unchanged if no views are found.
pub fn rewrite_views_inline(query: &str) -> Result<String, PgTrickleError> {
    let max_depth = 10;
    let mut current = query.to_string();

    for _depth in 0..max_depth {
        let rewritten = rewrite_views_inline_once(&current)?;
        if rewritten == current {
            return Ok(current); // Fixpoint reached — no more views
        }
        current = rewritten;
    }

    Err(PgTrickleError::QueryParseError(format!(
        "View inlining exceeded maximum nesting depth of {max_depth}. \
         This may indicate circular view dependencies."
    )))
}

/// Single-pass view inlining: parse the query, walk FROM items, replace
/// any view RangeVars with `(pg_get_viewdef(oid, true)) AS alias`.
///
/// Returns the original string unchanged if no views are found.
///
/// P-6 (v0.54.0): Passes a per-invocation relkind cache through the call chain
/// so each unique `(schema, relname)` pair is only resolved via SPI once per
/// iteration, even when the same relation appears multiple times (e.g., in
/// UNION ALL arms or repeated subqueries).
fn rewrite_views_inline_once(query: &str) -> Result<String, PgTrickleError> {
    let select = match parse_first_select(query)? {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        Some(s) => pg_deref!(s),
        None => return Ok(query.to_string()),
    };

    // Build a substitution map: views found in FROM → their definitions
    let mut subs = Vec::new();
    let mut found_view = false;

    // P-6 (v0.54.0): Per-invocation relkind cache — each (schema, relname) pair
    // is resolved via SPI at most once per rewrite_views_inline_once() call.
    // Uses HashMap<(String, String), Option<String>> where the value is the relkind
    // string ('r', 'v', 'm', 'f', etc.) or None if not found in pg_class.
    let mut relkind_cache: std::collections::HashMap<(String, String), Option<String>> =
        std::collections::HashMap::new();

    // Walk the FROM clause to find views. Also walk set-operation arms and CTEs.
    collect_view_substitutions(select, &mut subs, &mut found_view, &mut relkind_cache)?;

    if !found_view {
        return Ok(query.to_string());
    }

    // Deparse the full query with view substitutions applied
    deparse_select_stmt_with_view_subs(select, &subs)
}

/// Information about a view found in the FROM clause that should be
/// replaced with an inline subquery.
pub(crate) struct ViewSubstitution {
    /// Schema name of the view (or empty if unqualified).
    schema: String,
    /// Relation name of the view.
    relname: String,
    /// The view's SQL definition from `pg_get_viewdef()`.
    view_sql: String,
    /// Alias to use for the inline subquery.
    alias: String,
}

/// Resolve `relkind` for a relation given schema and name.
///
/// Returns: `'r'` (table), `'v'` (view), `'m'` (matview), `'f'` (foreign),
/// `'p'` (partitioned table), etc.
fn resolve_relkind(schema: &str, relname: &str) -> Result<Option<String>, PgTrickleError> {
    let sql = format!(
        "SELECT c.relkind::text FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = '{}' AND c.relname = '{}'",
        schema.replace('\'', "''"),
        relname.replace('\'', "''"),
    );
    Spi::connect(|client| {
        let mut result = client
            .select(&sql, None, &[])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        // Use iterator to safely handle empty result sets (avoids
        // "SpiTupleTable positioned before the start or after the end" on empty results).
        if let Some(row) = result.next() {
            return row
                .get::<String>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()));
        }
        Ok(None)
    })
}

/// Get the SQL definition of a view using `pg_get_viewdef(oid, true)`.
fn get_view_definition(schema: &str, relname: &str) -> Result<String, PgTrickleError> {
    let sql = format!(
        "SELECT pg_get_viewdef(c.oid, true) FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = '{}' AND c.relname = '{}'",
        schema.replace('\'', "''"),
        relname.replace('\'', "''"),
    );
    Spi::connect(|client| {
        let mut result = client
            .select(&sql, None, &[])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        // Use iterator to safely handle empty result sets (avoids
        // "SpiTupleTable positioned before the start or after the end" on empty results).
        if let Some(row) = result.next() {
            let raw = row
                .get::<String>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::QueryParseError(format!(
                        "Could not retrieve view definition for {schema}.{relname}"
                    ))
                })?;
            return Ok(strip_view_definition_suffix(&raw));
        }
        Err(PgTrickleError::QueryParseError(format!(
            "Could not retrieve view definition for {schema}.{relname}: view not found"
        )))
    })
}

/// Strip a trailing semicolon (and surrounding whitespace) from a view
/// definition returned by `pg_get_viewdef()`.
///
/// PostgreSQL 18 may include a trailing semicolon. When the definition is
/// embedded inside a subquery parenthesis, the semicolon produces a syntax
/// error. Extracted as a pure function for unit-testability.
pub(crate) fn strip_view_definition_suffix(raw: &str) -> String {
    raw.trim_end_matches(';').trim().to_string()
}

/// Resolve schema for a RangeVar. If schemaname is NULL, resolve via search_path.
///
/// Returns `None` if the relation cannot be found in `pg_class` (e.g. CTE
/// names, subquery aliases, or function-call ranges).
pub(crate) fn resolve_rangevar_schema(
    rv: &pg_sys::RangeVar,
) -> Result<Option<String>, PgTrickleError> {
    if !rv.schemaname.is_null() {
        let schema = pg_cstr_to_str(rv.schemaname)?;
        return Ok(Some(schema.to_string()));
    }

    // Resolve from search_path
    if rv.relname.is_null() {
        return Ok(None);
    }
    let relname = pg_cstr_to_str(rv.relname)?;

    let sql = format!(
        "SELECT n.nspname::text FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relname = '{}' \
         AND n.nspname = ANY(string_to_array(current_setting('search_path'), ', ')::text[] || 'public'::text) \
         ORDER BY array_position(string_to_array(current_setting('search_path'), ', ')::text[] || 'public'::text, n.nspname) \
         LIMIT 1",
        relname.replace('\'', "''"),
    );
    Spi::connect(|client| {
        let result = client
            .select(&sql, None, &[])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        // Iterate to handle empty results (CTE names, subquery aliases, etc.)
        for row in result {
            if let Ok(Some(schema)) = row.get::<String>(1) {
                return Ok(Some(schema));
            }
        }
        Ok(None)
    })
}

/// Walk a SelectStmt's FROM clause (and set-operation arms, CTEs) to find
/// view references and build the substitution list.
///
/// P-6 (v0.54.0): Takes a `relkind_cache` that is populated on first lookup
/// for each `(schema, relname)` pair and reused for subsequent lookups within
/// the same `rewrite_views_inline_once()` call, eliminating redundant SPI
/// round-trips for relations that appear in multiple FROM positions.
///
/// # Safety
/// Caller must ensure `select` points to a valid `SelectStmt`.
fn collect_view_substitutions(
    select: *const pg_sys::SelectStmt,
    subs: &mut Vec<ViewSubstitution>,
    found_view: &mut bool,
    relkind_cache: &mut std::collections::HashMap<(String, String), Option<String>>,
) -> Result<(), PgTrickleError> {
    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let s = pg_deref!(select);

    // Handle set operations: recurse into larg/rarg
    if s.op != pg_sys::SetOperation::SETOP_NONE {
        if !s.larg.is_null() {
            collect_view_substitutions(s.larg, subs, found_view, relkind_cache)?;
        }
        if !s.rarg.is_null() {
            collect_view_substitutions(s.rarg, subs, found_view, relkind_cache)?;
        }
        return Ok(());
    }

    // Walk the FROM clause
    let from_list = pg_list::<pg_sys::Node>(s.fromClause);
    for node_ptr in from_list.iter_ptr() {
        collect_view_subs_from_item(node_ptr, subs, found_view, relkind_cache)?;
    }

    // Walk CTE bodies if present
    if !s.withClause.is_null() {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let wc = pg_deref!(s.withClause);
        let cte_list = pg_list::<pg_sys::Node>(wc.ctes);
        for node_ptr in cte_list.iter_ptr() {
            if let Some(cte) = cast_node!(node_ptr, T_CommonTableExpr, pg_sys::CommonTableExpr)
                && !cte.ctequery.is_null()
                // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
                && is_node_type!(cte.ctequery, T_SelectStmt)
            {
                collect_view_substitutions(
                    cte.ctequery as *const pg_sys::SelectStmt,
                    subs,
                    found_view,
                    relkind_cache,
                )?;
            }
        }
    }

    Ok(())
}

/// Walk a single FROM item looking for view references.
///
/// # Safety
/// Caller must ensure `node` points to a valid parse tree node.
fn collect_view_subs_from_item(
    node: *mut pg_sys::Node,
    subs: &mut Vec<ViewSubstitution>,
    found_view: &mut bool,
    relkind_cache: &mut std::collections::HashMap<(String, String), Option<String>>,
) -> Result<(), PgTrickleError> {
    if node.is_null() {
        return Ok(());
    }

    if let Some(rv) = cast_node!(node, T_RangeVar, pg_sys::RangeVar) {
        // Get relname
        if rv.relname.is_null() {
            return Ok(());
        }
        let relname = pg_cstr_to_str(rv.relname)?.to_string();

        // Resolve schema — if not found, this is a CTE/subquery alias, skip.
        let schema = match resolve_rangevar_schema(rv)? {
            Some(s) => s,
            None => return Ok(()),
        };

        // P-6 (v0.54.0): Check relkind cache before issuing SPI.
        // Cache key is (schema, relname); value is the relkind string or None.
        let cache_key = (schema.clone(), relname.clone());
        let relkind = if let Some(cached) = relkind_cache.get(&cache_key) {
            cached.clone()
        } else {
            let result = resolve_relkind(&schema, &relname)?;
            relkind_cache.insert(cache_key, result.clone());
            result
        };

        match relkind.as_deref() {
            Some("v") => {
                // It's a view — get definition and record substitution
                let view_sql = get_view_definition(&schema, &relname)?;

                // Determine alias: explicit alias or view name
                let alias = if !rv.alias.is_null() {
                    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
                    let a = pg_deref!(rv.alias);
                    pg_cstr_to_str(a.aliasname).unwrap_or(&relname).to_string()
                } else {
                    relname.clone()
                };

                subs.push(ViewSubstitution {
                    schema: schema.clone(),
                    relname: relname.clone(),
                    view_sql,
                    alias,
                });
                *found_view = true;
            }
            _ => {
                // Not a view — leave as-is
            }
        }
    } else if let Some(join) = cast_node!(node, T_JoinExpr, pg_sys::JoinExpr) {
        collect_view_subs_from_item(join.larg, subs, found_view, relkind_cache)?;
        collect_view_subs_from_item(join.rarg, subs, found_view, relkind_cache)?;
    } else if let Some(sub) = cast_node!(node, T_RangeSubselect, pg_sys::RangeSubselect)
        && !sub.subquery.is_null()
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        && is_node_type!(sub.subquery, T_SelectStmt)
    {
        collect_view_substitutions(
            sub.subquery as *const pg_sys::SelectStmt,
            subs,
            found_view,
            relkind_cache,
        )?;
    }
    // JSON_TABLE: skip — it doesn't reference tables that could be views.
    // The context item expression references the left-hand table which is
    // already traversed via the FROM list iteration.

    Ok(())
}

/// Deparse a SelectStmt back to SQL, replacing view RangeVars with inline
/// subqueries according to the substitution list.
///
/// # Safety
/// Caller must ensure `stmt` points to a valid `SelectStmt`.
pub(crate) fn deparse_select_stmt_with_view_subs(
    stmt: *const pg_sys::SelectStmt,
    subs: &[ViewSubstitution],
) -> Result<String, PgTrickleError> {
    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let s = pg_deref!(stmt);

    // Handle set operations: deparse each arm separately
    if s.op != pg_sys::SetOperation::SETOP_NONE {
        let op_str = match s.op {
            pg_sys::SetOperation::SETOP_UNION if s.all => "UNION ALL",
            pg_sys::SetOperation::SETOP_UNION => "UNION",
            pg_sys::SetOperation::SETOP_INTERSECT if s.all => "INTERSECT ALL",
            pg_sys::SetOperation::SETOP_INTERSECT => "INTERSECT",
            pg_sys::SetOperation::SETOP_EXCEPT if s.all => "EXCEPT ALL",
            pg_sys::SetOperation::SETOP_EXCEPT => "EXCEPT",
            _ => "UNION",
        };

        let left = if !s.larg.is_null() {
            deparse_select_stmt_with_view_subs(s.larg, subs)?
        } else {
            String::new()
        };
        let right = if !s.rarg.is_null() {
            deparse_select_stmt_with_view_subs(s.rarg, subs)?
        } else {
            String::new()
        };

        let mut result = format!("{left} {op_str} {right}");

        // Preserve WITH clause on the outer SETOP node (CTE + UNION ALL pattern).
        // The CTE definitions live here, NOT on larg/rarg, so they must be
        // prepended after the arms have been recursively processed.
        if !s.withClause.is_null() {
            let with_sql = deparse_with_clause_with_view_subs(s.withClause, subs)?;
            result = format!("{with_sql} {result}");
        }

        // Top-level ORDER BY / LIMIT / OFFSET on set operations
        let sort_list = pg_list::<pg_sys::Node>(s.sortClause);
        if !sort_list.is_empty() {
            // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
            let sorts = unsafe { deparse_sort_clause(&sort_list)? };
            result.push_str(&format!(" ORDER BY {sorts}"));
        }
        if !s.limitCount.is_null() {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let expr = unsafe { node_to_expr(s.limitCount)? };
            result.push_str(&format!(" LIMIT {}", expr.to_sql()));
        }
        if !s.limitOffset.is_null() {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let expr = unsafe { node_to_expr(s.limitOffset)? };
            result.push_str(&format!(" OFFSET {}", expr.to_sql()));
        }

        return Ok(result);
    }

    // Regular SELECT — deparse with substitutions
    let mut parts = Vec::new();

    // WITH clause (CTEs) — deparse CTE bodies with view subs too
    if !s.withClause.is_null() {
        let with_sql = deparse_with_clause_with_view_subs(s.withClause, subs)?;
        parts.push(with_sql);
    }

    // DISTINCT
    let distinct_list = pg_list::<pg_sys::Node>(s.distinctClause);
    let select_kw = if !distinct_list.is_empty() {
        // Check if it's DISTINCT ON or plain DISTINCT
        let has_real_exprs = distinct_list.iter_ptr().any(|ptr| !ptr.is_null());
        if has_real_exprs {
            // DISTINCT ON (expr, expr, ...)
            let mut exprs = Vec::new();
            for node_ptr in distinct_list.iter_ptr() {
                if !node_ptr.is_null() {
                    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                    let expr = unsafe { node_to_expr(node_ptr)? };
                    exprs.push(expr.to_sql());
                }
            }
            format!("SELECT DISTINCT ON ({})", exprs.join(", "))
        } else {
            "SELECT DISTINCT".to_string()
        }
    } else {
        "SELECT".to_string()
    };

    // Target list
    // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
    let targets = unsafe { deparse_target_list(s.targetList)? };
    parts.push(format!("{select_kw} {targets}"));

    // FROM clause with view substitutions
    let from_list = pg_list::<pg_sys::Node>(s.fromClause);
    if !from_list.is_empty() {
        let mut from_items = Vec::new();
        for node_ptr in from_list.iter_ptr() {
            let item = deparse_from_item_with_view_subs(node_ptr, subs)?;
            from_items.push(item);
        }
        parts.push(format!("FROM {}", from_items.join(", ")));
    }

    // WHERE
    if !s.whereClause.is_null() {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = unsafe { node_to_expr(s.whereClause)? };
        parts.push(format!("WHERE {}", expr.to_sql()));
    }

    // GROUP BY
    let group_list = pg_list::<pg_sys::Node>(s.groupClause);
    if !group_list.is_empty() {
        let mut groups = Vec::new();
        for node_ptr in group_list.iter_ptr() {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let expr = unsafe { node_to_expr(node_ptr)? };
            groups.push(expr.to_sql());
        }
        parts.push(format!("GROUP BY {}", groups.join(", ")));
    }

    // HAVING
    if !s.havingClause.is_null() {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = unsafe { node_to_expr(s.havingClause)? };
        parts.push(format!("HAVING {}", expr.to_sql()));
    }

    // WINDOW clause
    let window_list = pg_list::<pg_sys::Node>(s.windowClause);
    if !window_list.is_empty() {
        let mut window_parts = Vec::new();
        for node_ptr in window_list.iter_ptr() {
            if let Some(wdef) = cast_node!(node_ptr, T_WindowDef, pg_sys::WindowDef) {
                let wname = if !wdef.name.is_null() {
                    pg_cstr_to_str(wdef.name).unwrap_or("w").to_string()
                } else {
                    continue;
                };
                let wspec = deparse_window_def(wdef)?;
                window_parts.push(format!("{wname} AS ({wspec})"));
            }
        }
        if !window_parts.is_empty() {
            parts.push(format!("WINDOW {}", window_parts.join(", ")));
        }
    }

    // ORDER BY
    let sort_list = pg_list::<pg_sys::Node>(s.sortClause);
    if !sort_list.is_empty() {
        // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
        let sorts = unsafe { deparse_sort_clause(&sort_list)? };
        parts.push(format!("ORDER BY {sorts}"));
    }

    // LIMIT
    if !s.limitCount.is_null() {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = unsafe { node_to_expr(s.limitCount)? };
        parts.push(format!("LIMIT {}", expr.to_sql()));
    }

    // OFFSET
    if !s.limitOffset.is_null() {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = unsafe { node_to_expr(s.limitOffset)? };
        parts.push(format!("OFFSET {}", expr.to_sql()));
    }

    Ok(parts.join(" "))
}

/// Deparse a WITH clause, applying view substitutions to CTE bodies.
///
/// # Safety
/// Caller must ensure `with_clause` points to a valid `WithClause`.
fn deparse_with_clause_with_view_subs(
    with_clause: *mut pg_sys::WithClause,
    subs: &[ViewSubstitution],
) -> Result<String, PgTrickleError> {
    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let wc = pg_deref!(with_clause);
    let cte_list = pg_list::<pg_sys::Node>(wc.ctes);
    let mut cte_parts = Vec::new();

    for node_ptr in cte_list.iter_ptr() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if !is_node_type!(node_ptr, T_CommonTableExpr) {
            continue;
        }
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let cte = pg_deref!(node_ptr as *const pg_sys::CommonTableExpr);
        let cte_name = pg_cstr_to_str(cte.ctename).unwrap_or("cte").to_string();

        // Column aliases
        let col_aliases = extract_cte_def_colnames(cte)?;
        let alias_part = if col_aliases.is_empty() {
            String::new()
        } else {
            format!("({})", col_aliases.join(", "))
        };

        // CTE body
        let body_sql = if !cte.ctequery.is_null()
            // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
            && is_node_type!(cte.ctequery, T_SelectStmt)
        {
            deparse_select_stmt_with_view_subs(cte.ctequery as *const pg_sys::SelectStmt, subs)?
        } else {
            // Fallback: shouldn't happen for valid queries
            "SELECT 1".to_string()
        };

        cte_parts.push(format!("{cte_name}{alias_part} AS ({body_sql})"));
    }

    let recursive = if wc.recursive { "RECURSIVE " } else { "" };
    Ok(format!("WITH {recursive}{}", cte_parts.join(", ")))
}

/// Deparse a single FROM item, replacing view RangeVars with inline subqueries.
///
/// # Safety
/// Caller must ensure `node` points to a valid parse tree node.
fn deparse_from_item_with_view_subs(
    node: *mut pg_sys::Node,
    subs: &[ViewSubstitution],
) -> Result<String, PgTrickleError> {
    if node.is_null() {
        return Err(PgTrickleError::QueryParseError(
            "NULL FROM item node".into(),
        ));
    }

    if let Some(rv) = cast_node!(node, T_RangeVar, pg_sys::RangeVar) {
        // Get relname
        let relname = if !rv.relname.is_null() {
            pg_cstr_to_str(rv.relname).unwrap_or("").to_string()
        } else {
            return Ok("?".to_string());
        };

        // Get schema (may be NULL for unqualified names)
        let schema = if !rv.schemaname.is_null() {
            Some(pg_cstr_to_str(rv.schemaname).unwrap_or("").to_string())
        } else {
            None
        };

        // Get explicit alias
        let explicit_alias = if !rv.alias.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let a = pg_deref!(rv.alias);
            Some(pg_cstr_to_str(a.aliasname).unwrap_or("").to_string())
        } else {
            None
        };

        // Check if this RangeVar matches a view substitution
        if let Some(sub) = subs.iter().find(|s| {
            s.relname == relname
                && (schema.as_deref() == Some(&s.schema)
                    || (schema.is_none() && !s.schema.is_empty()))
        }) {
            // Replace with inline subquery — use the alias from this specific
            // RangeVar, not from the substitution (which may come from a
            // different occurrence of the same view with a different alias).
            let alias = explicit_alias.as_deref().unwrap_or(&relname);
            return Ok(format!("({}) AS {alias}", sub.view_sql));
        }

        // Not a view — deparse normally
        let mut name = String::new();
        if let Some(ref s) = schema {
            name.push_str(s);
            name.push('.');
        }
        name.push_str(&relname);
        if let Some(ref a) = explicit_alias
            && a != &relname
        {
            name.push(' ');
            name.push_str(a);
        }
        Ok(name)
    } else if let Some(join) = cast_node!(node, T_JoinExpr, pg_sys::JoinExpr) {
        let left = deparse_from_item_with_view_subs(join.larg, subs)?;
        let right = deparse_from_item_with_view_subs(join.rarg, subs)?;

        let join_type = match join.jointype {
            pg_sys::JoinType::JOIN_INNER if join.isNatural => "NATURAL JOIN",
            pg_sys::JoinType::JOIN_INNER => "JOIN",
            pg_sys::JoinType::JOIN_LEFT if join.isNatural => "NATURAL LEFT JOIN",
            pg_sys::JoinType::JOIN_LEFT => "LEFT JOIN",
            pg_sys::JoinType::JOIN_FULL if join.isNatural => "NATURAL FULL JOIN",
            pg_sys::JoinType::JOIN_FULL => "FULL JOIN",
            pg_sys::JoinType::JOIN_RIGHT if join.isNatural => "NATURAL RIGHT JOIN",
            pg_sys::JoinType::JOIN_RIGHT => "RIGHT JOIN",
            _ => "JOIN",
        };

        if join.isNatural || join.quals.is_null() {
            Ok(format!("{left} {join_type} {right}"))
        } else {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let condition = unsafe { node_to_expr(join.quals)? };
            Ok(format!(
                "{left} {join_type} {right} ON {}",
                condition.to_sql()
            ))
        }
    } else if let Some(sub) = cast_node!(node, T_RangeSubselect, pg_sys::RangeSubselect) {
        if sub.subquery.is_null()
            // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
            || !is_node_type!(sub.subquery, T_SelectStmt)
        {
            return Err(PgTrickleError::QueryParseError(
                "RangeSubselect without valid SelectStmt".into(),
            ));
        }
        let sub_stmt = sub.subquery as *const pg_sys::SelectStmt;
        // Recurse into the subquery to replace any view references there too
        let inner_sql = deparse_select_stmt_with_view_subs(sub_stmt, subs)?;

        let lateral_kw = if sub.lateral { "LATERAL " } else { "" };
        let mut result = format!("{lateral_kw}({inner_sql})");
        if !sub.alias.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let a = pg_deref!(sub.alias);
            let alias_name = pg_cstr_to_str(a.aliasname).unwrap_or("");
            result.push_str(&format!(" AS {alias_name}"));
        }
        Ok(result)
    // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
    } else if is_node_type!(node, T_RangeFunction) {
        // Function in FROM — deparse as-is using existing infrastructure
        // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
        unsafe { deparse_from_item_to_sql(node) }
    // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
    } else if is_node_type!(node, T_JsonTable) {
        // JSON_TABLE — deparse using dedicated deparser
        // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
        unsafe { deparse_from_item_to_sql(node) }
    } else {
        // Fallback
        // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
        unsafe { deparse_from_item_to_sql(node) }
    }
}

/// Deparse a WindowDef to the window specification string (inside parentheses).
///
/// # Safety
/// Caller must ensure `wdef` points to a valid `WindowDef`.
fn deparse_window_def(wdef: &pg_sys::WindowDef) -> Result<String, PgTrickleError> {
    let mut parts = Vec::new();

    // PARTITION BY
    let part_list = pg_list::<pg_sys::Node>(wdef.partitionClause);
    if !part_list.is_empty() {
        let mut exprs = Vec::new();
        for node_ptr in part_list.iter_ptr() {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let expr = unsafe { node_to_expr(node_ptr)? };
            exprs.push(expr.to_sql());
        }
        parts.push(format!("PARTITION BY {}", exprs.join(", ")));
    }

    // ORDER BY
    let sort_list = pg_list::<pg_sys::Node>(wdef.orderClause);
    if !sort_list.is_empty() {
        // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
        let sorts = unsafe { deparse_sort_clause(&sort_list)? };
        parts.push(format!("ORDER BY {sorts}"));
    }

    Ok(parts.join(" "))
}

/// Reject materialized views in the defining query for DIFFERENTIAL mode.
///
/// Materialized views are stale snapshots — CDC triggers cannot track
/// `REFRESH MATERIALIZED VIEW`. The user should use the underlying query
/// directly or switch to FULL refresh mode.
pub fn reject_materialized_views(query: &str) -> Result<(), PgTrickleError> {
    let select = match parse_first_select(query)? {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        Some(s) => pg_deref!(s),
        None => return Ok(()),
    };
    check_for_matviews_or_foreign(select)
}

/// Reject foreign tables in the defining query for DIFFERENTIAL mode.
///
/// Foreign tables do not support row-level triggers, so CDC cannot track them.
pub fn reject_foreign_tables(query: &str) -> Result<(), PgTrickleError> {
    // Uses the same implementation as reject_materialized_views;
    // the check function handles both.
    reject_materialized_views(query)
}

/// Recursively check a SelectStmt for materialized view or foreign table
/// references in the FROM clause.
///
/// # Safety
/// Caller must ensure `select` points to a valid `SelectStmt`.
fn check_for_matviews_or_foreign(select: *const pg_sys::SelectStmt) -> Result<(), PgTrickleError> {
    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let s = pg_deref!(select);

    // Handle set operations
    if s.op != pg_sys::SetOperation::SETOP_NONE {
        if !s.larg.is_null() {
            check_for_matviews_or_foreign(s.larg)?;
        }
        if !s.rarg.is_null() {
            check_for_matviews_or_foreign(s.rarg)?;
        }
        return Ok(());
    }

    let from_list = pg_list::<pg_sys::Node>(s.fromClause);
    for node_ptr in from_list.iter_ptr() {
        check_from_item_for_matview_or_foreign(node_ptr)?;
    }

    Ok(())
}

/// Check a single FROM item for materialized view or foreign table references.
///
/// # Safety
/// Caller must ensure `node` points to a valid parse tree node.
fn check_from_item_for_matview_or_foreign(node: *mut pg_sys::Node) -> Result<(), PgTrickleError> {
    if node.is_null() {
        return Ok(());
    }

    if let Some(rv) = cast_node!(node, T_RangeVar, pg_sys::RangeVar) {
        if rv.relname.is_null() {
            return Ok(());
        }
        let relname = pg_cstr_to_str(rv.relname).unwrap_or("").to_string();

        // Skip CTE names / subquery aliases that are not real relations.
        let schema = match resolve_rangevar_schema(rv)? {
            Some(s) => s,
            None => return Ok(()),
        };
        let relkind = resolve_relkind(&schema, &relname)?;

        match relkind.as_deref() {
            Some("m") if !crate::config::pg_trickle_matview_polling() => {
                return Err(PgTrickleError::UnsupportedOperator(format!(
                    "Materialized view '{schema}.{relname}' cannot be used as a source in \
                     DIFFERENTIAL mode. Materialized views are stale snapshots — CDC triggers \
                     cannot track REFRESH MATERIALIZED VIEW. Use the underlying query directly, \
                     or switch to FULL refresh mode. \
                     Alternatively, enable polling-based CDC with: \
                     SET pg_trickle.matview_polling = on;"
                )));
            }
            Some("f") if !crate::config::pg_trickle_foreign_table_polling() => {
                return Err(PgTrickleError::UnsupportedOperator(format!(
                    "Foreign table '{schema}.{relname}' cannot be used as a source in \
                     DIFFERENTIAL or IMMEDIATE mode. Row-level triggers cannot be created \
                     on foreign tables. Use FULL refresh mode instead, which re-queries \
                     the foreign table on each refresh cycle. For postgres_fdw tables, \
                     consider using IMPORT FOREIGN SCHEMA to keep the local schema in sync. \
                     Alternatively, enable polling-based CDC with: \
                     SET pg_trickle.foreign_table_polling = on;"
                )));
            }
            _ => {}
        }
    } else if let Some(join) = cast_node!(node, T_JoinExpr, pg_sys::JoinExpr) {
        check_from_item_for_matview_or_foreign(join.larg)?;
        check_from_item_for_matview_or_foreign(join.rarg)?;
    } else if let Some(sub) = cast_node!(node, T_RangeSubselect, pg_sys::RangeSubselect)
        && !sub.subquery.is_null()
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        && is_node_type!(sub.subquery, T_SelectStmt)
    {
        check_for_matviews_or_foreign(sub.subquery as *const pg_sys::SelectStmt)?;
    }

    Ok(())
}

/// Lightweight check that rejects LIMIT / OFFSET in a defining query.
///
/// Uses `raw_parser()` to parse the query and inspects the top-level
/// `SelectStmt` for `limitCount` / `limitOffset`. This is called at
/// stream table creation time for **all** refresh modes (including FULL),
/// before the full DVM parser runs.
///
/// For set-operation queries (UNION/INTERSECT/EXCEPT), LIMIT/OFFSET on
/// the top-level wrapper is checked. LIMIT inside subqueries or LATERAL
/// is intentionally allowed.
/// Rewrite `SELECT DISTINCT ON (...)` inside `WITH` clause CTE bodies.
///
/// The top-level `rewrite_distinct_on` function only handles `DISTINCT ON` at
/// the outermost SELECT. When a CTE body itself uses `DISTINCT ON`, it escapes
/// the rewrite and the DVM parser later rejects it with `UnsupportedOperator`.
///
/// This helper iterates every non-recursive CTE in the `WITH` clause, deparsed
/// each CTE body that contains `DISTINCT ON` back to SQL, runs
/// `rewrite_distinct_on` recursively on that body, and reassembles the full
/// query with the rewritten bodies.
///
/// Returns the original query string unchanged when no CTE body uses
/// `DISTINCT ON`.
fn rewrite_cte_bodies_for_distinct_on(query: &str) -> Result<String, PgTrickleError> {
    let select = match parse_first_select(query)? {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        Some(s) => pg_deref!(s),
        None => return Ok(query.to_string()),
    };

    // Only applies to plain SELECT with a WITH clause (not set operations).
    if select.withClause.is_null() || select.op != pg_sys::SetOperation::SETOP_NONE {
        return Ok(query.to_string());
    }

    // SAFETY: withClause is non-null, confirmed above.
    let wc = pg_deref!(select.withClause);
    let cte_list = pg_list::<pg_sys::Node>(wc.ctes);

    // First pass: check whether any CTE body uses DISTINCT ON.
    let has_distinct_on_cte = cte_list.iter_ptr().any(|node_ptr| {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if !is_node_type!(node_ptr, T_CommonTableExpr) {
            return false;
        }
        // SAFETY: pgrx::is_a confirmed T_CommonTableExpr.
        let cte = pg_deref!(node_ptr as *const pg_sys::CommonTableExpr);
        if cte.ctequery.is_null()
            // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
            || !is_node_type!(cte.ctequery, T_SelectStmt)
        {
            return false;
        }
        // SAFETY: pgrx::is_a confirmed T_SelectStmt.
        let body = pg_deref!(cte.ctequery as *const pg_sys::SelectStmt);
        if body.distinctClause.is_null() {
            return false;
        }
        let distinct_list = pg_list::<pg_sys::Node>(body.distinctClause);
        // DISTINCT ON has real (non-null) expression nodes; plain DISTINCT uses null nodes.
        distinct_list.iter_ptr().any(|ptr| !ptr.is_null())
    });
    if !has_distinct_on_cte {
        return Ok(query.to_string());
    }

    // Second pass: rebuild the WITH clause with rewritten CTE bodies.
    let mut new_cte_parts: Vec<String> = Vec::new();
    for node_ptr in cte_list.iter_ptr() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if !is_node_type!(node_ptr, T_CommonTableExpr) {
            continue;
        }
        // SAFETY: pgrx::is_a confirmed T_CommonTableExpr.
        let cte = pg_deref!(node_ptr as *const pg_sys::CommonTableExpr);
        let cte_name = pg_cstr_to_str(cte.ctename).unwrap_or("cte").to_string();
        let quoted_name = format!("\"{}\"", cte_name.replace('"', "\"\""));

        let col_aliases = extract_cte_def_colnames(cte)?;
        let alias_part = if col_aliases.is_empty() {
            String::new()
        } else {
            let q: Vec<String> = col_aliases
                .iter()
                .map(|a| format!("\"{}\"", a.replace('"', "\"\"")))
                .collect();
            format!("({})", q.join(", "))
        };

        if !cte.ctequery.is_null()
            // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
            && is_node_type!(cte.ctequery, T_SelectStmt)
        {
            // Deparse the CTE body and recursively rewrite DISTINCT ON.
            // (Handles nested WITH…DISTINCT ON inside the body as well.)
            let body_sql =
                deparse_select_stmt_with_view_subs(cte.ctequery as *const pg_sys::SelectStmt, &[])?;
            let rewritten = rewrite_distinct_on(&body_sql)?;
            new_cte_parts.push(format!("{quoted_name}{alias_part} AS ({rewritten})"));
        } else {
            // Non-SELECT CTE body — should not arise for valid SQL, kept for safety.
            new_cte_parts.push(format!("{quoted_name}{alias_part} AS (SELECT 1)"));
        }
    }

    let recursive = if wc.recursive { "RECURSIVE " } else { "" };
    let new_with = format!("WITH {recursive}{}", new_cte_parts.join(",\n  "));

    // Reconstruct the full query as: <new WITH clause>\n<original main SELECT body>.
    // Both the full deparsed form and the WITH prefix are produced by the same
    // deparsing logic from the same parse tree, so stripping the prefix is exact.
    let full_deparsed = deparse_select_stmt_with_view_subs(select, &[])?;
    let with_prefix = deparse_with_clause_with_view_subs(select.withClause, &[])?;
    let main_body = if let Some(rest) = full_deparsed.strip_prefix(&with_prefix) {
        rest.trim_start().to_string()
    } else {
        // Fallback: should not happen; return the full statement as-is.
        full_deparsed
    };

    Ok(format!("{new_with}\n{main_body}"))
}

/// Detect and rewrite `DISTINCT ON (...)` queries.
///
/// `SELECT DISTINCT ON (e1, e2) col1, col2 FROM t ORDER BY e1, e2, col3`
///
/// is rewritten to:
///
/// ```sql
/// SELECT col1, col2 FROM (
///   SELECT col1, col2, ROW_NUMBER() OVER (PARTITION BY e1, e2 ORDER BY ...) AS __pgt_rn
///   FROM t
/// ) __pgt_do WHERE __pgt_rn = 1
/// ```
///
/// Also rewrites `DISTINCT ON` inside `WITH` clause CTE bodies so that the
/// DVM parser never encounters unsupported `DISTINCT ON` in a CTE SelectStmt.
///
/// Returns the original query unchanged if it does not use DISTINCT ON.
pub fn rewrite_distinct_on(query: &str) -> Result<String, PgTrickleError> {
    // Rewrite DISTINCT ON inside any CTE bodies first.  The DVM parser calls
    // parse_select_stmt on CTE bodies and rejects DISTINCT ON there; doing the
    // rewrite here lets those CTE bodies through to the differential path.
    let owned = rewrite_cte_bodies_for_distinct_on(query)?;
    let query = owned.as_str();

    let select = match parse_first_select(query)? {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        Some(s) => pg_deref!(s),
        None => return Ok(query.to_string()),
    };

    // Only rewrite if DISTINCT ON (not plain DISTINCT or no DISTINCT)
    if select.distinctClause.is_null() {
        return Ok(query.to_string());
    }
    let distinct_list = pg_list::<pg_sys::Node>(select.distinctClause);
    let has_real_exprs = distinct_list.iter_ptr().any(|ptr| !ptr.is_null());
    if !has_real_exprs {
        // Plain DISTINCT (all NULLs in distinctClause) — no rewrite needed
        return Ok(query.to_string());
    }

    // Set operations with DISTINCT ON don't make sense — skip
    if select.op != pg_sys::SetOperation::SETOP_NONE {
        return Ok(query.to_string());
    }

    // Extract DISTINCT ON expressions as SQL text
    let mut distinct_on_exprs = Vec::new();
    for node_ptr in distinct_list.iter_ptr() {
        if node_ptr.is_null() {
            continue;
        }
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr_sql = safe_node_to_expr(node_ptr)
            .map(|e| e.to_sql())
            .unwrap_or_else(|_| {
                // Fallback: deparse using PostgreSQL's nodeToString
                "?".to_string()
            });
        if expr_sql != "?" {
            distinct_on_exprs.push(expr_sql);
        }
    }

    if distinct_on_exprs.is_empty() {
        return Ok(query.to_string());
    }

    // F37 (G5.1): Warn when DISTINCT ON is used without ORDER BY.
    // Without ORDER BY, PostgreSQL picks an arbitrary row from each group.
    // The chosen row may differ between FULL and DIFFERENTIAL refresh,
    // causing the stream table to non-deterministically switch rows.
    if select.sortClause.is_null() {
        pgrx::warning!(
            "pg_trickle: DISTINCT ON without ORDER BY produces non-deterministic results. \
             The chosen row per group may differ between FULL and DIFFERENTIAL refresh. \
             Add ORDER BY to ensure deterministic row selection."
        );
    }

    // Extract ORDER BY clause as SQL text (if any)
    let order_by_sql = if !select.sortClause.is_null() {
        let sort_list = pg_list::<pg_sys::Node>(select.sortClause);
        let mut order_parts = Vec::new();
        for node_ptr in sort_list.iter_ptr() {
            if node_ptr.is_null() {
                continue;
            }
            if let Some(sort_by) = cast_node!(node_ptr, T_SortBy, pg_sys::SortBy)
                && !sort_by.node.is_null()
            {
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let expr_sql = safe_node_to_expr(sort_by.node)
                    .map(|e| e.to_sql())
                    .unwrap_or_else(|_| "?".to_string());
                let dir = match sort_by.sortby_dir {
                    pg_sys::SortByDir::SORTBY_DESC => " DESC",
                    pg_sys::SortByDir::SORTBY_ASC => " ASC",
                    _ => "",
                };
                let nulls = match sort_by.sortby_nulls {
                    pg_sys::SortByNulls::SORTBY_NULLS_FIRST => " NULLS FIRST",
                    pg_sys::SortByNulls::SORTBY_NULLS_LAST => " NULLS LAST",
                    _ => "",
                };
                order_parts.push(format!("{expr_sql}{dir}{nulls}"));
            }
        }
        if order_parts.is_empty() {
            None
        } else {
            Some(order_parts.join(", "))
        }
    } else {
        None
    };

    // Extract target list column names/aliases for the outer SELECT
    let target_list = pg_list::<pg_sys::Node>(select.targetList);
    let mut outer_cols = Vec::new();
    let mut inner_target_parts = Vec::new();
    for node_ptr in target_list.iter_ptr() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if node_ptr.is_null() || !is_node_type!(node_ptr, T_ResTarget) {
            continue;
        }
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let rt = pg_deref!(node_ptr as *const pg_sys::ResTarget);

        // Extract alias name
        let alias = if !rt.name.is_null() {
            let name = pg_cstr_to_str(rt.name).unwrap_or("?col");
            Some(name.to_string())
        } else {
            None
        };

        // Extract expression SQL
        if rt.val.is_null() {
            continue;
        }
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr_sql = safe_node_to_expr(rt.val)
            .map(|e| e.to_sql())
            .unwrap_or_else(|_| "?".to_string());

        // For outer SELECT, use the alias if it exists, otherwise the expression
        let outer_name = alias.as_deref().unwrap_or(&expr_sql);
        outer_cols.push(format!("__pgt_do.\"{}\"", outer_name.replace('"', "\"\"")));

        // Inner target: expression AS alias (or just expression)
        if let Some(ref a) = alias {
            inner_target_parts.push(format!("{} AS \"{}\"", expr_sql, a.replace('"', "\"\"")));
        } else {
            inner_target_parts.push(expr_sql);
        }
    }

    if inner_target_parts.is_empty() || outer_cols.is_empty() {
        return Ok(query.to_string());
    }

    // Build the ROW_NUMBER() window function
    let partition_by = distinct_on_exprs.join(", ");
    let order_clause = order_by_sql
        .map(|ob| format!(" ORDER BY {ob}"))
        .unwrap_or_default();
    let row_number =
        format!("ROW_NUMBER() OVER (PARTITION BY {partition_by}{order_clause}) AS __pgt_rn");

    // Reconstruct the inner SELECT (without DISTINCT ON and ORDER BY)
    // We need: FROM clause, WHERE clause, GROUP BY, HAVING
    // The simplest approach: strip "DISTINCT ON (...)" and "ORDER BY ..." from the original query
    // But this is fragile with raw string manipulation. Instead, reconstruct from parts.

    // Extract FROM clause as raw SQL from the original query
    // For robustness, use a subquery approach: wrap the original query minus DISTINCT ON/ORDER BY

    // Build FROM clause SQL from the original parse tree
    let from_sql = extract_from_clause_sql(select)?;
    let where_sql = if select.whereClause.is_null() {
        String::new()
    } else {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let where_expr = safe_node_to_expr(select.whereClause)
            .map(|e| e.to_sql())
            .map_err(|e| {
                PgTrickleError::QueryParseError(format!(
                    "failed to deparse DISTINCT ON WHERE clause: {e}"
                ))
            })?;
        format!(" WHERE {where_expr}")
    };

    // GROUP BY
    let group_sql = if select.groupClause.is_null() {
        String::new()
    } else {
        let group_list = pg_list::<pg_sys::Node>(select.groupClause);
        let mut parts = Vec::new();
        for node_ptr in group_list.iter_ptr() {
            if node_ptr.is_null() {
                continue;
            }
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            if let Ok(expr) = safe_node_to_expr(node_ptr) {
                parts.push(expr.to_sql());
            }
        }
        if parts.is_empty() {
            String::new()
        } else {
            format!(" GROUP BY {}", parts.join(", "))
        }
    };

    // HAVING
    let having_sql = if select.havingClause.is_null() {
        String::new()
    } else {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let having_expr = safe_node_to_expr(select.havingClause)
            .map(|e| e.to_sql())
            .map_err(|e| {
                PgTrickleError::QueryParseError(format!(
                    "failed to deparse DISTINCT ON HAVING clause: {e}"
                ))
            })?;
        format!(" HAVING {having_expr}")
    };

    // Build the rewritten query
    let inner_targets = inner_target_parts.join(", ");
    let outer_select = outer_cols.join(", ");
    let rewritten = format!(
        "SELECT {outer_select} FROM (\
         SELECT {inner_targets}, {row_number} \
         FROM {from_sql}{where_sql}{group_sql}{having_sql}\
         ) __pgt_do WHERE __pgt_do.__pgt_rn = 1"
    );

    pgrx::debug1!(
        "[pg_trickle] Rewrote DISTINCT ON query to ROW_NUMBER(): {}",
        rewritten
    );

    Ok(rewritten)
}

// ── GROUPING SETS / CUBE / ROLLUP → UNION ALL rewrite ──────────────

/// Rewrite a query using `GROUPING SETS`, `CUBE`, or `ROLLUP` into an
/// equivalent `UNION ALL` of separate `GROUP BY` queries.
///
/// This is called **before** the DVM parser so the downstream operator tree
/// only ever sees plain `GROUP BY` + `UNION ALL` — no new OpTree variants
/// are needed.
///
/// # Algorithm
///
/// 1. Detect `T_GroupingSet` nodes in the raw parse tree's `groupClause`.
/// 2. Expand `CUBE`/`ROLLUP` to their equivalent list of grouping sets.
/// 3. Collect any plain `GROUP BY` columns (always included in every set).
/// 4. For each grouping set, build a `SELECT … GROUP BY <set_cols>` where:
///    - Column references to non-grouped columns are replaced with `NULL`.
///    - `GROUPING(col, …)` calls are replaced with computed integer literals.
/// 5. Combine all branches with `UNION ALL`.
///
/// If the query contains no grouping sets, it is returned unchanged.
pub fn rewrite_grouping_sets(query: &str) -> Result<String, PgTrickleError> {
    let select = match parse_first_select(query)? {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        Some(s) => pg_deref!(s),
        None => return Ok(query.to_string()),
    };

    // Set operations — don't rewrite
    if select.op != pg_sys::SetOperation::SETOP_NONE {
        return Ok(query.to_string());
    }

    // No GROUP BY at all
    if select.groupClause.is_null() {
        return Ok(query.to_string());
    }

    // ── Scan groupClause to find GroupingSet nodes ──────────────────
    let group_list = pg_list::<pg_sys::Node>(select.groupClause);
    let mut has_grouping_sets = false;
    for node_ptr in group_list.iter_ptr() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if !node_ptr.is_null() && is_node_type!(node_ptr, T_GroupingSet) {
            has_grouping_sets = true;
            break;
        }
    }
    if !has_grouping_sets {
        return Ok(query.to_string());
    }

    // ── Separate plain columns from GroupingSet specifications ──────
    let mut plain_col_exprs: Vec<String> = Vec::new();
    let mut grouping_set_specs: Vec<Vec<Vec<String>>> = Vec::new();

    for node_ptr in group_list.iter_ptr() {
        if node_ptr.is_null() {
            continue;
        }
        if let Some(gs) = cast_node!(node_ptr, T_GroupingSet, pg_sys::GroupingSet) {
            let expanded = expand_grouping_set(gs)?;
            grouping_set_specs.push(expanded);
        } else {
            // Plain GROUP BY expression
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let expr_sql = safe_node_to_expr(node_ptr)
                .map(|e| e.output_name())
                .unwrap_or_else(|_| "?".to_string());
            plain_col_exprs.push(expr_sql);
        }
    }

    // ── Cross-product all grouping set specifications ───────────────
    // GROUP BY a, ROLLUP(b, c), CUBE(d) → cross product of the
    // ROLLUP sets and CUBE sets, with `a` always included.
    let mut final_sets: Vec<Vec<String>> = vec![vec![]];
    for spec_sets in &grouping_set_specs {
        let mut new_final = Vec::new();
        for existing in &final_sets {
            for new_set in spec_sets {
                let mut combined = existing.clone();
                combined.extend(new_set.iter().cloned());
                new_final.push(combined);
            }
        }
        final_sets = new_final;

        // Guard against combinatorial explosion: CUBE(n) produces 2^n branches,
        // ROLLUP(n) produces n+1, combined CUBE+ROLLUP can easily exceed memory.
        // Reject early rather than building a query too large for PG to parse (G5.2).
        // EC-02: Limit is configurable via pg_trickle.max_grouping_set_branches GUC.
        let max_grouping_branches = crate::config::pg_trickle_max_grouping_set_branches();
        if final_sets.len() > max_grouping_branches {
            return Err(PgTrickleError::QueryParseError(format!(
                "CUBE/ROLLUP generates {} grouping set branches which exceeds the limit of {}. \
                 Use explicit GROUPING SETS(...) to enumerate only the required combinations, \
                 or raise pg_trickle.max_grouping_set_branches.",
                final_sets.len(),
                max_grouping_branches
            )));
        }
    }

    // Prepend plain columns to every set
    if !plain_col_exprs.is_empty() {
        for set in &mut final_sets {
            let mut with_plain = plain_col_exprs.clone();
            with_plain.append(set);
            *set = with_plain;
        }
    }

    // Collect the union of all grouping columns (for NULL substitution)
    let all_grouping_cols: Vec<String> = {
        let mut all = std::collections::HashSet::new();
        for set in &final_sets {
            for col in set {
                all.insert(col.clone());
            }
        }
        all.into_iter().collect()
    };

    // ── Extract query components as SQL text ────────────────────────
    let from_sql = extract_from_clause_sql(select)?;

    let where_sql = if select.whereClause.is_null() {
        String::new()
    } else {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let where_expr = safe_node_to_expr(select.whereClause)
            .map(|e| e.to_sql())
            .map_err(|e| {
                PgTrickleError::QueryParseError(format!(
                    "failed to deparse GROUPING SETS WHERE clause: {e}"
                ))
            })?;
        format!(" WHERE {where_expr}")
    };

    let having_sql = if select.havingClause.is_null() {
        String::new()
    } else {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let having_expr = safe_node_to_expr(select.havingClause)
            .map(|e| e.to_sql())
            .map_err(|e| {
                PgTrickleError::QueryParseError(format!(
                    "failed to deparse GROUPING SETS HAVING clause: {e}"
                ))
            })?;
        format!(" HAVING {having_expr}")
    };

    // ── Parse target list ──────────────────────────────────────────
    let target_list = pg_list::<pg_sys::Node>(select.targetList);
    let mut targets: Vec<TargetEntry> = Vec::new();

    for node_ptr in target_list.iter_ptr() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if node_ptr.is_null() || !is_node_type!(node_ptr, T_ResTarget) {
            continue;
        }
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let rt = pg_deref!(node_ptr as *const pg_sys::ResTarget);
        if rt.val.is_null() {
            continue;
        }

        let alias = if !rt.name.is_null() {
            Some(pg_cstr_to_str(rt.name).unwrap_or("?").to_string())
        } else {
            None
        };

        // Check if target expression is a GROUPING() call
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if is_node_type!(rt.val, T_GroupingFunc) {
            // SAFETY: confirmed T_GroupingFunc
            let gf = pg_deref!(rt.val as *const pg_sys::GroupingFunc);
            let gf_args = extract_grouping_func_args(gf)?;
            targets.push(TargetEntry {
                kind: TargetKind::GroupingFunc(gf_args),
                alias,
            });
            continue;
        }

        // Parse as expression
        let expr =
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            safe_node_to_expr(rt.val).unwrap_or_else(|_| Expr::Raw("NULL".to_string()));

        // Check if this is a simple column ref that matches a grouping column
        let col_name = match &expr {
            Expr::ColumnRef { column_name, .. } => Some(column_name.clone()),
            _ => None,
        };

        if let Some(ref cn) = col_name
            && all_grouping_cols.iter().any(|gc| gc == cn)
        {
            targets.push(TargetEntry {
                kind: TargetKind::GroupingColumn(cn.clone(), expr),
                alias,
            });
            continue;
        }

        // Aggregate or other expression — emit as-is
        targets.push(TargetEntry {
            kind: TargetKind::Expression(expr),
            alias,
        });
    }

    // ── Build UNION ALL branches ───────────────────────────────────
    let mut branches: Vec<String> = Vec::new();

    for current_set in &final_sets {
        let mut select_parts: Vec<String> = Vec::new();

        for target in &targets {
            let sql = match &target.kind {
                TargetKind::GroupingFunc(args) => {
                    let value = compute_grouping_value(args, current_set);
                    let alias_part = target
                        .alias
                        .as_ref()
                        .map(|a| format!(" AS \"{}\"", a.replace('"', "\"\"")))
                        .unwrap_or_default();
                    format!("{value}{alias_part}")
                }
                TargetKind::GroupingColumn(col_name, expr) => {
                    let in_set = current_set.iter().any(|c| c == col_name);
                    let alias_part = match &target.alias {
                        Some(a) => format!(" AS \"{}\"", a.replace('"', "\"\"")),
                        None => format!(" AS \"{}\"", col_name.replace('"', "\"\"")),
                    };
                    if in_set {
                        format!("{}{alias_part}", expr.to_sql())
                    } else {
                        format!("NULL{alias_part}")
                    }
                }
                TargetKind::Expression(expr) => {
                    let alias_part = target
                        .alias
                        .as_ref()
                        .map(|a| format!(" AS \"{}\"", a.replace('"', "\"\"")))
                        .unwrap_or_default();
                    format!("{}{alias_part}", expr.to_sql())
                }
            };
            select_parts.push(sql);
        }

        let group_by_sql = if current_set.is_empty() {
            String::new()
        } else {
            let cols: Vec<String> = current_set
                .iter()
                .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
                .collect();
            format!(" GROUP BY {}", cols.join(", "))
        };

        branches.push(format!(
            "SELECT {} FROM {from_sql}{where_sql}{group_by_sql}{having_sql}",
            select_parts.join(", ")
        ));
    }

    let rewritten = branches.join(" UNION ALL ");

    pgrx::debug1!(
        "[pg_trickle] Rewrote GROUPING SETS query to UNION ALL: {}",
        rewritten
    );

    Ok(rewritten)
}

/// A target list entry classified for grouping-set rewriting.
struct TargetEntry {
    kind: TargetKind,
    alias: Option<String>,
}

/// Classification of a SELECT-list expression for grouping-set rewriting.
enum TargetKind {
    /// A reference to a grouping column (may be NULLified per branch).
    GroupingColumn(String, Expr),
    /// A `GROUPING(col, …)` call (replaced with a literal per branch).
    GroupingFunc(Vec<String>),
    /// An aggregate or other expression (emitted as-is in every branch).
    Expression(Expr),
}

/// Expand a `GroupingSet` node into a list of grouping sets (each set
/// is a `Vec<String>` of column names).
fn expand_grouping_set(gs: &pg_sys::GroupingSet) -> Result<Vec<Vec<String>>, PgTrickleError> {
    let columns = extract_grouping_set_columns(gs)?;

    match gs.kind {
        pg_sys::GroupingSetKind::GROUPING_SET_EMPTY => Ok(vec![vec![]]),

        pg_sys::GroupingSetKind::GROUPING_SET_SIMPLE => Ok(vec![columns]),

        pg_sys::GroupingSetKind::GROUPING_SET_ROLLUP => {
            // ROLLUP(a, b, c) → [(a,b,c), (a,b), (a), ()]
            let mut sets = Vec::new();
            for i in (0..=columns.len()).rev() {
                sets.push(columns[..i].to_vec());
            }
            Ok(sets)
        }

        pg_sys::GroupingSetKind::GROUPING_SET_CUBE => {
            // CUBE(a, b, c) → all 2^n subsets, ordered largest-first
            let n = columns.len();
            let mut sets = Vec::new();
            // Iterate from (2^n - 1) down to 0 for largest-first ordering
            for mask in (0..(1u64 << n)).rev() {
                let mut set = Vec::new();
                for (i, col) in columns.iter().enumerate() {
                    if mask & (1u64 << (n - 1 - i)) != 0 {
                        set.push(col.clone());
                    }
                }
                sets.push(set);
            }
            Ok(sets)
        }

        pg_sys::GroupingSetKind::GROUPING_SET_SETS => {
            // GROUPING SETS ((a, b), (c), ()) — content is a list of
            // nested GroupingSet nodes (SIMPLE or EMPTY).
            if gs.content.is_null() {
                return Ok(vec![vec![]]);
            }
            let content_list = pg_list::<pg_sys::Node>(gs.content);
            let mut sets = Vec::new();
            for node_ptr in content_list.iter_ptr() {
                if node_ptr.is_null() {
                    continue;
                }
                if let Some(inner) = cast_node!(node_ptr, T_GroupingSet, pg_sys::GroupingSet) {
                    let inner_sets = expand_grouping_set(inner)?;
                    sets.extend(inner_sets);
                } else {
                    // Single column expression
                    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                    let col_sql = safe_node_to_expr(node_ptr)
                        .map(|e| e.output_name())
                        .unwrap_or_else(|_| "?".to_string());
                    sets.push(vec![col_sql]);
                }
            }
            Ok(sets)
        }

        _ => Err(PgTrickleError::UnsupportedOperator(format!(
            "Unknown GroupingSet kind: {:?}",
            gs.kind
        ))),
    }
}

/// Extract column names from a `GroupingSet`'s content list.
fn extract_grouping_set_columns(gs: &pg_sys::GroupingSet) -> Result<Vec<String>, PgTrickleError> {
    if gs.content.is_null() {
        return Ok(vec![]);
    }
    let content_list = pg_list::<pg_sys::Node>(gs.content);
    let mut cols = Vec::new();
    for node_ptr in content_list.iter_ptr() {
        if node_ptr.is_null() {
            continue;
        }
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let col_sql = safe_node_to_expr(node_ptr)
            .map(|e| e.output_name())
            .unwrap_or_else(|_| "?".to_string());
        cols.push(col_sql);
    }
    Ok(cols)
}

/// Extract `GROUPING(col, …)` argument column names from a raw-parse
/// `GroupingFunc` node.
fn extract_grouping_func_args(gf: &pg_sys::GroupingFunc) -> Result<Vec<String>, PgTrickleError> {
    if gf.args.is_null() {
        return Ok(vec![]);
    }
    let args_list = pg_list::<pg_sys::Node>(gf.args);
    let mut names = Vec::new();
    for node_ptr in args_list.iter_ptr() {
        if node_ptr.is_null() {
            continue;
        }
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let name = safe_node_to_expr(node_ptr)
            .map(|e| e.output_name())
            .unwrap_or_else(|_| "?".to_string());
        names.push(name);
    }
    Ok(names)
}

/// Compute the `GROUPING(col1, col2, …)` integer value for a given
/// grouping set. Returns a bitmask where bit *i* (MSB-first) is 1 if
/// the *i*-th argument is **not** in the current grouping set.
pub(crate) fn compute_grouping_value(args: &[String], current_set: &[String]) -> i64 {
    let mut value: i64 = 0;
    for arg in args {
        value <<= 1;
        if !current_set.iter().any(|c| c == arg) {
            value |= 1; // column is aggregated (not grouped) → 1
        }
    }
    value
}

// ── Scalar subquery in WHERE → CROSS JOIN rewrite ──────────────────

/// Rewrite scalar subqueries in the WHERE clause into CROSS JOINs.
///
/// ```sql
/// -- Input:
/// SELECT * FROM orders WHERE amount > (SELECT avg(amount) FROM orders)
/// -- Rewrite to:
/// SELECT * FROM orders
/// CROSS JOIN (SELECT avg(amount) AS __pgt_scalar_1 FROM orders) AS __pgt_sq_1
/// WHERE amount > __pgt_sq_1.__pgt_scalar_1
/// ```
///
/// This is called **before** the DVM parser so the downstream operator tree
/// only sees a simple CROSS JOIN + Filter — no special scalar-subquery-in-WHERE
/// handling is needed.
///
/// Only handles EXPR_SUBLINK (scalar subqueries) in the top-level WHERE clause
/// (both bare and under AND/OR conjunctions). Correlated scalar subqueries
/// are NOT rewritten (they reference outer columns).
pub fn rewrite_scalar_subquery_in_where(query: &str) -> Result<String, PgTrickleError> {
    let select = match parse_first_select(query)? {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        Some(s) => pg_deref!(s),
        None => return Ok(query.to_string()),
    };

    // Set operations — don't rewrite (the individual branches are separate SELECTs)
    if select.op != pg_sys::SetOperation::SETOP_NONE {
        return Ok(query.to_string());
    }

    // No WHERE clause — nothing to rewrite
    if select.whereClause.is_null() {
        return Ok(query.to_string());
    }

    // Collect scalar subqueries from WHERE clause
    let mut scalar_subqueries: Vec<ScalarSubqueryExtract> = Vec::new();
    collect_scalar_sublinks_in_where(select.whereClause, &mut scalar_subqueries)?;

    if scalar_subqueries.is_empty() {
        return Ok(query.to_string());
    }

    // Check for correlated subqueries — skip rewriting those (they reference
    // outer columns and can't be trivially cross-joined).
    // Detect correlation by checking if the subquery's WHERE references
    // column names from tables in the outer FROM clause.
    let outer_tables = collect_from_clause_table_names(select);

    // Classify each scalar subquery:
    //   1. Dot-qualified correlated (existing heuristic) → skip entirely
    //   2. Bare-column correlated (catalog-detected)   → decorrelate to INNER JOIN
    //   3. Non-correlated                              → CROSS JOIN
    let mut non_correlated: Vec<&ScalarSubqueryExtract> = Vec::new();
    let mut to_decorrelate: Vec<(
        &ScalarSubqueryExtract,
        std::collections::HashMap<String, String>,
    )> = Vec::new();

    for sq in &scalar_subqueries {
        if sq.is_correlated(&outer_tables) {
            // Dot-qualified correlation (e.g., "outer_table.col") — skip
            continue;
        }
        let outer_cols = detect_correlation_columns(sq, &outer_tables);
        if outer_cols.is_empty() {
            non_correlated.push(sq);
        } else {
            to_decorrelate.push((sq, outer_cols));
        }
    }

    if non_correlated.is_empty() && to_decorrelate.is_empty() {
        return Ok(query.to_string());
    }

    // ── Build rewritten query components ─────────────────────────────

    // FROM clause
    let from_sql = extract_from_clause_sql(select)?;

    // Unified index counter for all subquery aliases
    let mut next_idx = 1usize;

    // Build CROSS JOIN additions for each non-correlated scalar subquery.
    // The wrapper SELECT uses the original scalar subquery as a derived table
    // in its FROM clause, so the DVM parser (which requires FROM) can handle it.
    //
    // Format: CROSS JOIN (SELECT v."c" AS "scalar_alias"
    //                      FROM (inner_sql) AS v("c")) AS "sq_alias"
    let mut extra_joins: Vec<String> = Vec::new();
    let mut extra_from_items: Vec<String> = Vec::new();
    let mut extra_where_parts: Vec<String> = Vec::new();
    let mut where_replacements: Vec<(String, String)> = Vec::new();

    for sq in &non_correlated {
        let idx = next_idx;
        next_idx += 1;
        let sq_alias = format!("__pgt_sq_{idx}");
        let scalar_alias = format!("__pgt_scalar_{idx}");

        if sq.has_limit_or_offset {
            // Scalar subqueries with LIMIT/OFFSET cannot be nested inside a
            // derived-table wrapper (the DVM parser rejects LIMIT in FROM-clause
            // subqueries).  Use CROSS JOIN LATERAL instead: the inner SQL is
            // evaluated as-is (LIMIT preserved) and the DVM registers the inner
            // source in `subquery_source_oids`, enabling diff_lateral_subquery to
            // re-evaluate ALL outer rows when the scalar source changes.
            extra_joins.push(format!(
                "CROSS JOIN LATERAL ({inner_sql}) AS \"{sq_alias}\"(\"{scalar_alias}\")",
                inner_sql = sq.subquery_sql,
            ));
        } else {
            let inner_alias = format!("__pgt_v_{idx}");
            let inner_col = format!("__pgt_c_{idx}");
            // The inner scalar subquery returns exactly 1 column and 1 row.
            // We wrap it so the DVM parser sees a subquery with a valid FROM clause:
            //   (SELECT v."c" AS "scalar" FROM (original_subquery) AS v("c")) AS "sq"
            extra_joins.push(format!(
                "CROSS JOIN (SELECT \"{inner_alias}\".\"{inner_col}\" AS \"{scalar_alias}\" \
                 FROM ({sq_sql}) AS \"{inner_alias}\"(\"{inner_col}\")) AS \"{sq_alias}\"",
                sq_sql = sq.subquery_sql,
            ));
        }
        let replacement = format!("\"{sq_alias}\".\"{scalar_alias}\"");
        where_replacements.push((sq.expr_sql.clone(), replacement));
    }

    // Build comma-joined FROM items for decorrelated correlated subqueries.
    // Decorrelated subqueries are added as comma-separated FROM items (not
    // INNER JOIN) to avoid SQL precedence issues with comma-joins.
    for (sq, outer_cols) in &to_decorrelate {
        let idx = next_idx;
        next_idx += 1;
        match decorrelate_scalar_subquery(sq, outer_cols, idx) {
            Ok(decorrelated) => {
                extra_from_items.push(decorrelated.from_item);
                extra_where_parts.extend(decorrelated.extra_where_conditions);
                where_replacements.push((sq.expr_sql.clone(), decorrelated.scalar_ref));
            }
            Err(e) => {
                pgrx::debug1!(
                    "[pg_trickle] Skipping decorrelation of scalar subquery: {}",
                    e
                );
                // Fall back: skip this subquery
                continue;
            }
        }
    }

    if extra_joins.is_empty() && extra_from_items.is_empty() {
        return Ok(query.to_string());
    }

    // Rewrite the WHERE expression, replacing scalar subquery occurrences
    // with column references to the CROSS/INNER JOIN aliases.
    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
    let where_expr = safe_node_to_expr(select.whereClause)
        .map(|e| e.to_sql())
        .unwrap_or_else(|_| "TRUE".to_string());

    let mut rewritten_where = where_expr;
    for (find, replacement) in &where_replacements {
        rewritten_where = rewritten_where.replace(find, replacement);
    }
    // Append decorrelation join conditions to WHERE
    for cond in &extra_where_parts {
        rewritten_where = format!("{rewritten_where} AND {cond}");
    }

    // Target list
    let target_list = pg_list::<pg_sys::Node>(select.targetList);
    let mut targets = Vec::new();
    for node_ptr in target_list.iter_ptr() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if node_ptr.is_null() || !is_node_type!(node_ptr, T_ResTarget) {
            continue;
        }
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let rt = pg_deref!(node_ptr as *const pg_sys::ResTarget);
        if rt.val.is_null() {
            continue;
        }
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = safe_node_to_expr(rt.val)
            .map(|e| e.to_sql())
            .unwrap_or_else(|_| "NULL".to_string());
        let alias_part = if !rt.name.is_null() {
            let name = pg_cstr_to_str(rt.name).unwrap_or("?");
            format!(" AS \"{}\"", name.replace('"', "\"\""))
        } else {
            String::new()
        };
        targets.push(format!("{expr}{alias_part}"));
    }

    // GROUP BY
    let group_sql = if select.groupClause.is_null() {
        String::new()
    } else {
        let group_list = pg_list::<pg_sys::Node>(select.groupClause);
        if group_list.is_empty() {
            String::new()
        } else {
            let mut groups = Vec::new();
            for node_ptr in group_list.iter_ptr() {
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let expr = safe_node_to_expr(node_ptr)
                    .map(|e| e.to_sql())
                    .unwrap_or_else(|_| "?".to_string());
                groups.push(expr);
            }
            format!(" GROUP BY {}", groups.join(", "))
        }
    };

    // HAVING
    let having_sql = if select.havingClause.is_null() {
        String::new()
    } else {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let having_expr = safe_node_to_expr(select.havingClause)
            .map(|e| e.to_sql())
            .unwrap_or_else(|_| "TRUE".to_string());
        format!(" HAVING {having_expr}")
    };

    // ORDER BY
    let order_sql = if select.sortClause.is_null() {
        String::new()
    } else {
        let sort_list = pg_list::<pg_sys::Node>(select.sortClause);
        if sort_list.is_empty() {
            String::new()
        } else {
            let mut sorts = Vec::new();
            for node_ptr in sort_list.iter_ptr() {
                // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
                if node_ptr.is_null() || !is_node_type!(node_ptr, T_SortBy) {
                    continue;
                }
                // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
                let sb = pg_deref!(node_ptr as *const pg_sys::SortBy);
                if sb.node.is_null() {
                    continue;
                }
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let expr = safe_node_to_expr(sb.node)
                    .map(|e| e.to_sql())
                    .unwrap_or_else(|_| "?".to_string());
                let dir = match sb.sortby_dir {
                    pg_sys::SortByDir::SORTBY_ASC => " ASC",
                    pg_sys::SortByDir::SORTBY_DESC => " DESC",
                    _ => "",
                };
                let nulls = match sb.sortby_nulls {
                    pg_sys::SortByNulls::SORTBY_NULLS_FIRST => " NULLS FIRST",
                    pg_sys::SortByNulls::SORTBY_NULLS_LAST => " NULLS LAST",
                    _ => "",
                };
                sorts.push(format!("{expr}{dir}{nulls}"));
            }
            if sorts.is_empty() {
                String::new()
            } else {
                format!(" ORDER BY {}", sorts.join(", "))
            }
        }
    };

    // DISTINCT
    let distinct_sql = if select.distinctClause.is_null() {
        String::new()
    } else {
        " DISTINCT".to_string()
    };

    // ── Assemble rewritten query ─────────────────────────────────────
    // Build the complete FROM clause: original + comma-joined decorrelated subqueries + CROSS JOINs
    let mut from_parts = vec![from_sql];
    from_parts.extend(extra_from_items.iter().cloned());
    let full_from = from_parts.join(", ");

    let rewritten = format!(
        "SELECT{distinct_sql} {targets} FROM {full_from} {extra_joins} WHERE {rewritten_where}{group_sql}{having_sql}{order_sql}",
        targets = targets.join(", "),
        extra_joins = extra_joins.join(" "),
    );

    pgrx::debug1!(
        "[pg_trickle] Rewrote scalar subquery in WHERE: {}",
        rewritten
    );

    Ok(rewritten)
}

// ── Correlated scalar subquery in SELECT → LEFT JOIN rewrite ───────

/// Rewrite correlated scalar subqueries in the SELECT list into LEFT JOINs.
///
/// ```sql
/// -- Input:
/// SELECT d.name, (SELECT MAX(e.salary) FROM emp e WHERE e.dept_id = d.id) AS max_sal
/// FROM dept d
/// -- Rewrite to:
/// SELECT d.name, "__pgt_sq_1"."__pgt_scalar_1" AS max_sal
/// FROM dept d
/// LEFT JOIN (SELECT e.dept_id AS "__pgt_corr_key_1", MAX(e.salary) AS "__pgt_scalar_1"
///            FROM emp e GROUP BY e.dept_id) AS "__pgt_sq_1"
/// ON d.id = "__pgt_sq_1"."__pgt_corr_key_1"
/// ```
///
/// Non-correlated scalar subqueries are left untouched — they are handled
/// by the `ScalarSubquery` OpTree path during parsing.
///
/// This must run **before** the DVM parser so that the downstream operator
/// tree sees a standard LEFT JOIN instead of a correlated scalar subquery.
pub fn rewrite_correlated_scalar_in_select(query: &str) -> Result<String, PgTrickleError> {
    let select = match parse_first_select(query)? {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        Some(s) => pg_deref!(s),
        None => return Ok(query.to_string()),
    };

    // Skip set operations
    if select.op != pg_sys::SetOperation::SETOP_NONE {
        return Ok(query.to_string());
    }

    // Skip if no FROM clause (can't have correlation without outer tables)
    if select.fromClause.is_null() {
        return Ok(query.to_string());
    }

    let target_list = pg_list::<pg_sys::Node>(select.targetList);

    // Collect scalar subqueries from the SELECT list
    struct SelectScalarSubquery {
        target_idx: usize,
        inner_select: *const pg_sys::SelectStmt,
        subquery_sql: String,
        alias: Option<String>,
    }

    let mut scalar_targets: Vec<SelectScalarSubquery> = Vec::new();
    for (idx, node_ptr) in target_list.iter_ptr().enumerate() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if node_ptr.is_null() || !is_node_type!(node_ptr, T_ResTarget) {
            continue;
        }
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let rt = pg_deref!(node_ptr as *const pg_sys::ResTarget);
        if rt.val.is_null() {
            continue;
        }
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if !is_node_type!(rt.val, T_SubLink) {
            continue;
        }
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let sublink = pg_deref!(rt.val as *const pg_sys::SubLink);
        if sublink.subLinkType != pg_sys::SubLinkType::EXPR_SUBLINK {
            continue;
        }
        if sublink.subselect.is_null()
            // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
            || !is_node_type!(sublink.subselect, T_SelectStmt)
        {
            continue;
        }
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let inner = pg_deref!(sublink.subselect as *const pg_sys::SelectStmt);
        // Only handle simple SELECTs (no set operations)
        if inner.op != pg_sys::SetOperation::SETOP_NONE {
            continue;
        }
        let inner_sql = deparse_select_to_sql(sublink.subselect)?;
        let alias = if !rt.name.is_null() {
            pg_cstr_to_str(rt.name).ok().map(|s| s.to_string())
        } else {
            None
        };
        scalar_targets.push(SelectScalarSubquery {
            target_idx: idx,
            inner_select: inner,
            subquery_sql: inner_sql,
            alias,
        });
    }

    if scalar_targets.is_empty() {
        return Ok(query.to_string());
    }

    // Collect outer FROM table names for correlation detection
    let outer_tables = collect_from_clause_table_names(select);

    // For each scalar target, detect correlation and build decorrelation
    struct SelectScalarDecorrelation {
        target_idx: usize,
        left_join_sql: String,
        scalar_ref: String,
        alias: Option<String>,
    }

    let mut decorrelations: Vec<SelectScalarDecorrelation> = Vec::new();
    let mut next_sq_idx = 1usize;

    for st in &scalar_targets {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let inner = pg_deref!(st.inner_select);

        // Collect inner FROM table names
        let inner_tables = collect_from_clause_table_names(inner);

        // Build ScalarSubqueryExtract for correlation detection
        let sq_extract = ScalarSubqueryExtract {
            subquery_sql: st.subquery_sql.clone(),
            expr_sql: format!("({})", st.subquery_sql),
            inner_tables: inner_tables.clone(),
            // has_limit_or_offset is not used for the correlation check below;
            // the LIMIT guard is applied separately via inner.limitCount.
            has_limit_or_offset: !inner.limitCount.is_null() || !inner.limitOffset.is_null(),
        };

        // Check dot-qualified correlation first (e.g., "d.id" in subquery)
        if !sq_extract.is_correlated(&outer_tables) {
            // Also check bare-column correlation via catalog
            let outer_cols = detect_correlation_columns(&sq_extract, &outer_tables);
            if outer_cols.is_empty() {
                continue; // Not correlated — skip, handled by ScalarSubquery OpTree path
            }
        }

        // ── Decorrelate this correlated scalar subquery ──────────────

        // Correlated scalar subqueries with LIMIT/OFFSET cannot be correctly
        // decorrelated by the standard LEFT JOIN + GROUP BY approach because
        // removing LIMIT changes the semantics (e.g. ORDER BY … LIMIT 1 picks
        // the top row per group; a plain LEFT JOIN with all matching rows is
        // wrong).
        //
        // Instead, rewrite them as LEFT JOIN LATERAL, preserving the full inner
        // SQL (including ORDER BY + LIMIT) so that PostgreSQL evaluates it
        // correctly for each outer row.  The DVM handles LATERAL subqueries via
        // `diff_lateral_subquery`, which correctly re-evaluates ALL affected
        // outer rows when the inner source table changes.
        if !inner.limitCount.is_null() || !inner.limitOffset.is_null() {
            let lat_idx = next_sq_idx;
            next_sq_idx += 1;
            let lat_alias = format!("__pgt_lat_{lat_idx}");
            let scalar_alias = format!("__pgt_scalar_{lat_idx}");

            // Use the original inner SQL verbatim inside a LATERAL subquery.
            // The outer table alias (e.g. `i.category`) is a valid lateral
            // reference and PostgreSQL evaluates it for every outer row.
            let left_join_sql = format!(
                "LEFT JOIN LATERAL ({inner_sql}) AS \"{lat_alias}\"(\"{scalar_alias}\") ON TRUE",
                inner_sql = st.subquery_sql,
            );
            let scalar_ref = format!("\"{lat_alias}\".\"{scalar_alias}\"");

            decorrelations.push(SelectScalarDecorrelation {
                target_idx: st.target_idx,
                left_join_sql,
                scalar_ref,
                alias: st.alias.clone(),
            });
            continue;
        }

        // Parse the inner WHERE to separate correlation from inner conditions
        if inner.whereClause.is_null() {
            continue; // No WHERE → no correlation conditions to extract
        }

        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let inner_where_expr = safe_node_to_expr(inner.whereClause).map_err(|_| {
            PgTrickleError::QueryParseError("Failed to parse inner scalar subquery WHERE".into())
        })?;

        let (corr_pairs, remaining_where) =
            split_exists_correlation(&inner_where_expr, &inner_tables);

        if corr_pairs.is_empty() {
            continue; // Could not extract correlation conditions
        }

        let sq_alias = format!("__pgt_sq_{next_sq_idx}");
        let scalar_alias = format!("__pgt_scalar_{next_sq_idx}");
        next_sq_idx += 1;

        // Extract inner target expressions
        let inner_target_list = pg_list::<pg_sys::Node>(inner.targetList);
        let mut inner_target_exprs: Vec<String> = Vec::new();
        for node_ptr in inner_target_list.iter_ptr() {
            // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
            if node_ptr.is_null() || !is_node_type!(node_ptr, T_ResTarget) {
                continue;
            }
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let rt = pg_deref!(node_ptr as *const pg_sys::ResTarget);
            if rt.val.is_null() {
                continue;
            }
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let expr = safe_node_to_expr(rt.val)
                .map(|e| e.strip_qualifier().to_sql())
                .unwrap_or_else(|_| "NULL".to_string());
            inner_target_exprs.push(expr);
        }

        let scalar_expr = inner_target_exprs.join(", ");

        // Detect if the inner query has aggregates (determines GROUP BY strategy)
        let has_group_by = !inner.groupClause.is_null();
        let has_having = !inner.havingClause.is_null();
        let is_aggregate = has_group_by || has_having || expr_has_aggregate(&scalar_expr);

        // Build SELECT items for decorrelated subquery
        let mut select_items: Vec<String> = Vec::new();
        let mut group_by_items: Vec<String> = Vec::new();
        let mut on_conditions: Vec<String> = Vec::new();

        for (j, (outer_expr, inner_col_name)) in corr_pairs.iter().enumerate() {
            let key_alias = format!("__pgt_corr_key_{}", j + 1);
            let inner_col_unqualified = strip_table_qualifier(inner_col_name);
            select_items.push(format!("{inner_col_unqualified} AS \"{key_alias}\""));
            if is_aggregate {
                group_by_items.push(inner_col_unqualified);
            }
            on_conditions.push(format!(
                "{} = \"{sq_alias}\".\"{key_alias}\"",
                outer_expr.to_sql()
            ));
        }

        select_items.push(format!("{scalar_expr} AS \"{scalar_alias}\""));

        // Inner FROM clause
        let inner_from_sql = extract_from_clause_sql(inner)?;

        // Inner WHERE (non-correlation conditions only)
        let where_sql = if let Some(ref remaining) = remaining_where {
            format!(" WHERE {}", remaining.to_sql())
        } else {
            String::new()
        };

        // GROUP BY
        let group_by_sql = if is_aggregate && !group_by_items.is_empty() {
            // If inner already has GROUP BY, prepend correlation keys
            if has_group_by {
                let existing_groups = pg_list::<pg_sys::Node>(inner.groupClause);
                let mut groups = group_by_items;
                for node_ptr in existing_groups.iter_ptr() {
                    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                    let expr = safe_node_to_expr(node_ptr)
                        .map(|e| e.to_sql())
                        .unwrap_or_else(|_| "?".to_string());
                    groups.push(expr);
                }
                format!(" GROUP BY {}", groups.join(", "))
            } else {
                format!(" GROUP BY {}", group_by_items.join(", "))
            }
        } else {
            String::new()
        };

        // HAVING (preserve from inner if present)
        let having_sql = if has_having {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let having_expr = safe_node_to_expr(inner.havingClause)
                .map(|e| e.to_sql())
                .unwrap_or_else(|_| "TRUE".to_string());
            format!(" HAVING {having_expr}")
        } else {
            String::new()
        };

        // Build LEFT JOIN clause
        let decorrelated_sql = format!(
            "SELECT {selects} FROM {from}{where_clause}{group_by}{having}",
            selects = select_items.join(", "),
            from = inner_from_sql,
            where_clause = where_sql,
            group_by = group_by_sql,
            having = having_sql,
        );

        let left_join_sql = format!(
            "LEFT JOIN ({decorrelated_sql}) AS \"{sq_alias}\" ON {on_cond}",
            on_cond = on_conditions.join(" AND "),
        );

        let scalar_ref = format!("\"{sq_alias}\".\"{scalar_alias}\"");

        decorrelations.push(SelectScalarDecorrelation {
            target_idx: st.target_idx,
            left_join_sql,
            scalar_ref,
            alias: st.alias.clone(),
        });
    }

    if decorrelations.is_empty() {
        return Ok(query.to_string());
    }

    // ── Reconstruct the query ────────────────────────────────────────

    // Build target list, replacing decorrelated scalar targets
    let mut targets = Vec::new();
    let decc_map: std::collections::HashMap<usize, &SelectScalarDecorrelation> =
        decorrelations.iter().map(|d| (d.target_idx, d)).collect();

    for (idx, node_ptr) in target_list.iter_ptr().enumerate() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if node_ptr.is_null() || !is_node_type!(node_ptr, T_ResTarget) {
            continue;
        }
        if let Some(dec) = decc_map.get(&idx) {
            let alias_part = if let Some(ref a) = dec.alias {
                format!(" AS \"{}\"", a.replace('"', "\"\""))
            } else {
                String::new()
            };
            targets.push(format!("{}{alias_part}", dec.scalar_ref));
        } else {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let rt = pg_deref!(node_ptr as *const pg_sys::ResTarget);
            if rt.val.is_null() {
                continue;
            }
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let expr = safe_node_to_expr(rt.val)
                .map(|e| e.to_sql())
                .unwrap_or_else(|_| "NULL".to_string());
            let alias_part = if !rt.name.is_null() {
                let name = pg_cstr_to_str(rt.name).unwrap_or("?");
                format!(" AS \"{}\"", name.replace('"', "\"\""))
            } else {
                String::new()
            };
            targets.push(format!("{expr}{alias_part}"));
        }
    }

    // FROM clause + LEFT JOINs
    let from_sql = extract_from_clause_sql(select)?;
    let left_joins: String = decorrelations
        .iter()
        .map(|d| d.left_join_sql.as_str())
        .collect::<Vec<_>>()
        .join(" ");

    // WHERE clause (outer, unchanged)
    let where_sql = if select.whereClause.is_null() {
        String::new()
    } else {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let w = safe_node_to_expr(select.whereClause)
            .map(|e| e.to_sql())
            .unwrap_or_else(|_| "TRUE".to_string());
        format!(" WHERE {w}")
    };

    // GROUP BY (outer)
    let group_sql = if select.groupClause.is_null() {
        String::new()
    } else {
        let group_list = pg_list::<pg_sys::Node>(select.groupClause);
        if group_list.is_empty() {
            String::new()
        } else {
            let mut groups = Vec::new();
            for node_ptr in group_list.iter_ptr() {
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let expr = safe_node_to_expr(node_ptr)
                    .map(|e| e.to_sql())
                    .unwrap_or_else(|_| "?".to_string());
                groups.push(expr);
            }
            format!(" GROUP BY {}", groups.join(", "))
        }
    };

    // HAVING (outer)
    let having_sql = if select.havingClause.is_null() {
        String::new()
    } else {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let h = safe_node_to_expr(select.havingClause)
            .map(|e| e.to_sql())
            .unwrap_or_else(|_| "TRUE".to_string());
        format!(" HAVING {h}")
    };

    // ORDER BY (outer)
    let order_sql = if select.sortClause.is_null() {
        String::new()
    } else {
        let sort_list = pg_list::<pg_sys::Node>(select.sortClause);
        if sort_list.is_empty() {
            String::new()
        } else {
            let mut sorts = Vec::new();
            for node_ptr in sort_list.iter_ptr() {
                // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
                if node_ptr.is_null() || !is_node_type!(node_ptr, T_SortBy) {
                    continue;
                }
                // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
                let sb = pg_deref!(node_ptr as *const pg_sys::SortBy);
                if sb.node.is_null() {
                    continue;
                }
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let expr = safe_node_to_expr(sb.node)
                    .map(|e| e.to_sql())
                    .unwrap_or_else(|_| "?".to_string());
                let dir = match sb.sortby_dir {
                    pg_sys::SortByDir::SORTBY_ASC => " ASC",
                    pg_sys::SortByDir::SORTBY_DESC => " DESC",
                    _ => "",
                };
                let nulls = match sb.sortby_nulls {
                    pg_sys::SortByNulls::SORTBY_NULLS_FIRST => " NULLS FIRST",
                    pg_sys::SortByNulls::SORTBY_NULLS_LAST => " NULLS LAST",
                    _ => "",
                };
                sorts.push(format!("{expr}{dir}{nulls}"));
            }
            if sorts.is_empty() {
                String::new()
            } else {
                format!(" ORDER BY {}", sorts.join(", "))
            }
        }
    };

    // DISTINCT (outer)
    let distinct_sql = if select.distinctClause.is_null() {
        String::new()
    } else {
        " DISTINCT".to_string()
    };

    let rewritten = format!(
        "SELECT{distinct_sql} {targets} FROM {from_sql} {left_joins}{where_sql}{group_sql}{having_sql}{order_sql}",
        targets = targets.join(", "),
    );

    pgrx::debug1!(
        "[pg_trickle] Rewrote correlated scalar subquery in SELECT: {}",
        rewritten
    );

    Ok(rewritten)
}

/// Check whether a SQL expression string contains common aggregate function calls.
fn expr_has_aggregate(expr: &str) -> bool {
    let lower = expr.to_lowercase();
    const AGG_PREFIXES: &[&str] = &[
        "max(",
        "min(",
        "sum(",
        "count(",
        "avg(",
        "array_agg(",
        "string_agg(",
        "bool_and(",
        "bool_or(",
        "every(",
        "jsonb_agg(",
        "json_agg(",
        "xmlagg(",
        "bit_and(",
        "bit_or(",
    ];
    AGG_PREFIXES.iter().any(|p| lower.contains(p))
}

/// Information about a scalar subquery extracted from the WHERE clause.
pub(crate) struct ScalarSubqueryExtract {
    /// The inner SELECT statement as SQL (e.g., `SELECT avg(amount) FROM orders`).
    pub(crate) subquery_sql: String,
    /// The expression as rendered by `node_to_expr().to_sql()` for text replacement.
    /// e.g. `(SELECT avg("amount") FROM "orders")`
    pub(crate) expr_sql: String,
    /// Table names referenced in the subquery's FROM clause (lowercase).
    pub(crate) inner_tables: Vec<String>,
    /// True if the inner SELECT has a LIMIT or OFFSET clause.  Such subqueries
    /// are skipped by the CROSS-JOIN and decorrelation rewrites so that the DVM
    /// parser treats them as opaque `Expr::Raw` expressions.
    pub(crate) has_limit_or_offset: bool,
}

impl ScalarSubqueryExtract {
    /// Check if this scalar subquery is correlated with the outer query.
    ///
    /// A subquery is considered correlated if its SQL text references any
    /// column that could come from an outer table. We use a heuristic:
    /// if the subquery SQL contains a table name from the outer FROM clause
    /// (as part of a column reference like `t.col` or a bare column matching
    /// the outer table names), we treat it as potentially correlated.
    fn is_correlated(&self, outer_tables: &[String]) -> bool {
        let sq_lower = self.subquery_sql.to_lowercase();
        for outer_table in outer_tables {
            // Skip tables that also appear in the inner FROM clause —
            // those are legitimate inner references, not correlations.
            if self.inner_tables.contains(outer_table) {
                continue;
            }
            // Check if the outer table name appears as a qualified column
            // reference prefix. The deparsed SQL may use either unquoted
            // (`i.col`) or quoted (`"i"."col"`) column references, so we
            // check for both patterns.
            let unquoted = format!("{}.", outer_table);
            let quoted = format!("\"{}\".", outer_table);
            if sq_lower.contains(&unquoted) || sq_lower.contains(&quoted) {
                return true;
            }
        }
        false
    }
}

/// Collect table names (and aliases) from a SelectStmt's FROM clause.
///
/// Returns lowercased table names and alias names for correlation detection.
///
/// # Safety
/// Caller must ensure `select` points to a valid `pg_sys::SelectStmt`.
fn collect_from_clause_table_names(select: &pg_sys::SelectStmt) -> Vec<String> {
    let mut names = Vec::new();
    if select.fromClause.is_null() {
        return names;
    }
    let from_list = pg_list::<pg_sys::Node>(select.fromClause);
    for node_ptr in from_list.iter_ptr() {
        if !node_ptr.is_null() {
            collect_table_names_from_node(node_ptr, &mut names);
        }
    }
    names
}

/// Recursively collect table names and aliases from a FROM item node.
///
/// # Safety
/// Caller must ensure `node` points to a valid `pg_sys::Node`.
fn collect_table_names_from_node(node: *mut pg_sys::Node, out: &mut Vec<String>) {
    if node.is_null() {
        return;
    }
    if let Some(rv) = cast_node!(node, T_RangeVar, pg_sys::RangeVar) {
        if let Ok(s) = pg_cstr_to_str(rv.relname) {
            out.push(s.to_lowercase());
        }
        if !rv.alias.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let a = pg_deref!(rv.alias);
            if let Ok(s) = pg_cstr_to_str(a.aliasname) {
                let lower = s.to_lowercase();
                if !out.contains(&lower) {
                    out.push(lower);
                }
            }
        }
    } else if let Some(join) = cast_node!(node, T_JoinExpr, pg_sys::JoinExpr) {
        if !join.larg.is_null() {
            collect_table_names_from_node(join.larg, out);
        }
        if !join.rarg.is_null() {
            collect_table_names_from_node(join.rarg, out);
        }
    } else if let Some(sub) = cast_node!(node, T_RangeSubselect, pg_sys::RangeSubselect)
        && !sub.alias.is_null()
    {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let a = pg_deref!(sub.alias);
        if let Ok(s) = pg_cstr_to_str(a.aliasname) {
            let lower = s.to_lowercase();
            if !out.contains(&lower) {
                out.push(lower);
            }
        }
    }
}

/// Recursively collect EXPR_SUBLINK nodes from a WHERE clause tree.
///
/// # Safety
/// Caller must ensure `node` points to a valid `pg_sys::Node`.
fn collect_scalar_sublinks_in_where(
    node: *mut pg_sys::Node,
    out: &mut Vec<ScalarSubqueryExtract>,
) -> Result<(), PgTrickleError> {
    if node.is_null() {
        return Ok(());
    }

    if let Some(sublink) = cast_node!(node, T_SubLink, pg_sys::SubLink) {
        if sublink.subLinkType == pg_sys::SubLinkType::EXPR_SUBLINK {
            // Scalar subquery — extract it
            if !sublink.subselect.is_null() {
                let inner_sql = deparse_select_to_sql(sublink.subselect)?;
                let expr_sql = format!("({inner_sql})");

                // Collect table names from the subquery's FROM clause
                // for correlation detection.
                let (inner_tables, has_limit_or_offset) = if let Some(inner_select) =
                    cast_node!(sublink.subselect, T_SelectStmt, pg_sys::SelectStmt)
                {
                    let tables = collect_from_clause_table_names(inner_select);
                    // SAFETY: inner_select is a valid SelectStmt pointer from cast_node!
                    let has_lim =
                        !inner_select.limitCount.is_null() || !inner_select.limitOffset.is_null();
                    (tables, has_lim)
                } else {
                    (Vec::new(), false)
                };

                out.push(ScalarSubqueryExtract {
                    subquery_sql: inner_sql,
                    expr_sql,
                    inner_tables,
                    has_limit_or_offset,
                });
            }
        }
        // Don't recurse into the subquery itself — it's a separate query context
        return Ok(());
    }

    // Recurse into BoolExpr (AND/OR/NOT)
    if let Some(boolexpr) = cast_node!(node, T_BoolExpr, pg_sys::BoolExpr)
        && !boolexpr.args.is_null()
    {
        let args = pg_list::<pg_sys::Node>(boolexpr.args);
        for arg_ptr in args.iter_ptr() {
            if !arg_ptr.is_null() {
                collect_scalar_sublinks_in_where(arg_ptr, out)?;
            }
        }
    }

    // Recurse into comparison operators (A_Expr) since scalar subqueries
    // are typically inside comparisons: `col > (SELECT ...)`
    if let Some(aexpr) = cast_node!(node, T_A_Expr, pg_sys::A_Expr) {
        if !aexpr.lexpr.is_null() {
            collect_scalar_sublinks_in_where(aexpr.lexpr, out)?;
        }
        if !aexpr.rexpr.is_null() {
            collect_scalar_sublinks_in_where(aexpr.rexpr, out)?;
        }
    }

    Ok(())
}

// ── Correlated scalar subquery decorrelation ────────────────────────

/// Information about a decorrelated scalar subquery, ready to be added
/// as a comma-joined subquery in the outer FROM clause with extra WHERE conditions.
struct DecorrelatedSubquery {
    /// The subquery to add to FROM as a comma-separated item.
    /// e.g., `(SELECT ...) AS "__pgt_sq_1"`
    from_item: String,
    /// Additional equality conditions to add to WHERE (decorrelation join).
    /// e.g., `p_partkey = "__pgt_sq_1"."__pgt_corr_key_1"`
    extra_where_conditions: Vec<String>,
    /// The reference to the scalar result column for WHERE replacement.
    /// e.g., `"__pgt_sq_1"."__pgt_scalar_1"`
    scalar_ref: String,
}

/// A correlation condition found in a scalar subquery's WHERE clause.
struct CorrelationCondition {
    /// The column name from the outer query (e.g., `p_partkey`).
    outer_column: String,
    /// The full inner column expression as SQL (the side that stays in the subquery).
    /// e.g., `ps_partkey` or `l2.l_partkey`
    inner_expr_sql: String,
}

/// Detect whether a scalar subquery is correlated with the outer query by
/// looking up column names of outer-only tables (tables in the outer FROM
/// but NOT in the inner FROM) in `pg_catalog`.
///
/// Returns a map from column name → table name for outer columns found in
/// the subquery text. Returns an empty map if not correlated.
fn detect_correlation_columns(
    sq: &ScalarSubqueryExtract,
    outer_tables: &[String],
) -> std::collections::HashMap<String, String> {
    use std::collections::HashMap;

    // Find tables that appear in the outer FROM but NOT in the inner FROM.
    let outer_only: Vec<&String> = outer_tables
        .iter()
        .filter(|t| !sq.inner_tables.contains(t))
        .collect();

    if outer_only.is_empty() {
        return HashMap::new();
    }

    let sq_lower = sq.subquery_sql.to_lowercase();
    let mut outer_columns: HashMap<String, String> = HashMap::new();

    let result = pgrx::Spi::connect(|client| {
        for table_name in &outer_only {
            let query = format!(
                "SELECT a.attname::text \
                 FROM pg_attribute a \
                 JOIN pg_class c ON a.attrelid = c.oid \
                 LEFT JOIN pg_namespace n ON c.relnamespace = n.oid \
                 WHERE c.relname = '{}' \
                 AND (n.nspname = 'public' OR n.nspname = current_schema()) \
                 AND a.attnum > 0 AND NOT a.attisdropped",
                table_name.replace('\'', "''")
            );
            let rows = client.select(&query, None, &[])?;
            for row in rows {
                if let Some(col_name) = row.get::<String>(1)? {
                    let col_lower = col_name.to_lowercase();
                    if contains_word_boundary(&sq_lower, &col_lower) {
                        outer_columns.insert(col_lower, table_name.to_string());
                    }
                }
            }
        }
        Ok::<_, pgrx::spi::Error>(())
    });

    if result.is_err() {
        return std::collections::HashMap::new();
    }
    outer_columns
}

/// Strip a leading table qualifier from a column reference.
///
/// e.g., `"l2.l_partkey"` → `"l_partkey"`, `"ps_partkey"` → `"ps_partkey"`.
fn strip_table_qualifier(expr: &str) -> String {
    if let Some(dot_pos) = expr.find('.') {
        expr[dot_pos + 1..].to_string()
    } else {
        expr.to_string()
    }
}

/// Check if `word` appears as a whole word in `text`.
///
/// A word boundary is defined as the start/end of string or a character
/// that is not alphanumeric and not underscore (since SQL identifiers
/// can contain underscores).
pub(crate) fn contains_word_boundary(text: &str, word: &str) -> bool {
    // An empty search word is present at every position — return true for any text.
    if word.is_empty() {
        return true;
    }
    let text_bytes = text.as_bytes();
    let word_len = word.len();
    let mut start = 0;
    while start + word_len <= text.len() {
        if let Some(pos) = text[start..].find(word) {
            let abs_pos = start + pos;
            let before_ok = abs_pos == 0 || {
                let c = text_bytes[abs_pos - 1];
                !c.is_ascii_alphanumeric() && c != b'_'
            };
            let after_pos = abs_pos + word_len;
            let after_ok = after_pos >= text.len() || {
                let c = text_bytes[after_pos];
                !c.is_ascii_alphanumeric() && c != b'_'
            };
            if before_ok && after_ok {
                return true;
            }
            start = abs_pos + 1;
        } else {
            break;
        }
    }
    false
}

/// Flatten a WHERE clause AND tree into individual conditions.
///
/// Recursively unpacks `BoolExpr(AND, args)` nodes, collecting leaf
/// condition nodes.
///
/// # Safety
/// Caller must ensure `node` points to a valid parse tree `Node`.
fn flatten_and_conditions(node: *mut pg_sys::Node, out: &mut Vec<*mut pg_sys::Node>) {
    if node.is_null() {
        return;
    }
    if let Some(boolexpr) = cast_node!(node, T_BoolExpr, pg_sys::BoolExpr)
        && boolexpr.boolop == pg_sys::BoolExprType::AND_EXPR
        && !boolexpr.args.is_null()
    {
        let args = pg_list::<pg_sys::Node>(boolexpr.args);
        for arg_ptr in args.iter_ptr() {
            if !arg_ptr.is_null() {
                flatten_and_conditions(arg_ptr, out);
            }
        }
        return;
    }
    out.push(node);
}

/// Check if a WHERE condition node is an equality between a bare outer column
/// and an inner expression. Returns the correlation info if so.
///
/// # Safety
/// Caller must ensure `node` points to a valid parse tree `Node`.
fn check_correlation_condition(
    node: *mut pg_sys::Node,
    outer_columns: &std::collections::HashMap<String, String>,
) -> Option<CorrelationCondition> {
    // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
    if !is_node_type!(node, T_A_Expr) {
        return None;
    }
    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let aexpr = pg_deref!(node as *const pg_sys::A_Expr);
    if aexpr.kind != pg_sys::A_Expr_Kind::AEXPR_OP {
        return None;
    }
    // SAFETY: Parse-tree node pointer from raw_parser; valid within current memory context.
    let op_name = unsafe { extract_operator_name(aexpr.name) }.ok()?;
    if op_name != "=" {
        return None;
    }
    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
    let left_expr = safe_node_to_expr(aexpr.lexpr).ok()?;
    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
    let right_expr = safe_node_to_expr(aexpr.rexpr).ok()?;

    // Check if left side is a bare outer column
    if let Expr::ColumnRef {
        table_alias: None,
        ref column_name,
    } = left_expr
        && outer_columns.contains_key(&column_name.to_lowercase())
    {
        return Some(CorrelationCondition {
            outer_column: column_name.clone(),
            inner_expr_sql: right_expr.to_sql(),
        });
    }

    // Check if right side is a bare outer column
    if let Expr::ColumnRef {
        table_alias: None,
        ref column_name,
    } = right_expr
        && outer_columns.contains_key(&column_name.to_lowercase())
    {
        return Some(CorrelationCondition {
            outer_column: column_name.clone(),
            inner_expr_sql: left_expr.to_sql(),
        });
    }

    None
}

/// Decorrelate a correlated scalar subquery into an INNER JOIN.
///
/// Given a correlated scalar subquery like:
/// ```sql
/// (SELECT MIN(ps_supplycost) FROM partsupp, ... WHERE p_partkey = ps_partkey AND ...)
/// ```
///
/// Produces:
/// ```sql
/// INNER JOIN (SELECT ps_partkey AS "__pgt_corr_key_1",
///             MIN(ps_supplycost) AS "__pgt_scalar_1"
///             FROM partsupp, ... WHERE ... GROUP BY ps_partkey)
///     AS "__pgt_sq_1" ON p_partkey = "__pgt_sq_1"."__pgt_corr_key_1"
/// ```
fn decorrelate_scalar_subquery(
    sq: &ScalarSubqueryExtract,
    outer_columns: &std::collections::HashMap<String, String>,
    idx: usize,
) -> Result<DecorrelatedSubquery, PgTrickleError> {
    let sq_alias = format!("__pgt_sq_{idx}");
    let scalar_alias = format!("__pgt_scalar_{idx}");

    // Re-parse the inner subquery to get AST access
    let inner_select = match parse_first_select(sq.subquery_sql.as_str())? {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        Some(s) => pg_deref!(s),
        None => {
            return Err(PgTrickleError::QueryParseError(
                "Scalar subquery is not a SelectStmt".into(),
            ));
        }
    };

    // ── Flatten WHERE into AND-ed conditions ─────────────────────────
    let mut conditions: Vec<*mut pg_sys::Node> = Vec::new();
    if !inner_select.whereClause.is_null() {
        flatten_and_conditions(inner_select.whereClause, &mut conditions);
    }

    // ── Separate correlation vs. regular conditions ──────────────────
    let mut correlations: Vec<CorrelationCondition> = Vec::new();
    let mut regular_conditions: Vec<String> = Vec::new();

    for cond_node in &conditions {
        if let Some(corr) = check_correlation_condition(*cond_node, outer_columns) {
            correlations.push(corr);
        } else {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let expr = safe_node_to_expr(*cond_node)
                .map(|e| e.to_sql())
                .unwrap_or_else(|_| "TRUE".to_string());
            regular_conditions.push(expr);
        }
    }

    if correlations.is_empty() {
        return Err(PgTrickleError::QueryParseError(
            "No correlation conditions found in scalar subquery".into(),
        ));
    }

    // ── Extract the original target list (scalar expression) ─────────
    let target_list = pg_list::<pg_sys::Node>(inner_select.targetList);
    let mut original_target_exprs: Vec<String> = Vec::new();
    for node_ptr in target_list.iter_ptr() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if node_ptr.is_null() || !is_node_type!(node_ptr, T_ResTarget) {
            continue;
        }
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let rt = pg_deref!(node_ptr as *const pg_sys::ResTarget);
        if rt.val.is_null() {
            continue;
        }
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = safe_node_to_expr(rt.val)
            .map(|e| e.to_sql())
            .unwrap_or_else(|_| "NULL".to_string());
        original_target_exprs.push(expr);
    }

    // ── FROM clause (unchanged) ──────────────────────────────────────
    let from_sql = extract_from_clause_sql(inner_select)?;

    // ── Build decorrelated SELECT items ──────────────────────────────
    let mut select_items: Vec<String> = Vec::new();
    let mut group_by_items: Vec<String> = Vec::new();
    let mut extra_where_conds: Vec<String> = Vec::new();

    for (j, corr) in correlations.iter().enumerate() {
        let key_alias = format!("__pgt_corr_key_{}", j + 1);
        // Strip table qualifier from inner expression to avoid alias scoping
        // issues in DVM delta queries (e.g., "l2.l_partkey" → "l_partkey").
        let inner_col_unqualified = strip_table_qualifier(&corr.inner_expr_sql);
        select_items.push(format!("{} AS \"{}\"", inner_col_unqualified, key_alias));
        group_by_items.push(inner_col_unqualified);
        extra_where_conds.push(format!(
            "{} = \"{}\".\"{}\"",
            corr.outer_column, sq_alias, key_alias
        ));
    }

    // The original scalar expression becomes the scalar alias
    let scalar_expr = original_target_exprs.join(", ");
    select_items.push(format!("{} AS \"{}\"", scalar_expr, scalar_alias));

    // ── WHERE clause (without correlation conditions) ─────────────────
    let where_sql = if regular_conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", regular_conditions.join(" AND "))
    };

    // ── GROUP BY ─────────────────────────────────────────────────────
    let group_by_sql = format!(" GROUP BY {}", group_by_items.join(", "));

    // ── Assemble decorrelated subquery ───────────────────────────────
    let decorrelated_sql = format!(
        "SELECT {} FROM {}{}{}",
        select_items.join(", "),
        from_sql,
        where_sql,
        group_by_sql,
    );

    // Use comma-join (not INNER JOIN) to avoid SQL precedence issues
    // when the outer FROM uses comma-separated tables.
    let from_item = format!("({decorrelated_sql}) AS \"{sq_alias}\"");

    let scalar_ref = format!("\"{sq_alias}\".\"{scalar_alias}\"");

    pgrx::debug1!("[pg_trickle] Decorrelated scalar subquery: {}", from_item);

    Ok(DecorrelatedSubquery {
        from_item,
        extra_where_conditions: extra_where_conds,
        scalar_ref,
    })
}

// ── De Morgan normalization for sublink-bearing NOT(AND/OR) ─────────

/// Apply De Morgan's law to expose OR+sublink patterns hidden behind NOT.
///
/// ```sql
/// -- Input:
/// SELECT * FROM t WHERE NOT (x AND NOT EXISTS (SELECT 1 FROM vip WHERE vip.id = t.id))
/// -- Rewrite to:
/// SELECT * FROM t WHERE (NOT (x) OR EXISTS (SELECT 1 FROM vip WHERE vip.id = t.id))
/// ```
///
/// The transformation rules are:
/// - `NOT (a AND b)` → `(NOT a) OR (NOT b)`
/// - `NOT (a OR b)` → `(NOT a) AND (NOT b)`
/// - `NOT NOT a` → `a`
///
/// Only applied when the NOT wraps AND/OR nodes containing SubLink nodes,
/// so that the subsequent `rewrite_sublinks_in_or` pass can convert the
/// resulting OR+sublink into UNION branches.
pub fn rewrite_demorgan_sublinks(query: &str) -> Result<String, PgTrickleError> {
    use std::ffi::CString;

    let c_query = CString::new(query)
        .map_err(|_| PgTrickleError::QueryParseError("Query contains null bytes".into()))?;

    // SAFETY: raw_parser is safe within a PostgreSQL backend with a valid memory context.
    let raw_list =
        // SAFETY: PostgreSQL C function called within a valid backend process and memory context.
        unsafe { pg_sys::raw_parser(c_query.as_ptr(), pg_sys::RawParseMode::RAW_PARSE_DEFAULT) };
    if raw_list.is_null() {
        return Ok(query.to_string());
    }

    // SAFETY: PgList::from_pg is safe for both null and valid list pointers from the parser.
    let list = unsafe { pgrx::PgList::<pg_sys::RawStmt>::from_pg(raw_list) };
    let raw_stmt = match list.head() {
        Some(rs) => rs,
        None => return Ok(query.to_string()),
    };

    let node = pg_deref!(raw_stmt).stmt;
    // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
    if !is_node_type!(node, T_SelectStmt) {
        return Ok(query.to_string());
    }

    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let select = pg_deref!(node as *const pg_sys::SelectStmt);

    if select.op != pg_sys::SetOperation::SETOP_NONE {
        return Ok(query.to_string());
    }

    if select.whereClause.is_null() {
        return Ok(query.to_string());
    }

    // Quick check: does the WHERE clause contain any NOT(AND/OR+sublink)?
    // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
    if !unsafe { where_has_not_demorgan_opportunity(select.whereClause) } {
        return Ok(query.to_string());
    }

    // Deparse the WHERE clause with De Morgan applied.
    // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
    let new_where_sql = unsafe { deparse_with_demorgan(select.whereClause)? };

    // Reconstruct the query with the normalized WHERE clause.
    let from_sql = extract_from_clause_sql(select)?;
    let target_sql = deparse_select_target_list(select);
    let group_sql = deparse_group_clause(select);
    let having_sql = deparse_having_clause(select);
    let order_sql = deparse_order_clause(select);

    let result = format!(
        "SELECT {target_sql} FROM {from_sql} WHERE {new_where_sql}{group_sql}{having_sql}{order_sql}"
    );

    pgrx::debug1!("[pg_trickle] De Morgan normalization: {}", result);

    Ok(result)
}

/// Check whether a WHERE clause tree contains NOT(AND/OR) patterns where
/// De Morgan normalization would expose sublinks for the UNION rewrite.
///
/// # Safety
/// Caller must ensure `node` points to a valid `pg_sys::Node`.
unsafe fn where_has_not_demorgan_opportunity(node: *mut pg_sys::Node) -> bool {
    if node.is_null() {
        return false;
    }
    // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
    if !is_node_type!(node, T_BoolExpr) {
        return false;
    }
    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let be = pg_deref!(node as *const pg_sys::BoolExpr);
    match be.boolop {
        pg_sys::BoolExprType::NOT_EXPR => {
            // SAFETY: PgList::from_pg is safe for both null and valid list pointers from the parser.
            let args = unsafe { pgrx::PgList::<pg_sys::Node>::from_pg(be.args) };
            if let Some(inner) = args.head()
                // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
                && is_node_type!(inner, T_BoolExpr)
            {
                // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
                let inner_be = pg_deref!(inner as *const pg_sys::BoolExpr);
                // NOT(AND/OR with sublinks) or NOT(NOT with sublinks)
                if node_tree_contains_sublink(inner) {
                    return matches!(
                        inner_be.boolop,
                        pg_sys::BoolExprType::AND_EXPR
                            | pg_sys::BoolExprType::OR_EXPR
                            | pg_sys::BoolExprType::NOT_EXPR
                    );
                }
            }
            false
        }
        pg_sys::BoolExprType::AND_EXPR | pg_sys::BoolExprType::OR_EXPR => {
            // Recurse into children.
            // SAFETY: PgList::from_pg is safe for both null and valid list pointers from the parser.
            let args = unsafe { pgrx::PgList::<pg_sys::Node>::from_pg(be.args) };
            for arg in args.iter_ptr() {
                // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
                if unsafe { where_has_not_demorgan_opportunity(arg) } {
                    return true;
                }
            }
            false
        }
        _ => false,
    }
}

/// Deparse a WHERE clause node, applying De Morgan normalization to any
/// NOT(AND/OR) nodes that contain sublinks.
///
/// Non-NOT nodes are recursed into (for AND/OR) or deparsed normally.
///
/// # Safety
/// Caller must ensure `node` points to a valid `pg_sys::Node`.
unsafe fn deparse_with_demorgan(node: *mut pg_sys::Node) -> Result<String, PgTrickleError> {
    if node.is_null() {
        return Ok("TRUE".to_string());
    }

    // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
    if !is_node_type!(node, T_BoolExpr) {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        return safe_node_to_expr(node).map(|e| e.to_sql());
    }

    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let be = pg_deref!(node as *const pg_sys::BoolExpr);
    match be.boolop {
        pg_sys::BoolExprType::NOT_EXPR => {
            // SAFETY: PgList::from_pg is safe for both null and valid list pointers from the parser.
            let args = unsafe { pgrx::PgList::<pg_sys::Node>::from_pg(be.args) };
            let inner = args
                .head()
                .ok_or_else(|| PgTrickleError::QueryParseError("Empty NOT expression".into()))?;

            // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
            if is_node_type!(inner, T_BoolExpr) {
                // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
                let inner_be = pg_deref!(inner as *const pg_sys::BoolExpr);

                // NOT(AND(a,b,...)) → (NOT a) OR (NOT b) OR ...
                if inner_be.boolop == pg_sys::BoolExprType::AND_EXPR
                    && node_tree_contains_sublink(inner)
                {
                    let inner_args =
                        // SAFETY: PgList::from_pg is safe for both null and valid list pointers from the parser.
                        unsafe { pgrx::PgList::<pg_sys::Node>::from_pg(inner_be.args) };
                    let mut parts: Vec<String> = Vec::new();
                    for arg in inner_args.iter_ptr() {
                        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
                        parts.push(unsafe { negate_node_sql(arg)? });
                    }
                    return Ok(format!("({})", parts.join(" OR ")));
                }

                // NOT(OR(a,b,...)) → (NOT a) AND (NOT b) AND ...
                if inner_be.boolop == pg_sys::BoolExprType::OR_EXPR
                    && node_tree_contains_sublink(inner)
                {
                    let inner_args =
                        // SAFETY: PgList::from_pg is safe for both null and valid list pointers from the parser.
                        unsafe { pgrx::PgList::<pg_sys::Node>::from_pg(inner_be.args) };
                    let mut parts: Vec<String> = Vec::new();
                    for arg in inner_args.iter_ptr() {
                        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
                        parts.push(unsafe { negate_node_sql(arg)? });
                    }
                    return Ok(format!("({})", parts.join(" AND ")));
                }

                // NOT(NOT(x)) → x
                if inner_be.boolop == pg_sys::BoolExprType::NOT_EXPR {
                    let inner_args =
                        // SAFETY: PgList::from_pg is safe for both null and valid list pointers from the parser.
                        unsafe { pgrx::PgList::<pg_sys::Node>::from_pg(inner_be.args) };
                    if let Some(x) = inner_args.head() {
                        // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
                        return unsafe { deparse_with_demorgan(x) };
                    }
                }
            }

            // NOT without applicable De Morgan — deparse normally.
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            safe_node_to_expr(node).map(|e| e.to_sql())
        }
        pg_sys::BoolExprType::AND_EXPR => {
            // SAFETY: PgList::from_pg is safe for both null and valid list pointers from the parser.
            let args = unsafe { pgrx::PgList::<pg_sys::Node>::from_pg(be.args) };
            let mut parts: Vec<String> = Vec::new();
            for arg in args.iter_ptr() {
                // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
                parts.push(unsafe { deparse_with_demorgan(arg)? });
            }
            Ok(parts.join(" AND "))
        }
        pg_sys::BoolExprType::OR_EXPR => {
            // SAFETY: PgList::from_pg is safe for both null and valid list pointers from the parser.
            let args = unsafe { pgrx::PgList::<pg_sys::Node>::from_pg(be.args) };
            let mut parts: Vec<String> = Vec::new();
            for arg in args.iter_ptr() {
                // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
                parts.push(unsafe { deparse_with_demorgan(arg)? });
            }
            Ok(parts.join(" OR "))
        }
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        _ => safe_node_to_expr(node).map(|e| e.to_sql()),
    }
}

/// Negate a node for De Morgan: NOT(NOT(x)) → x, otherwise NOT (x).
///
/// # Safety
/// Caller must ensure `node` points to a valid `pg_sys::Node`.
unsafe fn negate_node_sql(node: *mut pg_sys::Node) -> Result<String, PgTrickleError> {
    if node.is_null() {
        return Ok("TRUE".to_string());
    }

    // Double negation elimination: NOT(NOT(x)) → x
    // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
    if is_node_type!(node, T_BoolExpr) {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let be = pg_deref!(node as *const pg_sys::BoolExpr);
        if be.boolop == pg_sys::BoolExprType::NOT_EXPR {
            // SAFETY: PgList::from_pg is safe for both null and valid list pointers from the parser.
            let args = unsafe { pgrx::PgList::<pg_sys::Node>::from_pg(be.args) };
            if let Some(inner) = args.head() {
                // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
                return unsafe { deparse_with_demorgan(inner) };
            }
        }
    }

    // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
    let expr = unsafe { deparse_with_demorgan(node)? };
    Ok(format!("NOT ({expr})"))
}

// ── SubLinks inside OR → OR-to-UNION rewrite ───────────────────────

/// Rewrite queries where SubLinks (EXISTS/IN) appear inside OR conditions
/// into a UNION of the individual OR arms.
///
/// ```sql
/// -- Input:
/// SELECT * FROM t WHERE status = 'active' OR EXISTS (SELECT 1 FROM vip WHERE vip.id = t.id)
/// -- Rewrite to:
/// SELECT * FROM t WHERE status = 'active'
/// UNION
/// SELECT t.* FROM t WHERE EXISTS (SELECT 1 FROM vip WHERE vip.id = t.id)
/// ```
///
/// This is called **before** the DVM parser so the downstream operator tree
/// only ever sees non-OR SubLinks which are already handled by the
/// SemiJoin/AntiJoin extraction in `extract_where_sublinks()`.
///
/// The UNION (not UNION ALL) handles deduplication of rows that match
/// multiple OR arms.
pub fn rewrite_sublinks_in_or(query: &str) -> Result<String, PgTrickleError> {
    let select = match parse_first_select(query)? {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        Some(s) => pg_deref!(s),
        None => return Ok(query.to_string()),
    };

    // Set operations — don't rewrite
    if select.op != pg_sys::SetOperation::SETOP_NONE {
        return Ok(query.to_string());
    }

    // No WHERE clause — nothing to rewrite
    if select.whereClause.is_null() {
        return Ok(query.to_string());
    }

    // Check if the top-level WHERE is an OR containing SubLinks
    // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
    if !is_node_type!(select.whereClause, T_BoolExpr) {
        return Ok(query.to_string());
    }

    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let boolexpr = pg_deref!(select.whereClause as *const pg_sys::BoolExpr);

    // Only handle top-level OR, or AND where one of the conjuncts is an OR with sublinks
    if boolexpr.boolop != pg_sys::BoolExprType::OR_EXPR {
        // Check for AND with an inner OR containing sublinks.
        // Guard: only enter the AND rewriter if there is actually an
        // OR conjunct containing SubLink nodes. Without this check,
        // queries like Q04 (AND + EXISTS, no OR) would be unnecessarily
        // deparsed and round-tripped through node_to_expr, losing
        // information like agg_star on COUNT(*).
        if boolexpr.boolop == pg_sys::BoolExprType::AND_EXPR
            && and_contains_or_with_sublink(select.whereClause)
        {
            return rewrite_and_with_or_sublinks(select, select.whereClause);
        }
        return Ok(query.to_string());
    }

    // Top-level OR — check if it contains sublinks
    if !node_tree_contains_sublink(select.whereClause) {
        return Ok(query.to_string());
    }

    // ── Extract OR arms ────────────────────────────────────────────
    let args = pg_list::<pg_sys::Node>(boolexpr.args);
    let mut arm_exprs: Vec<String> = Vec::new();
    for arg_ptr in args.iter_ptr() {
        if arg_ptr.is_null() {
            continue;
        }
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = safe_node_to_expr(arg_ptr)
            .map(|e| e.to_sql())
            .unwrap_or_else(|_| "TRUE".to_string());
        arm_exprs.push(expr);
    }

    if arm_exprs.len() < 2 {
        return Ok(query.to_string());
    }

    // Build query components (shared across all UNION branches)
    let from_sql = extract_from_clause_sql(select)?;

    let target_sql = {
        let target_list = pg_list::<pg_sys::Node>(select.targetList);
        let mut targets = Vec::new();
        for node_ptr in target_list.iter_ptr() {
            // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
            if node_ptr.is_null() || !is_node_type!(node_ptr, T_ResTarget) {
                continue;
            }
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let rt = pg_deref!(node_ptr as *const pg_sys::ResTarget);
            if rt.val.is_null() {
                continue;
            }
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let expr = safe_node_to_expr(rt.val)
                .map(|e| e.to_sql())
                .unwrap_or_else(|_| "NULL".to_string());
            let alias_part = if !rt.name.is_null() {
                let name = pg_cstr_to_str(rt.name).unwrap_or("?");
                format!(" AS \"{}\"", name.replace('"', "\"\""))
            } else {
                String::new()
            };
            targets.push(format!("{expr}{alias_part}"));
        }
        targets.join(", ")
    };

    // GROUP BY / HAVING / ORDER BY
    let group_sql = deparse_group_clause(select);
    let having_sql = deparse_having_clause(select);
    let order_sql = deparse_order_clause(select);

    // Build UNION branches — one per OR arm
    let mut branches: Vec<String> = Vec::new();
    for arm in &arm_exprs {
        branches.push(format!(
            "SELECT {target_sql} FROM {from_sql} WHERE {arm}{group_sql}{having_sql}"
        ));
    }

    let rewritten = format!("{}{order_sql}", branches.join(" UNION "));

    pgrx::debug1!(
        "[pg_trickle] Rewrote SubLinks-in-OR to UNION: {}",
        rewritten
    );

    Ok(rewritten)
}

/// Handle AND conjunction (at any nesting depth) where one or more arms
/// are OR expressions containing sublinks.
///
/// ```sql
/// -- Input (single OR+sublink conjunct):
/// SELECT * FROM t WHERE a > 10 AND (status = 'active' OR EXISTS (...))
/// -- Rewrite to:
/// SELECT * FROM t WHERE a > 10 AND status = 'active'
/// UNION
/// SELECT * FROM t WHERE a > 10 AND EXISTS (...)
///
/// -- Input (multiple OR+sublink conjuncts):
/// SELECT * FROM t WHERE (a OR EXISTS(s1)) AND (b OR NOT EXISTS(s2))
/// -- Rewrite to cartesian product of UNION branches:
/// SELECT * FROM t WHERE a AND b
/// UNION
/// SELECT * FROM t WHERE a AND NOT EXISTS(s2)
/// UNION
/// SELECT * FROM t WHERE EXISTS(s1) AND b
/// UNION
/// SELECT * FROM t WHERE EXISTS(s1) AND NOT EXISTS(s2)
/// ```
///
/// The `where_node` argument is the entire WHERE clause node (typically a
/// BoolExpr AND chain). The function flattens all nested AND layers before
/// searching for OR arms, so arbitrarily deep `AND(AND(OR(...)))` nesting
/// is handled correctly.
///
/// A combinatorial guard limits expansion to 16 UNION branches (4 binary
/// OR+sublink conjuncts). Queries exceeding this produce an error directing
/// the user to simplify the WHERE clause.
fn rewrite_and_with_or_sublinks(
    select: &pg_sys::SelectStmt,
    where_node: *mut pg_sys::Node,
) -> Result<String, PgTrickleError> {
    const MAX_UNION_BRANCHES: usize = 16;

    // Flatten the entire AND chain into a list of atomic conjuncts.
    // AND(a, AND(b, OR(...))) → [a, b, OR(...)]
    let mut conjuncts: Vec<*mut pg_sys::Node> = Vec::new();
    flatten_and_conjuncts(where_node, &mut conjuncts);

    // Separate OR+sublink conjuncts from plain conjuncts.
    let mut or_arms_list: Vec<Vec<String>> = Vec::new();
    let mut other_conjuncts: Vec<String> = Vec::new();

    for arg_ptr in conjuncts {
        if arg_ptr.is_null() {
            continue;
        }
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if is_node_type!(arg_ptr, T_BoolExpr) {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let inner_bool = pg_deref!(arg_ptr as *const pg_sys::BoolExpr);
            if inner_bool.boolop == pg_sys::BoolExprType::OR_EXPR
                && node_tree_contains_sublink(arg_ptr)
            {
                // SAFETY: PgList::from_pg is safe for both null and valid list pointers from the parser.
                let or_args = unsafe { pgrx::PgList::<pg_sys::Node>::from_pg(inner_bool.args) };
                let mut arms = Vec::new();
                for a in or_args.iter_ptr() {
                    if a.is_null() {
                        continue;
                    }
                    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                    let expr = safe_node_to_expr(a)
                        .map(|e| e.to_sql())
                        .unwrap_or_else(|_| "TRUE".to_string());
                    arms.push(expr);
                }
                if arms.len() >= 2 {
                    or_arms_list.push(arms);
                    continue;
                }
            }
        }
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = safe_node_to_expr(arg_ptr)
            .map(|e| e.to_sql())
            .unwrap_or_else(|_| "TRUE".to_string());
        other_conjuncts.push(expr);
    }

    if or_arms_list.is_empty() {
        return deparse_full_select(select);
    }

    // Compute total branch count (cartesian product of all OR arm counts).
    let branch_count: usize = or_arms_list.iter().map(|arms| arms.len()).product();
    if branch_count > MAX_UNION_BRANCHES {
        return Err(PgTrickleError::UnsupportedOperator(format!(
            "Query would produce {branch_count} UNION branches after OR+sublink rewriting \
             (limit: {MAX_UNION_BRANCHES}). Simplify the WHERE clause or use FULL refresh mode.",
        )));
    }

    // Build the common AND prefix from non-OR conjuncts.
    let and_prefix = if other_conjuncts.is_empty() {
        String::new()
    } else {
        format!("{} AND ", other_conjuncts.join(" AND "))
    };

    // Generate the cartesian product of all OR arm combinations.
    // Start with a single empty combination and expand one OR at a time.
    let mut combinations: Vec<Vec<&str>> = vec![vec![]];
    for or_arms in &or_arms_list {
        let mut next = Vec::with_capacity(combinations.len() * or_arms.len());
        for combo in &combinations {
            for arm in or_arms {
                let mut new_combo = combo.clone();
                new_combo.push(arm.as_str());
                next.push(new_combo);
            }
        }
        combinations = next;
    }

    // Build query components
    let from_sql = extract_from_clause_sql(select)?;
    let target_sql = deparse_select_target_list(select);
    let group_sql = deparse_group_clause(select);
    let having_sql = deparse_having_clause(select);
    let order_sql = deparse_order_clause(select);

    let mut branches: Vec<String> = Vec::with_capacity(combinations.len());
    for combo in &combinations {
        let combo_where = combo.join(" AND ");
        branches.push(format!(
            "SELECT {target_sql} FROM {from_sql} WHERE {and_prefix}{combo_where}{group_sql}{having_sql}"
        ));
    }

    let rewritten = format!("{}{order_sql}", branches.join(" UNION "));

    pgrx::debug1!(
        "[pg_trickle] Rewrote AND(..OR-sublinks..) to {} UNION branches: {}",
        branches.len(),
        rewritten
    );

    Ok(rewritten)
}

/// Deparse a full SELECT statement back to SQL text.
fn deparse_full_select(select: &pg_sys::SelectStmt) -> Result<String, PgTrickleError> {
    let from_sql = extract_from_clause_sql(select)?;
    let target_sql = deparse_select_target_list(select);
    let where_sql = if select.whereClause.is_null() {
        String::new()
    } else {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = safe_node_to_expr(select.whereClause)
            .map(|e| e.to_sql())
            .unwrap_or_else(|_| "TRUE".to_string());
        format!(" WHERE {expr}")
    };
    let group_sql = deparse_group_clause(select);
    let having_sql = deparse_having_clause(select);
    let order_sql = deparse_order_clause(select);
    Ok(format!(
        "SELECT {target_sql} FROM {from_sql}{where_sql}{group_sql}{having_sql}{order_sql}"
    ))
}

/// Deparse target list to SQL text (from a SelectStmt reference).
fn deparse_select_target_list(select: &pg_sys::SelectStmt) -> String {
    let target_list = pg_list::<pg_sys::Node>(select.targetList);
    let mut targets = Vec::new();
    for node_ptr in target_list.iter_ptr() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if node_ptr.is_null() || !is_node_type!(node_ptr, T_ResTarget) {
            continue;
        }
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let rt = pg_deref!(node_ptr as *const pg_sys::ResTarget);
        if rt.val.is_null() {
            continue;
        }
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = safe_node_to_expr(rt.val)
            .map(|e| e.to_sql())
            .unwrap_or_else(|_| "NULL".to_string());
        let alias_part = if !rt.name.is_null() {
            let name = pg_cstr_to_str(rt.name).unwrap_or("?");
            format!(" AS \"{}\"", name.replace('"', "\"\""))
        } else {
            String::new()
        };
        targets.push(format!("{expr}{alias_part}"));
    }
    targets.join(", ")
}

/// Deparse GROUP BY clause.
fn deparse_group_clause(select: &pg_sys::SelectStmt) -> String {
    if select.groupClause.is_null() {
        return String::new();
    }
    let group_list = pg_list::<pg_sys::Node>(select.groupClause);
    if group_list.is_empty() {
        return String::new();
    }
    let mut groups = Vec::new();
    for node_ptr in group_list.iter_ptr() {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = safe_node_to_expr(node_ptr)
            .map(|e| e.to_sql())
            .unwrap_or_else(|_| "?".to_string());
        groups.push(expr);
    }
    format!(" GROUP BY {}", groups.join(", "))
}

/// Deparse HAVING clause.
fn deparse_having_clause(select: &pg_sys::SelectStmt) -> String {
    if select.havingClause.is_null() {
        return String::new();
    }
    // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
    let expr = safe_node_to_expr(select.havingClause)
        .map(|e| e.to_sql())
        .unwrap_or_else(|_| "TRUE".to_string());
    format!(" HAVING {expr}")
}

/// Deparse ORDER BY clause.
pub(crate) fn deparse_order_clause(select: &pg_sys::SelectStmt) -> String {
    if select.sortClause.is_null() {
        return String::new();
    }
    let sort_list = pg_list::<pg_sys::Node>(select.sortClause);
    if sort_list.is_empty() {
        return String::new();
    }
    let mut sorts = Vec::new();
    for node_ptr in sort_list.iter_ptr() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if node_ptr.is_null() || !is_node_type!(node_ptr, T_SortBy) {
            continue;
        }
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let sb = pg_deref!(node_ptr as *const pg_sys::SortBy);
        if sb.node.is_null() {
            continue;
        }
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = safe_node_to_expr(sb.node)
            .map(|e| e.to_sql())
            .unwrap_or_else(|_| "?".to_string());
        let dir = match sb.sortby_dir {
            pg_sys::SortByDir::SORTBY_ASC => " ASC",
            pg_sys::SortByDir::SORTBY_DESC => " DESC",
            _ => "",
        };
        let nulls = match sb.sortby_nulls {
            pg_sys::SortByNulls::SORTBY_NULLS_FIRST => " NULLS FIRST",
            pg_sys::SortByNulls::SORTBY_NULLS_LAST => " NULLS LAST",
            _ => "",
        };
        sorts.push(format!("{expr}{dir}{nulls}"));
    }
    if sorts.is_empty() {
        String::new()
    } else {
        format!(" ORDER BY {}", sorts.join(", "))
    }
}

// ── ROWS FROM() multi-function rewrite ─────────────────────────────

/// Rewrite `ROWS FROM(f1(...), f2(...), ...)` into a form the DVM parser
/// supports.
///
/// **All-unnest optimisation:** when every function is `unnest`, merge into
/// a single multi-argument `unnest(A, B, ...)` call — PostgreSQL's built-in
/// multi-arg `unnest()` already implements zip-with-NULL-padding semantics.
///
/// **General case:** rewrite to an ordinal-based LEFT JOIN LATERAL chain:
/// ```sql
/// generate_series(1, 2147483647) WITH ORDINALITY AS __pgt_idx(v, ord)
/// LEFT JOIN LATERAL f1(...) WITH ORDINALITY AS __pgt_f0(col, ord)
///   ON __pgt_f0.ord = __pgt_idx.ord
/// LEFT JOIN LATERAL f2(...) WITH ORDINALITY AS __pgt_f1(col, ord)
///   ON __pgt_f1.ord = __pgt_idx.ord
/// ```
/// with a `WHERE __pgt_f0.ord IS NOT NULL OR __pgt_f1.ord IS NOT NULL`
/// filter to stop the infinite `generate_series` once all SRFs are exhausted.
pub fn rewrite_rows_from(query: &str) -> Result<String, PgTrickleError> {
    let select = match parse_first_select(query)? {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        Some(s) => pg_deref!(s),
        None => return Ok(query.to_string()),
    };

    // Set operations — recurse into each branch.
    if select.op != pg_sys::SetOperation::SETOP_NONE {
        return rewrite_rows_from_in_set_op(select);
    }

    // Walk the FROM clause looking for a multi-function ROWS FROM.
    if select.fromClause.is_null() {
        return Ok(query.to_string());
    }

    let from_list = pg_list::<pg_sys::Node>(select.fromClause);
    let mut found_rows_from = false;
    for node_ptr in from_list.iter_ptr() {
        if node_ptr.is_null() {
            continue;
        }
        if has_multi_rows_from(node_ptr) {
            found_rows_from = true;
            break;
        }
    }

    if !found_rows_from {
        return Ok(query.to_string());
    }

    // ── Extract query components ─────────────────────────────────────

    // SELECT list
    // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
    let target_sql = unsafe { deparse_target_list(select.targetList)? };

    // WHERE clause
    let where_sql = if select.whereClause.is_null() {
        None
    } else {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = unsafe { node_to_expr(select.whereClause)? };
        Some(expr.to_sql())
    };

    // GROUP BY
    let group_list = pg_list::<pg_sys::Node>(select.groupClause);
    let group_sql = if group_list.is_empty() {
        None
    } else {
        let mut groups = Vec::new();
        for gn in group_list.iter_ptr() {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let expr = unsafe { node_to_expr(gn)? };
            groups.push(expr.to_sql());
        }
        Some(groups.join(", "))
    };

    // HAVING
    let having_sql = if select.havingClause.is_null() {
        None
    } else {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = unsafe { node_to_expr(select.havingClause)? };
        Some(expr.to_sql())
    };

    // ORDER BY + LIMIT + OFFSET
    let order_sql = deparse_order_clause(select);
    let limit_sql = if select.limitCount.is_null() {
        None
    } else {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = unsafe { node_to_expr(select.limitCount)? };
        Some(expr.to_sql())
    };
    let offset_sql = if select.limitOffset.is_null() {
        None
    } else {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = unsafe { node_to_expr(select.limitOffset)? };
        Some(expr.to_sql())
    };

    // DISTINCT
    let distinct_list = pg_list::<pg_sys::Node>(select.distinctClause);
    let distinct_prefix = if !distinct_list.is_empty() {
        "SELECT DISTINCT"
    } else {
        "SELECT"
    };

    // ── Rewrite FROM items ──────────────────────────────────────────
    let mut from_parts = Vec::new();
    for node_ptr in from_list.iter_ptr() {
        if node_ptr.is_null() {
            continue;
        }
        let sql = rewrite_from_item_rows_from(node_ptr)?;
        from_parts.push(sql);
    }

    // ── Rebuild query ───────────────────────────────────────────────
    let mut parts = Vec::new();
    parts.push(format!("{distinct_prefix} {target_sql}"));
    parts.push(format!("FROM {}", from_parts.join(", ")));
    if let Some(w) = &where_sql {
        parts.push(format!("WHERE {w}"));
    }
    if let Some(g) = &group_sql {
        parts.push(format!("GROUP BY {g}"));
    }
    if let Some(h) = &having_sql {
        parts.push(format!("HAVING {h}"));
    }
    if !order_sql.is_empty() {
        parts.push(order_sql.trim().to_string());
    }
    if let Some(l) = &limit_sql {
        parts.push(format!("LIMIT {l}"));
    }
    if let Some(o) = &offset_sql {
        parts.push(format!("OFFSET {o}"));
    }

    let rewritten = parts.join(" ");
    pgrx::debug1!("[pg_trickle] rewrite_rows_from: {}", rewritten);
    Ok(rewritten)
}

/// Check if a FROM-clause node is (or contains) a multi-function ROWS FROM.
///
/// # Safety
/// Caller must ensure `node` points to a valid parse tree Node.
fn has_multi_rows_from(node: *mut pg_sys::Node) -> bool {
    if node.is_null() {
        return false;
    }
    if let Some(rf) = cast_node!(node, T_RangeFunction, pg_sys::RangeFunction) {
        let func_list = pg_list::<pg_sys::Node>(rf.functions);
        return rf.is_rowsfrom && func_list.len() > 1;
    }
    if let Some(join) = cast_node!(node, T_JoinExpr, pg_sys::JoinExpr) {
        return has_multi_rows_from(join.larg) || has_multi_rows_from(join.rarg);
    }
    false
}

/// Rewrite a single FROM-clause item, replacing any multi-function ROWS FROM
/// with its rewritten equivalent.
///
/// # Safety
/// Caller must ensure `node` points to a valid parse tree Node.
fn rewrite_from_item_rows_from(node: *mut pg_sys::Node) -> Result<String, PgTrickleError> {
    if node.is_null() {
        return Ok("".to_string());
    }

    if let Some(rf) = cast_node!(node, T_RangeFunction, pg_sys::RangeFunction) {
        let func_list = pg_list::<pg_sys::Node>(rf.functions);

        if !rf.is_rowsfrom || func_list.len() <= 1 {
            // Single function — deparse normally.
            // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
            return unsafe { deparse_from_item_to_sql(node) };
        }

        // ── Multi-function ROWS FROM ────────────────────────────────
        // Extract each function call + its args.
        let mut func_sqls: Vec<String> = Vec::new();
        let mut func_names: Vec<String> = Vec::new();
        let mut func_args: Vec<Vec<String>> = Vec::new();

        for inner_node in func_list.iter_ptr() {
            if inner_node.is_null() {
                continue;
            }
            let inner_list = pg_list::<pg_sys::Node>(inner_node as *mut pg_sys::List);
            if inner_list.is_empty() {
                continue;
            }
            // INVARIANT: inner_list is non-empty (checked above), so head() cannot fail.
            let func_node = match inner_list.head() {
                Some(n) => n,
                None => continue,
            };
            // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
            if !is_node_type!(func_node, T_FuncCall) {
                continue;
            }
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let fcall = pg_deref!(func_node as *const pg_sys::FuncCall);
            // SAFETY: Parse-tree node pointer from raw_parser; valid within current memory context.
            let name = unsafe { extract_func_name(fcall.funcname)? };
            // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
            let sql = unsafe { deparse_func_call(func_node as *const pg_sys::FuncCall)? };

            let args_list = pg_list::<pg_sys::Node>(fcall.args);
            let mut args = Vec::new();
            for n in args_list.iter_ptr() {
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let expr = unsafe { node_to_expr(n)? };
                args.push(expr.to_sql());
            }

            func_names.push(name);
            func_sqls.push(sql);
            func_args.push(args);
        }

        if func_sqls.is_empty() {
            return Ok("(SELECT 1 WHERE false) AS __pgt_empty".to_string());
        }

        // Extract alias + column aliases from the ROWS FROM node.
        let rows_alias = if !rf.alias.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let a = pg_deref!(rf.alias);
            pg_cstr_to_str(a.aliasname)
                .unwrap_or("__pgt_rf")
                .to_string()
        } else {
            "__pgt_rf".to_string()
        };
        let col_aliases = if !rf.alias.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let a = pg_deref!(rf.alias);
            extract_alias_colnames(a)?
        } else {
            Vec::new()
        };

        let with_ordinality = rf.ordinality;

        // ── All-unnest optimisation ─────────────────────────────────
        let all_unnest = func_names.iter().all(|n| n == "unnest");
        if all_unnest {
            // Merge: ROWS FROM(unnest(A), unnest(B)) → unnest(A, B) AS t(c1, c2)
            let all_args: Vec<String> = func_args.iter().flat_map(|a| a.clone()).collect();
            let mut result = format!("unnest({})", all_args.join(", "));
            if with_ordinality {
                result.push_str(" WITH ORDINALITY");
            }
            if !col_aliases.is_empty() {
                result.push_str(&format!(" AS {}({})", rows_alias, col_aliases.join(", ")));
            } else {
                result.push_str(&format!(" AS {rows_alias}"));
            }
            return Ok(result);
        }

        // ── General case: ordinal-based FULL OUTER JOIN chain ────────
        // Each SRF is wrapped in a subquery that adds row_number() so we
        // can match them by ordinal.  We chain them via FULL OUTER JOIN
        // which naturally handles different-length results by padding
        // with NULLs — matching ROWS FROM semantics.
        //
        //   LATERAL (SELECT __pgt_f0.__pgt_c0, __pgt_f1.__pgt_c1 FROM
        //     (SELECT val AS __pgt_c0, row_number() OVER () AS __pgt_rn
        //      FROM f0(...) AS val) AS __pgt_f0
        //     FULL OUTER JOIN
        //     (SELECT val AS __pgt_c1, row_number() OVER () AS __pgt_rn
        //      FROM f1(...) AS val) AS __pgt_f1
        //     ON __pgt_f0.__pgt_rn = __pgt_f1.__pgt_rn
        //   ) AS alias
        let mut subqueries = Vec::new();
        let mut select_parts = Vec::new();

        for (i, func_sql) in func_sqls.iter().enumerate() {
            let f_alias = format!("__pgt_f{i}");
            let col_alias = format!("__pgt_c{i}");

            // Each SRF is wrapped in a subquery with its result column
            // given a known alias and row_number() for ordinal matching.
            let sub = format!(
                "(SELECT {col_alias}, row_number() OVER () AS __pgt_rn \
                 FROM {func_sql} AS {col_alias}) AS {f_alias}"
            );
            subqueries.push((f_alias.clone(), sub));
            select_parts.push(format!("{f_alias}.{col_alias}"));
        }

        // Chain subqueries via FULL OUTER JOIN on ordinal.
        // For >2 SRFs, the join ON uses COALESCE of prior ordinals.
        let inner_from = if subqueries.len() == 1 {
            subqueries[0].1.clone()
        } else {
            let mut chain = subqueries[0].1.clone();
            let mut coalesce_parts = vec![format!("{}.__pgt_rn", subqueries[0].0)];
            for (f_alias, sub) in subqueries.iter().skip(1) {
                let on_left = if coalesce_parts.len() == 1 {
                    coalesce_parts[0].clone()
                } else {
                    format!("COALESCE({})", coalesce_parts.join(", "))
                };
                chain = format!("{chain} FULL OUTER JOIN {sub} ON {on_left} = {f_alias}.__pgt_rn");
                coalesce_parts.push(format!("{f_alias}.__pgt_rn"));
            }
            chain
        };

        let inner_select = select_parts.join(", ");

        // Use LATERAL so SRF arguments can reference columns from
        // earlier FROM items (e.g. `unnest(m.arr)` where `m` is a
        // preceding table).
        let mut result = format!("LATERAL (SELECT {inner_select} FROM {inner_from})");
        if with_ordinality {
            // Ordinality for the whole ROWS FROM — add row_number outside.
            result = format!(
                "LATERAL (SELECT __pgt_rfo.*, row_number() OVER () AS ordinality \
                 FROM {result} AS __pgt_rfo)"
            );
        }
        if !col_aliases.is_empty() {
            result.push_str(&format!(" AS {}({})", rows_alias, col_aliases.join(", ")));
        } else {
            result.push_str(&format!(" AS {rows_alias}"));
        }

        Ok(result)
    } else if let Some(join) = cast_node!(node, T_JoinExpr, pg_sys::JoinExpr) {
        let left = rewrite_from_item_rows_from(join.larg)?;
        let right = rewrite_from_item_rows_from(join.rarg)?;
        let join_type = match join.jointype {
            pg_sys::JoinType::JOIN_LEFT => "LEFT JOIN",
            pg_sys::JoinType::JOIN_FULL => "FULL JOIN",
            pg_sys::JoinType::JOIN_RIGHT => "RIGHT JOIN",
            pg_sys::JoinType::JOIN_INNER if join.quals.is_null() => "CROSS JOIN",
            pg_sys::JoinType::JOIN_INNER => "JOIN",
            _ => "JOIN",
        };
        let on_clause = if join.quals.is_null() {
            String::new()
        } else {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let cond = unsafe { node_to_expr(join.quals)? };
            format!(" ON {}", cond.to_sql())
        };
        Ok(format!("{left} {join_type} {right}{on_clause}"))
    } else {
        // Not a ROWS FROM — deparse normally using existing helper.
        // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
        unsafe { deparse_from_item_to_sql(node) }
    }
}

/// Handle UNION/INTERSECT/EXCEPT branches for ROWS FROM rewriting.
fn rewrite_rows_from_in_set_op(select: &pg_sys::SelectStmt) -> Result<String, PgTrickleError> {
    // Recurse into each branch, rewrite, then reconstruct.
    let left = if select.larg.is_null() {
        return Ok(String::new());
    } else {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let left_select = pg_deref!(select.larg);
        if left_select.op != pg_sys::SetOperation::SETOP_NONE {
            rewrite_rows_from_in_set_op(left_select)?
        } else {
            // Leaf branch — build a temporary query and rewrite it.
            // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
            let leaf_sql = unsafe { deparse_select_stmt_to_sql(select.larg as *const _)? };
            rewrite_rows_from(&leaf_sql)?
        }
    };

    let right = if select.rarg.is_null() {
        return Ok(left);
    } else {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let right_select = pg_deref!(select.rarg);
        if right_select.op != pg_sys::SetOperation::SETOP_NONE {
            rewrite_rows_from_in_set_op(right_select)?
        } else {
            // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
            let leaf_sql = unsafe { deparse_select_stmt_to_sql(select.rarg as *const _)? };
            rewrite_rows_from(&leaf_sql)?
        }
    };

    let op_str = match select.op {
        pg_sys::SetOperation::SETOP_UNION if select.all => "UNION ALL",
        pg_sys::SetOperation::SETOP_UNION => "UNION",
        pg_sys::SetOperation::SETOP_INTERSECT if select.all => "INTERSECT ALL",
        pg_sys::SetOperation::SETOP_INTERSECT => "INTERSECT",
        pg_sys::SetOperation::SETOP_EXCEPT if select.all => "EXCEPT ALL",
        pg_sys::SetOperation::SETOP_EXCEPT => "EXCEPT",
        _ => "UNION",
    };

    let body = format!("{left} {op_str} {right}");

    // Preserve any WITH clause attached to this set-operation SelectStmt.
    // The CTE definitions live on the outermost SETOP node — they are NOT
    // propagated to larg/rarg — so reconstructing from the arms alone drops them.
    if !select.withClause.is_null() {
        // SAFETY: withClause is non-null and points to a valid WithClause node
        // obtained from the raw parse tree of a validated SQL string.
        let with_sql = deparse_with_clause_with_view_subs(select.withClause, &[])?;
        Ok(format!("{with_sql} {body}"))
    } else {
        Ok(body)
    }
}

// ── Multiple PARTITION BY → multi-pass window rewrite ──────────────

/// Rewrite a query with window functions using different PARTITION BY clauses
/// into a multi-pass plan.
///
/// When window functions use different PARTITION BY clauses, the parser normally
/// rejects the query. Instead, we split the window functions into groups by
/// their PARTITION BY clause and build a chain of subqueries — each adding
/// one group of window columns.
///
/// ```sql
/// -- Input:
/// SELECT id, region, dept,
///        SUM(amount) OVER (PARTITION BY region) AS region_sum,
///        SUM(amount) OVER (PARTITION BY dept)   AS dept_sum
/// FROM orders
/// -- Rewrite to:
/// SELECT __pgt_w1.id, __pgt_w1.region, __pgt_w1.dept,
///        __pgt_w1.region_sum, __pgt_w2.dept_sum
/// FROM (
///   SELECT id, region, dept, amount,
///          SUM(amount) OVER (PARTITION BY region) AS region_sum
///   FROM orders
/// ) __pgt_w1
/// JOIN (
///   SELECT id, region, dept, amount,
///          SUM(amount) OVER (PARTITION BY dept) AS dept_sum
///   FROM orders
/// ) __pgt_w2 ON __pgt_w1.__pgt_row_marker = __pgt_w2.__pgt_row_marker
/// ```
///
/// Each subquery computes one group of window functions with the same
/// PARTITION BY, plus a row marker for joining. The final query selects
/// pass-through columns from the first subquery and window columns from each.
pub fn rewrite_multi_partition_windows(query: &str) -> Result<String, PgTrickleError> {
    let select = match parse_first_select(query)? {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        Some(s) => pg_deref!(s),
        None => return Ok(query.to_string()),
    };

    // Set operations — don't rewrite
    if select.op != pg_sys::SetOperation::SETOP_NONE {
        return Ok(query.to_string());
    }

    // Need a target list with window functions
    if select.targetList.is_null() {
        return Ok(query.to_string());
    }

    // ── Extract window function info from the target list ───────────
    let target_list = pg_list::<pg_sys::Node>(select.targetList);
    let has_windows = target_list_has_windows(&target_list);
    if !has_windows {
        return Ok(query.to_string());
    }

    // Parse window expressions to check PARTITION BY diversity
    let window_infos = extract_window_info_from_targets(&target_list, select)?;

    if window_infos.is_empty() {
        return Ok(query.to_string());
    }

    // Group window functions by their PARTITION BY clause (as SQL text)
    let mut partition_groups: Vec<(String, Vec<WindowInfo>)> = Vec::new();
    for wi in window_infos {
        let found = partition_groups
            .iter_mut()
            .find(|(k, _)| *k == wi.partition_key);
        if let Some((_, group)) = found {
            group.push(wi);
        } else {
            partition_groups.push((wi.partition_key.clone(), vec![wi]));
        }
    }

    // If all window functions share the same PARTITION BY, no rewrite needed
    if partition_groups.len() <= 1 {
        return Ok(query.to_string());
    }

    // ── Collect non-window target expressions (pass-through columns) ─
    let mut pass_through: Vec<(String, Option<String>)> = Vec::new(); // (expr_sql, alias)
    for node_ptr in target_list.iter_ptr() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if node_ptr.is_null() || !is_node_type!(node_ptr, T_ResTarget) {
            continue;
        }
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let rt = pg_deref!(node_ptr as *const pg_sys::ResTarget);
        if rt.val.is_null() {
            continue;
        }
        // Skip if this is a window function
        // SAFETY: Node pointer from parse-tree; allocated by raw_parser.
        if unsafe { node_contains_window_func(rt.val) } {
            continue;
        }
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = safe_node_to_expr(rt.val)
            .map(|e| e.to_sql())
            .unwrap_or_else(|_| "NULL".to_string());
        let alias = if !rt.name.is_null() {
            Some(pg_cstr_to_str(rt.name).unwrap_or("?").to_string())
        } else {
            None
        };
        pass_through.push((expr, alias));
    }

    // ── Build FROM clause and WHERE clause SQL ──────────────────────
    let from_sql = extract_from_clause_sql(select)?;
    let where_sql = if select.whereClause.is_null() {
        String::new()
    } else {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = safe_node_to_expr(select.whereClause)
            .map(|e| e.to_sql())
            .unwrap_or_else(|_| "TRUE".to_string());
        format!(" WHERE {expr}")
    };
    let group_sql = deparse_group_clause(select);
    let having_sql = deparse_having_clause(select);

    // ── Build pass-through column list ──────────────────────────────
    let pt_select: Vec<String> = pass_through
        .iter()
        .map(|(expr, alias)| match alias {
            Some(a) => format!("{expr} AS \"{}\"", a.replace('"', "\"\"")),
            None => expr.clone(),
        })
        .collect();
    let pt_names: Vec<String> = pass_through
        .iter()
        .enumerate()
        .map(|(i, (expr, alias))| {
            alias.clone().unwrap_or_else(|| {
                // Try to extract a simple column name from the expression.
                // Handles both quoted ("name") and unquoted (name) column refs.
                let trimmed = expr.trim_matches('"');
                if trimmed.contains('.') || trimmed.contains('(') || trimmed.contains(' ') {
                    format!("__pgt_col_{}", i + 1)
                } else {
                    trimmed.to_string()
                }
            })
        })
        .collect();

    // ── Build CTE base with row marker ────────────────────────────────
    // Use a CTE so the row marker (ROW_NUMBER() OVER ()) is computed once
    // and referenced as a plain column in each subquery. This avoids
    // adding a second window function to each subquery which would
    // trigger the multi-PARTITION BY check recursively.
    // List columns explicitly (not SELECT *) to avoid A_Star parse issues.
    let cte_cols: Vec<String> = pt_select.clone();
    let cte_base = format!(
        "SELECT {}, ROW_NUMBER() OVER () AS __pgt_row_marker FROM {from_sql}{where_sql}{group_sql}{having_sql}",
        cte_cols.join(", ")
    );

    // ── Build subqueries for each partition group ─────────────────────
    let mut subquery_aliases: Vec<String> = Vec::new();
    let mut subquery_sqls: Vec<String> = Vec::new();
    let mut window_col_sources: Vec<(String, String)> = Vec::new(); // (subquery_alias, col_alias)

    for (group_idx, (_partition_key, group)) in partition_groups.iter().enumerate() {
        let sq_alias = format!("__pgt_w{}", group_idx + 1);

        let mut sq_select_parts: Vec<String> = Vec::new();

        // Include pass-through columns
        sq_select_parts.extend(pt_select.iter().cloned());

        // Include window function columns for this group
        for wi in group {
            sq_select_parts.push(format!(
                "{} AS \"{}\"",
                wi.func_sql,
                wi.alias.replace('"', "\"\"")
            ));
            window_col_sources.push((sq_alias.clone(), wi.alias.clone()));
        }

        // Include row marker (now a plain column from the CTE, not a window function)
        sq_select_parts.push("__pgt_row_marker".to_string());

        let sq_sql = format!("SELECT {} FROM __pgt_numbered", sq_select_parts.join(", "));
        subquery_sqls.push(sq_sql);
        subquery_aliases.push(sq_alias);
    }

    // ── Build outer SELECT ──────────────────────────────────────────
    let mut outer_select_parts: Vec<String> = Vec::new();

    // Pass-through columns from first subquery
    let first_sq = &subquery_aliases[0];
    for pt_name in &pt_names {
        outer_select_parts.push(format!(
            "\"{first_sq}\".\"{pt}\"",
            pt = pt_name.replace('"', "\"\"")
        ));
    }

    // Window columns from their respective subqueries
    for (sq_alias, col_alias) in &window_col_sources {
        outer_select_parts.push(format!(
            "\"{sq_alias}\".\"{col}\"",
            col = col_alias.replace('"', "\"\"")
        ));
    }

    // Include __pgt_row_marker in the outer SELECT so the DVM can store
    // it in the stream table. The Window diff reads old rows from the ST
    // including pass-through columns; without __pgt_row_marker in storage,
    // the CTE that fetches old rows would fail with "column does not exist".
    outer_select_parts.push(format!("\"{first_sq}\".\"__pgt_row_marker\"",));

    // ── Build outer FROM (JOIN chain) ──────────────────────────────
    let mut outer_from = format!(
        "({}) AS \"{}\"",
        subquery_sqls[0],
        subquery_aliases[0].replace('"', "\"\"")
    );

    for i in 1..subquery_sqls.len() {
        outer_from = format!(
            "{outer_from} JOIN ({}) AS \"{}\" ON \"{}\".\"__pgt_row_marker\" = \"{}\".\"__pgt_row_marker\"",
            subquery_sqls[i],
            subquery_aliases[i].replace('"', "\"\""),
            subquery_aliases[0].replace('"', "\"\""),
            subquery_aliases[i].replace('"', "\"\""),
        );
    }

    // ── ORDER BY rewrite ────────────────────────────────────────────
    let order_sql = deparse_order_clause(select);

    let rewritten = format!(
        "WITH __pgt_numbered AS ({cte_base}) SELECT {} FROM {outer_from}{order_sql}",
        outer_select_parts.join(", ")
    );

    pgrx::debug1!(
        "[pg_trickle] Rewrote multi-PARTITION BY windows: {}",
        rewritten
    );

    Ok(rewritten)
}

/// Information about a single window function in the target list.
pub(crate) struct WindowInfo {
    /// Full window function call as SQL (e.g., `SUM("amount") OVER (PARTITION BY "region")`).
    pub(crate) func_sql: String,
    /// Output alias for this window function.
    pub(crate) alias: String,
    /// Canonical key for the PARTITION BY clause (sorted, for grouping).
    pub(crate) partition_key: String,
}

/// Extract window function information from the target list.
///
/// # Safety
/// Caller must ensure target_list and select are valid.
fn extract_window_info_from_targets(
    target_list: &pgrx::PgList<pg_sys::Node>,
    _select: &pg_sys::SelectStmt,
) -> Result<Vec<WindowInfo>, PgTrickleError> {
    let mut infos = Vec::new();

    for node_ptr in target_list.iter_ptr() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if node_ptr.is_null() || !is_node_type!(node_ptr, T_ResTarget) {
            continue;
        }
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let rt = pg_deref!(node_ptr as *const pg_sys::ResTarget);
        if rt.val.is_null() {
            continue;
        }

        // Check for FuncCall with OVER clause (window function)
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if !is_node_type!(rt.val, T_FuncCall) {
            continue;
        }
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let func = pg_deref!(rt.val as *const pg_sys::FuncCall);
        if func.over.is_null() {
            continue; // Not a window function
        }

        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let over = pg_deref!(func.over);

        // Get partition key
        let partition_key = if over.partitionClause.is_null() {
            String::new()
        } else {
            let parts = pg_list::<pg_sys::Node>(over.partitionClause);
            let mut keys: Vec<String> = Vec::new();
            for p in parts.iter_ptr() {
                // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
                let expr = safe_node_to_expr(p)
                    .map(|e| e.to_sql())
                    .unwrap_or_else(|_| "?".to_string());
                keys.push(expr);
            }
            keys.sort();
            keys.join(",")
        };

        // Check if the OVER clause references a named window definition
        if !over.name.is_null() {
            let _win_name = pg_cstr_to_str(over.name).unwrap_or("?");
            // Look up the named window definition in windowClause to get
            // the full PARTITION BY. For simplicity, we include the name
            // as the partition key (named windows with same name share PARTITION BY).
            // A more complete impl would resolve the name, but this covers
            // the common case.
        }

        // Get the full function call as SQL
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let func_sql = safe_node_to_expr(rt.val)
            .map(|e| e.to_sql())
            .unwrap_or_else(|_| "NULL".to_string());

        // Get alias
        let alias = if !rt.name.is_null() {
            pg_cstr_to_str(rt.name).unwrap_or("?").to_string()
        } else {
            // Generate a default alias
            format!("__pgt_wfn_{}", infos.len() + 1)
        };

        infos.push(WindowInfo {
            func_sql,
            alias,
            partition_key,
        });
    }

    Ok(infos)
}

/// Deparse the WINDOW clause of a SelectStmt into SQL, e.g. `w AS (PARTITION BY x ORDER BY y)`.
///
/// Returns an empty string when there is no WINDOW clause.
///
/// # Safety
/// Caller must ensure `select` points to a valid `SelectStmt`.
fn deparse_select_window_clause(select: &pg_sys::SelectStmt) -> Result<String, PgTrickleError> {
    if select.windowClause.is_null() {
        return Ok(String::new());
    }
    let window_list = pg_list::<pg_sys::Node>(select.windowClause);
    if window_list.is_empty() {
        return Ok(String::new());
    }
    let mut parts = Vec::new();
    for node_ptr in window_list.iter_ptr() {
        if let Some(wdef) = cast_node!(node_ptr, T_WindowDef, pg_sys::WindowDef) {
            let wname = if !wdef.name.is_null() {
                pg_cstr_to_str(wdef.name).unwrap_or("w").to_string()
            } else {
                continue;
            };
            let wspec = deparse_window_def(wdef)?;
            parts.push(format!("{wname} AS ({wspec})"));
        }
    }
    Ok(parts.join(", "))
}

/// Rewrite queries where window functions are nested inside expressions.
///
/// The DVM engine requires window functions to appear at the top level of a
/// SELECT target (i.e. `wf_expr AS alias`). When a window function is wrapped
/// inside another expression — e.g. `ABS(ROW_NUMBER() OVER (...) - 5)` — the
/// parser rejects the query with an "unsupported" error.
///
/// This pass lifts all nested window functions into an inner subquery:
///
/// ```sql
/// -- Input
/// SELECT ABS(ROW_NUMBER() OVER (ORDER BY score) - 5) AS dist FROM t;
///
/// -- Output
/// SELECT "abs"("__pgt_wf_inner"."__pgt_wf_1" - 5) AS "dist"
///   FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY score) AS "__pgt_wf_1"
///         FROM t) "__pgt_wf_inner";
/// ```
///
/// Returns the original query unchanged when:
/// - The statement is a set operation (UNION / INTERSECT / EXCEPT)
/// - `GROUP BY` is present (interaction with window functions is complex)
/// - No nested window function expressions are found
pub fn rewrite_nested_window_exprs(query: &str) -> Result<String, PgTrickleError> {
    let select = match parse_first_select(query)? {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        Some(s) => pg_deref!(s),
        None => return Ok(query.to_string()),
    };

    // Set operations — don't rewrite; the union/intersect/except paths handle those
    if select.op != pg_sys::SetOperation::SETOP_NONE {
        return Ok(query.to_string());
    }

    // GROUP BY present — bail; window-over-aggregate interactions are non-trivial
    if !select.groupClause.is_null() {
        let group_list = pg_list::<pg_sys::Node>(select.groupClause);
        if !group_list.is_empty() {
            return Ok(query.to_string());
        }
    }

    if select.targetList.is_null() {
        return Ok(query.to_string());
    }

    let target_list = pg_list::<pg_sys::Node>(select.targetList);

    // ── Detect targets with nested window function expressions ───────────
    // "Nested" means: the ResTarget val is NOT a bare FuncCall-with-OVER,
    // but node_contains_window_func says there IS a window func somewhere inside it.
    let mut has_nested = false;
    for node_ptr in target_list.iter_ptr() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if node_ptr.is_null() || !is_node_type!(node_ptr, T_ResTarget) {
            continue;
        }
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let rt = pg_deref!(node_ptr as *const pg_sys::ResTarget);
        if rt.val.is_null() {
            continue;
        }
        // Skip bare window functions (direct FuncCall-with-OVER, not nested)
        if let Some(fc) = cast_node!(rt.val, T_FuncCall, pg_sys::FuncCall)
            && !fc.over.is_null()
        {
            continue;
        }
        // SAFETY: Node pointer from parse-tree; allocated by raw_parser.
        if unsafe { node_contains_window_func(rt.val) } {
            has_nested = true;
            break;
        }
    }

    if !has_nested {
        return Ok(query.to_string());
    }

    // ── Collect all distinct window function SQL strings ─────────────────
    // Walk every target to harvest FuncCall-with-OVER nodes; deduplicate by SQL text.
    let mut wf_entries: Vec<(String, String)> = Vec::new(); // (wf_sql, synthetic_alias)
    for node_ptr in target_list.iter_ptr() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if node_ptr.is_null() || !is_node_type!(node_ptr, T_ResTarget) {
            continue;
        }
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let rt = pg_deref!(node_ptr as *const pg_sys::ResTarget);
        if rt.val.is_null() {
            continue;
        }
        let mut wf_nodes: Vec<*mut pg_sys::Node> = Vec::new();
        // SAFETY: Node pointer from parse-tree target list; allocated by raw_parser.
        unsafe { collect_all_window_func_nodes(rt.val, &mut wf_nodes) };
        for wf_node in wf_nodes {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let wf_sql = match safe_node_to_expr(wf_node) {
                Ok(e) => e.to_sql(),
                Err(_) => continue,
            };
            if wf_entries.iter().any(|(s, _)| *s == wf_sql) {
                continue; // Already seen this window function
            }
            let alias = format!("__pgt_wf_{}", wf_entries.len() + 1);
            wf_entries.push((wf_sql, alias));
        }
    }

    if wf_entries.is_empty() {
        return Ok(query.to_string());
    }

    // ── Construct the inner SELECT ────────────────────────────────────────
    // SELECT *, wf1_sql AS "__pgt_wf_1", wf2_sql AS "__pgt_wf_2", ... FROM ...
    let from_sql = extract_from_clause_sql(select)?;

    let where_sql = if select.whereClause.is_null() {
        String::new()
    } else {
        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let expr = safe_node_to_expr(select.whereClause)
            .map(|e| e.to_sql())
            .unwrap_or_else(|_| "TRUE".to_string());
        format!(" WHERE {expr}")
    };

    // Carry the WINDOW clause into the inner SELECT so named window
    // references (OVER w) are still resolvable there.
    let window_clause_sql = deparse_select_window_clause(select)?;
    let window_sql = if window_clause_sql.is_empty() {
        String::new()
    } else {
        format!(" WINDOW {window_clause_sql}")
    };

    let inner_wf_parts: Vec<String> = wf_entries
        .iter()
        .map(|(wf_sql, alias)| format!("{wf_sql} AS \"{}\"", alias.replace('"', "\"\"")))
        .collect();

    let inner_select = format!(
        "SELECT *, {} FROM {from_sql}{where_sql}{window_sql}",
        inner_wf_parts.join(", ")
    );

    // ── Construct the outer SELECT ────────────────────────────────────────
    // For non-nested targets: deparse with table qualifiers stripped.
    // For nested targets: deparse, strip qualifiers, then replace each
    // embedded wf_sql substring with its synthetic alias reference.
    let mut outer_parts: Vec<String> = Vec::new();

    for node_ptr in target_list.iter_ptr() {
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if node_ptr.is_null() || !is_node_type!(node_ptr, T_ResTarget) {
            continue;
        }
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let rt = pg_deref!(node_ptr as *const pg_sys::ResTarget);
        if rt.val.is_null() {
            continue;
        }

        // Does this target contain a nested window function?
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        let is_direct_wf = is_node_type!(rt.val, T_FuncCall) && {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let fc = pg_deref!(rt.val as *const pg_sys::FuncCall);
            !fc.over.is_null()
        };
        // SAFETY: Node pointer from parse-tree; allocated by raw_parser.
        let has_wf_inside = !is_direct_wf && unsafe { node_contains_window_func(rt.val) };

        // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
        let mut expr_sql = match safe_node_to_expr(rt.val) {
            Ok(e) => e.strip_qualifier().to_sql(),
            Err(_) => continue,
        };

        if has_wf_inside {
            // Replace embedded window function SQL with synthetic alias references.
            // The alias is qualified with the inner subquery alias to resolve
            // potential ambiguity when the outer query joins multiple tables.
            for (wf_sql, alias) in &wf_entries {
                let replacement = format!("\"__pgt_wf_inner\".\"{}\"", alias.replace('"', "\"\""));
                expr_sql = expr_sql.replace(wf_sql.as_str(), &replacement);
            }
        }

        let alias_part = if !rt.name.is_null() {
            let a = pg_cstr_to_str(rt.name).unwrap_or("col");
            format!(" AS \"{}\"", a.replace('"', "\"\""))
        } else {
            String::new()
        };

        outer_parts.push(format!("{expr_sql}{alias_part}"));
    }

    // ── Preserve ORDER BY from the outer query ───────────────────────────
    let order_sql = deparse_order_clause(select);

    let rewritten = format!(
        "SELECT {} FROM ({inner_select}) \"__pgt_wf_inner\"{order_sql}",
        outer_parts.join(", ")
    );

    pgrx::debug1!("[pg_trickle] rewrite_nested_window_exprs: {}", rewritten);

    Ok(rewritten)
}

/// Extract FROM clause as SQL text from a SelectStmt.
pub(crate) fn extract_from_clause_sql(
    select: &pg_sys::SelectStmt,
) -> Result<String, PgTrickleError> {
    if select.fromClause.is_null() {
        return Err(PgTrickleError::QueryParseError(
            "DISTINCT ON query must have a FROM clause".into(),
        ));
    }
    let from_list = pg_list::<pg_sys::Node>(select.fromClause);
    let cte_names = if select.withClause.is_null() {
        Vec::new()
    } else {
        let with_clause = pg_deref!(select.withClause);
        pg_list::<pg_sys::CommonTableExpr>(with_clause.ctes)
            .iter_ptr()
            .filter_map(|cte| {
                // SAFETY: CTE pointers originate from the valid parser list.
                unsafe { cte.as_ref() }
            })
            .filter_map(|cte| pg_cstr_to_str(cte.ctename).ok())
            .collect::<Vec<_>>()
    };
    let mut parts = Vec::new();
    for node_ptr in from_list.iter_ptr() {
        if node_ptr.is_null() {
            continue;
        }
        parts.push(from_item_to_sql(node_ptr, &cte_names)?);
    }
    if parts.is_empty() {
        return Err(PgTrickleError::QueryParseError(
            "DISTINCT ON query FROM clause is empty".into(),
        ));
    }
    Ok(parts.join(", "))
}

/// Convert a FROM clause item (RangeVar, JoinExpr, RangeSubselect) back to SQL.
///
/// # Safety
/// Caller must ensure `node` points to a valid parse tree Node.
fn from_item_to_sql(node: *mut pg_sys::Node, cte_names: &[&str]) -> Result<String, PgTrickleError> {
    if let Some(rv) = cast_node!(node, T_RangeVar, pg_sys::RangeVar) {
        let mut name = String::new();
        let relname = if rv.relname.is_null() {
            None
        } else {
            Some(pg_cstr_to_str(rv.relname).unwrap_or("?"))
        };
        let schema = if !rv.schemaname.is_null() {
            Some(
                pg_cstr_to_str(rv.schemaname)
                    .unwrap_or("public")
                    .to_string(),
            )
        } else if relname.is_some_and(|relname| cte_names.contains(&relname)) {
            None
        } else {
            resolve_rangevar_schema(rv)?
        };
        if let Some(schema) = schema {
            name.push_str(&format!("\"{}\".", schema.replace('"', "\"\"")));
        }
        if let Some(rel) = relname {
            name.push_str(&format!("\"{}\"", rel.replace('"', "\"\"")));
        }
        if !rv.alias.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let alias_struct = pg_deref!(rv.alias);
            if !alias_struct.aliasname.is_null() {
                let alias = pg_cstr_to_str(alias_struct.aliasname).unwrap_or("?");
                name.push_str(&format!(" AS \"{}\"", alias.replace('"', "\"\"")));
            }
        }
        Ok(name)
    } else if let Some(join) = cast_node!(node, T_JoinExpr, pg_sys::JoinExpr) {
        let left = if join.larg.is_null() {
            "?".to_string()
        } else {
            from_item_to_sql(join.larg, cte_names)?
        };
        let right = if join.rarg.is_null() {
            "?".to_string()
        } else {
            from_item_to_sql(join.rarg, cte_names)?
        };
        let join_type = match join.jointype {
            pg_sys::JoinType::JOIN_LEFT => "LEFT JOIN",
            pg_sys::JoinType::JOIN_FULL => "FULL JOIN",
            pg_sys::JoinType::JOIN_RIGHT => "RIGHT JOIN",
            pg_sys::JoinType::JOIN_INNER if join.quals.is_null() => "CROSS JOIN",
            pg_sys::JoinType::JOIN_INNER => "JOIN",
            _ => "JOIN",
        };
        let on_clause = if join.quals.is_null() {
            String::new()
        } else {
            // SAFETY: Node pointer from a valid parse-tree list; allocated by raw_parser.
            let cond = unsafe { node_to_expr(join.quals)? };
            format!(" ON {}", cond.to_sql())
        };
        Ok(format!("{left} {join_type} {right}{on_clause}"))
    } else if let Some(sub) = cast_node!(node, T_RangeSubselect, pg_sys::RangeSubselect) {
        if sub.subquery.is_null()
            // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
            || !is_node_type!(sub.subquery, T_SelectStmt)
        {
            return Ok("?".to_string());
        }
        let inner_sql = deparse_select_to_sql(sub.subquery)?;
        let lateral_kw = if sub.lateral { "LATERAL " } else { "" };
        let mut result = format!("{lateral_kw}({inner_sql})");
        if !sub.alias.is_null() {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let a = pg_deref!(sub.alias);
            if !a.aliasname.is_null() {
                let alias = pg_cstr_to_str(a.aliasname).unwrap_or("?");
                result.push_str(&format!(" AS \"{}\"", alias.replace('"', "\"\"")));
            }
        }
        Ok(result)
    // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
    } else if is_node_type!(node, T_RangeFunction) {
        // Function in FROM — use fallback deparser
        // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
        unsafe { deparse_from_item_to_sql(node) }
    } else {
        // Fallback for other FROM items
        // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
        unsafe { deparse_from_item_to_sql(node) }
    }
}

pub fn reject_limit_offset(query: &str) -> Result<(), PgTrickleError> {
    // If the query matches the TopK pattern (ORDER BY + LIMIT), allow it.
    if detect_topk_pattern(query)?.is_some() {
        return Ok(());
    }

    let select = match parse_first_select(query)? {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        Some(s) => pg_deref!(s),
        None => return Ok(()),
    };

    if !select.limitCount.is_null() {
        // LIMIT ALL is semantically equivalent to no LIMIT — don't reject.
        // SAFETY: limit node is non-null and points to a valid Node.
        if !is_limit_all_node(select.limitCount) {
            return Err(PgTrickleError::UnsupportedOperator(
                "LIMIT is not supported in defining queries without ORDER BY. \
                 Use ORDER BY + LIMIT for TopK stream tables, or omit LIMIT \
                 to materialize the full result set."
                    .into(),
            ));
        }
    }
    if !select.limitOffset.is_null() {
        return Err(PgTrickleError::UnsupportedOperator(
            "OFFSET is not supported in defining queries without ORDER BY + LIMIT. \
             Use ORDER BY + LIMIT + OFFSET for paged TopK stream tables, \
             or omit OFFSET to materialize the full result set."
                .into(),
        ));
    }

    Ok(())
}

/// Metadata for a TopK stream table (ORDER BY + LIMIT pattern).
#[derive(Debug, Clone)]
pub struct TopKInfo {
    /// The LIMIT value as an integer.
    pub limit_value: i64,
    /// The OFFSET value as an integer, if present. `None` means no OFFSET.
    pub offset_value: Option<i64>,
    /// The full defining query including ORDER BY + LIMIT [+ OFFSET] (for refresh).
    pub full_query: String,
    /// The defining query with ORDER BY, LIMIT, and OFFSET stripped (for DVM, deps).
    pub base_query: String,
    /// The deparsed ORDER BY clause (e.g., "score DESC, name ASC").
    pub order_by_sql: String,
}

/// Detect the TopK pattern in a defining query: ORDER BY + LIMIT with
/// a constant integer limit value, no OFFSET, and no set operations.
///
/// Returns `Some(TopKInfo)` if the pattern matches, `None` otherwise.
///
/// Validation rules:
/// - `LIMIT` without `ORDER BY` → not TopK (will be rejected later)
/// - `ORDER BY` without `LIMIT` → not TopK (ORDER BY silently discarded)
/// - `ORDER BY` + `LIMIT` → TopK pattern ✓
/// - `ORDER BY` + `LIMIT` + `OFFSET` → error
/// - `LIMIT ALL` → not TopK (equivalent to no LIMIT)
/// - `LIMIT 0` → TopK with zero rows
/// - `LIMIT (SELECT ...)` → error (require constant integer)
/// - Set operations (UNION, INTERSECT, EXCEPT) → not TopK
pub fn detect_topk_pattern(query: &str) -> Result<Option<TopKInfo>, PgTrickleError> {
    let select = match parse_first_select(query)? {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        Some(s) => pg_deref!(s),
        None => return Ok(None),
    };

    // Not TopK if it's a set operation (UNION, INTERSECT, EXCEPT)
    if select.op != pg_sys::SetOperation::SETOP_NONE {
        return Ok(None);
    }

    // Not TopK if no LIMIT
    if select.limitCount.is_null() {
        return Ok(None);
    }

    // Not TopK if no ORDER BY
    if select.sortClause.is_null() {
        return Ok(None);
    }
    let sort_list = pg_list::<pg_sys::Node>(select.sortClause);
    if sort_list.is_empty() {
        return Ok(None);
    }

    // Extract OFFSET value if present (must be a constant non-negative integer).
    let offset_value = if !select.limitOffset.is_null() {
        match extract_const_int_from_node(select.limitOffset) {
            Some(v) if v >= 0 => {
                if v == 0 {
                    None
                } else {
                    Some(v)
                }
            }
            Some(_) => {
                return Err(PgTrickleError::UnsupportedOperator(
                    "OFFSET must be a non-negative constant integer.".into(),
                ));
            }
            None => {
                return Err(PgTrickleError::UnsupportedOperator(
                    "OFFSET in defining queries must be a constant integer \
                     (e.g., OFFSET 10). Dynamic expressions like OFFSET (SELECT ...) \
                     are not supported."
                        .into(),
                ));
            }
        }
    } else {
        None
    };

    // Extract LIMIT value — must be a constant integer.
    let limit_node = select.limitCount;
    let limit_value = extract_const_int_from_node(limit_node);
    let limit_value = match limit_value {
        Some(v) => v,
        None => {
            // Check if this is LIMIT ALL (A_Const with isnull=true) — treat as
            // "no LIMIT" → not a TopK table.
            if is_limit_all_node(limit_node) {
                return Ok(None);
            }
            // Otherwise it's a non-constant expression like LIMIT (SELECT ...).
            return Err(PgTrickleError::UnsupportedOperator(
                "LIMIT in defining queries must be a constant integer \
                 (e.g., LIMIT 100). Dynamic expressions like LIMIT (SELECT ...) \
                 are not supported for TopK stream tables."
                    .into(),
            ));
        }
    };

    // LIMIT ALL is represented as a very large or negative value in some
    // parse trees — treat it as "no limit" → not TopK.
    if limit_value < 0 {
        return Ok(None);
    }

    // Deparse the ORDER BY clause from the sort items.
    // SAFETY: Parse-tree node pointers from raw_parser; valid within current memory context.
    let order_by_sql = unsafe { deparse_sort_clause(&sort_list)? };

    // Build the base query by stripping ORDER BY and LIMIT.
    let base_query = strip_order_by_and_limit(query);

    Ok(Some(TopKInfo {
        limit_value,
        offset_value,
        full_query: query.to_string(),
        base_query,
        order_by_sql,
    }))
}

/// Extract a constant integer value from a parse tree Node.
///
/// Returns `Some(value)` for Integer constants, `None` for anything else.
///
/// # Safety
/// Caller must ensure `node` points to a valid Node.
fn extract_const_int_from_node(node: *mut pg_sys::Node) -> Option<i64> {
    if node.is_null() {
        return None;
    }

    // Check for A_Const (Integer constant in raw parse tree)
    if let Some(a_const) = cast_node!(node, T_A_Const, pg_sys::A_Const) {
        // In PostgreSQL 18, A_Const uses a union `val` with a `node.type` tag field.
        // Check if it's an Integer type.
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        // SAFETY: accessing union fields of A_Const requires unsafe; we check
        // the node tag before reading the corresponding union variant.
        // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
        let val_tag = unsafe { a_const.val.node.type_ };
        if val_tag == pg_sys::NodeTag::T_Integer {
            // SAFETY: we checked the tag is T_Integer, so accessing ival is valid.
            let ival = unsafe { a_const.val.ival.ival } as i64;
            return Some(ival);
        }

        // Check for T_String — could be LIMIT ALL or similar
        if val_tag == pg_sys::NodeTag::T_String {
            // LIMIT ALL is represented as a NULL limitCount in most cases;
            // if it's a string, it's not a constant int.
            return None;
        }

        return None;
    }

    // TypeCast wrapping an integer constant (e.g., LIMIT 5::bigint)
    if let Some(tc) = cast_node!(node, T_TypeCast, pg_sys::TypeCast) {
        return extract_const_int_from_node(tc.arg);
    }

    None
}

/// Check whether a LIMIT node represents `LIMIT ALL`.
///
/// In PostgreSQL 18, `LIMIT ALL` is parsed as a non-null A_Const with
/// `isnull = true`. This is semantically equivalent to no LIMIT.
///
/// # Safety
/// Caller must ensure `node` points to a valid Node (or is null).
fn is_limit_all_node(node: *mut pg_sys::Node) -> bool {
    if node.is_null() {
        return false;
    }
    if let Some(a_const) = cast_node!(node, T_A_Const, pg_sys::A_Const) {
        return a_const.isnull;
    }
    false
}

/// Strip ORDER BY, LIMIT, OFFSET, and FETCH FIRST clauses from a query string.
///
/// Finds the last top-level `ORDER BY` keyword (not inside parentheses) and
/// removes everything from there to the end. This is safe because TopK
/// detection already verified the query has a top-level ORDER BY + LIMIT
/// (no set operations). OFFSET always follows ORDER BY at the top level,
/// so stripping from ORDER BY onward removes all three clauses.
pub(crate) fn strip_order_by_and_limit(query: &str) -> String {
    let q = query.trim().trim_end_matches(';').trim();
    let upper = q.to_uppercase();

    // Walk backwards through the string to find the last top-level ORDER BY.
    // Track parenthesis depth to skip ORDER BY inside subqueries.
    let bytes = upper.as_bytes();
    let mut depth: i32 = 0;
    let mut last_order_by_pos: Option<usize> = None;

    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'\'' => {
                // Skip string literals
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            i += 2; // escaped quote
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'O' if depth == 0 && upper[i..].starts_with("ORDER") => {
                // Check for "ORDER BY" at this position
                let after_order = i + 5;
                if after_order < bytes.len() && bytes[after_order].is_ascii_whitespace() {
                    // Skip whitespace
                    let mut j = after_order;
                    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                        j += 1;
                    }
                    if upper[j..].starts_with("BY")
                        && (j + 2 >= bytes.len() || !bytes[j + 2].is_ascii_alphanumeric())
                    {
                        last_order_by_pos = Some(i);
                    }
                }
            }
            _ => {}
        }
        i += 1;
    }

    match last_order_by_pos {
        Some(pos) => q[..pos].trim().to_string(),
        None => q.to_string(),
    }
}

/// F13 (G4.2): Warn when a FROM-clause subquery or LATERAL uses LIMIT without
/// ORDER BY.
///
/// Without ORDER BY, LIMIT picks an arbitrary subset of rows. The chosen
/// rows may differ between FULL refresh (fresh plan / statistics) and
/// DIFFERENTIAL refresh (delta-driven), causing non-deterministic results.
///
/// Emits a `pgrx::warning!()` for each such occurrence. Does **not** reject
/// the query — LIMIT with ORDER BY is a legitimate, deterministic pattern.
/// NS-1: Returns `true` when the top-level SELECT has an `ORDER BY` clause
/// but no `LIMIT`. This is a no-op in a stream table context because the
/// storage table row order is undefined, so the user is likely confused.
pub fn has_order_by_without_limit(query: &str) -> bool {
    let select = match parse_first_select(query) {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        Ok(Some(s)) => pg_deref!(s),
        _ => return false,
    };
    // Only warn for plain (non-set-operation) SELECTs.
    if select.op != pg_sys::SetOperation::SETOP_NONE {
        return false;
    }
    // Has ORDER BY ...
    if select.sortClause.is_null() {
        return false;
    }
    let sort_list = pg_list::<pg_sys::Node>(select.sortClause);
    if sort_list.is_empty() {
        return false;
    }
    // ... but no LIMIT
    select.limitCount.is_null()
}

pub fn warn_limit_without_order_in_subqueries(query: &str) {
    let select = match parse_first_select(query) {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        Ok(Some(s)) => pg_deref!(s),
        _ => return,
    };
    walk_from_for_limit_warning(select);
}

/// Walk a SelectStmt's FROM clause looking for subqueries with LIMIT but
/// no ORDER BY.
///
/// # Safety
/// Caller must ensure `select` points to a valid `SelectStmt`.
fn walk_from_for_limit_warning(select: *const pg_sys::SelectStmt) {
    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let s = pg_deref!(select);

    // Handle set operations
    if s.op != pg_sys::SetOperation::SETOP_NONE {
        if !s.larg.is_null() {
            walk_from_for_limit_warning(s.larg);
        }
        if !s.rarg.is_null() {
            walk_from_for_limit_warning(s.rarg);
        }
        return;
    }

    let from_list = pg_list::<pg_sys::Node>(s.fromClause);
    for node_ptr in from_list.iter_ptr() {
        check_from_item_limit_warning(node_ptr);
    }

    // Also check CTEs
    if !s.withClause.is_null() {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let wc = pg_deref!(s.withClause);
        let cte_list = pg_list::<pg_sys::Node>(wc.ctes);
        for node_ptr in cte_list.iter_ptr() {
            if let Some(cte) = cast_node!(node_ptr, T_CommonTableExpr, pg_sys::CommonTableExpr)
                && !cte.ctequery.is_null()
                // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
                && is_node_type!(cte.ctequery, T_SelectStmt)
            {
                walk_from_for_limit_warning(cte.ctequery as *const pg_sys::SelectStmt);
            }
        }
    }
}

/// Check a single FROM-clause item for LIMIT without ORDER BY.
///
/// # Safety
/// Caller must ensure `node` points to a valid parse tree node.
fn check_from_item_limit_warning(node: *mut pg_sys::Node) {
    if node.is_null() {
        return;
    }

    if let Some(sub) = cast_node!(node, T_RangeSubselect, pg_sys::RangeSubselect) {
        if !sub.subquery.is_null()
            // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
            && is_node_type!(sub.subquery, T_SelectStmt)
        {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            let inner = pg_deref!(sub.subquery as *const pg_sys::SelectStmt);
            let has_order_by = !inner.sortClause.is_null() && {
                let sort_list = pg_list::<pg_sys::Node>(inner.sortClause);
                !sort_list.is_empty()
            };

            // Check: has LIMIT but no ORDER BY
            if !inner.limitCount.is_null() && !has_order_by {
                let alias = resolve_range_subselect_alias(sub);
                pgrx::warning!(
                    "pg_trickle: subquery '{}' uses LIMIT without ORDER BY. \
                     This produces non-deterministic results that may differ between \
                     FULL and DIFFERENTIAL refresh. Add ORDER BY for deterministic behavior.",
                    alias
                );
            }
            // G2: Check: has OFFSET but no ORDER BY
            if !inner.limitOffset.is_null() && !has_order_by {
                let alias = resolve_range_subselect_alias(sub);
                pgrx::warning!(
                    "pg_trickle: subquery '{}' uses OFFSET without ORDER BY. \
                     This produces non-deterministic results that may differ between \
                     FULL and DIFFERENTIAL refresh. Add ORDER BY for deterministic behavior.",
                    alias
                );
            }
            // Recurse into the subquery's FROM clause
            walk_from_for_limit_warning(sub.subquery as *const pg_sys::SelectStmt);
        }
    } else if let Some(join) = cast_node!(node, T_JoinExpr, pg_sys::JoinExpr) {
        check_from_item_limit_warning(join.larg);
        check_from_item_limit_warning(join.rarg);
    }
}

/// Extract the alias name from a `RangeSubselect` node.
///
/// # Safety
/// Caller must ensure `sub` points to a valid `RangeSubselect`.
fn resolve_range_subselect_alias(sub: &pg_sys::RangeSubselect) -> String {
    if !sub.alias.is_null() {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        let a = pg_deref!(sub.alias);
        if !a.aliasname.is_null() {
            return pg_cstr_to_str(a.aliasname)
                .unwrap_or("(subquery)")
                .to_string();
        }
    }
    "(subquery)".to_string()
}

/// Lightweight validation that rejects SQL constructs unsupported in
/// **any** refresh mode (FULL or DIFFERENTIAL).
///
/// Currently checks:
/// - **NATURAL JOIN**: the raw parser sets `isNatural` on `JoinExpr`
/// - **DISTINCT ON**: `distinctClause` contains non-null expression nodes
/// - **Subquery expressions** (EXISTS, IN subquery, scalar subquery):
///   `T_SubLink` nodes in the WHERE clause or target list
///
/// This avoids running the full DVM parser (which resolves table OIDs,
/// builds the operator tree, etc.) for queries that would fail anyway.
/// Convert a byte offset from a PostgreSQL parser `location` field into a
/// 1-based (line, column) pair within `query`.  Returns `None` when the
/// offset is unknown (PostgreSQL sets it to `-1` when unavailable).
fn query_byte_offset_to_linecol(query: &str, offset: i32) -> Option<(usize, usize)> {
    if offset < 0 {
        return None;
    }
    let offset = (offset as usize).min(query.len());
    let prefix = &query[..offset];
    let line = prefix.bytes().filter(|&b| b == b'\n').count() + 1;
    let col = prefix.rfind('\n').map_or(offset + 1, |pos| offset - pos);
    Some((line, col))
}

pub fn reject_unsupported_constructs(query: &str) -> Result<(), PgTrickleError> {
    let select = match parse_first_select(query)? {
        // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
        Some(s) => pg_deref!(s),
        None => return Ok(()),
    };

    // For set operations (UNION/INTERSECT/EXCEPT), recurse into both sides
    if select.op != pg_sys::SetOperation::SETOP_NONE {
        if !select.larg.is_null() {
            // SAFETY: larg points to a valid SelectStmt
            check_select_unsupported(pg_deref!(select.larg), query)?;
        }
        if !select.rarg.is_null() {
            // SAFETY: rarg points to a valid SelectStmt
            check_select_unsupported(pg_deref!(select.rarg), query)?;
        }
        return Ok(());
    }

    // SAFETY: select is a valid SelectStmt
    check_select_unsupported(select, query)
}

/// Check a single SelectStmt for unsupported constructs.
///
/// # Safety
/// Caller must ensure `select` points to a valid `pg_sys::SelectStmt`.
fn check_select_unsupported(
    select: &pg_sys::SelectStmt,
    query: &str,
) -> Result<(), PgTrickleError> {
    // ── DISTINCT ON ─────────────────────────────────────────────────
    // DISTINCT ON is handled by auto-rewriting to a ROW_NUMBER() window
    // function in the DVM parser (`rewrite_distinct_on()`). No rejection
    // needed here.

    // ── FROM clause items (TABLESAMPLE) ─────────────────────────────
    // NATURAL JOIN is supported (S9): common columns are resolved and an
    // explicit equi-join is synthesized. Only TABLESAMPLE is rejected here.
    if !select.fromClause.is_null() {
        let from_list = pg_list::<pg_sys::Node>(select.fromClause);
        for node_ptr in from_list.iter_ptr() {
            if !node_ptr.is_null() {
                // SAFETY: node_ptr is valid from the from_list
                check_from_item_unsupported(node_ptr, query)?;
            }
        }
    }

    // ── Subquery expressions in WHERE ──────────────────────────────────
    // SubLinks (EXISTS, IN subquery, scalar subquery) in the WHERE clause
    // are handled during parse_select_stmt by extracting them into
    // SemiJoin/AntiJoin OpTree nodes. We only reject ALL_SUBLINK here
    // since that's not yet supported.
    if !select.whereClause.is_null() {
        // SAFETY: whereClause is valid from the SelectStmt
        check_where_for_unsupported_sublinks(select.whereClause)?;
    }

    // ── FOR UPDATE / FOR SHARE / FOR NO KEY UPDATE / FOR KEY SHARE ──
    if !select.lockingClause.is_null() {
        return Err(PgTrickleError::UnsupportedOperator(
            "FOR UPDATE/FOR SHARE is not supported in defining queries. \
             Stream tables do not support row-level locking. \
             Remove the FOR UPDATE/FOR SHARE clause."
                .into(),
        ));
    }

    // ── GROUPING SETS / CUBE / ROLLUP ────────────────────────────────
    // Handled by auto-rewriting to UNION ALL of separate GROUP BY queries
    // in `rewrite_grouping_sets()`. No rejection needed here.

    Ok(())
}

/// Recursively check FROM clause items for unsupported features.
///
/// Currently rejects:
/// - `TABLESAMPLE` — Stream tables materialize complete result sets;
///   non-deterministic sampling is not meaningful.
///
/// Note: NATURAL JOIN is now supported (S9) — common columns are resolved
/// from the parsed OpTree and an explicit equi-join condition is synthesized.
///
/// # Safety
/// Caller must ensure `node` points to a valid `pg_sys::Node`.
fn check_from_item_unsupported(node: *mut pg_sys::Node, query: &str) -> Result<(), PgTrickleError> {
    if let Some(join) = cast_node!(node, T_JoinExpr, pg_sys::JoinExpr) {
        // Recursively check join children
        if !join.larg.is_null() {
            // SAFETY: larg is valid from JoinExpr
            check_from_item_unsupported(join.larg, query)?;
        }
        if !join.rarg.is_null() {
            // SAFETY: rarg is valid from JoinExpr
            check_from_item_unsupported(join.rarg, query)?;
        }
    } else if let Some(rts) = cast_node!(node, T_RangeTableSample, pg_sys::RangeTableSample) {
        // TABLESAMPLE: SELECT * FROM t TABLESAMPLE BERNOULLI(10)
        // Stream tables materialize complete result sets; non-deterministic
        // sampling is not meaningful. Reject in both FULL and DIFFERENTIAL modes.
        let loc = query_byte_offset_to_linecol(query, rts.location)
            .map(|(l, c)| format!(" at line {l}, column {c}"))
            .unwrap_or_default();
        return Err(PgTrickleError::UnsupportedOperator(format!(
            "TABLESAMPLE{loc} is not supported in defining queries. \
             Stream tables materialize the complete result set; \
             use a WHERE condition with random() if sampling is needed."
        )));
    }
    // RangeVar and RangeSubselect are fine — no check needed
    Ok(())
}

/// Check if a WHERE clause node tree contains unsupported SubLink types.
///
/// EXISTS_SUBLINK, ANY_SUBLINK (IN), and ALL_SUBLINK are now supported
/// via SemiJoin/AntiJoin. EXPR_SUBLINK (scalar subquery) is supported
/// in the target list. No SubLink types are rejected here any longer.
///
/// # Safety
/// Caller must ensure `node` points to a valid `pg_sys::Node`.
fn check_where_for_unsupported_sublinks(node: *mut pg_sys::Node) -> Result<(), PgTrickleError> {
    // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
    if is_node_type!(node, T_SubLink) {
        // EXISTS, ANY (IN), ALL, and EXPR SubLinks are handled during parsing
        return Ok(());
    }
    // Check inside BoolExpr (AND/OR/NOT) which commonly wraps SubLinks
    if let Some(boolexpr) = cast_node!(node, T_BoolExpr, pg_sys::BoolExpr)
        && !boolexpr.args.is_null()
    {
        let args = pg_list::<pg_sys::Node>(boolexpr.args);
        for arg_ptr in args.iter_ptr() {
            if !arg_ptr.is_null() {
                // SAFETY: arg_ptr is valid from BoolExpr args list
                check_where_for_unsupported_sublinks(arg_ptr)?;
            }
        }
    }
    Ok(())
}

// ── Predicate pushdown into cross joins ──────────────────────────────────
//
// Comma-separated FROM items produce a left-deep InnerJoin chain with
// `condition = Literal("TRUE")`.  The WHERE clause predicates are placed
// in a single Filter above the entire chain.
//
// This pass promotes predicates from the Filter into the appropriate
// JOIN ON clauses.  Benefits:
//
//  1. Eliminates cross-product intermediates in delta CTEs
//  2. Enables semi-join optimisation in diff_inner_join
//  3. Fixes Part 3 correction accuracy for multi-table joins (Q07)
//
// Algorithm:
//   a) Split the filter predicate into AND-connected parts.
//   b) For each part, collect referenced source-table aliases.
//   c) Walk the InnerJoin chain top-down. At each level, if the right
//      child contains any alias referenced by the predicate, that is
//      the level to attach it (all remaining aliases are guaranteed to
//      be in the left subtree of a left-deep chain).
//   d) Any predicate that can't be promoted stays in the Filter.

/// Push filter predicates down into cross-join ON clauses.
pub(crate) fn push_filter_into_cross_joins(tree: OpTree) -> Result<OpTree, PgTrickleError> {
    let OpTree::Filter { predicate, child } = tree else {
        return Ok(tree);
    };

    // Quick check: does the child contain any cross join?
    if !has_cross_join(&child) {
        return Ok(OpTree::Filter { predicate, child });
    }

    // Split predicate into AND-connected parts
    let parts = split_and_predicates(predicate);

    // Classify parts: collect referenced source aliases for each
    let mut to_promote: Vec<(Expr, Vec<String>)> = Vec::new();
    let mut remaining: Vec<Expr> = Vec::new();

    for part in parts {
        // Guard (a): never promote predicates containing scalar subqueries.
        // Correlated subqueries (e.g. TPC-H Q17) appear as Expr::Raw with
        // an embedded SELECT — promoting them removes column references that
        // the correlation still needs.
        if expr_contains_subquery(&part) {
            remaining.push(part);
            continue;
        }

        let mut aliases = Vec::new();
        collect_expr_source_aliases(&part, &child, &mut aliases);
        aliases.sort();
        aliases.dedup();

        if aliases.len() >= 2 {
            // References multiple source tables — promote into the join
            to_promote.push((part, aliases));
        } else {
            // Single-table predicate or can't determine — keep as filter
            remaining.push(part);
        }
    }

    if to_promote.is_empty() {
        // Nothing to promote — rebuild the original Filter
        return Ok(OpTree::Filter {
            predicate: join_and_predicates(remaining)?,
            child,
        });
    }

    // Promote each predicate into the join chain
    let mut new_tree = *child;
    for (pred, aliases) in to_promote {
        new_tree = promote_predicate(new_tree, pred, &aliases);
    }

    // Wrap remaining predicates (if any)
    if remaining.is_empty() {
        Ok(new_tree)
    } else {
        Ok(OpTree::Filter {
            predicate: join_and_predicates(remaining)?,
            child: Box::new(new_tree),
        })
    }
}

/// Check if an OpTree contains any InnerJoin with condition = Literal("TRUE").
fn has_cross_join(op: &OpTree) -> bool {
    match op {
        OpTree::InnerJoin {
            condition,
            left,
            right,
            ..
        } => {
            matches!(condition, Expr::Literal(s) if s == "TRUE")
                || has_cross_join(left)
                || has_cross_join(right)
        }
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Subquery { child, .. } => has_cross_join(child),
        OpTree::SemiJoin { left, .. } | OpTree::AntiJoin { left, .. } => has_cross_join(left),
        _ => false,
    }
}

/// Returns `true` if an `Expr` tree contains a scalar subquery (embedded
/// SELECT).  Correlated scalar subqueries arrive as `Expr::Raw(sql)` from
/// the parser; we also recurse into `BinaryOp` / `FuncCall` children.
fn expr_contains_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::Raw(sql) => {
            // Case-insensitive check for SELECT keyword inside raw SQL
            let upper = sql.to_uppercase();
            upper.contains("SELECT") || upper.contains("EXISTS")
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_subquery(left) || expr_contains_subquery(right)
        }
        Expr::FuncCall { args, .. } => args.iter().any(expr_contains_subquery),
        _ => false,
    }
}

/// Split an AND-connected expression into its individual conjuncts.
pub(crate) fn split_and_predicates(expr: Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp { op, left, right } if op.eq_ignore_ascii_case("AND") => {
            let mut parts = split_and_predicates(*left);
            parts.extend(split_and_predicates(*right));
            parts
        }
        other => vec![other],
    }
}

/// Join predicates with AND.
///
/// Returns `Err(PgTrickleError::InvalidArgument)` if `parts` is empty.
/// All call sites guarantee at least one predicate, but this function
/// avoids a production `expect()` panic to comply with the AGENTS.md
/// no-unwrap convention.
pub(crate) fn join_and_predicates(parts: Vec<Expr>) -> Result<Expr, PgTrickleError> {
    let mut iter = parts.into_iter();
    let mut result = iter
        .next()
        .ok_or_else(|| PgTrickleError::InvalidArgument("empty predicate set".into()))?;
    for part in iter {
        result = Expr::BinaryOp {
            op: "AND".to_string(),
            left: Box::new(result),
            right: Box::new(part),
        };
    }
    Ok(result)
}

/// Collect all source-table aliases referenced by column refs in an expression.
///
/// For qualified column refs (`n1.n_name`), the table alias is used directly
/// if it matches a Scan in the tree.  For unqualified refs (`s_suppkey`),
/// the tree is searched to find which Scan owns the column.
fn collect_expr_source_aliases(expr: &Expr, tree: &OpTree, aliases: &mut Vec<String>) {
    match expr {
        Expr::ColumnRef {
            table_alias: Some(tbl),
            column_name: _,
        } if scan_has_alias(tree, tbl) => {
            aliases.push(tbl.clone());
        }
        Expr::ColumnRef {
            table_alias: None,
            column_name,
        } => {
            if let Some(alias) = find_scan_for_column(tree, column_name) {
                aliases.push(alias);
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_expr_source_aliases(left, tree, aliases);
            collect_expr_source_aliases(right, tree, aliases);
        }
        Expr::FuncCall { args, .. } => {
            for arg in args {
                collect_expr_source_aliases(arg, tree, aliases);
            }
        }
        Expr::Raw(sql) => {
            // Best-effort: look for `alias.column` patterns in raw SQL.
            // This won't catch everything but handles common cases.
            for alias in collect_tree_scan_aliases(tree) {
                if sql.contains(&format!("{}.", alias)) || sql.contains(&format!("\"{}\".", alias))
                {
                    aliases.push(alias);
                }
            }
        }
        _ => {}
    }
}

/// Check if the tree contains a Scan with the given alias.
fn scan_has_alias(op: &OpTree, alias: &str) -> bool {
    match op {
        OpTree::Scan { alias: a, .. } => a == alias,
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. }
        | OpTree::SemiJoin { left, right, .. }
        | OpTree::AntiJoin { left, right, .. } => {
            scan_has_alias(left, alias) || scan_has_alias(right, alias)
        }
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Subquery { child, .. }
        | OpTree::Aggregate { child, .. }
        | OpTree::Distinct { child, .. } => scan_has_alias(child, alias),
        OpTree::LateralFunction { child, .. } | OpTree::LateralSubquery { child, .. } => {
            scan_has_alias(child, alias)
        }
        _ => false,
    }
}

/// Find which Scan node owns a given column name.
fn find_scan_for_column(op: &OpTree, column_name: &str) -> Option<String> {
    match op {
        OpTree::Scan { alias, columns, .. } => {
            if columns.iter().any(|c| c.name == column_name) {
                Some(alias.clone())
            } else {
                None
            }
        }
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. }
        | OpTree::SemiJoin { left, right, .. }
        | OpTree::AntiJoin { left, right, .. } => find_scan_for_column(left, column_name)
            .or_else(|| find_scan_for_column(right, column_name)),
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Subquery { child, .. }
        | OpTree::Aggregate { child, .. }
        | OpTree::Distinct { child, .. } => find_scan_for_column(child, column_name),
        OpTree::LateralFunction { child, .. } | OpTree::LateralSubquery { child, .. } => {
            find_scan_for_column(child, column_name)
        }
        _ => None,
    }
}

/// Collect all Scan aliases in the tree.
fn collect_tree_scan_aliases(op: &OpTree) -> Vec<String> {
    match op {
        OpTree::Scan { alias, .. } => vec![alias.clone()],
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. }
        | OpTree::SemiJoin { left, right, .. }
        | OpTree::AntiJoin { left, right, .. } => {
            let mut v = collect_tree_scan_aliases(left);
            v.extend(collect_tree_scan_aliases(right));
            v
        }
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Subquery { child, .. }
        | OpTree::Aggregate { child, .. }
        | OpTree::Distinct { child, .. } => collect_tree_scan_aliases(child),
        OpTree::LateralFunction { child, .. } | OpTree::LateralSubquery { child, .. } => {
            collect_tree_scan_aliases(child)
        }
        _ => vec![],
    }
}

/// Promote a predicate into the appropriate InnerJoin level.
///
/// Walks a left-deep InnerJoin chain top-down.  At each level, if the
/// right child contains any alias referenced by the predicate, this is
/// the level to attach it (the left subtree contains all other aliases).
fn promote_predicate(tree: OpTree, pred: Expr, aliases: &[String]) -> OpTree {
    match tree {
        OpTree::InnerJoin {
            condition,
            left,
            right,
        } => {
            // Check if the right child contains any of the referenced aliases
            let right_has_alias = aliases.iter().any(|a| scan_has_alias(&right, a));

            if right_has_alias && matches!(&condition, Expr::Literal(s) if s == "TRUE") {
                // Promote: replace TRUE with the predicate
                OpTree::InnerJoin {
                    condition: pred,
                    left,
                    right,
                }
            } else if right_has_alias {
                // Already has a condition — combine with AND
                OpTree::InnerJoin {
                    condition: Expr::BinaryOp {
                        op: "AND".to_string(),
                        left: Box::new(condition),
                        right: Box::new(pred),
                    },
                    left,
                    right,
                }
            } else {
                // The right child doesn't have any referenced alias.
                // Recurse into the left child (which is the deeper part
                // of the left-deep chain).
                OpTree::InnerJoin {
                    condition,
                    left: Box::new(promote_predicate(*left, pred, aliases)),
                    right,
                }
            }
        }
        // Pass through transparent wrappers (SemiJoin/AntiJoin wrap the join chain)
        OpTree::SemiJoin {
            condition,
            left,
            right,
        } => OpTree::SemiJoin {
            condition,
            left: Box::new(promote_predicate(*left, pred, aliases)),
            right,
        },
        OpTree::AntiJoin {
            condition,
            left,
            right,
        } => OpTree::AntiJoin {
            condition,
            left: Box::new(promote_predicate(*left, pred, aliases)),
            right,
        },
        // Can't promote further — shouldn't happen in well-formed trees
        other => other,
    }
}

#[cfg(feature = "pg_test")]
#[pg_schema]
mod pg_tests {
    use super::*;

    fn sorted_unique_oids(mut oids: Vec<u32>) -> Vec<u32> {
        oids.sort_unstable();
        oids.dedup();
        oids
    }

    fn regclass_oid(qualified_name: &str) -> u32 {
        Spi::get_one::<i32>(&format!("SELECT '{}'::regclass::oid::int4", qualified_name)) // nosemgrep: rust.spi.query.dynamic-format — test-only helper; qualified_name is always a hard-coded literal in tests, never runtime user input
            .expect("failed to look up relation oid")
            .expect("relation oid query returned NULL") as u32
    }

    fn tree_contains(tree: &OpTree, predicate: &impl Fn(&OpTree) -> bool) -> bool {
        if predicate(tree) {
            return true;
        }

        match tree {
            OpTree::Scan { .. } | OpTree::CteScan { .. } | OpTree::RecursiveSelfRef { .. } => false,
            OpTree::Project { child, .. }
            | OpTree::Filter { child, .. }
            | OpTree::Aggregate { child, .. }
            | OpTree::Distinct { child }
            | OpTree::Subquery { child, .. }
            | OpTree::Window { child, .. }
            | OpTree::LateralFunction { child, .. }
            | OpTree::LateralSubquery { child, .. } => tree_contains(child, predicate),
            OpTree::InnerJoin { left, right, .. }
            | OpTree::LeftJoin { left, right, .. }
            | OpTree::FullJoin { left, right, .. }
            | OpTree::Intersect { left, right, .. }
            | OpTree::Except { left, right, .. }
            | OpTree::SemiJoin { left, right, .. }
            | OpTree::AntiJoin { left, right, .. } => {
                tree_contains(left, predicate) || tree_contains(right, predicate)
            }
            OpTree::UnionAll { children } => {
                children.iter().any(|child| tree_contains(child, predicate))
            }
            OpTree::RecursiveCte {
                base, recursive, ..
            } => tree_contains(base, predicate) || tree_contains(recursive, predicate),
            OpTree::ScalarSubquery {
                subquery, child, ..
            } => tree_contains(subquery, predicate) || tree_contains(child, predicate),
            OpTree::ConstantSelect { .. } => false,
        }
    }

    #[pg_test]
    fn test_parse_defining_query_full_summarizes_cte_join_query() {
        Spi::run(
            "CREATE TABLE parser_orders_cte (
                id INT PRIMARY KEY,
                customer_id INT NOT NULL,
                amount INT NOT NULL
            )",
        )
        .expect("failed to create parser_orders_cte");
        Spi::run(
            "CREATE TABLE parser_customers_cte (
                id INT PRIMARY KEY,
                name TEXT NOT NULL
            )",
        )
        .expect("failed to create parser_customers_cte");

        let result = parse_defining_query_full(
            "WITH totals AS (
                SELECT customer_id, SUM(amount) AS total
                FROM parser_orders_cte
                GROUP BY customer_id
            )
            SELECT LOWER(c.name) AS customer_name, t.total
            FROM parser_customers_cte c
            JOIN totals t ON c.id = t.customer_id",
        )
        .expect("failed to parse CTE join query");

        let customers_oid = regclass_oid("parser_customers_cte");
        let orders_oid = regclass_oid("parser_orders_cte");
        let source_oids = sorted_unique_oids({
            let mut combined = result.tree.source_oids();
            combined.extend(result.cte_registry.source_oids());
            combined
        });

        assert!(!result.has_recursion);
        assert_eq!(result.cte_registry.entries.len(), 1);
        assert_eq!(result.tree.output_columns(), vec!["customer_name", "total"]);
        assert_eq!(source_oids, vec![customers_oid, orders_oid]);
        assert!(tree_contains(&result.tree, &|node| matches!(
            node,
            OpTree::CteScan { .. }
        )));
        assert_eq!(result.functions_used(), vec!["lower"]);
        assert_eq!(
            result
                .source_columns_used()
                .get(&customers_oid)
                .cloned()
                .unwrap_or_default(),
            vec!["id".to_string(), "name".to_string()]
        );
        assert_eq!(
            result
                .source_columns_used()
                .get(&orders_oid)
                .cloned()
                .unwrap_or_default(),
            vec!["amount".to_string(), "customer_id".to_string()]
        );
        assert!(check_ivm_support_with_registry(&result).is_ok());
        assert!(query_has_cte(
            "WITH totals AS (SELECT customer_id, SUM(amount) AS total FROM parser_orders_cte GROUP BY customer_id) SELECT LOWER(c.name) AS customer_name, t.total FROM parser_customers_cte c JOIN totals t ON c.id = t.customer_id"
        )
        .expect("failed to detect WITH clause"));
    }

    #[pg_test]
    fn test_parse_defining_query_full_summarizes_window_query() {
        Spi::run(
            "CREATE TABLE parser_sales_window (
                id INT PRIMARY KEY,
                region TEXT NOT NULL,
                amount INT NOT NULL
            )",
        )
        .expect("failed to create parser_sales_window");

        let result = parse_defining_query_full(
            "SELECT id, region,
                    ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn
             FROM parser_sales_window",
        )
        .expect("failed to parse window query");

        let sales_oid = regclass_oid("parser_sales_window");

        assert!(!result.has_recursion);
        assert!(result.cte_registry.entries.is_empty());
        assert_eq!(result.tree.output_columns(), vec!["id", "region", "rn"]);
        assert_eq!(
            sorted_unique_oids(result.tree.source_oids()),
            vec![sales_oid]
        );
        assert!(tree_contains(&result.tree, &|node| matches!(
            node,
            OpTree::Window { .. }
        )));
        assert_eq!(
            result
                .source_columns_used()
                .get(&sales_oid)
                .cloned()
                .unwrap_or_default(),
            vec!["amount".to_string(), "id".to_string(), "region".to_string()]
        );
        assert!(check_ivm_support_with_registry(&result).is_ok());
        assert!(
            !query_has_cte(
                "SELECT id, region, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn FROM parser_sales_window"
            )
            .expect("failed to detect CTE absence")
        );
    }

    #[pg_test]
    fn test_parse_defining_query_full_summarizes_scalar_subquery() {
        Spi::run(
            "CREATE TABLE parser_orders_scalar (
                id INT PRIMARY KEY,
                amount INT NOT NULL
            )",
        )
        .expect("failed to create parser_orders_scalar");
        Spi::run(
            "CREATE TABLE parser_config_scalar (
                id INT PRIMARY KEY,
                tax_rate INT NOT NULL
            )",
        )
        .expect("failed to create parser_config_scalar");

        let result = parse_defining_query_full(
            "SELECT o.id,
                    (SELECT c.tax_rate FROM parser_config_scalar c WHERE c.id = 1) AS current_tax
             FROM parser_orders_scalar o",
        )
        .expect("failed to parse scalar subquery");

        let orders_oid = regclass_oid("parser_orders_scalar");
        let config_oid = regclass_oid("parser_config_scalar");

        assert!(!result.has_recursion);
        assert!(result.cte_registry.entries.is_empty());
        assert_eq!(result.tree.output_columns(), vec!["id", "current_tax"]);
        assert_eq!(
            sorted_unique_oids(result.tree.source_oids()),
            vec![config_oid, orders_oid]
        );
        assert!(tree_contains(&result.tree, &|node| matches!(
            node,
            OpTree::ScalarSubquery { .. }
        )));
        assert_eq!(
            result
                .source_columns_used()
                .get(&orders_oid)
                .cloned()
                .unwrap_or_default(),
            vec!["id".to_string()]
        );
        assert_eq!(
            result
                .source_columns_used()
                .get(&config_oid)
                .cloned()
                .unwrap_or_default(),
            vec!["id".to_string(), "tax_rate".to_string()]
        );
        assert!(check_ivm_support_with_registry(&result).is_ok());
    }

    #[pg_test]
    fn test_parse_defining_query_full_detects_recursive_ctes() {
        Spi::run(
            "CREATE TABLE parser_nodes_recursive (
                id INT PRIMARY KEY,
                parent_id INT
            )",
        )
        .expect("failed to create parser_nodes_recursive");

        let query = "WITH RECURSIVE tree AS (
                SELECT id, parent_id
                FROM parser_nodes_recursive
                WHERE parent_id IS NULL
                UNION ALL
                SELECT n.id, n.parent_id
                FROM parser_nodes_recursive n
                JOIN tree t ON n.parent_id = t.id
            )
            SELECT id, parent_id FROM tree";

        let result = parse_defining_query_full(query).expect("failed to parse recursive CTE");

        assert!(result.has_recursion);
        assert!(query_has_cte(query).expect("failed to detect recursive WITH"));
        assert!(query_has_recursive_cte(query).expect("failed to detect recursive WITH"));
    }
}

// ── TEST-3: pure-Rust unit tests (no backend required) ──────────────────────
//
// 30+ tests covering pure-Rust helpers inside the rewrite module.
// Functions that require a live PostgreSQL backend are tested via the
// #[pg_test] suite above; these cover only backend-independent logic.

#[cfg(test)]
mod unit_tests {
    use super::*;

    // ── expr_has_aggregate ──────────────────────────────────────────────

    #[test]
    fn test_expr_has_aggregate_sum() {
        assert!(expr_has_aggregate("sum(amount)"));
    }

    #[test]
    fn test_expr_has_aggregate_max() {
        assert!(expr_has_aggregate("MAX(price)"));
    }

    #[test]
    fn test_expr_has_aggregate_count() {
        assert!(expr_has_aggregate("count(*)"));
    }

    #[test]
    fn test_expr_has_aggregate_avg() {
        assert!(expr_has_aggregate("avg(value)"));
    }

    #[test]
    fn test_expr_has_aggregate_array_agg() {
        assert!(expr_has_aggregate("array_agg(id ORDER BY id)"));
    }

    #[test]
    fn test_expr_has_aggregate_false_for_plain_col() {
        assert!(!expr_has_aggregate("price"));
    }

    #[test]
    fn test_expr_has_aggregate_false_for_window() {
        // row_number() is a window func, not an aggregate
        assert!(!expr_has_aggregate("row_number()"));
    }

    // ── strip_table_qualifier ───────────────────────────────────────────

    #[test]
    fn test_strip_qualifier_unqualified() {
        assert_eq!(strip_table_qualifier("id"), "id");
    }

    #[test]
    fn test_strip_qualifier_single_dot() {
        assert_eq!(strip_table_qualifier("t.id"), "id");
    }

    #[test]
    fn test_strip_qualifier_schema_table_col() {
        // Only the part after the FIRST dot is kept
        assert_eq!(strip_table_qualifier("public.orders.id"), "orders.id");
    }

    // ── contains_word_boundary ──────────────────────────────────────────

    #[test]
    fn test_word_boundary_present() {
        assert!(contains_word_boundary(
            "SELECT DISTINCT col FROM t",
            "DISTINCT"
        ));
    }

    #[test]
    fn test_word_boundary_at_start() {
        assert!(contains_word_boundary("DISTINCT col", "DISTINCT"));
    }

    #[test]
    fn test_word_boundary_at_end() {
        assert!(contains_word_boundary("col DISTINCT", "DISTINCT"));
    }

    #[test]
    fn test_word_boundary_absent() {
        assert!(!contains_word_boundary(
            "SELECT DISTINCTLY col FROM t",
            "DISTINCT"
        ));
    }

    #[test]
    fn test_word_boundary_partial_prefix() {
        // "INTO" should not match "INTO" inside "NATURAL"
        assert!(!contains_word_boundary("NATURAL JOIN t", "INTO"));
    }

    #[test]
    fn test_word_boundary_underscore_adjacent_not_boundary() {
        // "name_col" should NOT count as word-boundary match for "name"
        assert!(!contains_word_boundary("name_col", "name"));
    }

    #[test]
    fn test_word_boundary_exact_string() {
        assert!(contains_word_boundary("RECURSIVE", "RECURSIVE"));
    }

    // ── query-level structural detection (pure-regex, no parser) ────────
    // These call the same pattern-based helpers used internally.

    #[test]
    fn test_expr_has_aggregate_bool_and() {
        assert!(expr_has_aggregate("bool_and(flag)"));
    }

    #[test]
    fn test_expr_has_aggregate_bool_or() {
        assert!(expr_has_aggregate("bool_or(flag)"));
    }

    #[test]
    fn test_expr_has_aggregate_every() {
        assert!(expr_has_aggregate("every(active)"));
    }

    #[test]
    fn test_expr_has_aggregate_min() {
        assert!(expr_has_aggregate("min(timestamp)"));
    }

    #[test]
    fn test_strip_qualifier_empty_string() {
        assert_eq!(strip_table_qualifier(""), "");
    }

    #[test]
    fn test_word_boundary_empty_text() {
        assert!(!contains_word_boundary("", "DISTINCT"));
    }

    #[test]
    fn test_word_boundary_empty_word() {
        // Searching for empty word: every position matches
        assert!(contains_word_boundary("any text", ""));
    }

    #[test]
    fn test_word_boundary_with_comma_separator() {
        // "a, b" — word "a" at start, followed by comma (non-alnum, non-underscore)
        assert!(contains_word_boundary("a, b", "a"));
    }

    #[test]
    fn test_word_boundary_with_paren_separator() {
        assert!(contains_word_boundary("SUM(col)", "SUM"));
    }

    #[test]
    fn test_word_boundary_distinct_in_distinct_on() {
        // "DISTINCT ON" — "DISTINCT" appears followed by space
        assert!(contains_word_boundary("DISTINCT ON (col)", "DISTINCT"));
    }

    #[test]
    fn test_strip_qualifier_dot_at_start() {
        // Edge case: dot at position 0
        assert_eq!(strip_table_qualifier(".col"), "col");
    }

    #[test]
    fn test_expr_has_aggregate_string_agg() {
        assert!(expr_has_aggregate("string_agg(name, ',')"));
    }
}
