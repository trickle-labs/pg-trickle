//! LATERAL subquery differentiation via row-scoped recomputation.
//!
//! Strategy: When outer source rows change, re-execute the correlated
//! subquery only for affected rows. This is the same strategy as
//! [`LateralFunction`] but for full subqueries instead of SRFs.
//!
//! CTE chain:
//! 1. Child delta (from recursive diff_node on the LATERAL dependency)
//! 2. Old ST rows for changed source rows (emitted as 'D' actions)
//! 3. Re-execute the subquery for inserted/updated source rows (emitted as 'I' actions)
//! 4. Combine deletes + inserts into final delta
//!
//! Row identity: `hash(child_row_columns || '/' || subquery_result)` — content-based.
//!
//! LEFT JOIN LATERAL: uses `LEFT JOIN LATERAL (...) ON true` in the expand
//! CTE so that outer rows without matching inner rows produce NULL-padded rows.

use crate::dvm::diff::{DiffContext, DiffResult, col_list, quote_ident};
use crate::dvm::operators::scan::build_hash_expr;
use crate::dvm::parser::OpTree;
use crate::error::PgTrickleError;

/// Sentinel row_id for inner-change-branch dummy rows.
///
/// Uses `i64::MIN + 1` to minimise collision probability with real
/// `pg_trickle_hash` values (which span the full i64 range via
/// bit-pattern cast from xxHash). The previous value `0` was within
/// the common output range of the hash function.
///
/// Note: `i64::MIN` (-9223372036854775808) cannot be written as a
/// PostgreSQL bigint literal because the parser sees it as
/// `-(9223372036854775808::BIGINT)` and `9223372036854775808 > i64::MAX`,
/// producing "bigint out of range". `i64::MIN + 1` avoids this.
const LATERAL_INNER_DUMMY_ROW_ID: i64 = i64::MIN + 1; // -9223372036854775807

/// Differentiate a LateralSubquery node via row-scoped recomputation.
///
/// For each source row that changed (INSERT/UPDATE/DELETE), delete old
/// subquery results from the ST and re-execute the subquery for the
/// new version of the source row.
pub fn diff_lateral_subquery(
    ctx: &mut DiffContext,
    op: &OpTree,
) -> Result<DiffResult, PgTrickleError> {
    let OpTree::LateralSubquery {
        subquery_sql,
        alias,
        column_aliases,
        output_cols,
        is_left_join,
        subquery_source_oids,
        correlation_predicates,
        child,
    } = op
    else {
        return Err(PgTrickleError::InternalError(
            "diff_lateral_subquery called on non-LateralSubquery node".into(),
        ));
    };

    // ── Differentiate child to get the source delta ────────────────────
    let child_result = ctx.diff_node(child)?;

    let st_table = ctx
        .st_qualified_name
        .clone()
        .unwrap_or_else(|| "/* st_table */".to_string());

    // Column names from the child (source table columns)
    let child_cols = &child_result.columns;

    // Subquery result column names
    let sub_cols: Vec<String> = if column_aliases.is_empty() {
        output_cols.clone()
    } else {
        column_aliases.clone()
    };

    // All output columns = child columns + subquery columns
    let mut all_output_cols: Vec<String> = child_cols.clone();
    all_output_cols.extend(sub_cols.iter().cloned());

    // ── Resolve ST column names ─────────────────────────────────────
    //
    // The ST may have aliased column names (e.g. `a.id AS a_id`
    // becomes `a_id` in the ST, while the child scan has `id`).
    // `ctx.st_user_columns` may have been remapped by diff_project to
    // use child-level names. Use `st_column_alias_map` to translate
    // back to the *actual* ST column names for `st.*` references.
    let actual_st_col = |col: &str| -> String {
        if let Some(ref map) = ctx.st_column_alias_map {
            map.get(col).cloned().unwrap_or_else(|| col.to_string())
        } else {
            col.to_string()
        }
    };

    let st_col_names: Vec<String> = all_output_cols.iter().map(|c| actual_st_col(c)).collect();
    let st_child_cols: Vec<String> = child_cols.iter().map(|c| actual_st_col(c)).collect();

    // Determine which output columns actually exist in the stream table.
    // When a Project above narrows the defining query's output, some of
    // this operator's columns (e.g., source PK not in SELECT, scalar
    // subquery alias from WHERE rewrite) won't exist in the ST.
    let col_in_st: Vec<bool> = if let Some(ref st_cols) = ctx.st_user_columns {
        let st_set: std::collections::HashSet<String> =
            st_cols.iter().map(|s| actual_st_col(s)).collect();
        all_output_cols
            .iter()
            .map(|c| st_set.contains(&actual_st_col(c)))
            .collect()
    } else {
        vec![true; all_output_cols.len()]
    };

    // Old rows must be identified by the same stable outer identity used by
    // the child operator. Matching on only the projected columns can delete
    // multiple outer rows (or degrade to ON TRUE) when a Project removed the
    // identity. Fail closed instead of generating a broad deletion join.
    let outer_identity = child.row_id_key_columns().ok_or_else(|| {
        PgTrickleError::UnsupportedOperator(
            "LATERAL subquery requires an exact outer row identity for deletion; \
             use FULL or retain a stable outer key in the query output."
                .into(),
        )
    })?;
    if outer_identity.is_empty() {
        return Err(PgTrickleError::UnsupportedOperator(
            "LATERAL subquery has no usable outer row identity for deletion; \
             use FULL or retain a stable outer key in the query output."
                .into(),
        ));
    }
    for key in &outer_identity {
        let child_index = child_cols.iter().position(|c| c == key).ok_or_else(|| {
            PgTrickleError::UnsupportedOperator(format!(
                "LATERAL subquery outer identity column \"{key}\" is not available \
                 from the child delta; use FULL or retain the outer key."
            ))
        })?;
        if !col_in_st[child_index] {
            return Err(PgTrickleError::UnsupportedOperator(format!(
                "LATERAL subquery outer identity column \"{key}\" is projected away \
                 from the stream table; use FULL or retain the outer key."
            )));
        }
    }

    // ── CTE 1: Find source rows that changed ───────────────────────────
    //
    // Starts with rows from the child delta (outer table changes).
    // When inner subquery sources also have changes, ALL current outer
    // rows are included so that their LATERAL subquery results are
    // recomputed (the inner aggregate value may have changed).
    let changed_sources_cte = ctx.next_cte_name("lat_sq_changed");

    // Build an inner-source-change check: if ANY inner source table has
    // new change-buffer rows, we must recompute the LATERAL subquery for
    // every current outer row.
    let inner_change_branch = build_inner_change_branch(
        ctx,
        child,
        child_cols,
        subquery_source_oids,
        correlation_predicates,
    )?;

    let changed_sources_sql = if let Some(inner_branch) = &inner_change_branch {
        format!(
            "SELECT DISTINCT \"__pgt_row_id\", \"__pgt_action\", {child_col_list}\n\
             FROM {child_delta}\n\
             UNION ALL\n\
             {inner_branch}",
            child_col_list = col_list(child_cols),
            child_delta = child_result.cte_name,
        )
    } else {
        format!(
            "SELECT DISTINCT \"__pgt_row_id\", \"__pgt_action\", {child_col_list}\n\
             FROM {child_delta}",
            child_col_list = col_list(child_cols),
            child_delta = child_result.cte_name,
        )
    };
    let inner_change_branch_present = inner_change_branch.is_some();
    ctx.add_cte(changed_sources_cte.clone(), changed_sources_sql);

    // ── CTE 2: Re-execute subquery for inserted/updated source rows ────
    //
    // Defined BEFORE old_rows so that old_rows can reference expand's
    // column types when NULL-padding absent columns (see CTE 3 below).
    let expand_cte = ctx.next_cte_name("lat_sq_expand");

    // Build column references for the subquery result
    let sub_col_refs: Vec<String> = sub_cols
        .iter()
        .map(|c| format!("{}.{}", quote_ident(alias), quote_ident(c)))
        .collect();
    let sub_col_refs_str = sub_col_refs.join(", ");

    // Use the outer table's original alias for the changed-sources CTE
    // so that the subquery's column references resolve naturally.
    let outer_alias = child.alias().to_string();
    let child_col_refs: Vec<String> = child_cols
        .iter()
        .map(|c| format!("{}.{}", quote_ident(&outer_alias), quote_ident(c)))
        .collect();
    let child_col_refs_str = child_col_refs.join(", ");

    // Build hash expression for the row ID: hash all output columns.
    // Do NOT use COALESCE for NULL — pg_trickle_hash_multi handles NULL
    // elements by hashing a '\x00NULL\x00' sentinel, which keeps this
    // consistent with the initial-load hash in row_id_expr_for_query.
    let hash_exprs: Vec<String> = child_cols
        .iter()
        .map(|c| format!("{}.{}::TEXT", quote_ident(&outer_alias), quote_ident(c)))
        .chain(
            sub_cols
                .iter()
                .map(|c| format!("{}.{}::TEXT", quote_ident(alias), quote_ident(c))),
        )
        .collect();
    let row_id_expr = build_hash_expr(&hash_exprs);

    // Build the subquery alias clause
    let sub_alias_clause = if column_aliases.is_empty() {
        quote_ident(alias)
    } else {
        let col_alias_list = sub_cols
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        format!("{} ({col_alias_list})", quote_ident(alias))
    };

    // ── P2-7: Pre-compute LATERAL results per correlation group ────────
    //
    // When correlation predicates are available (self-referencing LATERAL
    // with scoped inner branch), the LATERAL subquery result depends only
    // on the correlation columns for many common patterns (aggregates,
    // scalar subqueries). Pre-computing the LATERAL for each distinct
    // correlation group and JOINing avoids O(N × L) nested loop execution
    // without Memoize on CTE scans (PostgreSQL doesn't add Memoize for
    // CTEs), reducing cost to O(G × L + N) where G = distinct groups.
    //
    // The pre-computation uses the outer alias so that column references
    // in the subquery_sql (e.g. `s.region`) resolve correctly.
    let use_precomp = inner_change_branch_present && !correlation_predicates.is_empty();

    if use_precomp {
        // Determine the outer column names used in correlation predicates.
        // These columns fully determine the LATERAL subquery result for
        // the common aggregate pattern.
        let corr_outer_cols: Vec<String> = {
            let mut cols: Vec<String> = correlation_predicates
                .iter()
                .map(|p| p.outer_col.clone())
                .collect();
            cols.sort();
            cols.dedup();
            cols
        };

        let outer_alias_q = quote_ident(&outer_alias);
        let corr_col_refs: Vec<String> = corr_outer_cols
            .iter()
            .map(|c| format!("{outer_alias_q}.{}", quote_ident(c)))
            .collect();
        let corr_col_list = corr_col_refs.join(", ");

        // CTE: distinct correlation groups from changed_sources
        let groups_cte = ctx.next_cte_name("lat_sq_groups");
        let groups_sql = format!(
            "SELECT DISTINCT {corr_col_list}\n\
             FROM {changed_sources_cte} {outer_alias_q}\n\
             WHERE {outer_alias_q}.\"__pgt_action\" = 'I'",
        );
        ctx.add_cte(groups_cte.clone(), groups_sql);

        // CTE: pre-computed LATERAL results per group.
        // The groups CTE is aliased with the outer alias so that
        // subquery_sql references (e.g. `s.region`) resolve correctly.
        let precomp_cte = ctx.next_cte_name("lat_sq_precomp");
        let precomp_lateral = if *is_left_join {
            format!(
                "FROM {groups_cte} AS {outer_alias_q}\n\
                 LEFT JOIN LATERAL ({subquery_sql}) AS {sub_alias_clause} ON true",
            )
        } else {
            format!(
                "FROM {groups_cte} AS {outer_alias_q},\n\
                      LATERAL ({subquery_sql}) AS {sub_alias_clause}",
            )
        };
        let precomp_sub_refs: Vec<String> = sub_cols
            .iter()
            .map(|c| format!("{}.{}", quote_ident(alias), quote_ident(c)))
            .collect();
        let precomp_sql = format!(
            "SELECT {corr_col_list}, {sub_refs}\n\
             {precomp_lateral}",
            sub_refs = precomp_sub_refs.join(", "),
        );
        ctx.add_cte(precomp_cte.clone(), precomp_sql);

        // Build the expand CTE as a JOIN with the pre-computed results
        // instead of a row-by-row LATERAL evaluation.
        let join_type = if *is_left_join { "LEFT JOIN" } else { "JOIN" };
        let corr_join_cond: Vec<String> = corr_outer_cols
            .iter()
            .map(|c| {
                let qc = quote_ident(c);
                format!(
                    "{outer_alias_q}.{qc} IS NOT DISTINCT FROM {pc}.{qc}",
                    pc = quote_ident(&format!("__pgt_pc_{}", alias)),
                )
            })
            .collect();
        let pc_alias = quote_ident(&format!("__pgt_pc_{}", alias));
        let pc_sub_refs: Vec<String> = sub_cols
            .iter()
            .map(|c| format!("{pc_alias}.{}", quote_ident(c)))
            .collect();

        // Rebuild hash_exprs using the pre-computed sub-column references
        let hash_exprs_precomp: Vec<String> = child_cols
            .iter()
            .map(|c| format!("{outer_alias_q}.{}::TEXT", quote_ident(c)))
            .chain(
                sub_cols
                    .iter()
                    .map(|c| format!("{pc_alias}.{}::TEXT", quote_ident(c))),
            )
            .collect();
        let row_id_expr_precomp = build_hash_expr(&hash_exprs_precomp);

        let expand_sql = format!(
            "SELECT {row_id_expr_precomp} AS \"__pgt_row_id\",\n\
                    {child_col_refs_str},\n\
                    {pc_sub_refs_str}\n\
             FROM {changed_sources_cte} AS {outer_alias_q}\n\
             {join_type} {precomp_cte} AS {pc_alias}\n\
               ON {corr_join_cond_str}\n\
             WHERE {outer_alias_q}.\"__pgt_action\" = 'I'",
            pc_sub_refs_str = pc_sub_refs.join(", "),
            corr_join_cond_str = corr_join_cond.join(" AND "),
        );
        ctx.add_cte(expand_cte.clone(), expand_sql);
    } else {
        // Standard path: evaluate LATERAL per row (efficient for small deltas)
        let (lateral_clause, action_filter_prefix) = if *is_left_join {
            (
                format!(
                    "FROM {changed_sources_cte} AS {outer_alias_q}\n\
                     LEFT JOIN LATERAL ({subquery_sql}) AS {sub_alias_clause} ON true",
                    outer_alias_q = quote_ident(&outer_alias),
                ),
                format!(
                    "{outer_alias_q}.\"__pgt_action\" = 'I'",
                    outer_alias_q = quote_ident(&outer_alias),
                ),
            )
        } else {
            (
                format!(
                    "FROM {changed_sources_cte} AS {outer_alias_q},\n\
                          LATERAL ({subquery_sql}) AS {sub_alias_clause}",
                    outer_alias_q = quote_ident(&outer_alias),
                ),
                format!(
                    "{outer_alias_q}.\"__pgt_action\" = 'I'",
                    outer_alias_q = quote_ident(&outer_alias),
                ),
            )
        };

        let expand_sql = format!(
            "SELECT {row_id_expr} AS \"__pgt_row_id\",\n\
                    {child_col_refs_str},\n\
                    {sub_col_refs_str}\n\
             {lateral_clause}\n\
             WHERE {action_filter_prefix}",
        );
        ctx.add_cte(expand_cte.clone(), expand_sql);
    }

    // ── CTE 3: Old ST rows for changed source rows (DELETE actions) ────
    let old_rows_cte = ctx.next_cte_name("lat_sq_old");

    // Build a join condition: match st.{st_col} = cs.{child_col}
    // The ST may use aliased names, while the changed_sources CTE uses
    // the child's original column names.
    // Only join on columns that actually exist in the ST (col_in_st).
    let join_parts: Vec<String> = outer_identity
        .iter()
        .map(|key| {
            let i = child_cols.iter().position(|c| c == key).ok_or_else(|| {
                PgTrickleError::InternalError(format!(
                    "LATERAL outer identity column \"{key}\" disappeared from child columns"
                ))
            })?;
            let st_c = &st_child_cols[i];
            let qc_child = quote_ident(key);
            let qc_st = quote_ident(st_c);
            Ok(format!("st.{qc_st} IS NOT DISTINCT FROM cs.{qc_child}"))
        })
        .collect::<Result<Vec<_>, PgTrickleError>>()?;
    let join_on_child_cols = join_parts.join(" AND ");

    // SELECT st columns with aliases back to the expected output names.
    // Columns that don't exist in the ST (projected away) are NULL-padded.
    let all_cols_st = all_output_cols
        .iter()
        .enumerate()
        .zip(st_col_names.iter())
        .map(|((i, out_c), st_c)| {
            if col_in_st[i] {
                let qst = quote_ident(st_c);
                let qout = quote_ident(out_c);
                if st_c == out_c {
                    format!("st.{qst}")
                } else {
                    format!("st.{qst} AS {qout}")
                }
            } else {
                format!("NULL AS {}", quote_ident(out_c))
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    // When some columns are absent from the ST (col_in_st has false entries),
    // the bare `NULL AS "col"` values in the old_rows CTE resolve to `text`
    // type. This causes a UNION type mismatch in the final CTE where expand
    // has the correctly typed columns (e.g., integer). To fix this, prefix
    // old_rows with `SELECT * FROM {expand_cte} WHERE FALSE` — this returns
    // zero rows but establishes the correct column types via PostgreSQL's
    // UNION type resolution. The expand CTE is defined earlier in the chain,
    // so it's available for reference.
    let has_absent_cols = col_in_st.iter().any(|&b| !b);

    // ── P2-8: Optimize old_rows for self-referencing laterals ─────────
    //
    // When the inner branch is present, changed_sources can be very large
    // (all outer rows). The EXISTS semi-join on a materialized CTE uses a
    // Nested Loop plan: O(N × M). Instead, extract DISTINCT child column
    // values into a separate CTE and use INNER JOIN, which PostgreSQL can
    // execute as a Hash Join: O(N + M).
    let old_rows_sql = if inner_change_branch_present {
        let keys_cte = ctx.next_cte_name("lat_sq_keys");
        let key_col_refs: Vec<String> = outer_identity
            .iter()
            .map(|c| format!("cs.{}", quote_ident(c)))
            .collect();
        let key_col_refs_str = key_col_refs.join(", ");
        let keys_sql = format!(
            "SELECT DISTINCT {key_col_refs_str}\n\
             FROM {changed_sources_cte} cs",
        );
        ctx.add_cte(keys_cte.clone(), keys_sql);

        // Build JOIN condition between ST and keys CTE.
        // Use IS NOT DISTINCT FROM for null-safety. PostgreSQL 14+ can
        // use Hash Join with this operator, so it doesn't prevent good
        // plans. The keys CTE gives PostgreSQL a separate relation to
        // hash, enabling O(N+M) execution instead of O(N×M) EXISTS.
        let key_join_parts: Vec<String> = outer_identity
            .iter()
            .map(|key| {
                let i = child_cols.iter().position(|c| c == key).ok_or_else(|| {
                    PgTrickleError::InternalError(format!(
                        "LATERAL outer identity column \"{key}\" disappeared from child columns"
                    ))
                })?;
                let qc_child = quote_ident(key);
                let qc_st = quote_ident(&st_child_cols[i]);
                Ok(format!("st.{qc_st} IS NOT DISTINCT FROM ck.{qc_child}"))
            })
            .collect::<Result<Vec<_>, PgTrickleError>>()?;
        let key_join_cond = key_join_parts.join(" AND ");

        if has_absent_cols {
            format!(
                "SELECT \"__pgt_row_id\", {all_cols_name} FROM {expand_cte} WHERE FALSE\n\
                 UNION ALL\n\
                 SELECT st.\"__pgt_row_id\", {all_cols_st}\n\
                 FROM {st_table} st\n\
                 JOIN {keys_cte} ck ON {key_join_cond}",
                all_cols_name = col_list(&all_output_cols),
            )
        } else {
            format!(
                "SELECT st.\"__pgt_row_id\", {all_cols_st}\n\
                 FROM {st_table} st\n\
                 JOIN {keys_cte} ck ON {key_join_cond}",
            )
        }
    } else if has_absent_cols {
        format!(
            "SELECT \"__pgt_row_id\", {all_cols_name} FROM {expand_cte} WHERE FALSE\n\
             UNION ALL\n\
             SELECT st.\"__pgt_row_id\", {all_cols_st}\n\
             FROM {st_table} st\n\
             WHERE EXISTS (\n\
                 SELECT 1 FROM {changed_sources_cte} cs\n\
                 WHERE {join_on_child_cols}\n\
             )",
            all_cols_name = col_list(&all_output_cols),
        )
    } else {
        format!(
            "SELECT st.\"__pgt_row_id\", {all_cols_st}\n\
             FROM {st_table} st\n\
             WHERE EXISTS (\n\
                 SELECT 1 FROM {changed_sources_cte} cs\n\
                 WHERE {join_on_child_cols}\n\
             )",
        )
    };
    ctx.add_cte(old_rows_cte.clone(), old_rows_sql);

    // ── CTE 4: Final delta — DELETE old + INSERT new ───────────────────
    let final_cte = ctx.next_cte_name("lat_sq_final");

    let all_cols_name = col_list(&all_output_cols);

    let final_sql = format!(
        "-- Delete old subquery results for changed source rows\n\
         SELECT \"__pgt_row_id\", 'D' AS \"__pgt_action\", {all_cols_name}\n\
         FROM {old_rows_cte}\n\
         UNION ALL\n\
         -- Insert re-executed subquery results for new/updated source rows\n\
         SELECT \"__pgt_row_id\", 'I' AS \"__pgt_action\", {all_cols_name}\n\
         FROM {expand_cte}",
    );
    ctx.add_cte(final_cte.clone(), final_sql);

    Ok(DiffResult {
        cte_name: final_cte,
        columns: all_output_cols.clone(),
        schema: child_result
            .schema
            .concat(&crate::dvm::schema::RelationSchema::from_names(&sub_cols)),
        is_deduplicated: false,
        has_key_changed: false,
    })
}

/// Build a SQL branch that selects outer rows affected by inner source changes.
///
/// **P2-6 optimization:** When correlation predicates are available (e.g.,
/// `inner.fk = outer.pk`), only outer rows whose correlated column value
/// appears in the inner change buffer are selected — reducing the scan
/// from O(|outer|) to O(delta).
///
/// Falls back to selecting ALL outer rows when:
/// - No correlation predicates were extracted (complex WHERE clause)
/// - An inner source OID has no matching correlation predicate
///
/// Returns `None` if there are no inner source OIDs to monitor, or if
/// the delta source is not change-buffer based (IMMEDIATE mode).
fn build_inner_change_branch(
    ctx: &DiffContext,
    child: &OpTree,
    child_cols: &[String],
    inner_oids: &[u32],
    correlation_predicates: &[crate::dvm::parser::CorrelationPredicate],
) -> Result<Option<String>, PgTrickleError> {
    use crate::dvm::diff::DeltaSource;
    use crate::dvm::operators::join_common::build_snapshot_sql;

    // Only change-buffer mode has persistent change tables to check.
    if !matches!(ctx.delta_source, DeltaSource::ChangeBuffer) {
        return Ok(None);
    }

    if inner_oids.is_empty() {
        return Ok(None);
    }
    if inner_oids.contains(&0) {
        return Err(PgTrickleError::UnsupportedOperator(
            "LATERAL subquery has an unresolved inner source dependency; use FULL or AUTO.".into(),
        ));
    }

    // Deduplicate inner OIDs (a shared OID may appear in both the child
    // and the inner subquery, e.g. a self-referencing LATERAL join).
    // We must NOT filter out OIDs that overlap with the child's own OID:
    // while the child delta covers new/changed outer rows, EXISTING
    // (unchanged) outer rows still need LATERAL recomputation when the
    // shared table has changes that affect the inner subquery result.
    let mut unique_inner_oids: Vec<u32> = inner_oids.to_vec();
    unique_inner_oids.sort_unstable();
    unique_inner_oids.dedup();

    let outer_snap = build_snapshot_sql(child);
    let outer_alias = child.alias();
    let col_refs: Vec<String> = child_cols
        .iter()
        .map(|c| format!("{}.{}", quote_ident(outer_alias), quote_ident(c)))
        .collect();
    let col_refs_str = col_refs.join(", ");

    // P2-6: Try to build scoped EXISTS using correlation predicates.
    // For each inner OID, check if we have a correlation predicate that
    // connects an inner column to an outer column. If so, use it to
    // scope the change buffer scan.
    let exists_checks: Vec<String> = unique_inner_oids
        .iter()
        .map(|oid| {
            let change_table = ctx.change_table_for_source(*oid);
            let prev_lsn = ctx.get_prev_lsn(*oid);
            let new_lsn = ctx.get_new_lsn(*oid);

            let lsn_filter =
                format!("c.lsn > '{prev_lsn}'::pg_lsn AND c.lsn <= '{new_lsn}'::pg_lsn");

            // Find correlation predicates for this inner OID.
            let corr_preds: Vec<_> = correlation_predicates
                .iter()
                .filter(|p| p.inner_oid == *oid)
                .collect();

            if corr_preds.is_empty() {
                // No scoping possible — check if ANY changes exist (full scan).
                Ok(format!(
                    "EXISTS (SELECT 1 FROM {change_table} c \
                     WHERE {lsn_filter} \
                     LIMIT 1)"
                ))
            } else {
                // Scoped: join change buffer on correlation column(s).
                // D+I flat schema (A44-10): each row carries the relevant
                // value in the flat column — old value for D-rows, new value
                // for I-rows. A single equality predicate covers both.
                let corr_conditions: Vec<String> = corr_preds
                    .iter()
                    .map(|p| {
                        if p.inner_oid == 0
                            || !inner_oids.contains(&p.inner_oid)
                            || !child_cols.iter().any(|c| c == &p.outer_col)
                            || p.inner_alias.is_empty()
                            || p.inner_col.is_empty()
                        {
                            return Err(PgTrickleError::UnsupportedOperator(
                                "LATERAL subquery has an unresolved outer/inner dependency; \
                                 use FULL or AUTO."
                                    .into(),
                            ));
                        }
                        let outer_ref =
                            format!("{}.{}", quote_ident(outer_alias), quote_ident(&p.outer_col));
                        let col = quote_ident(&p.inner_col);
                        Ok(format!("c.{col} = {outer_ref}"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let corr_filter = corr_conditions.join(" AND ");

                Ok(format!(
                    "EXISTS (SELECT 1 FROM {change_table} c \
                     WHERE {lsn_filter} AND {corr_filter})"
                ))
            }
        })
        .collect::<Result<Vec<_>, PgTrickleError>>()?;
    let any_inner_changed = exists_checks.join(" OR ");

    Ok(Some(format!(
        "-- Outer rows affected by inner subquery source changes\n\
         SELECT {LATERAL_INNER_DUMMY_ROW_ID}::BIGINT AS \"__pgt_row_id\",\n\
                'I'::TEXT AS \"__pgt_action\",\n\
                {col_refs_str}\n\
         FROM {outer_snap} {outer_alias_q}\n\
         WHERE {any_inner_changed}",
        outer_alias_q = quote_ident(outer_alias),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm::operators::test_helpers::*;

    /// Build a LateralSubquery node for tests.
    fn lateral_subquery(
        subquery_sql: &str,
        alias: &str,
        col_aliases: Vec<&str>,
        output_cols: Vec<&str>,
        is_left_join: bool,
        subquery_source_oids: Vec<u32>,
        child: OpTree,
    ) -> OpTree {
        OpTree::LateralSubquery {
            subquery_sql: subquery_sql.to_string(),
            alias: alias.to_string(),
            column_aliases: col_aliases.into_iter().map(|c| c.to_string()).collect(),
            output_cols: output_cols.into_iter().map(|c| c.to_string()).collect(),
            is_left_join,
            subquery_source_oids,
            correlation_predicates: Vec::new(),
            child: Box::new(child),
        }
    }

    /// Build a LateralSubquery node with correlation predicates for testing P2-6.
    #[allow(clippy::too_many_arguments)]
    fn lateral_subquery_with_corr(
        subquery_sql: &str,
        alias: &str,
        col_aliases: Vec<&str>,
        output_cols: Vec<&str>,
        is_left_join: bool,
        subquery_source_oids: Vec<u32>,
        correlation_predicates: Vec<crate::dvm::parser::CorrelationPredicate>,
        child: OpTree,
    ) -> OpTree {
        OpTree::LateralSubquery {
            subquery_sql: subquery_sql.to_string(),
            alias: alias.to_string(),
            column_aliases: col_aliases.into_iter().map(|c| c.to_string()).collect(),
            output_cols: output_cols.into_iter().map(|c| c.to_string()).collect(),
            is_left_join,
            subquery_source_oids,
            correlation_predicates,
            child: Box::new(child),
        }
    }

    #[test]
    fn test_diff_lateral_subquery_basic() {
        let mut ctx = test_ctx_with_st("public", "my_st");
        let child = scan(1, "orders", "public", "o", &["id", "customer"]);
        let tree = lateral_subquery(
            "SELECT amount, created_at FROM line_items li WHERE li.order_id = o.id ORDER BY created_at DESC LIMIT 1",
            "latest",
            vec![],
            vec!["amount", "created_at"],
            false,
            vec![2],
            child,
        );
        let result = diff_lateral_subquery(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Output should include child columns + subquery columns
        assert!(result.columns.contains(&"id".to_string()));
        assert!(result.columns.contains(&"customer".to_string()));
        assert!(result.columns.contains(&"amount".to_string()));
        assert!(result.columns.contains(&"created_at".to_string()));

        // Should have the CTE chain
        assert_sql_contains(&sql, "lat_sq_changed");
        assert_sql_contains(&sql, "lat_sq_old");
        assert_sql_contains(&sql, "lat_sq_expand");
        assert_sql_contains(&sql, "lat_sq_final");
    }

    #[test]
    fn test_diff_lateral_subquery_left_join() {
        let mut ctx = test_ctx_with_st("public", "my_st");
        let child = scan(1, "departments", "public", "d", &["id", "name"]);
        let tree = lateral_subquery(
            "SELECT SUM(salary) AS total, COUNT(*) AS cnt FROM employees e WHERE e.dept_id = d.id",
            "stats",
            vec!["total", "cnt"],
            vec!["total", "cnt"],
            true,
            vec![2],
            child,
        );
        let result = diff_lateral_subquery(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Should use LEFT JOIN LATERAL
        assert_sql_contains(&sql, "LEFT JOIN LATERAL");
        assert_sql_contains(&sql, "ON true");

        // Output should include all columns
        assert!(result.columns.contains(&"id".to_string()));
        assert!(result.columns.contains(&"name".to_string()));
        assert!(result.columns.contains(&"total".to_string()));
        assert!(result.columns.contains(&"cnt".to_string()));
    }

    #[test]
    fn test_diff_lateral_subquery_uses_original_alias() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "orders", "public", "o", &["id", "customer"]);
        let tree = lateral_subquery(
            "SELECT amount FROM line_items li WHERE li.order_id = o.id LIMIT 1",
            "latest",
            vec![],
            vec!["amount"],
            false,
            vec![2],
            child,
        );
        let result = diff_lateral_subquery(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // The expand CTE should alias the changed-sources row with the original "o" alias
        // so that the subquery's `o.id` reference resolves correctly
        assert_sql_contains(&sql, "AS \"o\"");
    }

    #[test]
    fn test_diff_lateral_subquery_old_rows_join_condition() {
        let mut ctx = test_ctx_with_st("public", "my_st");
        let child = scan(1, "parent", "public", "p", &["id", "data"]);
        let tree = lateral_subquery(
            "SELECT val FROM child_table c WHERE c.parent_id = p.id",
            "sub",
            vec![],
            vec!["val"],
            false,
            vec![2],
            child,
        );
        let result = diff_lateral_subquery(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Old rows CTE should join on child columns with IS NOT DISTINCT FROM
        assert_sql_contains(&sql, "IS NOT DISTINCT FROM");
    }

    #[test]
    fn test_diff_lateral_subquery_expand_filters_inserts() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["id", "val"]);
        let tree = lateral_subquery(
            "SELECT x FROM other o WHERE o.fk = t.id",
            "sub",
            vec![],
            vec!["x"],
            false,
            vec![2],
            child,
        );
        let result = diff_lateral_subquery(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // The expand CTE should only process INSERT actions
        assert_sql_contains(&sql, "__pgt_action\" = 'I'");
    }

    #[test]
    fn test_diff_lateral_subquery_hash_includes_all_columns() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["id", "data"]);
        let tree = lateral_subquery(
            "SELECT val FROM sub_t s WHERE s.fk = t.id",
            "sub",
            vec!["val"],
            vec!["val"],
            false,
            vec![2],
            child,
        );
        let result = diff_lateral_subquery(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Row ID hash should include both child and subquery columns
        assert_sql_contains(&sql, "pg_trickle_hash");
    }

    #[test]
    fn test_diff_lateral_subquery_output_columns() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["id", "data"]);
        let tree = lateral_subquery(
            "SELECT val FROM sub_t s WHERE s.fk = t.id",
            "sub",
            vec![],
            vec!["val"],
            false,
            vec![2],
            child,
        );
        let result = diff_lateral_subquery(&mut ctx, &tree).unwrap();
        assert_eq!(result.columns, vec!["id", "data", "val"]);
    }

    #[test]
    fn test_diff_lateral_subquery_not_deduplicated() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["id"]);
        let tree = lateral_subquery(
            "SELECT x FROM sub_t WHERE fk = t.id",
            "sub",
            vec![],
            vec!["x"],
            false,
            vec![2],
            child,
        );
        let result = diff_lateral_subquery(&mut ctx, &tree).unwrap();
        assert!(!result.is_deduplicated);
    }

    #[test]
    fn test_diff_lateral_subquery_error_on_wrong_node() {
        let mut ctx = test_ctx_with_st("public", "st");
        let tree = scan(1, "t", "public", "t", &["id"]);
        let result = diff_lateral_subquery(&mut ctx, &tree);
        assert!(result.is_err());
    }

    #[test]
    fn test_diff_lateral_subquery_with_column_aliases() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["id"]);
        let tree = lateral_subquery(
            "SELECT SUM(x) AS total FROM sub_t WHERE fk = t.id",
            "agg",
            vec!["total_amount"],
            vec!["total"],
            false,
            vec![2],
            child,
        );
        let result = diff_lateral_subquery(&mut ctx, &tree).unwrap();
        // When column_aliases are provided, they override output_cols
        assert!(result.columns.contains(&"total_amount".to_string()));
        assert!(!result.columns.contains(&"total".to_string()));
    }

    #[test]
    fn test_diff_lateral_subquery_left_join_null_safe_hash() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["id"]);
        let tree = lateral_subquery(
            "SELECT val FROM sub_t WHERE fk = t.id",
            "sub",
            vec!["val"],
            vec!["val"],
            true, // LEFT JOIN
            vec![2],
            child,
        );
        let result = diff_lateral_subquery(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // LEFT JOIN uses raw col::TEXT in hash (pg_trickle_hash_multi
        // handles NULLs via \x00NULL\x00 sentinel). No COALESCE needed.
        assert_sql_contains(&sql, "LEFT JOIN LATERAL");
        // Hash expression uses sub.val::TEXT without COALESCE
        assert_sql_contains(&sql, "\"sub\".\"val\"::TEXT");
    }

    #[test]
    fn test_diff_lateral_subquery_contains_lateral_keyword() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "t", "public", "t", &["id", "data"]);
        let tree = lateral_subquery(
            "SELECT val FROM sub_t s WHERE s.fk = t.id",
            "sub",
            vec![],
            vec!["val"],
            false,
            vec![2],
            child,
        );
        let result = diff_lateral_subquery(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Must use LATERAL keyword
        assert_sql_contains(&sql, "LATERAL (SELECT");
    }

    // ── OpTree method tests ─────────────────────────────────────────────

    #[test]
    fn test_lateral_subquery_output_columns_with_aliases() {
        let child = scan(1, "t", "public", "t", &["id", "data"]);
        let tree = lateral_subquery(
            "SELECT val FROM sub_t",
            "sub",
            vec!["result_val"],
            vec!["val"],
            false,
            vec![],
            child,
        );
        assert_eq!(tree.output_columns(), vec!["id", "data", "result_val"]);
    }

    #[test]
    fn test_lateral_subquery_output_columns_defaults_to_output_cols() {
        let child = scan(1, "t", "public", "t", &["id", "tags"]);
        let tree = lateral_subquery(
            "SELECT name FROM items",
            "sub",
            vec![],
            vec!["name"],
            false,
            vec![],
            child,
        );
        assert_eq!(tree.output_columns(), vec!["id", "tags", "name"]);
    }

    #[test]
    fn test_lateral_subquery_source_oids_includes_child_and_subquery() {
        let child = scan(42, "t", "public", "t", &["id"]);
        let tree = lateral_subquery(
            "SELECT val FROM other_table",
            "sub",
            vec![],
            vec!["val"],
            false,
            vec![99],
            child,
        );
        let oids = tree.source_oids();
        assert!(oids.contains(&42));
        assert!(oids.contains(&99));
    }

    #[test]
    fn test_lateral_subquery_alias() {
        let child = scan(1, "t", "public", "t", &["id"]);
        let tree = lateral_subquery(
            "SELECT val FROM items",
            "my_alias",
            vec![],
            vec!["val"],
            false,
            vec![],
            child,
        );
        assert_eq!(tree.alias(), "my_alias");
    }

    #[test]
    fn test_lateral_subquery_node_kind() {
        let child = scan(1, "t", "public", "t", &["id"]);
        let tree = lateral_subquery(
            "SELECT val FROM items",
            "sub",
            vec![],
            vec!["val"],
            false,
            vec![],
            child,
        );
        assert_eq!(tree.node_kind(), "lateral subquery");
    }

    #[test]
    fn test_lateral_subquery_is_left_join_flag() {
        let child = scan(1, "t", "public", "t", &["id"]);
        let tree_inner = lateral_subquery(
            "SELECT val FROM items",
            "sub",
            vec![],
            vec!["val"],
            false,
            vec![],
            child.clone(),
        );
        let tree_left = lateral_subquery(
            "SELECT val FROM items",
            "sub",
            vec![],
            vec!["val"],
            true,
            vec![],
            child,
        );
        assert!(matches!(
            tree_inner,
            OpTree::LateralSubquery {
                is_left_join: false,
                ..
            }
        ));
        assert!(matches!(
            tree_left,
            OpTree::LateralSubquery {
                is_left_join: true,
                ..
            }
        ));
    }

    // ── P2-6: Scoped inner-change re-execution tests ────────────────

    #[test]
    fn test_diff_lateral_subquery_scoped_inner_change_uses_correlation() {
        use crate::dvm::parser::CorrelationPredicate;

        let mut ctx = test_ctx_with_st("public", "my_st");
        let child = scan(1, "orders", "public", "o", &["id", "customer"]);
        let tree = lateral_subquery_with_corr(
            "SELECT amount FROM line_items li WHERE li.order_id = o.id ORDER BY created_at DESC LIMIT 1",
            "latest",
            vec![],
            vec!["amount"],
            false,
            vec![2],
            vec![CorrelationPredicate {
                outer_col: "id".to_string(),
                inner_alias: "li".to_string(),
                inner_col: "order_id".to_string(),
                inner_oid: 2,
            }],
            child,
        );
        let result = diff_lateral_subquery(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Scoped EXISTS should reference the flat correlation column (D+I schema, A44-10)
        assert_sql_contains(&sql, "\"order_id\"");
        assert!(
            !sql.contains("\"new_order_id\""),
            "must not use new_order_id (old schema)"
        );
        assert!(
            !sql.contains("\"old_order_id\""),
            "must not use old_order_id (old schema)"
        );
        // Should still have the full CTE chain
        assert_sql_contains(&sql, "lat_sq_changed");
        assert_sql_contains(&sql, "lat_sq_final");
    }

    #[test]
    fn test_diff_lateral_subquery_no_correlation_falls_back() {
        let mut ctx = test_ctx_with_st("public", "my_st");
        let child = scan(1, "orders", "public", "o", &["id", "customer"]);
        // No correlation predicates — should use the LIMIT 1 full-scan fallback
        let tree = lateral_subquery(
            "SELECT amount FROM line_items li WHERE li.order_id = o.id",
            "latest",
            vec![],
            vec!["amount"],
            false,
            vec![2],
            child,
        );
        let result = diff_lateral_subquery(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Without correlation predicates, uses the old-style LIMIT 1 EXISTS
        assert_sql_contains(&sql, "LIMIT 1");
    }

    #[test]
    fn test_diff_lateral_subquery_scoped_does_not_have_limit() {
        use crate::dvm::parser::CorrelationPredicate;

        let mut ctx = test_ctx_with_st("public", "my_st");
        let child = scan(1, "t", "public", "t", &["id"]);
        let tree = lateral_subquery_with_corr(
            "SELECT val FROM sub_t s WHERE s.fk = t.id",
            "sub",
            vec![],
            vec!["val"],
            false,
            vec![2],
            vec![CorrelationPredicate {
                outer_col: "id".to_string(),
                inner_alias: "s".to_string(),
                inner_col: "fk".to_string(),
                inner_oid: 2,
            }],
            child,
        );
        let result = diff_lateral_subquery(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Scoped EXISTS should NOT have LIMIT 1 — it uses correlation join
        // The only place LIMIT could appear is in the subquery SQL itself
        let parts: Vec<&str> = sql.splitn(2, "lat_sq_changed").collect();
        assert_eq!(
            parts.len(),
            2,
            "expected 'lat_sq_changed' marker in generated SQL\nSQL:\n{sql}"
        );
        let inner_branch = parts[1];
        // The inner-change EXISTS should use correlation, not LIMIT 1
        // D+I flat schema: flat column "fk" is used, not "new_fk"/"old_fk"
        assert!(
            inner_branch.contains("\"fk\""),
            "must reference flat column \"fk\""
        );
        assert!(
            !inner_branch.contains("\"new_fk\""),
            "must not use new_fk (old schema)"
        );
        assert!(
            !inner_branch.contains("\"old_fk\""),
            "must not use old_fk (old schema)"
        );
    }

    // ── SF-8: Inner-change dummy row_id sentinel ────────────────────

    #[test]
    fn test_inner_change_branch_uses_min_bigint_sentinel() {
        let mut ctx = test_ctx_with_st("public", "st");
        let child = scan(1, "orders", "public", "o", &["id"]);
        let tree = lateral_subquery(
            "SELECT amount FROM items i WHERE i.oid = o.id",
            "items",
            vec![],
            vec!["amount"],
            false,
            vec![2],
            child,
        );
        let result = diff_lateral_subquery(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // The inner-change branch should use i64::MIN+1 sentinel, not 0 or i64::MIN.
        // i64::MIN (-9223372036854775808) cannot be written as a PostgreSQL bigint
        // literal without overflow, so i64::MIN+1 (-9223372036854775807) is used.
        assert_sql_contains(&sql, "-9223372036854775807::BIGINT");
        assert!(
            !sql.contains("SELECT 0::BIGINT AS \"__pgt_row_id\""),
            "Should not use 0 as dummy row_id"
        );
        assert!(
            !sql.contains("-9223372036854775808::BIGINT"),
            "Should not use i64::MIN (out of range as a PG bigint literal)"
        );
    }

    /// A projected-away outer identity must fail closed instead of producing
    /// an `ON TRUE` deletion join.
    #[test]
    fn test_diff_lateral_subquery_projected_away_identity_is_rejected() {
        let mut ctx = test_ctx_with_st("public", "my_st");
        // Simulate a Project above having set st_user_columns to only the
        // projected columns (no "id" identity column).
        ctx.st_user_columns = Some(vec!["name".to_string(), "price".to_string()]);

        let child = scan(
            1,
            "products",
            "public",
            "products",
            &["id", "name", "price"],
        );
        let tree = lateral_subquery(
            "SELECT min_price FROM thresholds LIMIT 1",
            "__pgt_sq_1",
            vec!["__pgt_scalar_1"],
            vec!["min_price"],
            false,
            vec![2],
            child,
        );
        let err = diff_lateral_subquery(&mut ctx, &tree).unwrap_err();
        assert!(err.to_string().contains("projected away"));
    }

    #[test]
    fn test_diff_lateral_subquery_composite_nullable_identity_is_null_safe() {
        let mut ctx = test_ctx_with_st("public", "my_st");
        let child = scan_with_pk(
            1,
            "orders",
            "public",
            "o",
            &["tenant_id", "order_id", "value"],
            &["tenant_id", "order_id"],
        );
        let tree = lateral_subquery(
            "SELECT x FROM items i WHERE i.tenant_id = o.tenant_id",
            "sub",
            vec!["x"],
            vec!["x"],
            false,
            vec![2],
            child,
        );
        let result = diff_lateral_subquery(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);
        assert!(sql.matches("IS NOT DISTINCT FROM").count() >= 2);
        assert!(!sql.contains("JOIN st ON TRUE"));
        assert!(!sql.contains("WHERE TRUE"));
    }

    /// Regression: When the inner LATERAL subquery references the same
    /// table as the outer child (self-referencing LATERAL join), the
    /// inner change branch must NOT be skipped. Existing unchanged outer
    /// rows need recomputation because the inner subquery result depends
    /// on the same table that received new rows.
    #[test]
    fn test_diff_lateral_subquery_shared_oid_generates_inner_branch() {
        let mut ctx = test_ctx_with_st("public", "my_st");
        // Both child and inner subquery reference OID 1 (same table)
        let child = scan(1, "items", "public", "i", &["id", "category", "score"]);
        let tree = lateral_subquery(
            "SELECT src.id FROM items src WHERE src.category = i.category ORDER BY src.score DESC LIMIT 1",
            "__pgt_lat_1",
            vec!["top_id"],
            vec!["id"],
            true,
            vec![1], // same OID as child
            child,
        );
        let result = diff_lateral_subquery(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // The inner change branch should exist (check for change buffer
        // EXISTS on OID 1), not be skipped.
        assert_sql_contains(&sql, "changes_1");
        // The changed_sources CTE should have UNION ALL for the inner branch
        let parts: Vec<&str> = sql.splitn(2, "lat_sq_changed").collect();
        assert_eq!(
            parts.len(),
            2,
            "expected 'lat_sq_changed' marker in generated SQL\nSQL:\n{sql}"
        );
        let changed_cte = parts[1];
        assert!(
            changed_cte.contains("UNION ALL"),
            "Shared-OID inner branch should produce UNION ALL in changed_sources.\nSQL:\n{sql}"
        );
    }
}
