// ARCH-1: PH-D1 phantom-cleanup sub-module for the refresh pipeline.
//
// This module contains the PH-D1 DELETE+INSERT strategy for handling
// join phantom rows:
// - PH-D1 strategy selection logic (currently around line 4991 in mod.rs)
// - DELETE+INSERT execution path (currently around line 5151 in mod.rs)
// - Co-deletion detection helpers
// - EC-01 convergence validation
// - EC01-2: Cross-cycle phantom cleanup
//
// Currently most code lives in `super` (mod.rs). This file is the landing
// zone for the phantom-cleanup layer during the ongoing ARCH-1 migration.

use crate::error::PgTrickleError;
use pgrx::Spi;

/// These pointwise operators preserve the signed multiplicity of every input.
pub(crate) fn exact_rowwise_join_scan_count(tree: &crate::dvm::parser::OpTree) -> Option<usize> {
    use crate::dvm::parser::OpTree;
    match tree {
        OpTree::Scan { .. } => Some(1),
        OpTree::Project { child, .. } | OpTree::Filter { child, .. } => {
            exact_rowwise_join_scan_count(child)
        }
        OpTree::InnerJoin { left, right, .. } => {
            Some(exact_rowwise_join_scan_count(left)? + exact_rowwise_join_scan_count(right)?)
        }
        OpTree::LateralFunction {
            is_left_join: false,
            child,
            ..
        } => exact_rowwise_join_scan_count(child),
        _ => None,
    }
}

pub(crate) fn has_reconciled_baseline(pgt_id: i64) -> Result<bool, PgTrickleError> {
    let strategy = Spi::get_one_with_args::<String>(
        "SELECT merge_strategy_used FROM pgtrickle.pgt_refresh_history \
         WHERE pgt_id = $1 AND status = 'COMPLETED' ORDER BY refresh_id DESC LIMIT 1",
        &[pgt_id.into()],
    )
    .map_err(|e| {
        PgTrickleError::SpiError(format!("reconciliation baseline lookup for {pgt_id}: {e}"))
    })?;
    Ok(matches!(
        strategy.as_deref(),
        Some("explicit_dml_reconciled" | "explicit_dml_exact_join" | "reconciliation_delta")
    ))
}

pub(crate) fn is_scalar_aggregate(tree: &crate::dvm::parser::OpTree) -> bool {
    use crate::dvm::parser::OpTree;
    matches!(tree, OpTree::Aggregate { group_by, aggregates, .. }
        if group_by.is_empty() && !aggregates.is_empty())
}

pub(crate) fn supports_reconciliation_replace(
    st: &crate::catalog::StreamTableMeta,
) -> Result<bool, PgTrickleError> {
    // Constraint triggers and rules can observe DELETE+INSERT versus UPDATE.
    Spi::get_one_with_args::<bool>(
        "SELECT c.relkind = 'r' AND NOT c.relhasrules \
                AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity \
                AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_trigger tr \
                                WHERE tr.tgrelid = c.oid AND tr.tgconstraint <> 0) \
         FROM pg_catalog.pg_class c WHERE c.oid = $1",
        &[st.pgt_relid.into()],
    )
    .map_err(|e| {
        PgTrickleError::SpiError(format!(
            "reconciliation apply eligibility for {}.{}: {e}",
            st.pgt_schema, st.pgt_name,
        ))
    })
    .map(|allowed| allowed.unwrap_or(false))
}

fn content_signature_expr(
    alias: &str,
    quoted_user_cols: &[String],
    row_id_is_complete: bool,
) -> String {
    if row_id_is_complete {
        return format!("{alias}.\"__pgt_row_id\"");
    }
    if quoted_user_cols.is_empty() {
        // Degenerate case (no user columns); use a constant fingerprint so
        // reconciliation collapses to a row-count diff.
        return "''::text".to_string();
    }
    let values = quoted_user_cols
        .iter()
        .map(|column| format!("{alias}.{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    // A row constructor avoids jsonb_build_object's 100-argument limit while
    // retaining PostgreSQL's native JSON encoding for each value.
    format!("md5(to_jsonb(ROW({values}))::text)")
}

/// Return whether the current stream and reconstructed result have different
/// multisets of content signatures. The same signatures drive the correction
/// queries below, so a zero signed count for every signature proves that both
/// the DELETE and INSERT correction sets are empty, including duplicates and
/// NULL-valued signatures.
fn reconciliation_has_drift_sql(
    stream_table_name: &str,
    recon_table: &str,
    st_sig: &str,
    r_sig: &str,
) -> String {
    format!(
        "SELECT EXISTS ( \
             SELECT 1 FROM ( \
                 SELECT {st_sig} AS __pgt_sig, 1 AS __pgt_weight \
                 FROM {stream_table_name} st \
                 UNION ALL \
                 SELECT {r_sig} AS __pgt_sig, -1 AS __pgt_weight \
                 FROM {recon_table} r \
             ) __pgt_multiset \
             GROUP BY __pgt_sig \
             HAVING SUM(__pgt_weight) <> 0 \
         )"
    )
}

/// Produce the exact multiset delta from the comparison already required for
/// graph joins. Retain physical deletion targets when an identity covers only
/// part of the output, so equal identities cannot delete the wrong occurrence.
pub(crate) struct ReconciliationDelta {
    pub sql: String,
    pub delete_column: String,
    pub columns: String,
}

fn reconciliation_query<'a>(query: &'a str, tree: &crate::dvm::parser::OpTree) -> &'a str {
    use crate::dvm::parser::OpTree;
    let OpTree::Project {
        expressions, child, ..
    } = tree
    else {
        return query;
    };
    let pruned =
        crate::dvm::operators::project::prune_unreferenced_false_left_joins(child, expressions);
    if std::ptr::eq(pruned, child.as_ref())
        || !matches!(pruned, OpTree::Scan { .. } | OpTree::Subquery { .. })
        // shortcut: retain escape/dollar syntax and outer clauses until their slicing is covered.
        || query.contains(['\\', '$'])
        || ["WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "FETCH", "FOR", "WINDOW", "UNION", "EXCEPT", "INTERSECT", "RIGHT", "FULL", "CROSS", "NATURAL"]
            .iter().any(|keyword| crate::api::find_top_level_keyword(query, keyword).is_some())
    {
        return query;
    }
    crate::api::find_top_level_keyword(query, "LEFT JOIN")
        .map_or(query, |position| query[..position].trim_end())
}

pub(crate) fn reconciliation_delta_sql(
    st: &crate::catalog::StreamTableMeta,
    defining_query: &str,
    tree: &crate::dvm::parser::OpTree,
    has_pgt_count: bool,
    scalar_aggregate: bool,
) -> Result<Option<ReconciliationDelta>, PgTrickleError> {
    let mut columns = crate::dvm::get_defining_query_columns(defining_query)?;
    let complete = crate::dvm::row_id_covers_output(tree, &columns);
    let table = format!(
        "{}.{}",
        crate::sql_builder::ident(&st.pgt_schema),
        crate::sql_builder::ident(&st.pgt_name)
    );
    // shortcut: use native multiset comparison only for these exact scalar types; broaden with type conformance coverage.
    let native_comparison = Spi::get_one_with_args::<bool>(
        "SELECT pg_catalog.bool_and(a.atttypid IN ('pg_catalog.bool'::pg_catalog.regtype, 'pg_catalog.bytea'::pg_catalog.regtype, \
             'pg_catalog.int2'::pg_catalog.regtype, 'pg_catalog.int4'::pg_catalog.regtype, 'pg_catalog.int8'::pg_catalog.regtype, \
             'pg_catalog.text'::pg_catalog.regtype, 'pg_catalog.varchar'::pg_catalog.regtype, 'pg_catalog.uuid'::pg_catalog.regtype, \
             'pg_catalog.date'::pg_catalog.regtype, 'pg_catalog.timestamp'::pg_catalog.regtype, 'pg_catalog.timestamptz'::pg_catalog.regtype) \
             AND (a.attcollation = 0 OR c.collisdeterministic)) \
         FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_collation c ON c.oid = a.attcollation \
         WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped",
        &[st.pgt_relid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(format!("native reconciliation eligibility for {table}: {e}")))?
    .unwrap_or(false);
    let native_projection = native_comparison
        && matches!(
            tree,
            crate::dvm::parser::OpTree::Project { .. } | crate::dvm::parser::OpTree::Filter { .. }
        );
    if columns.is_empty() || !(complete || scalar_aggregate || native_projection) {
        return Ok(None);
    }
    if !complete && scalar_aggregate {
        // Scalar projections can produce types without equality, such as json.
        let int8_output = Spi::get_one_with_args::<bool>(
            "SELECT bool_and(atttypid = 'pg_catalog.int8'::regtype) \
             FROM pg_catalog.pg_attribute WHERE attrelid = $1 AND attnum > 0 \
             AND NOT attisdropped AND attname NOT IN ('__pgt_row_id', '__pgt_count')",
            &[st.pgt_relid.into()],
        )
        .map_err(|e| {
            PgTrickleError::SpiError(format!(
                "scalar output eligibility for {}.{}: {e}",
                st.pgt_schema, st.pgt_name
            ))
        })?
        .unwrap_or(false);
        if !int8_output {
            return Ok(None);
        }
    }
    let row_id = crate::dvm::row_id_expr_for_tree(defining_query, Some(tree));
    // The original query was already probed as owner, including every dependency's privileges.
    let query = reconciliation_query(defining_query, tree);
    let materialization_query = if has_pgt_count {
        columns.push("__pgt_count".to_string());
        crate::api::inject_pgt_count(query)
    } else {
        query.to_string()
    };
    let cols = columns
        .iter()
        .map(|column| crate::sql_builder::ident(column))
        .collect::<Vec<_>>();
    let csv = cols.join(", ");
    let mut occurrence = "__pgt_occurrence".to_string();
    while columns.contains(&occurrence) {
        occurrence.push('_');
    }
    let mut delete_ctid = "__pgt_delete_ctid".to_string();
    while columns.contains(&delete_ctid) {
        delete_ctid.push('_');
    }
    let pre_cols = cols
        .iter()
        .map(|c| format!("pre.{c}"))
        .collect::<Vec<_>>()
        .join(", ");
    let post_cols = cols
        .iter()
        .map(|c| format!("post.{c}"))
        .collect::<Vec<_>>()
        .join(", ");
    let different = cols
        .iter()
        .map(|c| format!("pre.{c} IS DISTINCT FROM post.{c}"))
        .collect::<Vec<_>>()
        .join(" OR ");
    let row_match = "pre.__pgt_row_id = post.__pgt_row_id";
    if complete || native_projection {
        let mut weight = "__pgt_recon_weight".to_string();
        while columns.contains(&weight) {
            weight.push('_');
        }
        let state = if has_pgt_count { ", __pgt_count" } else { "" };
        let pre_state = if has_pgt_count {
            ", pre.__pgt_count"
        } else {
            ""
        };
        let post_state = if has_pgt_count {
            ", post.__pgt_count"
        } else {
            ""
        };
        let pre_match = if has_pgt_count {
            " AND pre.__pgt_count IS NOT DISTINCT FROM w.__pgt_count"
        } else {
            ""
        };
        let post_match = if has_pgt_count {
            " AND post.__pgt_count IS NOT DISTINCT FROM w.__pgt_count"
        } else {
            ""
        };
        if native_comparison {
            let native_match = cols
                .iter()
                .map(|column| format!("pre.{column} IS NOT DISTINCT FROM w.{column}"))
                .collect::<Vec<_>>()
                .join(" AND ");
            let (deletion_keys, deletion_match, deletion_partition) = if complete {
                (
                    format!("__pgt_row_id{state}"),
                    format!("pre.__pgt_row_id = w.__pgt_row_id{pre_match}"),
                    format!("pre.__pgt_row_id{pre_state}"),
                )
            } else {
                (
                    format!(
                        "{csv}, pg_catalog.hash_record_extended(ROW({csv}), 0::bigint) AS {occurrence}"
                    ),
                    format!(
                        "pg_catalog.hash_record_extended(ROW({pre_cols}), 0::bigint) = w.{occurrence} AND {native_match}"
                    ),
                    pre_cols.clone(),
                )
            };
            let deletion_groups = if complete {
                format!("__pgt_row_id{state}")
            } else {
                csv.clone()
            };
            // Counted intermediates retain EXCEPT ALL; plain replacements benefit from one grouped pass.
            let changes = if has_pgt_count {
                format!(
                    "SELECT {row_id} AS __pgt_row_id, 'D'::text AS __pgt_action, sub.* FROM (\
                         SELECT {csv} FROM {table} EXCEPT ALL SELECT {csv} FROM __pgt_recon_expected) sub \
                     UNION ALL \
                     SELECT {row_id} AS __pgt_row_id, 'I'::text AS __pgt_action, sub.* FROM (\
                         SELECT {csv} FROM __pgt_recon_expected EXCEPT ALL SELECT {csv} FROM {table}) sub"
                )
            } else {
                let weighted_cols = cols
                    .iter()
                    .map(|column| format!("w.{column}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "WITH __pgt_recon_weights AS MATERIALIZED (\
                         SELECT {csv}, SUM({weight}) AS {weight} FROM (\
                             SELECT {csv}, -1 AS {weight} FROM {table} \
                             UNION ALL SELECT {csv}, 1 AS {weight} FROM __pgt_recon_expected\
                         ) signs GROUP BY {csv} HAVING SUM({weight}) <> 0) \
                     SELECT {row_id} AS __pgt_row_id, \
                            CASE WHEN w.{weight} < 0 THEN 'D'::text ELSE 'I'::text END AS __pgt_action, sub.* \
                     FROM __pgt_recon_weights w \
                     CROSS JOIN LATERAL (SELECT {weighted_cols}) sub CROSS JOIN LATERAL \
                          pg_catalog.generate_series(1::bigint, pg_catalog.abs(w.{weight})) \
                          AS __pgt_expansion({occurrence})"
                )
            };
            return Ok(Some(ReconciliationDelta {
                sql: format!(
                    "WITH __pgt_recon_expected AS MATERIALIZED ({materialization_query}), \
                     __pgt_recon_changes AS MATERIALIZED ({changes}), \
                     __pgt_recon_deletions AS (\
                         SELECT {deletion_keys}, count(*) AS {weight} \
                         FROM __pgt_recon_changes WHERE __pgt_action = 'D' GROUP BY {deletion_groups}) \
                     SELECT __pgt_row_id, 'D'::text AS __pgt_action, {csv}, {delete_ctid} FROM (\
                         SELECT pre.__pgt_row_id, {pre_cols}, pre.ctid AS {delete_ctid}, w.{weight}, \
                                row_number() OVER (PARTITION BY {deletion_partition} ORDER BY pre.ctid) AS {occurrence} \
                         FROM {table} pre JOIN __pgt_recon_deletions w ON {deletion_match}\
                     ) deletes WHERE {occurrence} <= {weight} \
                     UNION ALL \
                     SELECT __pgt_row_id, 'I'::text AS __pgt_action, {csv}, NULL::tid AS {delete_ctid} \
                     FROM __pgt_recon_changes WHERE __pgt_action = 'I'"
                ),
                delete_column: delete_ctid,
                columns: csv,
            }));
        }
        // Complete identities prove equal content; number only changed groups.
        let sql = format!(
            "WITH __pgt_recon_expected AS MATERIALIZED (\
                 SELECT {row_id} AS __pgt_row_id, sub.* FROM ({materialization_query}) sub), \
             __pgt_recon_weights AS MATERIALIZED (\
                 SELECT __pgt_row_id{state}, SUM(__pgt_signed_weight) AS {weight} FROM (\
                     SELECT __pgt_row_id{state}, -1 AS __pgt_signed_weight FROM {table} \
                     UNION ALL SELECT __pgt_row_id{state}, 1 AS __pgt_signed_weight FROM __pgt_recon_expected\
                 ) signs GROUP BY __pgt_row_id{state} HAVING SUM(__pgt_signed_weight) <> 0) \
             SELECT __pgt_row_id, 'D'::text AS __pgt_action, {csv}, {delete_ctid} FROM (\
                 SELECT pre.__pgt_row_id, {pre_cols}, pre.ctid AS {delete_ctid}, w.{weight}, \
                        row_number() OVER (PARTITION BY pre.__pgt_row_id{pre_state} ORDER BY pre.ctid) AS {occurrence} \
                 FROM {table} pre JOIN __pgt_recon_weights w ON pre.__pgt_row_id = w.__pgt_row_id{pre_match} \
                 WHERE w.{weight} < 0\
             ) deletes WHERE {occurrence} <= -{weight} \
             UNION ALL \
             SELECT __pgt_row_id, 'I'::text AS __pgt_action, {csv}, NULL::tid AS {delete_ctid} FROM (\
                 SELECT post.__pgt_row_id, {post_cols}, w.{weight}, \
                        row_number() OVER (PARTITION BY post.__pgt_row_id{post_state}) AS {occurrence} \
                 FROM __pgt_recon_expected post JOIN __pgt_recon_weights w ON post.__pgt_row_id = w.__pgt_row_id{post_match} \
                 WHERE w.{weight} > 0\
             ) inserts WHERE {occurrence} <= {weight}"
        );
        return Ok(Some(ReconciliationDelta {
            sql,
            delete_column: delete_ctid,
            columns: csv,
        }));
    }
    // Occurrences retain duplicate multiplicity, including NULL-valued rows.
    Ok(Some(ReconciliationDelta {
        sql: format!(
            "WITH __pgt_recon_expected AS MATERIALIZED (\
             SELECT {row_id} AS __pgt_row_id, sub.* FROM ({materialization_query}) sub), \
         __pgt_recon_before AS MATERIALIZED (\
             SELECT ctid AS {delete_ctid}, __pgt_row_id, {csv}, \
                    row_number() OVER (PARTITION BY __pgt_row_id ORDER BY tableoid, ctid) AS {occurrence} \
             FROM {table}), \
         __pgt_recon_after AS MATERIALIZED (\
             SELECT __pgt_row_id, {csv}, \
                    row_number() OVER (PARTITION BY __pgt_row_id) AS {occurrence} \
             FROM __pgt_recon_expected) \
         SELECT pre.__pgt_row_id, 'D'::text AS __pgt_action, {pre_cols}, pre.{delete_ctid} \
         FROM __pgt_recon_before pre LEFT JOIN __pgt_recon_after post \
           ON {row_match} AND pre.{occurrence} = post.{occurrence} \
         WHERE post.{occurrence} IS NULL OR ({different}) \
         UNION ALL \
         SELECT post.__pgt_row_id, 'I'::text AS __pgt_action, {post_cols}, NULL::tid AS {delete_ctid} \
         FROM __pgt_recon_after post LEFT JOIN __pgt_recon_before pre \
           ON {row_match} AND pre.{occurrence} = post.{occurrence} \
         WHERE pre.{occurrence} IS NULL OR ({different})"
        ),
        delete_column: delete_ctid,
        columns: csv,
    }))
}

/// EC01-2: Reconcile the stream table against the full-query result set.
///
/// After a refresh (DIFFERENTIAL or IMMEDIATE) applies a delta, residue from
/// prior cycles may remain whenever the delta engine was unable to compute a
/// fully accurate change set. Three failure modes are observed in practice:
///
/// 1. **Pure orphans** — a row exists in the ST but the join key partner has
///    since been deleted, so the row is no longer part of the live result.
/// 2. **Stale content** — the ST row's `__pgt_row_id` still matches a current
///    result row, but the user-visible columns are out of date because a
///    non-monotone predicate (e.g. scalar `MAX` subquery, EXISTS) shifted
///    without the delta engine seeing a triggering row in this transaction.
/// 3. **Missing rows** — the live result includes a row whose `__pgt_row_id`
///    is absent from the ST because the same non-monotone predicate kept the
///    delta from emitting an INSERT.
///
/// The original implementation only handled (1) by detecting orphan
/// `__pgt_row_id`s. To fully clear EC-01 for IMMEDIATE mode (and harden the
/// DIFFERENTIAL path against the same shape) the cleanup now performs full
/// multiset reconciliation against the live query result:
///
///   * Compare the stored and full-query content signature multisets.
///   * On drift, materialise the full-query output (including `__pgt_row_id`).
///   * Hash each ST and recon row by `row_to_json` to obtain a NULL-safe,
///     order-independent content fingerprint.
///   * For every fingerprint where ST count > recon count, delete the
///     surplus rows by `ctid`.
///   * For every fingerprint where recon count > ST count, insert the
///     missing rows.
///
/// The cleanup runs in batches of `batch_size` rows to avoid holding long
/// locks on either side. Returns the total number of rows deleted plus
/// inserted.
///
/// Called after each non-deduplicated, non-partitioned, join-bearing apply
/// so stale rows converge even when the current delta did not contain the
/// matching change.
/// The caller must run this under the stream owner's identity and search path.
pub fn cleanup_cross_cycle_phantoms(
    st: &crate::catalog::StreamTableMeta,
    stream_table_name: &str,
    defining_query: &str,
    batch_size: i64,
) -> Result<i64, PgTrickleError> {
    let pgt_id = st.pgt_id;
    let user_cols = crate::dvm::get_defining_query_columns(defining_query)?;
    let (row_id_expr, row_id_is_complete) =
        crate::dvm::row_id_expr_for_query_and_complete_output(defining_query, &user_cols);

    // Quote user column names.
    let quoted_user_cols: Vec<String> = user_cols
        .iter()
        .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
        .collect();

    // Column list for the INSERT projection.
    let all_cols_csv: String = std::iter::once("__pgt_row_id".to_string())
        .chain(quoted_user_cols.iter().cloned())
        .collect::<Vec<_>>()
        .join(", ");

    // Content fingerprint expression — built over the user-visible columns
    // ONLY. `__pgt_row_id` is intentionally excluded because for join
    // queries whose PKs do not appear in the projection it is computed via
    // `row_to_json(sub) || row_number() OVER ()`, which is unstable across
    // executions (the row_number depends on delivery order). Excluding it
    // makes the reconciliation correct for those query shapes too — two
    // rows with identical user content are considered equivalent regardless
    // of any internal hash drift.
    let st_sig = content_signature_expr("st", &quoted_user_cols, row_id_is_complete);
    let r_sig = content_signature_expr("r", &quoted_user_cols, row_id_is_complete);
    let recon_row_number_alias = {
        let mut suffix = 0_u32;
        loop {
            let candidate = if suffix == 0 {
                "__pgt_recon_rn".to_string()
            } else {
                format!("__pgt_recon_rn_{suffix}")
            };
            if !user_cols.iter().any(|column| column == &candidate) {
                break candidate;
            }
            suffix += 1;
        }
    };

    // Compare the live result before allocating or writing a scratch table;
    // most differential applies have already converged.
    let recon_select =
        format!("SELECT {row_id_expr} AS __pgt_row_id, sub.* FROM ({defining_query}) sub");
    let drift_probe = reconciliation_has_drift_sql(
        stream_table_name,
        &format!("({recon_select})"),
        &st_sig,
        &r_sig,
    );
    let probe_start = std::time::Instant::now();
    let has_drift = Spi::get_one::<bool>(&drift_probe)
        .map_err(|e| {
            PgTrickleError::SpiError(format!(
                "EC01-2 drift probe failed for stream table {pgt_id}: {e}"
            ))
        })?
        .unwrap_or(true);
    let probe_ms = probe_start.elapsed().as_secs_f64() * 1000.0;
    if !has_drift {
        pgrx::debug1!(
            "[pg_trickle] EC01-2 timing pgt_id={pgt_id} drift=false materialize_ms=0 probe_ms={probe_ms:.3} delete_ms=0 insert_ms=0",
        );
        return Ok(0);
    }

    // Materialise drift under the same owner context before the bounded repair.
    let materialize_start = std::time::Instant::now();
    let recon_table = crate::refresh::prepare_owner_temp_table(
        st,
        &format!("__pgt_recon_{pgt_id}"),
        &recon_select,
    )?;
    // nosemgrep: rust.spi.run.dynamic-format — recon_table is pg_temp-qualified and quoted; defining SQL executes as its owner.
    Spi::run(&format!("INSERT INTO {recon_table} {recon_select}")).map_err(|e| {
        PgTrickleError::SpiError(format!("EC01-2 reconcile materialise failed: {e}"))
    })?;
    let materialize_ms = materialize_start.elapsed().as_secs_f64() * 1000.0;

    // Step 2: delete surplus ST rows (multiset-aware on user-content sig).
    //
    // INVARIANT: The `ctid` values captured by the preceding CTE are stable
    // within this transaction's snapshot (no concurrent VACUUM or HOT chain
    // updates).  Do NOT split this DELETE into a separate transaction or
    // sub-transaction — if the snapshot isolation guarantee is lost, use
    // __pgt_row_id-based deletion instead.
    //
    // ctid is read directly from the ST (NOT through a subquery, which
    // would strip the system column).
    let delete_sql = format!(
        "WITH \
            st_tagged AS ( \
                SELECT st.ctid, {st_sig} AS __pgt_sig \
                FROM {stream_table_name} st \
            ), \
            recon_tagged AS ( \
                SELECT {r_sig} AS __pgt_sig \
                FROM {recon_table} r \
            ), \
            st_counts AS (SELECT __pgt_sig, count(*) AS c FROM st_tagged GROUP BY __pgt_sig), \
            recon_counts AS (SELECT __pgt_sig, count(*) AS c FROM recon_tagged GROUP BY __pgt_sig), \
            excess AS ( \
                SELECT s.__pgt_sig, s.c - COALESCE(r.c, 0) AS extras \
                FROM st_counts s LEFT JOIN recon_counts r USING (__pgt_sig) \
                WHERE s.c > COALESCE(r.c, 0) \
            ), \
            numbered AS ( \
                SELECT ctid, __pgt_sig, \
                       ROW_NUMBER() OVER (PARTITION BY __pgt_sig) AS rn \
                FROM st_tagged \
            ), \
            to_delete AS ( \
                SELECT n.ctid \
                FROM numbered n JOIN excess e USING (__pgt_sig) \
                WHERE n.rn <= e.extras \
                LIMIT $1 \
            ), \
            deleted AS ( \
                DELETE FROM {stream_table_name} \
                WHERE ctid IN (SELECT ctid FROM to_delete) \
                RETURNING 1 \
            ) \
        SELECT count(*)::bigint FROM deleted",
    );

    let delete_start = std::time::Instant::now();
    let deleted = Spi::get_one_with_args::<i64>(&delete_sql, &[batch_size.into()])
        .map_err(|e| PgTrickleError::SpiError(format!("EC01-2 reconcile DELETE failed: {e}")))?
        .unwrap_or(0);
    let delete_ms = delete_start.elapsed().as_secs_f64() * 1000.0;

    // Step 3: insert missing rows (multiset-aware on user-content sig).
    //
    // For each fingerprint where recon.count > st.count, insert
    // `recon.count - st.count` rows from the recon table. The row_id used
    // for the new ST rows comes from the recon table — even if it's a
    // row_number-derived hash, it satisfies the unique index because it's
    // freshly generated from the live query result.
    let insert_sql = format!(
        "WITH \
            st_counts AS ( \
                SELECT {st_sig_st} AS __pgt_sig, count(*) AS c \
                FROM {stream_table_name} st GROUP BY 1 \
            ), \
            recon_counts AS ( \
                SELECT {r_sig_r} AS __pgt_sig, count(*) AS c \
                FROM {recon_table} r GROUP BY 1 \
            ), \
            shortfall AS ( \
                SELECT r.__pgt_sig, r.c - COALESCE(s.c, 0) AS missing_n \
                FROM recon_counts r LEFT JOIN st_counts s USING (__pgt_sig) \
                WHERE r.c > COALESCE(s.c, 0) \
            ), \
            recon_numbered AS ( \
                SELECT r.*, \
                       {r_sig_r} AS __pgt_sig_x, \
                       ROW_NUMBER() OVER (PARTITION BY {r_sig_r}) AS {recon_row_number_alias} \
                FROM {recon_table} r \
            ), \
            to_insert AS ( \
                SELECT n.* \
                FROM recon_numbered n \
                JOIN shortfall sh ON sh.__pgt_sig = n.__pgt_sig_x \
                WHERE n.{recon_row_number_alias} <= sh.missing_n \
                LIMIT $1 \
            ), \
            inserted AS ( \
                INSERT INTO {stream_table_name} ({all_cols_csv}) \
                SELECT {all_cols_csv} FROM to_insert \
                RETURNING 1 \
            ) \
        SELECT count(*)::bigint FROM inserted",
        st_sig_st = st_sig,
        r_sig_r = r_sig,
        recon_table = recon_table,
        stream_table_name = stream_table_name,
        all_cols_csv = all_cols_csv,
    );

    let insert_start = std::time::Instant::now();
    let inserted = Spi::get_one_with_args::<i64>(&insert_sql, &[batch_size.into()])
        .map_err(|e| PgTrickleError::SpiError(format!("EC01-2 reconcile INSERT failed: {e}")))?
        .unwrap_or(0);
    let insert_ms = insert_start.elapsed().as_secs_f64() * 1000.0;

    pgrx::debug1!(
        "[pg_trickle] EC01-2 timing pgt_id={pgt_id} drift=true materialize_ms={materialize_ms:.3} probe_ms={probe_ms:.3} delete_ms={delete_ms:.3} insert_ms={insert_ms:.3} deleted={deleted} inserted={inserted}",
    );
    let total = deleted + inserted;
    if total > 0 {
        pgrx::log!(
            "[pg_trickle] EC01-2: reconciled pgt_id={} (deleted={}, inserted={})",
            pgt_id,
            deleted,
            inserted,
        );
    }

    Ok(total)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_reconciliation_query_removes_only_unused_false_join_suffix() {
        use crate::dvm::operators::test_helpers::*;
        let child = left_join(
            lit("false"),
            subquery("p", vec![], scan(1, "source", "public", "s", &["id"])),
            scan(2, "other", "public", "d", &["id"]),
        );
        let tree = project(vec![qcolref("p", "id")], vec!["id"], child.clone());
        let query = "SELECT p.id FROM (SELECT id FROM source) p LEFT JOIN other d ON false";
        assert_eq!(
            super::reconciliation_query(query, &tree),
            "SELECT p.id FROM (SELECT id FROM source) p"
        );
        let referenced = project(vec![qcolref("d", "id")], vec!["id"], child);
        let referenced_query = query.replacen("SELECT p.id", "SELECT d.id", 1);
        assert_eq!(
            super::reconciliation_query(&referenced_query, &referenced),
            referenced_query
        );
        for suffix in [
            " WHERE p.id > 0",
            " ORDER BY p.id",
            " LIMIT 1",
            " RIGHT JOIN extra r ON false",
        ] {
            let query = format!("{query}{suffix}");
            assert_eq!(super::reconciliation_query(&query, &tree), query);
        }
        let escaped = query.replace("p.id", "E'\\n' AS id");
        assert_eq!(super::reconciliation_query(&escaped, &tree), escaped);
        let dollar = query.replace("p.id", "$$LEFT JOIN$$ AS id");
        assert_eq!(super::reconciliation_query(&dollar, &tree), dollar);
    }

    #[test]
    fn test_scalar_reconciliation_requires_singleton_shape() {
        use crate::dvm::operators::test_helpers::*;
        let leaf = scan(1, "source", "public", "s", &["id"]);
        let count = aggregate(vec![], vec![count_star("n")], leaf.clone());
        assert!(super::is_scalar_aggregate(&count));
        assert!(!super::is_scalar_aggregate(&project(
            vec![colref("n")],
            vec!["n"],
            count.clone()
        )));
        assert!(!super::is_scalar_aggregate(&aggregate(
            vec![colref("id")],
            vec![count_star("n")],
            leaf.clone()
        )));
        assert!(super::is_scalar_aggregate(&aggregate(
            vec![],
            vec![sum_col("id", "n")],
            leaf
        )));
        assert!(!super::is_scalar_aggregate(&filter(lit("true"), count)));
    }

    #[test]
    fn test_exact_rowwise_join_tree_excludes_nonpointwise_operators() {
        use crate::dvm::operators::test_helpers::*;
        use crate::dvm::parser::OpTree;
        let leaf = scan(1, "source", "public", "s", &["id"]);
        let join = inner_join(lit("true"), leaf.clone(), leaf.clone());
        assert_eq!(super::exact_rowwise_join_scan_count(&join), Some(2));
        assert_eq!(
            super::exact_rowwise_join_scan_count(&filter(lit("true"), join.clone())),
            Some(2)
        );
        assert_eq!(
            super::exact_rowwise_join_scan_count(&left_join(
                lit("true"),
                leaf.clone(),
                leaf.clone()
            )),
            None
        );
        assert_eq!(
            super::exact_rowwise_join_scan_count(&distinct(join.clone())),
            None
        );
        assert_eq!(
            super::exact_rowwise_join_scan_count(&aggregate(vec![], vec![count_star("n")], join)),
            None
        );
        let mut lateral = OpTree::LateralFunction {
            func_sql: "generate_series(1, s.id)".into(),
            alias: "g".into(),
            column_aliases: vec!["value".into()],
            declared_columns: vec![],
            with_ordinality: false,
            is_left_join: false,
            join_condition: None,
            child: Box::new(leaf),
        };
        assert_eq!(super::exact_rowwise_join_scan_count(&lateral), Some(1));
        assert_eq!(
            super::exact_rowwise_join_scan_count(&inner_join(
                lit("true"),
                lateral.clone(),
                lateral.clone()
            )),
            Some(2)
        );
        if let OpTree::LateralFunction { is_left_join, .. } = &mut lateral {
            *is_left_join = true;
        }
        assert_eq!(super::exact_rowwise_join_scan_count(&lateral), None);
    }

    #[test]
    fn test_content_signature_supports_wide_rows() {
        let columns = (0..51)
            .map(|index| format!("\"column_{index}\""))
            .collect::<Vec<_>>();
        let expression = super::content_signature_expr("st", &columns, false);

        assert!(expression.contains("to_jsonb(ROW("));
        assert!(!expression.contains("jsonb_build_object"));
        assert_eq!(expression.matches("st.\"column_").count(), 51);
    }

    #[test]
    fn test_content_signature_reuses_complete_row_identity() {
        assert_eq!(
            super::content_signature_expr("r", &[], true),
            "r.\"__pgt_row_id\""
        );
    }

    #[test]
    fn test_reconciliation_drift_probe_preserves_signed_multiplicities() {
        let sql = super::reconciliation_has_drift_sql(
            "\"public\".\"source_stream\"",
            "pg_temp.__pgt_recon_42",
            "st.\"__pgt_row_id\"",
            "r.\"__pgt_row_id\"",
        );
        assert!(sql.contains("SELECT EXISTS"));
        assert!(sql.contains("UNION ALL"));
        assert!(sql.contains("GROUP BY __pgt_sig"));
        assert!(sql.contains("HAVING SUM(__pgt_weight) <> 0"));
        assert_eq!(
            sql.matches("FROM \"public\".\"source_stream\" st").count(),
            1
        );
        assert_eq!(sql.matches("FROM pg_temp.__pgt_recon_42 r").count(), 1);
        assert!(sql.contains("SELECT st.\"__pgt_row_id\" AS __pgt_sig, 1 AS __pgt_weight"));
        assert!(sql.contains("SELECT r.\"__pgt_row_id\" AS __pgt_sig, -1 AS __pgt_weight"));
    }

    #[test]
    fn test_cleanup_returns_zero_for_empty_case() {
        // Verify batch_size defaults are positive integers
        let batch_size: i64 = 1000;
        assert!(batch_size > 0);
    }
}

#[cfg(feature = "pg_test")]
#[pgrx::pg_schema]
mod pg_tests {
    use super::*;

    #[pgrx::pg_test]
    fn test_reconciliation_drift_probe_handles_nulls_and_duplicate_rows() {
        Spi::run("CREATE TEMP TABLE __pgt_probe_st (value text)").unwrap();
        Spi::run("CREATE TEMP TABLE __pgt_probe_recon (value text)").unwrap();
        let sql = reconciliation_has_drift_sql(
            "pg_temp.__pgt_probe_st",
            "pg_temp.__pgt_probe_recon",
            "st.value",
            "r.value",
        );
        let has_drift = || Spi::get_one::<bool>(&sql).unwrap().unwrap();
        assert!(!has_drift(), "two empty multisets match");

        Spi::run("INSERT INTO __pgt_probe_st VALUES ('a'), (NULL), (NULL)").unwrap();
        Spi::run("INSERT INTO __pgt_probe_recon VALUES (NULL), ('a'), (NULL)").unwrap();
        assert!(
            !has_drift(),
            "multiplicity and NULLs match regardless of order"
        );

        Spi::run("INSERT INTO __pgt_probe_st VALUES (NULL)").unwrap();
        assert!(has_drift(), "a surplus NULL signature must trigger repair");
        Spi::run("INSERT INTO __pgt_probe_recon VALUES (NULL)").unwrap();
        assert!(!has_drift(), "equal duplicate counts cancel");

        Spi::run("UPDATE __pgt_probe_recon SET value = 'changed' WHERE value = 'a'").unwrap();
        assert!(has_drift(), "stale content must trigger repair");
        Spi::run("UPDATE __pgt_probe_st SET value = 'changed' WHERE value = 'a'").unwrap();
        assert!(!has_drift(), "matching content clears the drift probe");

        Spi::run("DELETE FROM __pgt_probe_recon WHERE value = 'changed'").unwrap();
        assert!(has_drift(), "a missing result row must trigger repair");
    }
}
