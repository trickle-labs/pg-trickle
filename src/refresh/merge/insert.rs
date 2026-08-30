// Sub-module of src/refresh/merge — see mod.rs for overview.
#[allow(unused_imports)]
use super::*;

pub fn execute_topk_refresh(st: &StreamTableMeta) -> Result<(i64, i64), PgTrickleError> {
    // G12-ERM-1: Record the effective mode for this execution path.
    set_effective_mode("TOP_K");
    crate::refresh::set_last_rows_updated(0);

    // EC-25/EC-26: Ensure the internal_refresh flag is set so DML guard
    // triggers allow the refresh executor to modify the storage table.
    Spi::run("SET LOCAL pg_trickle.internal_refresh = 'true'")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    let schema = &st.pgt_schema;
    let name = &st.pgt_name;

    let topk_limit = st.topk_limit.ok_or_else(|| {
        PgTrickleError::InternalError("execute_topk_refresh called on non-TopK stream table".into())
    })?;
    let topk_order_by = st.topk_order_by.as_deref().ok_or_else(|| {
        PgTrickleError::InternalError("TopK stream table missing order_by metadata".into())
    })?;

    // G12-2: TopK runtime validation — re-parse the reconstructed full query
    // and verify the detected TopK pattern matches stored catalog metadata.
    // On mismatch, fall back to FULL refresh to prevent silent correctness issues.
    if let Err(reason) = crate::refresh::with_stream_owner(st, || {
        validate_topk_metadata(
            &st.defining_query,
            topk_limit,
            topk_order_by,
            st.topk_offset,
        )
        .map_err(PgTrickleError::InvalidArgument)
    }) {
        pgrx::warning!(
            "pg_trickle: TopK metadata inconsistency for {}.{}: {}. \
             Falling back to FULL refresh.",
            schema,
            name,
            reason,
        );
        set_effective_mode("FULL");
        return execute_full_refresh(st);
    }

    let quoted_table = format!(
        "\"{}\".\"{}\"",
        schema.replace('"', "\"\""),
        name.replace('"', "\"\""),
    );
    let pre_table_basename = format!("__pgt_topk_state_{}", st.pgt_id);
    let pre_select = format!("SELECT * FROM {quoted_table}");
    let pre_table = crate::refresh::prepare_owner_temp_table(st, &pre_table_basename, &pre_select)?;
    crate::refresh::with_stream_owner(st, || {
        Spi::run(&format!("INSERT INTO {pre_table} {pre_select}")) // nosemgrep: rust.spi.run.dynamic-format — table is quoted and source is a quoted storage relation.
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))
    })?;

    let downstream_cols = if has_downstream_st_consumers(st.pgt_id) {
        let cols = get_st_user_columns(st);
        if cols.is_empty() {
            return Err(PgTrickleError::RefreshFinalizationFailed {
                pgt_id: st.pgt_id,
                stage: "topk downstream capture".to_string(),
                reason: "downstream consumers require at least one user column".to_string(),
            });
        }
        let col_list = cols
            .iter()
            .map(|col| format!("\"{}\"", col.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");
        let pre_select = format!("SELECT __pgt_row_id, {col_list} FROM {quoted_table}");
        // pre_select only references the ST's own fully-qualified storage
        // relation, and this table must stay readable by the privileged
        // downstream-diff capture call later, so this intentionally runs
        // un-wrapped (privileged).
        crate::refresh::prepare_owner_temp_table(
            st,
            &format!("__pgt_pre_{}", st.pgt_id),
            &pre_select,
        )
        .map_err(|e| PgTrickleError::RefreshFinalizationFailed {
            pgt_id: st.pgt_id,
            stage: "topk downstream snapshot".to_string(),
            reason: e.to_string(),
        })?;
        Some(cols)
    } else {
        None
    };

    // Reconstruct the full TopK query from base query + ORDER BY + LIMIT [+ OFFSET].
    let topk_query = if let Some(offset) = st.topk_offset {
        format!(
            "{} ORDER BY {} LIMIT {} OFFSET {}",
            st.defining_query, topk_order_by, topk_limit, offset
        )
    } else {
        format!(
            "{} ORDER BY {} LIMIT {}",
            st.defining_query, topk_order_by, topk_limit
        )
    };

    // Compute row_id using the same hash formula as normal refresh.
    let row_id_expr = crate::dvm::row_id_expr_for_query(&st.defining_query);

    // Build the source subquery with row IDs.
    // Use alias `sub` to match what row_id_expr_for_query() generates.
    let source_sql = format!("SELECT {row_id_expr} AS __pgt_row_id, sub.* FROM ({topk_query}) sub");

    // Get column names from the storage table (excluding __pgt_row_id).
    let columns = crate::refresh::with_stream_owner(st, || {
        crate::dvm::get_defining_query_columns(&st.defining_query)
    })?;

    // Build the MERGE statement.
    let col_list: Vec<String> = columns
        .iter()
        .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
        .collect();

    let update_set: Vec<String> = col_list
        .iter()
        .map(|c| format!("{c} = __pgt_topk_src.{c}"))
        .collect();

    let insert_cols: String = std::iter::once("__pgt_row_id".to_string())
        .chain(col_list.iter().cloned())
        .collect::<Vec<_>>()
        .join(", ");

    let insert_vals: String = std::iter::once("__pgt_topk_src.__pgt_row_id".to_string())
        .chain(col_list.iter().map(|c| format!("__pgt_topk_src.{c}")))
        .collect::<Vec<_>>()
        .join(", ");

    // Build an IS DISTINCT FROM check for change detection in WHEN MATCHED.
    let is_distinct_check = if col_list.is_empty() {
        "TRUE".to_string()
    } else {
        col_list
            .iter()
            .map(|c| format!("{quoted_table}.{c}::text IS DISTINCT FROM __pgt_topk_src.{c}::text"))
            .collect::<Vec<_>>()
            .join(" OR ")
    };

    let merge_sql = format!(
        "MERGE INTO {quoted_table} \
         USING ({source_sql}) AS __pgt_topk_src \
         ON pgtrickle.row_probe_v1({quoted_table}.__pgt_row_id) = pgtrickle.row_probe_v1(__pgt_topk_src.__pgt_row_id) \
            AND {quoted_table}.__pgt_row_id = __pgt_topk_src.__pgt_row_id \
         WHEN MATCHED AND ({is_distinct_check}) THEN \
           UPDATE SET {update_set} \
         WHEN NOT MATCHED THEN \
           INSERT ({insert_cols}) VALUES ({insert_vals}) \
         WHEN NOT MATCHED BY SOURCE THEN \
           DELETE",
        update_set = update_set.join(", "),
    );

    with_stream_owner(st, || {
        Spi::connect_mut(|client| {
            client
                .update(&merge_sql, None, &[])
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            Ok::<(), PgTrickleError>(())
        })
    })?;

    let changed_columns = col_list
        .iter()
        .map(|col| format!("t.{col} IS DISTINCT FROM p.{col}"))
        .collect::<Vec<_>>()
        .join(" OR ");
    let counts_sql = format!(
        "SELECT count(*) FILTER (WHERE p.__pgt_row_id IS NULL)::bigint, \
                count(*) FILTER (WHERE t.__pgt_row_id IS NULL)::bigint, \
                count(*) FILTER (WHERE t.__pgt_row_id IS NOT NULL \
                                  AND p.__pgt_row_id IS NOT NULL \
                                  AND ({changed_columns}))::bigint \
         FROM {quoted_table} t FULL JOIN {pre_table} p USING (__pgt_row_id)"
    );
    let (rows_inserted, rows_deleted, rows_updated) = with_stream_owner(st, || {
        Spi::connect(|client| {
            let mut rows = client
                .select(&counts_sql, None, &[])
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            let row = rows
                .next()
                .ok_or_else(|| PgTrickleError::RefreshFinalizationFailed {
                    pgt_id: st.pgt_id,
                    stage: "topk merge accounting".to_string(),
                    reason: "target change counts were not returned".to_string(),
                })?;
            Ok::<(i64, i64, i64), PgTrickleError>((
                row.get::<i64>(1)
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                    .unwrap_or(0),
                row.get::<i64>(2)
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                    .unwrap_or(0),
                row.get::<i64>(3)
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                    .unwrap_or(0),
            ))
        })
    })?;
    crate::refresh::set_last_rows_updated(rows_updated);

    if let Some(cols) = downstream_cols {
        crate::refresh::capture_full_refresh_diff_to_st_buffer(st, &cols).map_err(|e| {
            PgTrickleError::RefreshFinalizationFailed {
                pgt_id: st.pgt_id,
                stage: "topk downstream capture".to_string(),
                reason: e.to_string(),
            }
        })?;
    }

    pgrx::debug1!(
        "[pg_trickle] TopK refresh of {}.{}: MERGE processed {} rows",
        schema,
        name,
        rows_inserted,
    );

    Ok((rows_inserted, rows_deleted))
}
