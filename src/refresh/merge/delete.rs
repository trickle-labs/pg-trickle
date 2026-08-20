// Sub-module of src/refresh/merge — see mod.rs for overview.
#[allow(unused_imports)]
use super::*;

pub(crate) fn execute_incremental_truncate_delete(
    st: &StreamTableMeta,
) -> Result<(i64, i64), PgTrickleError> {
    // Suppress the CDC trigger on the ST itself during the operation.
    Spi::run("SET LOCAL pg_trickle.internal_refresh = 'true'")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    let schema = &st.pgt_schema;
    let name = &st.pgt_name;
    let quoted_table = format!(
        "\"{}\".\"{}\"",
        schema.replace('"', "\"\""),
        name.replace('"', "\"\"")
    );

    let rows_deleted = Spi::connect_mut(|client| {
        // nosemgrep: rust.spi.connect_mut.dynamic-format — quoted_table is built from double-quote-escaped relation identifiers.
        let result = client
            .update(&format!("DELETE FROM {quoted_table}"), None, &[])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        Ok::<i64, PgTrickleError>(result.len() as i64)
    })?;

    pgrx::notice!(
        "[pg_trickle] Incremental TRUNCATE: deleted {} row(s) from {}.{} \
         (pure TRUNCATE window — skipping full query re-execution)",
        rows_deleted,
        schema,
        name,
    );
    Ok((0, rows_deleted))
}

// ── A1-2: Partition key range extraction ────────────────────────────────────

/// Safely quote a SQL string literal (standard SQL single-quote escaping).
/// Used to embed partition key range values in MERGE ON-clause predicates.
pub(crate) fn execute_hash_partitioned_merge(
    merge_sql: &str,
    _resolved_delta_sql: &str,
    schema: &str,
    name: &str,
    _parent_oid: pg_sys::Oid,
    _partition_key: &str,
    _pgt_id: i64,
) -> Result<usize, PgTrickleError> {
    // Strip the __PGT_PART_PRED__ placeholder — HASH partitions do not use
    // a range predicate; PostgreSQL routes each row to the correct child.
    let sql = merge_sql.replace("__PGT_PART_PRED__", "");

    pgrx::debug1!(
        "[pg_trickle] A1-3b: HASH parent-level MERGE for {}.{}",
        schema,
        name,
    );

    Spi::connect_mut(|client| {
        let result = client
            .update(&sql, None, &[])
            .map_err(|e| PgTrickleError::SpiError(format!("hash merge: {e}")))?;
        Ok::<usize, PgTrickleError>(result.len())
    })
}

/// Build a MERGE SQL statement targeting a specific HASH child partition.
///
/// The delta is filtered to only rows whose partition key hashes to this child
/// using PostgreSQL's `satisfies_hash_partition()` function.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_hash_child_merge(
    child_target: &str,
    temp_delta: &str,
    quoted_partition_col: &str,
    parent_oid: pg_sys::Oid,
    modulus: i32,
    remainder: i32,
    original_merge: &str,
    parent_target: &str,
) -> String {
    // The original MERGE has a USING clause that references the delta.
    // We replace the entire MERGE to target the child with a filtered delta.
    //
    // Strategy: rewrite the original merge_sql by:
    // 1. Replacing the parent target with ONLY child_target
    // 2. Wrapping the USING subquery to filter through satisfies_hash_partition
    // 3. Removing the __PGT_PART_PRED__ placeholder

    // Find and replace "USING (...) AS d" with filtered version that reads
    // from the materialized temp table.
    let using_start = original_merge.find("USING (");
    let on_clause = original_merge.find(" ON st.");

    if let (Some(us), Some(on)) = (using_start, on_clause) {
        // Reconstruct: everything before USING + filtered USING + everything from ON
        let before_using = &original_merge[..us];
        let from_on = &original_merge[on..];

        // Build filtered USING clause
        let filtered_using = format!(
            "USING (SELECT * FROM {temp_delta} WHERE \
             satisfies_hash_partition({parent_oid}::oid, {modulus}, {remainder}, {quoted_partition_col})) AS d",
            parent_oid = parent_oid.to_u32(),
        );

        let result = format!("{before_using}{filtered_using}{from_on}",);

        // Replace parent target with ONLY child_target and strip predicate placeholder
        result
            .replace(parent_target, &format!("ONLY {child_target}"))
            .replace("__PGT_PART_PRED__", "")
    } else {
        // Fallback: simple replacement (shouldn't happen in practice)
        original_merge
            .replace(parent_target, &format!("ONLY {child_target}"))
            .replace("__PGT_PART_PRED__", "")
    }
}

// ── DAG-3: Delta amplification detection ────────────────────────────

/// Compute the amplification ratio between input delta and output delta.
///
/// Returns `output / input` when `input > 0`, otherwise `0.0` (no
/// amplification measurable when there was no input).
///
/// This is a pure function separated from the SPI layer so it can be
/// unit-tested without a PostgreSQL backend.
pub(crate) fn compute_amplification_ratio(input_delta: i64, output_delta: i64) -> f64 {
    if input_delta <= 0 {
        return 0.0;
    }
    output_delta as f64 / input_delta as f64
}

/// Determine whether the amplification ratio exceeds the configured
/// threshold and a WARNING should be emitted.
///
/// Returns `false` when detection is disabled (threshold ≤ 0) or when
/// there is no meaningful input (input_delta ≤ 0).
pub(crate) fn should_warn_amplification(
    input_delta: i64,
    output_delta: i64,
    threshold: f64,
) -> bool {
    if threshold <= 0.0 || input_delta <= 0 {
        return false;
    }
    compute_amplification_ratio(input_delta, output_delta) > threshold
}
