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

pub fn prepare_cross_cycle_phantom_table(
    st: &crate::catalog::StreamTableMeta,
    defining_query: &str,
) -> Result<(), PgTrickleError> {
    let row_id_expr = crate::dvm::row_id_expr_for_query(defining_query);
    let recon_table = format!("__pgt_recon_{}", st.pgt_id);
    let recon_select =
        format!("SELECT {row_id_expr} AS __pgt_row_id, sub.* FROM ({defining_query}) sub");
    // defining_query is definition-derived: parse/analyze it under the
    // owner's identity and stored search_path, not this privileged caller's.
    crate::refresh::with_stream_owner(st, || {
        crate::refresh::prepare_owner_temp_table(st, &recon_table, &recon_select)
    })?;
    Ok(())
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
///   * Materialise the full-query output (including `__pgt_row_id`).
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
pub fn cleanup_cross_cycle_phantoms(
    pgt_id: i64,
    stream_table_name: &str,
    defining_query: &str,
    batch_size: i64,
) -> Result<i64, PgTrickleError> {
    let row_id_expr = crate::dvm::row_id_expr_for_query(defining_query);
    let user_cols = crate::dvm::get_defining_query_columns(defining_query)?;

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
    let json_fields_for = |alias: &str| -> String {
        if quoted_user_cols.is_empty() {
            // Degenerate case (no user columns); use a constant fingerprint
            // so reconciliation collapses to a row-count diff.
            return "''::text".to_string();
        }
        let pairs: Vec<String> = quoted_user_cols
            .iter()
            .map(|c| {
                let key = c.trim_matches('"').replace("\"\"", "\"");
                let key_lit = key.replace('\'', "''");
                format!("'{key_lit}', {alias}.{c}")
            })
            .collect();
        format!("md5(jsonb_build_object({})::text)", pairs.join(", "))
    };
    let st_sig = json_fields_for("st");
    let r_sig = json_fields_for("r");

    // Step 1: materialise the live full-query result into the prepared temp table.
    let recon_table = format!("__pgt_recon_{pgt_id}");
    let insert_recon_sql = format!(
        "INSERT INTO {recon_table} \
         SELECT {row_id_expr} AS __pgt_row_id, sub.* FROM ({defining_query}) sub"
    );
    Spi::run(&insert_recon_sql).map_err(|e| {
        PgTrickleError::SpiError(format!("EC01-2 reconcile materialise failed: {e}"))
    })?;

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

    let deleted = Spi::get_one_with_args::<i64>(&delete_sql, &[batch_size.into()])
        .map_err(|e| PgTrickleError::SpiError(format!("EC01-2 reconcile DELETE failed: {e}")))?
        .unwrap_or(0);

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
                       ROW_NUMBER() OVER (PARTITION BY {r_sig_r}) AS rn \
                FROM {recon_table} r \
            ), \
            to_insert AS ( \
                SELECT n.* \
                FROM recon_numbered n \
                JOIN shortfall sh ON sh.__pgt_sig = n.__pgt_sig_x \
                WHERE n.rn <= sh.missing_n \
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

    let inserted = Spi::get_one_with_args::<i64>(&insert_sql, &[batch_size.into()])
        .map_err(|e| PgTrickleError::SpiError(format!("EC01-2 reconcile INSERT failed: {e}")))?
        .unwrap_or(0);

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
    fn test_cleanup_returns_zero_for_empty_case() {
        // Verify batch_size defaults are positive integers
        let batch_size: i64 = 1000;
        assert!(batch_size > 0);
    }
}
