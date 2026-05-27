//! Polling-based CDC for foreign tables and materialized views (A45-7).
//!
//! Extracted from `cdc.rs` as part of module decomposition. Contains the
//! snapshot-based change detection logic for sources that do not support
//! trigger-based or WAL-based CDC.

use pgrx::prelude::*;

use crate::error::PgTrickleError;

pub fn setup_foreign_table_polling(
    source_oid: pg_sys::Oid,
    pgt_id: i64,
    change_schema: &str,
) -> Result<(), PgTrickleError> {
    let oid_u32 = source_oid.to_u32();
    // CITUS-4: Compute stable_name for buffer/snapshot naming.
    let stable_name =
        crate::citus::stable_name_for_oid(source_oid).unwrap_or_else(|_| oid_u32.to_string());

    // Check if already tracked
    let already_tracked = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_change_tracking WHERE source_relid = $1)",
        &[source_oid.into()],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if !already_tracked {
        let col_defs = super::resolve_source_column_defs(source_oid)?;

        // Create the change buffer table (same as trigger-based CDC).
        super::create_change_buffer_table(source_oid, change_schema, &col_defs, &stable_name)?;

        // Create a snapshot table: stores the previous contents of the
        // foreign table so we can compute EXCEPT-based deltas on each poll.
        let snapshot_table = format!("\"{change_schema}\".snapshot_{stable_name}");
        let source_table = Spi::get_one_with_args::<String>(
            "SELECT $1::oid::regclass::text",
            &[source_oid.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .ok_or_else(|| {
            PgTrickleError::NotFound(format!("Foreign table with OID {oid_u32} not found"))
        })?;

        // Create snapshot as an empty copy of the source table structure.
        // snapshot_table: extension-controlled name (change_schema + stable_name derived from OID);
        // source_table: PostgreSQL's own regclass::text output — already properly quoted by the DB.
        let create_snap_sql = format!(
            "CREATE TABLE IF NOT EXISTS {snapshot_table} (LIKE {source_table} INCLUDING ALL)"
        );
        Spi::run(&create_snap_sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?; // nosemgrep: semgrep.rust.spi.run.dynamic-format

        // Seed the snapshot with the current foreign table contents.
        let seed_snap_sql = format!("INSERT INTO {snapshot_table} SELECT * FROM {source_table}");
        Spi::run(&seed_snap_sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?; // nosemgrep: semgrep.rust.spi.run.dynamic-format

        // Record tracking with synthetic slot_name indicating polling CDC.
        Spi::run_with_args(
            "INSERT INTO pgtrickle.pgt_change_tracking \
             (source_relid, slot_name, source_stable_name, tracked_by_pgt_ids) \
             VALUES ($1, $2, $3, ARRAY[$4])",
            &[
                source_oid.into(),
                format!("foreign_poll_{stable_name}").as_str().into(),
                stable_name.as_str().into(),
                pgt_id.into(),
            ],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    } else {
        // Already tracked — add this pgt_id to the tracking array.
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_change_tracking \
             SET tracked_by_pgt_ids = array_append(tracked_by_pgt_ids, $1) \
             WHERE source_relid = $2 AND NOT ($1 = ANY(tracked_by_pgt_ids))",
            &[pgt_id.into(), source_oid.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    }

    Ok(())
}

/// Poll a foreign table source for changes and populate the change buffer.
///
/// Computes the symmetric difference between the current foreign table
/// contents and the snapshot table using EXCEPT ALL, inserts the deltas
/// into the change buffer, and refreshes the snapshot.
///
/// Uses `pg_current_wal_insert_lsn()` as the LSN for delta rows (even
/// though the foreign table itself has no WAL entries, the local write
/// to the change buffer does).
pub fn poll_foreign_table_changes(
    source_oid: pg_sys::Oid,
    change_schema: &str,
) -> Result<(), PgTrickleError> {
    let oid_u32 = source_oid.to_u32();
    // CITUS-4: Use stable names for change buffer and snapshot tables.
    let stable_name =
        crate::citus::stable_name_for_oid(source_oid).unwrap_or_else(|_| oid_u32.to_string());
    let change_table = format!("\"{change_schema}\".changes_{stable_name}");
    let snapshot_table = format!("\"{change_schema}\".snapshot_{stable_name}");

    let source_table =
        Spi::get_one_with_args::<String>("SELECT $1::oid::regclass::text", &[source_oid.into()])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            .ok_or_else(|| {
                PgTrickleError::NotFound(format!("Foreign table with OID {oid_u32} not found"))
            })?;

    let col_defs = super::resolve_source_column_defs(source_oid)?;
    if col_defs.is_empty() {
        return Ok(());
    }
    let pk_columns = super::resolve_pk_columns(source_oid)?;

    // Build column lists for INSERT into change buffer.
    // A44-10: Flat column names (no new_/old_ prefix) matching base table
    // change buffer schema created by super::create_change_buffer_table().
    // cb_col_names: change-buffer INSERT target names (uses super::cb_col_name() for reserved cols).
    // src_col_names: SELECT source names from the actual table (original names).
    let cb_col_names: Vec<String> = col_defs
        .iter()
        .map(|(name, _)| format!("\"{}\"", super::cb_col_name(name).replace('"', "\"\"")))
        .collect();
    let src_col_names: Vec<String> = col_defs
        .iter()
        .map(|(name, _)| format!("\"{}\"", name.replace('"', "\"\"")))
        .collect();

    // Build pk_hash expression for delta rows.
    let hash_cols: Vec<String> = if pk_columns.is_empty() {
        col_defs.iter().map(|(n, _)| n.clone()).collect()
    } else {
        pk_columns.clone()
    };
    let pk_hash_expr = if hash_cols.len() == 1 {
        let c = format!("\"{}\"", hash_cols[0].replace('"', "\"\""));
        format!("pgtrickle.pg_trickle_hash({c}::text)")
    } else {
        let items: Vec<String> = hash_cols
            .iter()
            .map(|c| format!("\"{}\"::text", c.replace('"', "\"\"")))
            .collect();
        format!(
            "pgtrickle.pg_trickle_hash_multi(ARRAY[{}])",
            items.join(", ")
        )
    };

    let cb_col_list = cb_col_names.join(", ");
    let src_col_list = src_col_names.join(", ");

    // ── Deleted rows: in snapshot but not in current foreign table ──
    // These appear as 'D' (delete) rows in the change buffer.
    // INSERT target uses cb_col_list (change-buffer names); SELECT uses src_col_list
    // (original source names). Values are matched positionally.
    let deleted_sql = format!(
        "INSERT INTO {change_table} (lsn, action, pk_hash, {cb_col_list}) \
         SELECT pg_current_wal_insert_lsn(), 'D', {pk_hash_expr}, {src_col_list} \
         FROM (\
           SELECT {src_col_list} FROM {snapshot_table} \
           EXCEPT ALL \
           SELECT {src_col_list} FROM {source_table}\
         ) __pgt_del"
    );
    Spi::run(&deleted_sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    // ── Inserted rows: in current foreign table but not in snapshot ──
    // These appear as 'I' (insert) rows in the change buffer.
    let inserted_sql = format!(
        "INSERT INTO {change_table} (lsn, action, pk_hash, {cb_col_list}) \
         SELECT pg_current_wal_insert_lsn(), 'I', {pk_hash_expr}, {src_col_list} \
         FROM (\
           SELECT {src_col_list} FROM {source_table} \
           EXCEPT ALL \
           SELECT {src_col_list} FROM {snapshot_table}\
         ) __pgt_ins"
    );
    Spi::run(&inserted_sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    // ── Refresh snapshot — replace contents with current foreign table ──
    // snapshot_table: extension-controlled name; source_table: PostgreSQL regclass::text (safe).
    let truncate_sql = format!("TRUNCATE {snapshot_table}");
    Spi::run(&truncate_sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?; // nosemgrep: semgrep.rust.spi.run.dynamic-format
    let refresh_sql = format!("INSERT INTO {snapshot_table} SELECT * FROM {source_table}");
    Spi::run(&refresh_sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?; // nosemgrep: semgrep.rust.spi.run.dynamic-format

    Ok(())
}

/// Set up polling-based CDC for a materialized view source (P2-4).
///
/// Identical to [`setup_foreign_table_polling`] — creates a snapshot table
/// and change buffer so that EXCEPT ALL can compute deltas when the matview
/// is refreshed externally.
pub fn setup_matview_polling(
    source_oid: pg_sys::Oid,
    pgt_id: i64,
    change_schema: &str,
) -> Result<(), PgTrickleError> {
    let oid_u32 = source_oid.to_u32();
    // CITUS-4: Compute stable_name for buffer/snapshot naming.
    let stable_name =
        crate::citus::stable_name_for_oid(source_oid).unwrap_or_else(|_| oid_u32.to_string());

    let already_tracked = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_change_tracking WHERE source_relid = $1)",
        &[source_oid.into()],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if !already_tracked {
        let col_defs = super::resolve_source_column_defs(source_oid)?;

        super::create_change_buffer_table(source_oid, change_schema, &col_defs, &stable_name)?;

        let snapshot_table = format!("\"{change_schema}\".snapshot_{stable_name}");
        let source_table = Spi::get_one_with_args::<String>(
            "SELECT $1::oid::regclass::text",
            &[source_oid.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .ok_or_else(|| {
            PgTrickleError::NotFound(format!("Materialized view with OID {oid_u32} not found"))
        })?;

        // DDL cannot be parameterized; snapshot_table is from a stable_name, source_table is oid::regclass::text, both extension-controlled.
        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS {snapshot_table} (LIKE {source_table} INCLUDING ALL)"
        );
        Spi::run(&create_sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        let insert_sql = format!("INSERT INTO {snapshot_table} SELECT * FROM {source_table}");
        Spi::run(&insert_sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        Spi::run_with_args(
            "INSERT INTO pgtrickle.pgt_change_tracking \
             (source_relid, slot_name, source_stable_name, tracked_by_pgt_ids) \
             VALUES ($1, $2, $3, ARRAY[$4])",
            &[
                source_oid.into(),
                format!("matview_poll_{stable_name}").as_str().into(),
                stable_name.as_str().into(),
                pgt_id.into(),
            ],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    } else {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_change_tracking \
             SET tracked_by_pgt_ids = array_append(tracked_by_pgt_ids, $1) \
             WHERE source_relid = $2 AND NOT ($1 = ANY(tracked_by_pgt_ids))",
            &[pgt_id.into(), source_oid.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    }

    Ok(())
}

/// Poll a materialized view source for changes (P2-4).
///
/// Identical to [`poll_foreign_table_changes`] — uses EXCEPT ALL between
/// the current matview contents and the snapshot to detect inserts/deletes.
pub fn poll_matview_changes(
    source_oid: pg_sys::Oid,
    change_schema: &str,
) -> Result<(), PgTrickleError> {
    // Delegate to the same implementation — the logic is identical regardless
    // of whether the source is a foreign table or a materialized view.
    poll_foreign_table_changes(source_oid, change_schema)
}

/// Build a `pk_hash` SQL expression for the given list of column names.
///
/// Single-column: `pgtrickle.pg_trickle_hash("col"::text)`
/// Multi-column:  `pgtrickle.pg_trickle_hash_multi(ARRAY["c1"::text, "c2"::text])`
///
/// This is a pure-Rust helper extracted from `poll_foreign_table_changes` and
/// `setup_foreign_table_polling` to enable unit testing.
pub(crate) fn build_pk_hash_expr(hash_cols: &[String]) -> String {
    if hash_cols.len() == 1 {
        let c = format!("\"{}\"", hash_cols[0].replace('"', "\"\""));
        format!("pgtrickle.pg_trickle_hash({c}::text)")
    } else {
        let items: Vec<String> = hash_cols
            .iter()
            .map(|c| format!("\"{}\"::text", c.replace('"', "\"\"")))
            .collect();
        format!(
            "pgtrickle.pg_trickle_hash_multi(ARRAY[{}])",
            items.join(", ")
        )
    }
}

// TEST-10-02 (v0.49.0): Unit tests for pure-Rust polling CDC logic.
#[cfg(test)]
mod tests {
    use super::build_pk_hash_expr;

    #[test]
    fn test_pk_hash_single_column() {
        let expr = build_pk_hash_expr(&["id".to_string()]);
        assert_eq!(expr, r#"pgtrickle.pg_trickle_hash("id"::text)"#);
    }

    #[test]
    fn test_pk_hash_multi_column() {
        let expr = build_pk_hash_expr(&["tenant_id".to_string(), "order_id".to_string()]);
        assert_eq!(
            expr,
            r#"pgtrickle.pg_trickle_hash_multi(ARRAY["tenant_id"::text, "order_id"::text])"#
        );
    }

    #[test]
    fn test_pk_hash_column_with_double_quotes() {
        // Column names that contain double-quotes must be escaped.
        let expr = build_pk_hash_expr(&["my\"col".to_string()]);
        assert!(
            expr.contains("\"\""),
            "embedded double-quote must be doubled: {expr}"
        );
    }

    #[test]
    fn test_pk_hash_three_columns() {
        let expr = build_pk_hash_expr(&["a".to_string(), "b".to_string(), "c".to_string()]);
        assert!(expr.starts_with("pgtrickle.pg_trickle_hash_multi(ARRAY["));
        assert!(expr.contains(r#""a"::text"#));
        assert!(expr.contains(r#""b"::text"#));
        assert!(expr.contains(r#""c"::text"#));
    }
}
