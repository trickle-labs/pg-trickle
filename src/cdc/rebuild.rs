//! CDC trigger rebuild and WAL availability functions (A45-7).
//!
//! Extracted from `cdc.rs` as part of module decomposition. Contains trigger
//! management functions (rebuild, existence check) and WAL/replica identity
//! helpers.

use pgrx::prelude::*;

use crate::config;
use crate::error::PgTrickleError;

pub fn rebuild_cdc_trigger_function(
    source_oid: pg_sys::Oid,
    change_schema: &str,
) -> Result<(), PgTrickleError> {
    let pk_columns = super::resolve_pk_columns(source_oid)?;
    // F15: use the minimal column set (union of columns_used across all downstream STs,
    // always including PK columns). Falls back to full capture when any ST uses SELECT *.
    let columns = super::resolve_referenced_column_defs(source_oid)?;

    // Nothing to rebuild if the table has no user columns.
    if columns.is_empty() {
        return Ok(());
    }

    let oid_u32 = source_oid.to_u32();
    // CITUS-4: Use stable_name for all trigger/function names.
    let cdc_name = super::get_cdc_name_for_source(source_oid);

    // Rebuild the function body(ies) for the current CDC trigger mode GUC.
    let mode = config::pg_trickle_cdc_trigger_mode();
    match mode {
        config::CdcTriggerMode::Statement => {
            let (ins_fn, upd_fn, del_fn) =
                super::build_stmt_trigger_fn_sql(change_schema, &cdc_name, &pk_columns, &columns);
            Spi::run(&ins_fn).map_err(|e| {
                PgTrickleError::SpiError(format!(
                    "Failed to rebuild CDC INSERT trigger function: {}",
                    e
                ))
            })?;
            Spi::run(&upd_fn).map_err(|e| {
                PgTrickleError::SpiError(format!(
                    "Failed to rebuild CDC UPDATE trigger function: {}",
                    e
                ))
            })?;
            Spi::run(&del_fn).map_err(|e| {
                PgTrickleError::SpiError(format!(
                    "Failed to rebuild CDC DELETE trigger function: {}",
                    e
                ))
            })?;
        }
        config::CdcTriggerMode::Row => {
            let fn_sql =
                super::build_row_trigger_fn_sql(change_schema, &cdc_name, &pk_columns, &columns);
            Spi::run(&fn_sql).map_err(|e| {
                PgTrickleError::SpiError(format!("Failed to rebuild CDC trigger function: {}", e))
            })?;
        }
    }
    // suppress unused warning when name == oid string
    let _ = oid_u32;

    // Sync change buffer table schema: add any columns that are present in
    // the current source but missing from the buffer (e.g. after ADD COLUMN).
    sync_change_buffer_columns(source_oid, change_schema, &columns)?;

    Ok(())
}

/// Rebuild the CDC trigger function body **and** replace the trigger DDL for a
/// source table.
///
/// Unlike `rebuild_cdc_trigger_function` (which only replaces the function body
/// via `CREATE OR REPLACE`), this function also:
/// 1. Drops the existing DML trigger on the source table.
/// 2. Creates a new trigger whose type (`FOR EACH STATEMENT` or `FOR EACH ROW`)
///    matches the current `pg_trickle.cdc_trigger_mode` GUC value.
///
/// Use this to migrate existing stream tables after changing the GUC, or call
/// it from the upgrade script via `pgtrickle.rebuild_cdc_triggers()`.
pub fn rebuild_cdc_trigger(
    source_oid: pg_sys::Oid,
    change_schema: &str,
) -> Result<String, PgTrickleError> {
    let pk_columns = super::resolve_pk_columns(source_oid)?;
    // F15: use the minimal column set (union of columns_used across all downstream STs,
    // always including PK columns). Falls back to full capture when any ST uses SELECT *.
    let columns = super::resolve_referenced_column_defs(source_oid)?;

    if columns.is_empty() {
        return Ok(String::new());
    }

    let oid_u32 = source_oid.to_u32();
    // CITUS-4: Use stable_name for all trigger/function names.
    let cdc_name = super::get_cdc_name_for_source(source_oid);

    // Resolve source table name; skip gracefully if the table no longer exists.
    let source_table = match super::resolve_relation_name(source_oid)? {
        Some(t) => t,
        None => return Ok(String::new()),
    };

    let mode = config::pg_trickle_cdc_trigger_mode();

    // 1. Rebuild trigger function(s) for the current mode.
    match mode {
        config::CdcTriggerMode::Statement => {
            let (ins_fn, upd_fn, del_fn) =
                super::build_stmt_trigger_fn_sql(change_schema, &cdc_name, &pk_columns, &columns);
            Spi::run(&ins_fn).map_err(|e| {
                PgTrickleError::SpiError(format!(
                    "Failed to rebuild CDC INSERT trigger function: {}",
                    e
                ))
            })?;
            Spi::run(&upd_fn).map_err(|e| {
                PgTrickleError::SpiError(format!(
                    "Failed to rebuild CDC UPDATE trigger function: {}",
                    e
                ))
            })?;
            Spi::run(&del_fn).map_err(|e| {
                PgTrickleError::SpiError(format!(
                    "Failed to rebuild CDC DELETE trigger function: {}",
                    e
                ))
            })?;
        }
        config::CdcTriggerMode::Row => {
            let fn_sql =
                super::build_row_trigger_fn_sql(change_schema, &cdc_name, &pk_columns, &columns);
            Spi::run(&fn_sql).map_err(|e| {
                PgTrickleError::SpiError(format!("Failed to rebuild CDC trigger function: {}", e))
            })?;
        }
    }

    // 2. Drop ALL existing trigger variants (handles both row-level and
    //    statement-level triggers — whichever mode was active before).
    //    Drop both stable-name and legacy OID-based variants for backward compat.
    for trig in &[
        format!("pg_trickle_cdc_{}", cdc_name),
        format!("pg_trickle_cdc_ins_{}", cdc_name),
        format!("pg_trickle_cdc_upd_{}", cdc_name),
        format!("pg_trickle_cdc_del_{}", cdc_name),
        format!("pg_trickle_cdc_{}", oid_u32),
        format!("pg_trickle_cdc_ins_{}", oid_u32),
        format!("pg_trickle_cdc_upd_{}", oid_u32),
        format!("pg_trickle_cdc_del_{}", oid_u32),
    ] {
        let _ = Spi::run(&format!("DROP TRIGGER IF EXISTS {trig} ON {source_table}")); // nosemgrep: rust.spi.run.dynamic-format — DDL cannot be parameterized; trig is an oid_u32 integer, source_table is a regclass-quoted identifier.
    }

    // CORR-4: pgt_refresh_history is INSERT-only; skip UPDATE/DELETE triggers.
    let insert_only = super::is_insert_only_table(source_oid);

    // 3. Create new trigger(s) matching the current mode.
    match mode {
        config::CdcTriggerMode::Statement => {
            Spi::run(&format!(
                "CREATE TRIGGER pg_trickle_cdc_ins_{name} \
                 AFTER INSERT ON {table} \
                 REFERENCING NEW TABLE AS __pgt_new \
                 FOR EACH STATEMENT EXECUTE FUNCTION {cs}.pg_trickle_cdc_ins_fn_{name}()",
                name = cdc_name,
                table = source_table,
                cs = change_schema,
            ))
            .map_err(|e| {
                PgTrickleError::SpiError(format!(
                    "Failed to create CDC INSERT trigger on {}: {}",
                    source_table, e
                ))
            })?;
            if !insert_only {
                Spi::run(&format!(
                    "CREATE TRIGGER pg_trickle_cdc_upd_{name} \
                     AFTER UPDATE ON {table} \
                     REFERENCING NEW TABLE AS __pgt_new OLD TABLE AS __pgt_old \
                     FOR EACH STATEMENT EXECUTE FUNCTION {cs}.pg_trickle_cdc_upd_fn_{name}()",
                    name = cdc_name,
                    table = source_table,
                    cs = change_schema,
                ))
                .map_err(|e| {
                    PgTrickleError::SpiError(format!(
                        "Failed to create CDC UPDATE trigger on {}: {}",
                        source_table, e
                    ))
                })?;
                Spi::run(&format!(
                    "CREATE TRIGGER pg_trickle_cdc_del_{name} \
                     AFTER DELETE ON {table} \
                     REFERENCING OLD TABLE AS __pgt_old \
                     FOR EACH STATEMENT EXECUTE FUNCTION {cs}.pg_trickle_cdc_del_fn_{name}()",
                    name = cdc_name,
                    table = source_table,
                    cs = change_schema,
                ))
                .map_err(|e| {
                    PgTrickleError::SpiError(format!(
                        "Failed to create CDC DELETE trigger on {}: {}",
                        source_table, e
                    ))
                })?;
            }
        }
        config::CdcTriggerMode::Row => {
            let dml_events = if insert_only {
                "INSERT"
            } else {
                "INSERT OR UPDATE OR DELETE"
            };
            Spi::run(&format!(
                "CREATE TRIGGER pg_trickle_cdc_{name} \
                 AFTER {events} ON {table} \
                 FOR EACH ROW EXECUTE FUNCTION {cs}.pg_trickle_cdc_fn_{name}()",
                name = cdc_name,
                events = dml_events,
                table = source_table,
                cs = change_schema,
            ))
            .map_err(|e| {
                PgTrickleError::SpiError(format!(
                    "Failed to create CDC trigger on {}: {}",
                    source_table, e
                ))
            })?;
        }
    }
    // 4. Sync the change buffer column schema.
    sync_change_buffer_columns(source_oid, change_schema, &columns)?;

    let primary_trig = match mode {
        config::CdcTriggerMode::Statement => format!("pg_trickle_cdc_ins_{}", cdc_name),
        config::CdcTriggerMode::Row => format!("pg_trickle_cdc_{}", cdc_name),
    };
    Ok(primary_trig)
}

/// Sync the change buffer table schema to match the current source columns.
///
/// A44-10 (v0.43.0 — D+I schema): adds flat `col` columns (no new_/old_ prefix).
///
/// **Schema version guard:** detects the current CB schema version before
/// computing the expected column set. Running the D+I-era logic against a
/// pre-migration wide-schema buffer would silently drop ALL user data columns.
/// If any existing column has a `new_` prefix, the buffer is in the old wide
/// schema and this function does a no-op (the migration path in ALTER EXTENSION
/// UPDATE will handle the rebuild).
///
/// F39: Columns that were dropped from the source are cleaned up by dropping
/// the orphaned flat columns from the buffer table.
fn sync_change_buffer_columns(
    source_oid: pg_sys::Oid,
    change_schema: &str,
    columns: &[(String, String)],
) -> Result<(), PgTrickleError> {
    let buffer_table = super::buffer_qualified_name_for_oid(change_schema, source_oid);

    // Fetch existing column names and types from the change buffer table.
    let existing_sql = format!(
        "SELECT attname::text, format_type(atttypid, atttypmod) \
         FROM pg_attribute \
         WHERE attrelid = '{buffer_table}'::regclass \
           AND attnum > 0 AND NOT attisdropped",
    );

    // Map of column name → current type string in the change buffer.
    let existing_cols: std::collections::HashMap<String, String> = Spi::connect(|client| {
        let result = client
            .select(&existing_sql, None, &[])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        let mut map = std::collections::HashMap::new();
        for row in result {
            let name: Option<String> = row
                .get(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            let type_str: Option<String> = row
                .get(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            if let Some(n) = name {
                map.insert(n, type_str.unwrap_or_default());
            }
        }
        Ok(map)
    })?;

    // A44-10 schema version guard: if any data column has a `new_` prefix, this
    // change buffer is still in the old wide schema. Skip D+I-era sync to avoid
    // silently dropping user data columns. The ALTER EXTENSION UPDATE migration
    // path handles the full rebuild.
    let is_wide_schema = existing_cols
        .keys()
        .any(|k| k.starts_with("new_") || k.starts_with("old_"));
    if is_wide_schema {
        pgrx::debug1!(
            "pg_trickle_cdc: {buffer_table} is in wide schema (pre-A44-10); \
             skipping D+I column sync — requires ALTER EXTENSION pg_trickle UPDATE"
        );
        return Ok(());
    }

    // Build the set of expected flat column names from current source columns.
    // A44-10: apply super::cb_col_name() so reserved source columns (e.g. "action") are
    // tracked as their CB-mapped names (e.g. "__usr_action") — otherwise the
    // orphan-drop loop would drop "__usr_action" because "action" is in
    // expected_data_cols but "__usr_action" is not.
    let expected_data_cols: std::collections::HashSet<String> = columns
        .iter()
        .map(|(col_name, _)| super::cb_col_name(col_name))
        .collect();

    // System columns: never dropped, not tracked as data columns.
    // changed_cols is a system column (Task 3.1 bitmask — preserved across schema changes).
    // __pgt_trace_context is the F10 trace propagation column — preserved as a system column.
    // source_xid and source_commit_at are v0.90 provenance columns.
    let system_cols: std::collections::HashSet<&str> = [
        "change_id",
        "lsn",
        "action",
        "source_xid",
        "source_commit_at",
        "__pgt_row_id",
        "changed_cols",
        "__pgt_trace_context",
    ]
    .iter()
    .copied()
    .collect();

    // Ensure changed_cols VARBIT column exists (WB-1: migrated from BIGINT).
    if !existing_cols.contains_key("changed_cols") {
        let add_sql =
            format!("ALTER TABLE {buffer_table} ADD COLUMN IF NOT EXISTS changed_cols VARBIT");
        if let Err(e) = Spi::run(&add_sql) {
            pgrx::debug1!("pg_trickle_cdc: failed to add changed_cols to {buffer_table}: {e}");
        }
    } else {
        // WB-1 migration: convert BIGINT bitmask column to VARBIT on existing buffers.
        let is_bigint = existing_cols
            .get("changed_cols")
            .map(|t| t == "bigint")
            .unwrap_or(false);
        if is_bigint {
            let migrate_sql = format!(
                "ALTER TABLE {buffer_table} ALTER COLUMN changed_cols TYPE VARBIT USING NULL"
            );
            if let Err(e) = Spi::run(&migrate_sql) {
                pgrx::debug1!(
                    "pg_trickle_cdc: failed to migrate changed_cols to VARBIT on {buffer_table}: {e}"
                );
            }
        }
    }

    for existing in existing_cols.keys() {
        if system_cols.contains(existing.as_str()) {
            continue;
        }
        if !expected_data_cols.contains(existing) {
            let sql = format!(
                "ALTER TABLE {buffer_table} DROP COLUMN IF EXISTS \"{}\"",
                existing.replace('"', "\"\"")
            );
            if let Err(e) = Spi::run(&sql) {
                pgrx::warning!(
                    "pg_trickle_cdc: failed to drop orphaned column \"{}\" from {}: {}",
                    existing,
                    buffer_table,
                    e
                );
            } else {
                pgrx::debug1!(
                    "pg_trickle_cdc: dropped orphaned column \"{}\" from {}",
                    existing,
                    buffer_table
                );
            }
        }
    }

    // For each source column, add flat "cb_col" if missing or widen if type changed.
    // A44-10: apply super::cb_col_name() so reserved source columns (e.g. "action") are
    // stored under their CB-mapped names (e.g. "__usr_action").
    for (col_name, col_type) in columns {
        let cb_col = super::cb_col_name(col_name);
        match existing_cols.get(cb_col.as_str()) {
            None => {
                let qcol = cb_col.replace('"', "\"\"");
                let sql = format!(
                    "ALTER TABLE {buffer_table} ADD COLUMN IF NOT EXISTS \"{qcol}\" {col_type}"
                );
                Spi::run(&sql).map_err(|e| {
                    PgTrickleError::SpiError(format!(
                        "Failed to add column \"{cb_col}\" to change buffer: {e}"
                    ))
                })?;
                pgrx::debug1!(
                    "pg_trickle_cdc: added column \"{}\" to {}",
                    cb_col,
                    buffer_table
                );
            }
            Some(existing_type) if existing_type != col_type => {
                // Type widening (e.g. VARCHAR(50) → VARCHAR(200)).
                let qcol = cb_col.replace('"', "\"\"");
                let sql =
                    format!("ALTER TABLE {buffer_table} ALTER COLUMN \"{qcol}\" TYPE {col_type}");
                if let Err(e) = Spi::run(&sql) {
                    pgrx::warning!(
                        "pg_trickle_cdc: failed to widen \"{}\" in {}: {}",
                        cb_col,
                        buffer_table,
                        e
                    );
                } else {
                    pgrx::debug1!(
                        "pg_trickle_cdc: widened column \"{}\" to {} in {}",
                        cb_col,
                        col_type,
                        buffer_table
                    );
                }
            }
            _ => {}
        }
    }

    Ok(())
}

/// Check if a CDC trigger exists for a source table.
pub fn trigger_exists(source_oid: pg_sys::Oid) -> Result<bool, PgTrickleError> {
    let oid_u32 = source_oid.to_u32();
    // CITUS-4: Triggers may use the stable_name (e.g. "pg_trickle_cdc_5fba..."
    // for new tables) OR the legacy OID-based name ("pg_trickle_cdc_12345").
    // Resolve the stable_name so we check both naming conventions.
    let stable_name = super::get_cdc_name_for_source(source_oid);
    // Check for any DML CDC trigger: the legacy combined row-level trigger
    // OR any of the three per-event statement-level triggers, under both
    // stable-name and OID-based naming.
    let exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(
            SELECT 1 FROM pg_trigger
            WHERE tgname IN ($1, $3, $4, $5, $6, $7, $8, $9)
              AND tgrelid = $2
        )",
        &[
            format!("pg_trickle_cdc_{}", oid_u32).as_str().into(),
            source_oid.into(),
            format!("pg_trickle_cdc_ins_{}", oid_u32).as_str().into(),
            format!("pg_trickle_cdc_upd_{}", oid_u32).as_str().into(),
            format!("pg_trickle_cdc_del_{}", oid_u32).as_str().into(),
            format!("pg_trickle_cdc_{}", stable_name).as_str().into(),
            format!("pg_trickle_cdc_ins_{}", stable_name)
                .as_str()
                .into(),
            format!("pg_trickle_cdc_upd_{}", stable_name)
                .as_str()
                .into(),
            format!("pg_trickle_cdc_del_{}", stable_name)
                .as_str()
                .into(),
        ],
    )
    .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;

    Ok(exists.unwrap_or(false))
}

/// Get the trigger name for a source OID.
pub fn trigger_name_for_source(source_oid: pg_sys::Oid) -> String {
    trigger_name_for_oid_u32(source_oid.to_u32())
}

/// Pure-Rust inner implementation of trigger name generation.
///
/// Separated from `trigger_name_for_source` to allow unit testing without
/// a PostgreSQL backend (avoids the `pg_sys::Oid` type in test code).
pub(crate) fn trigger_name_for_oid_u32(oid: u32) -> String {
    format!("pg_trickle_cdc_{oid}")
}

/// Pure-Rust helper: returns `true` when `cdc_mode` permits logical replication.
///
/// The `"trigger"` mode always bypasses logical replication regardless of
/// server configuration. Used as the early-exit guard in
/// `can_use_logical_replication_for_mode`.
pub(crate) fn cdc_mode_allows_logical(cdc_mode: &str) -> bool {
    !cdc_mode.eq_ignore_ascii_case("trigger")
}

/// Pure-Rust helper: returns `true` when `identity` is sufficient for WAL CDC.
///
/// Only `"default"` (PK-based) and `"full"` (all columns) provide the OLD
/// row values needed by logical decoding.
pub(crate) fn replica_identity_sufficient(identity: &str) -> bool {
    identity == "default" || identity == "full"
}

// ── WAL Availability Detection ─────────────────────────────────────────────

/// Check if the server supports logical replication for CDC.
///
/// Returns `true` if ALL of:
/// - `pg_trickle.cdc_mode` is `"auto"` or `"wal"` (not `"trigger"`)
/// - `wal_level` is `'logical'`
/// - `max_replication_slots` > currently used slots
///
/// When `cdc_mode = "wal"` and `wal_level != logical`, returns an error
/// instead of `false` (hard requirement).
pub fn can_use_logical_replication() -> Result<bool, PgTrickleError> {
    can_use_logical_replication_for_mode(&config::pg_trickle_cdc_mode())
}

/// Check if the server supports logical replication for a requested CDC mode.
pub fn can_use_logical_replication_for_mode(cdc_mode: &str) -> Result<bool, PgTrickleError> {
    if cdc_mode.eq_ignore_ascii_case("trigger") {
        return Ok(false);
    }

    let wal_level = Spi::get_one::<String>("SELECT current_setting('wal_level')")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .unwrap_or_default();

    if wal_level != "logical" {
        if cdc_mode.eq_ignore_ascii_case("wal") {
            return Err(PgTrickleError::InvalidArgument(
                "pg_trickle.cdc_mode = 'wal' requires wal_level = logical".into(),
            ));
        }
        return Ok(false);
    }

    // Check available replication slots
    let available = Spi::get_one::<i64>(
        "SELECT current_setting('max_replication_slots')::bigint \
         - (SELECT count(*) FROM pg_replication_slots)",
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or(0);

    Ok(available > 0)
}

/// Check if a source table has adequate REPLICA IDENTITY for logical decoding.
///
/// Returns `true` if REPLICA IDENTITY is `DEFAULT` (has PK) or `FULL`.
/// Returns `false` if `NOTHING` or if using an index that doesn't cover
/// all needed columns.
///
/// The `relreplident` column in `pg_class` encodes:
/// - `'d'` → DEFAULT (PK-based, sufficient for UPDATE/DELETE old values)
/// - `'f'` → FULL (always includes all columns)
/// - `'n'` → NOTHING (no old values for UPDATE/DELETE)
/// - `'i'` → INDEX (uses a specific index; may or may not be sufficient)
pub fn check_replica_identity(source_oid: pg_sys::Oid) -> Result<bool, PgTrickleError> {
    let identity = Spi::get_one_with_args::<String>(
        "SELECT CASE relreplident \
           WHEN 'd' THEN 'default' \
           WHEN 'f' THEN 'full' \
           WHEN 'n' THEN 'nothing' \
           WHEN 'i' THEN 'index' \
         END FROM pg_class WHERE oid = $1",
        &[source_oid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or_else(|| "nothing".into());

    // 'default' works if the table has a PK (which pg_trickle already checks)
    // 'full' always works
    // 'nothing' doesn't provide OLD values for UPDATE/DELETE
    // 'index' may work but needs further validation in later phases
    Ok(identity == "default" || identity == "full")
}

/// Return the REPLICA IDENTITY mode as a string for a source table.
///
/// Returns one of: `"default"`, `"full"`, `"nothing"`, `"index"`.
/// Used by the WAL transition guard (G2.2) to require REPLICA IDENTITY FULL.
pub fn get_replica_identity_mode(source_oid: pg_sys::Oid) -> Result<String, PgTrickleError> {
    let identity = Spi::get_one_with_args::<String>(
        "SELECT CASE relreplident \
           WHEN 'd' THEN 'default' \
           WHEN 'f' THEN 'full' \
           WHEN 'n' THEN 'nothing' \
           WHEN 'i' THEN 'index' \
         END FROM pg_class WHERE oid = $1",
        &[source_oid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or_else(|| "nothing".into());

    Ok(identity)
}

/// Returns true if the relation has any user-defined row-level triggers
/// (excluding internal triggers and pg_trickle's own CDC triggers).
///
/// Used by the refresh executor to decide whether to use the explicit DML
/// path (which fires triggers with correct `TG_OP` / `OLD` / `NEW`) instead
/// of the single-pass MERGE path.
///
/// This is a lightweight query — single index scan on `pg_trigger(tgrelid)`.
pub fn has_user_triggers(st_relid: pg_sys::Oid) -> Result<bool, PgTrickleError> {
    Spi::get_one::<bool>(&format!(
        "SELECT EXISTS(\
           SELECT 1 FROM pg_trigger \
           WHERE tgrelid = {}::oid \
             AND tgisinternal = false \
             AND tgname NOT LIKE 'pgt_%' \
             AND tgname NOT LIKE 'pg_trickle_%' \
         )",
        st_relid.to_u32(),
    ))
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))
    .map(|v| v.unwrap_or(false))
}

// TEST-10-02 (v0.49.0): Unit tests for pure-Rust CDC rebuild logic.
#[cfg(test)]
mod tests {
    use super::{cdc_mode_allows_logical, replica_identity_sufficient, trigger_name_for_oid_u32};

    // ── Trigger name generation ──────────────────────────────────────

    #[test]
    fn test_trigger_name_for_oid_u32_basic() {
        assert_eq!(trigger_name_for_oid_u32(12345), "pg_trickle_cdc_12345");
    }

    #[test]
    fn test_trigger_name_for_oid_u32_zero() {
        assert_eq!(trigger_name_for_oid_u32(0), "pg_trickle_cdc_0");
    }

    #[test]
    fn test_trigger_name_for_oid_u32_max() {
        let name = trigger_name_for_oid_u32(u32::MAX);
        assert!(
            name.starts_with("pg_trickle_cdc_"),
            "name must start with prefix: {name}"
        );
        assert!(
            name.ends_with(&u32::MAX.to_string()),
            "name must end with OID: {name}"
        );
    }

    // ── CDC mode logical replication guard ───────────────────────────

    #[test]
    fn test_cdc_mode_trigger_disallows_logical() {
        assert!(!cdc_mode_allows_logical("trigger"));
        assert!(!cdc_mode_allows_logical("TRIGGER"));
        assert!(!cdc_mode_allows_logical("Trigger"));
    }

    #[test]
    fn test_cdc_mode_wal_allows_logical() {
        assert!(cdc_mode_allows_logical("wal"));
        assert!(cdc_mode_allows_logical("WAL"));
    }

    #[test]
    fn test_cdc_mode_auto_allows_logical() {
        assert!(cdc_mode_allows_logical("auto"));
        assert!(cdc_mode_allows_logical("AUTO"));
    }

    // ── Replica identity sufficiency ─────────────────────────────────

    #[test]
    fn test_replica_identity_default_sufficient() {
        assert!(replica_identity_sufficient("default"));
    }

    #[test]
    fn test_replica_identity_full_sufficient() {
        assert!(replica_identity_sufficient("full"));
    }

    #[test]
    fn test_replica_identity_nothing_insufficient() {
        assert!(!replica_identity_sufficient("nothing"));
    }

    #[test]
    fn test_replica_identity_index_insufficient() {
        assert!(!replica_identity_sufficient("index"));
    }
}
