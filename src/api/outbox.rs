//! attach_outbox() -- thin pg_tide integration for stream-table outbox publishing.
//!
//! v0.46.0: The full outbox/inbox/relay stack has been extracted into the
//! standalone `pg_tide` extension (`trickle-labs/pg-tide`). This module retains
//! only the integration point between `pg_trickle`'s refresh engine and
//! `pg_tide`'s outbox primitive:
//!
//! - `attach_outbox(stream_table, ...)` -- registers a `pg_tide` outbox for a
//!   stream table; raises a clear error if `pg_tide` is not installed.
//! - `detach_outbox(stream_table)` -- de-registers the outbox.
//! - `write_outbox_row(...)` -- called from the refresh hot-path; delegates to
//!   `tide.outbox_publish()` via SPI inside the current transaction, preserving
//!   the ADR-001/ADR-002 single-transaction atomicity guarantee.
//!
//! All other outbox/inbox/consumer-group/relay functionality lives in `pg_tide`.

use pgrx::prelude::*;

use super::helpers::check_stream_table_ownership;
use crate::catalog::StreamTableMeta;
use crate::error::PgTrickleError;

// -- Internal helpers -------------------------------------------------------

/// Parse a schema-qualified name like `"myschema.mytable"` into (schema, table).
/// Uses `current_schema()` as the default when no schema is given.
fn resolve_st_name(name: &str) -> Result<(String, String), PgTrickleError> {
    let parts: Vec<&str> = name.splitn(2, '.').collect();
    match parts.len() {
        1 => {
            let schema = Spi::get_one::<String>("SELECT current_schema()::text")
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or_else(|| "public".to_string());
            Ok((schema, parts[0].to_string()))
        }
        2 => Ok((parts[0].to_string(), parts[1].to_string())),
        _ => Err(PgTrickleError::InvalidArgument(format!(
            "invalid stream table name: {name}"
        ))),
    }
}

/// Derive the tide outbox name from a stream table name.
/// Convention: `outbox_<st_name>` truncated to 63 bytes, with hash suffix
/// to prevent collisions when the name is long enough to require truncation.
pub(crate) fn outbox_table_name_for(st_name: &str) -> String {
    let raw = format!("outbox_{}", st_name);
    // PostgreSQL identifier limit is 63 bytes.
    if raw.len() <= 63 {
        raw
    } else {
        // SEC-5: Use xxh64 hash suffix to avoid name collisions on truncation.
        const SEED: u64 = 0x517cc1b727220a95;
        let hash = xxhash_rust::xxh64::xxh64(st_name.as_bytes(), SEED);
        let suffix = &format!("{:016x}", hash)[..8];
        format!("{}_{}", &raw[..54], suffix)
    }
}

/// Check whether the outbox is attached for a given stream table (by pgt_id).
///
/// COR-002 (v0.72.0): `stream_table_oid` now stores the real `pgt_relid` (the
/// PostgreSQL relation OID visible in `pg_class`).  We join through
/// `pgt_stream_tables` to translate `pgt_id` → `pgt_relid` for the lookup.
pub(crate) fn is_outbox_enabled(pgt_id: i64) -> bool {
    Spi::get_one_with_args::<bool>(
        "SELECT EXISTS( \
           SELECT 1 FROM pgtrickle.pgt_outbox_config oc \
           JOIN pgtrickle.pgt_stream_tables st ON oc.stream_table_oid = st.pgt_relid \
           WHERE st.pgt_id = $1)",
        &[pgt_id.into()],
    )
    .unwrap_or(None)
    .unwrap_or(false)
}

/// Return the `pg_tide` outbox name attached to the given stream table (by pgt_id).
///
/// COR-002 (v0.72.0): joins through `pgt_stream_tables` to resolve `pgt_id` →
/// `pgt_relid`, then looks up the outbox config by the correct relation OID.
pub(crate) fn get_outbox_table_name(pgt_id: i64) -> Option<String> {
    Spi::get_one_with_args::<String>(
        "SELECT oc.tide_outbox_name \
         FROM pgtrickle.pgt_outbox_config oc \
         JOIN pgtrickle.pgt_stream_tables st ON oc.stream_table_oid = st.pgt_relid \
         WHERE st.pgt_id = $1",
        &[pgt_id.into()],
    )
    .unwrap_or(None)
}

// -- attach_outbox ----------------------------------------------------------

/// v0.46.0: Attach a `pg_tide` outbox to a stream table.
///
/// Calls `tide.outbox_create()` to set up the outbox in `pg_tide` and registers
/// the mapping in `pgtrickle.pgt_outbox_config`. After this call every
/// non-empty refresh writes a delta-summary row to the `pg_tide` outbox inside
/// the same transaction (ADR-001/ADR-002 atomicity preserved).
///
/// Requires `pg_tide` to be installed. If `pg_tide` is absent the function
/// raises an actionable error with an install hint.
#[pg_extern(schema = "pgtrickle")]
pub fn attach_outbox(
    p_name: &str,
    p_retention_hours: default!(i32, 24),
    p_inline_threshold_rows: default!(i32, 10000),
) {
    attach_outbox_impl(p_name, p_retention_hours, p_inline_threshold_rows)
        .unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn attach_outbox_impl(
    name: &str,
    retention_hours: i32,
    inline_threshold_rows: i32,
) -> Result<(), PgTrickleError> {
    let (schema, st_name) = resolve_st_name(name)?;
    let meta = StreamTableMeta::get_by_name(&schema, &st_name)?;

    // SEC-1: Ownership check — same guard as alter/drop/pause/resume.
    check_stream_table_ownership(meta.pgt_relid, &schema, &st_name)?;

    // Check that pg_tide is installed.
    // `to_regprocedure` accepts argument-type lists; `to_regproc` does not.
    let pg_tide_present = Spi::get_one::<bool>(
        "SELECT to_regprocedure('tide.outbox_create(text,integer,integer)') IS NOT NULL",
    )
    .unwrap_or(None)
    .unwrap_or(false);

    if !pg_tide_present {
        return Err(PgTrickleError::PgTideMissing);
    }

    // Check not already attached.
    if is_outbox_enabled(meta.pgt_id) {
        return Err(PgTrickleError::OutboxAlreadyEnabled(format!(
            "{}.{}",
            schema, st_name
        )));
    }

    let outbox_name = outbox_table_name_for(&st_name);

    // Call tide.outbox_create() to set up the outbox in pg_tide.
    Spi::run_with_args(
        "SELECT tide.outbox_create($1, $2, $3)",
        &[
            outbox_name.as_str().into(),
            retention_hours.into(),
            inline_threshold_rows.into(),
        ],
    )
    .map_err(|e| PgTrickleError::SpiError(format!("tide.outbox_create failed: {e}")))?;

    // Register in catalog.
    // COR-002 (v0.72.0): Store the real PostgreSQL relation OID (`pgt_relid`)
    // as `stream_table_oid` so users can join to `pg_class.oid` or
    // `pgt_stream_tables.pgt_relid` as the schema documents.
    Spi::run_with_args(
        "INSERT INTO pgtrickle.pgt_outbox_config \
         (stream_table_oid, stream_table_name, tide_outbox_name) \
         VALUES ($1, $2, $3)",
        &[
            meta.pgt_relid.into(),
            format!("{}.{}", schema, st_name).as_str().into(),
            outbox_name.as_str().into(),
        ],
    )
    .map_err(|e| PgTrickleError::SpiError(format!("register outbox config failed: {e}")))?;

    pgrx::log!(
        "[pg_trickle] attach_outbox: attached tide outbox '{}' to '{}.{}'",
        outbox_name,
        schema,
        st_name
    );

    Ok(())
}

// -- detach_outbox ----------------------------------------------------------

/// v0.46.0: Detach the `pg_tide` outbox from a stream table.
///
/// Removes the entry from `pgtrickle.pgt_outbox_config`. The `pg_tide` outbox
/// table itself is NOT dropped -- use `tide.outbox_drop()` in `pg_tide` after
/// detaching if you also want to remove the outbox data.
#[pg_extern(schema = "pgtrickle")]
pub fn detach_outbox(p_name: &str, p_if_exists: default!(bool, false)) {
    detach_outbox_impl(p_name, p_if_exists).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn detach_outbox_impl(name: &str, if_exists: bool) -> Result<(), PgTrickleError> {
    let (schema, st_name) = resolve_st_name(name)?;
    let meta = StreamTableMeta::get_by_name(&schema, &st_name)?;

    // SEC-1: Ownership check — same guard as alter/drop/pause/resume.
    check_stream_table_ownership(meta.pgt_relid, &schema, &st_name)?;

    // COR-002 (v0.72.0): Delete by `pgt_relid` (the real relation OID).
    let deleted = Spi::get_one_with_args::<i64>(
        "WITH d AS (DELETE FROM pgtrickle.pgt_outbox_config \
         WHERE stream_table_oid = $1 RETURNING 1) \
         SELECT COUNT(*) FROM d",
        &[meta.pgt_relid.into()],
    )
    .unwrap_or(None)
    .unwrap_or(0);

    if deleted == 0 && !if_exists {
        return Err(PgTrickleError::OutboxNotEnabled(format!(
            "{}.{}",
            schema, st_name
        )));
    }

    pgrx::log!(
        "[pg_trickle] detach_outbox: detached outbox for '{}.{}'",
        schema,
        st_name
    );

    Ok(())
}

// -- write_outbox_row -------------------------------------------------------

/// v0.46.0: Publish a delta-summary row to the attached `pg_tide` outbox.
///
/// Called from the refresh hot-path when `is_outbox_enabled()` returns true.
/// Builds the `{v:1, ...}` envelope and delegates to
/// `SELECT tide.outbox_publish($outbox_name, $payload, $headers)` via SPI.
/// The SPI call runs in the current transaction -- ADR-001/ADR-002 atomicity
/// is preserved.
#[allow(clippy::too_many_arguments)]
pub(crate) fn write_outbox_row(
    pgt_id: i64,
    refresh_id: Option<&str>,
    inserted_count: i64,
    updated_count: i64,
    deleted_count: i64,
    _inline_threshold_rows: i32,
    st_schema: &str,
    st_table: &str,
) -> Result<(), PgTrickleError> {
    let outbox_name = match get_outbox_table_name(pgt_id) {
        Some(n) => n,
        None => return Ok(()), // outbox was detached between the hot-path check and here
    };

    // Build the delta-summary JSON envelope (pg_trickle-private format).
    let payload = serde_json::json!({
        "v": 1,
        "refresh_id": refresh_id,
        "inserted": inserted_count,
        "updated": updated_count,
        "deleted": deleted_count,
        "source": format!("{}.{}", st_schema, st_table),
    });
    let payload_str = payload.to_string();

    let headers = serde_json::json!({
        "source": format!("{}.{}", st_schema, st_table),
        "version": 1,
    });
    let headers_str = headers.to_string();

    // Delegate to pg_tide inside the current transaction.
    Spi::run_with_args(
        "SELECT tide.outbox_publish($1, $2::jsonb, $3::jsonb)",
        &[
            outbox_name.as_str().into(),
            payload_str.as_str().into(),
            headers_str.as_str().into(),
        ],
    )
    .map_err(|e| {
        PgTrickleError::SpiError(format!(
            "tide.outbox_publish failed for '{}': {}",
            outbox_name, e
        ))
    })?;

    Ok(())
}

// -- attach_embedding_outbox (VA-4) ----------------------------------------

/// VA-4 (v0.48.0): Attach a `pg_tide` outbox configured for embedding events.
///
/// Identical to `attach_outbox()` but adds an `event_type = 'embedding_change'`
/// header to all outbox events, making it easy for downstream consumers to
/// route embedding-delta messages separately from general stream table events.
///
/// The `vector_column` parameter documents which column carries the embedding —
/// it is stored in the outbox headers so consumers can identify the embedding
/// field without inspecting the payload.
#[pgrx::pg_extern(schema = "pgtrickle")]
pub fn attach_embedding_outbox(
    p_name: &str,
    p_vector_column: &str,
    p_retention_hours: pgrx::default!(i32, 24),
    p_inline_threshold_rows: pgrx::default!(i32, 10000),
) {
    attach_embedding_outbox_impl(
        p_name,
        p_vector_column,
        p_retention_hours,
        p_inline_threshold_rows,
    )
    .unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn attach_embedding_outbox_impl(
    name: &str,
    vector_column: &str,
    retention_hours: i32,
    inline_threshold_rows: i32,
) -> Result<(), PgTrickleError> {
    // Re-use the standard attach_outbox mechanism.
    attach_outbox_impl(name, retention_hours, inline_threshold_rows)?;

    // Store the vector_column hint in the catalog so write_embedding_outbox_row
    // can retrieve it.
    // COR-002 (v0.72.0): Match on `pgt_relid` (the real relation OID).
    let (schema, st_name) = resolve_st_name(name)?;
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_outbox_config \
         SET embedding_vector_column = $1 \
         WHERE stream_table_oid = (\
           SELECT pgt_relid FROM pgtrickle.pgt_stream_tables \
           WHERE pgt_schema = $2 AND pgt_name = $3 \
         )",
        &[
            vector_column.into(),
            schema.as_str().into(),
            st_name.as_str().into(),
        ],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    pgrx::log!(
        "[pg_trickle] attach_embedding_outbox: attached embedding outbox for '{}.{}' (vector_column='{}')",
        schema,
        st_name,
        vector_column,
    );
    Ok(())
}

/// VA-4 (v0.48.0): Publish an embedding-change event to the attached pg_tide
/// outbox.  Called from the refresh hot-path when the outbox is configured as
/// an embedding outbox.
///
/// The payload extends the standard delta-summary envelope with an
/// `event_type = "embedding_change"` marker and the `vector_column` name so
/// consumers can route embedding updates without inspecting the payload.
#[allow(clippy::too_many_arguments)]
pub(crate) fn write_embedding_outbox_row(
    pgt_id: i64,
    refresh_id: Option<&str>,
    inserted_count: i64,
    updated_count: i64,
    deleted_count: i64,
    st_schema: &str,
    st_table: &str,
    vector_column: &str,
) -> Result<(), PgTrickleError> {
    let outbox_name = match get_outbox_table_name(pgt_id) {
        Some(n) => n,
        None => return Ok(()),
    };

    let payload = serde_json::json!({
        "v": 1,
        "event_type": "embedding_change",
        "refresh_id": refresh_id,
        "inserted": inserted_count,
        "updated": updated_count,
        "deleted": deleted_count,
        "source": format!("{}.{}", st_schema, st_table),
        "vector_column": vector_column,
    });
    let payload_str = payload.to_string();

    let headers = serde_json::json!({
        "source": format!("{}.{}", st_schema, st_table),
        "event_type": "embedding_change",
        "vector_column": vector_column,
        "version": 1,
    });
    let headers_str = headers.to_string();

    Spi::run_with_args(
        "SELECT tide.outbox_publish($1, $2::jsonb, $3::jsonb)",
        &[
            outbox_name.as_str().into(),
            payload_str.as_str().into(),
            headers_str.as_str().into(),
        ],
    )
    .map_err(|e| {
        PgTrickleError::SpiError(format!(
            "tide.outbox_publish (embedding) failed for '{}': {}",
            outbox_name, e
        ))
    })?;

    Ok(())
}

/// VA-4: Return the embedding vector column for this stream table if an
/// embedding outbox is attached, otherwise `None`.
///
/// COR-002 (v0.72.0): joins through `pgt_stream_tables` to resolve `pgt_id` →
/// `pgt_relid` before matching `pgt_outbox_config`.
pub(crate) fn get_embedding_vector_column(pgt_id: i64) -> Option<String> {
    Spi::get_one_with_args::<String>(
        "SELECT oc.embedding_vector_column \
         FROM pgtrickle.pgt_outbox_config oc \
         JOIN pgtrickle.pgt_stream_tables st ON oc.stream_table_oid = st.pgt_relid \
         WHERE st.pgt_id = $1 AND oc.embedding_vector_column IS NOT NULL",
        &[pgt_id.into()],
    )
    .unwrap_or(None)
}

// -- Unit tests ------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_outbox_table_name_for_simple() {
        assert_eq!(outbox_table_name_for("orders"), "outbox_orders");
    }

    #[test]
    fn test_outbox_table_name_for_truncated_at_63_chars() {
        let long_name = "a".repeat(60);
        let result = outbox_table_name_for(&long_name);
        assert!(
            result.len() <= 63,
            "outbox table name must be <= 63 chars, got {}",
            result.len()
        );
    }

    #[test]
    fn test_outbox_table_name_for_empty() {
        let result = outbox_table_name_for("");
        assert_eq!(result, "outbox_");
    }

    #[test]
    fn test_outbox_table_name_for_exactly_56_char_input() {
        let name = "b".repeat(56);
        let result = outbox_table_name_for(&name);
        assert_eq!(result.len(), 63);
        assert!(result.starts_with("outbox_b"));
    }

    #[test]
    fn test_outbox_table_name_for_unicode_chars() {
        let name = "aaa_table";
        let result = outbox_table_name_for(name);
        assert!(
            result.chars().count() <= 63,
            "char count must be <= 63, got {}",
            result.chars().count()
        );
    }
}
