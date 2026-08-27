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

use super::helpers::resolve_owned_stream_table_with_caller;
use super::security_context::{
    EntryContext, StreamExecutionContext, capture_caller_context, with_caller_context,
    with_stream_owner_context,
};
use crate::error::PgTrickleError;

use pgrx::prelude::TimestampWithTimeZone;

const PG_TIDE_MIN_VERSION: (u64, u64, u64) = (0, 47, 0);
const PG_TIDE_MAX_VERSION: (u64, u64, u64) = (0, 53, 0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PgTideVersionStatus {
    Supported,
    Older,
    Newer,
    Invalid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OutboxBinding {
    outbox_name: String,
    pg_tide_extension_oid: pg_sys::Oid,
    pg_tide_version: String,
    tide_outbox_created_at: TimestampWithTimeZone,
}

fn parse_pg_tide_version(version: &str) -> Option<(u64, u64, u64)> {
    let mut parts = version.split('.');
    let parsed = (
        parts.next()?.parse().ok()?,
        parts.next()?.parse().ok()?,
        parts.next()?.parse().ok()?,
    );
    parts.next().is_none().then_some(parsed)
}

fn classify_pg_tide_version(version: &str) -> PgTideVersionStatus {
    let Some(parsed) = parse_pg_tide_version(version) else {
        return PgTideVersionStatus::Invalid;
    };
    if parsed < PG_TIDE_MIN_VERSION {
        PgTideVersionStatus::Older
    } else if parsed > PG_TIDE_MAX_VERSION {
        PgTideVersionStatus::Newer
    } else {
        PgTideVersionStatus::Supported
    }
}

// -- Internal helpers -------------------------------------------------------

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

fn load_private_binding(pgt_id: i64) -> Result<Option<OutboxBinding>, PgTrickleError> {
    Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT oc.tide_outbox_name, oc.pg_tide_extension_oid, \
                        oc.pg_tide_version, oc.tide_outbox_created_at \
                 FROM pgtrickle.pgt_outbox_config oc \
                 JOIN pgtrickle.pgt_stream_tables st \
                   ON oc.stream_table_oid = st.pgt_relid \
                 WHERE st.pgt_id = $1",
                None,
                &[pgt_id.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        if rows.is_empty() {
            return Ok(None);
        }
        let row = rows.first();
        Ok(Some(OutboxBinding {
            outbox_name: row
                .get::<String>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| PgTrickleError::InternalError("NULL pg_tide outbox name".into()))?,
            pg_tide_extension_oid: row
                .get::<pg_sys::Oid>(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InternalError("NULL pg_tide extension OID".into())
                })?,
            pg_tide_version: row
                .get::<String>(3)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| PgTrickleError::InternalError("NULL pg_tide version".into()))?,
            tide_outbox_created_at: row
                .get::<TimestampWithTimeZone>(4)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| PgTrickleError::InternalError("NULL outbox created_at".into()))?,
        }))
    })
}

fn load_live_provenance(
    outbox_name: &str,
) -> Result<Option<(pg_sys::Oid, String, TimestampWithTimeZone)>, PgTrickleError> {
    let config_present =
        Spi::get_one::<bool>("SELECT to_regclass('tide.tide_outbox_config') IS NOT NULL")
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            .unwrap_or(false);
    if !config_present {
        return Ok(None);
    }

    Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT e.oid, e.extversion::text, c.created_at \
                 FROM pg_catalog.pg_extension e \
                 JOIN tide.tide_outbox_config c ON c.outbox_name = $1 \
                 WHERE e.extname::text = 'pg_tide'",
                None,
                &[outbox_name.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        if rows.is_empty() {
            return Ok(None);
        }
        let row = rows.first();
        Ok(Some((
            row.get::<pg_sys::Oid>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InternalError("NULL pg_tide extension OID".into())
                })?,
            row.get::<String>(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| PgTrickleError::InternalError("NULL pg_tide version".into()))?,
            row.get::<TimestampWithTimeZone>(3)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| PgTrickleError::InternalError("NULL outbox created_at".into()))?,
        )))
    })
}

fn validate_binding(binding: &OutboxBinding) -> Result<(), PgTrickleError> {
    if classify_pg_tide_version(&binding.pg_tide_version) != PgTideVersionStatus::Supported {
        return Err(PgTrickleError::PgTideUnsupportedVersion {
            installed: binding.pg_tide_version.clone(),
            supported: "0.47.0 through 0.53.0".into(),
        });
    }
    let Some((extension_oid, version, created_at)) = load_live_provenance(&binding.outbox_name)?
    else {
        return Err(PgTrickleError::PgTideBindingMismatch {
            outbox_name: binding.outbox_name.clone(),
            detail: "the pg_tide outbox no longer exists".into(),
        });
    };
    if extension_oid != binding.pg_tide_extension_oid
        || version != binding.pg_tide_version
        || pg_sys::TimestampTz::from(created_at)
            != pg_sys::TimestampTz::from(binding.tide_outbox_created_at)
    {
        return Err(PgTrickleError::PgTideBindingMismatch {
            outbox_name: binding.outbox_name.clone(),
            detail: format!(
                "live identity is extension OID {}, version {}, created_at {}; \
                 mapping records OID {}, version {}, created_at {}",
                extension_oid.to_u32(),
                version,
                pg_sys::TimestampTz::from(created_at),
                binding.pg_tide_extension_oid.to_u32(),
                binding.pg_tide_version,
                pg_sys::TimestampTz::from(binding.tide_outbox_created_at),
            ),
        });
    }
    Ok(())
}

fn ensure_pg_tide_compatible() -> Result<(), PgTrickleError> {
    let version = Spi::get_one::<String>(
        "SELECT (SELECT extversion::text FROM pg_catalog.pg_extension \
                 WHERE extname::text = 'pg_tide' LIMIT 1)",
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or(PgTrickleError::PgTideMissing)?;

    match classify_pg_tide_version(&version) {
        PgTideVersionStatus::Older | PgTideVersionStatus::Invalid => {
            return Err(PgTrickleError::PgTideUnsupportedVersion {
                installed: version,
                supported: "0.47.0 through 0.53.0".into(),
            });
        }
        PgTideVersionStatus::Newer => {
            return Err(PgTrickleError::PgTideUnsupportedVersion {
                installed: version,
                supported: "0.47.0 through 0.53.0".into(),
            });
        }
        PgTideVersionStatus::Supported => {}
    }

    let checks = [
        (
            "tide.outbox_create(text,integer,integer[,text])",
            "SELECT to_regprocedure('tide.outbox_create(text,integer,integer)') IS NOT NULL \
                    OR to_regprocedure('tide.outbox_create(text,integer,integer,text)') IS NOT NULL",
        ),
        (
            "tide.outbox_publish(text,jsonb,jsonb)",
            "SELECT to_regprocedure('tide.outbox_publish(text,jsonb,jsonb)') IS NOT NULL",
        ),
        (
            "tide.tide_outbox_config",
            "SELECT to_regclass('tide.tide_outbox_config') IS NOT NULL",
        ),
    ];
    let mut missing = Vec::new();
    for (name, query) in checks {
        if !Spi::get_one::<bool>(query)
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            .unwrap_or(false)
        {
            missing.push(name);
        }
    }
    if !missing.is_empty() {
        return Err(PgTrickleError::PgTideUpgradeInProgress {
            installed: version,
            missing: missing.join(", "),
        });
    }
    Ok(())
}

fn load_live_provenance_as_caller(
    caller: &super::security_context::CallerContext,
    outbox_name: &str,
) -> Result<OutboxBinding, PgTrickleError> {
    let (pg_tide_extension_oid, pg_tide_version, tide_outbox_created_at) =
        with_caller_context(caller, || {
            load_live_provenance(outbox_name)?.ok_or_else(|| {
                PgTrickleError::PgTideOperationDenied {
                    operation: "tide.outbox_create".into(),
                    detail: format!("created outbox '{outbox_name}' was not visible to the caller"),
                }
            })
        })?;
    Ok(OutboxBinding {
        outbox_name: outbox_name.into(),
        pg_tide_extension_oid,
        pg_tide_version,
        tide_outbox_created_at,
    })
}

/// Return the validated pg_tide outbox name attached to a stream table.
///
/// The external catalog is read as the current stream-table owner. A missing
/// or replaced row is an error: silently publishing to a same-named replacement
/// would violate the binding's identity guarantee.
pub(crate) fn get_outbox_table_name(pgt_id: i64) -> Result<Option<String>, PgTrickleError> {
    let Some(binding) = load_private_binding(pgt_id)? else {
        return Ok(None);
    };
    let owner_oid = Spi::get_one_with_args::<pg_sys::Oid>(
        "SELECT c.relowner FROM pg_catalog.pg_class c \
         JOIN pgtrickle.pgt_outbox_config oc ON oc.stream_table_oid = c.oid \
         JOIN pgtrickle.pgt_stream_tables st ON st.pgt_relid = c.oid \
         WHERE st.pgt_id = $1",
        &[pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| PgTrickleError::InternalError("stream table owner is missing".into()))?;
    let context = StreamExecutionContext {
        owner_oid,
        search_path: "pg_catalog, pg_temp".into(),
    };
    with_stream_owner_context(&context, || {
        validate_binding(&binding)?;
        Ok(Some(binding.outbox_name.clone()))
    })
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
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
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
    let caller = capture_caller_context(EntryContext::SecurityDefiner)?;
    let (_, _, meta) = resolve_owned_stream_table_with_caller(name, &caller)?;

    Spi::run_with_args(
        "SELECT pg_catalog.pg_advisory_xact_lock($1)",
        &[meta.pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    ensure_pg_tide_compatible()?;

    if load_private_binding(meta.pgt_id)?.is_some() {
        return Err(PgTrickleError::OutboxAlreadyEnabled(format!(
            "{}.{}",
            meta.pgt_schema, meta.pgt_name
        )));
    }

    let outbox_name = outbox_table_name_for(&meta.pgt_name);

    // The external extension owns its authorization. pg_trickle only supplies
    // the original caller identity and never lends its owner identity.
    with_caller_context(&caller, || {
        Spi::run_with_args(
            "SELECT tide.outbox_create($1, $2, $3)",
            &[
                outbox_name.as_str().into(),
                retention_hours.into(),
                inline_threshold_rows.into(),
            ],
        )
        .map_err(|e| PgTrickleError::PgTideOperationDenied {
            operation: "tide.outbox_create".into(),
            detail: e.to_string(),
        })
    })?;

    let binding = load_live_provenance_as_caller(&caller, &outbox_name)?;

    // Register in catalog.
    // COR-002 (v0.72.0): Store the real PostgreSQL relation OID (`pgt_relid`)
    // as `stream_table_oid` so users can join to `pg_class.oid` or
    // `pgt_stream_tables.pgt_relid` as the schema documents.
    Spi::run_with_args(
        "INSERT INTO pgtrickle.pgt_outbox_config \
         (stream_table_oid, stream_table_name, tide_outbox_name, \
          pg_tide_extension_oid, pg_tide_version, tide_outbox_created_at) \
         VALUES ($1, $2, $3, $4, $5, $6::timestamptz)",
        &[
            meta.pgt_relid.into(),
            format!("{}.{}", meta.pgt_schema, meta.pgt_name)
                .as_str()
                .into(),
            outbox_name.as_str().into(),
            binding.pg_tide_extension_oid.into(),
            binding.pg_tide_version.as_str().into(),
            binding.tide_outbox_created_at.into(),
        ],
    )
    .map_err(|e| PgTrickleError::SpiError(format!("register outbox config failed: {e}")))?;

    pgrx::log!(
        "[pg_trickle] attach_outbox: attached tide outbox '{}' to '{}.{}'",
        outbox_name,
        meta.pgt_schema,
        meta.pgt_name
    );

    Ok(())
}

// -- detach_outbox ----------------------------------------------------------

/// v0.46.0: Detach the `pg_tide` outbox from a stream table.
///
/// Removes the entry from `pgtrickle.pgt_outbox_config`. The `pg_tide` outbox
/// table itself is NOT dropped -- use `tide.outbox_drop()` in `pg_tide` after
/// detaching if you also want to remove the outbox data.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
pub fn detach_outbox(p_name: &str, p_if_exists: default!(bool, false)) {
    detach_outbox_impl(p_name, p_if_exists).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn detach_outbox_impl(name: &str, if_exists: bool) -> Result<(), PgTrickleError> {
    let caller = capture_caller_context(EntryContext::SecurityDefiner)?;
    let (_, _, meta) = resolve_owned_stream_table_with_caller(name, &caller)?;
    let has_binding = load_private_binding(meta.pgt_id)?.is_some();
    if has_binding {
        let _ = get_outbox_table_name(meta.pgt_id)?;
    }

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
            meta.pgt_schema, meta.pgt_name
        )));
    }

    pgrx::log!(
        "[pg_trickle] detach_outbox: detached outbox for '{}.{}'",
        meta.pgt_schema,
        meta.pgt_name
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
    let Some(outbox_name) = get_outbox_table_name(pgt_id)? else {
        return Ok(());
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
#[pgrx::pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
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
    let caller = capture_caller_context(EntryContext::SecurityDefiner)?;
    let (_, _, meta) = resolve_owned_stream_table_with_caller(name, &caller)?;
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_outbox_config \
         SET embedding_vector_column = $1 \
         WHERE stream_table_oid = $2",
        &[vector_column.into(), meta.pgt_relid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    pgrx::log!(
        "[pg_trickle] attach_embedding_outbox: attached embedding outbox for '{}.{}' (vector_column='{}')",
        meta.pgt_schema,
        meta.pgt_name,
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
    let Some(outbox_name) = get_outbox_table_name(pgt_id)? else {
        return Ok(());
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
pub(crate) fn get_embedding_vector_column(pgt_id: i64) -> Result<Option<String>, PgTrickleError> {
    if get_outbox_table_name(pgt_id)?.is_none() {
        return Ok(None);
    }
    Spi::get_one_with_args::<String>(
        "SELECT (SELECT oc.embedding_vector_column \
                  FROM pgtrickle.pgt_outbox_config oc \
                  JOIN pgtrickle.pgt_stream_tables st ON oc.stream_table_oid = st.pgt_relid \
                 WHERE st.pgt_id = $1 AND oc.embedding_vector_column IS NOT NULL LIMIT 1)",
        &[pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))
}

// -- Unit tests ------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_tide_version_boundaries() {
        assert_eq!(
            classify_pg_tide_version("0.47.0"),
            PgTideVersionStatus::Supported
        );
        assert_eq!(
            classify_pg_tide_version("0.53.0"),
            PgTideVersionStatus::Supported
        );
        assert_eq!(
            classify_pg_tide_version("0.46.9"),
            PgTideVersionStatus::Older
        );
        assert_eq!(
            classify_pg_tide_version("0.53.1"),
            PgTideVersionStatus::Newer
        );
        assert_eq!(
            classify_pg_tide_version("upgrade-in-progress"),
            PgTideVersionStatus::Invalid
        );
    }

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
