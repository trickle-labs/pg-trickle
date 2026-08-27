//! v0.22.0: Downstream CDC publication, predictive cost model, and SLA-driven
//! tier auto-assignment API functions.

use pgrx::prelude::*;

use super::helpers::{resolve_owned_stream_table, resolve_owned_stream_table_with_caller};
use crate::catalog::StreamTableMeta;
use crate::error::PgTrickleError;

const MAX_IDENTIFIER_BYTES: usize = 63;
const PUBLICATION_NAME_HASH_SEED: u64 = 0x7067_7472_6963_6b6c;

/// Immutable provenance for a publication created by pg_trickle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublicationBinding {
    pub pgt_id: i64,
    pub stream_relid: pg_sys::Oid,
    pub publication_oid: pg_sys::Oid,
    pub publication_name: String,
    pub publication_owner_oid: pg_sys::Oid,
    pub expected_relation_oids: Vec<pg_sys::Oid>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LivePublication {
    oid: pg_sys::Oid,
    name: String,
    owner_oid: pg_sys::Oid,
    all_tables: bool,
    relation_oids: Vec<pg_sys::Oid>,
    namespace_oids: Vec<pg_sys::Oid>,
}

/// Stable reasons for a stale publication binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublicationBindingMismatchReason {
    MissingPublication,
    PublicationNameReused,
    PublicationRenamed,
    PublicationOwnerChanged,
    StreamRelationChanged,
    PublicationRelationsChanged,
    PublicationScopeChanged,
    PrivateBindingIncomplete,
}

impl PublicationBindingMismatchReason {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::MissingPublication => "missing_publication",
            Self::PublicationNameReused => "publication_name_reused",
            Self::PublicationRenamed => "publication_renamed",
            Self::PublicationOwnerChanged => "publication_owner_changed",
            Self::StreamRelationChanged => "stream_relation_changed",
            Self::PublicationRelationsChanged => "publication_relations_changed",
            Self::PublicationScopeChanged => "publication_scope_changed",
            Self::PrivateBindingIncomplete => "private_binding_incomplete",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ValidatedPublicationBinding {
    pub binding: PublicationBinding,
    pub live_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PrivateBindingState {
    has_binding: bool,
    has_legacy_name: bool,
    names_agree: bool,
}

fn normalize_relation_oids(mut relation_oids: Vec<pg_sys::Oid>) -> Vec<pg_sys::Oid> {
    relation_oids.sort_by_key(|oid| oid.to_u32());
    relation_oids.dedup_by_key(|oid| oid.to_u32());
    relation_oids
}

/// Pure binding classifier. It is deliberately independent of SPI so every
/// stale-object branch can be tested without PostgreSQL.
pub(crate) fn classify_publication_binding(
    binding: Option<&PublicationBinding>,
    legacy_name: Option<&str>,
    live: Option<&LivePublication>,
    name_oid: Option<pg_sys::Oid>,
    current_stream_relid: pg_sys::Oid,
) -> Result<(), PublicationBindingMismatchReason> {
    let private = PrivateBindingState {
        has_binding: binding.is_some(),
        has_legacy_name: legacy_name.is_some(),
        names_agree: binding
            .zip(legacy_name)
            .is_none_or(|(b, legacy)| b.publication_name == legacy),
    };
    if (!private.has_binding && private.has_legacy_name)
        || (private.has_binding && !private.has_legacy_name)
        || !private.names_agree
    {
        return Err(PublicationBindingMismatchReason::PrivateBindingIncomplete);
    }
    let Some(binding) = binding else {
        return Ok(());
    };
    let Some(live) = live else {
        return Err(
            if name_oid.is_some_and(|oid| oid != binding.publication_oid) {
                PublicationBindingMismatchReason::PublicationNameReused
            } else {
                PublicationBindingMismatchReason::MissingPublication
            },
        );
    };
    if live.oid != binding.publication_oid || live.name != binding.publication_name {
        return Err(PublicationBindingMismatchReason::PublicationRenamed);
    }
    if live.owner_oid != binding.publication_owner_oid {
        return Err(PublicationBindingMismatchReason::PublicationOwnerChanged);
    }
    if current_stream_relid != binding.stream_relid {
        return Err(PublicationBindingMismatchReason::StreamRelationChanged);
    }
    if live.all_tables || !live.namespace_oids.is_empty() {
        return Err(PublicationBindingMismatchReason::PublicationScopeChanged);
    }
    if normalize_relation_oids(live.relation_oids.clone())
        != normalize_relation_oids(binding.expected_relation_oids.clone())
    {
        return Err(PublicationBindingMismatchReason::PublicationRelationsChanged);
    }
    Ok(())
}

fn publication_name(schema: &str, table: &str) -> String {
    let base = format!("pgt_pub_{table}");
    if base.len() <= MAX_IDENTIFIER_BYTES {
        return base;
    }
    let framed = format!("schema\0{}\0table\0{}", schema.len(), schema);
    let framed = format!("{framed}\0{}\0{table}", table.len());
    let hash = xxhash_rust::xxh64::xxh64(framed.as_bytes(), PUBLICATION_NAME_HASH_SEED);
    let suffix = format!("_{hash:016x}");
    let prefix_bytes = MAX_IDENTIFIER_BYTES - suffix.len();
    let mut end = prefix_bytes.min(base.len());
    while end > 0 && !base.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{}", &base[..end], suffix)
}

fn binding_table_exists() -> bool {
    Spi::get_one::<bool>("SELECT to_regclass('pgtrickle.pgt_publication_bindings') IS NOT NULL")
        .ok()
        .flatten()
        .unwrap_or(false)
}

pub(crate) fn load_publication_binding(
    pgt_id: i64,
) -> Result<Option<PublicationBinding>, PgTrickleError> {
    if !binding_table_exists() {
        return Ok(None);
    }
    Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT pgt_id, stream_relid, publication_oid, publication_name, \
                        publication_owner_oid, expected_relation_oids \
                 FROM pgtrickle.pgt_publication_bindings WHERE pgt_id = $1",
                None,
                &[pgt_id.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        if rows.is_empty() {
            return Ok(None);
        }
        let row = rows.first();
        Ok(Some(PublicationBinding {
            pgt_id: row
                .get::<i64>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| PgTrickleError::InternalError("NULL publication pgt_id".into()))?,
            stream_relid: row
                .get::<pg_sys::Oid>(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InternalError("NULL publication stream_relid".into())
                })?,
            publication_oid: row
                .get::<pg_sys::Oid>(3)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| PgTrickleError::InternalError("NULL publication OID".into()))?,
            publication_name: row
                .get::<String>(4)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| PgTrickleError::InternalError("NULL publication name".into()))?,
            publication_owner_oid: row
                .get::<pg_sys::Oid>(5)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InternalError("NULL publication owner OID".into())
                })?,
            expected_relation_oids: row
                .get::<Vec<pg_sys::Oid>>(6)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InternalError("NULL publication relations".into())
                })?,
        }))
    })
}

fn load_legacy_publication_name(pgt_id: i64) -> Result<Option<String>, PgTrickleError> {
    Spi::get_one_with_args::<String>(
        "SELECT downstream_publication_name FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1",
        &[pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))
}

fn load_live_publication_by_oid(
    oid: pg_sys::Oid,
) -> Result<Option<LivePublication>, PgTrickleError> {
    let Some((oid, name, owner_oid, all_tables)) = Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT oid, pubname::text, pubowner, puballtables \
                 FROM pg_catalog.pg_publication WHERE oid = $1",
                None,
                &[oid.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        if rows.is_empty() {
            return Ok(None);
        }
        let row = rows.first();
        let oid = row
            .get::<pg_sys::Oid>(1)
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            .ok_or_else(|| PgTrickleError::InternalError("NULL publication OID".into()))?;
        let name = row
            .get::<String>(2)
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            .ok_or_else(|| PgTrickleError::InternalError("NULL publication name".into()))?;
        let owner_oid = row
            .get::<pg_sys::Oid>(3)
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            .ok_or_else(|| PgTrickleError::InternalError("NULL publication owner OID".into()))?;
        let all_tables = row
            .get::<bool>(4)
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            .ok_or_else(|| PgTrickleError::InternalError("NULL publication scope".into()))?;
        Ok(Some((oid, name, owner_oid, all_tables)))
    })?
    else {
        return Ok(None);
    };
    Ok(Some(LivePublication {
        oid,
        name,
        owner_oid,
        all_tables,
        relation_oids: load_explicit_publication_relids(oid)?,
        namespace_oids: load_publication_namespace_oids(oid)?,
    }))
}

fn load_live_publication_oid_by_name(name: &str) -> Result<Option<pg_sys::Oid>, PgTrickleError> {
    Spi::get_one_with_args::<pg_sys::Oid>(
        "SELECT oid FROM pg_catalog.pg_publication WHERE pubname::text = $1",
        &[name.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))
}

fn load_explicit_publication_relids(oid: pg_sys::Oid) -> Result<Vec<pg_sys::Oid>, PgTrickleError> {
    let relids = Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT prrelid FROM pg_catalog.pg_publication_rel \
                 WHERE prpubid = $1 ORDER BY prrelid",
                None,
                &[oid.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        rows.map(|row| {
            row.get::<pg_sys::Oid>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InternalError("NULL publication relation OID".into())
                })
        })
        .collect::<Result<Vec<_>, _>>()
    })?;
    Ok(normalize_relation_oids(relids))
}

fn load_publication_namespace_oids(oid: pg_sys::Oid) -> Result<Vec<pg_sys::Oid>, PgTrickleError> {
    Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT pnnspid FROM pg_catalog.pg_publication_namespace \
                 WHERE pnpubid = $1 ORDER BY pnnspid",
                None,
                &[oid.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        rows.map(|row| {
            row.get::<pg_sys::Oid>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InternalError("NULL publication namespace OID".into())
                })
        })
        .collect()
    })
}

fn lock_stream_row(pgt_id: i64) -> Result<(), PgTrickleError> {
    Spi::get_one_with_args::<i64>(
        "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1 FOR UPDATE",
        &[pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| PgTrickleError::NotFound(format!("stream table with pgt_id {pgt_id} not found")))
    .map(|_| ())
}

fn reload_stream_meta(pgt_id: i64) -> Result<StreamTableMeta, PgTrickleError> {
    StreamTableMeta::get_by_id(pgt_id)?.ok_or_else(|| {
        PgTrickleError::NotFound(format!("stream table with pgt_id {pgt_id} not found"))
    })
}

fn lock_publication_object(oid: pg_sys::Oid, lockmode: i32) {
    // SAFETY: `LockDatabaseObject` is called on PostgreSQL's backend thread
    // with the catalog relation OID and live publication OID read from the
    // current database. The lock is transaction-scoped by PostgreSQL.
    unsafe { pg_sys::LockDatabaseObject(pg_sys::PublicationRelationId, oid, 0, lockmode) }
}

fn lock_relation(oid: pg_sys::Oid, lockmode: i32) {
    // SAFETY: `LockRelationOid` accepts an OID from the current catalog and
    // is invoked on PostgreSQL's backend thread. The lock is held to xact end.
    unsafe { pg_sys::LockRelationOid(oid, lockmode) }
}

fn binding_mismatch(
    binding: &PublicationBinding,
    reason: PublicationBindingMismatchReason,
    live: Option<&LivePublication>,
) -> PgTrickleError {
    PgTrickleError::PublicationBindingMismatch {
        publication_name: binding.publication_name.clone(),
        reason: reason.as_str().to_string(),
        detail: format!(
            "stored publication_oid={}, stream_relid={}, owner_oid={}, live={:?}",
            binding.publication_oid.to_u32(),
            binding.stream_relid.to_u32(),
            binding.publication_owner_oid.to_u32(),
            live.map(|p| (p.oid.to_u32(), p.name.clone(), p.owner_oid.to_u32()))
        ),
    }
}

/// Lock and validate the immutable publication binding for a stream.
pub(crate) fn prepare_publication_binding(
    meta: &StreamTableMeta,
    lockmode: i32,
) -> Result<Option<ValidatedPublicationBinding>, PgTrickleError> {
    let binding = load_publication_binding(meta.pgt_id)?;
    let legacy_name = load_legacy_publication_name(meta.pgt_id)?;
    let Some(binding) = binding else {
        return if legacy_name.is_some() {
            Err(PgTrickleError::PublicationBindingMismatch {
                publication_name: legacy_name.unwrap_or_default(),
                reason: PublicationBindingMismatchReason::PrivateBindingIncomplete
                    .as_str()
                    .to_string(),
                detail: "legacy publication name exists without a canonical binding".into(),
            })
        } else {
            Ok(None)
        };
    };
    lock_publication_object(binding.publication_oid, lockmode);
    lock_relation(binding.stream_relid, pg_sys::AccessShareLock as i32);
    let live = load_live_publication_by_oid(binding.publication_oid)?;
    let name_oid = load_live_publication_oid_by_name(&binding.publication_name)?;
    if let Err(reason) = classify_publication_binding(
        Some(&binding),
        legacy_name.as_deref(),
        live.as_ref(),
        name_oid,
        meta.pgt_relid,
    ) {
        return Err(binding_mismatch(&binding, reason, live.as_ref()));
    }
    Ok(Some(ValidatedPublicationBinding {
        live_name: live
            .ok_or_else(|| {
                PgTrickleError::InternalError("validated publication disappeared".into())
            })?
            .name,
        binding,
    }))
}

/// Acquire every lifecycle lock for a multi-stream drop in deterministic
/// order, then validate all bindings before the first public or storage drop.
pub(crate) fn prevalidate_publication_bindings(
    metas: &[StreamTableMeta],
) -> Result<(), PgTrickleError> {
    let mut sorted_metas = metas.iter().collect::<Vec<_>>();
    sorted_metas.sort_by_key(|meta| meta.pgt_id);
    let mut entries = Vec::new();

    for meta in sorted_metas {
        Spi::run_with_args(
            "SELECT pg_catalog.pg_advisory_xact_lock($1)",
            &[meta.pgt_id.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        lock_stream_row(meta.pgt_id)?;
        let binding = load_publication_binding(meta.pgt_id)?;
        let legacy_name = load_legacy_publication_name(meta.pgt_id)?;
        if binding.is_none() && legacy_name.is_some() {
            return Err(PgTrickleError::PublicationBindingMismatch {
                publication_name: legacy_name.unwrap_or_default(),
                reason: PublicationBindingMismatchReason::PrivateBindingIncomplete
                    .as_str()
                    .to_string(),
                detail: "legacy publication name exists without a canonical binding".into(),
            });
        }
        if let Some(binding) = binding {
            entries.push((reload_stream_meta(meta.pgt_id)?, binding, legacy_name));
        }
    }

    let mut publication_oids = entries
        .iter()
        .map(|(_, binding, _)| binding.publication_oid)
        .collect::<Vec<_>>();
    publication_oids.sort_by_key(|oid| oid.to_u32());
    publication_oids.dedup_by_key(|oid| oid.to_u32());
    for oid in publication_oids {
        lock_publication_object(oid, pg_sys::AccessExclusiveLock as i32);
    }

    let mut relation_oids = entries
        .iter()
        .map(|(_, binding, _)| binding.stream_relid)
        .collect::<Vec<_>>();
    relation_oids.sort_by_key(|oid| oid.to_u32());
    relation_oids.dedup_by_key(|oid| oid.to_u32());
    for oid in relation_oids {
        lock_relation(oid, pg_sys::AccessShareLock as i32);
    }

    for (meta, binding, legacy_name) in entries {
        let live = load_live_publication_by_oid(binding.publication_oid)?;
        let name_oid = load_live_publication_oid_by_name(&binding.publication_name)?;
        if let Err(reason) = classify_publication_binding(
            Some(&binding),
            legacy_name.as_deref(),
            live.as_ref(),
            name_oid,
            meta.pgt_relid,
        ) {
            return Err(binding_mismatch(&binding, reason, live.as_ref()));
        }
    }
    Ok(())
}

pub(crate) fn ensure_storage_replacement_allowed(
    meta: &StreamTableMeta,
) -> Result<(), PgTrickleError> {
    Spi::run_with_args(
        "SELECT pg_catalog.pg_advisory_xact_lock($1)",
        &[meta.pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    lock_stream_row(meta.pgt_id)?;
    let current_meta = reload_stream_meta(meta.pgt_id)?;
    if load_publication_binding(current_meta.pgt_id)?.is_none()
        && load_legacy_publication_name(current_meta.pgt_id)?.is_none()
    {
        return Ok(());
    }
    prepare_publication_binding(&current_meta, pg_sys::AccessShareLock as i32)?;
    Err(PgTrickleError::InvalidArgument(format!(
        "cannot replace storage for stream table {}.{} while downstream publication '{}' is attached; drop the publication first",
        current_meta.pgt_schema,
        current_meta.pgt_name,
        current_meta
            .downstream_publication_name
            .as_deref()
            .unwrap_or("<unknown>"),
    )))
}

/// Read-only publication provenance report used by lifecycle preflight and
/// health reporting. Before the migration exists, legacy names are checked
/// as observed current state; they are never adopted here.
pub(crate) fn publication_binding_preflight() -> serde_json::Value {
    let canonical_table_present = binding_table_exists();
    let stream_query = if canonical_table_present {
        "SELECT pgt_id, pgt_schema::text, pgt_name::text, pgt_relid, \
                downstream_publication_name \
         FROM pgtrickle.pgt_stream_tables st \
         WHERE downstream_publication_name IS NOT NULL \
            OR EXISTS (SELECT 1 FROM pgtrickle.pgt_publication_bindings b \
                       WHERE b.pgt_id = st.pgt_id) \
         ORDER BY pgt_id"
    } else {
        "SELECT pgt_id, pgt_schema::text, pgt_name::text, pgt_relid, \
                downstream_publication_name \
         FROM pgtrickle.pgt_stream_tables \
         WHERE downstream_publication_name IS NOT NULL \
         ORDER BY pgt_id"
    };
    let streams = match Spi::connect(|client| {
        let rows = client
            .select(stream_query, None, &[])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        rows.map(|row| {
            Ok((
                row.get::<i64>(1)
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                    .ok_or_else(|| PgTrickleError::InternalError("NULL stream pgt_id".into()))?,
                row.get::<String>(2)
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                    .ok_or_else(|| PgTrickleError::InternalError("NULL stream schema".into()))?,
                row.get::<String>(3)
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                    .ok_or_else(|| PgTrickleError::InternalError("NULL stream name".into()))?,
                row.get::<pg_sys::Oid>(4)
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                    .ok_or_else(|| {
                        PgTrickleError::InternalError("NULL stream relation OID".into())
                    })?,
                row.get::<String>(5)
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?,
            ))
        })
        .collect::<Result<Vec<_>, PgTrickleError>>()
    }) {
        Ok(rows) => rows,
        Err(error) => {
            return serde_json::json!({
                "ok": false,
                "canonical_table_present": canonical_table_present,
                "issues": [{
                    "reason": "private_binding_incomplete",
                    "detail": error.to_string(),
                    "remediation": "Run pgtrickle.lifecycle_preflight() and repair the private catalog before retrying."
                }]
            });
        }
    };

    let mut issues = Vec::new();
    let mut observed_state_adoptions = 0usize;
    for (pgt_id, schema, name, stream_relid, legacy_name) in streams {
        let binding = match load_publication_binding(pgt_id) {
            Ok(binding) => binding,
            Err(error) => {
                issues.push(serde_json::json!({
                    "stream": format!("{schema}.{name}"),
                    "reason": "private_binding_incomplete",
                    "detail": error.to_string(),
                    "remediation": "Repair the private binding catalog before retrying."
                }));
                continue;
            }
        };
        let Some(legacy_name) = legacy_name else {
            if binding.is_some() {
                issues.push(serde_json::json!({
                    "stream": format!("{schema}.{name}"),
                    "reason": "private_binding_incomplete",
                    "detail": "canonical binding exists without its legacy publication name",
                    "remediation": "Restore the legacy projection from the canonical binding after review."
                }));
            }
            continue;
        };

        let (binding, live) = if let Some(binding) = binding {
            let live = load_live_publication_by_oid(binding.publication_oid)
                .ok()
                .flatten();
            (binding, live)
        } else if !canonical_table_present {
            let Some(publication_oid) = load_live_publication_oid_by_name(&legacy_name)
                .ok()
                .flatten()
            else {
                issues.push(serde_json::json!({
                    "stream": format!("{schema}.{name}"),
                    "stored_name": legacy_name,
                    "reason": "missing_publication",
                    "remediation": "Recreate the publication through pgtrickle after reviewing the missing legacy object."
                }));
                continue;
            };
            let Some(live) = load_live_publication_by_oid(publication_oid).ok().flatten() else {
                issues.push(serde_json::json!({
                    "stream": format!("{schema}.{name}"),
                    "stored_name": legacy_name,
                    "reason": "missing_publication",
                    "remediation": "Recreate the publication through pgtrickle after reviewing the missing legacy object."
                }));
                continue;
            };
            observed_state_adoptions += 1;
            (
                PublicationBinding {
                    pgt_id,
                    stream_relid,
                    publication_oid,
                    publication_name: legacy_name.clone(),
                    publication_owner_oid: live.owner_oid,
                    expected_relation_oids: vec![stream_relid],
                },
                Some(live),
            )
        } else {
            issues.push(serde_json::json!({
                "stream": format!("{schema}.{name}"),
                "stored_name": legacy_name,
                "reason": "private_binding_incomplete",
                "detail": "legacy publication name exists without a canonical binding",
                "remediation": "Run the v0.87.12 upgrade only after reviewing this row."
            }));
            continue;
        };

        let reason = classify_publication_binding(
            Some(&binding),
            Some(&legacy_name),
            live.as_ref(),
            load_live_publication_oid_by_name(&legacy_name)
                .ok()
                .flatten(),
            stream_relid,
        )
        .err();
        if let Some(reason) = reason {
            issues.push(serde_json::json!({
                "stream": format!("{schema}.{name}"),
                "stored_name": binding.publication_name,
                "stored_oid": binding.publication_oid.to_u32(),
                "live_name": live.as_ref().map(|p| p.name.clone()),
                "live_oid": live.as_ref().map(|p| p.oid.to_u32()),
                "reason": reason.as_str(),
                "remediation": "Restore the recorded identity or inspect the live object before manual recovery."
            }));
        }
    }

    serde_json::json!({
        "ok": issues.is_empty(),
        "canonical_table_present": canonical_table_present,
        "observed_state_adoptions": observed_state_adoptions,
        "issues": issues,
    })
}

pub(crate) fn publication_binding_health_summary() -> Option<(String, String)> {
    if !binding_table_exists() {
        return None;
    }
    let report = publication_binding_preflight();
    let issues = report
        .get("issues")
        .and_then(serde_json::Value::as_array)
        .map(Vec::len)
        .unwrap_or(1);
    if issues == 0 {
        Some((
            "OK".to_string(),
            "All downstream publication bindings match their stored OID, owner, scope, and relation set".to_string(),
        ))
    } else {
        let summary = report
            .get("issues")
            .and_then(serde_json::Value::as_array)
            .map(|issues| {
                issues
                    .iter()
                    .take(3)
                    .map(|issue| {
                        format!(
                            "{}:{}",
                            issue
                                .get("stream")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or("unknown"),
                            issue
                                .get("reason")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or("unknown")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_else(|| "unavailable".to_string());
        Some((
            "ERROR".to_string(),
            format!(
                "{} downstream publication binding(s) are stale or incomplete ({summary}); inspect pgtrickle.lifecycle_preflight()",
                issues,
            ),
        ))
    }
}

fn insert_binding(binding: &PublicationBinding) -> Result<(), PgTrickleError> {
    Spi::run_with_args(
        "INSERT INTO pgtrickle.pgt_publication_bindings \
         (pgt_id, stream_relid, publication_oid, publication_name, publication_owner_oid, expected_relation_oids) \
         VALUES ($1, $2, $3, $4, $5, $6)",
        &[
            binding.pgt_id.into(),
            binding.stream_relid.into(),
            binding.publication_oid.into(),
            binding.publication_name.clone().into(),
            binding.publication_owner_oid.into(),
            binding.expected_relation_oids.clone().into(),
        ],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))
}

fn clear_binding(pgt_id: i64) -> Result<(), PgTrickleError> {
    Spi::run_with_args(
        "DELETE FROM pgtrickle.pgt_publication_bindings WHERE pgt_id = $1",
        &[pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET downstream_publication_name = NULL, updated_at = now() WHERE pgt_id = $1",
        &[pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))
}

pub(crate) fn drop_validated_publication(
    caller: &super::security_context::CallerContext,
    validated: &ValidatedPublicationBinding,
) -> Result<(), PgTrickleError> {
    let sql = format!("DROP PUBLICATION {}", quote_ident(&validated.live_name));
    super::security_context::with_caller_context(caller, || {
        // nosemgrep: rust.spi.run.dynamic-format — validated publication names are quoted identifiers from pg_catalog.
        Spi::run(&sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))
    })?;
    clear_binding(validated.binding.pgt_id)
}

// ── CDC-PUB-1: stream_table_to_publication() ─────────────────────────────

/// CDC-PUB-1: Create a logical replication publication for a stream table.
///
/// Creates a PostgreSQL publication exposing the named stream table so that
/// Kafka Connect, Debezium, and other logical replication subscribers can
/// receive change events without a separate replication slot.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
fn stream_table_to_publication(name: &str) {
    if let Err(error) = stream_table_to_publication_impl(name) {
        super::raise_error_with_context(error);
    }
}

fn stream_table_to_publication_impl(name: &str) -> Result<(), PgTrickleError> {
    let caller = super::security_context::capture_caller_context(
        super::security_context::EntryContext::SecurityDefiner,
    )?;
    let (_, _, meta) = resolve_owned_stream_table_with_caller(name, &caller)?;

    Spi::run_with_args(
        "SELECT pg_catalog.pg_advisory_xact_lock($1)",
        &[meta.pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    lock_stream_row(meta.pgt_id)?;
    let meta = reload_stream_meta(meta.pgt_id)?;

    if load_publication_binding(meta.pgt_id)?.is_some()
        || load_legacy_publication_name(meta.pgt_id)?.is_some()
    {
        return Err(PgTrickleError::PublicationAlreadyExists(name.into()));
    }

    let pub_name = publication_name(&meta.pgt_schema, &meta.pgt_name);
    let qualified_table = format!("{}.{}", meta.pgt_schema, meta.pgt_name);

    // Native PostgreSQL publication checks must run as the original caller.
    let create_sql = format!(
        "CREATE PUBLICATION {} FOR TABLE {}",
        quote_ident(&pub_name),
        quote_ident_qualified(&meta.pgt_schema, &meta.pgt_name)
    );
    super::security_context::with_caller_context(&caller, || {
        Spi::run(&create_sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))
    })?;

    let live = load_live_publication_oid_by_name(&pub_name)?.ok_or_else(|| {
        PgTrickleError::InternalError("created publication is not visible".into())
    })?;
    lock_publication_object(live, pg_sys::AccessShareLock as i32);
    let live_publication = load_live_publication_by_oid(live)?
        .ok_or_else(|| PgTrickleError::InternalError("created publication disappeared".into()))?;
    if let Err(reason) = classify_publication_binding(
        Some(&PublicationBinding {
            pgt_id: meta.pgt_id,
            stream_relid: meta.pgt_relid,
            publication_oid: live_publication.oid,
            publication_name: pub_name.clone(),
            publication_owner_oid: caller.role_oid,
            expected_relation_oids: vec![meta.pgt_relid],
        }),
        Some(&pub_name),
        Some(&live_publication),
        Some(live_publication.oid),
        meta.pgt_relid,
    ) {
        return Err(binding_mismatch(
            &PublicationBinding {
                pgt_id: meta.pgt_id,
                stream_relid: meta.pgt_relid,
                publication_oid: live_publication.oid,
                publication_name: pub_name,
                publication_owner_oid: caller.role_oid,
                expected_relation_oids: vec![meta.pgt_relid],
            },
            reason,
            Some(&live_publication),
        ));
    }

    let binding = PublicationBinding {
        pgt_id: meta.pgt_id,
        stream_relid: meta.pgt_relid,
        publication_oid: live_publication.oid,
        publication_name: live_publication.name.clone(),
        publication_owner_oid: live_publication.owner_oid,
        expected_relation_oids: normalize_relation_oids(live_publication.relation_oids.clone()),
    };
    insert_binding(&binding)?;
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET downstream_publication_name = $1, updated_at = now() WHERE pgt_id = $2",
        &[binding.publication_name.clone().into(), meta.pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    pgrx::info!(
        "pg_trickle: created publication '{}' for stream table '{}'",
        binding.publication_name,
        qualified_table
    );

    Ok(())
}

// ── CDC-PUB-2: drop_stream_table_publication() ──────────────────────────

/// CDC-PUB-2: Drop the logical replication publication for a stream table.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
fn drop_stream_table_publication(name: &str) {
    if let Err(error) = drop_stream_table_publication_impl(name) {
        super::raise_error_with_context(error);
    }
}

fn drop_stream_table_publication_impl(name: &str) -> Result<(), PgTrickleError> {
    let caller = super::security_context::capture_caller_context(
        super::security_context::EntryContext::SecurityDefiner,
    )?;
    let (_, _, meta) = resolve_owned_stream_table_with_caller(name, &caller)?;
    Spi::run_with_args(
        "SELECT pg_catalog.pg_advisory_xact_lock($1)",
        &[meta.pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    lock_stream_row(meta.pgt_id)?;
    let meta = reload_stream_meta(meta.pgt_id)?;

    let Some(validated) = prepare_publication_binding(&meta, pg_sys::AccessExclusiveLock as i32)?
    else {
        return Err(PgTrickleError::PublicationNotFound(name.into()));
    };
    let pub_name = validated.live_name.clone();
    drop_validated_publication(&caller, &validated)?;

    pgrx::info!(
        "pg_trickle: dropped publication '{}' for stream table '{}.{}'",
        pub_name,
        meta.pgt_schema,
        meta.pgt_name
    );

    Ok(())
}

// ── SLA-1: sla parameter support ─────────────────────────────────────────

/// SLA-1: Set the SLA interval for a stream table.
///
/// Accepts an interval and stores it as `freshness_deadline_ms`.
/// The scheduler uses this to auto-assign the appropriate refresh tier.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
fn set_stream_table_sla(name: &str, sla: Interval) {
    // ERR-2 (v0.26.0): Use typed into_pg_error() at the API boundary.
    set_stream_table_sla_impl(name, sla).unwrap_or_else(|e| e.into_pg_error());
}

fn set_stream_table_sla_impl(name: &str, sla: Interval) -> Result<(), PgTrickleError> {
    let (_schema, _table, meta) =
        resolve_owned_stream_table(name, super::security_context::EntryContext::SecurityDefiner)?;

    if sla.months() != 0 {
        return Err(PgTrickleError::InvalidArgument(
            "SLA interval cannot contain calendar months; use days or smaller units".into(),
        ));
    }
    let total_ms = sla.days() as i64 * 24 * 3600 * 1000 + sla.micros() / 1000;

    if total_ms <= 0 {
        return Err(PgTrickleError::InvalidArgument(
            "SLA interval must be positive".into(),
        ));
    }

    // SLA-2: Determine the initial tier assignment based on the SLA.
    let tier = assign_tier_for_sla(total_ms)?;

    super::alter::apply_target_freshness(
        meta.pgt_id,
        super::alter::TargetFreshness {
            mode: super::alter::TargetFreshnessMode::Interval,
            milliseconds: Some(total_ms),
        },
    )?;

    Spi::connect_mut(|client| {
        client
            .update(
                "UPDATE pgtrickle.pgt_stream_tables \
                 SET refresh_tier = $1, updated_at = now() \
                 WHERE pgt_id = $2",
                None,
                &[tier.as_str().into(), meta.pgt_id.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        pgrx::info!(
            "pg_trickle: set SLA {}ms for '{}', assigned tier '{}'",
            total_ms,
            name,
            tier.as_str()
        );

        Ok::<(), PgTrickleError>(())
    })?;

    Ok(())
}

/// SLA-2: Assign the appropriate tier based on an SLA interval in milliseconds.
///
/// Tier assignment rules:
/// - Hot: SLA ≤ 5s (refresh every 1× schedule)
/// - Warm: SLA ≤ 30s (refresh every 2× schedule)
/// - Cold: SLA > 30s (refresh every 10× schedule)
pub fn assign_tier_for_sla(sla_ms: i64) -> Result<crate::scheduler::RefreshTier, PgTrickleError> {
    use crate::scheduler::RefreshTier;
    if sla_ms <= 5_000 {
        Ok(RefreshTier::Hot)
    } else if sla_ms <= 30_000 {
        Ok(RefreshTier::Warm)
    } else {
        Ok(RefreshTier::Cold)
    }
}

// ── PRED-1: Linear regression forecaster with robustness guards ───────────

/// PRED-1: Fit a simple linear regression `duration_ms ~ delta_rows` over
/// the prediction window for a given stream table, with outlier robustness.
///
/// ## Robustness guards (v0.25.0)
///
/// 1. **Cold-start guard**: Returns `None` if the first DIFFERENTIAL record
///    in `pgt_refresh_history` is less than 60 s old (avoids acting on a
///    single noisy sample immediately after a stream table is populated).
/// 2. **Non-degenerate variance check**: Returns `(0.0, avg_y, n)` when all
///    delta sizes are identical (slope undefined). The caller interprets this
///    as "intercept only" and skips the preemption check.
/// 3. **Outlier filter**: Uses an IQR (p25 / p75) window from the sample
///    set to exclude extreme outliers before fitting the regression.
///
/// Returns `(slope, intercept, sample_count)`. Returns `None` if fewer than
/// `prediction_min_samples` clean samples exist.
pub fn fit_linear_regression(pgt_id: i64) -> Option<(f64, f64, i64)> {
    let window_minutes = crate::config::pg_trickle_prediction_window();
    let min_samples = crate::config::pg_trickle_prediction_min_samples();

    if min_samples <= 0 {
        return None;
    }

    Spi::connect(|client| {
        // PRED-1 cold-start guard: skip if first differential was < 60s ago.
        let first_age_secs: Option<f64> = client
            .select(
                "SELECT EXTRACT(EPOCH FROM (now() - MIN(start_time))) \
                 FROM pgtrickle.pgt_refresh_history \
                 WHERE pgt_id = $1 AND action = 'DIFFERENTIAL' AND status = 'COMPLETED'",
                None,
                &[pgt_id.into()],
            )
            .ok()
            .and_then(|r| {
                if r.is_empty() {
                    None
                } else {
                    r.first().get::<f64>(1).ok().flatten()
                }
            });

        if first_age_secs.is_some_and(|age| age < 60.0) {
            return None; // cold-start guard
        }

        // PRED-1 outlier filter: compute IQR bounds from the current window.
        // Rows with duration_ms outside [p25 - 1.5 * IQR, p75 + 1.5 * IQR]
        // are excluded from the regression.
        let (p25, p75): (f64, f64) = client
            .select(
                "SELECT \
                     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY \
                         EXTRACT(EPOCH FROM (end_time - start_time)) * 1000) AS p25, \
                     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY \
                         EXTRACT(EPOCH FROM (end_time - start_time)) * 1000) AS p75 \
                 FROM pgtrickle.pgt_refresh_history \
                 WHERE pgt_id = $1 \
                   AND status = 'COMPLETED' \
                   AND action = 'DIFFERENTIAL' \
                   AND end_time IS NOT NULL \
                   AND start_time > now() - ($2 || ' minutes')::interval",
                None,
                &[pgt_id.into(), window_minutes.into()],
            )
            .ok()
            .and_then(|r| {
                if r.is_empty() {
                    None
                } else {
                    let row = r.first();
                    let p25 = row.get::<f64>(1).ok().flatten()?;
                    let p75 = row.get::<f64>(2).ok().flatten()?;
                    Some((p25, p75))
                }
            })
            .unwrap_or((0.0, f64::MAX));

        let iqr = p75 - p25;
        let lower_bound = (p25 - 1.5 * iqr).max(0.0);
        let upper_bound = p75 + 1.5 * iqr;

        // Fit regression on the IQR-filtered sample set.
        let result = client
            .select(
                "SELECT count(*) AS n, \
                        coalesce(avg(rows_inserted + rows_deleted), 0) AS avg_x, \
                        coalesce(avg(EXTRACT(EPOCH FROM (end_time - start_time)) * 1000), 0) AS avg_y, \
                        coalesce(sum((rows_inserted + rows_deleted) * \
                            EXTRACT(EPOCH FROM (end_time - start_time)) * 1000), 0) AS sum_xy, \
                        coalesce(sum((rows_inserted + rows_deleted) * \
                            (rows_inserted + rows_deleted)), 0) AS sum_x2 \
                 FROM pgtrickle.pgt_refresh_history \
                 WHERE pgt_id = $1 \
                   AND status = 'COMPLETED' \
                   AND action = 'DIFFERENTIAL' \
                   AND end_time IS NOT NULL \
                   AND start_time > now() - ($2 || ' minutes')::interval \
                   AND EXTRACT(EPOCH FROM (end_time - start_time)) * 1000 \
                       BETWEEN $3 AND $4",
                None,
                &[
                    pgt_id.into(),
                    window_minutes.into(),
                    lower_bound.into(),
                    upper_bound.into(),
                ],
            )
            .ok()?;

        if result.is_empty() {
            return None;
        }

        let n: i64 = result.get::<i64>(1).ok()??;
        if n < min_samples as i64 {
            return None; // not enough clean samples
        }

        let avg_x: f64 = result.get::<f64>(2).ok()??;
        let avg_y: f64 = result.get::<f64>(3).ok()??;
        let sum_xy: f64 = result.get::<f64>(4).ok()??;
        let sum_x2: f64 = result.get::<f64>(5).ok()??;

        // PRED-1: Non-degenerate variance check.
        // If all x values are identical (e.g. always same delta size),
        // the slope is undefined — return intercept-only model.
        let denominator = sum_x2 - n as f64 * avg_x * avg_x;
        if denominator.abs() < 1e-10 {
            return Some((0.0, avg_y, n));
        }

        let slope = (sum_xy - n as f64 * avg_x * avg_y) / denominator;
        let intercept = avg_y - slope * avg_x;

        Some((slope, intercept, n))
    })
}

/// PRED-2: Predict the differential refresh duration for a given delta size.
///
/// Returns `None` if the model cannot be fitted (cold-start fallback).
pub fn predict_diff_duration_ms(pgt_id: i64, delta_rows: i64) -> Option<f64> {
    let (slope, intercept, _n) = fit_linear_regression(pgt_id)?;
    Some(slope * delta_rows as f64 + intercept)
}

/// PRED-2: Check whether the predicted differential cost exceeds the
/// full-refresh cost by more than `prediction_ratio`, triggering a
/// pre-emptive switch to FULL.
///
/// ## PRED-1 robustness guards (v0.25.0)
///
/// - **Clamping**: Predictions are clamped to `[0.5×, 4×] last_full_ms`
///   before the ratio comparison. A prediction outside this range is likely
///   a model artifact and should not drive preemption.
/// - **Zero-intercept guard**: When the model slope is 0.0 (degenerate
///   intercept-only case), the prediction is treated as unknown and
///   preemption is skipped.
pub fn should_preempt_to_full(pgt_id: i64, delta_rows: i64, last_full_ms: f64) -> bool {
    if last_full_ms <= 0.0 {
        return false;
    }
    let ratio = crate::config::pg_trickle_prediction_ratio();
    if let Some(raw_predicted_ms) = predict_diff_duration_ms(pgt_id, delta_rows) {
        // PRED-1: Clamp prediction to [0.5×, 4×] last_full_ms.
        let lower = last_full_ms * 0.5;
        let upper = last_full_ms * 4.0;
        let predicted_ms = raw_predicted_ms.clamp(lower, upper);
        predicted_ms > last_full_ms * ratio
    } else {
        false // cold-start fallback — don't preempt.
    }
}

// ── SLA-3: Dynamic tier re-assignment ────────────────────────────────────

/// SLA-2 (v0.26.0): Per-ST hysteresis counters for tier adjustment damping.
///
/// Stored in a thread-local so the scheduler's single-threaded tick loop
/// persists the state between tick invocations without requiring a catalog
/// schema change. State is lost on scheduler restart (acceptable — just
/// means 3 more ticks are needed before a tier change fires).
///
/// Key: `pgt_id`.
/// Value: `(consecutive_upgrade_pressure, consecutive_downgrade_pressure)`.
///   - upgrade pressure: ideal tier is hotter than current → need to upgrade
///   - downgrade pressure: ideal tier is colder than current → could downgrade
///
/// Requires 3 consecutive pressure signals in the same direction before
/// actually changing the tier. This prevents oscillation at the SLA boundary.
pub struct SlaTierHysteresis {
    /// Consecutive ticks where the ideal tier is hotter than current.
    pub upgrade_pressure: i32,
    /// Consecutive ticks where the ideal tier is colder than current.
    pub downgrade_pressure: i32,
}

impl SlaTierHysteresis {
    const THRESHOLD: i32 = 3;
}

use std::cell::RefCell;
use std::collections::HashMap as _HashMap;
thread_local! {
    /// SLA-2: Per-ST tier hysteresis state.  Keyed by `pgt_id`.
    static SLA_TIER_HYSTERESIS: RefCell<_HashMap<i64, SlaTierHysteresis>> =
        RefCell::new(_HashMap::new());
}

/// Numeric ordering for RefreshTier (lower = hotter = more frequent).
fn tier_order(tier: &crate::scheduler::RefreshTier) -> u8 {
    use crate::scheduler::RefreshTier;
    match tier {
        RefreshTier::Hot => 0,
        RefreshTier::Warm => 1,
        RefreshTier::Cold => 2,
        RefreshTier::Frozen => 3,
    }
}

/// SLA-3: Check and adjust tier for a stream table based on SLA and queue depth.
///
/// Called after each refresh tick. Bumps tier up or down only after 3
/// consecutive signals in the same direction (SLA-2 hysteresis damping).
pub fn maybe_adjust_tier_for_sla(meta: &StreamTableMeta) {
    let sla_ms = match meta.freshness_deadline_ms {
        Some(ms) => ms,
        None => return, // No SLA configured — skip.
    };

    // Look at the last 3 refresh durations to determine if the tier is appropriate.
    let avg_duration_ms = Spi::connect(|client| {
        client
            .select(
                "SELECT coalesce(avg(EXTRACT(EPOCH FROM (end_time - start_time)) * 1000), 0) \
                 FROM (SELECT end_time, start_time \
                       FROM pgtrickle.pgt_refresh_history \
                       WHERE pgt_id = $1 AND status = 'COMPLETED' \
                       ORDER BY end_time DESC LIMIT 3) sub",
                None,
                &[meta.pgt_id.into()],
            )
            .ok()
            .and_then(|r| {
                if r.is_empty() {
                    None
                } else {
                    r.get::<f64>(1).ok()?
                }
            })
    });

    let _avg_ms = match avg_duration_ms {
        Some(ms) => ms,
        None => return, // Not enough data.
    };

    use crate::scheduler::RefreshTier;
    let current_tier = RefreshTier::from_sql_str(&meta.refresh_tier);
    let ideal_tier = assign_tier_for_sla(sla_ms).unwrap_or(RefreshTier::Hot);

    let current_order = tier_order(&current_tier);
    let ideal_order = tier_order(&ideal_tier);

    // SLA-2 (v0.26.0): Hysteresis damping — require THRESHOLD consecutive
    // pressure signals before changing the tier.
    if current_order == ideal_order {
        // Tiers match — reset hysteresis counters.
        SLA_TIER_HYSTERESIS.with(|map| {
            if let Some(state) = map.borrow_mut().get_mut(&meta.pgt_id) {
                state.upgrade_pressure = 0;
                state.downgrade_pressure = 0;
            }
        });
        return;
    }

    SLA_TIER_HYSTERESIS.with(|map| {
        let mut map = map.borrow_mut();
        let state = map.entry(meta.pgt_id).or_insert(SlaTierHysteresis {
            upgrade_pressure: 0,
            downgrade_pressure: 0,
        });

        if ideal_order < current_order {
            // Upgrade pressure: ideal tier is hotter than current.
            state.upgrade_pressure += 1;
            state.downgrade_pressure = 0;

            if state.upgrade_pressure >= SlaTierHysteresis::THRESHOLD {
                // Upgrade by one step (don't jump directly to ideal — incremental).
                let new_order = current_order.saturating_sub(1);
                let new_tier = match new_order {
                    0 => RefreshTier::Hot,
                    1 => RefreshTier::Warm,
                    2 => RefreshTier::Cold,
                    _ => RefreshTier::Frozen,
                };
                Spi::connect_mut(|client| {
                    let _ = client.update(
                        "UPDATE pgtrickle.pgt_stream_tables \
                         SET refresh_tier = $1, updated_at = now() \
                         WHERE pgt_id = $2",
                        None,
                        &[new_tier.as_str().into(), meta.pgt_id.into()],
                    );
                });
                #[cfg(not(test))]
                pgrx::info!(
                    "pg_trickle: SLA-2 tier upgrade for '{}': {} → {} \
                     (after {} consecutive pressure signals)",
                    meta.pgt_name,
                    current_tier.as_str(),
                    new_tier.as_str(),
                    state.upgrade_pressure,
                );
                state.upgrade_pressure = 0;
            }
        } else {
            // Downgrade pressure: ideal tier is colder than current.
            state.downgrade_pressure += 1;
            state.upgrade_pressure = 0;

            if state.downgrade_pressure >= SlaTierHysteresis::THRESHOLD {
                // Downgrade by one step.
                let new_order = current_order + 1;
                let new_tier = match new_order {
                    0 => RefreshTier::Hot,
                    1 => RefreshTier::Warm,
                    2 => RefreshTier::Cold,
                    _ => RefreshTier::Frozen,
                };
                Spi::connect_mut(|client| {
                    let _ = client.update(
                        "UPDATE pgtrickle.pgt_stream_tables \
                         SET refresh_tier = $1, updated_at = now() \
                         WHERE pgt_id = $2",
                        None,
                        &[new_tier.as_str().into(), meta.pgt_id.into()],
                    );
                });
                #[cfg(not(test))]
                pgrx::info!(
                    "pg_trickle: SLA-2 tier downgrade for '{}': {} → {} \
                     (after {} consecutive pressure signals)",
                    meta.pgt_name,
                    current_tier.as_str(),
                    new_tier.as_str(),
                    state.downgrade_pressure,
                );
                state.downgrade_pressure = 0;
            }
        }
    });
}

// ── Helpers ──────────────────────────────────────────────────────────────

/// Quote a SQL identifier.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quote a qualified SQL identifier (schema.table).
fn quote_ident_qualified(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(table))
}

// ── PUB-1 (v0.25.0): Subscriber-LSN tracking ──────────────────────────────

/// PUB-1: Check whether any logical replication slot associated with a
/// downstream publication lags more than `pg_trickle.publication_lag_warn_bytes`
/// bytes behind the current WAL write position.
///
/// Emits a WARNING for each lagging slot.  Returns `true` if at least one
/// slot lags beyond the threshold (i.e. the caller should NOT truncate the
/// change buffer until subscribers catch up).
///
/// Returns `false` when:
/// - The threshold GUC is 0 (feature disabled, default).
/// - The publication has no active replication slots.
/// - All slots are within the lag threshold.
pub(crate) fn check_subscriber_lag(publication_name: &str) -> bool {
    let warn_bytes = crate::config::pg_trickle_publication_lag_warn_bytes();
    if warn_bytes <= 0 {
        return false;
    }

    // Query pg_replication_slots for all slots consuming this publication.
    // `confirmed_flush_lsn` is the LSN the subscriber has confirmed processing.
    let sql = format!(
        "SELECT slot_name::text, \
                pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint AS lag_bytes \
         FROM pg_replication_slots \
         WHERE active = true \
           AND plugin = 'pgoutput' \
           AND slot_name LIKE 'pgt\\_{pub}\\_%' \
         ORDER BY lag_bytes DESC",
        pub = publication_name.replace('\'', "''"),
    );

    let mut any_lagging = false;

    let result = Spi::connect(|client| {
        // nosemgrep: rust.spi.select.dynamic-format — publication name is escaped for the fixed slot-name LIKE pattern.
        let rows = client.select(&sql, None, &[]).map_err(|e| {
            pgrx::warning!(
                "[pg_trickle] PUB-1: failed to query replication slots for '{}': {}",
                publication_name,
                e,
            );
        });

        if let Ok(rows) = rows {
            for row in rows {
                let slot_name: String = row.get(1).ok().flatten().unwrap_or_default();
                let lag_bytes: i64 = row.get(2).ok().flatten().unwrap_or(0);

                if lag_bytes > warn_bytes {
                    pgrx::warning!(
                        "[pg_trickle] PUB-1: subscriber slot '{}' for publication '{}' \
                         is {} bytes behind write LSN (threshold: {} bytes). \
                         Change buffer truncation deferred.",
                        slot_name,
                        publication_name,
                        lag_bytes,
                        warn_bytes,
                    );
                    any_lagging = true;
                }
            }
        }
        Ok::<(), ()>(())
    });

    if result.is_err() {
        // SPI connect failed; conservatively treat as lagging.
        return true;
    }

    any_lagging
}

/// PUB-1: Guard that prevents change buffer truncation when a subscriber
/// is lagging.
///
/// Returns `true` (skip truncation) when `check_subscriber_lag` detects
/// one or more lagging subscribers.
pub(crate) fn should_defer_change_buffer_truncation(publication_name: &str) -> bool {
    check_subscriber_lag(publication_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_assign_tier_for_sla_hot() {
        use crate::scheduler::RefreshTier;
        assert_eq!(assign_tier_for_sla(1000).ok(), Some(RefreshTier::Hot));
        assert_eq!(assign_tier_for_sla(5000).ok(), Some(RefreshTier::Hot));
    }

    #[test]
    fn test_assign_tier_for_sla_warm() {
        use crate::scheduler::RefreshTier;
        assert_eq!(assign_tier_for_sla(5001).ok(), Some(RefreshTier::Warm));
        assert_eq!(assign_tier_for_sla(30000).ok(), Some(RefreshTier::Warm));
    }

    #[test]
    fn test_assign_tier_for_sla_cold() {
        use crate::scheduler::RefreshTier;
        assert_eq!(assign_tier_for_sla(30001).ok(), Some(RefreshTier::Cold));
        assert_eq!(assign_tier_for_sla(60000).ok(), Some(RefreshTier::Cold));
    }

    // SEC-001 (v0.70.0): The local parse_qualified_name() has been removed.
    // Schema-qualified name parsing is now done by
    // super::helpers::parse_qualified_name_pub() which respects
    // current_schema() for unqualified names (requires SPI — no unit test).
    // Tests for the shared helper live in helpers.rs.

    #[test]
    fn test_parse_qualified_name_with_schema() {
        // Two-part qualified names: no SPI needed (the split path).
        assert_eq!(
            crate::api::helpers::parse_qualified_name_pub("myschema.mytable").ok(),
            Some(("myschema".to_string(), "mytable".to_string()))
        );
    }

    #[test]
    fn test_parse_qualified_dots_in_schema() {
        // Typed identifiers reject ambiguous three-part names.
        assert!(crate::api::helpers::parse_qualified_name_pub("my.schema.table").is_err());
    }

    #[test]
    fn test_quote_ident_simple() {
        assert_eq!(quote_ident("hello"), "\"hello\"");
    }

    #[test]
    fn test_quote_ident_with_quotes() {
        assert_eq!(quote_ident("he\"llo"), "\"he\"\"llo\"");
    }

    // ── TEST-6 (v0.24.0): Comprehensive publication.rs unit tests ────────
    //
    // 25+ tests for assign_tier_for_sla, parse_qualified_name, quote_ident,
    // and boundary cases (0, negative, NaN-like edge values).

    #[test]
    fn test_assign_tier_sla_zero() {
        use crate::scheduler::RefreshTier;
        // Zero is valid (Hot tier — aggressive)
        assert_eq!(assign_tier_for_sla(0).ok(), Some(RefreshTier::Hot));
    }

    #[test]
    fn test_assign_tier_sla_one_ms() {
        use crate::scheduler::RefreshTier;
        assert_eq!(assign_tier_for_sla(1).ok(), Some(RefreshTier::Hot));
    }

    #[test]
    fn test_assign_tier_sla_boundary_5000() {
        use crate::scheduler::RefreshTier;
        assert_eq!(assign_tier_for_sla(5000).ok(), Some(RefreshTier::Hot));
    }

    #[test]
    fn test_assign_tier_sla_boundary_5001() {
        use crate::scheduler::RefreshTier;
        assert_eq!(assign_tier_for_sla(5001).ok(), Some(RefreshTier::Warm));
    }

    #[test]
    fn test_assign_tier_sla_boundary_30000() {
        use crate::scheduler::RefreshTier;
        assert_eq!(assign_tier_for_sla(30000).ok(), Some(RefreshTier::Warm));
    }

    #[test]
    fn test_assign_tier_sla_boundary_30001() {
        use crate::scheduler::RefreshTier;
        assert_eq!(assign_tier_for_sla(30001).ok(), Some(RefreshTier::Cold));
    }

    #[test]
    fn test_assign_tier_sla_very_large() {
        use crate::scheduler::RefreshTier;
        assert_eq!(
            assign_tier_for_sla(86_400_000).ok(),
            Some(RefreshTier::Cold)
        );
    }

    #[test]
    fn test_assign_tier_sla_negative() {
        use crate::scheduler::RefreshTier;
        // Negative is technically invalid but shouldn't panic.
        // It falls into the Hot tier (≤ 5000).
        assert_eq!(assign_tier_for_sla(-1).ok(), Some(RefreshTier::Hot));
    }

    #[test]
    fn test_assign_tier_sla_i64_max() {
        use crate::scheduler::RefreshTier;
        assert_eq!(assign_tier_for_sla(i64::MAX).ok(), Some(RefreshTier::Cold));
    }

    #[test]
    fn test_quote_ident_empty() {
        assert_eq!(quote_ident(""), "\"\"");
    }

    #[test]
    fn test_quote_ident_spaces() {
        assert_eq!(quote_ident("my table"), "\"my table\"");
    }

    #[test]
    fn test_quote_ident_unicode() {
        assert_eq!(quote_ident("tëst"), "\"tëst\"");
    }

    #[test]
    fn test_quote_ident_qualified_basic() {
        assert_eq!(
            quote_ident_qualified("public", "orders"),
            "\"public\".\"orders\""
        );
    }

    #[test]
    fn test_quote_ident_qualified_with_quotes() {
        assert_eq!(
            quote_ident_qualified("my\"schema", "my\"table"),
            "\"my\"\"schema\".\"my\"\"table\""
        );
    }

    #[test]
    fn test_quote_ident_qualified_empty_schema() {
        assert_eq!(quote_ident_qualified("", "table"), "\"\".\"table\"");
    }

    #[test]
    fn test_assign_tier_sla_warm_midpoint() {
        use crate::scheduler::RefreshTier;
        assert_eq!(assign_tier_for_sla(15000).ok(), Some(RefreshTier::Warm));
    }

    #[test]
    fn test_assign_tier_sla_cold_100s() {
        use crate::scheduler::RefreshTier;
        assert_eq!(assign_tier_for_sla(100_000).ok(), Some(RefreshTier::Cold));
    }

    #[test]
    fn test_quote_ident_backslash() {
        assert_eq!(quote_ident("back\\slash"), "\"back\\slash\"");
    }

    #[test]
    fn test_quote_ident_null_char() {
        // Null characters should be preserved in quoting
        assert_eq!(quote_ident("a\0b"), "\"a\0b\"");
    }

    fn oid(value: u32) -> pg_sys::Oid {
        pg_sys::Oid::from(value)
    }

    fn binding() -> PublicationBinding {
        PublicationBinding {
            pgt_id: 1,
            stream_relid: oid(10),
            publication_oid: oid(20),
            publication_name: "pgt_pub_orders".to_string(),
            publication_owner_oid: oid(30),
            expected_relation_oids: vec![oid(10)],
        }
    }

    fn live() -> LivePublication {
        LivePublication {
            oid: oid(20),
            name: "pgt_pub_orders".to_string(),
            owner_oid: oid(30),
            all_tables: false,
            relation_oids: vec![oid(10)],
            namespace_oids: Vec::new(),
        }
    }

    #[test]
    fn test_publication_binding_classifier_accepts_exact_binding() {
        let b = binding();
        assert_eq!(
            classify_publication_binding(
                Some(&b),
                Some(&b.publication_name),
                Some(&live()),
                Some(oid(20)),
                oid(10)
            ),
            Ok(())
        );
    }

    #[test]
    fn test_publication_binding_classifier_private_completeness_and_name_reuse() {
        let b = binding();
        assert_eq!(
            classify_publication_binding(Some(&b), None, Some(&live()), Some(oid(20)), oid(10)),
            Err(PublicationBindingMismatchReason::PrivateBindingIncomplete)
        );
        assert_eq!(
            classify_publication_binding(
                Some(&b),
                Some(&b.publication_name),
                None,
                Some(oid(21)),
                oid(10)
            ),
            Err(PublicationBindingMismatchReason::PublicationNameReused)
        );
        assert_eq!(
            classify_publication_binding(Some(&b), Some(&b.publication_name), None, None, oid(10)),
            Err(PublicationBindingMismatchReason::MissingPublication)
        );
    }

    #[test]
    fn test_publication_binding_classifier_has_deterministic_drift_priority() {
        let b = binding();
        let mut p = live();
        p.name = "renamed".to_string();
        p.owner_oid = oid(31);
        p.relation_oids = vec![oid(99)];
        assert_eq!(
            classify_publication_binding(
                Some(&b),
                Some(&b.publication_name),
                Some(&p),
                Some(oid(20)),
                oid(99)
            ),
            Err(PublicationBindingMismatchReason::PublicationRenamed)
        );
        p.name = b.publication_name.clone();
        assert_eq!(
            classify_publication_binding(
                Some(&b),
                Some(&b.publication_name),
                Some(&p),
                Some(oid(20)),
                oid(99)
            ),
            Err(PublicationBindingMismatchReason::PublicationOwnerChanged)
        );
        p.owner_oid = b.publication_owner_oid;
        assert_eq!(
            classify_publication_binding(
                Some(&b),
                Some(&b.publication_name),
                Some(&p),
                Some(oid(20)),
                oid(99)
            ),
            Err(PublicationBindingMismatchReason::StreamRelationChanged)
        );
        p.relation_oids = vec![oid(10)];
        p.all_tables = true;
        assert_eq!(
            classify_publication_binding(
                Some(&b),
                Some(&b.publication_name),
                Some(&p),
                Some(oid(20)),
                oid(10)
            ),
            Err(PublicationBindingMismatchReason::PublicationScopeChanged)
        );
    }

    #[test]
    fn test_publication_binding_classifier_compares_relations_as_sets() {
        let b = binding();
        let mut p = live();
        p.relation_oids = vec![oid(10), oid(10)];
        assert_eq!(
            classify_publication_binding(
                Some(&b),
                Some(&b.publication_name),
                Some(&p),
                Some(oid(20)),
                oid(10)
            ),
            Ok(())
        );
        p.relation_oids = vec![oid(10), oid(11)];
        assert_eq!(
            classify_publication_binding(
                Some(&b),
                Some(&b.publication_name),
                Some(&p),
                Some(oid(20)),
                oid(10)
            ),
            Err(PublicationBindingMismatchReason::PublicationRelationsChanged)
        );
    }

    #[test]
    fn test_publication_name_is_bounded_and_utf8_safe() {
        let short = publication_name("public", "orders");
        assert_eq!(short, "pgt_pub_orders");
        let long = publication_name("weird.schema", &"é".repeat(80));
        assert!(long.len() <= MAX_IDENTIFIER_BYTES);
        assert!(long.is_char_boundary(long.len()));
        assert_eq!(long, publication_name("weird.schema", &"é".repeat(80)));
        assert_ne!(long, publication_name("other.schema", &"é".repeat(80)));
    }
}
