//! Durable output-delta consumers (Delta V1).
//!
//! The public log is populated from the existing, complete stream-table D/I
//! capture.  That keeps one source of truth for differential and full paths.

use super::helpers::{outer_user_id, role_owns_relation_or_is_superuser};
use crate::catalog::StreamTableMeta;
use crate::error::PgTrickleError;
use pgrx::prelude::*;
use std::cell::RefCell;
use std::collections::HashMap;

const ACTIVE: &str = "ACTIVE";
const PAUSED: &str = "PAUSED";
const RESNAPSHOT_REQUIRED: &str = "RESNAPSHOT_REQUIRED";
const INVALIDATED: &str = "INVALIDATED";
const INITIAL_BASELINE_REQUIRED: &str = "INITIAL_BASELINE_REQUIRED";
const DELTA_RELATION_PREFIX: &str = "output_delta_";

thread_local! {
    static CAPTURE_STARTS: RefCell<HashMap<i64, i64>> = RefCell::new(HashMap::new());
}

fn delta_error(code: &'static str, detail: impl Into<String>) -> PgTrickleError {
    PgTrickleError::IntegrationError {
        code,
        detail: detail.into(),
    }
}

fn spi<T>(result: Result<T, pgrx::spi::SpiError>) -> Result<T, PgTrickleError> {
    result.map_err(|error| PgTrickleError::SpiError(error.to_string()))
}

fn raise(error: PgTrickleError) -> ! {
    super::raise_error_with_context(error)
}

fn quoted_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn payload_name(pgt_id: i64) -> String {
    format!("{DELTA_RELATION_PREFIX}{pgt_id}")
}

fn batch_mode(rows_changed: i64, captured_rows: i64) -> &'static str {
    if rows_changed > 0 && captured_rows == 0 {
        "FULL_INVALIDATION"
    } else {
        "EXACT"
    }
}

fn database_instance_id() -> Result<String, PgTrickleError> {
    Ok(Spi::get_one::<String>(
        "SELECT COALESCE((SELECT instance_id::text FROM pgtrickle.pgt_capture_instance WHERE singleton), 'uninitialized')",
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or_else(|| "uninitialized".to_string()))
}

fn authorized_stream(meta: &StreamTableMeta) -> Result<(), PgTrickleError> {
    if !role_owns_relation_or_is_superuser(outer_user_id(), meta.pgt_relid)? {
        return Err(delta_error(
            "PGT_EXT_AUTHORIZATION",
            format!(
                "owner-equivalent authority is required for {}.{}",
                meta.pgt_schema, meta.pgt_name
            ),
        ));
    }
    Ok(())
}

fn contract(meta: &StreamTableMeta) -> Result<(Vec<u8>, i16), PgTrickleError> {
    crate::api::integration::stream_contract_digest(meta)
}

fn output_columns(meta: &StreamTableMeta) -> Result<Vec<(String, String)>, PgTrickleError> {
    let columns = crate::cdc::resolve_st_output_columns(meta.pgt_relid)?;
    for (name, _) in &columns {
        if matches!(
            name.as_str(),
            "batch_token" | "ordinal" | "action" | "row_identity"
        ) {
            return Err(delta_error(
                "PGT_EXT_CONTRACT_UNSUPPORTED",
                format!("output column '{name}' is reserved by Delta V1"),
            ));
        }
    }
    Ok(columns)
}

fn ensure_payload(
    meta: &StreamTableMeta,
    owner_oid: pg_sys::Oid,
) -> Result<pg_sys::Oid, PgTrickleError> {
    let columns = output_columns(meta)?;
    let defs = columns
        .iter()
        .map(|(name, typ)| format!(", {} {}", quoted_ident(name), typ))
        .collect::<String>();
    let name = payload_name(meta.pgt_id);
    let qualified = format!("pgtrickle_changes.{}", quoted_ident(&name));
    Spi::run(&format!(
        "CREATE TABLE IF NOT EXISTS {qualified} (\
            batch_token BIGINT NOT NULL, ordinal BIGINT NOT NULL,\
            action TEXT NOT NULL CHECK (action IN ('DELETE', 'INSERT')),\
            row_identity BYTEA NOT NULL{defs},\
            PRIMARY KEY (batch_token, ordinal))"
    ))
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    // nosemgrep: qualified is built only from quoted, OID-derived identifiers.
    Spi::run(&format!("REVOKE ALL ON TABLE {qualified} FROM PUBLIC"))
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    let role = Spi::get_one_with_args::<String>(
        "SELECT rolname::text FROM pg_catalog.pg_roles WHERE oid = $1",
        &[owner_oid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| {
        delta_error(
            "PGT_EXT_AUTHORIZATION",
            "consumer owner role no longer exists",
        )
    })?;
    Spi::run(&format!(
        "GRANT SELECT ON TABLE {qualified} TO {}",
        quoted_ident(&role)
    ))
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    Spi::get_one_with_args::<pg_sys::Oid>(
        "SELECT to_regclass($1)::oid",
        &[format!("pgtrickle_changes.{name}").into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| {
        delta_error(
            "PGT_EXT_CONSUMER_BLOCKED",
            "typed output-delta relation was not created",
        )
    })
}

fn ensure_buffer(meta: &StreamTableMeta) -> Result<(), PgTrickleError> {
    crate::cdc::ensure_st_change_buffer(
        meta.pgt_id,
        meta.pgt_relid,
        &crate::config::pg_trickle_change_buffer_schema(),
    )
}

fn consumer_owner(
    consumer_id: pgrx::Uuid,
) -> Result<(pg_sys::Oid, StreamTableMeta), PgTrickleError> {
    let row = Spi::connect(|client| {
        let table = client
            .select(
                "SELECT c.owner_oid, st.pgt_relid FROM pgtrickle.pgt_output_delta_consumers c JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = c.pgt_id WHERE c.consumer_id = $1",
                None,
                &[consumer_id.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        if table.is_empty() {
            return Ok::<Option<(pg_sys::Oid, pg_sys::Oid)>, PgTrickleError>(None);
        }
        let row = table.first();
        Ok(Some((
            row.get::<pg_sys::Oid>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| delta_error("PGT_EXT_TOKEN_INVALID", "consumer owner is NULL"))?,
            row.get::<pg_sys::Oid>(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    delta_error("PGT_EXT_TOKEN_INVALID", "consumer stream table is NULL")
                })?,
        )))
    })?;
    let (owner, relid) =
        row.ok_or_else(|| delta_error("PGT_EXT_TOKEN_INVALID", "unknown output-delta consumer"))?;
    if owner != outer_user_id()
        && !Spi::get_one_with_args::<bool>(
            "SELECT rolsuper FROM pg_catalog.pg_roles WHERE oid = $1",
            &[outer_user_id().into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .unwrap_or(false)
    {
        return Err(delta_error(
            "PGT_EXT_AUTHORIZATION",
            "consumer owner authority is required",
        ));
    }
    let meta = StreamTableMeta::get_by_relid(relid)?;
    Ok((owner, meta))
}

/// Start the output capture window immediately before a refresh executor runs.
pub(crate) fn begin_capture(pgt_id: i64) -> Result<(), PgTrickleError> {
    if !has_consumer(pgt_id) {
        return Ok(());
    }
    super::require_v098_capability(super::DELTA_V1_CAPABILITY)?;
    let schema = crate::config::pg_trickle_change_buffer_schema();
    let max_id = Spi::get_one::<i64>(&format!(
        "SELECT COALESCE(max(change_id), 0) FROM {}.changes_pgt_{pgt_id}",
        quoted_ident(&schema)
    ))
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or(0);
    CAPTURE_STARTS.with(|starts| starts.borrow_mut().insert(pgt_id, max_id));
    Ok(())
}

pub(crate) fn has_consumer(pgt_id: i64) -> bool {
    Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_output_delta_consumers WHERE pgt_id = $1 AND state <> 'DROPPED')",
        &[pgt_id.into()],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false)
}

/// Create one shared batch in the common refresh finalizer.
pub(crate) fn finalize(
    meta: &StreamTableMeta,
    refresh_id: i64,
    rows_changed: i64,
) -> Result<(), PgTrickleError> {
    if !has_consumer(meta.pgt_id) {
        return Ok(());
    }
    super::require_v098_capability(super::DELTA_V1_CAPABILITY)?;
    let start = CAPTURE_STARTS
        .with(|starts| starts.borrow_mut().remove(&meta.pgt_id))
        .unwrap_or_else(|| {
            let schema = crate::config::pg_trickle_change_buffer_schema();
            Spi::get_one::<i64>(&format!(
                "SELECT COALESCE(max(change_id), 0) FROM {}.changes_pgt_{}",
                quoted_ident(&schema),
                meta.pgt_id
            ))
            .unwrap_or(Some(0))
            .unwrap_or(0)
        });
    let digest = spi(Spi::get_one_with_args::<Vec<u8>>(
        "SELECT output_contract_digest FROM pgtrickle.pgt_output_delta_logs WHERE pgt_id = $1 FOR UPDATE",
        &[meta.pgt_id.into()],
    ))?
    .ok_or_else(|| delta_error("PGT_EXT_CONSUMER_BLOCKED", "output-delta log is missing"))?;
    let head = spi(Spi::get_one_with_args::<i64>(
        "SELECT log_head FROM pgtrickle.pgt_output_delta_logs WHERE pgt_id = $1",
        &[meta.pgt_id.into()],
    ))?
    .unwrap_or(0);
    let token = head.saturating_add(1);
    let columns = output_columns(meta)?;
    let col_names = columns
        .iter()
        .map(|(name, _)| quoted_ident(name))
        .collect::<Vec<_>>()
        .join(", ");
    let col_refs = columns
        .iter()
        .map(|(name, _)| format!("b.{}", quoted_ident(name)))
        .collect::<Vec<_>>()
        .join(", ");
    let schema = crate::config::pg_trickle_change_buffer_schema();
    let source = format!("{}.changes_pgt_{}", quoted_ident(&schema), meta.pgt_id);
    let payload = format!(
        "pgtrickle_changes.{}",
        quoted_ident(&payload_name(meta.pgt_id))
    );
    let (changed_rows, unsupported_rows) = if rows_changed > 0 {
        let changed_rows = spi(Spi::get_one_with_args::<i64>(
            &format!("SELECT count(*)::bigint FROM {source} b WHERE b.change_id > $1"),
            &[start.into()],
        ))?
        .unwrap_or(0);
        let unsupported_rows = spi(Spi::get_one_with_args::<i64>(
            &format!("SELECT count(*)::bigint FROM {source} b WHERE b.change_id > $1 AND b.action NOT IN ('D', 'I')"),
            &[start.into()],
        ))?
        .unwrap_or(0);
        (changed_rows, unsupported_rows)
    } else {
        (0, 0)
    };
    let mode = if unsupported_rows > 0 {
        "FULL_INVALIDATION"
    } else {
        batch_mode(rows_changed, changed_rows)
    };
    let (payload_rows, inserted, deleted) = if mode == "EXACT" && changed_rows > 0 {
        let target_cols = if col_names.is_empty() {
            "batch_token, ordinal, action, row_identity".to_string()
        } else {
            format!("batch_token, ordinal, action, row_identity, {col_names}")
        };
        let select_cols = if col_refs.is_empty() {
            "b.__pgt_row_id".to_string()
        } else {
            format!("b.__pgt_row_id, {col_refs}")
        };
        let sql = format!(
            "INSERT INTO {payload} ({target_cols}) SELECT $1, row_number() OVER (ORDER BY b.change_id) - 1, CASE b.action WHEN 'D' THEN 'DELETE' ELSE 'INSERT' END, {select_cols} FROM {source} b WHERE b.change_id > $2"
        );
        let count = Spi::connect_mut(|client| {
            let result = client
                .update(&sql, None, &[token.into(), start.into()])
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            Ok::<i64, PgTrickleError>(result.len() as i64)
        })?;
        let inserted = spi(Spi::get_one_with_args::<i64>(
            &format!("SELECT count(*)::bigint FROM {payload} WHERE batch_token = $1 AND action = 'INSERT'"),
            &[token.into()],
        ))?
        .unwrap_or(0);
        let deleted = count.saturating_sub(inserted);
        (count, inserted, deleted)
    } else {
        (0, 0, 0)
    };
    let graph_refresh_id = crate::refresh::current_graph_refresh_id();
    let source_boundary_digest = crate::refresh::current_source_boundary_digest();
    Spi::run_with_args(
        "INSERT INTO pgtrickle.pgt_output_delta_batches (pgt_id, database_instance_id, batch_token, producing_refresh_id, graph_refresh_id, mode, row_count, rows_inserted, rows_deleted, output_contract_digest, row_identity_version, source_boundary_digest) SELECT $1, database_instance_id, $2, $3, $4, $5, $6, $7, $8, output_contract_digest, row_identity_version, $9 FROM pgtrickle.pgt_output_delta_logs WHERE pgt_id = $1",
        &[
            meta.pgt_id.into(),
            token.into(),
            refresh_id.into(),
            graph_refresh_id.into(),
            mode.into(),
            payload_rows.into(),
            inserted.into(),
            deleted.into(),
            source_boundary_digest.into(),
        ],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_output_delta_logs SET log_head = $1 WHERE pgt_id = $2",
        &[token.into(), meta.pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    prune(meta.pgt_id)?;
    let _ = digest;
    Ok(())
}

fn prune(pgt_id: i64) -> Result<(), PgTrickleError> {
    let min_ack = spi(Spi::get_one_with_args::<i64>(
        "SELECT min(acknowledged_batch_token) FROM pgtrickle.pgt_output_delta_consumers WHERE pgt_id = $1 AND state IN ('ACTIVE', 'PAUSED')",
        &[pgt_id.into()],
    ))?;
    let Some(min_ack) = min_ack else {
        return Ok(());
    };
    let payload = format!("pgtrickle_changes.{}", quoted_ident(&payload_name(pgt_id)));
    Spi::run_with_args(
        &format!("DELETE FROM {payload} WHERE batch_token <= $1"),
        &[min_ack.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    Spi::run_with_args(
        "DELETE FROM pgtrickle.pgt_output_delta_batches WHERE pgt_id = $1 AND batch_token <= $2",
        &[pgt_id.into(), min_ack.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    Ok(())
}

/// Register an owner-only cursor over one external stream table.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
#[allow(clippy::type_complexity)]
pub fn register_output_delta_consumer(
    stream_table: pg_sys::Oid,
    consumer_name: &str,
    expected_contract_digest: Vec<u8>,
    start_position: default!(&str, "'RESNAPSHOT_REQUIRED'"),
) -> TableIterator<
    'static,
    (
        name!(consumer_id, pgrx::Uuid),
        name!(stream_table, String),
        name!(consumer_name, String),
        name!(delta_relation, String),
        name!(acknowledged_batch_token, i64),
        name!(output_contract_digest, Vec<u8>),
        name!(row_identity_version, i16),
        name!(state, String),
        name!(state_reason, Option<String>),
    ),
> {
    let result = (|| -> Result<_, PgTrickleError> {
        super::require_v098_capability(super::DELTA_V1_CAPABILITY)?;
        crate::api::recovery::assert_capture_ready()?;
        let meta = StreamTableMeta::get_by_relid(stream_table)?;
        authorized_stream(&meta)?;
        if !meta.orchestration_mode.eq_ignore_ascii_case("EXTERNAL") {
            return Err(delta_error(
                "PGT_EXT_ORCHESTRATION_MODE",
                "output-delta consumers require EXTERNAL orchestration",
            ));
        }
        let (digest, row_version) = contract(&meta)?;
        if digest != expected_contract_digest {
            return Err(delta_error(
                "PGT_EXT_CONTRACT_MISMATCH",
                "expected stream-table contract digest does not match",
            ));
        }
        if row_version <= 0 {
            return Err(delta_error(
                "PGT_EXT_CONTRACT_UNSUPPORTED",
                "row identity version is unknown",
            ));
        }
        let start = start_position.to_ascii_uppercase();
        if start != RESNAPSHOT_REQUIRED && start != "CURRENT" {
            return Err(delta_error(
                "PGT_EXT_TOKEN_INVALID",
                "start_position must be RESNAPSHOT_REQUIRED or CURRENT",
            ));
        }
        if start == "CURRENT" {
            let empty = spi(Spi::get_one_with_args::<bool>(
                "SELECT NOT is_populated FROM pgtrickle.pgt_stream_tables WHERE pgt_relid = $1",
                &[meta.pgt_relid.into()],
            ))?
            .unwrap_or(false);
            if !empty {
                return Err(delta_error(
                    "PGT_EXT_RESNAPSHOT_REQUIRED",
                    "CURRENT is valid only for a pristine empty stream table",
                ));
            }
        }
        ensure_buffer(&meta)?;
        let owner = outer_user_id();
        let delta_oid = ensure_payload(&meta, owner)?;
        let instance = database_instance_id()?;
        spi(Spi::run_with_args(
            "INSERT INTO pgtrickle.pgt_output_delta_logs (pgt_id, database_instance_id, delta_relid, output_contract_digest, row_identity_version) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (pgt_id) DO UPDATE SET database_instance_id = EXCLUDED.database_instance_id, delta_relid = EXCLUDED.delta_relid, output_contract_digest = EXCLUDED.output_contract_digest, row_identity_version = EXCLUDED.row_identity_version",
            &[
                meta.pgt_id.into(),
                instance.clone().into(),
                delta_oid.into(),
                digest.clone().into(),
                row_version.into(),
            ],
        ))?;
        let id = spi(Spi::get_one::<pgrx::Uuid>(
            "SELECT md5(clock_timestamp()::text || random()::text || txid_current()::text)::uuid",
        ))?
        .ok_or_else(|| {
            delta_error(
                "PGT_EXT_TOKEN_INVALID",
                "could not allocate consumer identity",
            )
        })?;
        let existing = Spi::connect(|client| {
            let rows = client
                .select(
                    "SELECT consumer_id FROM pgtrickle.pgt_output_delta_consumers \
                     WHERE pgt_id = $1 AND owner_oid = $2 AND consumer_name = $3",
                    None,
                    &[meta.pgt_id.into(), owner.into(), consumer_name.into()],
                )
                .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
            if rows.is_empty() {
                return Ok(None);
            }
            rows.first()
                .get::<pgrx::Uuid>(1)
                .map_err(|error| PgTrickleError::SpiError(error.to_string()))
        })?;
        let (id, state, reason, ack) = if let Some(existing) = existing {
            let existing_digest = spi(Spi::get_one_with_args::<Vec<u8>>("SELECT output_contract_digest FROM pgtrickle.pgt_output_delta_consumers WHERE consumer_id = $1", &[existing.into()]))?.unwrap_or_default();
            if existing_digest != digest {
                return Err(delta_error(
                    "PGT_EXT_CONTRACT_MISMATCH",
                    "consumer already exists with a different contract",
                ));
            }
            let state = spi(Spi::get_one_with_args::<String>(
                "SELECT state FROM pgtrickle.pgt_output_delta_consumers WHERE consumer_id = $1",
                &[existing.into()],
            ))?
            .ok_or_else(|| {
                delta_error(
                    "PGT_EXT_TOKEN_INVALID",
                    "consumer disappeared during registration",
                )
            })?;
            let reason = spi(Spi::get_one_with_args::<String>(
                "SELECT state_reason FROM pgtrickle.pgt_output_delta_consumers WHERE consumer_id = $1",
                &[existing.into()],
            ))?;
            let ack = spi(Spi::get_one_with_args::<i64>(
                "SELECT acknowledged_batch_token FROM pgtrickle.pgt_output_delta_consumers WHERE consumer_id = $1",
                &[existing.into()],
            ))?
            .unwrap_or(0);
            (existing, state, reason, ack)
        } else {
            let (state, reason) = if start == "CURRENT" {
                (ACTIVE, None)
            } else {
                (RESNAPSHOT_REQUIRED, Some(INITIAL_BASELINE_REQUIRED))
            };
            spi(Spi::run_with_args(
                "INSERT INTO pgtrickle.pgt_output_delta_consumers (consumer_id, pgt_id, database_instance_id, owner_oid, consumer_name, expected_contract_digest, output_contract_digest, row_identity_version, state, state_reason) VALUES ($1, $2, $3, $4, $5, $6, $6, $7, $8, $9)",
                &[
                    id.into(),
                    meta.pgt_id.into(),
                    instance.into(),
                    owner.into(),
                    consumer_name.into(),
                    digest.clone().into(),
                    row_version.into(),
                    state.into(),
                    reason.into(),
                ],
            ))?;
            (id, state.to_string(), reason.map(str::to_string), 0)
        };
        Ok(TableIterator::once((
            id,
            format!("{}.{}", meta.pgt_schema, meta.pgt_name),
            consumer_name.to_string(),
            format!("pgtrickle_changes.{}", payload_name(meta.pgt_id)),
            ack,
            digest,
            row_version,
            state,
            reason,
        )))
    })();
    match result {
        Ok(rows) => rows,
        Err(error) => raise(error),
    }
}

/// Return pending batch metadata in token order.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
#[allow(clippy::type_complexity)]
pub fn output_delta_batches(
    consumer_id: pgrx::Uuid,
    through_token: default!(Option<i64>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(batch_token, i64),
        name!(producing_refresh_id, i64),
        name!(graph_refresh_id, Option<i64>),
        name!(mode, String),
        name!(row_count, i64),
        name!(rows_inserted, i64),
        name!(rows_deleted, i64),
        name!(output_contract_digest, Vec<u8>),
        name!(row_identity_version, i16),
        name!(source_boundary_digest, Option<Vec<u8>>),
        name!(created_at, TimestampWithTimeZone),
    ),
> {
    let result = (|| -> Result<_, PgTrickleError> {
        super::require_v098_capability(super::DELTA_V1_CAPABILITY)?;
        let (_, meta) = consumer_owner(consumer_id)?;
        let state = spi(Spi::get_one_with_args::<String>(
            "SELECT state FROM pgtrickle.pgt_output_delta_consumers WHERE consumer_id = $1",
            &[consumer_id.into()],
        ))?
        .unwrap_or_default();
        if state != ACTIVE && state != PAUSED {
            return Err(delta_error(
                "PGT_EXT_RESNAPSHOT_REQUIRED",
                "consumer requires a resnapshot",
            ));
        }
        let ack = spi(Spi::get_one_with_args::<i64>("SELECT acknowledged_batch_token FROM pgtrickle.pgt_output_delta_consumers WHERE consumer_id = $1", &[consumer_id.into()]))?.unwrap_or(0);
        let head = spi(Spi::get_one_with_args::<i64>(
            "SELECT log_head FROM pgtrickle.pgt_output_delta_logs WHERE pgt_id = $1",
            &[meta.pgt_id.into()],
        ))?
        .unwrap_or(0);
        let through = through_token.unwrap_or(head);
        if through < ack || through > head {
            return Err(delta_error(
                "PGT_EXT_TOKEN_INVALID",
                "through_token is outside the output log",
            ));
        }
        if through == ack {
            return Ok(TableIterator::new(Vec::new()));
        }
        let rows = Spi::connect(|client| -> Result<_, PgTrickleError> {
            let table = spi(client.select("SELECT batch_token, producing_refresh_id, graph_refresh_id, mode, row_count, rows_inserted, rows_deleted, output_contract_digest, row_identity_version, source_boundary_digest, created_at FROM pgtrickle.pgt_output_delta_batches WHERE pgt_id = $1 AND batch_token > $2 AND batch_token <= $3 ORDER BY batch_token", None, &[meta.pgt_id.into(), ack.into(), through.into()]))?;
            let mut out = Vec::new();
            let mut expected = ack + 1;
            for row in table {
                let token = spi(row.get::<i64>(1))?
                    .ok_or_else(|| delta_error("PGT_EXT_DELTA_GAP", "missing batch token"))?;
                if token != expected {
                    return Err(delta_error(
                        "PGT_EXT_DELTA_GAP",
                        "output delta log has a gap",
                    ));
                }
                expected += 1;
                out.push((
                    token,
                    spi(row.get::<i64>(2))?.unwrap_or(0),
                    spi(row.get::<i64>(3))?,
                    spi(row.get::<String>(4))?.unwrap_or_default(),
                    spi(row.get::<i64>(5))?.unwrap_or(0),
                    spi(row.get::<i64>(6))?.unwrap_or(0),
                    spi(row.get::<i64>(7))?.unwrap_or(0),
                    spi(row.get::<Vec<u8>>(8))?.unwrap_or_default(),
                    spi(row.get::<i16>(9))?.unwrap_or(0),
                    spi(row.get::<Vec<u8>>(10))?,
                    spi(row.get::<TimestampWithTimeZone>(11))?.ok_or_else(|| {
                        delta_error("PGT_EXT_DELTA_GAP", "missing batch timestamp")
                    })?,
                ));
            }
            if expected != through + 1 {
                return Err(delta_error(
                    "PGT_EXT_DELTA_GAP",
                    "output delta log is incomplete",
                ));
            }
            Ok(out)
        })?;
        Ok(TableIterator::new(rows))
    })();
    match result {
        Ok(rows) => rows,
        Err(error) => raise(error),
    }
}

/// Advance a consumer cursor transactionally.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
pub fn ack_output_delta(consumer_id: pgrx::Uuid, through_token: i64, disposition: &str) -> String {
    let result = (|| -> Result<String, PgTrickleError> {
        super::require_v098_capability(super::DELTA_V1_CAPABILITY)?;
        let (_, meta) = consumer_owner(consumer_id)?;
        let state = spi(Spi::get_one_with_args::<String>(
            "SELECT state FROM pgtrickle.pgt_output_delta_consumers WHERE consumer_id = $1",
            &[consumer_id.into()],
        ))?
        .unwrap_or_default();
        if state != ACTIVE {
            return Err(delta_error(
                "PGT_EXT_RESNAPSHOT_REQUIRED",
                "consumer is not ACTIVE",
            ));
        }
        let ack = spi(Spi::get_one_with_args::<i64>("SELECT acknowledged_batch_token FROM pgtrickle.pgt_output_delta_consumers WHERE consumer_id = $1", &[consumer_id.into()]))?.unwrap_or(0);
        let head = spi(Spi::get_one_with_args::<i64>(
            "SELECT log_head FROM pgtrickle.pgt_output_delta_logs WHERE pgt_id = $1",
            &[meta.pgt_id.into()],
        ))?
        .unwrap_or(0);
        if through_token < ack || through_token > head {
            return Err(delta_error(
                "PGT_EXT_ACK_OUT_OF_ORDER",
                "acknowledgement is outside the output log",
            ));
        }
        let invalidated = spi(Spi::get_one_with_args::<bool>("SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_output_delta_batches WHERE pgt_id = $1 AND batch_token > $2 AND batch_token <= $3 AND mode = 'FULL_INVALIDATION')", &[meta.pgt_id.into(), ack.into(), through_token.into()]))?.unwrap_or(false);
        let disposition = disposition.to_ascii_uppercase();
        if disposition == "APPLIED" && invalidated {
            return Err(delta_error(
                "PGT_EXT_ACK_OUT_OF_ORDER",
                "FULL_INVALIDATION requires RESYNCHRONIZED",
            ));
        }
        if disposition != "APPLIED" && disposition != "RESYNCHRONIZED" {
            return Err(delta_error(
                "PGT_EXT_TOKEN_INVALID",
                "disposition must be APPLIED or RESYNCHRONIZED",
            ));
        }
        spi(Spi::run_with_args(
            "UPDATE pgtrickle.pgt_output_delta_consumers SET acknowledged_batch_token = $1, acknowledged_at = now(), updated_at = now() WHERE consumer_id = $2",
            &[through_token.into(), consumer_id.into()],
        ))?;
        prune(meta.pgt_id)?;
        Ok(disposition)
    })();
    match result {
        Ok(value) => value,
        Err(error) => raise(error),
    }
}

/// Begin a transaction-scoped full baseline for a consumer.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
pub fn begin_output_delta_resnapshot(
    consumer_id: pgrx::Uuid,
) -> TableIterator<
    'static,
    (
        name!(stream_table, String),
        name!(log_head, i64),
        name!(output_contract_digest, Vec<u8>),
        name!(row_identity_version, i16),
        name!(resnapshot_token, pgrx::Uuid),
    ),
> {
    let result = (|| -> Result<_, PgTrickleError> {
        super::require_v098_capability(super::DELTA_V1_CAPABILITY)?;
        let (_, meta) = consumer_owner(consumer_id)?;
        spi(Spi::get_one_with_args::<i64>(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1 FOR UPDATE",
            &[meta.pgt_id.into()],
        ))?
        .ok_or_else(|| {
            delta_error(
                "PGT_EXT_REFRESH_BUSY",
                "stream table disappeared during resnapshot",
            )
        })?;
        let state = spi(Spi::get_one_with_args::<String>(
            "SELECT state FROM pgtrickle.pgt_output_delta_consumers WHERE consumer_id = $1",
            &[consumer_id.into()],
        ))?
        .unwrap_or_default();
        if state != RESNAPSHOT_REQUIRED && state != INVALIDATED {
            return Err(delta_error(
                "PGT_EXT_TOKEN_INVALID",
                "consumer does not require a resnapshot",
            ));
        }
        let head = spi(Spi::get_one_with_args::<i64>(
            "SELECT log_head FROM pgtrickle.pgt_output_delta_logs WHERE pgt_id = $1",
            &[meta.pgt_id.into()],
        ))?
        .unwrap_or(0);
        let digest = spi(Spi::get_one_with_args::<Vec<u8>>(
            "SELECT output_contract_digest FROM pgtrickle.pgt_output_delta_logs WHERE pgt_id = $1",
            &[meta.pgt_id.into()],
        ))?
        .unwrap_or_default();
        let version = spi(Spi::get_one_with_args::<i16>(
            "SELECT row_identity_version FROM pgtrickle.pgt_output_delta_logs WHERE pgt_id = $1",
            &[meta.pgt_id.into()],
        ))?
        .unwrap_or(0);
        let token = spi(Spi::get_one::<pgrx::Uuid>(
            "SELECT md5(clock_timestamp()::text || random()::text || txid_current()::text)::uuid",
        ))?
        .ok_or_else(|| {
            delta_error(
                "PGT_EXT_TOKEN_INVALID",
                "could not allocate resnapshot token",
            )
        })?;
        spi(Spi::run_with_args(
            "INSERT INTO pgtrickle.pgt_output_delta_resnapshots (resnapshot_token, consumer_id, pgt_id, log_head) VALUES ($1, $2, $3, $4)",
            &[
                token.into(),
                consumer_id.into(),
                meta.pgt_id.into(),
                head.into(),
            ],
        ))?;
        Ok(TableIterator::once((
            format!("{}.{}", meta.pgt_schema, meta.pgt_name),
            head,
            digest,
            version,
            token,
        )))
    })();
    match result {
        Ok(rows) => rows,
        Err(error) => raise(error),
    }
}

/// Commit a resnapshot baseline and activate the consumer.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
pub fn ack_output_delta_resnapshot(
    consumer_id: pgrx::Uuid,
    resnapshot_token: pgrx::Uuid,
) -> String {
    let result = (|| -> Result<String, PgTrickleError> {
        super::require_v098_capability(super::DELTA_V1_CAPABILITY)?;
        let (_, meta) = consumer_owner(consumer_id)?;
        let head = spi(Spi::get_one_with_args::<i64>("SELECT log_head FROM pgtrickle.pgt_output_delta_resnapshots WHERE resnapshot_token = $1 AND consumer_id = $2 AND pgt_id = $3", &[resnapshot_token.into(), consumer_id.into(), meta.pgt_id.into()]))?.ok_or_else(|| delta_error("PGT_EXT_TOKEN_INVALID", "unknown or replayed resnapshot token"))?;
        spi(Spi::run_with_args(
            "UPDATE pgtrickle.pgt_output_delta_consumers SET state = 'ACTIVE', state_reason = NULL, acknowledged_batch_token = $1, acknowledged_at = now(), updated_at = now() WHERE consumer_id = $2",
            &[head.into(), consumer_id.into()],
        ))?;
        spi(Spi::run_with_args(
            "DELETE FROM pgtrickle.pgt_output_delta_resnapshots WHERE resnapshot_token = $1",
            &[resnapshot_token.into()],
        ))?;
        prune(meta.pgt_id)?;
        Ok(ACTIVE.to_string())
    })();
    match result {
        Ok(value) => value,
        Err(error) => raise(error),
    }
}

/// Inspect consumers visible to the current stream-table owner.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
#[allow(clippy::type_complexity)]
pub fn output_delta_consumer_status() -> TableIterator<
    'static,
    (
        name!(consumer_id, pgrx::Uuid),
        name!(stream_table, String),
        name!(consumer_name, String),
        name!(delta_relation, String),
        name!(state, String),
        name!(state_reason, Option<String>),
        name!(acknowledged_batch_token, i64),
        name!(log_head, i64),
        name!(batch_lag, i64),
        name!(output_contract_digest, Vec<u8>),
        name!(row_identity_version, i16),
    ),
> {
    let result = (|| -> Result<_, PgTrickleError> {
        super::require_v098_capability(super::DELTA_V1_CAPABILITY)?;
        let rows = Spi::connect(|client| -> Result<_, PgTrickleError> {
            let table = spi(client.select("SELECT c.consumer_id, format('%I.%I', st.pgt_schema, st.pgt_name), c.consumer_name, format('%I.%I', 'pgtrickle_changes', 'output_delta_' || c.pgt_id), c.state, c.state_reason, c.acknowledged_batch_token, l.log_head, l.output_contract_digest, c.row_identity_version FROM pgtrickle.pgt_output_delta_consumers c JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = c.pgt_id JOIN pgtrickle.pgt_output_delta_logs l ON l.pgt_id = c.pgt_id WHERE c.owner_oid = $1 OR EXISTS (SELECT 1 FROM pg_roles r WHERE r.oid = $1 AND r.rolsuper) ORDER BY c.consumer_name", None, &[outer_user_id().into()]))?;
            let mut out = Vec::new();
            for row in table {
                let ack = spi(row.get::<i64>(7))?.unwrap_or(0);
                let head = spi(row.get::<i64>(8))?.unwrap_or(0);
                out.push((
                    spi(row.get::<pgrx::Uuid>(1))?.ok_or_else(|| {
                        delta_error("PGT_EXT_TOKEN_INVALID", "missing consumer id")
                    })?,
                    spi(row.get::<String>(2))?.unwrap_or_default(),
                    spi(row.get::<String>(3))?.unwrap_or_default(),
                    spi(row.get::<String>(4))?.unwrap_or_default(),
                    spi(row.get::<String>(5))?.unwrap_or_default(),
                    spi(row.get::<String>(6))?,
                    ack,
                    head,
                    head.saturating_sub(ack),
                    spi(row.get::<Vec<u8>>(9))?.unwrap_or_default(),
                    spi(row.get::<i16>(10))?.unwrap_or(0),
                ));
            }
            Ok(out)
        })?;
        Ok(TableIterator::new(rows))
    })();
    match result {
        Ok(rows) => rows,
        Err(error) => raise(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_payload_name_is_oid_scoped() {
        assert_eq!(payload_name(42), "output_delta_42");
    }

    #[test]
    fn test_delta_batch_mode_fails_closed_when_capture_is_missing() {
        assert_eq!(batch_mode(3, 0), "FULL_INVALIDATION");
        assert_eq!(batch_mode(0, 0), "EXACT");
        assert_eq!(batch_mode(3, 2), "EXACT");
    }
}
