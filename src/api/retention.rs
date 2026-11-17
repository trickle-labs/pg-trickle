//! Per-stream partition-retention configuration and inspection.

use pgrx::prelude::*;

use crate::catalog::StreamTableMeta;
use crate::dvm::retention::{self, RetentionBound, RetentionMode, RetentionState};
use crate::error::PgTrickleError;

use super::{helpers, security_context};

fn owned_stream(name: &str) -> Result<StreamTableMeta, PgTrickleError> {
    let caller =
        security_context::capture_caller_context(security_context::EntryContext::SecurityDefiner)?;
    let (_, _, st) = helpers::resolve_owned_stream_table_with_caller(name, &caller)?;
    Ok(st)
}

fn load_policy(pgt_id: i64) -> Result<Option<(pg_sys::Oid, RetentionState)>, PgTrickleError> {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT source_relid, column_snapshot->'partition_retention' FROM pgtrickle.pgt_dependencies \
             WHERE pgt_id = $1 AND column_snapshot ? 'partition_retention' ORDER BY source_relid",
            None, &[pgt_id.into()],
        ).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        let mut policy = None;
        for row in rows {
            if policy.is_some() {
                return Err(PgTrickleError::InvalidArgument(
                    "multiple partition retention states exist".into(),
                ));
            }
            let source = row
                .get::<pg_sys::Oid>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InvalidArgument("retention source is missing".into())
                })?;
            let value = row
                .get::<pgrx::JsonB>(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InvalidArgument("partition retention state is missing".into())
                })?;
            let state: RetentionState = serde_json::from_value(value.0).map_err(|e| {
                PgTrickleError::InvalidArgument(format!("invalid partition retention state: {e}"))
            })?;
            state.validate().map_err(|e| {
                PgTrickleError::InvalidArgument(format!("invalid partition retention state: {e:?}"))
            })?;
            policy = Some((source, state));
        }
        Ok(policy)
    })
}

fn set_policy(
    name: &str,
    policy: &str,
    source_column: Option<&str>,
    cutoff: Option<&str>,
) -> Result<(), PgTrickleError> {
    let caller =
        security_context::capture_caller_context(security_context::EntryContext::SecurityDefiner)?;
    let (_, _, initial) = helpers::resolve_owned_stream_table_with_caller(name, &caller)?;
    super::alter::lock_alter_target(initial.pgt_id)?;
    let (_, _, st) = helpers::resolve_owned_stream_table_with_caller(name, &caller)?;
    if st.pgt_id != initial.pgt_id {
        return Err(PgTrickleError::InvalidArgument(
            "stream table changed while acquiring its lock".into(),
        ));
    }
    let requested = policy.trim().to_ascii_uppercase();
    if requested == "OFF" {
        if source_column.is_some() || cutoff.is_some() {
            return Err(PgTrickleError::InvalidArgument(
                "OFF does not accept a source column or cutoff".into(),
            ));
        }
        retention::ensure_history_reconstructible(st.pgt_id)?;
        let previous = load_policy(st.pgt_id)?;
        if let Some((source_oid, _)) = &previous {
            crate::cdc::lock_source_relations(&[*source_oid])?;
        }
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_dependencies SET column_snapshot = column_snapshot - 'partition_retention' \
             WHERE pgt_id = $1 AND column_snapshot ? 'partition_retention'",
            &[st.pgt_id.into()],
        ).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        if let Some((source_oid, _)) = previous {
            crate::cdc::rebuild_cdc_trigger(
                source_oid,
                &crate::config::pg_trickle_change_buffer_schema(),
            )?;
            retention::remove_partition_truncate_guards_if_unused(source_oid)?;
        }
        crate::shmem::signal_dag_invalidation(st.pgt_id);
        crate::shmem::bump_cache_generation();
        return Ok(());
    }
    let mode = match requested.as_str() {
        "TRIM" => RetentionMode::Trim,
        "RETAIN" => RetentionMode::Retain,
        _ => {
            return Err(PgTrickleError::InvalidArgument(
                "retention policy must be TRIM, RETAIN, or OFF".into(),
            ));
        }
    };
    let key = source_column.filter(|key| !key.is_empty()).ok_or_else(|| {
        PgTrickleError::InvalidArgument("retention requires a source column".into())
    })?;
    let cutoff = cutoff.filter(|cutoff| !cutoff.is_empty()).ok_or_else(|| {
        PgTrickleError::InvalidArgument("retention requires an explicit cutoff".into())
    })?;
    let source_oid = retention::retention_source(&st)?;
    let candidate = crate::refresh::with_stream_owner(&st, || {
        let tree = crate::dvm::parser::parse_defining_query(&st.defining_query)?;
        let candidate =
            retention::query_group_candidate(&tree, source_oid.to_u32(), key).map_err(|error| {
                PgTrickleError::InvalidArgument(format!(
                    "retention query is not eligible: {error:?}"
                ))
            })?;
        retention::validate_builtin_aggregates(&st.defining_query, &tree)?;
        Ok(candidate)
    })?;
    let cutoff = retention::parse_retention_cutoff(candidate.source_type_oid, cutoff)?;
    let query_hash = crate::catalog::compute_defining_query_hash(&st.defining_query);
    let previous = load_policy(st.pgt_id)?;
    let old_policy = previous.clone();
    let state = if let Some((previous_source, mut previous)) = previous {
        if !previous.sealed.is_empty() {
            if previous.mode != mode
                || previous_source != source_oid
                || previous.candidate != candidate
                || previous.query_hash != query_hash
            {
                return Err(PgTrickleError::InvalidArgument(
                    "retained partition history prevents changing its policy, source, or group mapping".into(),
                ));
            }
            previous.advance_cutoff(cutoff).map_err(|error| {
                PgTrickleError::InvalidArgument(format!("invalid retention cutoff: {error:?}"))
            })?;
            previous
        } else {
            RetentionState {
                version: 1,
                mode,
                candidate,
                cutoff,
                query_hash,
                sealed: Vec::new(),
            }
        }
    } else {
        RetentionState {
            version: 1,
            mode,
            candidate,
            cutoff,
            query_hash,
            sealed: Vec::new(),
        }
    };
    state.validate().map_err(|error| {
        PgTrickleError::InvalidArgument(format!("invalid retention state: {error:?}"))
    })?;
    retention::validate_partition_layout_trusted(source_oid, &state.candidate, state.cutoff)?;
    retention::ensure_no_live_sealed_overlap(source_oid, &state, None)?;
    retention::validate_retention_storage(&st, source_oid, &state.candidate, mode, true)?;
    let capture_valid = if old_policy.is_some() {
        match retention::ensure_retention_capture(st.pgt_id) {
            Ok(()) => true,
            Err(PgTrickleError::InvalidArgument(_)) => false,
            Err(error) => return Err(error),
        }
    } else {
        false
    };
    let needs_baseline = !capture_valid;
    if needs_baseline
        && old_policy
            .as_ref()
            .is_some_and(|(_, old)| !old.sealed.is_empty())
    {
        return Err(PgTrickleError::InvalidArgument(
            "retained history cannot be reconfigured after partition CDC capture became incomplete"
                .into(),
        ));
    }
    if !needs_baseline
        && old_policy
            .as_ref()
            .is_some_and(|(source, old)| *source == source_oid && *old == state)
    {
        return Ok(());
    }
    crate::catalog::store_column_snapshot_for_pgt_id(st.pgt_id, source_oid)?;
    let value = pgrx::JsonB(
        serde_json::to_value(&state).map_err(|e| PgTrickleError::InternalError(e.to_string()))?,
    );
    let updated = Spi::get_one_with_args::<i64>(
        "UPDATE pgtrickle.pgt_dependencies SET column_snapshot = \
         pg_catalog.jsonb_set(COALESCE(column_snapshot, '{}'::jsonb), '{partition_retention}', $3, true) \
         WHERE pgt_id = $1 AND source_relid = $2 RETURNING pgt_id",
        &[st.pgt_id.into(), source_oid.into(), value.into()],
    ).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    if updated != Some(st.pgt_id) {
        return Err(PgTrickleError::InvalidArgument(
            "retention source changed during configuration".into(),
        ));
    }
    if needs_baseline {
        crate::cdc::lock_source_relations(&[source_oid])?;
        crate::cdc::rebuild_cdc_trigger(
            source_oid,
            &crate::config::pg_trickle_change_buffer_schema(),
        )?;
        retention::install_partition_truncate_guards(source_oid)?;
        retention::ensure_retention_capture(st.pgt_id)?;
        StreamTableMeta::mark_for_reinitialize(st.pgt_id)?;
        let mut refreshed = StreamTableMeta::get_by_id(st.pgt_id)?
            .ok_or_else(|| PgTrickleError::NotFound(format!("stream table {}", st.pgt_id)))?;
        // The catalog read can use the caller's snapshot before our SPI update.
        refreshed.needs_reinit = true;
        let safe_bound = crate::cdc::get_current_wal_lsn()?;
        let result = crate::refresh::with_refresh_context(
            crate::refresh::RefreshContext::graph(&safe_bound, crate::refresh::FullPolicy::Allow),
            || {
                super::refresh_ops::execute_manual_refresh(
                    &refreshed,
                    &st.pgt_schema,
                    &st.pgt_name,
                    &[source_oid],
                )
            },
        )?;
        if !matches!(result.action.as_str(), "FULL" | "REINITIALIZE") {
            return Err(PgTrickleError::InvalidArgument(format!(
                "retention CDC initialization performed unexpected refresh mode {}",
                result.action
            )));
        }
    }
    crate::shmem::signal_dag_invalidation(st.pgt_id);
    crate::shmem::bump_cache_generation();
    Ok(())
}

/// Configure or disable an explicit partition-retention policy for a stream.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
fn set_stream_table_partition_retention(
    name: &str,
    policy: &str,
    source_column: default!(Option<&str>, "NULL"),
    cutoff: default!(Option<&str>, "NULL"),
) {
    if let Err(error) = set_policy(name, policy, source_column, cutoff) {
        super::raise_error_with_context(error);
    }
}

/// Create and configure a differential stream in one transaction.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
fn create_stream_table_with_retention(
    name: &str,
    query: &str,
    policy: &str,
    source_column: &str,
    cutoff: &str,
    schedule: default!(Option<&str>, "'calculated'"),
) {
    let result = (|| -> Result<(), PgTrickleError> {
        if !matches!(
            policy.trim().to_ascii_uppercase().as_str(),
            "TRIM" | "RETAIN"
        ) {
            return Err(PgTrickleError::InvalidArgument(
                "a new retention stream requires TRIM or RETAIN".into(),
            ));
        }
        super::alter::create_stream_table_impl(super::alter::CreateStreamTableOptions {
            name,
            query,
            schedule,
            refresh_mode_str: "DIFFERENTIAL",
            initialize: true,
            orchestration_mode: Some("MANAGED"),
            entry_context: Some(security_context::EntryContext::SecurityDefiner),
            ..Default::default()
        })?;
        set_policy(name, policy, Some(source_column), Some(cutoff))
    })();
    if let Err(error) = result {
        super::raise_error_with_context(error);
    }
}

fn display_bound(bound: RetentionBound) -> Result<String, PgTrickleError> {
    let sql = format!("SELECT ({})::text", retention::retention_bound_sql(bound));
    Spi::get_one::<String>(&sql) // nosemgrep: rust.spi.get_one.dynamic-format — expression contains only validated numeric bounds.
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .ok_or_else(|| PgTrickleError::InternalError("retention bound is missing".into()))
}

/// Return the effective retention policy and its sealed ranges.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
fn get_stream_table_partition_retention(name: &str) -> pgrx::JsonB {
    let result = (|| -> Result<pgrx::JsonB, PgTrickleError> {
        let st = owned_stream(name)?;
        let Some((source, state)) = load_policy(st.pgt_id)? else {
            return Ok(pgrx::JsonB(serde_json::json!({"enabled": false})));
        };
        let source_name = Spi::get_one_with_args::<String>(
            "SELECT pg_catalog.format('%I.%I', n.nspname, c.relname) FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = $1",
            &[source.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .unwrap_or_else(|| format!("oid:{}", source.to_u32()));
        let ranges = state
            .sealed
            .iter()
            .map(|range| {
                Ok(serde_json::json!({
                    "lower": display_bound(range.lower)?, "upper": display_bound(range.upper)?,
                }))
            })
            .collect::<Result<Vec<_>, PgTrickleError>>()?;
        Ok(pgrx::JsonB(serde_json::json!({
            "enabled": true, "policy": state.mode, "source": source_name,
            "source_column": state.candidate.source_column, "group_column": state.candidate.output_column,
            "bucket": state.candidate.bucket, "cutoff": display_bound(state.cutoff)?, "sealed_ranges": ranges,
        })))
    })();
    result.unwrap_or_else(|error| super::raise_error_with_context(error))
}
