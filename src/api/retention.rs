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

/// Preserve non-eligibility errors while giving contract violations a targeted hint.
fn retention_eligibility_error(error: PgTrickleError) -> PgTrickleError {
    match error {
        PgTrickleError::InvalidArgument(reason) => PgTrickleError::RetentionQueryIneligible(reason),
        other => other,
    }
}

/// Arm automatic delete-with-source after a protected FULL materialization.
///
/// Initial population and reinitialization lock the source before reading
/// it and retain the locks through commit. Only such a baseline authorizes
/// automatic capture; never retrofit an unverified live CDC frontier.
#[cfg(not(test))]
pub(crate) fn arm_automatic_trim_after_full_snapshot(pgt_id: i64) -> Result<(), PgTrickleError> {
    use crate::dag::RefreshMode;

    let st = StreamTableMeta::get_by_id(pgt_id)?
        .ok_or_else(|| PgTrickleError::NotFound(format!("stream table {pgt_id}")))?;
    let opted_out = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_dependencies
         WHERE pgt_id = $1 AND column_snapshot->>'partition_auto_disabled' = 'true')",
        &[pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or(false);
    if st.refresh_mode != RefreshMode::Differential
        || !st.is_populated
        || st.needs_reinit
        || opted_out
        || load_policy(pgt_id)?.is_some()
    {
        return Ok(());
    }
    let source_oid = match retention::retention_source(&st) {
        Ok(source) => source,
        Err(PgTrickleError::InvalidArgument(_)) => return Ok(()),
        Err(error) => return Err(error),
    };

    // Only a plain single-column RANGE key is a safe range-delete predicate.
    // The partition layout validator below additionally rejects default,
    // overlapping, non-durable and unaligned descendants.
    let key = Spi::get_one_with_args::<String>(
        "SELECT a.attname::text FROM pg_catalog.pg_partitioned_table p
         JOIN pg_catalog.pg_attribute a
           ON a.attrelid = p.partrelid AND a.attnum = p.partattrs[0]
         WHERE p.partrelid = $1 AND p.partstrat = 'r'
           AND p.partnatts = 1 AND p.partexprs IS NULL
           AND a.attnotnull AND NOT a.attisdropped",
        &[source_oid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    let Some(key) = key else { return Ok(()) };

    let candidate = crate::refresh::with_stream_owner(&st, || {
        let tree = crate::dvm::parser::parse_defining_query(&st.defining_query)?;
        let candidate = match retention::query_group_candidate(&tree, source_oid.to_u32(), &key) {
            Ok(candidate) => candidate,
            Err(_) => return Ok(None),
        };
        for result in [
            retention::validate_retention_group_collations(&tree, source_oid),
            retention::validate_builtin_aggregates(&st.defining_query, &tree),
        ] {
            match result {
                Ok(()) => {}
                Err(PgTrickleError::InvalidArgument(_)) => return Ok(None),
                Err(error) => return Err(error),
            }
        }
        Ok(Some(candidate))
    })?;
    let Some(candidate) = candidate else {
        return Ok(());
    };

    let cutoff = match candidate.source_type_oid {
        1082 => RetentionBound::Date(0),
        1114 => RetentionBound::Timestamp(0),
        1184 => RetentionBound::Timestamptz(0),
        _ => return Ok(()),
    };
    match retention::validate_partition_layout_trusted(source_oid, &candidate, cutoff) {
        Ok(_) => {}
        Err(PgTrickleError::InvalidArgument(_)) => return Ok(()),
        Err(error) => return Err(error),
    }
    match retention::validate_retention_storage(
        &st,
        source_oid,
        &candidate,
        RetentionMode::Trim,
        true,
    ) {
        Ok(()) => {}
        Err(PgTrickleError::InvalidArgument(_)) => return Ok(()),
        Err(error) => return Err(error),
    }

    let state = RetentionState {
        version: 1,
        mode: RetentionMode::Trim,
        automatic: true,
        cutoff_unbounded: true,
        candidate,
        cutoff,
        query_hash: crate::catalog::compute_defining_query_hash(&st.defining_query),
        sealed: Vec::new(),
        pending: None,
    };
    state.validate().map_err(|e| {
        PgTrickleError::InvalidArgument(format!("invalid automatic retention state: {e:?}"))
    })?;
    // This write and trigger installation are atomic with the initial FULL
    // materialization. A failed setup aborts CREATE rather than trusting a
    // partial capture or silently switching to an unsafe range delete.
    crate::catalog::store_column_snapshot_for_pgt_id(pgt_id, source_oid)?;
    let value = pgrx::JsonB(
        serde_json::to_value(&state).map_err(|e| PgTrickleError::InternalError(e.to_string()))?,
    );
    let updated = Spi::get_one_with_args::<i64>(
        "UPDATE pgtrickle.pgt_dependencies SET column_snapshot =
         pg_catalog.jsonb_set(column_snapshot, '{partition_retention}', $3, true)
         WHERE pgt_id = $1 AND source_relid = $2 AND source_type = 'TABLE'
           AND NOT (column_snapshot ? 'partition_retention')
         RETURNING pgt_id",
        &[pgt_id.into(), source_oid.into(), value.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    if updated != Some(pgt_id) {
        return Err(PgTrickleError::InvalidArgument(
            "automatic partition capture was changed during initialization".into(),
        ));
    }
    crate::cdc::rebuild_cdc_trigger(
        source_oid,
        &crate::config::pg_trickle_change_buffer_schema(),
    )?;
    retention::install_partition_truncate_guards(source_oid)?;
    retention::ensure_retention_capture(pgt_id)?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn arm_automatic_trim_after_full_snapshot(_pgt_id: i64) -> Result<(), PgTrickleError> {
    Ok(())
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
            "UPDATE pgtrickle.pgt_dependencies
             SET column_snapshot = pg_catalog.jsonb_set(
                 COALESCE(column_snapshot, '{}'::jsonb) - 'partition_retention',
                 '{partition_auto_disabled}', 'true'::jsonb, true)
             WHERE pgt_id = $1 AND source_type = 'TABLE'",
            &[st.pgt_id.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
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
    // Omitted cutoffs make partition removal itself the retention signal.
    // Use a typed placeholder in persisted state (v1) for compatibility;
    // alignment and the live/sealed boundary remain independently validated.
    let supplied_cutoff = cutoff.filter(|cutoff| !cutoff.is_empty());
    let source_oid = retention::retention_source(&st)?;
    let candidate = crate::refresh::with_stream_owner(&st, || {
        let tree = crate::dvm::parser::parse_defining_query(&st.defining_query)?;
        let candidate = retention::query_group_candidate(&tree, source_oid.to_u32(), key)
            .map_err(|error| PgTrickleError::RetentionQueryIneligible(error.to_string()))?;
        retention::validate_retention_group_collations(&tree, source_oid)
            .map_err(retention_eligibility_error)?;
        retention::validate_builtin_aggregates(&st.defining_query, &tree)
            .map_err(retention_eligibility_error)?;
        Ok(candidate)
    })?;
    let implicit_trim_cutoff = match candidate.source_type_oid {
        1082 => "2000-01-01",
        1114 => "2000-01-01 00:00:00",
        1184 => "2000-01-01 00:00:00+00",
        _ => {
            return Err(PgTrickleError::InvalidArgument(
                "unsupported retention key type".into(),
            ));
        }
    };
    let cutoff_text = match supplied_cutoff {
        Some(value) => value,
        None => implicit_trim_cutoff,
    };
    let cutoff = retention::parse_retention_cutoff(candidate.source_type_oid, cutoff_text)?;
    let cutoff_unbounded = supplied_cutoff.is_none();
    let query_hash = crate::catalog::compute_defining_query_hash(&st.defining_query);
    let previous = load_policy(st.pgt_id)?;
    if previous
        .as_ref()
        .is_some_and(|(_, state)| state.pending.is_some())
    {
        return Err(PgTrickleError::InvalidArgument(
            "partition retention cannot be changed while a concurrent detach is pending".into(),
        ));
    }
    let old_policy = previous.clone();
    let state = if let Some((previous_source, mut previous)) = previous {
        if !previous.sealed.is_empty() {
            if previous.mode != mode
                || previous.cutoff_unbounded != cutoff_unbounded
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
                automatic: false,
                cutoff_unbounded,
                candidate,
                cutoff,
                query_hash,
                sealed: Vec::new(),
                pending: None,
            }
        }
    } else {
        RetentionState {
            version: 1,
            mode,
            automatic: false,
            cutoff_unbounded,
            candidate,
            cutoff,
            query_hash,
            sealed: Vec::new(),
            pending: None,
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
         pg_catalog.jsonb_set(COALESCE(column_snapshot, '{}'::jsonb) - 'partition_auto_disabled', '{partition_retention}', $3, true) \
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
    cutoff: default!(Option<&str>, "NULL"),
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
        set_policy(name, policy, Some(source_column), cutoff)
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
        // The internal delete-with-source fast path is not an explicit TRIM
        // policy. Report it separately so users can distinguish it from RETAIN.
        if state.automatic {
            return Ok(pgrx::JsonB(serde_json::json!({
                "enabled": false,
                "automatic_delete_with_source": true,
                "source_column": state.candidate.source_column,
                "group_column": state.candidate.output_column,
            })));
        }
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
        let mut result = serde_json::json!({
            "enabled": true, "policy": state.mode, "source": source_name,
            "source_column": state.candidate.source_column, "group_column": state.candidate.output_column,
            "bucket": state.candidate.bucket,
            "cutoff": if state.cutoff_unbounded { None } else { Some(display_bound(state.cutoff)?) },
            "sealed_ranges": ranges,
        });
        if let Some(pending) = state.pending {
            result["pending_detach"] = serde_json::json!({
                "parent_oid": pending.parent_oid, "target_oid": pending.target_oid,
                "lower": display_bound(pending.range.lower)?, "upper": display_bound(pending.range.upper)?,
            });
        }
        Ok(pgrx::JsonB(result))
    })();
    result.unwrap_or_else(|error| super::raise_error_with_context(error))
}
