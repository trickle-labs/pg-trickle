//! Internal helper functions shared across the API surface.
//!
//! CDC setup/teardown, name parsing, validation, cycle detection,
//! DDL generation, auxiliary column injection, and utility functions.

use super::*;

pub(super) fn resolve_source_oid(source: &str) -> Result<pg_sys::Oid, PgTrickleError> {
    let oid = Spi::get_one_with_args::<pg_sys::Oid>("SELECT $1::regclass::oid", &[source.into()])
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .ok_or_else(|| PgTrickleError::NotFound(format!("relation '{}' does not exist", source)))?;
    Ok(oid)
}

/// Verify that the invoker may create the requested stream table in `schema`.
pub(super) fn validate_output_schema_create(schema: &str) -> Result<(), PgTrickleError> {
    let invoker = outer_user_id();
    let allowed = Spi::get_one_with_args::<bool>(
        "SELECT has_schema_privilege($1, $2, 'CREATE')",
        &[invoker.into(), schema.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or(false);

    if allowed {
        Ok(())
    } else {
        Err(PgTrickleError::PermissionDenied(format!(
            "permission denied for schema {schema}: CREATE privilege is required to create a stream table"
        )))
    }
}

/// Verify the invoker can read every relation in the defining query.
pub(super) fn validate_source_access(
    source_relids: &[(pg_sys::Oid, String)],
) -> Result<(), PgTrickleError> {
    let invoker = outer_user_id();
    for (relid, _) in source_relids {
        let allowed = Spi::get_one_with_args::<bool>(
            "SELECT has_table_privilege($1, $2, 'SELECT') \
             AND has_schema_privilege($1, c.relnamespace, 'USAGE') \
             FROM pg_class c WHERE c.oid = $2",
            &[invoker.into(), (*relid).into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .unwrap_or(false);
        if !allowed {
            return Err(PgTrickleError::PermissionDenied(format!(
                "permission denied for source relation with OID {relid}: SELECT and schema USAGE are required"
            )));
        }
    }
    Ok(())
}

/// Make a successfully created stream table belong to its invoker.
pub(super) fn transfer_output_table_ownership(
    schema: &str,
    table_name: &str,
) -> Result<(), PgTrickleError> {
    let invoker_name = outer_user_name()?;
    let sql = format!(
        "ALTER TABLE {}.{} OWNER TO {}",
        quote_identifier(schema),
        quote_identifier(table_name),
        quote_identifier(&invoker_name),
    );
    Spi::run(&sql).map_err(|e| {
        PgTrickleError::SpiError(format!("Failed to transfer stream table ownership: {e}"))
    })
}

pub(crate) fn outer_user_name() -> Result<String, PgTrickleError> {
    let invoker = outer_user_id();
    Spi::get_one_with_args::<String>(
        "SELECT rolname::text FROM pg_roles WHERE oid = $1",
        &[invoker.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| PgTrickleError::NotFound(format!("Role with OID {invoker} not found")))
}

pub(crate) fn outer_user_id() -> pg_sys::Oid {
    unsafe {
        // SAFETY: PostgreSQL invokes SQL functions on its main backend thread;
        // the outer user remains the stream-table author throughout the call.
        pg_sys::GetOuterUserId()
    }
}

/// Run caller-controlled SQL with a captured invoker search path, restoring
/// whatever `search_path` was actually active immediately before this call
/// (not a hard-coded pinned-path guess) afterwards.
///
/// LSEC-7 (v0.87.9): restoring the *exact prior* value — rather than always
/// resetting to the pinned `pgtrickle, pg_catalog, pg_temp` definer path —
/// makes this helper safely nestable and callable more than once per
/// function. A caller that has itself already switched the ambient path to
/// something else (e.g. an outer `with_invoker_search_path` scoping an
/// entire SECURITY DEFINER call) gets that value back, instead of every
/// call silently collapsing the ambient path back to the pinned string
/// regardless of context.
pub(super) fn with_invoker_search_path<T>(
    invoker_search_path: &str,
    f: impl FnOnce() -> Result<T, PgTrickleError>,
) -> Result<T, PgTrickleError> {
    use std::panic::AssertUnwindSafe;

    let prior_search_path = active_search_path_or_pinned();

    Spi::run_with_args(
        "SELECT pg_catalog.set_config('search_path', $1, true)",
        &[invoker_search_path.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    unsafe {
        // SAFETY: PgTryBuilder runs the cleanup hook on both success and
        // PostgreSQL ERROR paths while the backend is in a valid state.
        pgrx::PgTryBuilder::new(AssertUnwindSafe(f))
            .finally(move || {
                if let Ok(prior_cstring) = std::ffi::CString::new(prior_search_path.clone()) {
                    pg_sys::set_config_option(
                        c"search_path".as_ptr(),
                        prior_cstring.as_ptr(),
                        pg_sys::GucContext::PGC_USERSET,
                        pg_sys::GucSource::PGC_S_SESSION,
                        pg_sys::GucAction::GUC_ACTION_LOCAL,
                        true,
                        pgrx::PgLogLevel::ERROR as i32,
                        false,
                    );
                }
            })
            .execute()
    }
}

/// Read the currently active `search_path` GUC, falling back to the pinned
/// definer path if it cannot be read for any reason (never leaves the
/// ambient path unrestorable).
fn active_search_path_or_pinned() -> String {
    Spi::get_one::<String>("SELECT current_setting('search_path')")
        .ok()
        .flatten()
        .unwrap_or_else(|| "pgtrickle, pg_catalog, pg_temp".to_string())
}

// ── Helper functions ───────────────────────────────────────────────────────

/// EC-25/EC-26: Install a guard trigger that blocks direct DML on a stream
/// table's storage table.
///
/// Creates a PL/pgSQL trigger function and a BEFORE trigger for
/// INSERT/UPDATE/DELETE that raises an exception if the caller is not
/// the pg_trickle refresh executor.  The trigger checks the
/// `pg_trickle.internal_refresh` GUC which is set to `true` only during
/// refresh execution.
///
/// Also installs an event trigger guard for TRUNCATE via a separate trigger.
pub(super) fn install_dml_guard_trigger(
    schema: &str,
    table_name: &str,
) -> Result<(), PgTrickleError> {
    let qualified = format!(
        "{}.{}",
        quote_identifier(schema),
        quote_identifier(table_name),
    );
    let trigger_func_name = format!(
        "{}._pgt_guard_{}",
        quote_identifier(schema),
        table_name.replace('"', ""),
    );

    // Create the guard trigger function
    //
    // IMPORTANT: For DELETE operations, NEW is NULL in PostgreSQL trigger
    // functions. A BEFORE trigger returning NULL silently cancels the
    // operation. We must return OLD for DELETE and NEW for INSERT/UPDATE
    // to allow the managed refresh executor to proceed.
    let create_func_sql = format!(
        "CREATE OR REPLACE FUNCTION {}() RETURNS trigger \
         LANGUAGE plpgsql AS $$ \
         BEGIN \
           IF current_setting('pg_trickle.internal_refresh', true) IS DISTINCT FROM 'true' THEN \
             RAISE EXCEPTION 'Direct DML on stream table % is not allowed. \
             Stream tables are maintained automatically by pg_trickle.', TG_TABLE_NAME; \
           END IF; \
           IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF; \
         END; $$",
        trigger_func_name,
    );
    Spi::run(&create_func_sql).map_err(|e| {
        PgTrickleError::SpiError(format!("Failed to create DML guard function: {}", e))
    })?;

    // Create the BEFORE INSERT/UPDATE/DELETE trigger
    let create_trigger_sql = format!(
        "CREATE TRIGGER pgt_dml_guard \
         BEFORE INSERT OR UPDATE OR DELETE ON {} \
         FOR EACH ROW EXECUTE FUNCTION {}()",
        qualified, trigger_func_name,
    );
    Spi::run(&create_trigger_sql).map_err(|e| {
        PgTrickleError::SpiError(format!("Failed to create DML guard trigger: {}", e))
    })?;

    // EC-25: Also guard against TRUNCATE via a statement-level trigger
    let create_truncate_trigger_sql = format!(
        "CREATE TRIGGER pgt_truncate_guard \
         BEFORE TRUNCATE ON {} \
         FOR EACH STATEMENT EXECUTE FUNCTION {}()",
        qualified, trigger_func_name,
    );
    Spi::run(&create_truncate_trigger_sql).map_err(|e| {
        PgTrickleError::SpiError(format!("Failed to create TRUNCATE guard trigger: {}", e))
    })?;

    Ok(())
}

/// Set up CDC tracking for a base table source.
///
/// Creates a change buffer table and a CDC trigger on the source table
/// that captures INSERT/UPDATE/DELETE changes directly into the buffer.
///
/// PK columns are resolved from `pg_constraint` and used to pre-compute
/// `pk_hash` in the trigger, avoiding expensive JSONB PK extraction during
/// scan delta window-function partitioning.
pub(super) fn setup_cdc_for_source(
    source_oid: pg_sys::Oid,
    pgt_id: i64,
    change_schema: &str,
) -> Result<(), PgTrickleError> {
    let requested_cdc_mode = StDependency::effective_requested_mode_for_source(source_oid)?
        .unwrap_or_else(|| "trigger".to_string());

    // Check if already tracked
    let already_tracked = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_change_tracking WHERE source_relid = $1)",
        &[source_oid.into()],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if requested_cdc_mode == "wal" {
        validate_requested_cdc_mode_requirements(&requested_cdc_mode)?;
    }

    if !already_tracked {
        // Resolve PK columns for trigger pk_hash computation
        let pk_columns = cdc::resolve_pk_columns(source_oid)?;

        // EC-19: If CDC mode is "wal" or "auto" and the source table has no
        // primary key, verify REPLICA IDENTITY FULL. Without it, WAL-based
        // CDC cannot produce correct old-row values for UPDATE/DELETE, leading
        // to silent data corruption.
        if pk_columns.is_empty() && requested_cdc_mode == "wal" {
            let identity = cdc::get_replica_identity_mode(source_oid)?;
            if identity != "full" {
                let table_name = Spi::get_one_with_args::<String>(
                    "SELECT format('%I.%I', n.nspname, c.relname) \
                     FROM pg_class c \
                     JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE c.oid = $1",
                    &[source_oid.into()],
                )
                .unwrap_or(None)
                .unwrap_or_else(|| format!("OID {}", source_oid.to_u32()));

                return Err(PgTrickleError::InvalidArgument(format!(
                    "Source table {} has no PRIMARY KEY and REPLICA IDENTITY is '{}'. \
                     WAL-based CDC (cdc_mode = '{}') requires either a PRIMARY KEY \
                     or REPLICA IDENTITY FULL on keyless tables. \
                     Fix: ALTER TABLE {} REPLICA IDENTITY FULL; \
                     or use cdc_mode = 'trigger'/'auto'.",
                    table_name, identity, requested_cdc_mode, table_name
                )));
            }
        }

        // F15: Resolve the minimal set of columns needed for CDC capture.
        let col_defs = cdc::resolve_referenced_column_defs(source_oid)?;

        // CITUS-4: Compute stable_name for this source.
        let src_id = crate::citus::SourceIdentifier::from_oid(source_oid)?;

        // Create the change buffer table (with typed columns + pk_hash always)
        cdc::create_change_buffer_table(source_oid, change_schema, &col_defs, &src_id.stable_name)?;

        // Create the CDC trigger on the source table (typed per-column INSERTs)
        let trigger_name = cdc::create_change_trigger(
            source_oid,
            change_schema,
            &pk_columns,
            &col_defs,
            &src_id.stable_name,
        )?;

        // Insert tracking record
        Spi::run_with_args(
            "INSERT INTO pgtrickle.pgt_change_tracking \
             (source_relid, slot_name, source_stable_name, tracked_by_pgt_ids) \
             VALUES ($1, $2, $3, ARRAY[$4])",
            &[
                source_oid.into(),
                trigger_name.as_str().into(),
                src_id.stable_name.as_str().into(),
                pgt_id.into(),
            ],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    } else {
        // Already tracked — add this pgt_id to the tracking array
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_change_tracking \
             SET tracked_by_pgt_ids = array_append(tracked_by_pgt_ids, $1) \
             WHERE source_relid = $2 AND NOT ($1 = ANY(tracked_by_pgt_ids))",
            &[pgt_id.into(), source_oid.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        // F15: The new ST may reference columns not yet in the change buffer.
        // Rebuild the trigger function + sync change buffer columns so the union
        // of all downstream ST column sets is reflected in the buffer.
        cdc::rebuild_cdc_trigger_function(source_oid, change_schema)?;

        // Invalidate the MERGE template cache for every existing ST that
        // depends on this source.  The rebuild above may have changed the
        // number of CDC columns (e.g. 3→4 when a new ST adds a column),
        // which changes the bit-mask width embedded in each ST's MERGE
        // template.  Without this invalidation, a cached 3-bit template
        // would be executed against 4-bit changed_cols rows and raise
        // "cannot AND bit strings of different sizes".
        let existing_dep_ids: Vec<i64> = Spi::connect(|client| {
            let table = client
                .select(
                    "SELECT DISTINCT pgt_id FROM pgtrickle.pgt_dependencies \
                         WHERE source_relid = $1",
                    None,
                    &[source_oid.into()],
                )
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;
            let mut ids = Vec::new();
            for row in table {
                if let Some(id) = row
                    .get::<i64>(1)
                    .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?
                {
                    ids.push(id);
                }
            }
            Ok::<_, PgTrickleError>(ids)
        })
        .unwrap_or_default();
        for dep_pgt_id in existing_dep_ids {
            crate::refresh::invalidate_merge_cache(dep_pgt_id);
        }
    }

    if requested_cdc_mode == "trigger" {
        wal_decoder::force_source_to_trigger(source_oid, change_schema)?;
    }

    Ok(())
}

/// Clean up CDC tracking for a source that may no longer be needed.
///
/// If no other STs reference this source, drop the CDC trigger and
/// change buffer table.
pub(super) fn cleanup_cdc_for_source(
    source_oid: pg_sys::Oid,
    cdc_mode: CdcMode,
    excluding_pgt_id: Option<i64>,
) -> Result<(), PgTrickleError> {
    // Check if any other STs still reference this source. During ALTER flows,
    // the current ST's dependency row still exists while cleanup runs, so it
    // must be excluded from the reference check.
    let still_referenced = if let Some(pgt_id) = excluding_pgt_id {
        Spi::get_one_with_args::<bool>(
            "SELECT EXISTS( \
                SELECT 1 FROM pgtrickle.pgt_dependencies \
                WHERE source_relid = $1 AND pgt_id <> $2 \
            )",
            &[source_oid.into(), pgt_id.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .unwrap_or(false)
    } else {
        Spi::get_one_with_args::<bool>(
            "SELECT EXISTS( \
                SELECT 1 FROM pgtrickle.pgt_dependencies WHERE source_relid = $1 \
            )",
            &[source_oid.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .unwrap_or(false)
    };

    if !still_referenced {
        let change_schema = config::pg_trickle_change_buffer_schema();

        // If WAL-based CDC was active (or transitioning), clean up
        // the replication slot and publication first.
        if matches!(cdc_mode, CdcMode::Wal | CdcMode::Transitioning) {
            let slot_name = wal_decoder::slot_name_for_source(source_oid);
            wal_decoder::drop_replication_slot(&slot_name)?;
            wal_decoder::drop_publication(source_oid)?;
        }

        // Drop the CDC trigger and trigger function (may not exist if
        // already in WAL mode, but safe to attempt)
        cdc::drop_change_trigger(source_oid, &change_schema)?;

        // Drop the change buffer table
        let buffer_name = cdc::buffer_base_name_for_oid(source_oid);
        let drop_buf_sql = format!(
            "DROP TABLE IF EXISTS {}.{} CASCADE",
            quote_identifier(&change_schema),
            quote_identifier(&buffer_name),
        );
        Spi::run(&drop_buf_sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        Spi::run_with_args(
            "DELETE FROM pgtrickle.pgt_change_buffers \
             WHERE source_kind = 'BASE' AND source_id = $1",
            &[i64::from(source_oid.to_u32()).into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        // EC-05: Drop the snapshot table (only exists for foreign table sources).
        let drop_snap_sql = format!(
            "DROP TABLE IF EXISTS {}.snapshot_{} CASCADE",
            quote_identifier(&change_schema),
            source_oid.to_u32(),
        );
        Spi::run(&drop_snap_sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        // Delete tracking record
        Spi::run_with_args(
            "DELETE FROM pgtrickle.pgt_change_tracking WHERE source_relid = $1",
            &[source_oid.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TypedIdentifier(String);

impl TypedIdentifier {
    pub(crate) fn parse(raw: &str) -> Result<Self, PgTrickleError> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(PgTrickleError::InvalidArgument(
                "identifier component must not be empty".to_string(),
            ));
        }
        if trimmed.contains('\0') {
            return Err(PgTrickleError::InvalidArgument(
                "identifier component must not contain NUL".to_string(),
            ));
        }

        if !trimmed.starts_with('"') {
            if trimmed.contains('"') {
                return Err(PgTrickleError::InvalidArgument(format!(
                    "invalid quoted identifier component: {raw}"
                )));
            }
            return Ok(Self(trimmed.to_string()));
        }

        let bytes = trimmed.as_bytes();
        let mut idx = 1usize;
        let mut value = String::with_capacity(trimmed.len().saturating_sub(2));

        while idx < bytes.len() {
            match bytes[idx] {
                b'"' => {
                    if idx + 1 < bytes.len() && bytes[idx + 1] == b'"' {
                        value.push('"');
                        idx += 2;
                        continue;
                    }

                    if !trimmed[idx + 1..].trim().is_empty() {
                        return Err(PgTrickleError::InvalidArgument(format!(
                            "invalid trailing text after quoted identifier component: {raw}"
                        )));
                    }

                    if value.is_empty() {
                        return Err(PgTrickleError::InvalidArgument(
                            "identifier component must not be empty".to_string(),
                        ));
                    }

                    return Ok(Self(value));
                }
                b'\0' => {
                    return Err(PgTrickleError::InvalidArgument(
                        "identifier component must not contain NUL".to_string(),
                    ));
                }
                _ => {
                    let ch = trimmed[idx..].chars().next().ok_or_else(|| {
                        PgTrickleError::InvalidArgument(format!(
                            "invalid quoted identifier component: {raw}"
                        ))
                    })?;
                    value.push(ch);
                    idx += ch.len_utf8();
                }
            }
        }

        Err(PgTrickleError::InvalidArgument(format!(
            "unterminated quoted identifier component: {raw}"
        )))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn into_inner(self) -> String {
        self.0
    }

    pub(crate) fn quoted(&self) -> String {
        quote_identifier(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QualifiedIdentifier {
    schema: TypedIdentifier,
    name: TypedIdentifier,
}

impl QualifiedIdentifier {
    pub(crate) fn parse_with_default(
        raw: &str,
        default_schema: &str,
    ) -> Result<Self, PgTrickleError> {
        let (schema, name) = split_qualified_identifier(raw)?;
        let schema = match schema {
            Some(component) => TypedIdentifier::parse(component)?,
            None => TypedIdentifier::parse(default_schema)?,
        };
        let name = TypedIdentifier::parse(name)?;
        Ok(Self { schema, name })
    }

    pub(crate) fn schema(&self) -> &str {
        self.schema.as_str()
    }

    pub(crate) fn name(&self) -> &str {
        self.name.as_str()
    }

    pub(crate) fn into_parts(self) -> (String, String) {
        (self.schema.into_inner(), self.name.into_inner())
    }

    pub(crate) fn quoted(&self) -> String {
        format!("{}.{}", self.schema.quoted(), self.name.quoted())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RelationIdentity {
    pub(crate) qualified: QualifiedIdentifier,
    pub(crate) relid: pg_sys::Oid,
    pub(crate) relkind: char,
    pub(crate) relowner: pg_sys::Oid,
}

fn split_qualified_identifier(raw: &str) -> Result<(Option<&str>, &str), PgTrickleError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(PgTrickleError::InvalidArgument(
            "qualified identifier must not be empty".to_string(),
        ));
    }
    if trimmed.contains('\0') {
        return Err(PgTrickleError::InvalidArgument(
            "qualified identifier must not contain NUL".to_string(),
        ));
    }

    let bytes = trimmed.as_bytes();
    let mut idx = 0usize;
    let mut in_quotes = false;
    let mut separator_idx = None;

    while idx < bytes.len() {
        match bytes[idx] {
            b'"' => {
                if in_quotes && idx + 1 < bytes.len() && bytes[idx + 1] == b'"' {
                    idx += 2;
                    continue;
                }
                in_quotes = !in_quotes;
            }
            b'.' if !in_quotes => {
                if separator_idx.is_some() {
                    return Err(PgTrickleError::InvalidArgument(format!(
                        "qualified identifier must contain at most one top-level dot: {raw}"
                    )));
                }
                separator_idx = Some(idx);
            }
            _ => {}
        }
        idx += 1;
    }

    if in_quotes {
        return Err(PgTrickleError::InvalidArgument(format!(
            "unterminated quoted identifier: {raw}"
        )));
    }

    Ok(match separator_idx {
        Some(idx) => (Some(&trimmed[..idx]), &trimmed[idx + 1..]),
        None => (None, trimmed),
    })
}

pub(crate) fn resolve_relation_identity(
    qualified: QualifiedIdentifier,
) -> Result<Option<RelationIdentity>, PgTrickleError> {
    Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT c.oid, c.relkind::text, c.relowner \
                 FROM pg_catalog.pg_class c \
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname = $2",
                None,
                &[qualified.schema().into(), qualified.name().into()],
            )
            .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;

        if rows.is_empty() {
            return Ok(None);
        }

        let row = rows.first();
        let relid = row
            .get::<pg_sys::Oid>(1)
            .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?
            .ok_or_else(|| PgTrickleError::InternalError("missing relation oid".to_string()))?;
        let relkind = row
            .get::<String>(2)
            .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?
            .and_then(|value| value.chars().next())
            .ok_or_else(|| PgTrickleError::InternalError("missing relation kind".to_string()))?;
        let relowner = row
            .get::<pg_sys::Oid>(3)
            .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?
            .ok_or_else(|| PgTrickleError::InternalError("missing relation owner".to_string()))?;

        Ok(Some(RelationIdentity {
            qualified,
            relid,
            relkind,
            relowner,
        }))
    })
}

fn current_schema_name() -> Result<String, PgTrickleError> {
    Ok(Spi::get_one::<String>("SELECT current_schema()::text")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .unwrap_or_else(|| "public".to_string()))
}

/// Check whether `caller_oid` owns `relid` or is a superuser.
///
/// The caller role is supplied explicitly so SECURITY DEFINER code can validate
/// the outer invoker via [`outer_user_id`] while invoker-only code can preserve
/// `current_user` semantics by passing its effective role OID.
pub(crate) fn role_owns_relation_or_is_superuser(
    caller_oid: pgrx::pg_sys::Oid,
    relid: pgrx::pg_sys::Oid,
) -> Result<bool, PgTrickleError> {
    Spi::get_one_with_args::<bool>(
        "SELECT EXISTS ( \
             SELECT 1 \
             FROM pg_catalog.pg_class c \
             WHERE c.oid = $2 \
               AND ( \
                   pg_has_role($1, c.relowner, 'USAGE') \
                   OR EXISTS ( \
                       SELECT 1 \
                       FROM pg_catalog.pg_roles caller \
                       WHERE caller.oid = $1 \
                         AND caller.rolsuper \
                   ) \
               ) \
         )",
        &[caller_oid.into(), relid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))
    .map(|value| value.unwrap_or(false))
}

/// SEC-1: Check that the outer invoker owns the stream table's storage table.
///
/// Uses `pg_catalog.pg_class.relowner` to verify ownership against the outer
/// caller role rather than `current_user`, so SECURITY DEFINER APIs enforce the
/// documented invoker ownership model. Superusers bypass the check.
pub(super) fn check_stream_table_ownership(
    pgt_relid: pgrx::pg_sys::Oid,
    schema: &str,
    table_name: &str,
) -> Result<(), PgTrickleError> {
    check_stream_table_ownership_for(outer_user_id(), pgt_relid, schema, table_name)
}

fn check_stream_table_ownership_for(
    caller_oid: pgrx::pg_sys::Oid,
    pgt_relid: pgrx::pg_sys::Oid,
    schema: &str,
    table_name: &str,
) -> Result<(), PgTrickleError> {
    let is_owner_or_superuser = role_owns_relation_or_is_superuser(caller_oid, pgt_relid)?;

    if !is_owner_or_superuser {
        return Err(PgTrickleError::PermissionDenied(format!(
            "must be owner of stream table {}.{}",
            schema, table_name,
        )));
    }

    Ok(())
}

/// Resolve an owner-lifecycle target under the original caller's path and
/// authorize it before any private catalog mutation occurs.
pub(super) fn resolve_owned_stream_table(
    name: &str,
    entry_context: security_context::EntryContext,
) -> Result<(String, String, StreamTableMeta), PgTrickleError> {
    let caller = security_context::capture_caller_context(entry_context)?;
    resolve_owned_stream_table_with_caller(name, &caller)
}

pub(super) fn resolve_owned_stream_table_with_caller(
    name: &str,
    caller: &security_context::CallerContext,
) -> Result<(String, String, StreamTableMeta), PgTrickleError> {
    let (schema, table_name) = resolve_qualified_name_as_caller(name, &caller.search_path)?;
    let st = StreamTableMeta::get_by_name(&schema, &table_name)?;
    check_stream_table_ownership_for(caller.role_oid, st.pgt_relid, &schema, &table_name)?;
    Ok((schema, table_name, st))
}

/// Parse a possibly schema-qualified name into `(schema, table)`.
pub(crate) fn parse_qualified_name_pub(name: &str) -> Result<(String, String), PgTrickleError> {
    parse_qualified_name(name)
}

// TEST-4: Public fuzz-harness wrappers — thin re-exports so the cargo-fuzz
// target (a separate crate) can reach these pure-Rust helpers without a
// PostgreSQL backend.

/// Public wrapper for [`parse_schedule`] used by the fuzz harness.
pub fn parse_schedule_pub(s: &str) -> Result<Schedule, PgTrickleError> {
    parse_schedule(s)
}

/// Public wrapper for [`validate_cron`] used by the fuzz harness.
pub fn validate_cron_pub(s: &str) -> Result<(), PgTrickleError> {
    validate_cron(s)
}

/// Public wrapper for [`detect_select_star`] used by the fuzz harness.
pub fn detect_select_star_pub(s: &str) -> bool {
    detect_select_star(s)
}

/// Public wrapper for [`detect_volatile_functions`] used by the fuzz harness.
pub fn detect_volatile_functions_pub(s: &str) -> Option<&'static str> {
    detect_volatile_functions(s)
}

/// Parse a possibly schema-qualified name into `(schema, table)`.
pub(super) fn parse_qualified_name(name: &str) -> Result<(String, String), PgTrickleError> {
    QualifiedIdentifier::parse_with_default(name, "public").map(QualifiedIdentifier::into_parts)
}

/// LSEC-7: Resolve a possibly-unqualified, caller-controlled name into
/// `(schema, table)` under the *original caller's* captured `search_path` —
/// never a hard-coded `"public"` default and never `current_schema()`
/// evaluated under a pinned `SECURITY DEFINER` path.
///
/// A name that is already schema-qualified (`"schema.table"`) is parsed
/// as-is. An unqualified name's default schema is resolved by temporarily
/// activating the caller's exact `search_path`
/// ([`with_invoker_search_path`]) and asking PostgreSQL for
/// `current_schema()` — the same resolution PostgreSQL itself would use for
/// the caller, not a guess.
pub(super) fn resolve_qualified_name_as_caller(
    name: &str,
    caller_search_path: &str,
) -> Result<(String, String), PgTrickleError> {
    let (schema_part, _) = split_qualified_identifier(name)?;
    if schema_part.is_some() {
        return parse_qualified_name(name);
    }
    let default_schema = with_invoker_search_path(caller_search_path, || {
        Spi::get_one::<String>("SELECT current_schema()::text")
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            .ok_or_else(|| {
                PgTrickleError::InvalidArgument(
                    "no schema in the caller's search_path is usable as a default; \
                     qualify the name explicitly (schema.table)"
                        .to_string(),
                )
            })
    })?;
    QualifiedIdentifier::parse_with_default(name, &default_schema)
        .map(QualifiedIdentifier::into_parts)
}

/// Look up a relation's current owner OID. Used to capture the
/// pre-recreation storage owner so it can be restored on the rebuilt table
/// (LSEC-8: ALTER must preserve the exact original storage owner).
pub(super) fn relation_owner(relid: pg_sys::Oid) -> Result<pg_sys::Oid, PgTrickleError> {
    Spi::get_one_with_args::<pg_sys::Oid>(
        "SELECT relowner FROM pg_catalog.pg_class WHERE oid = $1",
        &[relid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| {
        PgTrickleError::NotFound(format!("relation with OID {} not found", relid.to_u32()))
    })
}

/// Transfer a recreated storage table back to a specific pre-recreation
/// owner OID (as opposed to [`transfer_output_table_ownership`], which
/// always transfers to the current outer caller — the right choice on
/// first CREATE, but wrong for ALTER's storage-rebuild paths, which must
/// restore the table's *original* owner even when a superuser is the one
/// running the ALTER).
pub(super) fn set_relation_owner(
    schema: &str,
    table_name: &str,
    owner_oid: pg_sys::Oid,
) -> Result<(), PgTrickleError> {
    let owner_name = Spi::get_one_with_args::<String>(
        "SELECT rolname::text FROM pg_catalog.pg_roles WHERE oid = $1",
        &[owner_oid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| {
        PgTrickleError::NotFound(format!("role with OID {} not found", owner_oid.to_u32()))
    })?;
    let sql = format!(
        "ALTER TABLE {}.{} OWNER TO {}",
        quote_identifier(schema),
        quote_identifier(table_name),
        quote_identifier(&owner_name),
    );
    Spi::run(&sql).map_err(|e| {
        PgTrickleError::SpiError(format!(
            "Failed to preserve original stream table owner: {e}"
        ))
    })
}

pub(crate) fn parse_qualified_identifier_with_current_schema(
    name: &str,
) -> Result<QualifiedIdentifier, PgTrickleError> {
    let current_schema = current_schema_name()?;
    QualifiedIdentifier::parse_with_default(name, &current_schema)
}

/// A1-1: Validate the `partition_by` column name against the stream table's
/// SELECT output columns.
///
/// Checks:
/// 1. The supplied column name(s) are non-empty.
/// 2. Each column appears in the stream table's SELECT output (from `columns`).
///
/// A1-1b: Supports comma-separated multi-column partition keys
/// (e.g. `"event_day,customer_id"`).
///
/// A valid partition key ensures the refresh path can inject a range predicate
/// (A1-3) and that the partitioned storage table can be created correctly.
pub(super) fn validate_partition_key(
    partition_key: &str,
    columns: &[ColumnDef],
) -> Result<(), PgTrickleError> {
    let parts = parse_partition_key_columns(partition_key);
    if parts.is_empty() {
        return Err(PgTrickleError::InvalidArgument(
            "partition_by must contain at least one non-empty column name".to_string(),
        ));
    }
    // A1-1d/A1-3b: PostgreSQL LIST and HASH partitioning support exactly one column.
    let method = parse_partition_method(partition_key);
    if (method == PartitionMethod::List || method == PartitionMethod::Hash) && parts.len() > 1 {
        return Err(PgTrickleError::InvalidArgument(format!(
            "{} partitioning supports only a single column",
            match method {
                PartitionMethod::List => "LIST",
                PartitionMethod::Hash => "HASH",
                _ => unreachable!(),
            }
        )));
    }
    // A1-3b: Validate HASH modulus if specified.
    if method == PartitionMethod::Hash
        && let Some(m) = parse_hash_modulus(partition_key)
        && !(2..=256).contains(&m)
    {
        return Err(PgTrickleError::InvalidArgument(
            "HASH partition modulus must be between 2 and 256".to_string(),
        ));
    }
    let available: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
    for part in &parts {
        let found = columns.iter().any(|c| c.name.eq_ignore_ascii_case(part));
        if !found {
            return Err(PgTrickleError::InvalidArgument(format!(
                "partition_by column '{}' is not in the stream table's SELECT output. \
                 Available columns: {}",
                part,
                available.join(", "),
            )));
        }
    }
    Ok(())
}

/// Parse a comma-separated partition key specification into individual column
/// names. Trims whitespace from each component and filters out empty entries.
///
/// # Examples
/// ```text
/// "event_day"              → ["event_day"]
/// "event_day, customer_id" → ["event_day", "customer_id"]
/// " a , b , c "            → ["a", "b", "c"]
/// ```
pub(crate) fn parse_partition_key_columns(partition_key: &str) -> Vec<String> {
    let raw = strip_partition_mode_prefix(partition_key);
    raw.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// A1-1d/A1-3b: Partition method: RANGE (default), LIST, or HASH.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PartitionMethod {
    Range,
    List,
    Hash,
}

/// Parse the partition method from the `partition_by` specification.
///
/// Format: `"[LIST:|HASH:]col[,col2]"`.  The `LIST:` prefix selects LIST
/// partitioning, `HASH:` selects HASH; bare column names default to RANGE.
/// For HASH, an optional `:N` suffix sets the modulus (e.g. `HASH:id:8`).
///
/// # Examples
/// ```text
/// "sale_date"              → Range
/// "sale_date,region"       → Range  (multi-column RANGE)
/// "LIST:region"            → List
/// "HASH:customer_id"       → Hash  (default 4 partitions)
/// "HASH:customer_id:8"     → Hash  (8 partitions)
/// ```
pub(crate) fn parse_partition_method(partition_key: &str) -> PartitionMethod {
    let trimmed = partition_key.trim();
    let upper = trimmed.to_uppercase();
    if upper.starts_with("LIST:") {
        PartitionMethod::List
    } else if upper.starts_with("HASH:") {
        PartitionMethod::Hash
    } else {
        PartitionMethod::Range
    }
}

/// A1-3b: Parse the HASH modulus from a partition key specification.
///
/// `"HASH:id:8"` → `8`, `"HASH:id"` → `4` (default).
/// Returns `None` for non-HASH partition methods.
pub(crate) fn parse_hash_modulus(partition_key: &str) -> Option<u32> {
    if parse_partition_method(partition_key) != PartitionMethod::Hash {
        return None;
    }
    let trimmed = partition_key.trim();
    // Strip "HASH:" prefix (5 chars)
    let rest = &trimmed[5..];
    // Look for second ":" — "col:N"
    if let Some(pos) = rest.rfind(':') {
        let modulus_str = &rest[pos + 1..];
        if let Ok(m) = modulus_str.parse::<u32>() {
            return Some(m);
        }
    }
    Some(4) // default modulus
}

/// Strip the partition method prefix from a partition key specification,
/// returning only the column name(s).  Case-insensitive.
///
/// `"LIST:region"` → `"region"`, `"HASH:id:8"` → `"id"`,
/// `"sale_date"` → `"sale_date"`
pub(crate) fn strip_partition_mode_prefix(partition_key: &str) -> &str {
    let trimmed = partition_key.trim();
    if trimmed.len() >= 5 && trimmed[..5].eq_ignore_ascii_case("LIST:") {
        &trimmed[5..]
    } else if trimmed.len() >= 5 && trimmed[..5].eq_ignore_ascii_case("HASH:") {
        let rest = &trimmed[5..];
        // Strip optional ":N" modulus suffix
        if let Some(pos) = rest.rfind(':') {
            let suffix = &rest[pos + 1..];
            if suffix.parse::<u32>().is_ok() {
                return &rest[..pos];
            }
        }
        rest
    } else {
        trimmed
    }
}

/// Column metadata from a defining query.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub type_oid: PgOid,
}

#[cfg(not(test))]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct AnalyzedFunctionCall {
    oid: pg_sys::Oid,
    supplied_arg_numbers: Vec<i32>,
}

#[cfg(not(test))]
struct VolatilityCollector {
    function_oids: Vec<pg_sys::Oid>,
    function_calls: Vec<AnalyzedFunctionCall>,
    enum_io_function_oids: Vec<pg_sys::Oid>,
    worst: char,
}

#[cfg(not(test))]
unsafe extern "C-unwind" {
    #[link_name = "jspIsMutable"]
    fn jsonpath_is_mutable(
        path: *mut std::ffi::c_void,
        varnames: *mut pg_sys::List,
        varexprs: *mut pg_sys::List,
    ) -> bool;
}

#[cfg(not(test))]
fn max_analyzed_volatility(a: char, b: char) -> char {
    match (a, b) {
        ('v', _) | (_, 'v') => 'v',
        ('s', _) | (_, 's') => 's',
        _ => 'i',
    }
}

/// Resolve volatility from the analyzed expression nodes.
///
/// Aggregate implementation functions are deliberately not included: their
/// volatility describes the aggregate's internal state machinery, while DVM
/// admission handles the aggregate itself. Enum output is likewise stable in
/// `pg_proc`, but enum DDL invalidates dependent stream tables, so enum I/O is
/// safe here.
#[cfg(not(test))]
fn analyzed_query_volatility(query_node: *mut pg_sys::Query) -> Result<char, PgTrickleError> {
    let mut collector = VolatilityCollector {
        function_oids: Vec::new(),
        function_calls: Vec::new(),
        enum_io_function_oids: Vec::new(),
        worst: 'i',
    };

    // SAFETY: query_node is a valid analyzed Query allocated by PostgreSQL's
    // parser and remains valid for this function's duration.
    unsafe {
        pg_sys::query_tree_walker_impl(
            query_node,
            Some(collect_volatility_nodes),
            &mut collector as *mut VolatilityCollector as *mut std::ffi::c_void,
            pg_sys::QTW_EXAMINE_RTES_BEFORE as i32,
        );
    }

    collect_default_argument_volatility(&mut collector)?;

    collector
        .function_oids
        .sort_unstable_by_key(|oid| oid.to_u32());
    collector.function_oids.dedup();
    collector
        .enum_io_function_oids
        .sort_unstable_by_key(|oid| oid.to_u32());
    collector.enum_io_function_oids.dedup();

    for oid in collector.function_oids {
        if collector
            .enum_io_function_oids
            .binary_search_by_key(&oid.to_u32(), |candidate| candidate.to_u32())
            .is_ok()
        {
            continue;
        }
        let volatility = Spi::get_one_with_args::<String>(
            "SELECT provolatile::text FROM pg_catalog.pg_proc WHERE oid = $1",
            &[oid.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(format!("volatility lookup failed: {e}")))?
        .and_then(|value| value.chars().next())
        .unwrap_or('v');
        collector.worst = max_analyzed_volatility(collector.worst, volatility);
    }

    Ok(collector.worst)
}

/// Add volatility from function defaults omitted at the call site.
///
/// PostgreSQL leaves default expressions out of the analyzed `FuncExpr` and
/// inserts them during planning. Analyze only the defaults absent from this
/// call, using `NamedArgExpr.argnumber` for named and mixed notation.
#[cfg(not(test))]
fn collect_default_argument_volatility(
    collector: &mut VolatilityCollector,
) -> Result<(), PgTrickleError> {
    let mut checked_calls = std::collections::HashSet::new();
    let mut index = 0;

    while let Some(call) = collector.function_calls.get(index).cloned() {
        index += 1;
        if !checked_calls.insert(call.clone()) {
            continue;
        }

        let (pronargs, pronargdefaults, defaults_sql) =
            Spi::get_three_with_args::<i32, i32, String>(
                "SELECT pronargs::int, pronargdefaults::int, \
                    CASE WHEN pronargdefaults > 0 \
                         THEN pg_get_expr(proargdefaults, 0::oid) END \
             FROM pg_catalog.pg_proc WHERE oid = $1",
                &[call.oid.into()],
            )
            .map_err(|e| {
                PgTrickleError::SpiError(format!(
                    "default-argument volatility lookup failed for function OID {}: {e}",
                    call.oid
                ))
            })?;

        let (Some(pronargs), Some(pronargdefaults), Some(defaults_sql)) =
            (pronargs, pronargdefaults, defaults_sql)
        else {
            continue;
        };

        let first_default = pronargs - pronargdefaults;
        let omitted_defaults = (first_default..pronargs)
            .filter(|argnumber| call.supplied_arg_numbers.binary_search(argnumber).is_err())
            .map(|argnumber| (argnumber - first_default) as usize)
            .collect::<Vec<_>>();
        if omitted_defaults.is_empty() {
            continue;
        }

        let defaults_query = format!("SELECT {defaults_sql}");
        let defaults_node = analyze_query_tree(&defaults_query)?;

        // SAFETY: defaults_node is a valid analyzed Query allocated by
        // PostgreSQL and remains valid for this function's duration. Its
        // target list contains the defaults in declared positional order.
        unsafe {
            let targets = pgrx::PgList::<pg_sys::TargetEntry>::from_pg((*defaults_node).targetList);
            for default_index in omitted_defaults {
                let Some(target) = targets.get_ptr(default_index) else {
                    collector.worst = 'v';
                    continue;
                };
                if target.is_null() || (*target).expr.is_null() {
                    collector.worst = 'v';
                    continue;
                }
                collect_volatility_nodes(
                    (*target).expr as *mut pg_sys::Node,
                    collector as *mut VolatilityCollector as *mut std::ffi::c_void,
                );
            }
        }
    }

    Ok(())
}

#[cfg(not(test))]
unsafe extern "C-unwind" fn collect_function_oid(
    oid: pg_sys::Oid,
    context: *mut std::ffi::c_void,
) -> bool {
    // SAFETY: PostgreSQL invokes this callback synchronously with the
    // VolatilityCollector context passed to check_functions_in_node.
    unsafe {
        (*(context as *mut VolatilityCollector))
            .function_oids
            .push(oid);
    }
    false
}

#[cfg(not(test))]
fn record_function_call(
    oid: pg_sys::Oid,
    args: &pgrx::PgList<pg_sys::Node>,
    collector: &mut VolatilityCollector,
) {
    let mut supplied_arg_numbers = args
        .iter_ptr()
        .enumerate()
        .map(|(index, arg)| {
            // SAFETY: args comes from a valid analyzed FuncExpr or WindowFunc
            // and its nodes remain valid for the collector's duration.
            unsafe {
                if pgrx::is_a(arg, pg_sys::NodeTag::T_NamedArgExpr) {
                    (*(arg as *const pg_sys::NamedArgExpr)).argnumber
                } else {
                    index as i32
                }
            }
        })
        .collect::<Vec<_>>();
    supplied_arg_numbers.sort_unstable();
    collector.function_calls.push(AnalyzedFunctionCall {
        oid,
        supplied_arg_numbers,
    });
}

#[cfg(not(test))]
unsafe extern "C-unwind" fn collect_volatility_nodes(
    node: *mut pg_sys::Node,
    context: *mut std::ffi::c_void,
) -> bool {
    if node.is_null() {
        return false;
    }

    // SAFETY: PostgreSQL's walker supplies valid nodes, `context` is the
    // VolatilityCollector passed below, and every typed access is guarded by
    // an is_a check.
    unsafe {
        // QTW_EXAMINE_RTES_BEFORE passes RangeTblEntry nodes to the callback
        // before the query walker handles their subqueries. They are not
        // expression nodes and must not reach expression_tree_walker.
        if pgrx::is_a(node, pg_sys::NodeTag::T_RangeTblEntry) {
            return false;
        }

        let collector = &mut *(context as *mut VolatilityCollector);
        if pgrx::is_a(node, pg_sys::NodeTag::T_FuncExpr) {
            let function = &*(node as *const pg_sys::FuncExpr);
            let args = pgrx::PgList::<pg_sys::Node>::from_pg(function.args);
            record_function_call(function.funcid, &args, collector);
        } else if pgrx::is_a(node, pg_sys::NodeTag::T_WindowFunc) {
            let window = &*(node as *const pg_sys::WindowFunc);
            if !window.winagg {
                let args = pgrx::PgList::<pg_sys::Node>::from_pg(window.args);
                record_function_call(window.winfnoid, &args, collector);
            }
        }

        if pgrx::is_a(node, pg_sys::NodeTag::T_CoerceViaIO) {
            let cast = &*(node as *const pg_sys::CoerceViaIO);
            if !cast.arg.is_null() {
                let source_type = pg_sys::exprType(cast.arg as *const pg_sys::Node);
                if pg_sys::get_typtype(source_type) as u8 == pg_sys::TYPTYPE_ENUM {
                    let mut output = pg_sys::InvalidOid;
                    let mut is_varlena = false;
                    pg_sys::getTypeOutputInfo(source_type, &mut output, &mut is_varlena);
                    collector.enum_io_function_oids.push(output);
                }
            }
            if pg_sys::get_typtype(cast.resulttype) as u8 == pg_sys::TYPTYPE_ENUM {
                let mut input = pg_sys::InvalidOid;
                let mut io_param = pg_sys::InvalidOid;
                pg_sys::getTypeInputInfo(cast.resulttype, &mut input, &mut io_param);
                collector.enum_io_function_oids.push(input);
            }
        } else if pgrx::is_a(node, pg_sys::NodeTag::T_JsonConstructorExpr) {
            let constructor = &*(node as *const pg_sys::JsonConstructorExpr);
            if constructor.returning.is_null() || (*constructor.returning).format.is_null() {
                collector.worst = 'v';
            } else {
                let is_jsonb = (*(*constructor.returning).format).format_type
                    == pg_sys::JsonFormatType::JS_FORMAT_JSONB;
                let args = pgrx::PgList::<pg_sys::Node>::from_pg(constructor.args);
                for arg in args.iter_ptr() {
                    let arg_type = pg_sys::exprType(arg as *const pg_sys::Node);
                    let immutable = if is_jsonb {
                        pg_sys::to_jsonb_is_immutable(arg_type)
                    } else {
                        pg_sys::to_json_is_immutable(arg_type)
                    };
                    if !immutable {
                        collector.worst = max_analyzed_volatility(collector.worst, 's');
                    }
                }
            }
        } else if pgrx::is_a(node, pg_sys::NodeTag::T_JsonExpr) {
            let json = &*(node as *const pg_sys::JsonExpr);
            if !pgrx::is_a(json.path_spec, pg_sys::NodeTag::T_Const) {
                collector.worst = max_analyzed_volatility(collector.worst, 's');
            } else {
                let path = &*(json.path_spec as *const pg_sys::Const);
                if !path.constisnull {
                    if path.consttype != pg_sys::JSONPATHOID {
                        collector.worst = 'v';
                    } else {
                        let detoasted = pg_sys::pg_detoast_datum(
                            path.constvalue.cast_mut_ptr::<pg_sys::varlena>(),
                        );
                        if jsonpath_is_mutable(
                            detoasted as *mut std::ffi::c_void,
                            json.passing_names,
                            json.passing_values,
                        ) {
                            collector.worst = max_analyzed_volatility(collector.worst, 's');
                        }
                    }
                }
            }
        } else if pgrx::is_a(node, pg_sys::NodeTag::T_SQLValueFunction) {
            collector.worst = max_analyzed_volatility(collector.worst, 's');
        } else if pgrx::is_a(node, pg_sys::NodeTag::T_NextValueExpr) {
            collector.worst = 'v';
        }

        // Aggregate implementation volatility is intentionally excluded.
        // PostgreSQL handles every other function-containing node, including
        // both sides of CoerceViaIO and every RowCompareExpr operator.
        if !pgrx::is_a(node, pg_sys::NodeTag::T_Aggref) {
            pg_sys::check_functions_in_node(node, Some(collect_function_oid), context);
        }

        // expression_tree_walker does not recurse into nested Query nodes.
        if pgrx::is_a(node, pg_sys::NodeTag::T_Query) {
            pg_sys::query_tree_walker_impl(
                node as *mut pg_sys::Query,
                Some(collect_volatility_nodes),
                context,
                pg_sys::QTW_EXAMINE_RTES_BEFORE as i32,
            )
        } else {
            pg_sys::expression_tree_walker_impl(node, Some(collect_volatility_nodes), context)
        }
    }
}

#[cfg(test)]
fn analyzed_query_volatility(_query_node: *mut pg_sys::Query) -> Result<char, PgTrickleError> {
    Ok('i')
}

fn analyze_query_tree(query: &str) -> Result<*mut pg_sys::Query, PgTrickleError> {
    use pgrx::PgList;
    use std::ffi::CString;

    let c_sql = CString::new(query)
        .map_err(|e| PgTrickleError::QueryParseError(format!("Query contains null byte: {e}")))?;

    // SAFETY: PostgreSQL receives a valid SQL string inside a backend. The
    // returned analyzed Query remains valid in the current memory context.
    unsafe {
        let raw_list = pg_sys::raw_parser(c_sql.as_ptr(), pg_sys::RawParseMode::RAW_PARSE_DEFAULT);
        let stmts = PgList::<pg_sys::RawStmt>::from_pg(raw_list);

        if stmts.len() != 1 {
            return Err(PgTrickleError::QueryParseError(format!(
                "Expected 1 statement, got {}",
                stmts.len()
            )));
        }

        let raw_stmt = stmts.get_ptr(0).ok_or_else(|| {
            PgTrickleError::QueryParseError("Query produced no parse tree nodes".into())
        })?;
        let query_node = pg_sys::parse_analyze_fixedparams(
            raw_stmt,
            c_sql.as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null_mut(),
        );

        if query_node.is_null() {
            Err(PgTrickleError::QueryParseError(
                "Query analysis returned null".into(),
            ))
        } else {
            Ok(query_node)
        }
    }
}

/// Validate a defining query and extract its output columns and volatility via
/// parse analysis.
///
/// This avoids executing the query body during validation, which is important
/// for stream-table cycles where a plain `SELECT ... LIMIT 0` can still reach
/// change-buffer-dependent paths for upstream stream tables.
pub(crate) fn validate_defining_query(
    query: &str,
) -> Result<(Vec<ColumnDef>, char), PgTrickleError> {
    use pgrx::PgList;
    use std::ffi::CStr;

    let query_node = analyze_query_tree(query)?;

    // SAFETY: analyze_query_tree returned a valid Query allocated in the
    // current PostgreSQL memory context.
    unsafe {
        let target_list = PgList::<pg_sys::TargetEntry>::from_pg((*query_node).targetList);
        let mut columns = Vec::new();

        for (index, tle_ptr) in target_list.iter_ptr().enumerate() {
            if tle_ptr.is_null() {
                continue;
            }

            let tle = &*tle_ptr;
            if tle.resjunk {
                continue;
            }

            let name = if !tle.resname.is_null() {
                CStr::from_ptr(tle.resname).to_string_lossy().into_owned()
            } else {
                format!("column_{}", index + 1)
            };

            let type_oid = if tle.expr.is_null() {
                PgOid::Invalid
            } else {
                PgOid::from(pg_sys::exprType(tle.expr as *const pg_sys::Node))
            };

            columns.push(ColumnDef { name, type_oid });
        }

        if columns.is_empty() {
            return Err(PgTrickleError::QueryParseError(
                "Defining query returns no columns".into(),
            ));
        }

        let volatility = analyzed_query_volatility(query_node);

        Ok((columns, volatility?))
    }
}

/// Parsed schedule specification — either a duration-based schedule
/// or a cron expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Schedule {
    /// Duration-based: refresh when data is older than this many seconds.
    Duration(i64),
    /// Cron-based: refresh at the times specified by the cron expression.
    Cron(String),
}

/// Parse a Prometheus/GNU-style duration string into seconds.
///
/// Supported units: `s` (seconds), `m` (minutes), `h` (hours), `d` (days),
/// `w` (weeks). Compound durations like `1h30m` and `2m30s` are supported.
/// A bare integer (e.g., `"60"`) is treated as seconds.
///
/// Examples: `"30s"`, `"5m"`, `"1h"`, `"1h30m"`, `"1d"`, `"2w"`, `"60"`.
pub(crate) fn parse_duration(s: &str) -> Result<i64, PgTrickleError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(PgTrickleError::InvalidArgument(
            "schedule cannot be empty".into(),
        ));
    }

    // Bare integer → seconds
    if let Ok(secs) = s.parse::<i64>() {
        return if secs >= 0 {
            Ok(secs)
        } else {
            Err(PgTrickleError::InvalidArgument(format!(
                "schedule cannot be negative: '{s}'"
            )))
        };
    }

    let mut total_secs: i64 = 0;
    let mut num_buf = String::new();
    let mut found_unit = false;

    for ch in s.chars() {
        if ch.is_ascii_digit() {
            num_buf.push(ch);
        } else {
            let multiplier = match ch {
                's' => 1i64,
                'm' => 60,
                'h' => 3600,
                'd' => 86400,
                'w' => 604800,
                _ => {
                    return Err(PgTrickleError::InvalidArgument(format!(
                        "invalid duration unit '{ch}' in '{s}'. \
                         Use s (seconds), m (minutes), h (hours), d (days), w (weeks). \
                         Example: '5m', '1h30m', '2d'"
                    )));
                }
            };

            if num_buf.is_empty() {
                return Err(PgTrickleError::InvalidArgument(format!(
                    "expected a number before '{ch}' in duration '{s}'"
                )));
            }

            let n: i64 = num_buf.parse().map_err(|_| {
                PgTrickleError::InvalidArgument(format!(
                    "invalid number '{num_buf}' in duration '{s}'"
                ))
            })?;

            let component = n.checked_mul(multiplier).ok_or_else(|| {
                PgTrickleError::InvalidArgument(format!(
                    "duration value '{num_buf}{ch}' overflows i64 seconds in '{s}'"
                ))
            })?;
            total_secs = total_secs.checked_add(component).ok_or_else(|| {
                PgTrickleError::InvalidArgument(format!(
                    "total duration '{s}' overflows i64 seconds"
                ))
            })?;
            num_buf.clear();
            found_unit = true;
        }
    }

    // Trailing digits without a unit → error (require explicit unit)
    if !num_buf.is_empty() {
        if found_unit {
            return Err(PgTrickleError::InvalidArgument(format!(
                "trailing digits '{num_buf}' without a unit in duration '{s}'. \
                 Append s, m, h, d, or w. Example: '1h30m'"
            )));
        }
        // Pure digits already handled above; shouldn't reach here
        return Err(PgTrickleError::InvalidArgument(format!(
            "invalid duration '{s}'"
        )));
    }

    if total_secs < 0 {
        return Err(PgTrickleError::InvalidArgument(format!(
            "schedule cannot be negative: '{s}'"
        )));
    }

    Ok(total_secs)
}

/// Validate that schedule meets the minimum.
pub(super) fn validate_schedule(seconds: i64) -> Result<(), PgTrickleError> {
    // In test context the PostgreSQL GUC infrastructure is not available;
    // use the default minimum (1 s) directly to avoid FFI panics.
    #[cfg(not(test))]
    let min = config::pg_trickle_min_schedule_seconds() as i64;
    #[cfg(test)]
    let min = 1i64;

    if seconds < min {
        return Err(PgTrickleError::InvalidArgument(format!(
            "schedule must be at least {}s, got {}s",
            min, seconds
        )));
    }
    Ok(())
}

/// Parse a schedule string as either a duration or a cron expression.
///
/// **Duration strings** use Prometheus/GNU-style units: `30s`, `5m`, `1h`,
/// `1h30m`, `1d`, `2w`. A bare integer is treated as seconds.
///
/// **Cron expressions** follow standard 5-field (minute-granularity) or
/// 6-field (second-granularity) cron syntax, plus `@hourly`, `@daily`, etc.
/// aliases. Cron patterns are detected by the presence of spaces or a `@`
/// prefix.
///
/// Returns a `Schedule` variant.
pub(crate) fn parse_schedule(s: &str) -> Result<Schedule, PgTrickleError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(PgTrickleError::InvalidArgument(
            "schedule cannot be empty".into(),
        ));
    }

    // Heuristic: if the string starts with '@' or contains spaces, treat
    // it as a cron expression. Duration strings never contain spaces.
    if s.starts_with('@') || s.contains(' ') {
        validate_cron(s)?;
        Ok(Schedule::Cron(s.to_string()))
    } else {
        let secs = parse_duration(s)?;
        validate_schedule(secs)?;
        Ok(Schedule::Duration(secs))
    }
}

/// Validate a cron expression by parsing it with croner.
pub(super) fn validate_cron(expr: &str) -> Result<(), PgTrickleError> {
    use std::str::FromStr;

    // Standard cron has 5 fields (min hour dom mon dow).
    // We also accept 6-field cron with a leading seconds field.
    // 7 or more fields are not valid cron expressions.
    if !expr.starts_with('@') {
        let field_count = expr.split_whitespace().count();
        if field_count != 5 && field_count != 6 {
            return Err(PgTrickleError::InvalidArgument(format!(
                "invalid cron expression '{expr}': expected 5 or 6 fields, got {field_count}"
            )));
        }
    }

    croner::Cron::from_str(expr).map_err(|e| {
        PgTrickleError::InvalidArgument(format!("invalid cron expression '{expr}': {e}"))
    })?;

    Ok(())
}

/// Check whether a cron schedule is due for refresh.
///
/// Returns `true` if `now >= next_occurrence(last_refresh_at, cron_expr)`.
/// If `last_refresh_at` is `None`, always returns `true` (never refreshed).
pub(crate) fn cron_is_due(cron_expr: &str, last_refresh_epoch: Option<i64>) -> bool {
    use std::str::FromStr;

    let cron = match croner::Cron::from_str(cron_expr) {
        Ok(c) => c,
        Err(_) => return false,
    };

    let now = chrono::Utc::now();

    match last_refresh_epoch {
        None => true, // never refreshed → always due
        Some(epoch) => {
            let last = match chrono::DateTime::from_timestamp(epoch, 0) {
                Some(st) => st,
                None => return true,
            };
            // Find the next occurrence after the last refresh
            match cron.find_next_occurrence(&last, false) {
                Ok(next) => now >= next,
                Err(_) => false,
            }
        }
    }
}

/// Extract source relation OIDs from a defining query using PostgreSQL's parser/analyzer.
///
/// Uses `pg_sys::raw_parser()` + `pg_sys::parse_analyze_fixedparams()` to get
/// fully resolved table OIDs from the query's range table entries.
pub(crate) fn extract_source_relations(
    query: &str,
) -> Result<Vec<(pg_sys::Oid, String)>, PgTrickleError> {
    use pgrx::PgList;
    use std::ffi::CString;

    let c_sql = CString::new(query)
        .map_err(|e| PgTrickleError::QueryParseError(format!("Query contains null byte: {}", e)))?;

    // SAFETY: We're calling PostgreSQL C parser functions with valid inputs.
    // raw_parser and parse_analyze_fixedparams are safe when called within
    // a PostgreSQL backend with a valid memory context.
    unsafe {
        // Step 1: Parse the raw SQL into a parse tree
        let raw_list = pg_sys::raw_parser(c_sql.as_ptr(), pg_sys::RawParseMode::RAW_PARSE_DEFAULT);

        let stmts = PgList::<pg_sys::RawStmt>::from_pg(raw_list);
        let raw_stmt = stmts.get_ptr(0).ok_or_else(|| {
            PgTrickleError::QueryParseError("Query produced no parse tree nodes".into())
        })?;

        // Step 2: Analyze — resolves all table names to OIDs
        let query_node = pg_sys::parse_analyze_fixedparams(
            raw_stmt,
            c_sql.as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null_mut(),
        );

        if query_node.is_null() {
            return Err(PgTrickleError::QueryParseError(
                "Query analysis returned null".into(),
            ));
        }

        // Step 3: Extract relation OIDs from the analyzed query tree.
        //
        // The top-level rtable may NOT contain base tables referenced
        // inside CTEs — those live in the CTE's own sub-Query rtable.
        // Similarly, subqueries in FROM (RTE_SUBQUERY) have their own
        // rtables. We walk the full tree recursively.
        let mut relations = Vec::new();
        let mut seen_oids = std::collections::HashSet::new();

        collect_relation_oids(query_node, &mut relations, &mut seen_oids);

        if relations.is_empty() {
            return Err(PgTrickleError::QueryParseError(
                "Defining query references no tables".into(),
            ));
        }

        Ok(relations)
    }
}

/// Context for [`relation_oid_walker`] — collects `(Oid, source_type)` pairs.
pub(super) struct RelationCollectorCtx {
    relations: *mut Vec<(pg_sys::Oid, String)>,
    seen_oids: *mut std::collections::HashSet<pg_sys::Oid>,
}

/// Recursively collect `RTE_RELATION` OIDs from an analyzed `Query` node.
///
/// Uses PostgreSQL's `query_tree_walker_impl` with the
/// `QTW_EXAMINE_RTES_BEFORE` flag so that the callback visits every
/// `RangeTblEntry` in every (sub-)query. This covers:
///
/// 1. Base tables in FROM clauses (`RTE_RELATION`)
/// 2. Subqueries in FROM (`RTE_SUBQUERY` → walker recurses automatically)
/// 3. CTEs (`Query.cteList` → walker recurses automatically)
/// 4. EXISTS / IN / ANY subqueries in WHERE / HAVING / SELECT
///    (`SubLink.subselect` → `expression_tree_walker` recurses,
///    callback handles the resulting `T_Query` node)
///
/// # Safety
/// Caller must ensure `query_node` points to a valid analyzed `Query`.
unsafe fn collect_relation_oids(
    query_node: *mut pg_sys::Query,
    relations: &mut Vec<(pg_sys::Oid, String)>,
    seen_oids: &mut std::collections::HashSet<pg_sys::Oid>,
) {
    if query_node.is_null() {
        return;
    }

    let mut ctx = RelationCollectorCtx {
        relations: relations as *mut _,
        seen_oids: seen_oids as *mut _,
    };

    // SAFETY: query_node is a valid analyzed Query; the walker callback
    // only reads RTE fields and calls classify_source_relation (SPI).
    // QTW_EXAMINE_RTES_BEFORE = 16: the walker calls our callback for
    // each RangeTblEntry *before* recursing into subqueries / CTEs.
    unsafe {
        pg_sys::query_tree_walker_impl(
            query_node,
            Some(relation_oid_walker),
            &mut ctx as *mut RelationCollectorCtx as *mut std::ffi::c_void,
            pg_sys::QTW_EXAMINE_RTES_BEFORE as i32,
        );
    }
}

/// Walker callback for [`collect_relation_oids`].
///
/// Called by `query_tree_walker_impl` / `expression_tree_walker_impl` for
/// every node in the analyzed query tree.
///
/// - `T_RangeTblEntry` with `RTE_RELATION` → extract OID
/// - `T_Query` (from SubLink subselects) → recurse via `query_tree_walker`
/// - Everything else → recurse via `expression_tree_walker`
///
/// # Safety
/// `node` and `context` must be valid pointers provided by the PG walker.
unsafe extern "C-unwind" fn relation_oid_walker(
    node: *mut pg_sys::Node,
    context: *mut std::ffi::c_void,
) -> bool {
    if node.is_null() {
        return false;
    }

    // RTE_RELATION → record the OID
    if unsafe { pgrx::is_a(node, pg_sys::NodeTag::T_RangeTblEntry) } {
        // SAFETY: node tag verified as T_RangeTblEntry.
        let rte = unsafe { &*(node as *const pg_sys::RangeTblEntry) };
        if rte.rtekind == pg_sys::RTEKind::RTE_RELATION {
            // SAFETY: context is our RelationCollectorCtx.
            let ctx = unsafe { &mut *(context as *mut RelationCollectorCtx) };
            let seen = unsafe { &mut *ctx.seen_oids };
            if seen.insert(rte.relid) {
                let source_type = classify_source_relation(rte.relid);
                let rels = unsafe { &mut *ctx.relations };
                rels.push((rte.relid, source_type));
            }
        }
        return false; // continue walking
    }

    // T_Query → use query_tree_walker to handle rtable + expressions
    // (expression_tree_walker does NOT recurse into Query nodes)
    if unsafe { pgrx::is_a(node, pg_sys::NodeTag::T_Query) } {
        // SAFETY: node tag verified as T_Query.
        return unsafe {
            pg_sys::query_tree_walker_impl(
                node as *mut pg_sys::Query,
                Some(relation_oid_walker),
                context,
                pg_sys::QTW_EXAMINE_RTES_BEFORE as i32,
            )
        };
    }

    // All other node types → recurse into children
    // SAFETY: expression_tree_walker handles all standard node types.
    unsafe { pg_sys::expression_tree_walker_impl(node, Some(relation_oid_walker), context) }
}

/// Emit warnings/info for source table edge cases (F13, F14).
///
/// - **Partitioned tables** (F13): Log an info message confirming that CDC
///   triggers on the parent fire for partition-routed DML (PG 13+).
/// - **Logical replication targets** (F14): Emit a WARNING because changes
///   arriving via logical replication do **not** fire normal triggers, which
///   means CDC will miss those changes.
pub(super) fn warn_source_table_properties(source_relids: &[(pg_sys::Oid, String)]) {
    for (oid, source_type) in source_relids {
        if source_type != "TABLE" {
            continue;
        }

        // Resolve relkind and qualified name.
        let relkind = Spi::get_one_with_args::<String>(
            "SELECT relkind::text FROM pg_class WHERE oid = $1",
            &[(*oid).into()],
        )
        .unwrap_or(None);

        let relkind = match relkind {
            Some(rk) => rk,
            None => continue,
        };

        let table_name = Spi::get_one_with_args::<String>(
            "SELECT format('%I.%I', n.nspname, c.relname) \
             FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.oid = $1",
            &[(*oid).into()],
        )
        .unwrap_or(None)
        .unwrap_or_else(|| format!("OID {}", oid.to_u32()));

        // F13: Partitioned table info
        if relkind == "p" {
            pgrx::info!(
                "pg_trickle: source table {} is a partitioned table. \
                 CDC triggers on the parent fire for all DML routed to \
                 child partitions (PostgreSQL 13+). If you ATTACH PARTITION \
                 with pre-existing data, pg_trickle will automatically \
                 reinitialize affected stream tables.",
                table_name,
            );
        }

        // PT4: Foreign table info
        if relkind == "f" {
            pgrx::info!(
                "pg_trickle: source table {} is a foreign table. Foreign tables \
                 cannot use trigger-based or WAL-based CDC — only FULL refresh \
                 mode or polling-based change detection is supported.",
                table_name,
            );
        }

        // F14: Logical replication target warning
        let is_sub_target = Spi::get_one_with_args::<bool>(
            "SELECT EXISTS(\
                SELECT 1 FROM pg_subscription_rel WHERE srrelid = $1\
             )",
            &[(*oid).into()],
        )
        .unwrap_or(Some(false))
        .unwrap_or(false);

        if is_sub_target {
            pgrx::warning!(
                "pg_trickle: source table {} is a logical replication target. \
                 Changes arriving via replication will NOT fire CDC triggers — \
                 the stream table may become stale. Consider using \
                 cdc_mode = 'wal' or a FULL refresh schedule.",
                table_name,
            );
        }
    }
}

/// EC-15: Warn when the defining query contains `SELECT *` at the top level.
///
/// `SELECT *` makes the stream table fragile: if a column is added to or
/// removed from a source table, the stream table's storage schema will be
/// out of sync with the defining query, causing errors or silent data loss
/// on the next refresh.
///
/// This is a best-effort heuristic check using the raw query text. It looks
/// for `SELECT ... * ...` patterns that are not inside a subquery or aggregate
/// (e.g., `count(*)` is allowed).
pub(super) fn warn_select_star(query: &str) {
    if detect_select_star(query) {
        pgrx::warning!(
            "pg_trickle: defining query uses SELECT *. If source table columns \
             are added or removed, the stream table will require reinitialization. \
             Consider listing columns explicitly for resilience against schema \
             changes."
        );
    }
}

/// Pure detection logic for `SELECT *` patterns in a defining query.
///
/// Returns `true` if the query contains a bare `*` (or `table.*`) in the
/// top-level SELECT list. Ignores `*` inside function calls like `count(*)`.
///
/// This is intentionally conservative — false positives are OK (it's a
/// warning), but false negatives for `SELECT *` should be rare.
pub(super) fn detect_select_star(query: &str) -> bool {
    // Quick exit: no asterisk at all
    if !query.contains('*') {
        return false;
    }

    let upper = query.to_uppercase();

    // Find the first top-level SELECT ... FROM
    if let Some(select_pos) = upper.find("SELECT") {
        let after_select = &upper[select_pos + 6..];
        // Find FROM (at the same nesting level)
        let mut depth = 0i32;
        let mut from_offset = None;
        for (i, ch) in after_select.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => depth -= 1,
                _ => {}
            }
            if depth == 0 && after_select[i..].starts_with("FROM") {
                // Check if it's a word boundary (not part of a larger word)
                let before_ok = i == 0 || !after_select.as_bytes()[i - 1].is_ascii_alphanumeric();
                let after_ok = i + 4 >= after_select.len()
                    || !after_select.as_bytes()[i + 4].is_ascii_alphanumeric();
                if before_ok && after_ok {
                    from_offset = Some(i);
                    break;
                }
            }
        }

        if let Some(end) = from_offset {
            let select_list = &after_select[..end];
            // Check for bare `*` or `table.*` at top-level (depth 0)
            let mut depth = 0i32;
            let chars: Vec<char> = select_list.chars().collect();
            for &ch in chars.iter() {
                match ch {
                    '(' => depth += 1,
                    ')' => depth -= 1,
                    '*' if depth == 0 => {
                        return true;
                    }
                    _ => {}
                }
            }
        }
    }
    false
}

// ── OP-6: Volatile function detection ──────────────────────────────────────
//
// Warn (or reject in DIFFERENTIAL mode) when a defining query contains
// non-deterministic functions like `now()`, `random()`, `gen_random_uuid()`
// etc. without the `non_deterministic => true` flag. These functions produce
// different results each time the query runs, making differential maintenance
// incorrect: the IVM delta assumes stable row IDs and content, but volatile
// functions violate that assumption.
//
// This is a pre-v1.0 safety gate. False positives are acceptable (warn) but
// false negatives (missing detection) allow silent wrong-result bugs.

/// Well-known volatile built-in functions that should never appear in a
/// DIFFERENTIAL stream table's defining query without explicit acknowledgement.
const VOLATILE_FN_PATTERNS: &[&str] = &[
    "now()",
    "current_timestamp",
    "current_date",
    "current_time",
    "localtime",
    "localtimestamp",
    "clock_timestamp()",
    "transaction_timestamp()",
    "statement_timestamp()",
    "timeofday()",
    "random()",
    "setseed(",
    "gen_random_uuid()",
    "gen_random_bytes(",
    "uuid_generate_v1()",
    "uuid_generate_v4()",
    "txid_current()",
    "pg_current_xact_id()",
];

/// Detect whether `query` contains any of the known volatile functions.
///
/// Returns the first matching pattern if found. The check is case-insensitive
/// and operates on the raw query text. It is intentionally conservative —
/// false positives are OK (produces a warning), but false negatives silently
/// allow non-determinism into the stream table.
pub(super) fn detect_volatile_functions(query: &str) -> Option<&'static str> {
    let lower = query.to_lowercase();
    VOLATILE_FN_PATTERNS
        .iter()
        .find(|&&pattern| lower.contains(pattern))
        .copied()
}

/// Emit a WARNING if the defining query uses a volatile function.
///
/// Called at `create_stream_table` time. Does not fail creation — the warning
/// is advisory. Users who intentionally use non-deterministic functions
/// (e.g. a FULL-refresh-only stream table) may safely ignore it.
pub(super) fn warn_volatile_functions(query: &str) {
    if let Some(pattern) = detect_volatile_functions(query) {
        pgrx::warning!(
            "pg_trickle: defining query contains a volatile/non-deterministic function \
             ('{}') that may produce different results on each refresh. \
             DIFFERENTIAL stream tables rely on stable row identities — using volatile \
             functions can cause phantom rows, missed deletes, or stale data. \
             If this is intentional, use FULL refresh mode. \
             For append-only sources, consider using an explicit timestamp column \
             instead of now() or current_timestamp.",
            pattern
        );
    }
}

/// Classify a source relation as TABLE, STREAM_TABLE, or VIEW.
pub(super) fn classify_source_relation(oid: pg_sys::Oid) -> String {
    // Check if this OID is a stream table
    let is_st = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_relid = $1)",
        &[oid.into()],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if is_st {
        return "STREAM_TABLE".to_string();
    }

    // Check relkind: 'r' = table, 'v' = view, 'm' = matview
    let relkind = Spi::get_one_with_args::<String>(
        "SELECT relkind::text FROM pg_class WHERE oid = $1",
        &[oid.into()],
    )
    .unwrap_or(None)
    .unwrap_or_else(|| "r".to_string());

    match relkind.as_str() {
        "v" => "VIEW".to_string(),
        "m" => "MATVIEW".to_string(),
        "f" => "FOREIGN_TABLE".to_string(),
        _ => "TABLE".to_string(),
    }
}

pub(super) fn normalize_source_relations(
    source_relids: Vec<(pg_sys::Oid, String)>,
) -> Vec<(pg_sys::Oid, String)> {
    source_relids
        .into_iter()
        .map(|(oid, _)| (oid, classify_source_relation(oid)))
        .collect()
}

/// DIAG-2: Estimate the GROUP BY cardinality from pg_stats.n_distinct.
///
/// Queries `pg_stats` for the GROUP BY columns on any source table. Returns
/// the minimum `n_distinct` value across all matched columns (conservative
/// estimate of group count). Returns `None` if no statistics are available.
pub(super) fn estimate_group_cardinality(
    source_relids: &[(pg_sys::Oid, String)],
    group_cols: &[String],
) -> Option<i64> {
    if group_cols.is_empty() || source_relids.is_empty() {
        return None;
    }

    let mut min_distinct: Option<i64> = None;

    for (oid, _) in source_relids {
        // Look up schema.table_name for this OID.
        let schema_table: Option<(String, String)> = Spi::connect(|client| {
            let tbl = client
                .select(
                    "SELECT n.nspname::text, c.relname::text \
                     FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE c.oid = $1",
                    None,
                    &[(*oid).into()],
                )
                .ok()?;
            if tbl.is_empty() {
                return None;
            }
            let schema = tbl.get::<String>(1).ok()??;
            let name = tbl.get::<String>(2).ok()??;
            Some((schema, name))
        });

        if let Some((schema, table_name)) = schema_table {
            for col in group_cols {
                let n_distinct: Option<f32> = Spi::get_one_with_args::<f32>(
                    "SELECT n_distinct FROM pg_stats \
                     WHERE schemaname = $1 AND tablename = $2 AND attname = $3",
                    &[
                        schema.clone().into(),
                        table_name.clone().into(),
                        col.clone().into(),
                    ],
                )
                .unwrap_or(None);

                if let Some(nd) = n_distinct {
                    // In pg_stats, n_distinct > 0 means absolute count,
                    // n_distinct < 0 means fraction of reltuples (e.g. -0.5 = 50%).
                    let effective = if nd > 0.0 {
                        nd as i64
                    } else {
                        // Estimate using reltuples.
                        let reltuples: f32 = Spi::get_one_with_args::<f32>(
                            "SELECT reltuples FROM pg_class WHERE oid = $1",
                            &[(*oid).into()],
                        )
                        .unwrap_or(Some(0.0))
                        .unwrap_or(0.0);
                        ((-nd) * reltuples).max(1.0) as i64
                    };
                    min_distinct = Some(min_distinct.map_or(effective, |m: i64| m.min(effective)));
                }
            }
        }
    }

    min_distinct
}

/// Check for cycles after adding the proposed dependency edges.
///
/// Loads the existing DAG from the catalog, adds the proposed edges,
/// and runs Kahn's algorithm for cycle detection.
pub(super) fn check_for_cycles(
    source_relids: &[(pg_sys::Oid, String)],
) -> Result<(), PgTrickleError> {
    if source_relids.is_empty() {
        return Ok(());
    }

    // Check if any source is itself a stream table — only then can cycles exist
    let has_st_source = source_relids
        .iter()
        .any(|(_, stype)| stype == "STREAM_TABLE");

    if !has_st_source {
        // No stream table sources → no possible cycle
        return Ok(());
    }

    // Build the DAG from catalog and add proposed edges
    let mut dag = StDag::build_from_catalog(config::pg_trickle_default_schedule_seconds())?;

    // Create a temporary node for the proposed ST (use a sentinel pgt_id)
    let proposed_id = NodeId::StreamTable(i64::MAX);
    dag.add_st_node(DagNode {
        id: proposed_id,
        schedule: Some(std::time::Duration::from_secs(60)),
        effective_schedule: std::time::Duration::from_secs(60),
        name: "<proposed>".to_string(),
        status: StStatus::Initializing,
        schedule_raw: None,
    });

    // Add proposed edges
    for (source_oid, source_type) in source_relids {
        let source_node = if source_type == "STREAM_TABLE" {
            // Find the pgt_id for this source OID
            match crate::catalog::StreamTableMeta::get_by_relid(*source_oid) {
                Ok(meta) => NodeId::StreamTable(meta.pgt_id),
                Err(_) => NodeId::BaseTable(source_oid.to_u32()),
            }
        } else {
            NodeId::BaseTable(source_oid.to_u32())
        };
        dag.add_edge(source_node, proposed_id);
    }

    // Run cycle detection
    match dag.detect_cycles() {
        Ok(()) => Ok(()),
        Err(PgTrickleError::CycleDetected(nodes)) => {
            // CYC-6: Conditionally allow monotone cycles
            validate_cycle_allowed(&nodes)
        }
        Err(e) => Err(e),
    }
}

/// CYC-6: Validate that a detected cycle is allowed.
///
/// A cycle is allowed only when:
/// 1. `pg_trickle.allow_circular` GUC is enabled
/// 2. All existing cycle members use DIFFERENTIAL refresh mode
/// 3. All existing cycle members have monotone defining queries
///
/// The proposed (not-yet-created) ST is excluded from checks since its
/// catalog entry doesn't exist yet — it will be validated by the normal
/// `validate_and_parse_query` flow and its refresh mode is checked by
/// the caller after creation.
pub(super) fn validate_cycle_allowed(cycle_nodes: &[String]) -> Result<(), PgTrickleError> {
    validate_cycle_allowed_inner(cycle_nodes, None, None)
}

/// CYC-6: Variant of [`validate_cycle_allowed`] for the ALTER QUERY path.
///
/// Unlike `validate_cycle_allowed`, the ST being altered (`target_pgt_id`)
/// already has a catalog entry, but its query is being replaced. The
/// `proposed_query` is checked for monotonicity instead of the stored
/// catalog entry so that non-monotone cycles are correctly rejected.
pub(super) fn validate_cycle_allowed_alter(
    cycle_nodes: &[String],
    target_pgt_id: i64,
    proposed_query: &str,
) -> Result<(), PgTrickleError> {
    validate_cycle_allowed_inner(cycle_nodes, Some(target_pgt_id), Some(proposed_query))
}

/// Internal shared implementation for cycle-allowed checks.
///
/// `proposed_pgt_id` and `proposed_query` together override the defining
/// query used for monotonicity checks of a specific ST (ALTER path).
pub(super) fn validate_cycle_allowed_inner(
    cycle_nodes: &[String],
    proposed_pgt_id: Option<i64>,
    proposed_query: Option<&str>,
) -> Result<(), PgTrickleError> {
    if !config::pg_trickle_allow_circular() {
        return Err(PgTrickleError::CycleDetected(cycle_nodes.to_vec()));
    }

    // Check existing cycle members (skip the sentinel "<proposed>" node)
    for node_name in cycle_nodes {
        if node_name == "<proposed>" {
            continue;
        }

        // Parse "schema.name" to look up the stream table
        let (schema, name) = match node_name.split_once('.') {
            Some((s, n)) => (s, n),
            None => {
                // Shouldn't happen, but treat as error
                return Err(PgTrickleError::InternalError(format!(
                    "cannot parse cycle member name: {}",
                    node_name
                )));
            }
        };

        let meta = StreamTableMeta::get_by_name(schema, name)?;

        // All cycle members must use DIFFERENTIAL mode
        if meta.refresh_mode != RefreshMode::Differential {
            return Err(PgTrickleError::InvalidArgument(format!(
                "stream table '{}' must use DIFFERENTIAL refresh mode \
                 to participate in a circular dependency (current mode: {})",
                node_name,
                meta.refresh_mode.as_str(),
            )));
        }

        // For the ALTER path: if this node is the one being altered,
        // check the proposed (new) query for monotonicity instead of the
        // stored defining_query — the stored query is the old one and
        // would give a false pass.
        let query_to_check = if proposed_pgt_id == Some(meta.pgt_id) {
            proposed_query.unwrap_or(&meta.defining_query)
        } else {
            &meta.defining_query
        };

        // All cycle members must have monotone queries
        match crate::dvm::parse_defining_query_full(query_to_check) {
            Ok(pr) => crate::dvm::check_monotonicity_with_registry(&pr)?,
            Err(e) => {
                return Err(PgTrickleError::InvalidArgument(format!(
                    "cannot verify monotonicity of '{}': {}",
                    node_name, e,
                )));
            }
        }
    }

    Ok(())
}

/// Cycle detection variant for ALTER QUERY.
///
/// Instead of creating a sentinel node (as `check_for_cycles` does for CREATE),
/// this function re-uses the existing ST's node in the DAG and replaces its
/// incoming edges with the proposed new source dependencies. This correctly
/// detects cycles like A → B → A that a sentinel node would miss.
///
/// `proposed_query` is the new defining query being applied to `pgt_id`.
/// It is passed to the cycle validation so the monotonicity of the altered
/// ST's new query is checked — not the old stored query — when it would
/// participate in a cycle.
pub(super) fn check_for_cycles_alter(
    pgt_id: i64,
    source_relids: &[(pg_sys::Oid, String)],
    proposed_query: &str,
) -> Result<(), PgTrickleError> {
    if source_relids.is_empty() {
        return Ok(());
    }

    let has_st_source = source_relids
        .iter()
        .any(|(_, stype)| stype == "STREAM_TABLE");

    if !has_st_source {
        return Ok(());
    }

    let mut dag = StDag::build_from_catalog(config::pg_trickle_default_schedule_seconds())?;

    let target_node = NodeId::StreamTable(pgt_id);

    // Resolve new source node IDs
    let new_sources: Vec<NodeId> = source_relids
        .iter()
        .map(|(source_oid, source_type)| {
            if source_type == "STREAM_TABLE" {
                match crate::catalog::StreamTableMeta::get_by_relid(*source_oid) {
                    Ok(meta) => NodeId::StreamTable(meta.pgt_id),
                    Err(_) => NodeId::BaseTable(source_oid.to_u32()),
                }
            } else {
                NodeId::BaseTable(source_oid.to_u32())
            }
        })
        .collect();

    // Replace the ST's incoming edges with the proposed new ones
    dag.replace_incoming_edges(target_node, new_sources);

    match dag.detect_cycles() {
        Ok(()) => Ok(()),
        Err(PgTrickleError::CycleDetected(nodes)) => {
            validate_cycle_allowed_alter(&nodes, pgt_id, proposed_query)
        }
        Err(e) => Err(e),
    }
}

/// CYC-6: Recompute SCCs from the current DAG and persist `scc_id` for all
/// stream tables.
///
/// Cyclic SCC members get a positive `scc_id` (1, 2, …); acyclic singletons
/// get `scc_id = NULL`. This is called after CREATE and ALTER to keep SCC
/// assignments consistent.
pub(super) fn assign_scc_ids_from_dag() -> Result<(), PgTrickleError> {
    let dag = StDag::build_from_catalog(config::pg_trickle_default_schedule_seconds())?;
    let sccs = dag.compute_sccs()?;

    let mut next_scc_id: i32 = 1;
    for scc in &sccs {
        if scc.is_cyclic {
            for node_id in &scc.nodes {
                if let NodeId::StreamTable(pgt_id) = node_id {
                    StreamTableMeta::update_scc_id(*pgt_id, Some(next_scc_id))?;
                }
            }
            next_scc_id += 1;
        } else {
            // Acyclic singleton — clear any stale scc_id
            for node_id in &scc.nodes {
                if let NodeId::StreamTable(pgt_id) = node_id {
                    StreamTableMeta::update_scc_id(*pgt_id, None)?;
                }
            }
        }
    }

    Ok(())
}

/// Build CREATE TABLE DDL for the storage table.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_create_table_sql(
    schema: &str,
    name: &str,
    columns: &[ColumnDef],
    needs_pgt_count: bool,
    needs_dual_count: bool,
    avg_aux_columns: &[(String, String, String)],
    sum2_aux_columns: &[(String, String)],
    covar_aux_columns: &[(String, String)],
    nonnull_aux_columns: &[(String, String)],
    statistical_aux_types: &[(String, String)],
    // A1-1: when Some, emit PARTITION BY RANGE (<key>) suffix.
    partition_key: Option<&str>,
    // HOT-1: when Some, emit WITH (fillfactor=N) storage option.
    fillfactor: Option<i32>,
) -> String {
    let col_defs: Vec<String> = columns
        .iter()
        .map(|c| {
            // Use regtype to get the type name from the OID
            let type_name = match c.type_oid {
                PgOid::Invalid => "text".to_string(),
                oid => {
                    // Try to resolve the type name via SPI
                    Spi::get_one_with_args::<String>(
                        "SELECT $1::regtype::text",
                        &[oid.value().into()],
                    )
                    .unwrap_or(Some("text".to_string()))
                    .unwrap_or_else(|| "text".to_string())
                }
            };
            format!("    {} {}", quote_identifier(&c.name), type_name)
        })
        .collect();

    // Add __pgt_count auxiliary column for aggregate/distinct STs.
    let aux_cols = if needs_dual_count {
        // INTERSECT/EXCEPT need dual branch counts
        ",\n    __pgt_count_l BIGINT NOT NULL DEFAULT 0,\n    __pgt_count_r BIGINT NOT NULL DEFAULT 0"
    } else if needs_pgt_count {
        ",\n    __pgt_count BIGINT NOT NULL DEFAULT 0"
    } else {
        ""
    };

    // Add AVG auxiliary columns (__pgt_aux_sum_*, __pgt_aux_count_*) for
    // algebraic AVG maintenance. NUMERIC for sum (matches PostgreSQL AVG
    // precision), BIGINT for count.
    let mut avg_aux_sql = String::new();
    for (sum_col, count_col, _arg_sql) in avg_aux_columns {
        avg_aux_sql.push_str(&format!(
            ",\n    {} NUMERIC NOT NULL DEFAULT 0,\n    {} BIGINT NOT NULL DEFAULT 0",
            quote_identifier(sum_col),
            quote_identifier(count_col),
        ));
    }

    // Add sum-of-squares auxiliary columns (__pgt_aux_sum2_*) for
    // algebraic STDDEV/VAR maintenance.
    let mut sum2_aux_sql = String::new();
    for (sum2_col, _arg_sql) in sum2_aux_columns {
        let accumulator_type = statistical_aux_types
            .iter()
            .find(|(name, _)| name == sum2_col)
            .map(|(_, ty)| ty.as_str())
            .unwrap_or("numeric");
        sum2_aux_sql.push_str(&format!(
            ",\n    {} {} NOT NULL DEFAULT 0",
            quote_identifier(sum2_col),
            accumulator_type,
        ));
    }

    // Add cross-product auxiliary columns (__pgt_aux_sumx_*, sumy, sumxy,
    // sumx2, sumy2) for algebraic CORR/COVAR/REGR_* maintenance (P3-2).
    let mut covar_aux_sql = String::new();
    for (covar_col, _arg_sql) in covar_aux_columns {
        let accumulator_type = statistical_aux_types
            .iter()
            .find(|(name, _)| name == covar_col)
            .map(|(_, ty)| ty.as_str())
            .unwrap_or("numeric");
        covar_aux_sql.push_str(&format!(
            ",\n    {} {} NOT NULL DEFAULT 0",
            quote_identifier(covar_col),
            accumulator_type,
        ));
    }

    // Add nonnull-count auxiliary columns (__pgt_aux_nonnull_*) for
    // SUM NULL-transition correction (P2-2).
    let mut nonnull_aux_sql = String::new();
    for (nonnull_col, _arg_sql) in nonnull_aux_columns {
        nonnull_aux_sql.push_str(&format!(
            ",\n    {} BIGINT NOT NULL DEFAULT 0",
            quote_identifier(nonnull_col),
        ));
    }

    // A1-1/A1-1b/A1-1d: partition clause — appended after the closing ')' of
    // CREATE TABLE.  Supports RANGE (single/multi-column) and LIST keys.
    let partition_clause = partition_key
        .map(|k| {
            let method = parse_partition_method(k);
            let cols = parse_partition_key_columns(k);
            let quoted: Vec<String> = cols
                .iter()
                .map(|c| quote_identifier(c).to_string())
                .collect();
            let method_kw = match method {
                PartitionMethod::Range => "RANGE",
                PartitionMethod::List => "LIST",
                PartitionMethod::Hash => "HASH",
            };
            format!("\nPARTITION BY {} ({})", method_kw, quoted.join(", "))
        })
        .unwrap_or_default();

    // HOT-1: emit WITH (fillfactor=N) when the caller requests it.
    // Partitioned parents accept this syntax and propagate it to child
    // partitions created with PARTITION OF.
    let storage_clause = fillfactor
        .map(|ff| format!(" WITH (fillfactor = {})", ff.clamp(10, 100)))
        .unwrap_or_default();

    format!(
        "CREATE TABLE {}.{} (\n    __pgt_row_id BIGINT,\n{}{}{}{}{}{}\n){}{}",
        quote_identifier(schema),
        quote_identifier(name),
        col_defs.join(",\n"),
        aux_cols,
        avg_aux_sql,
        sum2_aux_sql,
        covar_aux_sql,
        nonnull_aux_sql,
        storage_clause,
        partition_clause,
    )
}

/// Get the OID of a table by schema and name.
pub(super) fn get_table_oid(schema: &str, name: &str) -> Result<pg_sys::Oid, PgTrickleError> {
    let oid = Spi::get_one_with_args::<pg_sys::Oid>(
        "SELECT ($1 || '.' || $2)::regclass::oid",
        &[schema.into(), name.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| {
        PgTrickleError::NotFound(format!(
            "table {}.{} not found after creation",
            schema, name
        ))
    })?;
    Ok(oid)
}

/// Convert legacy INTERSECT/EXCEPT storage to the guarded FULL-refresh shape.
///
/// Older tables may still contain visible dual-count columns and a unique
/// row-id index.  Keep the migration idempotent so every FULL path can call
/// it before materialization.
pub(crate) fn normalize_full_set_operation_storage(
    schema: &str,
    name: &str,
    pgt_relid: pg_sys::Oid,
    pgt_id: i64,
) -> Result<(), PgTrickleError> {
    let quoted_table = format!("{}.{}", quote_identifier(schema), quote_identifier(name),);

    for column in ["__pgt_count_l", "__pgt_count_r"] {
        Spi::run(&format!(
            "ALTER TABLE {quoted_table} DROP COLUMN IF EXISTS {}",
            quote_identifier(column),
        ))
        .map_err(|e| {
            PgTrickleError::SpiError(format!(
                "Failed to remove legacy set-operation column {column}: {e}"
            ))
        })?;
    }

    let indexes = Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT i.indexrelid::regclass::text, i.indisunique \
                 FROM pg_index i \
                 JOIN pg_attribute a \
                   ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
                 WHERE i.indrelid = $1 \
                   AND a.attname = '__pgt_row_id'",
                None,
                &[pgt_relid.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        let mut indexes = Vec::new();
        for row in rows {
            let index_name = row
                .get::<String>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::SpiError(
                        "row-id index catalog lookup returned NULL name".to_string(),
                    )
                })?;
            let is_unique = row
                .get::<bool>(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(false);
            indexes.push((index_name, is_unique));
        }
        Ok::<_, PgTrickleError>(indexes)
    })?;

    let has_nonunique = indexes.iter().any(|(_, is_unique)| !is_unique);
    let rebuild_index = indexes.iter().any(|(_, is_unique)| *is_unique) || !has_nonunique;
    if rebuild_index {
        for (index_name, _) in &indexes {
            Spi::run(&format!("DROP INDEX IF EXISTS {index_name}")) // nosemgrep: rust.spi.run.dynamic-format — index_name comes from pg_catalog::regclass.
                .map_err(|e| {
                    PgTrickleError::SpiError(format!(
                        "Failed to remove legacy row-id index {index_name}: {e}"
                    ))
                })?;
        }
    }

    if rebuild_index {
        let user_columns = crate::cdc::resolve_st_output_columns(pgt_relid)?;
        let include_clause = if crate::config::pg_trickle_auto_index()
            && !user_columns.is_empty()
            && user_columns.len() <= 8
        {
            let columns = user_columns
                .iter()
                .map(|(column, _)| quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ");
            format!(" INCLUDE ({columns})")
        } else {
            String::new()
        };
        // nosemgrep: rust.spi.run.dynamic-format — table and column names are quote_identifier()-escaped catalog identifiers.
        Spi::run(&format!(
            "CREATE INDEX ON {quoted_table} (__pgt_row_id){include_clause}"
        ))
        .map_err(|e| {
            PgTrickleError::SpiError(format!("Failed to create non-unique row-id index: {e}"))
        })?;
    }

    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET has_keyless_source = TRUE, updated_at = now() \
         WHERE pgt_id = $1 AND NOT has_keyless_source",
        &[pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    Ok(())
}

/// Initialize a stream table by populating it from its defining query.
#[allow(clippy::too_many_arguments)]
pub(super) fn initialize_st(
    schema: &str,
    name: &str,
    query: &str,
    pgt_id: i64,
    columns: &[ColumnDef],
    needs_pgt_count: bool,
    needs_dual_count: bool,
    needs_union_dedup: bool,
    topk_info: Option<&crate::dvm::TopKInfo>,
    avg_aux_columns: &[(String, String, String)],
    sum2_aux_columns: &[(String, String)],
    covar_aux_columns: &[(String, String)],
    nonnull_aux_columns: &[(String, String)],
    statistical_aux_types: &[(String, String)],
) -> Result<(), PgTrickleError> {
    // EC-25/EC-26: Set the internal_refresh flag so DML guard triggers
    // allow the initialization INSERT into the storage table.
    Spi::run("SET LOCAL pg_trickle.internal_refresh = 'true'")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    let source_oids: Vec<pg_sys::Oid> = StDependency::get_for_st(pgt_id)?
        .into_iter()
        .filter(|dep| {
            matches!(
                dep.source_type.as_str(),
                "TABLE" | "FOREIGN_TABLE" | "MATVIEW"
            )
        })
        .map(|dep| dep.source_relid)
        .collect();
    cdc::lock_source_relations(&source_oids)?;
    let safe_bound = cdc::get_current_wal_lsn()?;

    // For aggregate/distinct STs, inject COUNT(*) AS __pgt_count into the
    // defining query so the auxiliary column is populated correctly.
    let mut effective_query = if needs_pgt_count {
        inject_pgt_count(query)
    } else {
        query.to_string()
    };

    // For AVG algebraic maintenance, also inject SUM(arg) and COUNT(arg)
    // auxiliary columns into the initialization query.
    if !avg_aux_columns.is_empty() {
        effective_query = inject_avg_aux(&effective_query, avg_aux_columns);
    }

    // For STDDEV/VAR algebraic maintenance, inject SUM(arg*arg) auxiliary
    // columns for sum-of-squares tracking.
    if !sum2_aux_columns.is_empty() {
        let typed = typed_statistical_aux_columns(sum2_aux_columns, statistical_aux_types);
        effective_query = inject_sum2_aux_typed(&effective_query, &typed);
    }

    // P3-2: For CORR/COVAR/REGR_* algebraic maintenance, inject cross-product
    // auxiliary columns (sumx, sumy, sumxy, sumx2, sumy2).
    if !covar_aux_columns.is_empty() {
        let typed = typed_statistical_aux_columns(covar_aux_columns, statistical_aux_types);
        effective_query = inject_covar_aux_typed(&effective_query, &typed);
    }

    // For SUM NULL-transition correction (P2-2), inject COUNT(IS NOT NULL)
    // auxiliary columns for nonnull-count tracking.
    if !nonnull_aux_columns.is_empty() {
        effective_query = inject_nonnull_aux(&effective_query, nonnull_aux_columns);
    }

    // Compute row_id using the same hash formula as the delta query so
    // the MERGE ON clause matches during subsequent differential refreshes.
    // Guarded INTERSECT/EXCEPT queries are materialized directly; their
    // legacy branch-count state is not part of the visible FULL relation.
    // For UNION (without ALL) queries, convert to UNION ALL and count
    // per-unique-row multiplicities for the __pgt_count column.
    // For UNION ALL queries, decompose into per-branch subqueries with
    // child-prefixed row IDs matching diff_union_all's formula.
    let insert_body = if needs_dual_count {
        crate::dvm::direct_full_refresh_insert_body(query, &effective_query)
    } else if needs_union_dedup {
        let col_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        if let Some(union_sql) = crate::dvm::try_union_dedup_refresh_sql(query, &col_names) {
            union_sql
        } else {
            // Fallback: treat as normal query with __pgt_count = 1
            let row_id_expr = crate::dvm::row_id_expr_for_query(query);
            format!(
                "SELECT {row_id_expr} AS __pgt_row_id, sub.*, \
                 1::bigint AS __pgt_count \
                 FROM ({query}) sub",
            )
        }
    } else if let Some(ua_sql) = crate::dvm::try_union_all_refresh_sql(query) {
        ua_sql
    } else if let Some(info) = topk_info {
        // TopK: use the full query (with ORDER BY + LIMIT) for initial population,
        // so only the top K rows are inserted.
        let row_id_expr = crate::dvm::row_id_expr_for_query(query);
        format!(
            "SELECT {row_id_expr} AS __pgt_row_id, sub.* FROM ({topk_query}) sub",
            topk_query = info.full_query,
        )
    } else {
        let row_id_expr = crate::dvm::row_id_expr_for_query(query);
        format!("SELECT {row_id_expr} AS __pgt_row_id, sub.* FROM ({effective_query}) sub",)
    };

    let insert_sql = format!(
        "INSERT INTO {schema}.{table} {insert_body}",
        schema = quote_identifier(schema),
        table = quote_identifier(name),
    );

    let st = StreamTableMeta::get_by_id(pgt_id)?.ok_or_else(|| {
        PgTrickleError::NotFound(format!("stream table metadata for pgt_id={pgt_id}"))
    })?;
    crate::refresh::with_stream_owner(&st, || {
        Spi::run(&insert_sql)
            .map_err(|e| PgTrickleError::SpiError(format!("Failed to initialize ST: {}", e)))
    })?;

    // Seed the initial frontier at creation time so every initialized stream
    // table participates in shared change-buffer bookkeeping immediately.
    // Without this, one branch of a diamond can remain frontier-less after the
    // initial populate and later miss source changes that a sibling consumes.
    //
    // FOREIGN_TABLE sources are included so that the frontier is never empty
    // for FT-only stream tables.  An empty frontier causes
    // `execute_manual_differential_refresh` to treat every manual refresh as a
    // no-op (it assumes empty frontiers belong to ST-on-ST dependencies).
    // Including the FT OID with the current WAL LSN gives differential refresh
    // a valid lower bound from which to compare polled change-buffer rows.
    let slot_positions = cdc::get_slot_positions_at_bound(&source_oids, &safe_bound)?;
    let data_ts = get_data_timestamp_str();
    let frontier = version::compute_initial_frontier(&slot_positions, &data_ts);
    StreamTableMeta::store_frontier_and_complete_refresh(pgt_id, &frontier, 0)?;

    // Record the initial population in pgt_refresh_history so that monitoring
    // tools and tests can observe the initial fill event.  Errors here are
    // non-fatal — the table is already correctly populated.
    if let Ok(now_ts) = Spi::get_one::<TimestampWithTimeZone>("SELECT now()")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))
        .and_then(|v| v.ok_or_else(|| PgTrickleError::InternalError("now() returned NULL".into())))
        && let Ok(refresh_id) = RefreshRecord::insert(
            pgt_id,
            now_ts,
            "FULL",
            "RUNNING",
            0,
            0,
            None,
            Some("INITIAL"),
            None,
            0,
            None,
            false,
            None,
        )
    {
        let _ =
            RefreshRecord::complete(refresh_id, "COMPLETED", 0, 0, None, 0, Some("FULL"), false);
    }

    Ok(())
}

/// Quote a SQL identifier (escape double quotes).
pub(crate) fn quote_identifier(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Inject `COUNT(*) AS __pgt_count` into an aggregate/distinct defining query
/// so that the full refresh populates the auxiliary count column.
///
/// For aggregate queries, this adds `, COUNT(*) AS __pgt_count` before the
/// first top-level `FROM`.
///
/// For DISTINCT queries, transforms `SELECT DISTINCT cols FROM ...` into
/// `SELECT cols, COUNT(*) AS __pgt_count FROM ... GROUP BY cols`.
pub fn inject_pgt_count(query: &str) -> String {
    // Detect SELECT DISTINCT — needs special handling because we must
    // replace DISTINCT with GROUP BY (can't mix DISTINCT with aggregates).
    if let Some(distinct_info) = detect_and_strip_distinct(query) {
        // distinct_info.stripped is the query with DISTINCT removed,
        // e.g., "SELECT color, size FROM prop_dist"
        // distinct_info.columns are the SELECT-list columns before FROM.
        if let Some(from_pos) = find_top_level_keyword(&distinct_info.stripped, "FROM") {
            let select_part = distinct_info.stripped[..from_pos].trim_end();
            let from_part = &distinct_info.stripped[from_pos..];
            let col_list = distinct_info.columns.join(", ");
            return format!(
                "{select_part}, COUNT(*) AS __pgt_count {from_part} GROUP BY {col_list}",
            );
        }
        // Fallback if FROM not found after stripping DISTINCT
        return distinct_info.stripped;
    }

    // Non-DISTINCT (aggregate) queries: just inject COUNT(*) before FROM.
    if let Some(pos) = find_top_level_keyword(query, "FROM") {
        format!(
            "{}, COUNT(*) AS __pgt_count {}",
            query[..pos].trim_end(),
            &query[pos..],
        )
    } else {
        // Fallback: can't inject; return as-is (will leave __pgt_count = DEFAULT 0)
        query.to_string()
    }
}

/// Inject AVG auxiliary columns (`SUM(arg)` and `COUNT(arg)`) into a query.
///
/// These populate the `__pgt_aux_sum_*` and `__pgt_aux_count_*` storage columns
/// during initial population and full refresh. The query must already have
/// `__pgt_count` injected (if needed) before calling this.
pub fn inject_avg_aux(query: &str, avg_aux_columns: &[(String, String, String)]) -> String {
    if avg_aux_columns.is_empty() {
        return query.to_string();
    }

    if let Some(pos) = find_top_level_keyword(query, "FROM") {
        let mut extra = String::new();
        for (sum_col, count_col, arg_sql) in avg_aux_columns {
            // COALESCE ensures the NOT NULL aux-sum column receives 0 rather
            // than NULL when every row in a group has a NULL expression value
            // (e.g. NULLIF returning NULL for all rows).  COUNT already
            // returns 0 for zero non-NULL values, so no COALESCE needed there.
            extra.push_str(&format!(
                ", COALESCE(SUM({arg_sql}), 0) AS {}, COUNT({arg_sql}) AS {}",
                quote_identifier(sum_col),
                quote_identifier(count_col),
            ));
        }
        format!("{}{extra} {}", query[..pos].trim_end(), &query[pos..],)
    } else {
        query.to_string()
    }
}

/// Inject sum-of-squares auxiliary columns (`SUM((arg)*(arg))`) into a query.
///
/// Populates `__pgt_aux_sum2_*` columns for STDDEV/VAR algebraic maintenance
/// during initial population and full refresh. Call after `inject_avg_aux`.
pub fn inject_sum2_aux(query: &str, sum2_aux_columns: &[(String, String)]) -> String {
    let typed = sum2_aux_columns
        .iter()
        .map(|(name, arg)| (name.clone(), arg.clone(), "numeric".to_string()))
        .collect::<Vec<_>>();
    inject_sum2_aux_typed(query, &typed)
}

pub fn typed_statistical_aux_columns(
    columns: &[(String, String)],
    types: &[(String, String)],
) -> Vec<(String, String, String)> {
    columns
        .iter()
        .map(|(name, arg)| {
            let ty = types
                .iter()
                .find(|(type_name, _)| type_name == name)
                .map(|(_, ty)| ty.clone())
                .unwrap_or_else(|| "numeric".to_string());
            (name.clone(), arg.clone(), ty)
        })
        .collect()
}

/// Inject sum-of-squares auxiliaries using PostgreSQL's analyzed accumulator
/// type for the multiplication and stored state.
pub fn inject_sum2_aux_typed(query: &str, sum2_aux_columns: &[(String, String, String)]) -> String {
    if sum2_aux_columns.is_empty() {
        return query.to_string();
    }

    if let Some(pos) = find_top_level_keyword(query, "FROM") {
        let mut extra = String::new();
        for (sum2_col, arg_sql, accumulator_type) in sum2_aux_columns {
            // COALESCE guards against NULL (e.g. when arg_sql is NULL for all rows).
            extra.push_str(&format!(
                ", COALESCE(SUM(({arg_sql})::{accumulator_type} * \
                 ({arg_sql})::{accumulator_type}), 0::{accumulator_type}) AS {}",
                quote_identifier(sum2_col),
            ));
        }
        format!("{}{extra} {}", query[..pos].trim_end(), &query[pos..],)
    } else {
        query.to_string()
    }
}

/// Inject nonnull-count auxiliary columns (`COUNT(CASE WHEN arg IS NOT NULL ...)`)
/// for SUM NULL-transition correction (P2-2).
///
/// These populate the `__pgt_aux_nonnull_*` storage columns during initial
/// population and full refresh so the differential path can perform algebraic
/// NULL-transition correction without rescanning source data.
pub fn inject_nonnull_aux(query: &str, nonnull_aux_columns: &[(String, String)]) -> String {
    if nonnull_aux_columns.is_empty() {
        return query.to_string();
    }

    if let Some(pos) = find_top_level_keyword(query, "FROM") {
        let mut extra = String::new();
        for (nonnull_col, arg_sql) in nonnull_aux_columns {
            extra.push_str(&format!(
                ", COUNT(CASE WHEN ({arg_sql}) IS NOT NULL THEN 1 END) AS {}",
                quote_identifier(nonnull_col),
            ));
        }
        format!("{}{extra} {}", query[..pos].trim_end(), &query[pos..],)
    } else {
        query.to_string()
    }
}

/// Inject cross-product auxiliary columns for CORR/COVAR/REGR algebraic
/// maintenance (P3-2).
///
/// Each covar aux column maps to a specific SQL expression based on its name
/// prefix and `arg_sql` encoding:
///   `__pgt_aux_sumx_*`  → `SUM(x)`
///   `__pgt_aux_sumy_*`  → `SUM(y)`
///   `__pgt_aux_sumxy_*` → `SUM((x)*(y))`  (arg_sql = "x|y")
///   `__pgt_aux_sumx2_*` → `SUM((x)*(x))`
///   `__pgt_aux_sumy2_*` → `SUM((y)*(y))`
pub fn inject_covar_aux(query: &str, covar_aux_columns: &[(String, String)]) -> String {
    let typed = covar_aux_columns
        .iter()
        .map(|(name, arg)| (name.clone(), arg.clone(), "numeric".to_string()))
        .collect::<Vec<_>>();
    inject_covar_aux_typed(query, &typed)
}

/// Inject CORR/COVAR/REGR auxiliaries using the analyzed accumulator type.
pub fn inject_covar_aux_typed(
    query: &str,
    covar_aux_columns: &[(String, String, String)],
) -> String {
    if covar_aux_columns.is_empty() {
        return query.to_string();
    }

    if let Some(pos) = find_top_level_keyword(query, "FROM") {
        let mut extra = String::new();
        for (col_name, arg_sql, accumulator_type) in covar_aux_columns {
            // COALESCE guards each SUM against NULL when all values in a group
            // are NULL (e.g. NULL arguments to CORR/COVAR/REGR_*).
            let expr = if col_name.starts_with("__pgt_aux_sumxy_") {
                // arg_sql is "x_expr|y_expr"
                let parts: Vec<&str> = arg_sql.splitn(2, '|').collect();
                let (x, y) = if parts.len() == 2 {
                    (parts[0], parts[1])
                } else {
                    (arg_sql.as_str(), arg_sql.as_str())
                };
                format!(
                    "COALESCE(SUM(({x})::{accumulator_type} * \
                     ({y})::{accumulator_type}), 0::{accumulator_type})"
                )
            } else if col_name.starts_with("__pgt_aux_sumx2_")
                || col_name.starts_with("__pgt_aux_sumy2_")
            {
                format!(
                    "COALESCE(SUM(({arg_sql})::{accumulator_type} * \
                     ({arg_sql})::{accumulator_type}), 0::{accumulator_type})"
                )
            } else {
                // sumx_ or sumy_ — simple SUM
                format!(
                    "COALESCE(SUM(({arg_sql})::{accumulator_type}), \
                     0::{accumulator_type})"
                )
            };
            extra.push_str(&format!(", {} AS {}", expr, quote_identifier(col_name)));
        }
        format!("{}{extra} {}", query[..pos].trim_end(), &query[pos..])
    } else {
        query.to_string()
    }
}

/// Result of stripping DISTINCT from a query.
pub(super) struct DistinctStripped {
    /// The query with DISTINCT removed.
    pub(super) stripped: String,
    /// The column expressions from the SELECT list (between SELECT and FROM).
    pub(super) columns: Vec<String>,
}

/// Detect if a query starts with `SELECT DISTINCT` (at the top level) and
/// return the query with DISTINCT removed plus the extracted column list.
///
/// Returns `None` if the query does not have a top-level DISTINCT.
pub(super) fn detect_and_strip_distinct(query: &str) -> Option<DistinctStripped> {
    // Find top-level SELECT
    let select_pos = find_top_level_keyword(query, "SELECT")?;
    let after_select = &query[select_pos + 6..]; // len("SELECT") == 6

    // Check if DISTINCT follows (skipping whitespace)
    let trimmed = after_select.trim_start();
    if !trimmed.to_ascii_uppercase().starts_with("DISTINCT") {
        return None;
    }

    // Make sure DISTINCT is followed by a word boundary (not DISTINCT_ON or similar)
    let after_distinct = &trimmed[8..]; // len("DISTINCT") == 8
    if !after_distinct.is_empty() {
        let next_byte = after_distinct.as_bytes()[0];
        if next_byte.is_ascii_alphanumeric() || next_byte == b'_' {
            return None; // e.g., DISTINCTLY or DISTINCT_SOMETHING
        }
    }

    // Build the stripped query: everything before SELECT + "SELECT" + after DISTINCT
    let prefix = &query[..select_pos];
    let stripped = format!("{prefix}SELECT{after_distinct}");

    // Extract column list between SELECT and FROM in the stripped query
    let from_pos = find_top_level_keyword(&stripped, "FROM")?;
    let select_kw_end = find_top_level_keyword(&stripped, "SELECT")? + 6;
    let col_text = stripped[select_kw_end..from_pos].trim();

    // Split the column list on top-level commas
    let columns = split_top_level_commas(col_text);

    Some(DistinctStripped { stripped, columns })
}

/// Split a string on top-level commas (not inside parentheses or string literals).
/// Returns trimmed column expressions.
pub(super) fn split_top_level_commas(s: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut depth: i32 = 0;
    let mut in_string = false;
    let mut start = 0;
    let bytes = s.as_bytes();

    for i in 0..bytes.len() {
        if in_string {
            if bytes[i] == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    // skip escaped quote
                    continue;
                }
                in_string = false;
            }
            continue;
        }
        match bytes[i] {
            b'\'' => in_string = true,
            b'(' => depth += 1,
            b')' => depth -= 1,
            b',' if depth == 0 => {
                let col = s[start..i].trim().to_string();
                if !col.is_empty() {
                    result.push(col);
                }
                start = i + 1;
            }
            _ => {}
        }
    }
    // Last segment
    let col = s[start..].trim().to_string();
    if !col.is_empty() {
        result.push(col);
    }
    result
}

/// Find the byte offset of the first top-level occurrence of a SQL keyword
/// (not inside parentheses or string literals).
pub(super) fn find_top_level_keyword(sql: &str, keyword: &str) -> Option<usize> {
    let kw_len = keyword.len();
    let bytes = sql.as_bytes();
    let kw_upper = keyword.to_ascii_uppercase();
    let kw_bytes = kw_upper.as_bytes();
    let mut depth: i32 = 0;
    let mut in_string = false;
    let mut i = 0;
    while i < bytes.len() {
        if in_string {
            if bytes[i] == b'\'' {
                // Check for escaped quote ''
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                } else {
                    in_string = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }
        match bytes[i] {
            b'\'' => {
                in_string = true;
                i += 1;
            }
            // Skip single-line comments: -- until end of line
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                i += 2;
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                if i < bytes.len() {
                    i += 1; // skip the newline
                }
            }
            // Skip block comments: /* ... */
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                i += 2;
                let mut block_depth = 1i32;
                while i < bytes.len() && block_depth > 0 {
                    if bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                        block_depth += 1;
                        i += 2;
                    } else if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                        block_depth -= 1;
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
            }
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth -= 1;
                i += 1;
            }
            _ if depth == 0 && i + kw_len <= bytes.len() => {
                // Check if this position matches the keyword (case-insensitive)
                let candidate = &bytes[i..i + kw_len];
                if candidate
                    .iter()
                    .zip(kw_bytes.iter())
                    .all(|(a, b)| a.to_ascii_uppercase() == *b)
                {
                    // Verify word boundaries
                    let before_ok =
                        i == 0 || !bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_';
                    let after_ok = i + kw_len >= bytes.len()
                        || !bytes[i + kw_len].is_ascii_alphanumeric() && bytes[i + kw_len] != b'_';
                    if before_ok && after_ok {
                        return Some(i);
                    }
                }
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }
    None
}

/// Restore stream tables from catalog entries after pg_restore.
///
/// D-1c: Convert all existing logged change buffer tables to UNLOGGED.
///
/// Iterates all `pgtrickle_changes.changes_*` tables and converts any that
/// are currently WAL-logged (`relpersistence = 'p'`) to UNLOGGED (`'u'`).
/// Each conversion acquires `ACCESS EXCLUSIVE` lock on the buffer table,
/// so this function should be run during a low-traffic maintenance window.
///
/// Returns the number of buffer tables converted.
///
/// **Warning:** After conversion, buffer contents will be lost on crash
/// recovery. The scheduler will automatically schedule a FULL refresh for
/// affected stream tables after a crash (see D-1b).
#[pg_extern(schema = "pgtrickle")]
pub(super) fn convert_buffers_to_unlogged() -> Result<i64, PgTrickleError> {
    let change_schema = crate::config::pg_trickle_change_buffer_schema();

    // Find all logged buffer tables in the change schema.
    let logged_buffers: Vec<String> = Spi::connect(|client| {
        let table = client
            .select(
                &format!(
                    "SELECT c.relname::text \
                     FROM pg_class c \
                     JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE n.nspname = '{schema}' \
                       AND c.relname LIKE 'changes\\_%' \
                       AND c.relpersistence = 'p' \
                       AND c.relkind IN ('r', 'p')",
                    schema = change_schema,
                ),
                None,
                &[],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        let mut names = Vec::new();
        for row in table {
            if let Some(name) = row.get::<String>(1).ok().flatten() {
                names.push(name);
            }
        }
        Ok::<_, PgTrickleError>(names)
    })?;

    if logged_buffers.is_empty() {
        pgrx::notice!("no logged change buffer tables found — nothing to convert");
        return Ok(0);
    }

    let mut converted = 0i64;
    for table_name in &logged_buffers {
        let sql = format!(
            "ALTER TABLE {schema}.{table} SET UNLOGGED",
            schema = change_schema,
            table = table_name,
        );
        match Spi::run(&sql) {
            Ok(()) => {
                converted += 1;
                pgrx::notice!("converted {}.{} to UNLOGGED", change_schema, table_name,);
            }
            Err(e) => {
                pgrx::warning!(
                    "failed to convert {}.{} to UNLOGGED: {}",
                    change_schema,
                    table_name,
                    e,
                );
            }
        }
    }

    pgrx::notice!(
        "converted {} of {} change buffer tables to UNLOGGED",
        converted,
        logged_buffers.len(),
    );

    Ok(converted)
}

// ── DIAG-1c: recommend_refresh_mode() ─────────────────────────────────────

/// DIAG-1c: Analyze stream table refresh characteristics and recommend the
/// optimal refresh mode (FULL vs DIFFERENTIAL).
///
/// When `st_name` is NULL, returns one row per active stream table.
/// When provided, returns a single row for the named stream table (schema-qualified
/// or search-path resolved).
///
/// Read-only — no side effects.
#[allow(clippy::type_complexity)]
#[pg_extern(schema = "pgtrickle", name = "recommend_refresh_mode")]
pub(super) fn recommend_refresh_mode(
    st_name: default!(Option<String>, "NULL"),
) -> Result<
    TableIterator<
        'static,
        (
            name!(pgt_schema, String),
            name!(pgt_name, String),
            name!(current_mode, String),
            name!(effective_mode, Option<String>),
            name!(recommended_mode, String),
            name!(confidence, String),
            name!(reason, String),
            name!(signals, pgrx::JsonB),
        ),
    >,
    PgTrickleError,
> {
    use crate::diagnostics;

    let stream_tables = match st_name {
        Some(name) => {
            let (schema, table) = parse_qualified_name(&name)?;
            let st = StreamTableMeta::get_by_name(&schema, &table)?;
            vec![st]
        }
        None => StreamTableMeta::get_all()?,
    };

    let mut rows = Vec::new();
    for st in &stream_tables {
        let input = diagnostics::gather_all_signals(st);
        let signals = diagnostics::collect_signals(&input);

        // Effective mode: what actually ran last time
        let effective = Spi::get_one_with_args::<String>(
            "SELECT action FROM pgtrickle.pgt_refresh_history \
             WHERE pgt_id = $1 AND status = 'COMPLETED' \
             ORDER BY start_time DESC LIMIT 1",
            &[st.pgt_id.into()],
        )
        .unwrap_or(None);

        let rec = diagnostics::compute_recommendation(&signals, effective.as_deref());

        // Build signals JSONB
        let signals_json = build_signals_json(&input, &rec);

        rows.push((
            st.pgt_schema.clone(),
            st.pgt_name.clone(),
            st.refresh_mode.as_str().to_string(),
            effective,
            rec.recommended_mode.to_string(),
            rec.confidence.to_string(),
            rec.reason,
            pgrx::JsonB(signals_json),
        ));
    }

    Ok(TableIterator::new(rows))
}

/// Build the JSONB signals payload for recommend_refresh_mode output.
pub(super) fn build_signals_json(
    input: &crate::diagnostics::DiagnosticsInput,
    rec: &crate::diagnostics::Recommendation,
) -> serde_json::Value {
    let signal_array: Vec<serde_json::Value> = rec
        .signals
        .iter()
        .map(|s| {
            serde_json::json!({
                "name": s.name,
                "score": s.score,
                "weight": s.weight,
            })
        })
        .collect();

    serde_json::json!({
        "change_ratio_current": input.change_ratio_current,
        "change_ratio_avg": input.change_ratio_avg,
        "diff_avg_ms": input.diff_avg_ms,
        "full_avg_ms": input.full_avg_ms,
        "diff_p95_ms": input.diff_p95_ms,
        "target_size_bytes": input.target_size_bytes,
        "join_count": input.join_count,
        "has_covering_index": input.has_covering_index,
        "history_rows_diff": input.history_rows_diff,
        "history_rows_full": input.history_rows_full,
        "composite_score": rec.composite_score,
        "signals": signal_array,
    })
}

// ── DIAG-1d: refresh_efficiency ───────────────────────────────────────────

/// DIAG-1d: Per-table refresh efficiency metrics.
///
/// Returns operational metrics for each stream table: FULL vs DIFFERENTIAL
/// timing, change ratios, speedup factor, and refresh counts. Suitable for
/// monitoring dashboards and Grafana alerts.
#[allow(clippy::type_complexity)]
#[pg_extern(schema = "pgtrickle", name = "refresh_efficiency")]
pub(super) fn refresh_efficiency() -> Result<
    TableIterator<
        'static,
        (
            name!(pgt_schema, String),
            name!(pgt_name, String),
            name!(refresh_mode, String),
            name!(total_refreshes, i64),
            name!(diff_count, i64),
            name!(full_count, i64),
            name!(avg_diff_ms, Option<f64>),
            name!(avg_full_ms, Option<f64>),
            name!(avg_change_ratio, Option<f64>),
            name!(diff_speedup, Option<String>),
            name!(last_refresh_at, Option<String>),
        ),
    >,
    PgTrickleError,
> {
    use crate::diagnostics;

    let stream_tables = StreamTableMeta::get_all()?;
    let mut rows = Vec::new();

    for st in &stream_tables {
        let history = diagnostics::gather_history_stats(st.pgt_id, st.pgt_relid);

        let speedup = match (history.diff_avg_ms, history.full_avg_ms) {
            (Some(diff), Some(full)) if diff > 0.0 => Some(format!("{:.1}x", full / diff)),
            _ => None,
        };

        let last_refresh = st.data_timestamp.map(|ts| format!("{}", ts));

        rows.push((
            st.pgt_schema.clone(),
            st.pgt_name.clone(),
            st.refresh_mode.as_str().to_string(),
            history.total_rows,
            history.diff_count,
            history.full_count,
            history.diff_avg_ms,
            history.full_avg_ms,
            history.avg_change_ratio,
            speedup,
            last_refresh,
        ));
    }

    Ok(TableIterator::new(rows))
}

// ── G15-EX: export_definition() ───────────────────────────────────────────

/// G15-EX: Export a stream table's configuration as reproducible DDL.
///
/// Returns a `DROP STREAM TABLE IF EXISTS` + `CREATE STREAM TABLE ... WITH (...)`
/// statement that can recreate the stream table from scratch.
#[pg_extern(schema = "pgtrickle", name = "export_definition")]
pub(super) fn export_definition(st_name: &str) -> Result<String, PgTrickleError> {
    let (schema, table) = parse_qualified_name(st_name)?;
    let st = StreamTableMeta::get_by_name(&schema, &table)?;

    let qualified = format!("{}.{}", quote_ident(&schema), quote_ident(&table));

    let mut ddl = format!("DROP STREAM TABLE IF EXISTS {};\n", qualified);

    ddl.push_str(&format!(
        "SELECT pgtrickle.create_stream_table(\n  '{}'::text,\n  $pgt${}$pgt$::text",
        qualified.replace('\'', "''"),
        st.defining_query,
    ));

    // Optional parameters
    if let Some(ref schedule) = st.schedule {
        ddl.push_str(&format!(
            ",\n  schedule => '{}'",
            schedule.replace('\'', "''")
        ));
    }

    ddl.push_str(&format!(
        ",\n  refresh_mode => '{}'",
        st.refresh_mode.as_str()
    ));

    if let Some(ref cdc) = st.requested_cdc_mode {
        ddl.push_str(&format!(",\n  cdc_mode => '{}'", cdc));
    }

    if st.is_append_only {
        ddl.push_str(",\n  append_only => true");
    }

    if st.pooler_compatibility_mode {
        ddl.push_str(",\n  pooler_compatibility_mode => true");
    }

    if let Some(ref pk) = st.st_partition_key {
        ddl.push_str(&format!(
            ",\n  partition_by => '{}'",
            pk.replace('\'', "''")
        ));
    }

    if let Some(mdj) = st.max_differential_joins {
        ddl.push_str(&format!(",\n  max_differential_joins => {}", mdj));
    }

    if let Some(mdf) = st.max_delta_fraction {
        ddl.push_str(&format!(",\n  max_delta_fraction => {}", mdf));
    }

    let dc = st.diamond_consistency.as_str();
    if dc != "none" {
        ddl.push_str(&format!(",\n  diamond_consistency => '{}'", dc));
    }

    let dsp = st.diamond_schedule_policy.as_str();
    if dsp != "fastest" {
        ddl.push_str(&format!(",\n  diamond_schedule_policy => '{}'", dsp));
    }

    ddl.push_str("\n);\n");

    // Post-creation settings via ALTER
    let mut alters = Vec::new();

    if st.refresh_tier != "hot" {
        alters.push(format!("tier => '{}'", st.refresh_tier));
    }

    if st.fuse_mode != "off" {
        alters.push(format!("fuse => '{}'", st.fuse_mode));
    }

    if let Some(ceiling) = st.fuse_ceiling {
        alters.push(format!("fuse_ceiling => {}", ceiling));
    }

    if let Some(sensitivity) = st.fuse_sensitivity {
        alters.push(format!("fuse_sensitivity => {}", sensitivity));
    }

    if !alters.is_empty() {
        ddl.push_str(&format!(
            "\nSELECT pgtrickle.alter_stream_table('{}', {});\n",
            qualified.replace('\'', "''"),
            alters.join(", "),
        ));
    }

    Ok(ddl)
}

/// Quote an identifier for safe use in SQL.
pub(super) fn quote_ident(name: &str) -> String {
    if name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && !name.is_empty()
        && name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
    {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// During a `pg_restore`, `pg_dump` will restore the base storage tables and
/// the `pgtrickle.pgt_stream_tables` catalog, but the necessary CDC triggers,
/// dependency wiring, frontiers, and ownership state cannot be safely
/// reconstructed here without a protected reconciliation flow.
#[pg_extern(schema = "pgtrickle")]
pub fn restore_stream_tables() -> Result<(), crate::error::PgTrickleError> {
    Err(restore_stream_tables_blocked_error())
}

fn restore_stream_tables_blocked_error() -> PgTrickleError {
    PgTrickleError::InvalidArgument(
        "pgtrickle.restore_stream_tables() is disabled: logical restore requires protected reinitialization/reconciliation of stream-table storage, dependencies, CDC state, frontiers, and ownership before refreshes may resume".to_string(),
    )
}

// ── TEST-1: Unit tests for api/helpers.rs ─────────────────────────────────
//
// 25+ unit tests covering query validation helpers, schema helpers,
// CDC orchestration utilities, and the new OP-6 volatile-function detection.
// No PostgreSQL backend is required — all tested functions are pure Rust.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statistical_auxiliary_products_cast_to_numeric() {
        let sum2 = inject_sum2_aux(
            "SELECT dept FROM src GROUP BY dept",
            &[("__pgt_aux_sum2_sd".into(), "amount".into())],
        );
        assert!(sum2.contains("(amount)::numeric * (amount)::numeric"));

        let covar = inject_covar_aux(
            "SELECT dept FROM src GROUP BY dept",
            &[("__pgt_aux_sumxy_corr".into(), "x|y".into())],
        );
        assert!(covar.contains("(x)::numeric * (y)::numeric"));

        let typed = typed_statistical_aux_columns(
            &[("__pgt_aux_sum2_sd".into(), "amount".into())],
            &[("__pgt_aux_sum2_sd".into(), "double precision".into())],
        );
        let sum2 = inject_sum2_aux_typed("SELECT dept FROM src GROUP BY dept", &typed);
        assert!(sum2.contains("(amount)::double precision * (amount)::double precision"));
    }

    #[test]
    fn test_statistical_auxiliary_ddl_uses_analyzed_type() {
        let ddl = build_create_table_sql(
            "public",
            "stats",
            &[ColumnDef {
                name: "dept".into(),
                type_oid: PgOid::Invalid,
            }],
            false,
            false,
            &[],
            &[("__pgt_aux_sum2_sd".into(), "amount".into())],
            &[],
            &[],
            &[("__pgt_aux_sum2_sd".into(), "double precision".into())],
            None,
            None,
        );
        assert!(ddl.contains("\"__pgt_aux_sum2_sd\" double precision"));
    }

    // ── parse_qualified_name ────────────────────────────────────────────

    #[test]
    fn test_pqn_schema_dot_table() {
        let result = parse_qualified_name("myschema.orders");
        assert_eq!(
            result.unwrap(),
            ("myschema".to_string(), "orders".to_string())
        );
    }

    #[test]
    fn test_pqn_two_parts_with_uppercase() {
        let result = parse_qualified_name("Public.MyTable");
        assert_eq!(
            result.unwrap(),
            ("Public".to_string(), "MyTable".to_string())
        );
    }

    #[test]
    fn test_pqn_default_schema_for_single_part() {
        let result = parse_qualified_name("orders");
        assert_eq!(
            result.unwrap(),
            ("public".to_string(), "orders".to_string())
        );
    }

    #[test]
    fn test_qi_parses_quoted_dot_components() {
        let result = QualifiedIdentifier::parse_with_default(
            r#"  "weird.schema"  .  "table.with.dot"  "#,
            "public",
        )
        .unwrap();
        assert_eq!(result.schema(), "weird.schema");
        assert_eq!(result.name(), "table.with.dot");
    }

    #[test]
    fn test_qi_parses_doubled_quotes_and_unicode() {
        let result =
            QualifiedIdentifier::parse_with_default("\"sch\"\"ema\".\"naïve\"\"table\"", "public")
                .unwrap();
        assert_eq!(result.schema(), r#"sch"ema"#);
        assert_eq!(result.name(), r#"naïve"table"#);
    }

    #[test]
    fn test_qi_preserves_semicolons_and_comment_markers() {
        let result =
            QualifiedIdentifier::parse_with_default("\"semi;--/*\".\"name*/--;\"", "public")
                .unwrap();
        assert_eq!(result.schema(), "semi;--/*");
        assert_eq!(result.name(), "name*/--;");
    }

    #[test]
    fn test_qi_rejects_nul_bytes() {
        let err = QualifiedIdentifier::parse_with_default("good.\0bad", "public").unwrap_err();
        assert!(format!("{err}").contains("NUL"));
    }

    #[test]
    fn test_qi_rejects_empty_components() {
        assert!(QualifiedIdentifier::parse_with_default("schema.", "public").is_err());
        assert!(QualifiedIdentifier::parse_with_default(".table", "public").is_err());
        assert!(QualifiedIdentifier::parse_with_default("\"\".table", "public").is_err());
    }

    #[test]
    fn test_qi_rejects_multiple_top_level_dots() {
        let err = QualifiedIdentifier::parse_with_default("one.two.three", "public").unwrap_err();
        assert!(format!("{err}").contains("top-level dot"));
    }

    #[test]
    fn test_qi_rejects_unterminated_quotes() {
        let err = QualifiedIdentifier::parse_with_default(r#""unterminated.table"#, "public")
            .unwrap_err();
        assert!(format!("{err}").contains("unterminated"));
    }

    #[test]
    fn test_restore_stream_tables_fails_closed() {
        let msg = restore_stream_tables_blocked_error().to_string();
        assert!(msg.contains("restore_stream_tables() is disabled"));
        assert!(msg.contains("protected reinitialization/reconciliation"));
    }

    // ── quote_identifier ───────────────────────────────────────────────

    #[test]
    fn test_quote_identifier_lowercase_simple() {
        assert_eq!(quote_identifier("orders"), "\"orders\"");
    }

    #[test]
    fn test_quote_identifier_with_space() {
        assert_eq!(quote_identifier("my table"), "\"my table\"");
    }

    #[test]
    fn test_quote_identifier_with_double_quote() {
        assert_eq!(quote_identifier("my\"col"), "\"my\"\"col\"");
    }

    // ── quote_ident ────────────────────────────────────────────────────

    #[test]
    fn test_quote_ident_simple_lowercase() {
        // simple lower-case identifiers are returned unquoted
        assert_eq!(quote_ident("orders"), "orders");
    }

    #[test]
    fn test_quote_ident_uppercase_needs_quoting() {
        assert_eq!(quote_ident("Orders"), "\"Orders\"");
    }

    #[test]
    fn test_quote_ident_leading_digit_needs_quoting() {
        assert_eq!(quote_ident("1bad"), "\"1bad\"");
    }

    #[test]
    fn test_quote_ident_underscore_prefix_ok() {
        assert_eq!(quote_ident("_private"), "_private");
    }

    // ── parse_schedule ─────────────────────────────────────────────────

    #[test]
    fn test_parse_schedule_duration_seconds() {
        let s = parse_schedule("30s").unwrap();
        assert!(matches!(s, Schedule::Duration(30)));
    }

    #[test]
    fn test_parse_schedule_duration_minutes() {
        let s = parse_schedule("5m").unwrap();
        assert!(matches!(s, Schedule::Duration(300)));
    }

    #[test]
    fn test_parse_schedule_duration_hours() {
        let s = parse_schedule("1h").unwrap();
        assert!(matches!(s, Schedule::Duration(3600)));
    }

    #[test]
    fn test_parse_schedule_cron_at_prefix() {
        let s = parse_schedule("@daily").unwrap();
        assert!(matches!(s, Schedule::Cron(_)));
    }

    #[test]
    fn test_parse_schedule_cron_five_fields() {
        let s = parse_schedule("0 * * * *").unwrap();
        assert!(matches!(s, Schedule::Cron(_)));
    }

    #[test]
    fn test_parse_schedule_empty_is_error() {
        assert!(parse_schedule("").is_err());
    }

    #[test]
    fn test_parse_schedule_invalid_duration() {
        assert!(parse_schedule("99z").is_err());
    }

    #[test]
    fn test_parse_schedule_invalid_cron() {
        // 7 fields is not a valid 5-field cron
        assert!(parse_schedule("* * * * * * *").is_err());
    }

    // ── detect_select_star ─────────────────────────────────────────────

    #[test]
    fn test_detect_select_star_bare() {
        assert!(detect_select_star("SELECT * FROM t"));
    }

    #[test]
    fn test_detect_select_star_table_qualified() {
        assert!(detect_select_star("SELECT t.* FROM t"));
    }

    #[test]
    fn test_detect_select_star_count_star_ignored() {
        assert!(!detect_select_star("SELECT count(*) FROM t"));
    }

    #[test]
    fn test_detect_select_star_explicit_cols() {
        assert!(!detect_select_star("SELECT id, name FROM t"));
    }

    // ── detect_volatile_functions (OP-6) ────────────────────────────────

    #[test]
    fn test_volatile_now_detected() {
        assert!(detect_volatile_functions("SELECT now(), id FROM t").is_some());
    }

    #[test]
    fn test_volatile_random_detected() {
        assert!(detect_volatile_functions("SELECT random() AS r FROM t").is_some());
    }

    #[test]
    fn test_volatile_current_timestamp_detected() {
        assert!(detect_volatile_functions("SELECT id, current_timestamp FROM t").is_some());
    }

    #[test]
    fn test_volatile_gen_random_uuid_detected() {
        assert!(detect_volatile_functions("SELECT gen_random_uuid()").is_some());
    }

    #[test]
    fn test_volatile_clock_timestamp_detected() {
        assert!(detect_volatile_functions("SELECT clock_timestamp()").is_some());
    }

    #[test]
    fn test_volatile_none_for_stable_query() {
        let q = "SELECT id, name, amount FROM orders WHERE id > 100";
        assert!(detect_volatile_functions(q).is_none());
    }

    #[test]
    fn test_volatile_case_insensitive() {
        // NOW() in uppercase should still be detected
        assert!(detect_volatile_functions("SELECT NOW(), id FROM t").is_some());
    }

    #[test]
    fn test_volatile_txid_current_detected() {
        assert!(detect_volatile_functions("SELECT txid_current()").is_some());
    }

    #[test]
    fn test_volatile_uuid_v4_detected() {
        assert!(detect_volatile_functions("SELECT uuid_generate_v4()").is_some());
    }

    // ── cron_is_due ────────────────────────────────────────────────────

    #[test]
    fn test_cron_is_due_no_last_refresh() {
        // Never refreshed → always due
        assert!(cron_is_due("0 * * * *", None));
    }

    #[test]
    fn test_cron_is_due_epoch_zero() {
        // epoch=0 means it was refreshed at the Unix epoch — due by now
        assert!(cron_is_due("0 * * * *", Some(0)));
    }

    // ── validate_cron ──────────────────────────────────────────────────

    #[test]
    fn test_validate_cron_valid_daily() {
        assert!(validate_cron("@daily").is_ok());
    }

    #[test]
    fn test_validate_cron_valid_five_fields() {
        assert!(validate_cron("*/5 * * * *").is_ok());
    }

    #[test]
    fn test_validate_cron_invalid_expression() {
        assert!(validate_cron("not-a-cron").is_err());
    }
}
