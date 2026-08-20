//! DDL tracking via event triggers and object access hooks.
//!
//! Monitors schema changes on upstream tables and handles direct DROP TABLE
//! on stream table storage tables.
//!
//! ## Event trigger subsystem overview
//!
//! pg_trickle registers two PostgreSQL event triggers at extension-install time:
//!
//! | Trigger name | Event | Handler function |
//! |---|---|---|
//! | `pg_trickle_ddl_tracker` | `ddl_command_end` | `pgtrickle._on_ddl_end()` |
//! | `pg_trickle_sql_drop_tracker` | `sql_drop` | `pgtrickle._on_sql_drop()` |
//!
//! ### ddl_command_end — `_on_ddl_end()`
//!
//! Fires after every successful DDL statement (`ALTER TABLE`, `CREATE TABLE`,
//! `DROP TABLE`, `CREATE INDEX`, etc.).  The handler:
//!
//! 1. Calls `pg_event_trigger_ddl_commands()` to get the list of affected objects.
//! 2. For each affected OID, walks `pgtrickle.pgt_dependencies` to find all
//!    stream tables that depend on it.
//! 3. Marks affected STs `needs_reinit = true` (ALTER TABLE) or
//!    `status = 'ERROR'` (DROP TABLE on an upstream source).
//! 4. Writes an invalidation token to the **invalidation ring** in shared memory
//!    so the scheduler picks up the change on its next wakeup cycle.
//!    If the ring overflows (burst of DDL), a full DAG rebuild is triggered
//!    instead and `pg_trickle_ring_overflows` is incremented in shmem.
//!
//! ### sql_drop — `_on_sql_drop()`
//!
//! Fires specifically when objects are **dropped** (before the object is gone
//! from the catalog).  The handler:
//!
//! 1. Calls `pg_event_trigger_dropped_objects()` for the list of dropped OIDs.
//! 2. Handles `DROP TABLE` on upstream source tables — sets dependent STs to
//!    `ERROR` since the source no longer exists.
//! 3. Handles `DROP TABLE` on a ST storage table itself — cleans up the
//!    `pgt_stream_tables` catalog entry and triggers a DAG rebuild.
//! 4. Handles `DROP FUNCTION` that removes a trigger used by the CDC pipeline.
//!
//! ### Cascade invalidation
//!
//! When ST `A` depends on base table `T`, and ST `B` depends on ST `A`,
//! an ALTER TABLE on `T` must invalidate both `A` and `B`. The cascade
//! is resolved by walking transitive dependencies in `pgtrickle.pgt_dependencies`.
//!
//! ### Invalidation ring
//!
//! The ring is a fixed-capacity shared-memory circular buffer. Each write
//! stores the OID of an invalidated object. On the next scheduler wakeup, the
//! scheduler drains the ring and rebuilds only the affected DAG nodes.
//! If more objects are invalidated than the ring can hold, the overflow flag is
//! set and the scheduler falls back to a full DAG rebuild.
//! Overflow events are counted in `pgt_ring_overflows` (see `shmem.rs`).
//!
//! ## Event trigger: `pg_trickle_ddl_tracker`
//!
//! Installed via `extension_sql!()` as `ON ddl_command_end`. When any DDL
//! completes, the handler queries `pg_event_trigger_ddl_commands()` to
//! discover what changed, then checks `pgtrickle.pgt_dependencies` to find
//! affected stream tables.
//!
//! - **ALTER TABLE** on an upstream source → mark downstream STs
//!   `needs_reinit = true`. On the next scheduler cycle, the refresh
//!   executor will use `REINITIALIZE` instead of `DIFFERENTIAL`.
//!
//! - **DROP TABLE** on an upstream source → set downstream STs to
//!   `status = 'ERROR'` since the source no longer exists.
//!
//! - **DROP TABLE** on a ST storage table itself → clean up the
//!   catalog entry and signal a DAG rebuild.
//!
//! ## Cascade invalidation
//!
//! When ST `A` depends on base table `T`, and ST `B` depends on ST `A`,
//! an ALTER TABLE on `T` must invalidate both `A` and `B`. The cascade
//! is resolved by walking transitive dependencies in `pgtrickle.pgt_dependencies`.

use pgrx::prelude::*;

use crate::catalog::{CdcMode, StDependency, StreamTableMeta};
use crate::dag::StStatus;
use crate::error::PgTrickleError;
use crate::shmem;
use crate::{cdc, config, wal_decoder};

// ── Event trigger handler ──────────────────────────────────────────────────

/// Handler for the `ddl_command_end` event trigger.
///
/// This function is called by PostgreSQL after any DDL statement completes.
/// It inspects the affected objects via `pg_event_trigger_ddl_commands()` and:
/// - Marks dependent stream tables `needs_reinit = true` when their upstream
///   source tables are altered.
/// - Sets dependent stream tables to `status = 'ERROR'` when their upstream
///   source tables are dropped.
/// - Writes an OID invalidation token to the shared-memory invalidation ring
///   so the scheduler can do a partial DAG rebuild on its next wakeup.
///
/// If the ring overflows (too many concurrent DDL statements), the scheduler
/// falls back to a full DAG rebuild and increments the overflow counter
/// (visible via `pgtrickle.health_check()` as `ring_overflow_trend`).
///
/// # Event trigger registration (installed by extension_sql! in lib.rs)
/// ```sql
/// CREATE FUNCTION pgtrickle._on_ddl_end() RETURNS event_trigger
///   LANGUAGE c AS 'MODULE_PATHNAME';
/// CREATE EVENT TRIGGER pg_trickle_ddl_tracker ON ddl_command_end
///     EXECUTE FUNCTION pgtrickle._on_ddl_end();
/// ```
///
/// > **Internal**: This function is called by PostgreSQL trigger machinery,
/// > not directly by users.  It is exposed as `pg_extern` only because
/// > PostgreSQL requires C-callable functions for event triggers.
#[pg_extern(schema = "pgtrickle", name = "_on_ddl_end", sql = false)]
fn pg_trickle_on_ddl_end() {
    // Query the event trigger context for affected objects.
    // pg_event_trigger_ddl_commands() is only available inside an
    // event trigger context — calling it elsewhere will error.
    let commands = match collect_ddl_commands() {
        Ok(cmds) => cmds,
        Err(e) => {
            // Not inside an event trigger context, or SPI error.
            // This can happen during CREATE EXTENSION itself — safe to ignore.
            pgrx::debug1!("pg_trickle_ddl_tracker: could not read DDL commands: {}", e);
            return;
        }
    };

    for cmd in &commands {
        handle_ddl_command(cmd);
    }
}

/// A single DDL command extracted from `pg_event_trigger_ddl_commands()`.
#[derive(Debug, Clone)]
struct DdlCommand {
    /// OID of the affected object.
    objid: pg_sys::Oid,
    /// Object type string (e.g. "table", "index").
    object_type: String,
    /// Command tag (e.g. "ALTER TABLE", "DROP TABLE", "CREATE INDEX").
    command_tag: String,
    /// Schema name of the affected object, if available.
    schema_name: Option<String>,
    /// Object identity string (e.g. "public.orders").
    object_identity: Option<String>,
    /// A17 (v0.36.0): Typed classification derived from object_type + command_tag
    /// at collection time. Avoids repeated string matching after construction.
    kind: DdlCommandKind,
}

/// A17 (v0.36.0): Typed DDL command classification.
///
/// Populated from `pg_event_trigger_ddl_commands()` introspection data at
/// collection time, replacing ad-hoc string matching at usage sites. This
/// makes the classification explicit and avoids silent breakage if PostgreSQL
/// ever adjusts a command-tag wording in a future minor release.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DdlCommandKind {
    /// ALTER TABLE command on a heap table.
    AlterTable,
    /// CREATE TABLE command.
    CreateTable,
    /// Any CREATE/ALTER VIEW command.
    ViewChange,
    /// CREATE TRIGGER command.
    CreateTrigger,
    /// CREATE OR REPLACE FUNCTION / ALTER FUNCTION.
    FunctionChange,
    /// ALTER TYPE command.
    TypeChange,
    /// ALTER DOMAIN / CREATE DOMAIN.
    DomainChange,
    /// CREATE/ALTER/DROP POLICY.
    PolicyChange,
    /// CREATE EXTENSION.
    ExtensionChange,
    /// Any other DDL that pg_trickle does not need to react to.
    Ignored,
}

impl DdlCommandKind {
    /// Classify a DDL event from its PostgreSQL-provided object_type and
    /// command_tag strings.
    ///
    /// This function encapsulates all string matching in one place so that
    /// future PostgreSQL naming changes require only this function to be updated.
    pub(crate) fn from_event(object_type: &str, command_tag: &str) -> Self {
        match (object_type, command_tag) {
            ("table", "ALTER TABLE") => Self::AlterTable,
            ("table", "CREATE TABLE") => Self::CreateTable,
            ("view", "CREATE VIEW")
            | ("view", "CREATE OR REPLACE VIEW")
            | ("view", "ALTER VIEW") => Self::ViewChange,
            ("trigger", "CREATE TRIGGER") => Self::CreateTrigger,
            ("function", "CREATE FUNCTION")
            | ("function", "CREATE OR REPLACE FUNCTION")
            | ("function", "ALTER FUNCTION") => Self::FunctionChange,
            ("type", "ALTER TYPE") => Self::TypeChange,
            ("domain", "ALTER DOMAIN") | ("domain", "CREATE DOMAIN") => Self::DomainChange,
            ("policy", "CREATE POLICY")
            | ("policy", "ALTER POLICY")
            | ("policy", "DROP POLICY") => Self::PolicyChange,
            ("extension", "CREATE EXTENSION") => Self::ExtensionChange,
            _ => Self::Ignored,
        }
    }
}

/// Collect DDL commands from the event trigger context.
fn collect_ddl_commands() -> Result<Vec<DdlCommand>, PgTrickleError> {
    Spi::connect(|client| {
        let table = client
            .select(
                "SELECT objid, object_type, command_tag, schema_name::text, object_identity \
                 FROM pg_event_trigger_ddl_commands()",
                None,
                &[],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        let mut commands = Vec::new();
        for row in table {
            let map_spi = |e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string());

            let objid = row
                .get::<pg_sys::Oid>(1)
                .map_err(map_spi)?
                .unwrap_or(pg_sys::InvalidOid);
            let object_type = row.get::<String>(2).map_err(map_spi)?.unwrap_or_default();
            let command_tag = row.get::<String>(3).map_err(map_spi)?.unwrap_or_default();
            let schema_name = row.get::<String>(4).map_err(map_spi)?;
            let object_identity = row.get::<String>(5).map_err(map_spi)?;

            // A17 (v0.36.0): Classify at collection time via typed enum.
            let kind = DdlCommandKind::from_event(&object_type, &command_tag);

            commands.push(DdlCommand {
                objid,
                object_type,
                command_tag,
                schema_name,
                object_identity,
                kind,
            });
        }
        Ok(commands)
    })
}

/// Classification of a DDL event based on object type and command tag.
/// Kept for internal use by unit tests; at runtime, `DdlCommandKind::from_event`
/// is called at collection time and stored on `DdlCommand::kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // retained for test compatibility
enum DdlEventKind {
    AlterTable,
    CreateTable,
    ViewChange,
    CreateTrigger,
    FunctionChange,
    TypeChange,
    DomainChange,
    PolicyChange,
    ExtensionChange,
    Ignored,
}

/// Process a single DDL command: check for upstream/ST impact and react.
///
/// A17 (v0.36.0): dispatches on `cmd.kind` (pre-classified typed enum) rather
/// than calling `classify_ddl_event()` at dispatch time.
fn handle_ddl_command(cmd: &DdlCommand) {
    match cmd.kind {
        DdlCommandKind::AlterTable => {
            let identity = cmd.object_identity.as_deref().unwrap_or("unknown");
            handle_alter_table(cmd.objid, identity);
        }
        DdlCommandKind::CreateTable => {
            // New tables can't be upstream of any existing ST yet.
        }
        DdlCommandKind::ViewChange => {
            handle_view_change(cmd);
        }
        DdlCommandKind::CreateTrigger => {
            handle_create_trigger(cmd);
        }
        DdlCommandKind::FunctionChange => {
            handle_function_change(cmd);
        }
        DdlCommandKind::TypeChange => {
            handle_type_change(cmd);
        }
        DdlCommandKind::DomainChange => {
            handle_domain_change(cmd);
        }
        DdlCommandKind::PolicyChange => {
            handle_policy_change(cmd);
        }
        DdlCommandKind::ExtensionChange => {
            if let Some(ref ident) = cmd.object_identity
                && ident == "pg_trickle"
            {
                // SCAL-002 (v0.70.0): Bump the launcher install epoch so the
                // launcher detects this CREATE/DROP EXTENSION immediately and
                // re-probes with its fast-poll interval.
                crate::shmem::bump_launcher_install_epoch();
                pgrx::info!(
                    "pg_trickle extension loaded; checking for orphaned catalog entries to restore..."
                );
                if let Err(e) = crate::api::restore_stream_tables() {
                    pgrx::warning!("Failed to automatically restore stream tables: {}", e);
                }
            }
        }
        DdlCommandKind::Ignored => {}
    }
}

// ── View DDL handling ──────────────────────────────────────────────────────

/// Handle CREATE OR REPLACE VIEW / ALTER VIEW on a view that may be an
/// upstream (inlined) dependency of a stream table.
///
/// When a view definition changes, any stream table that inlined it has a
/// stale `defining_query` and needs reinitialising to re-run the view
/// inlining rewrite with the new definition.
fn handle_view_change(cmd: &DdlCommand) {
    let identity = cmd.object_identity.as_deref().unwrap_or("unknown");

    // Check if any ST depends on this view OID via pgt_dependencies.
    let affected_pgt_ids = match find_view_downstream_pgt_ids(cmd.objid) {
        Ok(ids) => ids,
        Err(e) => {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to query deps for view {}: {}",
                identity,
                e,
            );
            return;
        }
    };

    if affected_pgt_ids.is_empty() {
        return;
    }

    pgrx::info!(
        "pg_trickle: view {} changed, marking {} stream table(s) for reinit",
        identity,
        affected_pgt_ids.len(),
    );

    for pgt_id in &affected_pgt_ids {
        if let Err(e) = StreamTableMeta::mark_for_reinitialize(*pgt_id) {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to mark ST {} for reinit after view change: {}",
                pgt_id,
                e,
            );
        }
    }

    // Cascade: STs depending on affected STs also need reinit.
    let cascade_ids = match find_transitive_downstream_sts(&affected_pgt_ids) {
        Ok(ids) => ids,
        Err(e) => {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to cascade view reinit: {}",
                e
            );
            Vec::new()
        }
    };

    for pgt_id in &cascade_ids {
        if let Err(e) = StreamTableMeta::mark_for_reinitialize(*pgt_id) {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to cascade view reinit to ST {}: {}",
                pgt_id,
                e,
            );
        }
    }

    let total = affected_pgt_ids.len() + cascade_ids.len();
    log!(
        "pg_trickle_ddl_tracker: view {} changed → {} ST(s) marked for reinitialize",
        identity,
        total,
    );

    for &id in affected_pgt_ids.iter().chain(cascade_ids.iter()) {
        shmem::signal_dag_invalidation(id);
    }
    // G8.1: Notify other backends to flush their delta/MERGE template caches.
    shmem::bump_cache_generation();
}

// ── Function DDL handling ──────────────────────────────────────────────────

/// Handle CREATE OR REPLACE FUNCTION / ALTER FUNCTION on a function that
/// may be referenced in one or more stream table defining queries.
///
/// The `functions_used` TEXT[] column in `pgt_stream_tables` tracks every
/// function name used by the defining query (populated at creation time).
/// When a function definition changes, we look up affected STs via
/// `StreamTableMeta::find_by_function_name()` and mark them for reinit.
fn handle_function_change(cmd: &DdlCommand) {
    let identity = cmd.object_identity.as_deref().unwrap_or("unknown");

    // object_identity is e.g. "public.my_func(integer, text)" — extract
    // the bare function name (without schema or argument types).
    let func_name = extract_function_name(identity);

    let affected_pgt_ids = match StreamTableMeta::find_by_function_name(&func_name) {
        Ok(ids) => ids,
        Err(e) => {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to query STs for function {}: {}",
                identity,
                e,
            );
            return;
        }
    };

    if affected_pgt_ids.is_empty() {
        return;
    }

    pgrx::info!(
        "pg_trickle: function {} changed, marking {} stream table(s) for reinit",
        identity,
        affected_pgt_ids.len(),
    );

    for pgt_id in &affected_pgt_ids {
        if let Err(e) = StreamTableMeta::mark_for_reinitialize(*pgt_id) {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to mark ST {} for reinit after function change: {}",
                pgt_id,
                e,
            );
        }
    }

    // Cascade: STs depending on affected STs also need reinit.
    let cascade_ids = match find_transitive_downstream_sts(&affected_pgt_ids) {
        Ok(ids) => ids,
        Err(e) => {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to cascade function reinit: {}",
                e
            );
            Vec::new()
        }
    };

    for pgt_id in &cascade_ids {
        if let Err(e) = StreamTableMeta::mark_for_reinitialize(*pgt_id) {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to cascade function reinit to ST {}: {}",
                pgt_id,
                e,
            );
        }
    }

    let total = affected_pgt_ids.len() + cascade_ids.len();
    log!(
        "pg_trickle_ddl_tracker: function {} changed → {} ST(s) marked for reinitialize",
        identity,
        total,
    );

    for &id in affected_pgt_ids.iter().chain(cascade_ids.iter()) {
        shmem::signal_dag_invalidation(id);
    }
    shmem::bump_cache_generation();
}

/// Extract the bare function name from an `object_identity` string.
///
/// PostgreSQL reports function identity as `schema.name(arg_types)`.
/// We strip the schema prefix and the argument-type parenthesised suffix
/// to get the plain name used in `functions_used`.
fn extract_function_name(identity: &str) -> String {
    // Strip argument types: "public.my_func(integer, text)" → "public.my_func"
    let without_args = identity
        .find('(')
        .map(|i| &identity[..i])
        .unwrap_or(identity);

    // Strip schema prefix: "public.my_func" → "my_func"
    let name = without_args
        .rfind('.')
        .map(|i| &without_args[i + 1..])
        .unwrap_or(without_args);

    name.to_lowercase()
}

// ── ALTER TYPE handling (G3.1) ─────────────────────────────────────────────

/// Handle ALTER TYPE on a type that may be used by columns in source tables
/// tracked by stream tables.
///
/// ALTER TYPE ... ADD VALUE is benign (new enum values don't invalidate
/// existing data). ALTER TYPE ... RENAME VALUE or structural changes
/// (composite type modifications) may change query semantics, so we
/// reinitialize affected STs.
fn handle_type_change(cmd: &DdlCommand) {
    let identity = cmd.object_identity.as_deref().unwrap_or("unknown");

    // Find all STs that depend on source tables whose columns use this type.
    // We query pg_attribute to find tables with columns of this type OID,
    // then check if those tables are source dependencies of any ST.
    let affected_pgt_ids = match find_sts_using_type(cmd.objid) {
        Ok(ids) => ids,
        Err(e) => {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to query STs for type {}: {}",
                identity,
                e,
            );
            return;
        }
    };

    if affected_pgt_ids.is_empty() {
        return;
    }

    pgrx::info!(
        "pg_trickle: type {} changed, marking {} stream table(s) for reinit",
        identity,
        affected_pgt_ids.len(),
    );

    for pgt_id in &affected_pgt_ids {
        if let Err(e) = StreamTableMeta::mark_for_reinitialize(*pgt_id) {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to mark ST {} for reinit after type change: {}",
                pgt_id,
                e,
            );
        }
    }

    // Cascade to transitively dependent STs.
    let cascade_ids = match find_transitive_downstream_sts(&affected_pgt_ids) {
        Ok(ids) => ids,
        Err(e) => {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to cascade type reinit: {}",
                e
            );
            Vec::new()
        }
    };

    for pgt_id in &cascade_ids {
        if let Err(e) = StreamTableMeta::mark_for_reinitialize(*pgt_id) {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to cascade type reinit to ST {}: {}",
                pgt_id,
                e,
            );
        }
    }

    let total = affected_pgt_ids.len() + cascade_ids.len();
    log!(
        "pg_trickle_ddl_tracker: ALTER TYPE {} → {} ST(s) marked for reinitialize",
        identity,
        total,
    );

    for &id in affected_pgt_ids.iter().chain(cascade_ids.iter()) {
        shmem::signal_dag_invalidation(id);
    }
    shmem::bump_cache_generation();
}

/// Find all ST pgt_ids that depend on source tables containing columns of
/// the given type OID (or domain based on it).
fn find_sts_using_type(type_oid: pg_sys::Oid) -> Result<Vec<i64>, PgTrickleError> {
    Spi::connect(|client| {
        // Find source tables that have any column of this type, then join
        // with pgt_dependencies to find affected STs.
        let table = client
            .select(
                "SELECT DISTINCT d.pgt_id \
                 FROM pgtrickle.pgt_dependencies d \
                 JOIN pg_attribute a ON a.attrelid = d.source_relid \
                 WHERE a.atttypid = $1 \
                   AND a.attnum > 0 \
                   AND NOT a.attisdropped",
                None,
                &[type_oid.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        let mut ids = Vec::new();
        for row in table {
            if let Ok(Some(id)) = row.get::<i64>(1) {
                ids.push(id);
            }
        }
        Ok(ids)
    })
}

// ── ALTER DOMAIN handling (G3.2) ───────────────────────────────────────────

/// Handle ALTER DOMAIN on a domain that may be used by columns in source
/// tables tracked by stream tables.
///
/// Adding/dropping constraints on a domain can cause the next refresh to fail
/// if delta INSERT rows violate the new constraint. We proactively reinitialize
/// affected STs so the schema change is detected cleanly.
fn handle_domain_change(cmd: &DdlCommand) {
    let identity = cmd.object_identity.as_deref().unwrap_or("unknown");

    // Domains are types in pg_type. The OID from pg_event_trigger_ddl_commands()
    // is the domain's pg_type.oid. We reuse find_sts_using_type since domain
    // columns in pg_attribute have atttypid = the domain OID.
    let affected_pgt_ids = match find_sts_using_type(cmd.objid) {
        Ok(ids) => ids,
        Err(e) => {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to query STs for domain {}: {}",
                identity,
                e,
            );
            return;
        }
    };

    if affected_pgt_ids.is_empty() {
        return;
    }

    pgrx::info!(
        "pg_trickle: domain {} changed, marking {} stream table(s) for reinit",
        identity,
        affected_pgt_ids.len(),
    );

    for pgt_id in &affected_pgt_ids {
        if let Err(e) = StreamTableMeta::mark_for_reinitialize(*pgt_id) {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to mark ST {} for reinit after domain change: {}",
                pgt_id,
                e,
            );
        }
    }

    let cascade_ids = match find_transitive_downstream_sts(&affected_pgt_ids) {
        Ok(ids) => ids,
        Err(e) => {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to cascade domain reinit: {}",
                e
            );
            Vec::new()
        }
    };

    for pgt_id in &cascade_ids {
        if let Err(e) = StreamTableMeta::mark_for_reinitialize(*pgt_id) {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to cascade domain reinit to ST {}: {}",
                pgt_id,
                e,
            );
        }
    }

    let total = affected_pgt_ids.len() + cascade_ids.len();
    log!(
        "pg_trickle_ddl_tracker: ALTER DOMAIN {} → {} ST(s) marked for reinitialize",
        identity,
        total,
    );

    for &id in affected_pgt_ids.iter().chain(cascade_ids.iter()) {
        shmem::signal_dag_invalidation(id);
    }
    shmem::bump_cache_generation();
}

// ── Row-Level Security (RLS) policy handling (G3.3) ────────────────────────

/// Handle CREATE/ALTER/DROP POLICY on a table that may be a source table
/// tracked by stream tables.
///
/// RLS policy changes can silently alter the result set of the defining
/// query if the background worker's role is subject to RLS. We reinitialize
/// affected STs to ensure correctness.
fn handle_policy_change(cmd: &DdlCommand) {
    let identity = cmd.object_identity.as_deref().unwrap_or("unknown");

    // For policy events, the objid from pg_event_trigger_ddl_commands() is
    // the policy OID (pg_policy.oid), not the table OID. Look up the table
    // via pg_policy.polrelid.
    let table_oid = match Spi::get_one::<pg_sys::Oid>(&format!(
        "SELECT polrelid FROM pg_policy WHERE oid = {}",
        cmd.objid.to_u32(),
    )) {
        Ok(Some(oid)) => oid,
        _ => {
            // Can't resolve the table — may already be dropped. Ignore.
            return;
        }
    };

    let affected_pgt_ids = match find_downstream_pgt_ids(table_oid) {
        Ok(ids) => ids,
        Err(e) => {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to query deps for policy change on {}: {}",
                identity,
                e,
            );
            return;
        }
    };

    if affected_pgt_ids.is_empty() {
        return;
    }

    pgrx::info!(
        "pg_trickle: RLS policy {} changed, marking {} stream table(s) for reinit",
        identity,
        affected_pgt_ids.len(),
    );

    for pgt_id in &affected_pgt_ids {
        if let Err(e) = StreamTableMeta::mark_for_reinitialize(*pgt_id) {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to mark ST {} for reinit after policy change: {}",
                pgt_id,
                e,
            );
        }
    }

    let cascade_ids = match find_transitive_downstream_sts(&affected_pgt_ids) {
        Ok(ids) => ids,
        Err(e) => {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to cascade policy reinit: {}",
                e
            );
            Vec::new()
        }
    };

    for pgt_id in &cascade_ids {
        if let Err(e) = StreamTableMeta::mark_for_reinitialize(*pgt_id) {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to cascade policy reinit to ST {}: {}",
                pgt_id,
                e,
            );
        }
    }

    let total = affected_pgt_ids.len() + cascade_ids.len();
    log!(
        "pg_trickle_ddl_tracker: policy change on {} → {} ST(s) marked for reinitialize",
        identity,
        total,
    );

    for &id in affected_pgt_ids.iter().chain(cascade_ids.iter()) {
        shmem::signal_dag_invalidation(id);
    }
    shmem::bump_cache_generation();
}

// ── ALTER TABLE handling ───────────────────────────────────────────────────

/// Handle ALTER TABLE on an object that may be an upstream dependency or
/// a ST storage table itself.
fn handle_alter_table(objid: pg_sys::Oid, identity: &str) {
    // Check if this OID is an upstream source of any ST.
    // Fail closed when relevance cannot be determined: allowing the DDL could
    // leave a tracked source's CDC and downstream stream tables inconsistent.
    let affected_pgt_ids = match find_downstream_pgt_ids(objid) {
        Ok(ids) => ids,
        Err(e) => {
            pgrx::error!(
                "pg_trickle: DDL hook could not inspect dependencies — \
                 schema change blocked to prevent inconsistent state \
                 (source: {}, error: {})",
                identity,
                e
            );
        }
    };

    if affected_pgt_ids.is_empty() {
        // Not an upstream of any ST — might be a ST storage table being altered.
        // That's allowed (e.g., adding indexes), so ignore.
        return;
    }

    // Classify the schema change and only reinitialize for column-affecting
    // changes.  Benign DDL (adding indexes, comments, statistics) and
    // constraint-only changes skip reinit when column tracking is populated.
    let mut reinit_pgt_ids = Vec::new();
    for pgt_id in &affected_pgt_ids {
        let kind = match detect_schema_change_kind(objid, *pgt_id) {
            Ok(k) => k,
            Err(e) => {
                // On error, fall back to conservative reinit.
                pgrx::debug1!(
                    "pg_trickle_ddl_tracker: schema change detection failed for ST {}: {}, \
                     falling back to reinit",
                    pgt_id,
                    e,
                );
                SchemaChangeKind::ColumnChange
            }
        };

        match kind {
            SchemaChangeKind::Benign => {
                pgrx::debug1!(
                    "pg_trickle_ddl_tracker: ALTER TABLE on {} is benign for ST {} — skipping reinit",
                    identity,
                    pgt_id,
                );
            }
            SchemaChangeKind::AddColumnOnly => {
                // S8: When block_source_ddl GUC is enabled, ERROR instead of
                // extending in-place — ADD COLUMN is still a column-affecting DDL.
                if config::pg_trickle_block_source_ddl() {
                    pgrx::error!(
                        "pg_trickle: ALTER TABLE on {} blocked — column-affecting DDL is not \
                         allowed while pg_trickle.block_source_ddl = true (the default). \
                         To proceed: SET pg_trickle.block_source_ddl = false, run the DDL, \
                         then run ALTER STREAM TABLE ... to update the defining query, \
                         and re-enable: SET pg_trickle.block_source_ddl = true.",
                        identity,
                    );
                }
                // Task 3.5: Only new columns were added — extend the change buffer
                // and rebuild the CDC trigger in-place without a full reinit.
                pgrx::notice!(
                    "pg_trickle_ddl_tracker: ALTER TABLE ADD COLUMN on {} for ST {} \
                     — extending change buffer, no reinit needed",
                    identity,
                    pgt_id,
                );
                let cs = config::pg_trickle_change_buffer_schema();
                match crate::cdc::alter_change_buffer_add_columns(objid, &cs, *pgt_id) {
                    Ok(()) => {}
                    Err(e) => {
                        // Buffer update failed — fall back to conservative reinit.
                        pgrx::warning!(
                            "pg_trickle_ddl_tracker: failed to extend change buffer for ST {}: {} \
                             — falling back to reinit",
                            pgt_id,
                            e,
                        );
                        if let Err(e2) = StreamTableMeta::mark_for_reinitialize(*pgt_id) {
                            pgrx::warning!(
                                "pg_trickle_ddl_tracker: failed to mark ST {} for reinit: {}",
                                pgt_id,
                                e2,
                            );
                        }
                        reinit_pgt_ids.push(*pgt_id);
                    }
                }
            }
            SchemaChangeKind::ConstraintChange => {
                // Constraint-only change (e.g., adding/dropping a PK or unique
                // constraint). Currently treat same as benign — the row_id
                // strategy was chosen at creation time based on the PK that
                // existed then. A future enhancement (C-5 keyless tables)
                // could reinit here if the row_id strategy depends on PK.
                pgrx::debug1!(
                    "pg_trickle_ddl_tracker: ALTER TABLE on {} is constraint-only for ST {} \
                     — skipping reinit",
                    identity,
                    pgt_id,
                );
            }
            SchemaChangeKind::ColumnChange => {
                // S8: When block_source_ddl GUC is enabled, ERROR instead of reinit.
                if config::pg_trickle_block_source_ddl() {
                    pgrx::error!(
                        "pg_trickle: ALTER TABLE on {} blocked — column-affecting DDL is not \
                         allowed while pg_trickle.block_source_ddl = true (the default). \
                         To proceed: SET pg_trickle.block_source_ddl = false, run the DDL, \
                         then run ALTER STREAM TABLE ... to update the defining query, \
                         and re-enable: SET pg_trickle.block_source_ddl = true.",
                        identity,
                    );
                }

                if let Err(e) = StreamTableMeta::mark_for_reinitialize(*pgt_id) {
                    pgrx::warning!(
                        "pg_trickle_ddl_tracker: failed to mark ST {} for reinit: {}",
                        pgt_id,
                        e,
                    );
                }
                reinit_pgt_ids.push(*pgt_id);
            }
            SchemaChangeKind::RlsChange => {
                // R9: RLS state changed on source table (ENABLE/DISABLE ROW
                // LEVEL SECURITY or FORCE/NO FORCE ROW LEVEL SECURITY).
                // Stream tables always materialize the full result set
                // (bypassing RLS), but the state change should trigger a
                // reinit to update the stored snapshot and acknowledge the
                // new security posture.
                pgrx::info!(
                    "pg_trickle: RLS state changed on {} — stream table {} marked for reinit",
                    identity,
                    pgt_id,
                );
                if let Err(e) = StreamTableMeta::mark_for_reinitialize(*pgt_id) {
                    pgrx::warning!(
                        "pg_trickle_ddl_tracker: failed to mark ST {} for reinit \
                         after RLS change: {}",
                        pgt_id,
                        e,
                    );
                }
                reinit_pgt_ids.push(*pgt_id);
            }
            SchemaChangeKind::PartitionChange => {
                // PT2: ATTACH/DETACH PARTITION on a partitioned source table.
                // Pre-existing rows in newly attached partitions are not
                // captured by CDC triggers, so reinitialize to pick up all data.
                pgrx::info!(
                    "pg_trickle: partition structure changed on {} — \
                     stream table {} marked for reinit",
                    identity,
                    pgt_id,
                );
                if let Err(e) = StreamTableMeta::mark_for_reinitialize(*pgt_id) {
                    pgrx::warning!(
                        "pg_trickle_ddl_tracker: failed to mark ST {} for reinit \
                         after partition change: {}",
                        pgt_id,
                        e,
                    );
                }
                reinit_pgt_ids.push(*pgt_id);
            }
        }
    }

    // Cascade: find STs that depend on the affected STs (transitive).
    // Only cascade from STs that were actually marked for reinit.
    let cascade_ids = if reinit_pgt_ids.is_empty() {
        Vec::new()
    } else {
        match find_transitive_downstream_sts(&reinit_pgt_ids) {
            Ok(ids) => ids,
            Err(e) => {
                pgrx::warning!("pg_trickle_ddl_tracker: failed to cascade reinit: {}", e);
                Vec::new()
            }
        }
    };

    for pgt_id in &cascade_ids {
        if let Err(e) = StreamTableMeta::mark_for_reinitialize(*pgt_id) {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to cascade reinit to ST {}: {}",
                pgt_id,
                e,
            );
        }
    }

    // Rebuild the CDC trigger function to reflect the current column set.
    // When a column is dropped from the source table, the old trigger function
    // still references NEW."<dropped_col>" — any subsequent DML on the source
    // will fail with "record 'new' has no field '<dropped_col>'".
    // CREATE OR REPLACE replaces only the function body; the trigger binding and
    // change buffer table are unaffected.
    //
    // Always rebuild — even for benign changes — to stay in sync with the
    // catalog. The cost is negligible (single CREATE OR REPLACE FUNCTION).
    let change_schema = config::pg_trickle_change_buffer_schema();
    if let Err(e) = cdc::rebuild_cdc_trigger_function(objid, &change_schema) {
        pgrx::warning!(
            "pg_trickle_ddl_tracker: failed to rebuild CDC trigger function for {}: {}",
            identity,
            e,
        );
    }

    // If any dependency on this source uses WAL-based CDC, abort the
    // transition and fall back to triggers. The schema change invalidates
    // the WAL decoder's column mapping — pgoutput will send a new Relation
    // message, but it's safer to reinitialize from triggers.
    if !reinit_pgt_ids.is_empty() {
        handle_alter_table_wal_fallback(objid, identity, &change_schema);
    }

    let total = reinit_pgt_ids.len() + cascade_ids.len();
    if total > 0 {
        log!(
            "pg_trickle_ddl_tracker: ALTER TABLE on {} → {} ST(s) marked for reinitialize",
            identity,
            total,
        );
        // G8.1: Notify other backends to flush their delta/MERGE template caches.
        shmem::bump_cache_generation();
    } else {
        log!(
            "pg_trickle_ddl_tracker: ALTER TABLE on {} → benign for all {} dependent ST(s), \
             no reinitialize needed",
            identity,
            affected_pgt_ids.len(),
        );
    }
}

/// When ALTER TABLE is detected on a source using WAL-based CDC, abort the
/// WAL transition and fall back to trigger-based CDC.
///
/// `pgoutput` sends a Relation message when the schema changes, but our WAL
/// decoder may not yet handle dynamic column remapping. It's safer to fall
/// back to triggers, reinitialize the downstream STs, and let the transition
/// restart once the reinitialize completes.
fn handle_alter_table_wal_fallback(source_oid: pg_sys::Oid, identity: &str, change_schema: &str) {
    let deps = match StDependency::get_all() {
        Ok(d) => d,
        Err(_) => return,
    };

    for dep in &deps {
        if dep.source_relid != source_oid {
            continue;
        }
        match dep.cdc_mode {
            CdcMode::Wal | CdcMode::Transitioning => {
                if let Err(e) =
                    wal_decoder::abort_wal_transition(dep.source_relid, dep.pgt_id, change_schema)
                {
                    pgrx::warning!(
                        "pg_trickle_ddl_tracker: failed to abort WAL transition for {} (pgt_id={}): {}",
                        identity,
                        dep.pgt_id,
                        e,
                    );
                } else {
                    log!(
                        "pg_trickle_ddl_tracker: ALTER TABLE on {} — \
                         aborted WAL transition (pgt_id={}), reverted to triggers",
                        identity,
                        dep.pgt_id,
                    );
                }
            }
            CdcMode::Trigger => {
                // Already on triggers — nothing to do
            }
        }
    }
}

// ── CREATE TRIGGER warning ─────────────────────────────────────────────────

/// Handle CREATE TRIGGER: if the trigger is on a stream table, emit a
/// warning about trigger behavior during refresh.
fn handle_create_trigger(cmd: &DdlCommand) {
    // The event trigger's objid is the trigger OID (pg_trigger.oid), not the
    // table OID. Look up the table via pg_trigger.tgrelid.
    let tgrelid = match Spi::get_one::<pg_sys::Oid>(&format!(
        "SELECT tgrelid FROM pg_trigger WHERE oid = {}",
        cmd.objid.to_u32(),
    )) {
        Ok(Some(oid)) => oid,
        _ => return, // Can't resolve — ignore silently
    };

    // F35 (G3.5): Block triggers on change buffer tables.
    // User triggers on pgtrickle_changes.changes_<oid> could corrupt CDC data.
    let change_schema = config::pg_trickle_change_buffer_schema();
    let is_change_buffer = Spi::get_one::<bool>(&format!(
        "SELECT EXISTS (SELECT 1 FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.oid = {} AND n.nspname = '{}')",
        tgrelid.to_u32(),
        change_schema.replace('\'', "''"),
    ))
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if is_change_buffer {
        pgrx::error!(
            "pg_trickle: creating triggers on change buffer tables (schema '{}') is not allowed. \
             These tables are managed internally by pg_trickle for change data capture.",
            change_schema
        );
    }

    // Check if the table is a stream table.
    if !is_st_storage_table(tgrelid) {
        return;
    }

    let trigger_identity = cmd
        .object_identity
        .as_deref()
        .unwrap_or("(unknown trigger)");
    let user_triggers_mode = config::pg_trickle_user_triggers_mode();

    if user_triggers_mode == config::UserTriggersMode::Off {
        pgrx::warning!(
            "pg_trickle: trigger {} is on a stream table, but pg_trickle.user_triggers = 'off'. \
             This trigger will NOT fire correctly during refresh. \
             Set pg_trickle.user_triggers = 'auto' to enable trigger support.",
            trigger_identity,
        );
    } else {
        pgrx::notice!(
            "pg_trickle: trigger {} is on a stream table. \
             It will fire during DIFFERENTIAL refresh with correct TG_OP/OLD/NEW. \
             Note: row-level triggers do NOT fire during FULL refresh. \
             Use REFRESH MODE DIFFERENTIAL to ensure triggers fire on every change.",
            trigger_identity,
        );
    }
}

// ── DROP TABLE handling (via SQL event trigger for dropped objects) ─────

/// Handler for the `sql_drop` event trigger.
///
/// Fires when PostgreSQL objects are dropped.  The handler uses
/// `pg_event_trigger_dropped_objects()` to get the list of dropped OIDs and:
/// - For a dropped upstream source table: marks dependent stream tables as
///   `status = 'ERROR'` since the source no longer exists.
/// - For a dropped stream table storage table itself: removes the
///   `pgt_stream_tables` catalog entry and signals a DAG rebuild.
/// - For a dropped CDC trigger function: marks the associated stream table
///   for reinitialization.
///
/// The `sql_drop` trigger fires **before** the object is removed from the
/// PostgreSQL catalog, so the handler can still look up metadata about the
/// dropped objects via system catalog queries.
///
/// # Event trigger registration (installed by extension_sql! in lib.rs)
/// ```sql
/// CREATE FUNCTION pgtrickle._on_sql_drop() RETURNS event_trigger
///   LANGUAGE c AS 'MODULE_PATHNAME';
/// CREATE EVENT TRIGGER pg_trickle_sql_drop_tracker ON sql_drop
///     EXECUTE FUNCTION pgtrickle._on_sql_drop();
/// ```
///
/// > **Internal**: This function is called by PostgreSQL trigger machinery,
/// > not directly by users.  It is exposed as `pg_extern` only because
/// > PostgreSQL requires C-callable functions for event triggers.
#[pg_extern(schema = "pgtrickle", name = "_on_sql_drop", sql = false)]
fn pg_trickle_on_sql_drop() {
    let dropped = match collect_dropped_objects() {
        Ok(objs) => objs,
        Err(e) => {
            pgrx::debug1!(
                "pg_trickle_ddl_tracker: could not read dropped objects: {}",
                e
            );
            return;
        }
    };

    for obj in &dropped {
        match obj.object_type.as_str() {
            "table" => handle_dropped_table(obj),
            "view" => handle_dropped_view(obj),
            "function" => handle_dropped_function(obj),
            _ => {}
        }
    }
}

/// A dropped object from `pg_event_trigger_dropped_objects()`.
#[derive(Debug, Clone)]
struct DroppedObject {
    objid: pg_sys::Oid,
    object_type: String,
    schema_name: Option<String>,
    object_name: Option<String>,
    object_identity: Option<String>,
}

/// Collect dropped objects from the event trigger context.
fn collect_dropped_objects() -> Result<Vec<DroppedObject>, PgTrickleError> {
    Spi::connect(|client| {
        let table = client
            .select(
                "SELECT objid, object_type, schema_name::text, object_name::text, object_identity \
                 FROM pg_event_trigger_dropped_objects()",
                None,
                &[],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        let mut objects = Vec::new();
        for row in table {
            let map_spi = |e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string());

            let objid = row
                .get::<pg_sys::Oid>(1)
                .map_err(map_spi)?
                .unwrap_or(pg_sys::InvalidOid);
            let object_type = row.get::<String>(2).map_err(map_spi)?.unwrap_or_default();
            let schema_name = row.get::<String>(3).map_err(map_spi)?;
            let object_name = row.get::<String>(4).map_err(map_spi)?;
            let object_identity = row.get::<String>(5).map_err(map_spi)?;

            objects.push(DroppedObject {
                objid,
                object_type,
                schema_name,
                object_name,
                object_identity,
            });
        }
        Ok(objects)
    })
}

/// Handle a dropped table: either an upstream source or a ST storage table.
fn handle_dropped_table(obj: &DroppedObject) {
    let identity = obj.object_identity.as_deref().unwrap_or("unknown");

    // Case 1: Check if the dropped table is a ST storage table.
    let is_st = is_st_storage_table(obj.objid);
    if is_st {
        handle_st_storage_dropped(obj.objid, identity);
        return;
    }

    // A source drop does not cascade to pg_trickle's separately-owned CDC
    // buffer or its registry row. Remove both before a replacement relation
    // can reuse the stable buffer name.
    let buffer =
        cdc::buffer_qualified_name_for_oid(&config::pg_trickle_change_buffer_schema(), obj.objid);
    let drop_buffer_sql = format!("DROP TABLE IF EXISTS {buffer} CASCADE");
    if let Err(e) = Spi::run(&drop_buffer_sql) {
        // nosemgrep: rust.spi.run.dynamic-format — buffer name is derived from source OID.
        // nosemgrep: rust.spi.run.dynamic-format — buffer name is derived from source OID.
        pgrx::warning!(
            "pg_trickle_ddl_tracker: failed to drop CDC buffer for {}: {}",
            identity,
            e,
        );
    }
    let registry_exists =
        Spi::get_one::<bool>("SELECT to_regclass('pgtrickle.pgt_change_buffers') IS NOT NULL")
            .unwrap_or(Some(false))
            .unwrap_or(false);
    if registry_exists
        && let Err(e) = Spi::run_with_args(
            "DELETE FROM pgtrickle.pgt_change_buffers \
             WHERE source_kind = 'BASE' AND source_id = $1",
            &[i64::from(obj.objid.to_u32()).into()],
        )
    {
        pgrx::warning!(
            "pg_trickle_ddl_tracker: failed to remove CDC registry row for {}: {}",
            identity,
            e,
        );
    }
    let _ = Spi::run_with_args(
        "DELETE FROM pgtrickle.pgt_change_tracking WHERE source_relid = $1",
        &[obj.objid.into()],
    );

    // Case 2: Check if the dropped table is an upstream source of any ST.
    let affected_pgt_ids = match find_downstream_pgt_ids(obj.objid) {
        Ok(ids) => ids,
        Err(e) => {
            pgrx::error!(
                "pg_trickle: DROP TABLE hook could not inspect dependencies — \
                 drop blocked to prevent inconsistent state (source: {}, error: {})",
                identity,
                e
            );
        }
    };

    if affected_pgt_ids.is_empty() {
        return;
    }

    // Mark affected STs as ERROR — their source is gone.
    for pgt_id in &affected_pgt_ids {
        if let Err(e) = StreamTableMeta::update_status(*pgt_id, StStatus::Error) {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to set ST {} to ERROR: {}",
                pgt_id,
                e,
            );
        }
    }

    // Cascade: STs depending on now-errored STs also go to ERROR.
    let cascade_ids = match find_transitive_downstream_sts(&affected_pgt_ids) {
        Ok(ids) => ids,
        Err(e) => {
            pgrx::warning!("pg_trickle_ddl_tracker: failed to cascade error: {}", e);
            Vec::new()
        }
    };

    for pgt_id in &cascade_ids {
        if let Err(e) = StreamTableMeta::update_status(*pgt_id, StStatus::Error) {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to cascade ERROR to ST {}: {}",
                pgt_id,
                e,
            );
        }
    }

    shmem::signal_dag_rebuild();

    let total = affected_pgt_ids.len() + cascade_ids.len();
    log!(
        "pg_trickle_ddl_tracker: DROP TABLE {} → {} ST(s) set to ERROR",
        identity,
        total,
    );
}

/// Handle the case where a ST's own storage table was dropped.
///
/// Clean up the catalog entry and signal a DAG rebuild.
fn handle_st_storage_dropped(relid: pg_sys::Oid, identity: &str) {
    // Find and delete the ST catalog entry.
    let st = match StreamTableMeta::get_by_relid(relid) {
        Ok(st) => st,
        Err(_) => return, // Already cleaned up or not found
    };

    if let Err(e) = StreamTableMeta::delete(st.pgt_id) {
        pgrx::warning!(
            "pg_trickle_ddl_tracker: failed to clean up catalog for dropped ST {}: {}",
            identity,
            e,
        );
        return;
    }

    // Signal the scheduler to rebuild the DAG.
    shmem::signal_dag_invalidation(st.pgt_id);

    log!(
        "pg_trickle_ddl_tracker: ST storage table {} dropped → catalog cleaned, DAG rebuild signaled",
        identity,
    );
}

/// Handle a dropped view: if the view was inlined into any ST, mark those
/// STs as ERROR since the original query can no longer be re-expanded.
fn handle_dropped_view(obj: &DroppedObject) {
    let identity = obj.object_identity.as_deref().unwrap_or("unknown");

    let affected_pgt_ids = match find_view_downstream_pgt_ids(obj.objid) {
        Ok(ids) => ids,
        Err(e) => {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to query deps for dropped view {}: {}",
                identity,
                e,
            );
            return;
        }
    };

    if affected_pgt_ids.is_empty() {
        return;
    }

    // Mark affected STs as ERROR — the inlined view no longer exists,
    // so reinit would fail.
    for pgt_id in &affected_pgt_ids {
        if let Err(e) = StreamTableMeta::update_status(*pgt_id, StStatus::Error) {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to set ST {} to ERROR after view drop: {}",
                pgt_id,
                e,
            );
        }
    }

    // Cascade: STs depending on now-errored STs also go to ERROR.
    let cascade_ids = match find_transitive_downstream_sts(&affected_pgt_ids) {
        Ok(ids) => ids,
        Err(e) => {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to cascade view drop error: {}",
                e
            );
            Vec::new()
        }
    };

    for pgt_id in &cascade_ids {
        if let Err(e) = StreamTableMeta::update_status(*pgt_id, StStatus::Error) {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to cascade view drop ERROR to ST {}: {}",
                pgt_id,
                e,
            );
        }
    }

    let total = affected_pgt_ids.len() + cascade_ids.len();
    log!(
        "pg_trickle_ddl_tracker: DROP VIEW {} → {} ST(s) set to ERROR",
        identity,
        total,
    );
}

/// Handle a dropped function: look up STs that reference it via
/// `functions_used` and mark them for reinit (the function may be
/// recreated under the same name, so reinit is appropriate rather
/// than ERROR).
fn handle_dropped_function(obj: &DroppedObject) {
    let identity = obj.object_identity.as_deref().unwrap_or("unknown");
    let func_name = extract_function_name(identity);

    let affected_pgt_ids = match StreamTableMeta::find_by_function_name(&func_name) {
        Ok(ids) => ids,
        Err(e) => {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to query STs for dropped function {}: {}",
                identity,
                e,
            );
            return;
        }
    };

    if affected_pgt_ids.is_empty() {
        return;
    }

    pgrx::info!(
        "pg_trickle: function {} dropped, marking {} stream table(s) for reinit",
        identity,
        affected_pgt_ids.len(),
    );

    for pgt_id in &affected_pgt_ids {
        if let Err(e) = StreamTableMeta::mark_for_reinitialize(*pgt_id) {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to mark ST {} for reinit after function drop: {}",
                pgt_id,
                e,
            );
        }
    }

    // Cascade
    let cascade_ids = match find_transitive_downstream_sts(&affected_pgt_ids) {
        Ok(ids) => ids,
        Err(e) => {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to cascade function drop reinit: {}",
                e
            );
            Vec::new()
        }
    };

    for pgt_id in &cascade_ids {
        if let Err(e) = StreamTableMeta::mark_for_reinitialize(*pgt_id) {
            pgrx::warning!(
                "pg_trickle_ddl_tracker: failed to cascade function drop reinit to ST {}: {}",
                pgt_id,
                e,
            );
        }
    }

    let total = affected_pgt_ids.len() + cascade_ids.len();
    log!(
        "pg_trickle_ddl_tracker: DROP FUNCTION {} → {} ST(s) marked for reinitialize",
        identity,
        total,
    );

    for &id in affected_pgt_ids.iter().chain(cascade_ids.iter()) {
        shmem::signal_dag_invalidation(id);
    }
    shmem::bump_cache_generation();
}

// ── Dependency queries ─────────────────────────────────────────────────────

/// Find ST IDs that directly depend on a given base or foreign-table source OID.
///
/// Stream-table dependencies are handled separately by DAG/SCC logic. They
/// must not be treated as CDC-backed source-table dependencies here, or DDL on
/// a stream table's storage relation during refresh would incorrectly try to
/// rebuild change-buffer infrastructure for that stream table.
fn find_downstream_pgt_ids(source_oid: pg_sys::Oid) -> Result<Vec<i64>, PgTrickleError> {
    Spi::connect(|client| {
        let table = client
            .select(
                "SELECT pgt_id FROM pgtrickle.pgt_dependencies \
                 WHERE source_relid = $1 \
                   AND source_type IN ('TABLE', 'FOREIGN_TABLE')",
                None,
                &[source_oid.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        let mut ids = Vec::new();
        for row in table {
            if let Ok(Some(id)) = row.get::<i64>(1) {
                ids.push(id);
            }
        }
        Ok(ids)
    })
}

/// Find ST IDs that directly depend on a given view OID.
///
/// Used when a view is altered or dropped: views are recorded as
/// `source_type = 'VIEW'` in `pgt_dependencies` and are not covered by
/// the CDC-focused `find_downstream_pgt_ids`.
fn find_view_downstream_pgt_ids(view_oid: pg_sys::Oid) -> Result<Vec<i64>, PgTrickleError> {
    Spi::connect(|client| {
        let table = client
            .select(
                "SELECT pgt_id FROM pgtrickle.pgt_dependencies \
                 WHERE source_relid = $1 \
                   AND source_type = 'VIEW'",
                None,
                &[view_oid.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        let mut ids = Vec::new();
        for row in table {
            if let Ok(Some(id)) = row.get::<i64>(1) {
                ids.push(id);
            }
        }
        Ok(ids)
    })
}

/// Find all transitively downstream STs: given a set of directly-affected
/// ST IDs, walk the dependency graph to find STs that depend on them.
///
/// Returns only the *additional* ST IDs (not the input set).
fn find_transitive_downstream_sts(initial_pgt_ids: &[i64]) -> Result<Vec<i64>, PgTrickleError> {
    if initial_pgt_ids.is_empty() {
        return Ok(Vec::new());
    }

    // We need to find which STs have a dependency edge to the storage
    // table (pgt_relid) of any of the initial STs, and then repeat
    // transitively.
    //
    // Query: for each affected ST, get its pgt_relid, then find STs
    // that list that relid as a source.

    let mut visited: std::collections::HashSet<i64> = initial_pgt_ids.iter().copied().collect();
    let mut queue: std::collections::VecDeque<i64> = initial_pgt_ids.iter().copied().collect();
    let mut cascade_ids = Vec::new();

    while let Some(pgt_id) = queue.pop_front() {
        // Get the storage table OID for this ST.
        let relid = match get_pgt_relid(pgt_id) {
            Ok(Some(oid)) => oid,
            _ => continue,
        };

        // Find STs that depend on this ST's storage table.
        let downstream = find_downstream_pgt_ids(relid)?;
        for child_id in downstream {
            if visited.insert(child_id) {
                cascade_ids.push(child_id);
                queue.push_back(child_id);
            }
        }
    }

    Ok(cascade_ids)
}

/// Get the storage table OID (pgt_relid) for a stream table.
fn get_pgt_relid(pgt_id: i64) -> Result<Option<pg_sys::Oid>, PgTrickleError> {
    Spi::get_one_with_args::<pg_sys::Oid>(
        "SELECT pgt_relid FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1",
        &[pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))
}

/// Check if a given OID is a ST storage table.
fn is_st_storage_table(relid: pg_sys::Oid) -> bool {
    Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_relid = $1)",
        &[relid.into()],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false)
}

// ── Schema change detection helpers ────────────────────────────────────────

/// Detect what kind of schema change occurred on a table.
///
/// This can be used to determine whether a reinitialize is truly needed
/// (e.g., column add/drop/type change) vs. a benign change (e.g., adding
/// a constraint or comment).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaChangeKind {
    /// Column added, dropped, or type changed — requires reinitialize.
    ColumnChange,
    /// Only new columns were added; existing columns are unchanged.
    /// The change buffer and CDC trigger can be updated in-place — no reinit needed.
    AddColumnOnly,
    /// Constraint or index change — may not require reinitialize.
    ConstraintChange,
    /// RLS state changed (ENABLE/DISABLE ROW LEVEL SECURITY or FORCE/NO FORCE).
    RlsChange,
    /// Partition structure changed (ATTACH/DETACH PARTITION) — requires reinitialize
    /// because pre-existing rows in newly attached partitions are not captured by
    /// CDC triggers.
    PartitionChange,
    /// Other DDL (comment, owner change, etc.) — no reinitialize needed.
    Benign,
}

/// Detect the kind of schema change by comparing stored column metadata
/// against the current catalog state.
///
/// When a column snapshot exists (S7), performs precise comparison:
/// - Missing columns → `ColumnChange`
/// - Type OID changed → `ColumnChange`
/// - New columns added → `ColumnChange` (may affect NATURAL JOIN, SELECT *)
/// - Same fingerprint → `Benign` (fast path)
/// - All match → `ConstraintChange` (e.g., PK/unique constraint changes)
///
/// Without snapshots, falls back to checking `columns_used` existence.
pub fn detect_schema_change_kind(
    source_oid: pg_sys::Oid,
    pgt_id: i64,
) -> Result<SchemaChangeKind, PgTrickleError> {
    // Fast path: compare schema fingerprints.
    // If the stored fingerprint matches the current one, nothing changed
    // (neither columns, RLS state, nor partition structure).
    // Since v0.6.0, the fingerprint includes partition_child_count.
    if let Ok(Some(stored_fp)) = crate::catalog::get_schema_fingerprint(pgt_id, source_oid)
        && !stored_fp.is_empty()
        && let Ok((_, current_fp)) = crate::catalog::build_column_snapshot(source_oid)
        && stored_fp == current_fp
    {
        return Ok(SchemaChangeKind::Benign);
    }

    // Detailed path: compare stored column snapshot against current pg_attribute.
    if let Ok(Some(snapshot)) = crate::catalog::get_column_snapshot(pgt_id, source_oid) {
        match &snapshot.0 {
            // New format (v0.5.0+): Object with "columns", "rls_enabled", "rls_forced".
            serde_json::Value::Object(obj) => {
                if let Some(serde_json::Value::Array(entries)) = obj.get("columns")
                    && !entries.is_empty()
                {
                    let col_kind = detect_from_snapshot(source_oid, entries)?;

                    // Column-level changes take priority.
                    if matches!(
                        col_kind,
                        SchemaChangeKind::ColumnChange | SchemaChangeKind::AddColumnOnly
                    ) {
                        return Ok(col_kind);
                    }

                    // Columns unchanged — check if RLS state changed.
                    let stored_rls = obj
                        .get("rls_enabled")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    let stored_force = obj
                        .get("rls_forced")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    let (current_rls, current_force) = crate::catalog::query_rls_flags(source_oid)?;
                    if stored_rls != current_rls || stored_force != current_force {
                        return Ok(SchemaChangeKind::RlsChange);
                    }

                    // PT2: Check if partition structure changed.
                    let stored_child_count = obj
                        .get("partition_child_count")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    let current_child_count =
                        crate::catalog::query_partition_child_count(source_oid).unwrap_or(0);
                    if stored_child_count != current_child_count {
                        return Ok(SchemaChangeKind::PartitionChange);
                    }

                    return Ok(col_kind);
                }
            }
            // Legacy format (pre-v0.5.0): plain Array of column entries.
            serde_json::Value::Array(entries) if !entries.is_empty() => {
                return detect_from_snapshot(source_oid, entries);
            }
            _ => {}
        }
    }

    // Legacy fallback: no snapshot available — use columns_used presence check.
    let tracked_cols = get_tracked_columns(pgt_id, source_oid)?;

    if tracked_cols.is_empty() {
        // No column-level tracking — conservatively assume column change.
        return Ok(SchemaChangeKind::ColumnChange);
    }

    // Check if any tracked columns were altered or dropped.
    for col_name in &tracked_cols {
        let exists = Spi::get_one_with_args::<bool>(
            "SELECT EXISTS( \
                SELECT 1 FROM pg_attribute \
                WHERE attrelid = $1 AND attname = $2 \
                AND attnum > 0 AND NOT attisdropped \
            )",
            &[source_oid.into(), col_name.as_str().into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .unwrap_or(false);

        if !exists {
            return Ok(SchemaChangeKind::ColumnChange);
        }
    }

    // All tracked columns still exist — likely a benign change.
    Ok(SchemaChangeKind::ConstraintChange)
}

/// Compare a stored column snapshot against the current `pg_attribute` state.
///
/// Detects: columns dropped, columns added, type OID changed.
fn detect_from_snapshot(
    source_oid: pg_sys::Oid,
    stored_entries: &[serde_json::Value],
) -> Result<SchemaChangeKind, PgTrickleError> {
    // Build a map of current columns: name → type_oid
    let current_cols: std::collections::HashMap<String, i64> = Spi::connect(|client| {
        let sql = format!(
            "SELECT attname::text, atttypid::bigint \
             FROM pg_attribute \
             WHERE attrelid = {} AND attnum > 0 AND NOT attisdropped \
             ORDER BY attnum",
            source_oid.to_u32(),
        );
        let result = client
            .select(&sql, None, &[])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        let mut map = std::collections::HashMap::new();
        for row in result {
            let name: String = row
                .get(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or_default();
            let type_oid: i64 = row
                .get(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(0);
            map.insert(name, type_oid);
        }
        Ok(map)
    })?;

    compare_snapshot_with_current(stored_entries, &current_cols)
}

/// Pure comparison of stored column snapshot against current column state.
///
/// Detects: columns dropped, columns added, type OID changed.
fn compare_snapshot_with_current(
    stored_entries: &[serde_json::Value],
    current_cols: &std::collections::HashMap<String, i64>,
) -> Result<SchemaChangeKind, PgTrickleError> {
    // Check each stored column still exists with the same type.
    for entry in stored_entries {
        let name = entry["name"].as_str().unwrap_or("");
        let stored_type = entry["type_oid"].as_i64().unwrap_or(0);

        match current_cols.get(name) {
            None => return Ok(SchemaChangeKind::ColumnChange), // dropped
            Some(&current_type) if current_type != stored_type => {
                return Ok(SchemaChangeKind::ColumnChange); // type changed
            }
            _ => {} // matches
        }
    }

    // Check if new columns were added (may affect NATURAL JOIN, SELECT *).
    let stored_names: std::collections::HashSet<&str> = stored_entries
        .iter()
        .filter_map(|e| e["name"].as_str())
        .collect();

    let has_new = current_cols
        .keys()
        .any(|n| !stored_names.contains(n.as_str()));
    if has_new {
        // Only additive change: existing columns intact, new columns appear.
        // The change buffer can be extended in-place; no full reinit needed.
        return Ok(SchemaChangeKind::AddColumnOnly);
    }

    // Column set is identical — this is a constraint-only change.
    Ok(SchemaChangeKind::ConstraintChange)
}

/// Get column names tracked for a given ST + source pair.
fn get_tracked_columns(
    pgt_id: i64,
    source_oid: pg_sys::Oid,
) -> Result<Vec<String>, PgTrickleError> {
    // columns_used is stored as TEXT[] in pgt_dependencies.
    let cols = Spi::get_one_with_args::<Vec<String>>(
        "SELECT columns_used FROM pgtrickle.pgt_dependencies \
         WHERE pgt_id = $1 AND source_relid = $2",
        &[pgt_id.into(), source_oid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    Ok(cols.unwrap_or_default())
}

// ── Unit tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_change_kind_eq() {
        assert_eq!(
            SchemaChangeKind::ColumnChange,
            SchemaChangeKind::ColumnChange
        );
        assert_ne!(SchemaChangeKind::ColumnChange, SchemaChangeKind::Benign);
        assert_ne!(SchemaChangeKind::ConstraintChange, SchemaChangeKind::Benign,);
        assert_ne!(
            SchemaChangeKind::AddColumnOnly,
            SchemaChangeKind::ColumnChange
        );
        assert_ne!(SchemaChangeKind::AddColumnOnly, SchemaChangeKind::Benign);
        assert_eq!(
            SchemaChangeKind::AddColumnOnly,
            SchemaChangeKind::AddColumnOnly
        );
        assert_eq!(SchemaChangeKind::RlsChange, SchemaChangeKind::RlsChange);
        assert_ne!(SchemaChangeKind::RlsChange, SchemaChangeKind::Benign);
        assert_eq!(
            SchemaChangeKind::PartitionChange,
            SchemaChangeKind::PartitionChange
        );
        assert_ne!(SchemaChangeKind::PartitionChange, SchemaChangeKind::Benign);
        assert_ne!(
            SchemaChangeKind::PartitionChange,
            SchemaChangeKind::ColumnChange
        );
        assert_ne!(SchemaChangeKind::RlsChange, SchemaChangeKind::ColumnChange);
        assert_ne!(
            SchemaChangeKind::RlsChange,
            SchemaChangeKind::ConstraintChange
        );
    }

    #[test]
    fn test_ddl_command_debug() {
        let cmd = DdlCommand {
            objid: pg_sys::InvalidOid,
            object_type: "table".to_string(),
            command_tag: "ALTER TABLE".to_string(),
            schema_name: Some("public".to_string()),
            object_identity: Some("public.orders".to_string()),
            kind: DdlCommandKind::AlterTable,
        };
        let debug = format!("{:?}", cmd);
        assert!(debug.contains("ALTER TABLE"));
        assert!(debug.contains("public.orders"));
    }

    #[test]
    fn test_dropped_object_debug() {
        let obj = DroppedObject {
            objid: pg_sys::InvalidOid,
            object_type: "table".to_string(),
            schema_name: Some("public".to_string()),
            object_name: Some("orders".to_string()),
            object_identity: Some("public.orders".to_string()),
        };
        let debug = format!("{:?}", obj);
        assert!(debug.contains("public.orders"));
    }

    #[test]
    fn test_extract_function_name_with_schema_and_args() {
        assert_eq!(
            extract_function_name("public.my_func(integer, text)"),
            "my_func"
        );
    }

    #[test]
    fn test_extract_function_name_no_schema() {
        assert_eq!(extract_function_name("my_func(integer)"), "my_func");
    }

    #[test]
    fn test_extract_function_name_no_args() {
        assert_eq!(extract_function_name("public.my_func"), "my_func");
    }

    #[test]
    fn test_extract_function_name_bare() {
        assert_eq!(extract_function_name("my_func"), "my_func");
    }

    #[test]
    fn test_extract_function_name_case_insensitive() {
        assert_eq!(
            extract_function_name("public.MyMixedCase(INT)"),
            "mymixedcase"
        );
    }

    // ── classify_ddl_event tests (A17: now tests DdlCommandKind::from_event) ──

    #[test]
    fn test_classify_alter_table() {
        assert_eq!(
            DdlCommandKind::from_event("table", "ALTER TABLE"),
            DdlCommandKind::AlterTable,
        );
    }

    #[test]
    fn test_classify_create_table() {
        assert_eq!(
            DdlCommandKind::from_event("table", "CREATE TABLE"),
            DdlCommandKind::CreateTable,
        );
    }

    #[test]
    fn test_classify_create_view() {
        assert_eq!(
            DdlCommandKind::from_event("view", "CREATE VIEW"),
            DdlCommandKind::ViewChange,
        );
    }

    #[test]
    fn test_classify_create_or_replace_view() {
        assert_eq!(
            DdlCommandKind::from_event("view", "CREATE OR REPLACE VIEW"),
            DdlCommandKind::ViewChange,
        );
    }

    #[test]
    fn test_classify_alter_view() {
        assert_eq!(
            DdlCommandKind::from_event("view", "ALTER VIEW"),
            DdlCommandKind::ViewChange,
        );
    }

    #[test]
    fn test_classify_create_trigger() {
        assert_eq!(
            DdlCommandKind::from_event("trigger", "CREATE TRIGGER"),
            DdlCommandKind::CreateTrigger,
        );
    }

    #[test]
    fn test_classify_function_change() {
        assert_eq!(
            DdlCommandKind::from_event("function", "CREATE FUNCTION"),
            DdlCommandKind::FunctionChange,
        );
        assert_eq!(
            DdlCommandKind::from_event("function", "ALTER FUNCTION"),
            DdlCommandKind::FunctionChange,
        );
        // A17: Also handle CREATE OR REPLACE FUNCTION (v0.36.0 addition)
        assert_eq!(
            DdlCommandKind::from_event("function", "CREATE OR REPLACE FUNCTION"),
            DdlCommandKind::FunctionChange,
        );
    }

    #[test]
    fn test_classify_type_change() {
        assert_eq!(
            DdlCommandKind::from_event("type", "ALTER TYPE"),
            DdlCommandKind::TypeChange,
        );
    }

    #[test]
    fn test_classify_domain_change() {
        assert_eq!(
            DdlCommandKind::from_event("domain", "ALTER DOMAIN"),
            DdlCommandKind::DomainChange,
        );
        assert_eq!(
            DdlCommandKind::from_event("domain", "CREATE DOMAIN"),
            DdlCommandKind::DomainChange,
        );
    }

    #[test]
    fn test_classify_policy_change() {
        assert_eq!(
            DdlCommandKind::from_event("policy", "CREATE POLICY"),
            DdlCommandKind::PolicyChange,
        );
        assert_eq!(
            DdlCommandKind::from_event("policy", "ALTER POLICY"),
            DdlCommandKind::PolicyChange,
        );
        assert_eq!(
            DdlCommandKind::from_event("policy", "DROP POLICY"),
            DdlCommandKind::PolicyChange,
        );
    }

    #[test]
    fn test_classify_unknown_is_ignored() {
        assert_eq!(
            DdlCommandKind::from_event("index", "CREATE INDEX"),
            DdlCommandKind::Ignored,
        );
        assert_eq!(
            DdlCommandKind::from_event("table", "DROP TABLE"),
            DdlCommandKind::Ignored,
        );
    }

    // ── compare_snapshot_with_current tests ─────────────────────────

    #[test]
    fn test_snapshot_identical_is_constraint_change() {
        let stored = vec![
            serde_json::json!({"name": "id", "type_oid": 23}),
            serde_json::json!({"name": "val", "type_oid": 25}),
        ];
        let mut current = std::collections::HashMap::new();
        current.insert("id".to_string(), 23i64);
        current.insert("val".to_string(), 25i64);

        assert_eq!(
            compare_snapshot_with_current(&stored, &current).unwrap(),
            SchemaChangeKind::ConstraintChange,
        );
    }

    #[test]
    fn test_snapshot_column_dropped() {
        let stored = vec![
            serde_json::json!({"name": "id", "type_oid": 23}),
            serde_json::json!({"name": "old_col", "type_oid": 25}),
        ];
        let mut current = std::collections::HashMap::new();
        current.insert("id".to_string(), 23i64);
        // old_col is missing

        assert_eq!(
            compare_snapshot_with_current(&stored, &current).unwrap(),
            SchemaChangeKind::ColumnChange,
        );
    }

    #[test]
    fn test_snapshot_column_type_changed() {
        let stored = vec![
            serde_json::json!({"name": "id", "type_oid": 23}),
            serde_json::json!({"name": "val", "type_oid": 25}), // was text
        ];
        let mut current = std::collections::HashMap::new();
        current.insert("id".to_string(), 23i64);
        current.insert("val".to_string(), 1043i64); // now varchar

        assert_eq!(
            compare_snapshot_with_current(&stored, &current).unwrap(),
            SchemaChangeKind::ColumnChange,
        );
    }

    #[test]
    fn test_snapshot_column_added() {
        let stored = vec![serde_json::json!({"name": "id", "type_oid": 23})];
        let mut current = std::collections::HashMap::new();
        current.insert("id".to_string(), 23i64);
        current.insert("new_col".to_string(), 25i64);

        assert_eq!(
            compare_snapshot_with_current(&stored, &current).unwrap(),
            SchemaChangeKind::AddColumnOnly,
        );
    }

    #[test]
    fn test_snapshot_empty_stored_with_current_cols() {
        let stored: Vec<serde_json::Value> = vec![];
        let mut current = std::collections::HashMap::new();
        current.insert("id".to_string(), 23i64);

        // No stored entries → nothing to compare → no column dropped/changed
        // But current has cols that stored doesn't know about
        assert_eq!(
            compare_snapshot_with_current(&stored, &current).unwrap(),
            SchemaChangeKind::AddColumnOnly,
        );
    }

    // ── TEST-3: Table-driven DDL classification tests ─────────────────────────

    #[test]
    fn test_ddl_command_kind_from_event_table_driven() {
        // 14 cases covering every DdlCommandKind variant and key unknowns.
        let cases: &[(&str, &str, DdlCommandKind)] = &[
            ("table", "ALTER TABLE", DdlCommandKind::AlterTable),
            ("table", "CREATE TABLE", DdlCommandKind::CreateTable),
            ("view", "CREATE VIEW", DdlCommandKind::ViewChange),
            ("view", "CREATE OR REPLACE VIEW", DdlCommandKind::ViewChange),
            ("view", "ALTER VIEW", DdlCommandKind::ViewChange),
            ("trigger", "CREATE TRIGGER", DdlCommandKind::CreateTrigger),
            (
                "function",
                "CREATE FUNCTION",
                DdlCommandKind::FunctionChange,
            ),
            (
                "function",
                "CREATE OR REPLACE FUNCTION",
                DdlCommandKind::FunctionChange,
            ),
            ("function", "ALTER FUNCTION", DdlCommandKind::FunctionChange),
            ("type", "ALTER TYPE", DdlCommandKind::TypeChange),
            ("domain", "ALTER DOMAIN", DdlCommandKind::DomainChange),
            ("domain", "CREATE DOMAIN", DdlCommandKind::DomainChange),
            ("policy", "CREATE POLICY", DdlCommandKind::PolicyChange),
            (
                "extension",
                "CREATE EXTENSION",
                DdlCommandKind::ExtensionChange,
            ),
        ];

        for (object_type, command_tag, expected) in cases {
            assert_eq!(
                DdlCommandKind::from_event(object_type, command_tag),
                *expected,
                "failed for object_type={object_type:?}, command_tag={command_tag:?}",
            );
        }
    }

    #[test]
    fn test_ddl_command_kind_ignored_cases() {
        // Non-reactive DDL must map to Ignored.
        let ignored_cases: &[(&str, &str)] = &[
            ("index", "CREATE INDEX"),
            ("table", "DROP TABLE"),
            ("function", "DROP FUNCTION"),
            ("sequence", "CREATE SEQUENCE"),
            ("view", "DROP VIEW"),
        ];

        for (object_type, command_tag) in ignored_cases {
            assert_eq!(
                DdlCommandKind::from_event(object_type, command_tag),
                DdlCommandKind::Ignored,
                "expected Ignored for {object_type:?} / {command_tag:?}",
            );
        }
    }
}
