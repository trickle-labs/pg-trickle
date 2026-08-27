//! WAL-based Change Data Capture via logical replication.
//!
//! Provides an alternative CDC mechanism that uses PostgreSQL's built-in
//! logical decoding instead of row-level triggers. This eliminates the
//! synchronous write-side overhead (~2–15 μs per row) that triggers impose
//! on tracked source tables.
//!
//! # Architecture
//!
//! The WAL decoder uses a **polling** approach via SPI:
//! - Calls `pg_logical_slot_get_changes()` during the scheduler tick
//! - Decodes `pgoutput` protocol messages into typed buffer table rows
//! - Writes changes to the same `pgtrickle_changes.changes_<oid>` tables
//!   used by trigger-based CDC
//!
//! # Transition Lifecycle
//!
//! ```text
//! TRIGGER ──► TRANSITIONING ──► WAL
//!    ▲                           │
//!    └───────── (fallback) ──────┘
//! ```
//!
//! 1. **start**: Create publication + replication slot, set mode to TRANSITIONING
//! 2. **poll**: Both trigger and WAL decoder write to buffer (dedup at refresh)
//! 3. **complete**: Decoder caught up → drop trigger, set mode to WAL
//! 4. **fallback**: Timeout or error → drop slot/publication, revert to TRIGGER
//!
//! # Prerequisites
//!
//! - `wal_level = logical` in `postgresql.conf`
//! - Available replication slots (`max_replication_slots`)
//! - Source table has REPLICA IDENTITY DEFAULT (PK) or FULL
//! - `pg_trickle.cdc_mode` set to `'auto'` or `'wal'`

use pgrx::prelude::*;

use crate::catalog::{CdcMode, StDependency};
use crate::cdc;
use crate::config;
use crate::error::PgTrickleError;
use crate::monitor;

// ── Naming Conventions ─────────────────────────────────────────────────────

/// Replication slot name for a source table: `pgtrickle_<oid>`.
pub fn slot_name_for_source(source_oid: pg_sys::Oid) -> String {
    // CITUS-4: Use stable_name so slot names survive OID reassignment.
    let stable = crate::citus::stable_name_for_oid(source_oid)
        .unwrap_or_else(|_| source_oid.to_u32().to_string());
    format!("pgtrickle_{}", stable)
}

/// Publication name for a source table: `pgtrickle_cdc_<stable_name>`.
pub fn publication_name_for_source(source_oid: pg_sys::Oid) -> String {
    // CITUS-4: Use stable_name so publication names survive OID reassignment.
    let stable = crate::citus::stable_name_for_oid(source_oid)
        .unwrap_or_else(|_| source_oid.to_u32().to_string());
    format!("pgtrickle_cdc_{}", stable)
}

// ── Publication Management ─────────────────────────────────────────────────

/// Create a publication for a source table to enable logical decoding.
///
/// Publications tell `pgoutput` which tables to include in the change stream.
/// Each tracked source gets its own publication for independent lifecycle
/// management.
pub fn create_publication(source_oid: pg_sys::Oid) -> Result<(), PgTrickleError> {
    let pub_name = publication_name_for_source(source_oid);

    // Get the fully-qualified source table name
    let source_table =
        Spi::get_one_with_args::<String>("SELECT $1::oid::regclass::text", &[source_oid.into()])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            .ok_or_else(|| {
                PgTrickleError::NotFound(format!(
                    "Table with OID {} not found",
                    source_oid.to_u32()
                ))
            })?;

    // Create publication if it doesn't already exist.
    // PostgreSQL doesn't have CREATE PUBLICATION IF NOT EXISTS, so check first.
    let exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
        &[pub_name.as_str().into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or(false);

    if !exists {
        // PT3: For partitioned tables, use publish_via_partition_root = true
        // so child partition changes are published under the parent table's
        // identity, matching trigger-mode CDC behavior.
        let is_partitioned = Spi::get_one_with_args::<String>(
            "SELECT relkind::text FROM pg_class WHERE oid = $1",
            &[source_oid.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .map(|rk| rk == "p")
        .unwrap_or(false);

        let with_clause = if is_partitioned {
            " WITH (publish_via_partition_root = true)"
        } else {
            ""
        };

        let sql = format!(
            "CREATE PUBLICATION {} FOR TABLE {}{}",
            quote_ident(&pub_name),
            source_table,
            with_clause,
        );
        Spi::run(&sql) // nosemgrep: rust.spi.run.dynamic-format — publication name is quote_ident()-escaped.
            .map_err(|e| {
                PgTrickleError::WalTransitionError(format!(
                    "Failed to create publication {}: {}",
                    pub_name, e
                ))
            })?;
    }

    Ok(())
}

/// Drop a publication for a source table.
///
/// Safe to call even if the publication doesn't exist (uses IF EXISTS).
pub fn drop_publication(source_oid: pg_sys::Oid) -> Result<(), PgTrickleError> {
    let pub_name = publication_name_for_source(source_oid);
    let sql = format!("DROP PUBLICATION IF EXISTS {}", quote_ident(&pub_name));
    Spi::run(&sql) // nosemgrep: rust.spi.run.dynamic-format — publication name is quote_ident()-escaped.
        .map_err(|e| {
            PgTrickleError::WalTransitionError(format!(
                "Failed to drop publication {}: {}",
                pub_name, e
            ))
        })?;
    Ok(())
}

/// Check if a publication needs to be rebuilt because its source table was
/// converted to partitioned after publication creation (SF-11).
///
/// When a regular table is converted to a partitioned table (via
/// `CREATE TABLE ... PARTITION OF` or dump/restore), the existing
/// publication lacks `publish_via_partition_root = true`.  WAL events from
/// child partitions arrive with child-partition names instead of the parent
/// table name, causing the WAL decoder's table-name filter to silently skip
/// all changes — the stream table freezes with no error.
///
/// This function detects that condition and rebuilds the publication with
/// the correct setting.
pub fn check_publication_health(source_oid: pg_sys::Oid) -> Result<(), PgTrickleError> {
    let pub_name = publication_name_for_source(source_oid);

    // Check if publication exists at all
    let pub_exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
        &[pub_name.as_str().into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or(false);

    if !pub_exists {
        return Ok(());
    }

    // Check current relkind — is the table now partitioned?
    let is_partitioned = Spi::get_one_with_args::<String>(
        "SELECT relkind::text FROM pg_class WHERE oid = $1",
        &[source_oid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .map(|rk| rk == "p")
    .unwrap_or(false);

    if !is_partitioned {
        return Ok(());
    }

    // Table is partitioned — check if publication already has PVPR
    let has_pvpr = Spi::get_one_with_args::<bool>(
        "SELECT pubviaroot FROM pg_publication WHERE pubname = $1",
        &[pub_name.as_str().into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or(false);

    if has_pvpr {
        return Ok(());
    }

    // Publication exists, table is partitioned, but PVPR is not set.
    // Rebuild the publication with the correct setting.
    info!(
        "pg_trickle: source OID {} is now partitioned but publication '{}' \
         lacks publish_via_partition_root — rebuilding publication",
        source_oid.to_u32(),
        pub_name
    );

    // Drop and recreate with PVPR
    drop_publication(source_oid)?;
    create_publication(source_oid)?;

    Ok(())
}

// ── Replication Slot Management ────────────────────────────────────────────

/// Create a logical replication slot for WAL decoding.
///
/// Uses the `pgoutput` output plugin (built into PostgreSQL) which provides
/// structured change data including column names and values.
///
/// The slot captures WAL from the moment of creation, ensuring no changes
/// are missed between slot creation and the first poll.
///
/// # Implementation Note
///
/// This uses the low-level C replication API (`ReplicationSlotCreate`,
/// `CreateInitDecodingContext`, etc.) instead of the SQL function
/// `pg_create_logical_replication_slot()`.
///
/// Both the SQL wrapper and `CreateInitDecodingContext` reject calls from
/// transactions that have an assigned XID (transaction ID).  With
/// `wal_level = logical`, even read-only SPI queries can trigger hint-bit
/// WAL writes that assign an XID.
///
/// **CRITICAL**: This function must be called in a transaction that has not
/// done ANY prior SPI queries or catalog reads.  The prerequisite checks
/// (wal_level, permissions, replica identity) must be done in a *separate,
/// earlier* transaction.  `CheckSlotPermissions` and
/// `CheckLogicalDecodingRequirements` are intentionally skipped here because
/// they access the catalog (which could assign an XID); instead the caller
/// must verify prerequisites before calling this function.
pub fn create_replication_slot_pristine(slot_name: &str) -> Result<String, PgTrickleError> {
    // COR-004 (v0.72.0): Guard against transactions that have already been
    // assigned a transaction ID.  `CreateInitDecodingContext` (called inside
    // `create_replication_slot_internal`) rejects XID-assigned transactions,
    // so we detect the condition early and return an actionable error instead
    // of letting PostgreSQL abort with an opaque internal message.
    //
    // SAFETY: `GetCurrentTransactionIdIfAny()` reads `MyProc->xid` without
    // any side effects.  It is safe to call at any point from a PostgreSQL
    // background worker or executor context.
    unsafe {
        let xid = pg_sys::GetCurrentTransactionIdIfAny();
        if xid != pg_sys::InvalidTransactionId {
            return Err(PgTrickleError::ReplicationSlotError(format!(
                "cannot create logical replication slot '{}': current transaction has \
                 an assigned XID ({}). create_replication_slot_pristine() must be called \
                 in a fresh transaction with no prior SPI queries or catalog reads. \
                 Separate prerequisite checks from slot creation.",
                slot_name, xid
            )));
        }
    }
    create_replication_slot_internal(slot_name)
}

/// Check if a replication slot already exists and return its confirmed_flush_lsn.
///
/// Returns `Some(lsn)` if the slot exists, `None` if it doesn't.
/// This function does SPI reads and must NOT be called in the same
/// transaction as `create_replication_slot_pristine`.
pub fn get_existing_slot_lsn(slot_name: &str) -> Result<Option<String>, PgTrickleError> {
    // Filter by database = current_database() so that identically-named slots
    // belonging to other databases (e.g. parallel test databases in the same
    // PostgreSQL instance) are not mistakenly reused.  pg_logical_slot_get_changes
    // errors if called for a slot that belongs to a different database.
    let exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pg_replication_slots \
         WHERE slot_name = $1 AND database = current_database())",
        &[slot_name.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or(false);

    if !exists {
        return Ok(None);
    }

    let lsn = Spi::get_one_with_args::<String>(
        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots \
         WHERE slot_name = $1 AND database = current_database()",
        &[slot_name.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or_else(|| "0/0".to_string());

    Ok(Some(lsn))
}

/// Create a logical replication slot via the PostgreSQL C API.
///
/// Replicates the logic of `pg_create_logical_replication_slot()` from
/// `replicationfuncs.c` but skips the `XactHasPerformedWrites()` guard
/// AND the catalog-touching permission/requirement checks.  Those checks
/// must be done by the caller in a prior transaction.
///
/// **CRITICAL**: Must run in a pristine transaction with NO prior SPI
/// calls or catalog access, otherwise `CreateInitDecodingContext` will
/// fail because the transaction has an assigned XID.
fn create_replication_slot_internal(slot_name: &str) -> Result<String, PgTrickleError> {
    use std::ffi::CString;

    let c_slot_name = CString::new(slot_name)
        .map_err(|e| PgTrickleError::ReplicationSlotError(format!("Invalid slot name: {}", e)))?;
    // STAB-5: Use compile-time CStr literal — no NUL bytes possible, no runtime allocation.
    let c_plugin = c"test_decoding";

    // SAFETY: Calling PostgreSQL C API functions for replication slot management.
    // These are the same functions called by pg_create_logical_replication_slot(),
    // minus the XactHasPerformedWrites guard and minus CheckSlotPermissions /
    // CheckLogicalDecodingRequirements (which do catalog reads that would assign
    // an XID).  The caller guarantees prerequisites were checked in a prior
    // transaction.
    //
    // Sequence: ReplicationSlotCreate (ephemeral) → CreateInitDecodingContext →
    // DecodingContextFindStartpoint → persist → release.
    unsafe {
        // Create as ephemeral first — if anything fails, PG cleans up automatically.
        pg_sys::ReplicationSlotCreate(
            c_slot_name.as_ptr(),
            true, // db_specific
            pg_sys::ReplicationSlotPersistency::RS_EPHEMERAL,
            false, // two_phase
            false, // failover
            false, // synced
        );

        // Set up the XLogReaderRoutine with the standard local WAL readers.
        // We use thin wrappers because Rust edition 2024 does not implicitly
        // coerce function items across ABI boundaries.
        unsafe extern "C-unwind" fn page_read_wrapper(
            state: *mut pg_sys::XLogReaderState,
            target: pg_sys::XLogRecPtr,
            req_len: std::ffi::c_int,
            target_rec: pg_sys::XLogRecPtr,
            cur_page: *mut std::ffi::c_char,
        ) -> std::ffi::c_int {
            // SAFETY: Delegating to the PG-provided read_local_xlog_page with
            // the same arguments the caller passed.
            unsafe { pg_sys::read_local_xlog_page(state, target, req_len, target_rec, cur_page) }
        }
        unsafe extern "C-unwind" fn segment_open_wrapper(
            state: *mut pg_sys::XLogReaderState,
            next_seg_no: pg_sys::XLogSegNo,
            tli_p: *mut pg_sys::TimeLineID,
        ) {
            // SAFETY: Delegating to the PG-provided wal_segment_open.
            unsafe { pg_sys::wal_segment_open(state, next_seg_no, tli_p) }
        }
        unsafe extern "C-unwind" fn segment_close_wrapper(state: *mut pg_sys::XLogReaderState) {
            // SAFETY: Delegating to the PG-provided wal_segment_close.
            unsafe { pg_sys::wal_segment_close(state) }
        }
        let mut xl_routine = pg_sys::XLogReaderRoutine {
            page_read: Some(page_read_wrapper),
            segment_open: Some(segment_open_wrapper),
            segment_close: Some(segment_close_wrapper),
        };

        // Create the initial decoding context — this finds the starting LSN
        let ctx = pg_sys::CreateInitDecodingContext(
            c_plugin.as_ptr(),
            std::ptr::null_mut(), // output_plugin_options (NIL)
            false,                // need_full_snapshot
            pg_sys::InvalidXLogRecPtr as u64,
            &mut xl_routine,
            None, // prepare_write
            None, // do_write
            None, // update_progress
        );

        // Build the initial snapshot and find the start point
        pg_sys::DecodingContextFindStartpoint(ctx);

        // Read the confirmed_flush LSN before releasing
        let confirmed_flush = (*pg_sys::MyReplicationSlot).data.confirmed_flush;

        // Clean up the decoding context
        pg_sys::FreeDecodingContext(ctx);

        // Persist the slot (it was created as ephemeral)
        pg_sys::ReplicationSlotMarkDirty();
        pg_sys::ReplicationSlotSave();
        pg_sys::ReplicationSlotPersist();

        // Release the slot
        pg_sys::ReplicationSlotRelease();

        // Format LSN as "X/Y"
        let lsn_str = format!(
            "{:X}/{:X}",
            (confirmed_flush >> 32) as u32,
            confirmed_flush as u32
        );

        Ok(lsn_str)
    }
}

/// Drop a logical replication slot.
///
/// Safe to call even if the slot doesn't exist (checks first).
pub fn drop_replication_slot(slot_name: &str) -> Result<(), PgTrickleError> {
    // Filter by database = current_database() to avoid accidentally dropping a
    // slot that belongs to a different database but shares the same name.
    let exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pg_replication_slots \
         WHERE slot_name = $1 AND database = current_database())",
        &[slot_name.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or(false);

    if exists {
        Spi::run_with_args("SELECT pg_drop_replication_slot($1)", &[slot_name.into()]).map_err(
            |e| {
                PgTrickleError::ReplicationSlotError(format!(
                    "Failed to drop replication slot '{}': {}",
                    slot_name, e
                ))
            },
        )?;
    }

    Ok(())
}

/// Get the confirmed flush LSN for a replication slot.
///
/// Returns the LSN up to which the slot consumer has confirmed processing.
/// Returns `None` if the slot doesn't exist.
pub fn get_slot_confirmed_lsn(slot_name: &str) -> Result<Option<String>, PgTrickleError> {
    Spi::get_one_with_args::<String>(
        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots \
         WHERE slot_name = $1 AND database = current_database()",
        &[slot_name.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))
}

/// Get the lag in bytes between a slot's confirmed LSN and the current WAL position.
///
/// A high lag indicates the decoder is falling behind.
pub fn get_slot_lag_bytes(slot_name: &str) -> Result<i64, PgTrickleError> {
    Spi::get_one_with_args::<i64>(
        "SELECT (pg_current_wal_lsn() - confirmed_flush_lsn)::bigint \
         FROM pg_replication_slots \
         WHERE slot_name = $1 AND database = current_database()",
        &[slot_name.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))
    .map(|v| v.unwrap_or(0))
}

/// A12 (v0.36.0): Check whether WAL slot lag backpressure should pause CDC writes.
///
/// When `pg_trickle.enforce_backpressure = true` and the slot lag for `slot_name`
/// exceeds `slot_lag_critical_threshold_mb`, returns `true` to indicate that CDC
/// trigger writes should be suppressed (no-op).
///
/// Backpressure is released (returns `false`) when the lag drops below 50% of
/// the critical threshold.
///
/// When `enforce_backpressure = false` (default), always returns `false`.
pub fn is_backpressure_active(slot_name: &str) -> bool {
    if !crate::config::pg_trickle_enforce_backpressure() {
        return false;
    }
    let critical_bytes = crate::config::pg_trickle_slot_lag_critical_threshold_bytes();
    let lag_bytes = match get_slot_lag_bytes(slot_name) {
        Ok(b) => b,
        Err(_) => return false, // On error, don't suppress writes
    };
    // Backpressure active when: lag >= critical threshold
    // Backpressure released when: lag < 50% of critical threshold
    // Hysteresis prevents rapid on/off oscillation
    if lag_bytes >= critical_bytes {
        pgrx::warning!(
            "pg_trickle: WAL slot '{}' lag {}MB >= critical threshold {}MB, \
             enabling backpressure (CDC writes suppressed)",
            slot_name,
            lag_bytes / (1024 * 1024),
            critical_bytes / (1024 * 1024),
        );
        return true;
    }
    // Below the lower hysteresis threshold: backpressure off
    false
}

/// A44-3 (v0.43.0): Maximum number of changes to process per poll cycle.
///
/// Previously hardcoded at 10 000. Now controlled by
/// `pg_trickle.wal_max_changes_per_poll` GUC. Kept as fallback for tests.
#[cfg(test)]
const MAX_CHANGES_PER_POLL: i64 = 10_000;

/// Number of consecutive WAL poll errors before automatically falling back
/// to trigger-based CDC. Prevents a permanently broken WAL decoder from
/// blocking change capture indefinitely.
///
/// Set to 20 (≈ 20 seconds at a 1 s scheduler interval) so that transient
/// slot contention under CI load does not trigger an irreversible fallback.
/// The fallback should only fire for a genuinely broken slot that fails
/// reliably across many ticks — not for the brief spike that can occur
/// right after slot creation or on a loaded test machine.
const MAX_CONSECUTIVE_WAL_ERRORS: u32 = 20;

/// Poll WAL changes from a replication slot and write them to the buffer table.
///
/// Uses `pg_logical_slot_get_changes()` with the `test_decoding` plugin to
/// retrieve decoded WAL changes. Each change is parsed and inserted into
/// the appropriate `pgtrickle_changes.changes_<oid>` buffer table.
///
/// The `test_decoding` output format provides structured text output that
/// we parse to extract action type, column values, and LSN information.
/// Since `test_decoding` decodes ALL tables (not just the source), we
/// filter by matching the qualified table name in each row.
///
/// **Schema-change detection**: When the decoded column set doesn't match
/// our expected columns, this function returns `Err(WalTransitionError)`
/// so the caller can abort the WAL transition and fall back to triggers.
///
/// Returns the number of changes processed and the last confirmed LSN.
pub fn poll_wal_changes(
    source_oid: pg_sys::Oid,
    slot_name: &str,
    source_table_name: &str,
    change_schema: &str,
    pk_columns: &[String],
    columns: &[(String, String)],
) -> Result<(i64, Option<String>), PgTrickleError> {
    let oid_u32 = source_oid.to_u32();

    // Poll changes from the logical replication slot.
    // pg_logical_slot_get_changes() advances the slot position
    // automatically.  We use test_decoding which produces text output
    // in the format: "table schema.table: ACTION: col[type]:val ..."
    // A44-3: Read max changes per poll from GUC (default 10 000, previously hardcoded).
    let max_changes_per_poll = crate::config::pg_trickle_wal_max_changes_per_poll();
    let poll_sql = format!(
        "SELECT lsn::text, xid, data \
         FROM pg_logical_slot_get_changes(\
             '{slot_name}', NULL, {max_changes}\
         )",
        slot_name = slot_name,
        max_changes = max_changes_per_poll,
    );

    let mut count: i64 = 0;
    let mut last_lsn: Option<String> = None;

    cdc::set_sync_commit_for_buffer(&cdc::buffer_base_name_for_oid(source_oid))?;

    // COR-5: Resolve canonical qualified names for WAL filter matching once per
    // poll cycle. This handles case-sensitive quoted identifiers, search-path-
    // sensitive names, and partition routing (child table arrives instead of root).
    let filter_names = resolve_wal_filter_names(source_oid, source_table_name)?;

    Spi::connect(|client| {
        let result = client
            .select(&poll_sql, None, &[])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        for row in result {
            let lsn = row
                .get::<String>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or_default();
            let data = row
                .get::<String>(3)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or_default();

            // COR-5: OID-based filter via pre-resolved canonical names set.
            // test_decoding decodes ALL tables; skip rows not belonging to our source
            // (including partition children not tracked by this source OID).
            match extract_table_name_from_test_decoding(&data) {
                Some(name) if filter_names.contains(name) => {} // our table — process
                _ => {
                    // Not our table — skip but still track LSN
                    last_lsn = Some(lsn);
                    continue;
                }
            }

            if let Some(action) = parse_pgoutput_action(&data) {
                // Schema-change detection: when pgoutput emits a DML message
                // whose column set doesn't match our expected columns, a DDL
                // change likely occurred. Return an error so the caller can
                // fall back to triggers.
                if action != 'T' {
                    let parsed = parse_pgoutput_columns(&data);
                    if detect_schema_mismatch(&parsed, columns) {
                        return Err(PgTrickleError::WalTransitionError(format!(
                            "Schema change detected for source OID {} — \
                             decoded columns don't match expected columns",
                            oid_u32
                        )));
                    }
                }

                // Write the decoded change to the buffer table
                write_decoded_change(
                    oid_u32,
                    &lsn,
                    &action,
                    &data,
                    change_schema,
                    pk_columns,
                    columns,
                )?;
                count += 1;
            }

            last_lsn = Some(lsn);
        }

        Ok::<(), PgTrickleError>(())
    })?;

    Ok((count, last_lsn))
}

/// COR-5: Extract the qualified table name from a `test_decoding` output line.
///
/// Handles lines of the form:
/// `table schema.table: ACTION: col[type]:val ...`
///
/// Returns the slice `schema.table` (the substring between `"table "` and
/// the first `": "` separator).  Returns `None` for non-DML lines (`BEGIN`,
/// `COMMIT`, etc.) that do not start with `"table "`.
///
/// This is a pure function — it can be unit-tested without a PostgreSQL backend.
pub(crate) fn extract_table_name_from_test_decoding(data: &str) -> Option<&str> {
    let rest = data.strip_prefix("table ")?;
    let colon_pos = rest.find(": ")?;
    Some(&rest[..colon_pos])
}

#[doc(hidden)]
pub fn extract_test_decoding_table_for_fuzz(data: &str) -> Option<&str> {
    extract_table_name_from_test_decoding(data)
}

/// COR-5: Resolve the set of canonical qualified table names to match against
/// WAL filter output during a poll cycle.
///
/// Queries `pg_class` and `pg_inherits` ONCE to obtain:
/// 1. The canonical `schema.table` for `source_oid` itself.
/// 2. Canonical names for all immediate partition children, so that changes
///    routed to a child partition are also accepted.
///
/// The `fallback_name` (caller-supplied qualified name) is always inserted so
/// the common case is handled even if the catalog query finds nothing.
fn resolve_wal_filter_names(
    source_oid: pg_sys::Oid,
    fallback_name: &str,
) -> Result<std::collections::HashSet<String>, PgTrickleError> {
    let oid_val = source_oid.to_u32() as i64;
    let mut names = std::collections::HashSet::new();
    names.insert(fallback_name.to_string());

    let extra = Spi::connect(|client| {
        let sql = "\
            SELECT n.nspname::text || '.' || c.relname::text \
            FROM pg_catalog.pg_class c \
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
            WHERE c.oid = $1 \
            UNION ALL \
            SELECT n.nspname::text || '.' || c.relname::text \
            FROM pg_catalog.pg_class c \
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
            JOIN pg_catalog.pg_inherits i ON i.inhrelid = c.oid \
            WHERE i.inhparent = $1";
        let rows = client
            .select(sql, None, &[oid_val.into()])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        let mut resolved: Vec<String> = Vec::new();
        for row in rows {
            if let Ok(Some(name)) = row.get::<String>(1) {
                resolved.push(name);
            }
        }
        Ok::<Vec<String>, PgTrickleError>(resolved)
    })?;
    names.extend(extra);
    Ok(names)
}

/// Parse the action type from a pgoutput data string.
///
/// The `pgoutput` plugin with `proto_version = 1` outputs text lines like:
/// - `table public.users: INSERT: id[integer]:1 name[text]:'Alice'`
/// - `table public.users: UPDATE: ...`
/// - `table public.users: DELETE: ...`
/// - `table public.users: TRUNCATE: (no column data)`
///
/// Returns the action character ('I', 'U', 'D', 'T') or None if not a DML line.
///
/// Parses the action **positionally** rather than with `contains()` to avoid
/// false matches when a schema/table name or column value happens to contain
/// an action keyword (G2.3).
fn parse_pgoutput_action(data: &str) -> Option<char> {
    // Strip the fixed "table " prefix that prefixes all DML lines.
    let rest = data.strip_prefix("table ")?;
    // Skip over "schema.tablename" to the first ": " separator.
    let after_table_colon = rest.split_once(": ")?.1;
    // The action keyword is the next token up to the next ':'.
    let action = after_table_colon.split_once(':')?.0.trim();
    match action {
        "INSERT" => Some('I'),
        "UPDATE" => Some('U'),
        "DELETE" => Some('D'),
        "TRUNCATE" => Some('T'),
        _ => None,
    }
}

#[doc(hidden)]
pub fn parse_test_decoding_action_for_fuzz(data: &str) -> Option<char> {
    parse_pgoutput_action(data)
}

/// Parse column values from a pgoutput data line.
///
/// Extracts `column_name[type]:value` pairs from the pgoutput text format.
/// Returns a map from column name to string value.
fn parse_pgoutput_columns(data: &str) -> std::collections::HashMap<String, String> {
    let mut cols = std::collections::HashMap::new();
    let payload = if let Some(pos) = data.find("INSERT:") {
        data.get(pos + "INSERT:".len()..).unwrap_or("")
    } else if let Some(pos) = data.find("UPDATE:") {
        data.get(pos + "UPDATE:".len()..).unwrap_or("")
    } else if let Some(pos) = data.find("DELETE:") {
        data.get(pos + "DELETE:".len()..).unwrap_or("")
    } else {
        return cols;
    };
    for segment in payload.split_whitespace() {
        if let Some(bracket_pos) = segment.find('[') {
            let col_name = &segment[..bracket_pos];
            if let Some(colon_pos) = segment.find("]:") {
                let value = segment[colon_pos + 2..].trim_matches('\'');
                cols.insert(col_name.to_string(), value.to_string());
            }
        }
    }
    cols
}

#[doc(hidden)]
pub fn parse_test_decoding_columns_for_fuzz(
    data: &str,
) -> std::collections::HashMap<String, String> {
    parse_pgoutput_columns(data)
}

/// Parse old-tuple column values from a pgoutput UPDATE data line.
///
/// With `REPLICA IDENTITY FULL`, pgoutput UPDATE messages include an
/// "old-key:" section before the "new-tuple:" section:
/// ```text
/// table public.t: UPDATE: old-key: id[integer]:1 name[text]:'Alice' new-tuple: id[integer]:1 name[text]:'Bob'
/// ```
///
/// This function extracts the "old-key:" portion and parses column values.
/// Returns an empty map for non-UPDATE messages or messages without an
/// "old-key:" section (i.e., REPLICA IDENTITY DEFAULT where only PK
/// columns appear in old-key).
fn parse_pgoutput_old_columns(data: &str) -> std::collections::HashMap<String, String> {
    let mut cols = std::collections::HashMap::new();
    let old_key_start = match data.find("old-key:") {
        Some(pos) => pos + "old-key:".len(),
        None => return cols,
    };
    let old_key_end = data[old_key_start..]
        .find("new-tuple:")
        .map(|pos| old_key_start + pos)
        .unwrap_or(data.len());
    for segment in data[old_key_start..old_key_end].split_whitespace() {
        if let Some(bracket_pos) = segment.find('[') {
            let col_name = &segment[..bracket_pos];
            if let Some(colon_pos) = segment.find("]:") {
                let value = segment[colon_pos + 2..].trim_matches('\'');
                cols.insert(col_name.to_string(), value.to_string());
            }
        }
    }
    cols
}

#[doc(hidden)]
pub fn parse_test_decoding_old_columns_for_fuzz(
    data: &str,
) -> std::collections::HashMap<String, String> {
    parse_pgoutput_old_columns(data)
}

/// Write a decoded WAL change to the buffer table.
///
/// Maps the parsed pgoutput data into the typed buffer table columns,
/// matching the same schema used by trigger-based CDC.
fn write_decoded_change(
    source_oid: u32,
    lsn: &str,
    action: &char,
    data: &str,
    change_schema: &str,
    pk_columns: &[String],
    columns: &[(String, String)],
) -> Result<(), PgTrickleError> {
    // Handle TRUNCATE specially — mark downstream STs for reinit
    if *action == 'T' {
        mark_downstream_for_reinit(pg_sys::Oid::from(source_oid))?;
        return Ok(());
    }

    let parsed = parse_pgoutput_columns(data);

    // G2.2: Parse old-tuple values for UPDATE events.
    // With REPLICA IDENTITY FULL, pgoutput includes the old tuple in the
    // "old-key:" section before the new tuple. Parse both sections.
    let old_parsed = if *action == 'U' {
        parse_pgoutput_old_columns(data)
    } else {
        std::collections::HashMap::new()
    };

    let has_pk = !pk_columns.is_empty();

    // A42-13: Build a fully parameterized INSERT.
    // All data values are passed as SPI text parameters ($1, $2, ...)
    // instead of being escaped inline. This eliminates the risk of SQL
    // injection via WAL-decoded column values containing quotes, backslashes,
    // or other special characters.
    //
    // Column-name identifiers and schema/table identifiers are static
    // (derived from internal catalog / OID lookup) and are not user-controlled
    // at the point they reach this function.

    // param_values: the ordered list of bound values (None = SQL NULL).
    let mut param_values: Vec<Option<String>> = Vec::new();
    // col_names:  the quoted column identifiers for the INSERT list.
    let mut col_names: Vec<String> = Vec::new();
    // placeholder:  the corresponding $N expressions (or sub-expressions for pk_hash).
    let mut placeholders: Vec<String> = Vec::new();

    // $1: lsn (text cast to pg_lsn)
    param_values.push(Some(lsn.to_string()));
    col_names.push("lsn".to_string());
    let lsn_idx = param_values.len();
    placeholders.push(format!("${}::pg_lsn", lsn_idx));

    // $N: action (single character stored as text)
    param_values.push(Some(action.to_string()));
    col_names.push("action".to_string());
    let action_idx = param_values.len();
    placeholders.push(format!("${}", action_idx));

    // pk_hash column (uses subsequent $N params for PK column values)
    if has_pk {
        col_names.push("pk_hash".to_string());
        let pk_hash_expr = build_pk_hash_parameterized(pk_columns, &parsed, &mut param_values);
        placeholders.push(pk_hash_expr);
    }

    // Map parsed columns to flat buffer columns (A44-10 D+I schema).
    // A42-14: Add explicit `::col_type` casts to each placeholder so that
    // the text-typed SPI parameter datums are cast to the actual column type
    // at query-plan time. Without explicit casts, PostgreSQL rejects text→integer
    // (and other non-implicit-coercible) assignments in parameterized queries.

    // A44-10: UPDATE is decomposed into D-row (OLD values) + I-row (NEW values)
    // emitted as a single multi-row VALUES INSERT for atomicity (single SPI call,
    // single heap operation). This avoids the risk of a crash between D and I rows.
    if *action == 'U' {
        // A44-10 atomicity: single multi-row INSERT for D+I pair.
        // D-row: OLD values with action='D'. I-row: NEW values with action='I'.
        // D-row must appear before I-row (change_id ordering invariant).
        //
        // Build column list and value lists as Vecs to avoid comma-joining bugs.
        // $1 is shared for lsn; D-row data starts at $2; I-row data is offset
        // by the number of D-row params.

        let buf_name = cdc::buffer_base_name_for_oid(pg_sys::Oid::from(source_oid));
        assert_valid_identifier(&buf_name, "change buffer name")?;
        assert_valid_identifier(change_schema, "change schema")?;

        let d_pk_hash_expr = build_pk_hash_from_values(pk_columns, &old_parsed);
        let i_pk_hash_expr = build_pk_hash_from_values(pk_columns, &parsed);

        let num_columns = columns.len();
        let pk_extra = if has_pk { 1 } else { 0 };

        // PERF-5: Pre-allocate all column/value Vecs with known capacity to
        // eliminate incremental heap reallocation per column.

        // col_names: INSERT column list (shared for both rows).
        let mut col_names_u: Vec<String> = Vec::with_capacity(2 + pk_extra + num_columns);
        col_names_u.push("lsn".to_string());
        col_names_u.push("action".to_string());
        if has_pk {
            col_names_u.push("pk_hash".to_string());
        }
        for (col_name, _) in columns {
            let cb_name = crate::cdc::cb_col_name(col_name);
            col_names_u.push(format!("\"{}\"", cb_name.replace('"', "\"\"")));
        }

        // D-row params: $1=lsn, $2..=old column values.
        let mut d_all_params: Vec<Option<String>> = Vec::with_capacity(1 + num_columns);
        d_all_params.push(Some(lsn.to_string()));
        // D-row value expressions.
        let mut d_vals: Vec<String> = Vec::with_capacity(2 + pk_extra + num_columns);
        d_vals.push("$1::pg_lsn".to_string());
        d_vals.push("'D'".to_string());
        if has_pk {
            d_vals.push(d_pk_hash_expr);
        }
        for (col_name, col_type) in columns {
            d_all_params.push(old_parsed.get(col_name).cloned());
            d_vals.push(format!("${}::{}", d_all_params.len(), col_type));
        }

        // I-row params: offset by d_all_params.len() (they follow D params in the SPI args).
        let d_len = d_all_params.len();
        let mut i_all_params: Vec<Option<String>> = Vec::with_capacity(num_columns);
        // I-row value expressions.
        let mut i_vals: Vec<String> = Vec::with_capacity(2 + pk_extra + num_columns);
        i_vals.push("$1::pg_lsn".to_string());
        i_vals.push("'I'".to_string());
        if has_pk {
            i_vals.push(i_pk_hash_expr);
        }
        for (col_name, col_type) in columns {
            i_all_params.push(parsed.get(col_name).cloned());
            i_vals.push(format!("${}::{}", d_len + i_all_params.len(), col_type));
        }

        let sql = format!(
            // nosemgrep: rust.spi.query.dynamic-format
            "INSERT INTO {schema}.{buf_name} ({cols}) VALUES ({d_vals}), ({i_vals})",
            schema = change_schema,
            buf_name = buf_name,
            cols = col_names_u.join(", "),
            d_vals = d_vals.join(", "),
            i_vals = i_vals.join(", "),
        );

        let mut all_params = d_all_params;
        all_params.extend(i_all_params);
        let spi_args: Vec<pgrx::datum::DatumWithOid<'_>> =
            all_params.into_iter().map(|v| v.into()).collect();

        pgrx::Spi::run_with_args(&sql, &spi_args).map_err(|e| {
            PgTrickleError::WalTransitionError(format!(
                "Failed to write D+I pair for WAL UPDATE to buffer: {}",
                e
            ))
        })?;
        return Ok(());
    }

    for (col_name, col_type) in columns {
        let cb_name = crate::cdc::cb_col_name(col_name);
        let safe_cb_name = cb_name.replace('"', "\"\"");

        match action {
            'I' => {
                // A44-10: flat column "col" (was "new_col").
                // Use cb_col_name() so reserved names are stored as "__usr_{name}".
                col_names.push(format!("\"{}\"", safe_cb_name));
                let val = parsed.get(col_name).cloned();
                param_values.push(val);
                placeholders.push(format!("${}::{}", param_values.len(), col_type));
            }
            'D' => {
                // A44-10: flat column "col" (was "old_col").
                col_names.push(format!("\"{}\"", safe_cb_name));
                let val = parsed.get(col_name).cloned();
                param_values.push(val);
                placeholders.push(format!("${}::{}", param_values.len(), col_type));
            }
            _ => {}
        }
    }

    let buf_name = cdc::buffer_base_name_for_oid(pg_sys::Oid::from(source_oid));
    // A42-13: Validate the slot-name / buffer-name grammar before use.
    // Buffer names are generated from OIDs and follow the pattern
    // `changes_<oid>` — they must contain only alphanumerics and underscores.
    assert_valid_identifier(&buf_name, "change buffer name")?;
    assert_valid_identifier(change_schema, "change schema")?;

    let sql = format!(
        // nosemgrep: rust.spi.query.dynamic-format
        "INSERT INTO {schema}.{buf_name} ({cols}) VALUES ({vals})",
        schema = change_schema,
        buf_name = buf_name,
        cols = col_names.join(", "),
        vals = placeholders.join(", "),
    );

    // A42-13: Convert param_values to DatumWithOid args.
    // Option<String> implements IntoDatum (None → SQL NULL) so we can use
    // the standard .into() conversion for each element.
    let spi_args: Vec<pgrx::datum::DatumWithOid<'_>> =
        param_values.into_iter().map(|v| v.into()).collect();

    // A42-13: Use run_with_args so every value is passed as a type-safe
    // text parameter — no inline string escaping.
    pgrx::Spi::run_with_args(&sql, &spi_args).map_err(|e| {
        PgTrickleError::WalTransitionError(format!(
            "Failed to write decoded WAL change to buffer: {}",
            e
        ))
    })?;

    Ok(())
}

/// A42-13: Assert that an identifier contains only characters that are safe to
/// embed as unquoted names in SQL (alphanumerics and underscores).
///
/// Change-buffer names and the change schema are generated internally from OIDs
/// and configuration, so they should never contain special characters. This
/// assertion converts a programming error into an immediate, actionable error
/// instead of a silent SQL injection.
fn assert_valid_identifier(s: &str, context: &str) -> Result<(), PgTrickleError> {
    if s.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '"')
    {
        Ok(())
    } else {
        Err(PgTrickleError::InternalError(format!(
            "WAL decoder: {} '{}' contains unexpected characters (expected [A-Za-z0-9_\"])",
            context, s
        )))
    }
}

/// A42-13: Build a parameterized pk_hash sub-expression.
///
/// Instead of embedding the PK values as SQL string literals (old approach),
/// we add each PK value to `param_values` and reference them via `$N`
/// placeholders in the generated expression. NULL pk-column values produce
/// a 0 hash (matching the trigger behaviour).
fn build_pk_hash_parameterized(
    pk_columns: &[String],
    parsed: &std::collections::HashMap<String, String>,
    param_values: &mut Vec<Option<String>>,
) -> String {
    if pk_columns.is_empty() {
        return "0".to_string();
    }

    if pk_columns.len() == 1 {
        let val = parsed.get(&pk_columns[0]).cloned();
        param_values.push(val.clone());
        let idx = param_values.len();
        if val.is_some() {
            format!("pgtrickle.pg_trickle_hash(${})", idx)
        } else {
            // NULL pk → 0 hash (matches trigger)
            "0".to_string()
        }
    } else {
        let mut array_items: Vec<String> = Vec::new();
        for col in pk_columns {
            let val = parsed.get(col).cloned();
            param_values.push(val.clone());
            let idx = param_values.len();
            if val.is_some() {
                array_items.push(format!("${}", idx));
            } else {
                array_items.push("NULL".to_string());
            }
        }
        crate::hash::build_composite_hash_expr(&array_items)
    }
}

#[doc(hidden)]
pub fn build_test_decoding_parameter_plan_for_fuzz(
    pk_columns: &[String],
    parsed: &std::collections::HashMap<String, String>,
) -> (String, Vec<Option<String>>) {
    let mut params = Vec::new();
    let expression = build_pk_hash_parameterized(pk_columns, parsed, &mut params);
    (expression, params)
}

/// Build a pk_hash expression from parsed column values (legacy — kept for
/// any callers outside the parameterized path).
///
/// Uses the same hash computation as the trigger-based CDC to ensure
/// pk_hash values match between trigger and WAL decoder outputs.
fn build_pk_hash_from_values(
    pk_columns: &[String],
    parsed: &std::collections::HashMap<String, String>,
) -> String {
    if pk_columns.is_empty() {
        return "0".to_string();
    }

    if pk_columns.len() == 1 {
        if let Some(val) = parsed.get(&pk_columns[0]) {
            format!("pgtrickle.pg_trickle_hash('{}')", val.replace('\'', "''"))
        } else {
            "0".to_string()
        }
    } else {
        let array_items: Vec<String> = pk_columns
            .iter()
            .map(|col| {
                if let Some(val) = parsed.get(col) {
                    format!("'{}'", val.replace('\'', "''"))
                } else {
                    "NULL".to_string()
                }
            })
            .collect();
        crate::hash::build_composite_hash_expr(&array_items)
    }
}

/// Mark all downstream stream tables for reinitialization.
///
/// Called when a TRUNCATE is detected via WAL decoding. Since TRUNCATE
/// invalidates all existing change tracking, downstream STs need a
/// full refresh to resync.
fn mark_downstream_for_reinit(source_oid: pg_sys::Oid) -> Result<(), PgTrickleError> {
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET needs_reinit = true, updated_at = now() \
         WHERE pgt_id IN ( \
             SELECT pgt_id FROM pgtrickle.pgt_dependencies \
             WHERE source_relid = $1 \
         )",
        &[source_oid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    warning!(
        "pg_trickle: TRUNCATE detected on source OID {} via WAL — downstream STs marked for reinit",
        source_oid.to_u32()
    );

    Ok(())
}

// ── Transition Orchestration ───────────────────────────────────────────────

/// Check if the WAL transition is complete and finalize if so.
///
/// Called by the scheduler on each tick for sources in TRANSITIONING mode.
/// The transition is complete when the WAL decoder has caught up close to
/// the current WAL position (within a reasonable lag threshold).
///
/// If the transition has timed out, falls back to trigger-based CDC.
pub fn check_and_complete_transition(
    source_oid: pg_sys::Oid,
    pgt_id: i64,
    dep: &StDependency,
    change_schema: &str,
) -> Result<(), PgTrickleError> {
    // A41-3: Re-check eligibility (PK, replica identity FULL) before advancing
    // or completing the transition.  DDL executed concurrently during the
    // TRANSITIONING window must abort immediately rather than proceeding to WAL
    // mode with an invalid slot.
    match recheck_source_eligible_for_wal(source_oid) {
        Ok(true) => {}
        Ok(false) => {
            warning!(
                "pg_trickle: TRANSITIONING source OID {} is no longer eligible \
                 (PK or replica identity changed) — aborting WAL transition",
                source_oid.to_u32()
            );
            abort_wal_transition(source_oid, pgt_id, change_schema)?;
            return Ok(());
        }
        Err(e) => {
            warning!(
                "pg_trickle: eligibility recheck error for TRANSITIONING source OID {}: {} \
                 — aborting WAL transition",
                source_oid.to_u32(),
                e
            );
            abort_wal_transition(source_oid, pgt_id, change_schema)?;
            return Ok(());
        }
    }

    let default_slot = slot_name_for_source(source_oid);
    let slot_name = dep.slot_name.as_deref().unwrap_or(&default_slot);

    let required_lsn =
        dep.cutover_lsn
            .as_deref()
            .ok_or_else(|| PgTrickleError::CdcCutoverUnproven {
                source_oid: source_oid.to_u32(),
                target: "WAL".to_string(),
                required_lsn: "unknown".to_string(),
                confirmed_lsn: None,
            })?;
    let confirmed_lsn =
        get_existing_slot_lsn(slot_name)?.ok_or_else(|| PgTrickleError::CdcCutoverUnproven {
            source_oid: source_oid.to_u32(),
            target: "WAL".to_string(),
            required_lsn: required_lsn.to_string(),
            confirmed_lsn: None,
        })?;
    let lag_bytes = get_slot_lag_bytes(slot_name)?;

    if crate::version::lsn_gte(&confirmed_lsn, required_lsn) {
        // Decoder has committed the exact handoff LSN — complete the transition.
        complete_wal_transition(source_oid, pgt_id, change_schema)?;
        return Ok(());
    }

    // Not caught up — check for timeout with progressive backoff (F32: G2.4).
    // We allow up to 3× the configured timeout before aborting, logging
    // warnings at 1× and 2× to give operators visibility into slow transitions.
    if let Some(ref started_at) = dep.transition_started_at {
        let base_timeout = config::pg_trickle_wal_transition_timeout();

        // Check if we've exceeded the final deadline (3× base timeout)
        let final_deadline = base_timeout * 3;
        let exceeded_final = Spi::get_one_with_args::<bool>(
            "SELECT (now() - $1::timestamptz) > ($2 * interval '1 second')",
            &[started_at.as_str().into(), final_deadline.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .unwrap_or(false);

        if exceeded_final {
            warning!(
                "pg_trickle: WAL transition exhausted all retries for source OID {} \
                 (lag: {} bytes after {}s, max {}s); falling back to triggers",
                source_oid.to_u32(),
                lag_bytes,
                final_deadline,
                final_deadline,
            );
            abort_wal_transition(source_oid, pgt_id, change_schema)?;
            return Ok(());
        }

        // Emit warnings at intermediate checkpoints (1× and 2× base timeout)
        let exceeded_first = Spi::get_one_with_args::<bool>(
            "SELECT (now() - $1::timestamptz) > ($2 * interval '1 second')",
            &[started_at.as_str().into(), base_timeout.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .unwrap_or(false);

        if exceeded_first {
            let exceeded_second = Spi::get_one_with_args::<bool>(
                "SELECT (now() - $1::timestamptz) > ($2 * interval '1 second')",
                &[started_at.as_str().into(), (base_timeout * 2).into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            .unwrap_or(false);

            if exceeded_second {
                warning!(
                    "pg_trickle: WAL transition slow for source OID {} \
                     (lag: {} bytes, retry 2/3 — will abort after {}s)",
                    source_oid.to_u32(),
                    lag_bytes,
                    final_deadline,
                );
            } else {
                log!(
                    "pg_trickle: WAL transition slow for source OID {} \
                     (lag: {} bytes, retry 1/3 — extending deadline)",
                    source_oid.to_u32(),
                    lag_bytes,
                );
            }
        }
    }

    Ok(())
}

/// Complete the WAL transition — drop the trigger and switch to WAL mode.
///
/// Called when the WAL decoder has caught up past the handoff point.
///
/// COR-003 (v0.72.0): The handoff is now serialized by:
///
/// 1. Acquiring a session-level advisory lock keyed by `source_oid` so that
///    concurrent calls for the same source cannot interleave.
/// 2. Updating the catalog mode to `WAL` **before** dropping the trigger.
///    This ensures that once the catalog is flipped, any DML that observes
///    the new mode will write through WAL (duplication is handled by the
///    dedup logic at refresh time).  After `DROP TRIGGER` takes `ACCESS
///    EXCLUSIVE`, no new trigger invocations can start.
/// 3. Releasing the advisory lock after both steps complete.
fn complete_wal_transition(
    source_oid: pg_sys::Oid,
    _pgt_id: i64,
    change_schema: &str,
) -> Result<(), PgTrickleError> {
    let oid_u32 = source_oid.to_u32();
    // COR-003: Use the source OID as the advisory lock key.  A negative
    // cast avoids collisions with other positive-domain keys already used
    // in the codebase (refresh advisory locks use pgt_id, which is a
    // positive BIGINT sequence value).
    let lock_key = -(oid_u32 as i64);

    // Acquire a session-level advisory lock to serialize concurrent transition
    // completion for the same source.  Session-level (not xact-level) so the
    // lock spans the two SPI calls below even if they run in sub-transactions.
    Spi::run_with_args("SELECT pg_advisory_lock($1)", &[lock_key.into()])
        .map_err(|e| PgTrickleError::SpiError(format!("advisory lock for COR-003: {}", e)))?;
    let source_table = cdc::get_qualified_table_name(source_oid)?;
    Spi::run(&format!("LOCK TABLE {source_table} IN SHARE MODE")) // nosemgrep: rust.spi.run.dynamic-format — source_table is PostgreSQL format('%I.%I') output from a catalog OID.
        .map_err(|e| PgTrickleError::CdcCutoverUnproven {
            source_oid: oid_u32,
            target: "WAL".to_string(),
            required_lsn: "source lock".to_string(),
            confirmed_lsn: Some(e.to_string()),
        })?;

    // Step 1: Update catalog to WAL mode FIRST.
    // After this point the scheduler knows the source is in WAL mode.  Any
    // concurrent DML that fires before DROP TRIGGER takes effect will write
    // through both trigger and WAL (dedup at refresh) — this is the safe side.
    if let Err(e) = StDependency::update_cdc_mode_for_source(source_oid, CdcMode::Wal, None, None) {
        let _ = Spi::run_with_args("SELECT pg_advisory_unlock($1)", &[lock_key.into()]);
        return Err(e);
    }

    // Step 2: Drop the CDC trigger (WAL decoder now covers all changes).
    // DROP TRIGGER takes ACCESS EXCLUSIVE — it waits for all in-flight DML
    // that might be executing the trigger body before proceeding.
    let trigger_result = cdc::drop_change_trigger(source_oid, change_schema);

    // Release the advisory lock regardless of trigger-drop outcome.
    let _ = Spi::run_with_args("SELECT pg_advisory_unlock($1)", &[lock_key.into()]);

    trigger_result?;
    StDependency::set_cutover_for_source(source_oid, None, None)?;

    info!(
        "pg_trickle: completed WAL transition for source OID {} — catalog set to WAL, trigger dropped",
        oid_u32
    );

    // Emit NOTIFY for transition completion
    let slot_name = slot_name_for_source(source_oid);
    monitor::emit_cdc_transition_notify(
        source_oid,
        CdcMode::Transitioning,
        CdcMode::Wal,
        Some(&slot_name),
    );

    Ok(())
}

/// Abort the WAL transition and fall back to trigger-based CDC.
///
/// Called when the transition times out or encounters an unrecoverable error.
/// Cleans up WAL decoder resources and reverts to trigger mode.
pub fn abort_wal_transition(
    source_oid: pg_sys::Oid,
    _pgt_id: i64,
    change_schema: &str,
) -> Result<(), PgTrickleError> {
    let oid_u32 = source_oid.to_u32();
    let slot_name = slot_name_for_source(source_oid);

    let lock_key = -(oid_u32 as i64);
    Spi::run_with_args("SELECT pg_advisory_xact_lock($1)", &[lock_key.into()]).map_err(|e| {
        PgTrickleError::CdcCutoverUnproven {
            source_oid: oid_u32,
            target: "TRIGGER".to_string(),
            required_lsn: "advisory lock".to_string(),
            confirmed_lsn: Some(e.to_string()),
        }
    })?;

    let source_table = cdc::get_qualified_table_name(source_oid)?;
    Spi::run(&format!("LOCK TABLE {source_table} IN SHARE MODE")) // nosemgrep: rust.spi.run.dynamic-format — source_table is PostgreSQL format('%I.%I') output from a catalog OID.
        .map_err(|e| PgTrickleError::CdcCutoverUnproven {
            source_oid: oid_u32,
            target: "TRIGGER".to_string(),
            required_lsn: "source lock".to_string(),
            confirmed_lsn: Some(e.to_string()),
        })?;

    // Future writes must be captured before WAL resources are released.
    ensure_trigger_for_source(source_oid, change_schema)?;

    // Step 1: Drop the replication slot (stops WAL retention)
    if let Err(e) = drop_replication_slot(&slot_name) {
        warning!(
            "pg_trickle: failed to drop replication slot {} during abort: {}",
            slot_name,
            e
        );
    }

    // Step 2: Drop the publication
    if let Err(e) = drop_publication(source_oid) {
        warning!(
            "pg_trickle: failed to drop publication during abort for OID {}: {}",
            oid_u32,
            e
        );
    }

    // Step 3: Revert catalog to trigger mode
    // Step 3: Revert catalog to trigger mode for all dependents of this source.
    StDependency::update_cdc_mode_for_source(source_oid, CdcMode::Trigger, None, None)?;
    StDependency::set_cutover_for_source(source_oid, None, None)?;

    // Step 4: Verify the trigger still exists — recreate if lost.
    // This step is best-effort: if the source table lost its primary key (the
    // common case when falling back due to A41-3 eligibility failure), trigger
    // recreation will fail.  Log a warning and continue — the catalog is already
    // in TRIGGER mode and the slot/publication have been cleaned up.
    if !cdc::trigger_exists(source_oid)? {
        match cdc::resolve_pk_columns(source_oid) {
            Ok(pk_columns) if !pk_columns.is_empty() => {
                match cdc::resolve_source_column_defs(source_oid) {
                    Ok(columns) => {
                        let src_id = crate::citus::SourceIdentifier::from_oid(source_oid)
                            .unwrap_or_else(|_| {
                                crate::citus::SourceIdentifier::from_oid_and_stable_name(
                                    source_oid,
                                    source_oid.to_u32().to_string(),
                                )
                            });
                        if let Err(e) = cdc::create_change_trigger(
                            source_oid,
                            change_schema,
                            &pk_columns,
                            &columns,
                            &src_id.stable_name,
                        ) {
                            warning!(
                                "pg_trickle: could not recreate CDC trigger for source OID {} \
                                 during abort (non-fatal): {}",
                                oid_u32,
                                e
                            );
                        } else {
                            warning!(
                                "pg_trickle: recreated CDC trigger for source OID {} during abort",
                                oid_u32
                            );
                        }
                    }
                    Err(e) => {
                        warning!(
                            "pg_trickle: could not resolve column defs for source OID {} \
                             during abort (non-fatal): {}",
                            oid_u32,
                            e
                        );
                    }
                }
            }
            Ok(_) => {
                // No primary key — cannot recreate trigger.  Log and continue.
                // The source must have a PK restored before CDC can resume.
                warning!(
                    "pg_trickle: source OID {} has no primary key — \
                     CDC trigger not recreated during WAL abort. \
                     Restore a primary key to re-enable trigger-based CDC.",
                    oid_u32
                );
            }
            Err(e) => {
                warning!(
                    "pg_trickle: could not resolve PK columns for source OID {} \
                     during abort (non-fatal): {}",
                    oid_u32,
                    e
                );
            }
        }
    }

    warning!(
        "pg_trickle: aborted WAL transition for source OID {}; reverted to triggers",
        oid_u32
    );

    // Emit NOTIFY for transition abort (fallback to triggers)
    monitor::emit_cdc_transition_notify(source_oid, CdcMode::Wal, CdcMode::Trigger, None);

    Ok(())
}

fn ensure_trigger_for_source(
    source_oid: pg_sys::Oid,
    change_schema: &str,
) -> Result<(), PgTrickleError> {
    if cdc::trigger_exists(source_oid)? {
        return Ok(());
    }
    let pk_columns = cdc::resolve_pk_columns(source_oid)?;
    if pk_columns.is_empty() {
        return Err(PgTrickleError::CdcStateInvalid {
            pgt_id: 0,
            source_name: format!("OID {}", source_oid.to_u32()),
            buffer: "CDC trigger".to_string(),
            reason: "cannot restore trigger without a primary key".to_string(),
        });
    }
    let columns = cdc::resolve_source_column_defs(source_oid)?;
    let source_id = crate::citus::SourceIdentifier::from_oid(source_oid).unwrap_or_else(|_| {
        crate::citus::SourceIdentifier::from_oid_and_stable_name(
            source_oid,
            source_oid.to_u32().to_string(),
        )
    });
    cdc::create_change_trigger(
        source_oid,
        change_schema,
        &pk_columns,
        &columns,
        &source_id.stable_name,
    )?;
    Ok(())
}

/// Force a source back to trigger-based CDC to satisfy a conservative request.
pub fn force_source_to_trigger(
    source_oid: pg_sys::Oid,
    change_schema: &str,
) -> Result<(), PgTrickleError> {
    let deps = StDependency::get_all()?;
    let source_deps: Vec<_> = deps
        .into_iter()
        .filter(|dep| dep.source_relid == source_oid && dep.source_type == "TABLE")
        .collect();

    let previous_mode = if source_deps.iter().any(|dep| dep.cdc_mode == CdcMode::Wal) {
        Some(CdcMode::Wal)
    } else if source_deps
        .iter()
        .any(|dep| dep.cdc_mode == CdcMode::Transitioning)
    {
        Some(CdcMode::Transitioning)
    } else {
        None
    };

    let source_table = cdc::get_qualified_table_name(source_oid)?;
    Spi::run(&format!("LOCK TABLE {source_table} IN SHARE MODE")) // nosemgrep: rust.spi.run.dynamic-format — source_table is PostgreSQL format('%I.%I') output from a catalog OID.
        .map_err(|e| PgTrickleError::CdcCutoverUnproven {
            source_oid: source_oid.to_u32(),
            target: "TRIGGER".to_string(),
            required_lsn: "source lock".to_string(),
            confirmed_lsn: Some(e.to_string()),
        })?;
    ensure_trigger_for_source(source_oid, change_schema)?;

    let slot_name = slot_name_for_source(source_oid);
    if let Err(e) = drop_replication_slot(&slot_name) {
        warning!(
            "pg_trickle: failed to drop replication slot {} while forcing trigger CDC: {}",
            slot_name,
            e
        );
    }
    if let Err(e) = drop_publication(source_oid) {
        warning!(
            "pg_trickle: failed to drop publication while forcing trigger CDC for OID {}: {}",
            source_oid.to_u32(),
            e
        );
    }

    StDependency::update_cdc_mode_for_source(source_oid, CdcMode::Trigger, None, None)?;
    StDependency::set_cutover_for_source(source_oid, None, None)?;

    if !cdc::trigger_exists(source_oid)? {
        let pk_columns = cdc::resolve_pk_columns(source_oid)?;
        let columns = cdc::resolve_source_column_defs(source_oid)?;
        let src_id = crate::citus::SourceIdentifier::from_oid(source_oid).unwrap_or_else(|_| {
            crate::citus::SourceIdentifier::from_oid_and_stable_name(
                source_oid,
                source_oid.to_u32().to_string(),
            )
        });
        cdc::create_change_trigger(
            source_oid,
            change_schema,
            &pk_columns,
            &columns,
            &src_id.stable_name,
        )?;
    }

    if let Some(prev) = previous_mode {
        monitor::emit_cdc_transition_notify(source_oid, prev, CdcMode::Trigger, None);
    }

    Ok(())
}

// ── Scheduler Integration ──────────────────────────────────────────────────

/// Pending slot creation request, collected in Phase 1 and executed in Phase 2.
pub struct PendingSlotCreation {
    pub source_relid: pg_sys::Oid,
    pub pgt_id: i64,
    pub slot_name: String,
}

/// WAL source that reached the error threshold and needs to be aborted
/// (reverted to trigger CDC) in a separate transaction.
pub struct PendingAbort {
    pub source_relid: pg_sys::Oid,
    pub pgt_id: i64,
}

/// Result from Phase 1: pending slot creations and pending aborts.
pub struct Phase1Result {
    pub pending_slots: Vec<PendingSlotCreation>,
    pub pending_aborts: Vec<PendingAbort>,
}

/// Phase 1: Check eligibility, collect pending slot creations, and handle
/// already-transitioned/WAL sources.
///
/// This phase does SPI reads (catalog, pg_replication_slots).
/// Must run in its own transaction BEFORE Phase 2.
///
/// Returns pending slot creations (Phase 2) and pending aborts (Phase 4).
/// WAL poll panics (from missing slots) are caught and counted. Sources
/// that exceed `MAX_CONSECUTIVE_WAL_ERRORS` are queued for abort in a
/// separate transaction (because the SPI connection is broken after a
/// caught panic).
pub fn advance_wal_transitions_phase1(change_schema: &str) -> Result<Phase1Result, PgTrickleError> {
    let cdc_mode = config::pg_trickle_cdc_mode();

    // Get all dependencies to check their CDC mode
    let all_deps = StDependency::get_all()?;

    // Group by source_relid to avoid processing the same source multiple times
    let mut processed_sources = std::collections::HashSet::new();
    let mut pending_slots = Vec::new();
    let mut pending_aborts: Vec<PendingAbort> = Vec::new();

    for dep in &all_deps {
        // Only process TABLE sources (not STREAM_TABLE or VIEW)
        if dep.source_type != "TABLE" {
            continue;
        }

        // Skip if we already processed this source in this tick
        let source_key = dep.source_relid.to_u32();
        if !processed_sources.insert(source_key) {
            continue;
        }

        let requested_mode = StDependency::effective_requested_mode_for_source(dep.source_relid)?;
        match requested_mode.as_deref() {
            None | Some("trigger") => {
                if dep.cdc_mode != CdcMode::Trigger {
                    pending_aborts.push(PendingAbort {
                        source_relid: dep.source_relid,
                        pgt_id: dep.pgt_id,
                    });
                }
                continue;
            }
            Some("auto") | Some("wal") => {}
            Some(_) => continue,
        }

        match dep.cdc_mode {
            CdcMode::Trigger => {
                // Check if this source is eligible for WAL transition
                match check_transition_eligible(dep, requested_mode.as_deref().unwrap_or("auto")) {
                    Ok(true) => {
                        let slot_name = slot_name_for_source(dep.source_relid);
                        // Check if slot already exists (SPI read — fine in Phase 1)
                        match get_existing_slot_lsn(&slot_name)? {
                            Some(slot_lsn) => {
                                // Slot already exists — go straight to Phase 3
                                log!(
                                    "pg_trickle: slot '{}' already exists, finishing transition",
                                    slot_name
                                );
                                if let Err(e) = finish_wal_transition(
                                    dep.source_relid,
                                    dep.pgt_id,
                                    &slot_name,
                                    &slot_lsn,
                                ) {
                                    log!(
                                        "pg_trickle: failed to finish WAL transition for OID {}: {}",
                                        source_key,
                                        e
                                    );
                                }
                            }
                            None => {
                                // Slot needs creation — queue for Phase 2
                                log!(
                                    "pg_trickle: source OID {} eligible for WAL transition, queuing slot creation",
                                    source_key
                                );
                                pending_slots.push(PendingSlotCreation {
                                    source_relid: dep.source_relid,
                                    pgt_id: dep.pgt_id,
                                    slot_name,
                                });
                            }
                        }
                    }
                    Ok(false) => {
                        // Not eligible — stay on triggers
                        if cdc_mode == "auto" {
                            emit_auto_cdc_stuck_log(dep);
                        }
                    }
                    Err(e) => {
                        log!(
                            "pg_trickle: failed to check WAL transition eligibility for source OID {}: {}",
                            source_key,
                            e
                        );
                    }
                }
            }
            CdcMode::Transitioning => {
                // Poll WAL changes (both trigger and WAL are active).
                // Use catch_unwind for the same reason as the Wal branch:
                // a missing/invalid slot causes a PG ERROR → Rust panic.
                let poll_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    poll_source_changes(dep, change_schema)
                }));
                let poll_err = match poll_result {
                    Ok(Ok(())) => None,
                    Ok(Err(e)) => Some(e.to_string()),
                    Err(_panic) => {
                        Some("PG error during TRANSITIONING WAL poll (likely missing slot)".into())
                    }
                };

                if let Some(err_msg) = poll_err {
                    let count = bump_wal_error_count(source_key);
                    if count >= MAX_CONSECUTIVE_WAL_ERRORS {
                        warning!(
                            "pg_trickle: TRANSITIONING WAL poll failed {} consecutive times \
                             for source OID {} — aborting transition back to triggers. \
                             Last error: {}",
                            count,
                            source_key,
                            err_msg
                        );
                        reset_wal_error_count(source_key);
                        // Defer abort to Phase 4 (separate transaction)
                        // because the SPI connection may be broken after
                        // catch_unwind of a PG ERROR.
                        pending_aborts.push(PendingAbort {
                            source_relid: dep.source_relid,
                            pgt_id: dep.pgt_id,
                        });
                    } else {
                        warning!(
                            "pg_trickle: TRANSITIONING WAL poll error for source OID {} \
                             ({}/{} before abort): {}",
                            source_key,
                            count,
                            MAX_CONSECUTIVE_WAL_ERRORS,
                            err_msg
                        );
                    }
                } else {
                    reset_wal_error_count(source_key);
                    // Check if transition is complete or timed out
                    if let Err(e) = check_and_complete_transition(
                        dep.source_relid,
                        dep.pgt_id,
                        dep,
                        change_schema,
                    ) {
                        log!(
                            "pg_trickle: transition check error for source OID {}: {}",
                            source_key,
                            e
                        );
                    }
                }
            }
            CdcMode::Wal => {
                // Poll WAL changes (steady-state WAL mode).
                // Use catch_unwind because a missing/invalid slot causes a
                // PG ERROR → Rust panic that would bypass the error counter.
                let poll_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    poll_source_changes(dep, change_schema)
                }));
                let poll_err = match poll_result {
                    Ok(Ok(())) => None,
                    Ok(Err(e)) => Some(e.to_string()),
                    Err(_panic) => Some("PG error during WAL poll (likely missing slot)".into()),
                };

                if let Some(err_msg) = poll_err {
                    let count = bump_wal_error_count(source_key);
                    if count >= MAX_CONSECUTIVE_WAL_ERRORS {
                        warning!(
                            "pg_trickle: WAL poll failed {} consecutive times for source OID {} \
                             — falling back to triggers. Last error: {}",
                            count,
                            source_key,
                            err_msg
                        );
                        reset_wal_error_count(source_key);
                        // Defer abort to Phase 4 (separate transaction) because
                        // after catch_unwind of a PG ERROR the SPI connection
                        // is broken in this transaction.
                        pending_aborts.push(PendingAbort {
                            source_relid: dep.source_relid,
                            pgt_id: dep.pgt_id,
                        });
                    } else {
                        warning!(
                            "pg_trickle: WAL poll error for source OID {} ({}/{} before fallback): {}",
                            source_key,
                            count,
                            MAX_CONSECUTIVE_WAL_ERRORS,
                            err_msg
                        );
                    }
                } else {
                    reset_wal_error_count(source_key);
                    // Check decoder health periodically (slot existence, lag)
                    if let Err(e) =
                        check_decoder_health(dep.source_relid, dep.pgt_id, change_schema)
                    {
                        log!(
                            "pg_trickle: health check error for WAL source OID {}: {}",
                            source_key,
                            e
                        );
                    }
                }
            }
        }
    }

    Ok(Phase1Result {
        pending_slots,
        pending_aborts,
    })
}

/// Phase 3: Finish WAL transitions for slots that were created in Phase 2.
///
/// Creates publications and updates the catalog for each successfully created slot.
/// This phase does SPI writes and must run in its own transaction AFTER Phase 2.
pub fn advance_wal_transitions_phase3(
    created_slots: &[(PendingSlotCreation, String)],
) -> Result<(), PgTrickleError> {
    for (pending, slot_lsn) in created_slots {
        if let Err(e) = finish_wal_transition(
            pending.source_relid,
            pending.pgt_id,
            &pending.slot_name,
            slot_lsn,
        ) {
            log!(
                "pg_trickle: failed to finish WAL transition for OID {}: {}",
                pending.source_relid.to_u32(),
                e
            );
        }
    }
    Ok(())
}

fn check_transition_eligible(
    dep: &StDependency,
    requested_mode: &str,
) -> Result<bool, PgTrickleError> {
    if !cdc::can_use_logical_replication_for_mode(requested_mode)? {
        return Ok(false);
    }

    if !cdc::check_replica_identity(dep.source_relid)? {
        return Ok(false);
    }

    let pk_columns = cdc::resolve_pk_columns(dep.source_relid)?;
    if pk_columns.is_empty() {
        return Ok(false);
    }

    let identity = cdc::get_replica_identity_mode(dep.source_relid)?;
    if identity != "full" {
        return Ok(false);
    }

    Ok(true)
}

/// Finish a WAL transition after the replication slot has been created.
///
/// Creates the publication and updates the catalog to TRANSITIONING mode.
/// Called from the scheduler after slot creation succeeds in a separate
/// transaction.
///
/// A41-3: Re-checks table eligibility (relkind, existence, PK,
/// replica identity FULL) immediately before committing the TRANSITIONING
/// catalog state update.  If any check fails, the transition is aborted:
/// the replication slot is dropped, the catalog status is reset to TRIGGER
/// mode, and a warning is emitted.  This closes the TOCTOU window between
/// eligibility check and catalog commit.
pub fn finish_wal_transition(
    source_oid: pg_sys::Oid,
    _pgt_id: i64,
    slot_name: &str,
    slot_lsn: &str,
) -> Result<(), PgTrickleError> {
    // A41-3: Eligibility recheck before committing the TRANSITIONING state.
    //
    // Between Phase 1 (eligibility check) and Phase 3 (this function), a
    // concurrent DDL statement could have changed the table's relkind,
    // dropped its primary key, or changed its replica identity.  Re-verify
    // before committing to TRANSITIONING so we never get stuck with an
    // invalid WAL slot.
    let still_eligible = recheck_source_eligible_for_wal(source_oid);
    if let Ok(false) | Err(_) = still_eligible {
        let reason = match still_eligible {
            Ok(false) => "eligibility check failed (relkind, PK, or replica identity changed)",
            Err(ref e) => {
                let _ = e; // drop borrow
                "error during eligibility recheck"
            }
            Ok(true) => unreachable!(),
        };
        warning!(
            "pg_trickle: A41-3: WAL transition eligibility recheck failed for source OID {} \
             — aborting transition back to trigger mode. Reason: {}",
            source_oid.to_u32(),
            reason,
        );
        // Drop the replication slot that was already created in Phase 2.
        // If this fails (e.g. slot was already cleaned up), log and continue —
        // the transition abort is still the correct outcome.
        if let Err(e) = crate::wal_decoder::drop_replication_slot(slot_name) {
            log!(
                "pg_trickle: A41-3: could not drop slot '{}' during abort (non-fatal): {}",
                slot_name,
                e,
            );
        }
        // Update catalog to reflect fallback to trigger mode.
        if let Err(e) =
            StDependency::update_cdc_mode_for_source(source_oid, CdcMode::Trigger, None, None)
        {
            log!(
                "pg_trickle: A41-3: could not reset CDC mode for OID {} (non-fatal): {}",
                source_oid.to_u32(),
                e,
            );
        }
        return Err(PgTrickleError::WalTransitionError(format!(
            "eligibility recheck failed for source OID {}: {}",
            source_oid.to_u32(),
            reason,
        )));
    }

    // Create publication for this source table
    create_publication(source_oid)?;

    StDependency::set_cutover_for_source(source_oid, Some("WAL"), Some(slot_lsn))?;

    // Update catalog — mark as TRANSITIONING
    StDependency::update_cdc_mode_for_source(
        source_oid,
        CdcMode::Transitioning,
        Some(slot_name),
        Some(slot_lsn),
    )?;

    info!(
        "pg_trickle: started WAL transition for source OID {} \
         (slot: {}, slot LSN: {})",
        source_oid.to_u32(),
        slot_name,
        slot_lsn
    );

    // Emit NOTIFY so clients can track the transition
    monitor::emit_cdc_transition_notify(
        source_oid,
        CdcMode::Trigger,
        CdcMode::Transitioning,
        Some(slot_name),
    );

    Ok(())
}

/// A41-3: Re-check whether a source table is still eligible for WAL-based CDC.
///
/// Checks table existence (relkind = 'r'), primary key presence, and
/// replica identity = FULL.  Returns `Ok(true)` when all checks pass,
/// `Ok(false)` when the table is no longer eligible, and `Err` when a
/// catalog lookup fails.
///
/// This is a pure eligibility check — it does not modify any state.
pub(crate) fn recheck_source_eligible_for_wal(
    source_oid: pg_sys::Oid,
) -> Result<bool, PgTrickleError> {
    // 1. Table must still exist and be a regular table.
    let relkind: Option<String> = Spi::get_one(&format!(
        "SELECT relkind::text FROM pg_class WHERE oid = {}",
        source_oid.to_u32(),
    ))
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    match relkind.as_deref() {
        Some("r") => {} // regular table — OK
        Some(k) => {
            log!(
                "pg_trickle: A41-3: source OID {} relkind is '{}', expected 'r'",
                source_oid.to_u32(),
                k,
            );
            return Ok(false);
        }
        None => {
            log!(
                "pg_trickle: A41-3: source OID {} no longer exists",
                source_oid.to_u32(),
            );
            return Ok(false);
        }
    }

    // 2. Table must have a primary key.
    let pk_columns = cdc::resolve_pk_columns(source_oid)?;
    if pk_columns.is_empty() {
        log!(
            "pg_trickle: A41-3: source OID {} has no primary key",
            source_oid.to_u32(),
        );
        return Ok(false);
    }

    // 3. Replica identity must be FULL.
    let identity = cdc::get_replica_identity_mode(source_oid)?;
    if identity != "full" {
        log!(
            "pg_trickle: A41-3: source OID {} replica identity is '{}', expected 'full'",
            source_oid.to_u32(),
            identity,
        );
        return Ok(false);
    }

    Ok(true)
}

// ── Consecutive WAL error tracking ─────────────────────────────────────────

use std::sync::Mutex;

/// Shared consecutive-error counters per source OID.
static WAL_ERROR_COUNTS: Mutex<Option<std::collections::HashMap<u32, u32>>> = Mutex::new(None);

/// Increment the consecutive error counter for a WAL source and return the
/// new count.
fn bump_wal_error_count(source_oid: u32) -> u32 {
    let mut guard = WAL_ERROR_COUNTS.lock().unwrap_or_else(|e| e.into_inner());
    let map = guard.get_or_insert_with(std::collections::HashMap::new);
    let entry = map.entry(source_oid).or_insert(0);
    *entry += 1;
    *entry
}

/// Reset the consecutive error counter for a source after a successful poll.
fn reset_wal_error_count(source_oid: u32) {
    let mut guard = WAL_ERROR_COUNTS.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(map) = guard.as_mut() {
        map.remove(&source_oid);
    }
}

/// EC-18: Rate-limited LOG explaining why `auto` CDC mode is stuck in TRIGGER
/// phase for a particular source.
///
/// Uses a simple modular counter on scheduler ticks. Only emits once every
/// ~60 invocations (approximately once per minute at the default 1s
/// scheduler interval).
fn emit_auto_cdc_stuck_log(dep: &StDependency) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static TICK_COUNTER: AtomicU64 = AtomicU64::new(0);

    let tick = TICK_COUNTER.fetch_add(1, Ordering::Relaxed);
    if !tick.is_multiple_of(60) {
        return;
    }

    let source_oid = dep.source_relid;
    let reason = match cdc::can_use_logical_replication() {
        Ok(false) | Err(_) => {
            "wal_level is not 'logical'. Set wal_level = logical in postgresql.conf and restart."
                .to_string()
        }
        Ok(true) => {
            // WAL is available, check other prerequisites
            let pk_columns = cdc::resolve_pk_columns(source_oid).unwrap_or_default();
            if pk_columns.is_empty() {
                format!(
                    "source OID {} has no PRIMARY KEY. WAL-based CDC requires a PK. \
                     Add a PRIMARY KEY or switch to cdc_mode = 'trigger'.",
                    source_oid.to_u32()
                )
            } else {
                let identity = cdc::get_replica_identity_mode(source_oid)
                    .unwrap_or_else(|_| "unknown".to_string());
                if identity != "full" {
                    format!(
                        "source OID {} has REPLICA IDENTITY '{}' (need FULL). \
                         Run: ALTER TABLE ... REPLICA IDENTITY FULL",
                        source_oid.to_u32(),
                        identity
                    )
                } else {
                    format!(
                        "source OID {} meets prerequisites but transition has not started yet. \
                         This may resolve on the next scheduler tick.",
                        source_oid.to_u32()
                    )
                }
            }
        }
    };

    log!(
        "pg_trickle: cdc_mode = 'auto' but source OID {} is still using triggers. Reason: {}",
        source_oid.to_u32(),
        reason
    );
}

/// Poll WAL changes for a source that's in TRANSITIONING or WAL mode.
fn poll_source_changes(dep: &StDependency, change_schema: &str) -> Result<(), PgTrickleError> {
    let slot_name = match &dep.slot_name {
        Some(name) => name.clone(),
        None => slot_name_for_source(dep.source_relid),
    };

    // COR-3: Acquire a transaction-scoped advisory lock keyed on the source OID
    // before consuming the replication slot.  This serialises the eligibility
    // check (recheck_source_eligible_for_wal) and WAL consumption into an atomic
    // unit, preventing a concurrent pg_drop_replication_slot() from invalidating
    // the slot between the check and the first pg_logical_slot_get_changes() call.
    let lock_key = dep.source_relid.to_u32() as i64;
    Spi::run_with_args(
        "SELECT pg_advisory_xact_lock($1::bigint)",
        &[lock_key.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(format!("COR-3: advisory lock acquire failed: {e}")))?;

    // Resolve qualified source table name for filtering test_decoding output
    let source_table_name = cdc::get_qualified_table_name(dep.source_relid)?;

    // Resolve source column definitions for decoding
    let pk_columns = cdc::resolve_pk_columns(dep.source_relid)?;
    let columns = cdc::resolve_source_column_defs(dep.source_relid)?;

    // Poll and decode changes
    let (count, last_lsn) = poll_wal_changes(
        dep.source_relid,
        &slot_name,
        &source_table_name,
        change_schema,
        &pk_columns,
        &columns,
    )?;

    // OBS-3: Publish the count of records written in this poll cycle.
    crate::shmem::set_wal_decoder_pending_records(count as u64);

    // Update the decoder confirmed LSN in the catalog
    if let Some(ref lsn) = last_lsn {
        StDependency::update_cdc_mode_for_source(
            dep.source_relid,
            dep.cdc_mode,
            dep.slot_name.as_deref(),
            Some(lsn),
        )?;
    }

    if count > 0 {
        log!(
            "pg_trickle: polled {} WAL changes for source OID {} (last LSN: {})",
            count,
            dep.source_relid.to_u32(),
            last_lsn.as_deref().unwrap_or("none")
        );
    }

    Ok(())
}

/// Check health of a WAL decoder for a source in WAL mode.
///
/// Verifies the replication slot exists, `wal_level` is still `logical`,
/// and lag is within bounds.
/// If the slot is missing, `wal_level` changed, or lag is excessive,
/// attempts recovery or fallback.
pub fn check_decoder_health(
    source_oid: pg_sys::Oid,
    pgt_id: i64,
    change_schema: &str,
) -> Result<(), PgTrickleError> {
    let slot_name = slot_name_for_source(source_oid);

    // Check wal_level hasn't been changed (takes effect after restart)
    let wal_level = Spi::get_one::<String>("SELECT current_setting('wal_level')")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .unwrap_or_default();
    if wal_level != "logical" {
        warning!(
            "pg_trickle: wal_level changed from 'logical' to '{}' — \
             WAL decoder for source OID {} will fail after next restart. \
             Falling back to triggers now.",
            wal_level,
            source_oid.to_u32()
        );
        abort_wal_transition(source_oid, pgt_id, change_schema)?;
        return Ok(());
    }

    // Check if the slot still exists (scoped to the current database to avoid
    // false positives from identically-named slots in other databases).
    let slot_exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pg_replication_slots \
         WHERE slot_name = $1 AND database = current_database())",
        &[slot_name.as_str().into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or(false);

    if !slot_exists {
        warning!(
            "pg_trickle: replication slot '{}' for source OID {} is missing — \
             falling back to triggers",
            slot_name,
            source_oid.to_u32()
        );
        abort_wal_transition(source_oid, pgt_id, change_schema)?;
        return Ok(());
    }

    // A41-3: Re-check eligibility (primary key, replica identity FULL) for the
    // WAL steady-state.  DDL executed after the transition completed (DROP
    // CONSTRAINT, ALTER TABLE REPLICA IDENTITY) can invalidate the session;
    // detect this and fall back to triggers immediately.
    match recheck_source_eligible_for_wal(source_oid) {
        Ok(true) => {}
        Ok(false) => {
            warning!(
                "pg_trickle: WAL source OID {} is no longer eligible \
                 (PK or replica identity changed) — falling back to triggers",
                source_oid.to_u32()
            );
            abort_wal_transition(source_oid, pgt_id, change_schema)?;
            return Ok(());
        }
        Err(e) => {
            warning!(
                "pg_trickle: eligibility recheck error for WAL source OID {}: {} \
                 — falling back to triggers",
                source_oid.to_u32(),
                e
            );
            abort_wal_transition(source_oid, pgt_id, change_schema)?;
            return Ok(());
        }
    }

    // Check lag — if excessive (>1GB), warn but keep running
    let lag_bytes = get_slot_lag_bytes(&slot_name)?;
    const WARN_LAG_BYTES: i64 = 1_073_741_824; // 1 GB

    if lag_bytes > WARN_LAG_BYTES {
        warning!(
            "pg_trickle: WAL decoder for source OID {} has excessive lag: {} bytes",
            source_oid.to_u32(),
            lag_bytes
        );
    }

    // SF-11: Check if the publication needs rebuilding because the source
    // table was converted to partitioned after publication creation.
    check_publication_health(source_oid)?;

    Ok(())
}

// ── Helpers ────────────────────────────────────────────────────────────────

/// Quote a SQL identifier (simple quoting for generated names).
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Detect a schema mismatch between decoded pgoutput columns and the
/// expected column definitions.
///
/// Returns `true` if:
/// - The decoded row contains a column that doesn't appear in the expected set
///   (e.g., a column was added via ALTER TABLE ADD COLUMN).
/// - The decoded row has at least as many columns as expected but some expected
///   columns are missing (e.g., a column was renamed). F33: G2.5.
///   The "at least as many" guard avoids false positives on partial-column
///   messages (DELETE with non-FULL replica identity sends only PK columns).
///
/// DDL event triggers in hooks.rs handle the reinitialize; this provides a
/// safety net for DDL that bypasses event triggers.
fn detect_schema_mismatch(
    parsed: &std::collections::HashMap<String, String>,
    expected_columns: &[(String, String)],
) -> bool {
    if parsed.is_empty() {
        return false;
    }

    let expected_names: std::collections::HashSet<&str> = expected_columns
        .iter()
        .map(|(name, _)| name.as_str())
        .collect();

    // Check for unknown columns (additions)
    for col_name in parsed.keys() {
        if !expected_names.contains(col_name.as_str()) {
            return true;
        }
    }

    // Check for missing expected columns (renames) — F33
    // Only check when the decoded message has at least as many columns as
    // expected, to avoid false positives from DELETE messages that only
    // carry PK columns with non-FULL replica identity.
    if parsed.len() >= expected_columns.len() {
        let parsed_names: std::collections::HashSet<&str> =
            parsed.keys().map(|k| k.as_str()).collect();
        for expected_name in &expected_names {
            if !parsed_names.contains(*expected_name) {
                return true;
            }
        }
    }

    false
}

#[doc(hidden)]
pub fn detect_test_decoding_schema_mismatch_for_fuzz(
    parsed: &std::collections::HashMap<String, String>,
    expected_columns: &[(String, String)],
) -> bool {
    detect_schema_mismatch(parsed, expected_columns)
}

// ── Per-worker change buffer write (Citus distributed CDC) ───────────────────

/// Process change rows from a temp table (fetched from a remote Citus worker via
/// `dblink`) and write decoded events into the local change buffer.
///
/// This mirrors [`poll_wal_changes`] but reads from a pre-fetched local temp
/// table `temp_table` (columns: `lsn text, xid text, data text`) instead of
/// calling `pg_logical_slot_get_changes()` directly.  The `test_decoding`
/// text format is identical on remote workers and locally, so the same
/// parsing logic applies.
///
/// Returns the number of change rows successfully written to the buffer.
pub fn write_worker_changes_to_buffer(
    temp_table: &str,
    source_table_name: &str,
    change_schema: &str,
    source_oid: pg_sys::Oid,
    pk_columns: &[String],
    columns: &[(String, String)],
) -> Result<i64, PgTrickleError> {
    let oid_u32 = source_oid.to_u32();

    // Fetch all rows from the temp table created by the dblink call.
    let select_sql = format!(
        "SELECT lsn, data FROM {} WHERE data IS NOT NULL AND data != '' ORDER BY lsn",
        temp_table
    );

    let mut count: i64 = 0;

    // COR-5: Pre-resolve canonical names for OID-based filter (same approach as poll_wal_changes).
    let filter_names = resolve_wal_filter_names(source_oid, source_table_name)?;

    Spi::connect(|client| {
        let result = client
            .select(&select_sql, None, &[])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        for row in result {
            let lsn = row
                .get::<String>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or_default();
            let data = row
                .get::<String>(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or_default();

            // COR-5: OID-based filter via pre-resolved canonical names set.
            match extract_table_name_from_test_decoding(&data) {
                Some(name) if filter_names.contains(name) => {} // our table — process
                _ => continue,
            }

            if let Some(action) = parse_pgoutput_action(&data) {
                if action != 'T' {
                    let parsed = parse_pgoutput_columns(&data);
                    if detect_schema_mismatch(&parsed, columns) {
                        return Err(PgTrickleError::WalTransitionError(format!(
                            "Schema change detected on worker for source OID {} — \
                             decoded columns don't match expected",
                            oid_u32
                        )));
                    }
                }

                write_decoded_change(
                    oid_u32,
                    &lsn,
                    &action,
                    &data,
                    change_schema,
                    pk_columns,
                    columns,
                )?;
                count += 1;
            }
        }

        Ok::<(), PgTrickleError>(())
    })?;

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // ── Naming convention tests ────────────────────────────────────
    // NOTE: slot_name_for_source and publication_name_for_source now use
    // stable hash names (CITUS-4, v0.32.0) and require SPI context to
    // resolve OID → schema.table. They are tested at the pg_test / integration
    // level where a real PostgreSQL connection is available.

    // ── quote_ident tests ──────────────────────────────────────────

    #[test]
    fn test_quote_ident_simple() {
        assert_eq!(quote_ident("my_slot"), "\"my_slot\"");
    }

    #[test]
    fn test_quote_ident_with_quotes() {
        assert_eq!(quote_ident("my\"slot"), "\"my\"\"slot\"");
    }

    // ── COR-5: extract_table_name_from_test_decoding tests ────────

    #[test]
    fn test_extract_table_name_insert() {
        let data = "table public.orders: INSERT: id[integer]:1";
        assert_eq!(
            extract_table_name_from_test_decoding(data),
            Some("public.orders")
        );
    }

    #[test]
    fn test_extract_table_name_update() {
        let data = "table myschema.events: UPDATE: id[integer]:7 name[text]:'X'";
        assert_eq!(
            extract_table_name_from_test_decoding(data),
            Some("myschema.events")
        );
    }

    #[test]
    fn test_extract_table_name_partition_child() {
        // partition child name — OID filter accepts it if inhparent matches
        let data = "table public.orders_2024: INSERT: id[integer]:2";
        assert_eq!(
            extract_table_name_from_test_decoding(data),
            Some("public.orders_2024")
        );
    }

    #[test]
    fn test_extract_table_name_begin_returns_none() {
        assert_eq!(extract_table_name_from_test_decoding("BEGIN 12345"), None);
    }

    #[test]
    fn test_extract_table_name_commit_returns_none() {
        assert_eq!(
            extract_table_name_from_test_decoding("COMMIT 12345 (at ...)"),
            None
        );
    }

    // ── parse_pgoutput_action tests ────────────────────────────────

    #[test]
    fn test_parse_pgoutput_insert() {
        let data = "table public.users: INSERT: id[integer]:1 name[text]:'Alice'";
        assert_eq!(parse_pgoutput_action(data), Some('I'));
    }

    #[test]
    fn test_parse_pgoutput_update() {
        let data = "table public.users: UPDATE: id[integer]:1 name[text]:'Bob'";
        assert_eq!(parse_pgoutput_action(data), Some('U'));
    }

    #[test]
    fn test_parse_pgoutput_delete() {
        let data = "table public.users: DELETE: id[integer]:1";
        assert_eq!(parse_pgoutput_action(data), Some('D'));
    }

    #[test]
    fn test_parse_pgoutput_truncate() {
        let data = "table public.users: TRUNCATE: (no column data)";
        assert_eq!(parse_pgoutput_action(data), Some('T'));
    }

    #[test]
    fn test_parse_pgoutput_begin() {
        let data = "BEGIN 12345";
        assert_eq!(parse_pgoutput_action(data), None);
    }

    #[test]
    fn test_parse_pgoutput_commit() {
        let data = "COMMIT 12345";
        assert_eq!(parse_pgoutput_action(data), None);
    }

    #[test]
    fn test_parse_pgoutput_table_named_insert_log() {
        // Table named INSERT_LOG must not be misclassified (G2.3 edge case).
        let data = "table public.INSERT_LOG: UPDATE: id[integer]:1 msg[text]:'hello'";
        assert_eq!(parse_pgoutput_action(data), Some('U'));
    }

    #[test]
    fn test_parse_pgoutput_column_value_contains_delete() {
        // Column value containing "DELETE:" must not be misclassified (G2.3 edge case).
        let data = "table audit.log: UPDATE: op[text]:'DELETE: old row' id[integer]:42";
        assert_eq!(parse_pgoutput_action(data), Some('U'));
    }

    #[test]
    fn test_parse_pgoutput_schema_named_insert() {
        // Schema named "insert" must not affect action classification.
        let data = "table insert.orders: DELETE: id[integer]:7";
        assert_eq!(parse_pgoutput_action(data), Some('D'));
    }

    // ── parse_pgoutput_columns tests ───────────────────────────────

    #[test]
    fn test_parse_pgoutput_columns_insert() {
        let data = "table public.users: INSERT: id[integer]:1 name[text]:'Alice'";
        let cols = parse_pgoutput_columns(data);
        assert_eq!(cols.get("id").map(|s| s.as_str()), Some("1"));
        assert_eq!(cols.get("name").map(|s| s.as_str()), Some("Alice"));
    }

    #[test]
    fn test_parse_pgoutput_columns_empty() {
        let data = "BEGIN 12345";
        let cols = parse_pgoutput_columns(data);
        assert!(cols.is_empty());
    }

    #[test]
    fn test_parse_pgoutput_columns_truncated_action_marker() {
        let data = format!("{}DELETE:", "x".repeat(24));
        assert_eq!(data.len(), 31);
        assert!(parse_pgoutput_columns(&data).is_empty());
    }

    // ── build_pk_hash_from_values tests ────────────────────────────

    #[test]
    fn test_build_pk_hash_empty() {
        let pk: Vec<String> = vec![];
        let parsed = std::collections::HashMap::new();
        assert_eq!(build_pk_hash_from_values(&pk, &parsed), "0");
    }

    #[test]
    fn test_build_pk_hash_single_key() {
        let pk = vec!["id".to_string()];
        let mut parsed = std::collections::HashMap::new();
        parsed.insert("id".to_string(), "42".to_string());
        let result = build_pk_hash_from_values(&pk, &parsed);
        assert!(result.contains("pg_trickle_hash"));
        assert!(result.contains("42"));
    }

    #[test]
    fn test_build_pk_hash_composite_key() {
        let pk = vec!["a".to_string(), "b".to_string()];
        let mut parsed = std::collections::HashMap::new();
        parsed.insert("a".to_string(), "1".to_string());
        parsed.insert("b".to_string(), "2".to_string());
        let result = build_pk_hash_from_values(&pk, &parsed);
        assert!(result.contains("pg_trickle_hash_multi"));
        assert!(result.contains("'1'"));
        assert!(result.contains("'2'"));
    }

    #[test]
    fn test_build_pk_hash_missing_key() {
        let pk = vec!["id".to_string()];
        let parsed = std::collections::HashMap::new(); // no "id" key
        assert_eq!(build_pk_hash_from_values(&pk, &parsed), "0");
    }

    #[test]
    fn test_build_pk_hash_sql_injection_safe() {
        let pk = vec!["id".to_string()];
        let mut parsed = std::collections::HashMap::new();
        parsed.insert("id".to_string(), "'; DROP TABLE users; --".to_string());
        let result = build_pk_hash_from_values(&pk, &parsed);
        // Value should have single quotes escaped
        assert!(result.contains("''"));
    }

    // ── detect_schema_mismatch tests ───────────────────────────────

    #[test]
    fn test_schema_mismatch_no_mismatch() {
        let expected = vec![
            ("id".to_string(), "integer".to_string()),
            ("name".to_string(), "text".to_string()),
        ];
        let mut parsed = std::collections::HashMap::new();
        parsed.insert("id".to_string(), "42".to_string());
        parsed.insert("name".to_string(), "Alice".to_string());
        assert!(!detect_schema_mismatch(&parsed, &expected));
    }

    #[test]
    fn test_schema_mismatch_new_column() {
        let expected = vec![
            ("id".to_string(), "integer".to_string()),
            ("name".to_string(), "text".to_string()),
        ];
        let mut parsed = std::collections::HashMap::new();
        parsed.insert("id".to_string(), "42".to_string());
        parsed.insert("name".to_string(), "Alice".to_string());
        parsed.insert("email".to_string(), "alice@example.com".to_string());
        assert!(detect_schema_mismatch(&parsed, &expected));
    }

    #[test]
    fn test_schema_mismatch_empty_parsed() {
        let expected = vec![("id".to_string(), "integer".to_string())];
        let parsed = std::collections::HashMap::new();
        assert!(!detect_schema_mismatch(&parsed, &expected));
    }

    #[test]
    fn test_schema_mismatch_subset_ok() {
        // Fewer decoded columns than expected is OK (e.g., DELETE only sends PK)
        let expected = vec![
            ("id".to_string(), "integer".to_string()),
            ("name".to_string(), "text".to_string()),
        ];
        let mut parsed = std::collections::HashMap::new();
        parsed.insert("id".to_string(), "42".to_string());
        assert!(!detect_schema_mismatch(&parsed, &expected));
    }

    #[test]
    fn test_schema_mismatch_column_rename() {
        // F33: Column renamed from "name" to "full_name" — same count, different names
        let expected = vec![
            ("id".to_string(), "integer".to_string()),
            ("name".to_string(), "text".to_string()),
        ];
        let mut parsed = std::collections::HashMap::new();
        parsed.insert("id".to_string(), "42".to_string());
        parsed.insert("full_name".to_string(), "Alice".to_string());
        assert!(detect_schema_mismatch(&parsed, &expected));
    }

    // ── parse_pgoutput_old_columns tests (G2.2) ───────────────────

    // ── TEST-3: Additional WAL decoder coverage ────────────────────

    /// TEST-3a: old_col_* extraction for UPDATE with REPLICA IDENTITY FULL
    /// returns all old column values, not just PK columns.
    #[test]
    fn test_old_columns_full_replica_identity_all_values() {
        let data = "table public.products: UPDATE: old-key: id[integer]:5 name[text]:'Widget' price[numeric]:9.99 new-tuple: id[integer]:5 name[text]:'Gadget' price[numeric]:19.99";
        let old = parse_pgoutput_old_columns(data);
        assert_eq!(old.get("id").map(|s| s.as_str()), Some("5"));
        assert_eq!(old.get("name").map(|s| s.as_str()), Some("Widget"));
        assert_eq!(old.get("price").map(|s| s.as_str()), Some("9.99"));
        assert_eq!(
            old.len(),
            3,
            "FULL replica identity should return all old columns"
        );
    }

    /// TEST-3b: old_col_* returns empty for INSERT (no old tuple exists).
    #[test]
    fn test_old_columns_insert_returns_empty() {
        let data = "table public.products: INSERT: id[integer]:1 name[text]:'New'";
        let old = parse_pgoutput_old_columns(data);
        assert!(old.is_empty(), "INSERT should have no old columns");
    }

    /// TEST-3c: pk_hash for keyless table (empty PK) returns "0".
    #[test]
    fn test_pk_hash_keyless_table_returns_zero() {
        // Keyless tables have no PK columns, so pk_hash must be "0"
        let pk_cols: Vec<String> = vec![];
        let mut parsed = std::collections::HashMap::new();
        parsed.insert("val".to_string(), "hello".to_string());
        parsed.insert("num".to_string(), "42".to_string());
        assert_eq!(
            build_pk_hash_from_values(&pk_cols, &parsed),
            "0",
            "Keyless table pk_hash must be 0"
        );
    }

    /// TEST-3d: Action string parsing uses exact position-based comparison,
    /// not substring search (table named "DELETE_LOG" must parse correctly).
    #[test]
    fn test_action_exact_matching_not_substring() {
        // Table named "DELETE_LOG" — action is INSERT, not DELETE
        let data = "table public.DELETE_LOG: INSERT: id[integer]:1 msg[text]:'row deleted'";
        assert_eq!(parse_pgoutput_action(data), Some('I'));

        // Table named "UPDATE_AUDIT" — action is DELETE
        let data2 = "table public.UPDATE_AUDIT: DELETE: id[integer]:99";
        assert_eq!(parse_pgoutput_action(data2), Some('D'));
    }

    /// TEST-3e: pk_hash with special characters in PK values are properly
    /// escaped to prevent SQL injection in generated expressions.
    #[test]
    fn test_pk_hash_special_chars_escaped() {
        let pk = vec!["name".to_string()];
        let mut parsed = std::collections::HashMap::new();
        parsed.insert("name".to_string(), "O'Brien".to_string());
        let result = build_pk_hash_from_values(&pk, &parsed);
        // The single quote should be doubled for SQL safety
        assert!(
            result.contains("O''Brien"),
            "Single quotes in PK values must be escaped: {result}"
        );
    }

    #[test]
    fn test_parse_old_columns_update_with_old_key() {
        let data = "table public.users: UPDATE: old-key: id[integer]:1 name[text]:'Alice' new-tuple: id[integer]:1 name[text]:'Bob'";
        let old = parse_pgoutput_old_columns(data);
        assert_eq!(old.get("id").map(|s| s.as_str()), Some("1"));
        assert_eq!(old.get("name").map(|s| s.as_str()), Some("Alice"));
    }

    #[test]
    fn test_parse_old_columns_no_old_key_section() {
        // UPDATE without REPLICA IDENTITY FULL produces no old-key section
        let data = "table public.users: UPDATE: id[integer]:1 name[text]:'Bob'";
        let old = parse_pgoutput_old_columns(data);
        assert!(old.is_empty());
    }

    #[test]
    fn test_parse_old_columns_truncated_old_key_marker() {
        let old = parse_pgoutput_old_columns("old-key:");
        assert!(old.is_empty());
    }

    #[test]
    fn test_parse_old_columns_insert_has_no_old_key() {
        let data = "table public.users: INSERT: id[integer]:1 name[text]:'Alice'";
        let old = parse_pgoutput_old_columns(data);
        assert!(old.is_empty());
    }

    #[test]
    fn test_parse_old_columns_delete_has_no_old_key() {
        let data = "table public.users: DELETE: id[integer]:1";
        let old = parse_pgoutput_old_columns(data);
        assert!(old.is_empty());
    }

    #[test]
    fn test_parse_old_columns_old_key_at_end() {
        // Edge case: old-key section without a following new-tuple marker
        let data = "table public.users: UPDATE: old-key: id[integer]:99 name[text]:'Zara'";
        let old = parse_pgoutput_old_columns(data);
        assert_eq!(old.get("id").map(|s| s.as_str()), Some("99"));
        assert_eq!(old.get("name").map(|s| s.as_str()), Some("Zara"));
    }

    #[test]
    fn test_parse_old_columns_composite_pk() {
        let data = "table public.orders: UPDATE: old-key: customer_id[integer]:5 order_id[integer]:10 new-tuple: customer_id[integer]:5 order_id[integer]:10 status[text]:'shipped'";
        let old = parse_pgoutput_old_columns(data);
        assert_eq!(old.get("customer_id").map(|s| s.as_str()), Some("5"));
        assert_eq!(old.get("order_id").map(|s| s.as_str()), Some("10"));
        assert_eq!(old.len(), 2);
    }

    // ── P2 property / fuzz tests ──────────────────────────────────────────

    proptest! {
        #[test]
        fn prop_parse_pgoutput_action_no_panic(input in ".*") {
            let result = parse_pgoutput_action(&input);
            if let Some(c) = result {
                prop_assert!(matches!(c, 'I' | 'U' | 'D' | 'T'));
            }
        }

        #[test]
        fn prop_parse_pgoutput_columns_no_panic(input in ".*") {
            let _ = parse_pgoutput_columns(&input);
        }

        #[test]
        fn prop_parse_pgoutput_old_columns_no_panic(input in ".*") {
            let _ = parse_pgoutput_old_columns(&input);
        }

        #[test]
        fn prop_build_pk_hash_empty_pk_returns_zero(
            values in proptest::collection::hash_map(
                "[a-z]{1,10}",
                "[a-z0-9]{1,20}",
                0..5usize
            )
        ) {
            let pk_cols: Vec<String> = vec![];
            let result = build_pk_hash_from_values(&pk_cols, &values);
            prop_assert_eq!(result, "0".to_string());
        }

        #[test]
        fn prop_build_pk_hash_no_panic(
            pk_cols in proptest::collection::vec("[a-z]{1,10}", 0..5usize),
            values in proptest::collection::hash_map(
                "[a-z]{1,10}",
                "[a-z0-9]{1,20}",
                0..10usize
            )
        ) {
            let _ = build_pk_hash_from_values(&pk_cols, &values);
        }

        #[test]
        fn prop_detect_schema_mismatch_empty_parsed_is_false(
            expected in proptest::collection::vec(
                ("[a-z]{1,10}", "[a-z]{1,10}"),
                0..5usize
            )
        ) {
            let parsed = std::collections::HashMap::<String, String>::new();
            prop_assert!(!detect_schema_mismatch(&parsed, &expected));
        }

        #[test]
        fn prop_detect_schema_mismatch_no_panic(
            parsed in proptest::collection::hash_map(
                "[a-z]{1,10}",
                "[a-z]{1,10}",
                0..5usize
            ),
            expected in proptest::collection::vec(
                ("[a-z]{1,10}", "[a-z]{1,10}"),
                0..5usize
            )
        ) {
            let _ = detect_schema_mismatch(&parsed, &expected);
        }
    }
}
