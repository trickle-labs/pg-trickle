//! Transactional IVM (Incremental View Maintenance) for IMMEDIATE mode.
//!
//! This module implements statement-level AFTER trigger-based IVM that
//! maintains stream tables synchronously within the same transaction as
//! the base table DML.
//!
//! ## Architecture
//!
//! Instead of row-level CDC triggers that write to change buffer tables
//! (used by DIFFERENTIAL mode), IMMEDIATE mode uses:
//!
//! 1. **Statement-level BEFORE triggers** on each base table.
//!    These acquire an `ExclusiveLock` on the stream table to prevent
//!    concurrent conflicting updates.
//!
//! 2. **Statement-level AFTER triggers** with transition tables
//!    (`REFERENCING NEW TABLE AS ... OLD TABLE AS ...`).
//!    When `pg_trickle.ivm_use_enr = true` (default, PG18+), the trigger
//!    body calls `pgt_ivm_apply_delta_enr` which references the ENR names
//!    directly. When false (legacy mode), the trigger body copies
//!    transition table data into temp tables
//!    (`__pgt_newtable_<oid>` / `__pgt_oldtable_<oid>`), then calls
//!    `pgt_ivm_apply_delta`.
//!
//! 3. **Delta computation** reuses the existing DVM engine with
//!    `DeltaSource::TransitionTable` — the `Scan` operator reads
//!    from the ENR or temp tables.
//!
//! 4. **Delta application** uses the same explicit DML path (DELETE +
//!    UPDATE + INSERT) as the deferred mode's user-trigger path.
//!
//! ## Current Limitations
//!
//! - TRUNCATE on a base table causes a full refresh of the stream table.
//!   future optimization).
//! - Recursive CTEs emit a warning about potential stack-depth issues.
//! - TopK tables use micro-refresh (recompute top-K on each DML) gated by
//!   `pg_trickle.ivm_topk_max_limit`.

use crate::api::quote_identifier;
use crate::error::PgTrickleError;
use pgrx::prelude::*;

// ── Lock Mode Analysis ─────────────────────────────────────────────────

/// The type of lock acquired by BEFORE triggers on the stream table.
///
/// Determines the concurrency strategy for IMMEDIATE mode:
/// - `Exclusive` — advisory lock prevents all concurrent IVM updates.
///   Required for queries with aggregates, DISTINCT, or multi-table joins
///   where concurrent modifications could produce incorrect results.
/// - `RowExclusive` — lighter lock allowing concurrent inserts to proceed
///   in parallel when safe. Suitable for simple single-table SELECT queries
///   without aggregates or DISTINCT, where each row_id is independent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IvmLockMode {
    /// ExclusiveLock equivalent — serializes all IVM updates.
    Exclusive,
    /// RowExclusiveLock equivalent — allows concurrent non-conflicting inserts.
    RowExclusive,
}

impl IvmLockMode {
    /// Analyze a defining query to determine the appropriate lock mode.
    ///
    /// Uses `RowExclusive` only when the query is a simple single-table
    /// projection/filter without aggregates or DISTINCT. All other queries
    /// use `Exclusive` to prevent incorrect delta computation from
    /// concurrent modifications.
    pub fn for_query(defining_query: &str) -> Self {
        // Try to parse the defining query. If parsing fails, default to Exclusive.
        let result = match crate::dvm::parse_defining_query(defining_query) {
            Ok(tree) => tree,
            Err(_) => {
                // PERF-3 (v0.31.0): Count parse failures that force Exclusive lock mode.
                crate::shmem::increment_ivm_lock_parse_errors();
                return IvmLockMode::Exclusive;
            }
        };

        if Self::is_simple_scan_chain(&result) {
            IvmLockMode::RowExclusive
        } else {
            IvmLockMode::Exclusive
        }
    }

    /// Check if an OpTree is a simple scan chain:
    /// Scan → optional Filter → optional Project
    /// (no joins, no aggregates, no distinct, no subqueries, etc.)
    fn is_simple_scan_chain(tree: &crate::dvm::parser::OpTree) -> bool {
        use crate::dvm::parser::OpTree;
        match tree {
            OpTree::Scan { .. } => true,
            OpTree::Filter { child, .. } | OpTree::Project { child, .. } => {
                Self::is_simple_scan_chain(child)
            }
            _ => false,
        }
    }
}

// ── Delta SQL Template Cache ───────────────────────────────────────────

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Cache key for IVM delta templates: (pgt_id, source_oid, has_new, has_old, use_enr).
///
/// The delta SQL varies by which transition tables are present:
/// - INSERT: has_new=true, has_old=false
/// - UPDATE: has_new=true, has_old=true
/// - DELETE: has_new=false, has_old=true
///
/// The `use_enr` flag determines whether the SQL references ENR names
/// (`__pgt_newtable`/`__pgt_oldtable`) or OID-suffixed temp table names.
#[derive(Hash, PartialEq, Eq, Clone)]
struct IvmCacheKey {
    pgt_id: i64,
    source_oid: u32,
    has_new: bool,
    has_old: bool,
    /// PERF-4 (v0.31.0): true = ENR names, false = temp-table names.
    use_enr: bool,
}

/// Cached IVM delta template — stores the pre-computed delta SQL,
/// output columns, and the hash of the defining query for invalidation.
#[derive(Clone)]
struct CachedIvmDelta {
    /// Hash of the defining query — for staleness detection.
    defining_query_hash: u64,
    /// Pre-computed delta SQL (references transition temp tables).
    delta_sql: String,
    /// User-facing column names from the delta result.
    user_columns: Vec<String>,
    /// STAB-2 (v0.30.0): Insertion order counter for clock-style eviction.
    last_used: u64,
}

fn hash_str(s: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

thread_local! {
    /// Per-session cache of IVM delta SQL templates.
    /// Keyed by (pgt_id, source_oid, has_new, has_old).
    /// Invalidated when the defining query hash changes or the
    /// shared cache generation counter advances.
    static IVM_DELTA_CACHE: RefCell<HashMap<IvmCacheKey, CachedIvmDelta>> =
        RefCell::new(HashMap::new());

    /// Local snapshot of the shared cache generation counter.
    static LOCAL_IVM_CACHE_GEN: Cell<u64> = const { Cell::new(0) };

    /// STAB-2 (v0.30.0): Monotone insertion counter for clock-style eviction.
    static IVM_CACHE_CLOCK: Cell<u64> = const { Cell::new(0) };

    /// STAB-6 (v0.30.0): Whether the subxact-abort callback has been registered
    /// for this session.  Reset to false only on new session init (never in practice
    /// since thread-locals persist for the session lifetime).
    static IVM_ABORT_CALLBACK_REGISTERED: Cell<bool> = const { Cell::new(false) };
}

/// STAB-6 (v0.30.0): Register a subxact-abort callback that clears IVM_DELTA_CACHE
/// on subxact abort.  Idempotent: uses a thread-local guard to register only once
/// per session.
fn ensure_ivm_abort_callback_registered() {
    IVM_ABORT_CALLBACK_REGISTERED.with(|registered| {
        if registered.get() {
            return;
        }
        // SAFETY: register_subxact_callback is safe to call from a PG function context.
        let _receipt = pgrx::register_subxact_callback(
            pgrx::PgSubXactCallbackEvent::AbortSub,
            |_subid, _parent_subid| {
                // Clear the IVM delta cache on any subxact abort.
                // This ensures stale entries from a failed apply are not reused.
                IVM_DELTA_CACHE.with(|cache| cache.borrow_mut().clear());
                pgrx::debug1!("[pg_trickle] STAB-6: IVM_DELTA_CACHE cleared on subxact abort");
            },
        );
        // Intentionally leak the receipt — we want the callback to live forever.
        std::mem::forget(_receipt);
        registered.set(true);
    });
}

/// Invalidate all cached IVM delta templates for a given pgt_id.
pub fn invalidate_ivm_delta_cache(pgt_id: i64) {
    IVM_DELTA_CACHE.with(|cache| {
        cache.borrow_mut().retain(|k, _| k.pgt_id != pgt_id);
    });
}

// ── IVM trigger / function naming ────────────────────────────────────────

/// All trigger and function names used by IVM for a single
/// (pgt_id, source_oid) pair.
///
/// Extracted as a pure struct so naming is unit-testable and consistent
/// between `setup_ivm_triggers` and `cleanup_ivm_triggers`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IvmTriggerNames {
    // BEFORE triggers (statement-level)
    pub before_insert: String,
    pub before_update: String,
    pub before_delete: String,
    pub before_trunc: String,

    // AFTER triggers (statement-level, with transition tables)
    pub after_ins: String,
    pub after_upd: String,
    pub after_del: String,
    pub after_trunc: String,

    // Trigger functions (PL/pgSQL)
    pub before_fn: String,
    pub after_ins_fn: String,
    pub after_upd_fn: String,
    pub after_del_fn: String,
    pub after_trunc_fn: String,
}

impl IvmTriggerNames {
    /// Generate all names for a given stream table / source table pair.
    pub fn new(pgt_id: i64, source_oid_u32: u32) -> Self {
        Self {
            before_insert: format!("pgt_ivm_before_insert_{pgt_id}"),
            before_update: format!("pgt_ivm_before_update_{pgt_id}"),
            before_delete: format!("pgt_ivm_before_delete_{pgt_id}"),
            before_trunc: format!("pgt_ivm_before_trunc_{pgt_id}"),

            after_ins: format!("pgt_ivm_after_ins_{pgt_id}"),
            after_upd: format!("pgt_ivm_after_upd_{pgt_id}"),
            after_del: format!("pgt_ivm_after_del_{pgt_id}"),
            after_trunc: format!("pgt_ivm_after_trunc_{pgt_id}"),

            before_fn: format!("pgtrickle.pgt_ivm_before_fn_{pgt_id}_{source_oid_u32}"),
            after_ins_fn: format!("pgtrickle.pgt_ivm_after_ins_fn_{pgt_id}_{source_oid_u32}"),
            after_upd_fn: format!("pgtrickle.pgt_ivm_after_upd_fn_{pgt_id}_{source_oid_u32}"),
            after_del_fn: format!("pgtrickle.pgt_ivm_after_del_fn_{pgt_id}_{source_oid_u32}"),
            after_trunc_fn: format!("pgtrickle.pgt_ivm_after_trunc_fn_{pgt_id}_{source_oid_u32}"),
        }
    }

    /// All trigger names (for DROP TRIGGER).
    pub fn all_triggers(&self) -> [&str; 8] {
        [
            &self.before_insert,
            &self.before_update,
            &self.before_delete,
            &self.before_trunc,
            &self.after_ins,
            &self.after_upd,
            &self.after_del,
            &self.after_trunc,
        ]
    }

    /// All function names (for DROP FUNCTION).
    pub fn all_functions(&self) -> [&str; 5] {
        [
            &self.before_fn,
            &self.after_ins_fn,
            &self.after_upd_fn,
            &self.after_del_fn,
            &self.after_trunc_fn,
        ]
    }
}

/// Install IVM triggers on a source table for an IMMEDIATE-mode stream table.
///
/// Creates statement-level BEFORE and AFTER triggers for INSERT, UPDATE,
/// DELETE, and TRUNCATE operations. The AFTER triggers use `REFERENCING`
/// clauses to capture transition tables.
///
/// # Arguments
///
/// * `source_oid` — OID of the base table to install triggers on.
/// * `pgt_id` — The stream table's `pgt_id` (catalog primary key).
/// * `st_relid` — OID of the stream table (for locking).
/// * `lock_mode` — The lock mode to use for BEFORE triggers.
pub fn setup_ivm_triggers(
    source_oid: pg_sys::Oid,
    pgt_id: i64,
    st_relid: pg_sys::Oid,
    lock_mode: IvmLockMode,
) -> Result<(), PgTrickleError> {
    let oid_u32 = source_oid.to_u32();
    let st_oid_u32 = st_relid.to_u32();
    let names = IvmTriggerNames::new(pgt_id, oid_u32);

    // Resolve the fully-qualified source table name.
    let source_table =
        Spi::get_one_with_args::<String>("SELECT $1::oid::regclass::text", &[source_oid.into()])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            .ok_or_else(|| {
                PgTrickleError::NotFound(format!("Table with OID {} not found", oid_u32))
            })?;

    // ── BEFORE triggers (statement-level) ────────────────────────────
    let lock_body = match lock_mode {
        IvmLockMode::Exclusive => format!("PERFORM pg_advisory_xact_lock({st_oid_u32}::BIGINT);"),
        IvmLockMode::RowExclusive => {
            format!("PERFORM pg_try_advisory_xact_lock({st_oid_u32}::BIGINT);")
        }
    };

    let before_fn = &names.before_fn;
    // R4: IVM trigger functions are SECURITY DEFINER for private bookkeeping.
    // Definition-derived delta SQL switches to the stream owner before it is
    // evaluated. SET search_path prevents search_path hijacking.
    // A45-1: BEFORE trigger has restricted search_path (no `public`) because
    // its body only acquires an advisory lock — it never executes user SQL.
    // AFTER trigger functions include `public` in their search_path so that
    // the user's delta SQL can resolve unqualified source table references.
    // The AFTER trigger body only calls schema-qualified `pgtrickle.*` functions,
    // so adding `public` to the header does not introduce shadowing risk for
    // extension internals.
    let create_fn_sql = format!(
        "CREATE OR REPLACE FUNCTION {before_fn}()
         RETURNS trigger LANGUAGE plpgsql
         SECURITY DEFINER -- nosemgrep: sql.security-definer.present
         SET search_path = pgtrickle_changes, pgtrickle, pg_catalog, pg_temp
         AS $$
         BEGIN
             -- Lock stream table for IVM ({lock_mode:?} mode).
             {lock_body}
             RETURN NULL;
         END;
         $$"
    );
    Spi::run(&create_fn_sql).map_err(|e| {
        PgTrickleError::SpiError(format!(
            "Failed to create IVM BEFORE trigger function: {}",
            e
        ))
    })?;

    for (op, trigger_name) in [
        ("INSERT", &names.before_insert),
        ("UPDATE", &names.before_update),
        ("DELETE", &names.before_delete),
    ] {
        let create_trigger_sql = format!(
            "CREATE TRIGGER {trigger_name}
             BEFORE {op} ON {source_table}
             FOR EACH STATEMENT
             EXECUTE FUNCTION {before_fn}()"
        );
        Spi::run(&create_trigger_sql).map_err(|e| {
            PgTrickleError::SpiError(format!(
                "Failed to create IVM BEFORE trigger on {}: {}",
                source_table, e
            ))
        })?;
    }

    // ── BEFORE TRUNCATE trigger ──────────────────────────────────────
    let create_trunc_before_sql = format!(
        "CREATE TRIGGER {trunc_trig}
         BEFORE TRUNCATE ON {source_table}
         FOR EACH STATEMENT
         EXECUTE FUNCTION {before_fn}()",
        trunc_trig = names.before_trunc,
    );
    Spi::run(&create_trunc_before_sql).map_err(|e| {
        PgTrickleError::SpiError(format!(
            "Failed to create IVM BEFORE TRUNCATE trigger on {}: {}",
            source_table, e
        ))
    })?;

    // ── AFTER triggers (statement-level, with transition tables) ─────

    // PERF-4 (v0.31.0): Choose trigger body based on ENR mode GUC.
    // When use_enr = true, reference ENRs directly (no CTAS to temp table).
    // When use_enr = false, use legacy temp-table copy approach.
    let use_enr = crate::config::pg_trickle_ivm_use_enr();

    // AFTER INSERT: only NEW table
    let create_after_ins_fn = if use_enr {
        format!(
            "CREATE OR REPLACE FUNCTION {fn}()
         RETURNS trigger LANGUAGE plpgsql
         SECURITY DEFINER -- nosemgrep: sql.security-definer.present
         SET search_path = pgtrickle_changes, pgtrickle, public, pg_catalog, pg_temp -- nosemgrep: security-definer-after-trigger-includes-public-for-user-delta-sql
         AS $$
         BEGIN
             PERFORM pgtrickle.pgt_ivm_apply_delta_enr({pgt_id}, {oid_u32}, true, false);
             RETURN NULL;
         END;
         $$",
            fn = names.after_ins_fn,
        )
    } else {
        format!(
            "CREATE OR REPLACE FUNCTION {fn}()
         RETURNS trigger LANGUAGE plpgsql
         SECURITY DEFINER -- nosemgrep: sql.security-definer.present
         SET search_path = pgtrickle_changes, pgtrickle, public, pg_catalog, pg_temp -- nosemgrep: security-definer-after-trigger-includes-public-for-user-delta-sql
         AS $$
         BEGIN
             CREATE TEMP TABLE __pgt_newtable_{oid_u32} ON COMMIT DROP AS
                 SELECT * FROM __pgt_newtable;
             PERFORM pgtrickle.pgt_ivm_apply_delta({pgt_id}, {oid_u32}, true, false);
             DROP TABLE IF EXISTS __pgt_newtable_{oid_u32};
             RETURN NULL;
         END;
         $$",
            fn = names.after_ins_fn,
        )
    };
    Spi::run(&create_after_ins_fn).map_err(|e| {
        PgTrickleError::SpiError(format!("Failed to create IVM AFTER INSERT function: {}", e))
    })?;

    let create_after_ins_trigger = format!(
        "CREATE TRIGGER {trig}
         AFTER INSERT ON {source_table}
         REFERENCING NEW TABLE AS __pgt_newtable
         FOR EACH STATEMENT
         EXECUTE FUNCTION {fn}()",
        trig = names.after_ins,
        fn = names.after_ins_fn,
    );
    Spi::run(&create_after_ins_trigger).map_err(|e| {
        PgTrickleError::SpiError(format!(
            "Failed to create IVM AFTER INSERT trigger on {}: {}",
            source_table, e
        ))
    })?;

    // AFTER UPDATE: both OLD and NEW tables
    let create_after_upd_fn = if use_enr {
        format!(
            "CREATE OR REPLACE FUNCTION {fn}()
         RETURNS trigger LANGUAGE plpgsql
         SECURITY DEFINER -- nosemgrep: sql.security-definer.present
         SET search_path = pgtrickle_changes, pgtrickle, public, pg_catalog, pg_temp -- nosemgrep: security-definer-after-trigger-includes-public-for-user-delta-sql
         AS $$
         BEGIN
             PERFORM pgtrickle.pgt_ivm_apply_delta_enr({pgt_id}, {oid_u32}, true, true);
             RETURN NULL;
         END;
         $$",
            fn = names.after_upd_fn,
        )
    } else {
        format!(
            "CREATE OR REPLACE FUNCTION {fn}()
         RETURNS trigger LANGUAGE plpgsql
         SECURITY DEFINER -- nosemgrep: sql.security-definer.present
         SET search_path = pgtrickle_changes, pgtrickle, public, pg_catalog, pg_temp -- nosemgrep: security-definer-after-trigger-includes-public-for-user-delta-sql
         AS $$
         BEGIN
             CREATE TEMP TABLE __pgt_newtable_{oid_u32} ON COMMIT DROP AS
                 SELECT * FROM __pgt_newtable;
             CREATE TEMP TABLE __pgt_oldtable_{oid_u32} ON COMMIT DROP AS
                 SELECT * FROM __pgt_oldtable;
             PERFORM pgtrickle.pgt_ivm_apply_delta({pgt_id}, {oid_u32}, true, true);
             DROP TABLE IF EXISTS __pgt_newtable_{oid_u32};
             DROP TABLE IF EXISTS __pgt_oldtable_{oid_u32};
             RETURN NULL;
         END;
         $$",
            fn = names.after_upd_fn,
        )
    };
    Spi::run(&create_after_upd_fn).map_err(|e| {
        PgTrickleError::SpiError(format!("Failed to create IVM AFTER UPDATE function: {}", e))
    })?;

    let create_after_upd_trigger = format!(
        "CREATE TRIGGER {trig}
         AFTER UPDATE ON {source_table}
         REFERENCING OLD TABLE AS __pgt_oldtable NEW TABLE AS __pgt_newtable
         FOR EACH STATEMENT
         EXECUTE FUNCTION {fn}()",
        trig = names.after_upd,
        fn = names.after_upd_fn,
    );
    Spi::run(&create_after_upd_trigger).map_err(|e| {
        PgTrickleError::SpiError(format!(
            "Failed to create IVM AFTER UPDATE trigger on {}: {}",
            source_table, e
        ))
    })?;

    // AFTER DELETE: only OLD table
    let create_after_del_fn = if use_enr {
        format!(
            "CREATE OR REPLACE FUNCTION {fn}()
         RETURNS trigger LANGUAGE plpgsql
         SECURITY DEFINER -- nosemgrep: sql.security-definer.present
         SET search_path = pgtrickle_changes, pgtrickle, public, pg_catalog, pg_temp -- nosemgrep: security-definer-after-trigger-includes-public-for-user-delta-sql
         AS $$
         BEGIN
             PERFORM pgtrickle.pgt_ivm_apply_delta_enr({pgt_id}, {oid_u32}, false, true);
             RETURN NULL;
         END;
         $$",
            fn = names.after_del_fn,
        )
    } else {
        format!(
            "CREATE OR REPLACE FUNCTION {fn}()
         RETURNS trigger LANGUAGE plpgsql
         SECURITY DEFINER -- nosemgrep: sql.security-definer.present
         SET search_path = pgtrickle_changes, pgtrickle, public, pg_catalog, pg_temp -- nosemgrep: security-definer-after-trigger-includes-public-for-user-delta-sql
         AS $$
         BEGIN
             CREATE TEMP TABLE __pgt_oldtable_{oid_u32} ON COMMIT DROP AS
                 SELECT * FROM __pgt_oldtable;
             PERFORM pgtrickle.pgt_ivm_apply_delta({pgt_id}, {oid_u32}, false, true);
             DROP TABLE IF EXISTS __pgt_oldtable_{oid_u32};
             RETURN NULL;
         END;
         $$",
            fn = names.after_del_fn,
        )
    };
    Spi::run(&create_after_del_fn).map_err(|e| {
        PgTrickleError::SpiError(format!("Failed to create IVM AFTER DELETE function: {}", e))
    })?;

    let create_after_del_trigger = format!(
        "CREATE TRIGGER {trig}
         AFTER DELETE ON {source_table}
         REFERENCING OLD TABLE AS __pgt_oldtable
         FOR EACH STATEMENT
         EXECUTE FUNCTION {fn}()",
        trig = names.after_del,
        fn = names.after_del_fn,
    );
    Spi::run(&create_after_del_trigger).map_err(|e| {
        PgTrickleError::SpiError(format!(
            "Failed to create IVM AFTER DELETE trigger on {}: {}",
            source_table, e
        ))
    })?;

    // AFTER TRUNCATE: no transition tables, full refresh
    let create_after_trunc_fn = format!(
        "CREATE OR REPLACE FUNCTION {fn}()
         RETURNS trigger LANGUAGE plpgsql
         SECURITY DEFINER -- nosemgrep: sql.security-definer.present
         SET search_path = pgtrickle_changes, pgtrickle, public, pg_catalog, pg_temp -- nosemgrep: security-definer-after-trigger-includes-public-for-user-delta-sql
         AS $$
         BEGIN
             PERFORM pgtrickle.pgt_ivm_handle_truncate({pgt_id});
             RETURN NULL;
         END;
         $$",
        fn = names.after_trunc_fn,
    );
    Spi::run(&create_after_trunc_fn).map_err(|e| {
        PgTrickleError::SpiError(format!(
            "Failed to create IVM AFTER TRUNCATE function: {}",
            e
        ))
    })?;

    let create_after_trunc_trigger = format!(
        "CREATE TRIGGER {trig}
         AFTER TRUNCATE ON {source_table}
         FOR EACH STATEMENT
         EXECUTE FUNCTION {fn}()",
        trig = names.after_trunc,
        fn = names.after_trunc_fn,
    );
    Spi::run(&create_after_trunc_trigger).map_err(|e| {
        PgTrickleError::SpiError(format!(
            "Failed to create IVM AFTER TRUNCATE trigger on {}: {}",
            source_table, e
        ))
    })?;

    pgrx::log!(
        "[pg_trickle] Installed IVM triggers on {} for ST pgt_id={}",
        source_table,
        pgt_id,
    );

    Ok(())
}

/// Remove IVM triggers from a source table for a specific stream table.
///
/// Drops all BEFORE/AFTER triggers and their PL/pgSQL functions that were
/// created by `setup_ivm_triggers`.
pub fn cleanup_ivm_triggers(source_relid: pg_sys::Oid, pgt_id: i64) -> Result<(), PgTrickleError> {
    let oid_u32 = source_relid.to_u32();
    let names = IvmTriggerNames::new(pgt_id, oid_u32);

    // Get the source table name for trigger drop.
    let source_table =
        Spi::get_one_with_args::<String>("SELECT $1::oid::regclass::text", &[source_relid.into()])
            .unwrap_or(None);

    if let Some(ref table) = source_table {
        for trigger in &names.all_triggers() {
            let drop_sql = format!("DROP TRIGGER IF EXISTS {trigger} ON {table}");
            let _ = Spi::run(&drop_sql); // nosemgrep: rust.spi.run.dynamic-format — trigger identifiers come from catalog metadata.
        }
    }

    // Drop the trigger functions (CASCADE to be safe).
    for fn_name in &names.all_functions() {
        let drop_sql = format!("DROP FUNCTION IF EXISTS {fn_name}() CASCADE");
        let _ = Spi::run(&drop_sql); // nosemgrep: rust.spi.run.dynamic-format — function identifiers come from catalog metadata.
    }

    pgrx::log!(
        "[pg_trickle] Cleaned up IVM triggers for source OID {} (pgt_id={})",
        oid_u32,
        pgt_id,
    );

    Ok(())
}

/// EC-01b: cross-cycle phantom-row reconciliation for IMMEDIATE mode.
///
/// Mirrors `cleanup_cross_cycle_phantoms` in the DIFFERENTIAL refresh path.
/// In IMMEDIATE mode, statement-level AFTER triggers compute and apply a
/// delta in the same transaction as the base-table DML, but partial delta
/// application can still leave stale `__pgt_row_id` values in the stream
/// table when the defining query contains a join — for example, a
/// non-monotone `... = (SELECT MAX(...) ...)` predicate over a comma-joined
/// derived table where an UPDATE shifts the MAX but the previous winning
/// row is not part of the current delta.
///
/// Gating mirrors the DIFFERENTIAL path (join-bearing, keyed, non-partitioned)
/// to keep the cost localised to queries that actually need reconciliation.
/// When parsing fails we err on the side of safety and run the cleanup.
/// EC-01b: cross-cycle phantom-row reconciliation for IMMEDIATE mode.
///
/// Mirrors `cleanup_cross_cycle_phantoms` in the DIFFERENTIAL refresh path
/// but with a relaxed gate: in IMMEDIATE mode the trigger-based delta
/// engine can leave stale rows even for queries whose `__pgt_row_id` is
/// derived from `row_to_json(sub)` (because the join output omits one
/// side's PK — TPC-H q07 / q15 are the canonical cases). The full-query
/// reconciliation is still safe in that regime because the hash is
/// deterministic on row content: an ST row whose hash is absent from the
/// current full result is unambiguously stale.
///
/// Partitioned stream tables are excluded because the per-partition
/// reconciliation requires partition-key-aware orphan detection that the
/// shared cleanup helper does not yet implement.
fn run_immediate_phantom_cleanup(
    st: &crate::catalog::StreamTableMeta,
    st_qualified: &str,
) -> Result<(), PgTrickleError> {
    let query_has_join = crate::dvm::query_has_join(&st.defining_query).unwrap_or(true);
    if !query_has_join || st.st_partition_key.is_some() {
        return Ok(());
    }
    // Do NOT run the full-query reconciliation for recursive CTEs. The
    // ivm_recursive_max_depth guard intentionally limits how many rows
    // the delta engine materialises.  Running the full query here would
    // bypass the guard and insert the suppressed rows back.
    if crate::dvm::query_has_recursive_cte(&st.defining_query).unwrap_or(false) {
        return Ok(());
    }

    crate::refresh::phd1::prepare_cross_cycle_phantom_table(st, &st.defining_query)?;
    let cleanup_result = crate::refresh::with_stream_owner(st, || {
        crate::refresh::phd1::cleanup_cross_cycle_phantoms(
            st.pgt_id,
            st_qualified,
            &st.defining_query,
            10_000,
        )
    });
    crate::refresh::drop_owner_temp_table(&format!("__pgt_recon_{}", st.pgt_id));
    let phantom_cleanup_count = cleanup_result?;

    // Mark downstream ST consumers for reinit when phantom rows were
    // removed, mirroring the DIFFERENTIAL refresh behaviour so chained
    // stream tables stay consistent with the cleaned-up upstream state.
    if phantom_cleanup_count > 0
        && let Ok(downstream_ids) =
            crate::catalog::StDependency::get_downstream_pgt_ids(st.pgt_relid)
    {
        for ds_id in &downstream_ids {
            if let Err(e) = crate::catalog::StreamTableMeta::mark_for_reinitialize(*ds_id) {
                pgrx::warning!(
                    "[pg_trickle] EC-01b: failed to mark downstream ST {ds_id} for reinit \
                     after IMMEDIATE phantom cleanup: {e}"
                );
            }
        }
    }

    Ok(())
}

fn apply_ivm_owner_delta(
    st: &crate::catalog::StreamTableMeta,
    delta_sql: &str,
    user_columns: &[String],
    delta_table: &str,
) -> Result<i64, PgTrickleError> {
    let st_qualified = format!(
        "\"{}\".\"{}\"",
        st.pgt_schema.replace('"', "\"\""),
        st.pgt_name.replace('"', "\"\""),
    );
    let delta_table_quoted = quote_identifier(delta_table);

    crate::refresh::prepare_owner_temp_table(st, &delta_table_quoted, delta_sql)?;

    let result = crate::refresh::with_stream_owner(st, || {
        Spi::run(&format!("INSERT INTO {delta_table_quoted} {delta_sql}")) // nosemgrep: rust.spi.run.dynamic-format — table is quoted and delta_sql is generated by the DVM engine.
            .map_err(|e| {
                PgTrickleError::SpiError(format!("Failed to materialize IVM delta: {e}"))
            })?;

        let delta_count =
            Spi::get_one::<i64>(&format!("SELECT count(*) FROM {delta_table_quoted}"))
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(0);

        if delta_count > 0 {
            Spi::run(&build_ivm_delete_sql(
                &st_qualified,
                delta_table,
                st.has_keyless_source,
            ))
            .map_err(|e| PgTrickleError::SpiError(format!("IVM delta DELETE failed: {e}")))?;
            Spi::run(&build_ivm_insert_sql(
                &st_qualified,
                delta_table,
                user_columns,
                st.has_keyless_source,
            ))
            .map_err(|e| PgTrickleError::SpiError(format!("IVM delta INSERT failed: {e}")))?;
        }

        Ok(delta_count)
    });
    crate::refresh::drop_owner_temp_table(&delta_table_quoted);
    result
}

fn prepare_ivm_owner_transition_tables(
    st: &crate::catalog::StreamTableMeta,
    source_oid: u32,
    has_new: bool,
    has_old: bool,
    copy_from_enr: bool,
) -> Result<(), PgTrickleError> {
    let owner = quote_identifier(&crate::refresh::stream_owner_name(st)?);
    for (has_table, kind) in [(has_new, "new"), (has_old, "old")] {
        if !has_table {
            continue;
        }
        let name = format!("__pgt_{kind}table_{source_oid}");
        let table = format!("pg_temp.{}", quote_identifier(&name));
        if copy_from_enr {
            Spi::run(&format!("DROP TABLE IF EXISTS {table}")) // nosemgrep: rust.spi.run.dynamic-format — table is derived from a numeric source OID.
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            Spi::run(&format!(
                "CREATE TEMP TABLE {table} ON COMMIT DROP AS SELECT * FROM __pgt_{kind}table"
            ))
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        }
        let privileges = if kind == "new" {
            "SELECT, DELETE"
        } else {
            "SELECT"
        };
        Spi::run(&format!("GRANT {privileges} ON TABLE {table} TO {owner}")) // nosemgrep: rust.spi.run.dynamic-format — privilege token is fixed; table and owner are quoted identifiers.
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    }

    if has_new {
        let (source_table, hash_columns) =
            crate::refresh::source_visibility_key(pg_sys::Oid::from(source_oid))?;
        let source_hash = crate::refresh::codegen::build_content_hash_expr("s.", &hash_columns);
        let transition_hash = crate::refresh::codegen::build_content_hash_expr("n.", &hash_columns);
        let new_table = format!(
            "pg_temp.{}",
            quote_identifier(&format!("__pgt_newtable_{source_oid}"))
        );
        let filter_sql = format!(
            "DELETE FROM {new_table} n WHERE NOT EXISTS (\
               SELECT 1 FROM {source_table} s WHERE {source_hash} = {transition_hash}\
             )"
        );
        crate::refresh::with_stream_owner(st, || {
            Spi::run(&filter_sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))
        })?;
        Spi::run(&format!("REVOKE DELETE ON TABLE {new_table} FROM {owner}")) // nosemgrep: rust.spi.run.dynamic-format — table and owner are quoted identifiers.
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    }
    Ok(())
}

fn drop_ivm_transition_copies(source_oid: u32, has_new: bool, has_old: bool) {
    for (has_table, kind) in [(has_new, "new"), (has_old, "old")] {
        if has_table {
            let table = quote_identifier(&format!("__pgt_{kind}table_{source_oid}"));
            let _ = Spi::run(&format!("DROP TABLE IF EXISTS pg_temp.{table}")); // nosemgrep: rust.spi.run.dynamic-format — table is derived from a numeric source OID.
        }
    }
}

/// SQL-callable function: apply IVM delta for a stream table.
///
/// Called from the PL/pgSQL AFTER trigger body after the transition tables
/// have been copied to temp tables (`__pgt_newtable_<oid>` /
/// `__pgt_oldtable_<oid>`).
///
/// This function:
/// 1. Loads the stream table metadata from the catalog.
/// 2. Generates (or retrieves from cache) the delta SQL using the DVM engine.
/// 3. Applies the delta (DELETE + INSERT ON CONFLICT) to the stream table.
///
/// Delta SQL templates are cached per (pgt_id, source_oid, has_new, has_old)
/// to avoid re-parsing the defining query on every trigger invocation.
#[pg_extern(schema = "pgtrickle")]
fn pgt_ivm_apply_delta(
    pgt_id: i64,
    source_oid: i32,
    has_new: bool,
    has_old: bool,
) -> Result<(), PgTrickleError> {
    use crate::catalog::StreamTableMeta;

    // STAB-6 (v0.30.0): Register a per-session subxact-abort callback (once) so
    // that if this trigger function fails mid-statement, the thread-local
    // IVM_DELTA_CACHE is cleared.  Without this, a stale entry can survive
    // a failed apply and be reused in the next statement.
    ensure_ivm_abort_callback_registered();

    // EC-25/EC-26: Set the internal_refresh flag so DML guard triggers
    // allow the IVM delta application to modify the storage table.
    Spi::run("SET LOCAL pg_trickle.internal_refresh = 'true'")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    let source_oid_u32 = source_oid as u32;

    // Load stream table metadata.
    let st = StreamTableMeta::get_by_id(pgt_id)?.ok_or_else(|| {
        PgTrickleError::NotFound(format!("Stream table with pgt_id={pgt_id} not found"))
    })?;

    // ── Task 5.2: TopK micro-refresh in IMMEDIATE mode ──────────────
    // Instead of delta computation, recompute the top-K rows and diff
    // against the current stream table contents.
    if st.topk_limit.is_some() {
        return apply_topk_micro_refresh(&st);
    }

    prepare_ivm_owner_transition_tables(&st, source_oid_u32, has_new, has_old, false)?;

    // Try to get cached delta SQL template.
    let (delta_sql, user_columns) =
        get_or_compute_ivm_delta(pgt_id, source_oid_u32, has_new, has_old, &st, false)?;

    let st_qualified = format!(
        "\"{}\".\"{}\"",
        st.pgt_schema.replace('"', "\"\""),
        st.pgt_name.replace('"', "\"\""),
    );

    let delta_table = format!("__pgt_ivm_delta_{pgt_id}");
    let delta_count = apply_ivm_owner_delta(&st, &delta_sql, &user_columns, &delta_table)?;

    // EC-01b: reconcile cross-cycle phantom rows in IMMEDIATE mode.
    // Mirrors the post-DML cleanup in `execute_differential_refresh`.
    // Run unconditionally — even when this trigger invocation produced an
    // empty delta, a prior invocation in the same statement (or a prior
    // statement) may have left orphans for non-monotone predicates.
    run_immediate_phantom_cleanup(&st, &st_qualified)?;

    // Update data_timestamp so downstream stream tables that compare
    // upstream timestamps (ST-on-ST) detect the change.
    if delta_count > 0 {
        let now = Spi::get_one::<pgrx::datum::TimestampWithTimeZone>("SELECT now()")
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            .ok_or_else(|| PgTrickleError::InternalError("now() returned NULL".into()))?;
        StreamTableMeta::update_after_refresh(pgt_id, now, delta_count)?;
    }

    pgrx::debug1!(
        "[pg_trickle] IVM delta applied for pgt_id={}, source_oid={}, delta_rows={}",
        pgt_id,
        source_oid_u32,
        delta_count,
    );

    Ok(())
}

/// PERF-4 (v0.31.0): SQL-callable function: apply IVM delta using ENR names.
///
/// ENR variant of `pgt_ivm_apply_delta`. Called from AFTER trigger bodies
/// when `pg_trickle.ivm_use_enr = true`. References the ephemeral named
/// relations (ENRs) `__pgt_newtable` / `__pgt_oldtable` directly, eliminating
/// the `CREATE TEMP TABLE … AS SELECT * FROM __pgt_newtable` copy overhead.
///
/// Requires PostgreSQL 18+ which propagates ENRs to nested SPI calls within
/// trigger execution contexts.
#[pg_extern(schema = "pgtrickle")]
fn pgt_ivm_apply_delta_enr(
    pgt_id: i64,
    source_oid: i32,
    has_new: bool,
    has_old: bool,
) -> Result<(), PgTrickleError> {
    use crate::catalog::StreamTableMeta;

    ensure_ivm_abort_callback_registered();

    Spi::run("SET LOCAL pg_trickle.internal_refresh = 'true'")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    let source_oid_u32 = source_oid as u32;

    let st = StreamTableMeta::get_by_id(pgt_id)?.ok_or_else(|| {
        PgTrickleError::NotFound(format!("Stream table with pgt_id={pgt_id} not found"))
    })?;

    if st.topk_limit.is_some() {
        return apply_topk_micro_refresh(&st);
    }

    prepare_ivm_owner_transition_tables(&st, source_oid_u32, has_new, has_old, true)?;

    // Owner execution uses the filtered OID-suffixed copies staged above.
    let (delta_sql, user_columns) =
        get_or_compute_ivm_delta(pgt_id, source_oid_u32, has_new, has_old, &st, false)?;

    let st_qualified = format!(
        "\"{}\".\"{}\"",
        st.pgt_schema.replace('"', "\"\""),
        st.pgt_name.replace('"', "\"\""),
    );

    let delta_table = format!("__pgt_ivm_delta_enr_{pgt_id}");
    let delta_count = apply_ivm_owner_delta(&st, &delta_sql, &user_columns, &delta_table)?;
    drop_ivm_transition_copies(source_oid_u32, has_new, has_old);

    // EC-01b: reconcile cross-cycle phantom rows in IMMEDIATE mode (ENR path).
    run_immediate_phantom_cleanup(&st, &st_qualified)?;

    if delta_count > 0 {
        let now = Spi::get_one::<pgrx::datum::TimestampWithTimeZone>("SELECT now()")
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            .ok_or_else(|| PgTrickleError::InternalError("now() returned NULL".into()))?;
        StreamTableMeta::update_after_refresh(pgt_id, now, delta_count)?;
    }

    pgrx::debug1!(
        "[pg_trickle] IVM delta (ENR) applied for pgt_id={}, source_oid={}, delta_rows={}",
        pgt_id,
        source_oid_u32,
        delta_count,
    );

    Ok(())
}

/// Task 5.2: TopK micro-refresh for IMMEDIATE mode.
///
/// Instead of delta-based incremental maintenance, recompute the top-K rows
/// using the full defining query (with ORDER BY + LIMIT) and diff against
/// the current stream table contents. This is a "micro-full-refresh" scoped
/// to K rows — fast enough for inline trigger execution when K is small.
fn apply_topk_micro_refresh(st: &crate::catalog::StreamTableMeta) -> Result<(), PgTrickleError> {
    let st_qualified = format!(
        "\"{}\".\"{}\"",
        st.pgt_schema.replace('"', "\"\""),
        st.pgt_name.replace('"', "\"\""),
    );

    let topk_limit = st.topk_limit.ok_or_else(|| {
        PgTrickleError::InternalError("TopK micro-refresh called on non-TopK stream table".into())
    })?;
    let topk_order = st.topk_order_by.as_deref().ok_or_else(|| {
        PgTrickleError::InternalError("TopK stream table missing order_by metadata".into())
    })?;

    // Build the full TopK query from the stored defining query + metadata.
    let topk_query = if let Some(offset) = st.topk_offset {
        format!(
            "{} ORDER BY {} LIMIT {} OFFSET {}",
            st.defining_query, topk_order, topk_limit, offset
        )
    } else {
        format!(
            "{} ORDER BY {} LIMIT {}",
            st.defining_query, topk_order, topk_limit
        )
    };

    let row_id_expr = crate::dvm::row_id_expr_for_query(&st.defining_query);
    let columns = crate::dvm::get_defining_query_columns(&st.defining_query)?;

    // Materialize the new top-K into a temp table.
    let new_topk = format!("__pgt_ivm_topk_{}", st.pgt_id);
    let topk_select =
        format!("SELECT {row_id_expr} AS __pgt_row_id, sub.* FROM ({topk_query}) sub");
    crate::refresh::prepare_owner_temp_table(st, &new_topk, &topk_select)?;
    let result = crate::refresh::with_stream_owner(st, || {
        Spi::run(&format!("INSERT INTO {new_topk} {topk_select}")) // nosemgrep: rust.spi.run.dynamic-format — table is numeric-ID-derived and topk_select is validated defining SQL.
            .map_err(|e| {
                PgTrickleError::SpiError(format!("TopK micro-refresh materialize failed: {e}"))
            })?;

        // DELETE rows that left the top-K.
        let delete_sql = format!(
            "DELETE FROM {st_qualified} AS t \
         WHERE NOT EXISTS (\
             SELECT 1 FROM {new_topk} AS n WHERE n.__pgt_row_id = t.__pgt_row_id\
         )"
        );
        Spi::run(&delete_sql).map_err(|e| {
            PgTrickleError::SpiError(format!("TopK micro-refresh DELETE failed: {e}"))
        })?;

        // Build column lists for INSERT.
        let col_list = columns
            .iter()
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");
        let n_col_list = columns
            .iter()
            .map(|c| format!("n.\"{}\"", c.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");
        let update_set = columns
            .iter()
            .map(|c| {
                let q = format!("\"{}\"", c.replace('"', "\"\""));
                format!("{q} = EXCLUDED.{q}")
            })
            .collect::<Vec<_>>()
            .join(", ");

        // INSERT new rows that entered the top-K (or update changed rows).
        let insert_sql = format!(
            "INSERT INTO {st_qualified} (__pgt_row_id, {col_list}) \
         SELECT n.__pgt_row_id, {n_col_list} \
         FROM {new_topk} n \
         ON CONFLICT (__pgt_row_id) DO UPDATE SET {update_set}"
        );
        Spi::run(&insert_sql) // nosemgrep: rust.spi.run.dynamic-format — SQL contains validated internal relation identifiers.
            .map_err(|e| {
                PgTrickleError::SpiError(format!("TopK micro-refresh INSERT failed: {e}"))
            })?;

        Ok(())
    });
    crate::refresh::drop_owner_temp_table(&new_topk);
    result?;

    pgrx::debug1!(
        "[pg_trickle] TopK micro-refresh applied for pgt_id={}",
        st.pgt_id,
    );

    Ok(())
}

/// Get or compute the IVM delta SQL for a given trigger invocation.
///
/// Uses a thread-local cache to avoid re-parsing and re-differentiating
/// the defining query on every trigger invocation. Cache entries are
/// keyed by (pgt_id, source_oid, has_new, has_old, use_enr) and invalidated when
/// the defining query changes or the shared cache generation advances.
///
/// When `use_enr = true` (PERF-4), the generated SQL references the ENR names
/// `__pgt_newtable` / `__pgt_oldtable` directly. When `false`, it uses the
/// OID-suffixed temp-table names `__pgt_newtable_{source_oid}` / etc.
fn get_or_compute_ivm_delta(
    pgt_id: i64,
    source_oid: u32,
    has_new: bool,
    has_old: bool,
    st: &crate::catalog::StreamTableMeta,
    use_enr: bool,
) -> Result<(String, Vec<String>), PgTrickleError> {
    use crate::dvm::diff::{DeltaSource, DiffContext, TransitionTableNames};
    use crate::dvm::parser::parse_defining_query_full;
    use crate::version::Frontier;

    let defining_query = &st.defining_query;
    let query_hash = hash_str(defining_query);

    let cache_key = IvmCacheKey {
        pgt_id,
        source_oid,
        has_new,
        has_old,
        use_enr,
    };

    // Cross-session invalidation: flush if the shared generation counter
    // has advanced past our local snapshot.
    let shared_gen = crate::shmem::current_cache_generation();
    LOCAL_IVM_CACHE_GEN.with(|local| {
        if local.get() < shared_gen {
            IVM_DELTA_CACHE.with(|cache| cache.borrow_mut().clear());
            local.set(shared_gen);
        }
    });

    // Check the thread-local cache.
    let cached = IVM_DELTA_CACHE.with(|cache| {
        let map = cache.borrow();
        map.get(&cache_key)
            .filter(|entry| entry.defining_query_hash == query_hash)
            .cloned()
    });

    if let Some(entry) = cached {
        return Ok((entry.delta_sql, entry.user_columns));
    }

    // Cache miss — parse, differentiate, and cache.
    let result = parse_defining_query_full(defining_query).map_err(|e| {
        PgTrickleError::InternalError(format!("Failed to parse defining query for IVM: {e}"))
    })?;
    let op_tree = result.tree;
    let cte_registry = result.cte_registry;

    // Build transition table names.
    // PERF-4: when use_enr = true, reference the ENR names directly.
    let mut tables = HashMap::new();
    tables.insert(
        source_oid,
        TransitionTableNames {
            new_name: if has_new {
                if use_enr {
                    Some("__pgt_newtable".to_string())
                } else {
                    Some(format!("__pgt_newtable_{source_oid}"))
                }
            } else {
                None
            },
            old_name: if has_old {
                if use_enr {
                    Some("__pgt_oldtable".to_string())
                } else {
                    Some(format!("__pgt_oldtable_{source_oid}"))
                }
            } else {
                None
            },
        },
    );

    // Create a DiffContext with transition table source.
    let dummy_frontier = Frontier::default();
    let st_user_cols = op_tree.output_columns();
    let has_pgt_count = op_tree.needs_pgt_count();
    let mut ctx = DiffContext::new(dummy_frontier.clone(), dummy_frontier)
        .with_delta_source(DeltaSource::TransitionTable { tables })
        .with_pgt_name(&st.pgt_schema, &st.pgt_name)
        .with_cte_registry(cte_registry)
        .with_defining_query(defining_query);
    ctx.st_user_columns = Some(st_user_cols);
    ctx.st_has_pgt_count = has_pgt_count;

    // Differentiate the operator tree to get delta SQL.
    let (delta_sql, user_columns, _is_dedup, _has_key_changed) =
        ctx.differentiate_with_columns(&op_tree)?;

    // Store in cache.
    // STAB-2 (v0.30.0): Clock-style eviction respecting `template_cache_max_entries`.
    IVM_DELTA_CACHE.with(|cache| {
        let max_entries = crate::config::pg_trickle_template_cache_max_entries();
        let clock = IVM_CACHE_CLOCK.with(|c| {
            let v = c.get().wrapping_add(1);
            c.set(v);
            v
        });
        let mut map = cache.borrow_mut();
        // Evict the oldest entry (smallest `last_used`) when the cache is full.
        if max_entries > 0
            && map.len() >= max_entries as usize
            && let Some(oldest_key) = map
                .iter()
                .min_by_key(|(_, v)| v.last_used)
                .map(|(k, _)| k.clone())
        {
            map.remove(&oldest_key);
        }
        map.insert(
            cache_key,
            CachedIvmDelta {
                defining_query_hash: query_hash,
                delta_sql: delta_sql.clone(),
                user_columns: user_columns.clone(),
                last_used: clock,
            },
        );
    });

    Ok((delta_sql, user_columns))
}

/// SQL-callable function: handle TRUNCATE on a base table for an
/// IMMEDIATE-mode stream table.
///
/// Truncates the stream table (equivalent to a full refresh with empty
/// base table for simple views).
#[pg_extern(schema = "pgtrickle")]
fn pgt_ivm_handle_truncate(pgt_id: i64) -> Result<(), PgTrickleError> {
    use crate::catalog::StreamTableMeta;

    let st = StreamTableMeta::get_by_id(pgt_id)?.ok_or_else(|| {
        PgTrickleError::NotFound(format!("Stream table with pgt_id={pgt_id} not found"))
    })?;

    // EC-25/EC-26: Set the internal_refresh flag so DML guard triggers
    // allow the IVM executor to modify the storage table.
    Spi::run("SET LOCAL pg_trickle.internal_refresh = 'true'")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    // For TRUNCATE, we do a full refresh: truncate the ST and re-populate.
    let st_qualified = format!(
        "\"{}\".\"{}\"",
        st.pgt_schema.replace('"', "\"\""),
        st.pgt_name.replace('"', "\"\""),
    );

    crate::refresh::with_stream_owner(&st, || {
        // Truncate the stream table.
        Spi::run(&format!("TRUNCATE {st_qualified}")) // nosemgrep: rust.spi.run.dynamic-format — relation is quote_ident()-escaped.
            .map_err(|e| PgTrickleError::SpiError(format!("IVM TRUNCATE failed: {e}")))?;

        // Re-populate from the defining query (which now reads from the
        // truncated base table — i.e., produces no/fewer rows).
        {
            let defining_query = &st.defining_query;
            // Get the user columns from the stream table.
            let col_info = Spi::connect(|client| {
                let query = format!(
                    "SELECT attname::text FROM pg_attribute
                 WHERE attrelid = {}::oid AND attnum > 0
                   AND NOT attisdropped AND attname != '__pgt_row_id'
                 ORDER BY attnum",
                    st.pgt_relid.to_u32(),
                );
                let table = client.select(&query, None, &[])?;
                let mut cols = Vec::new();
                for row in table {
                    if let Some(name) = row.get::<String>(1)? {
                        cols.push(name);
                    }
                }
                Ok::<Vec<String>, pgrx::spi::SpiError>(cols)
            })
            .map_err(|e| PgTrickleError::SpiError(format!("Failed to get ST columns: {e}")))?;

            if !col_info.is_empty() {
                let col_list = col_info
                    .iter()
                    .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
                    .collect::<Vec<_>>()
                    .join(", ");

                // Re-populate: INSERT with hash-based row_id.
                let hash_cols: Vec<String> = col_info
                    .iter()
                    .map(|c| format!("(\"{}\")::TEXT", c.replace('"', "\"\"")))
                    .collect();

                let row_id_expr = if hash_cols.len() == 1 {
                    format!("pgtrickle.pg_trickle_hash({})", hash_cols[0])
                } else {
                    crate::hash::build_composite_hash_expr(&hash_cols)
                };

                let repopulate_sql = format!(
                    "INSERT INTO {st_qualified} (__pgt_row_id, {col_list})
                 SELECT {row_id_expr}, {col_list} FROM ({defining_query}) __pgt_base"
                );
                Spi::run(&repopulate_sql).map_err(|e| {
                    PgTrickleError::SpiError(format!("IVM repopulate after TRUNCATE failed: {e}"))
                })?;
            }
        }
        Ok(())
    })?;

    pgrx::log!("[pg_trickle] IVM TRUNCATE handled for pgt_id={}", pgt_id,);

    Ok(())
}

// ── Extracted SQL Builders (unit-testable) ──────────────────────────────────

/// Build the DELETE SQL for IVM delta application.
///
/// Keyless sources use counted-DELETE (ROW_NUMBER matching) to avoid
/// deleting ALL duplicates when only a subset should be removed.
fn build_ivm_delete_sql(st_qualified: &str, delta_table: &str, has_keyless_source: bool) -> String {
    if has_keyless_source {
        format!(
            "DELETE FROM {st_qualified} \
             WHERE ctid IN (\
               SELECT numbered_st.st_ctid \
               FROM (\
                 SELECT st2.ctid AS st_ctid, \
                        st2.__pgt_row_id, \
                        ROW_NUMBER() OVER (\
                          PARTITION BY st2.__pgt_row_id ORDER BY st2.ctid\
                        ) AS st_rn \
                 FROM {st_qualified} st2 \
                 WHERE st2.__pgt_row_id IN (\
                   SELECT DISTINCT __pgt_row_id \
                   FROM {delta_table} \
                   WHERE __pgt_action = 'D'\
                 )\
               ) numbered_st \
               JOIN (\
                 SELECT __pgt_row_id, \
                        COUNT(*)::INT AS del_count \
                 FROM {delta_table} \
                 WHERE __pgt_action = 'D' \
                 GROUP BY __pgt_row_id\
               ) dc ON numbered_st.__pgt_row_id = dc.__pgt_row_id \
               WHERE numbered_st.st_rn <= dc.del_count\
             )"
        )
    } else {
        format!(
            "DELETE FROM {st_qualified} AS t
                 USING {delta_table} AS d
                 WHERE d.__pgt_action = 'D'
                   AND t.__pgt_row_id = d.__pgt_row_id"
        )
    }
}

/// Build the INSERT SQL for IVM delta application.
///
/// Keyless sources use plain INSERT (no ON CONFLICT) since duplicate
/// `__pgt_row_id` values are expected.
fn build_ivm_insert_sql(
    st_qualified: &str,
    delta_table: &str,
    user_columns: &[String],
    has_keyless_source: bool,
) -> String {
    let col_list = user_columns
        .iter()
        .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");

    let d_col_list = user_columns
        .iter()
        .map(|c| format!("d.\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");

    if has_keyless_source {
        format!(
            "INSERT INTO {st_qualified} (__pgt_row_id, {col_list})
                 SELECT d.__pgt_row_id, {d_col_list}
                 FROM {delta_table} d
                 WHERE d.__pgt_action = 'I'"
        )
    } else {
        let update_set = user_columns
            .iter()
            .map(|c| {
                let quoted = format!("\"{}\"", c.replace('"', "\"\""));
                format!("{quoted} = EXCLUDED.{quoted}")
            })
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "INSERT INTO {st_qualified} (__pgt_row_id, {col_list})
                 SELECT d.__pgt_row_id, {d_col_list}
                 FROM {delta_table} d
                 WHERE d.__pgt_action = 'I'
                 ON CONFLICT (__pgt_row_id) DO UPDATE SET {update_set}"
        )
    }
}

/// Build the three column-list fragments used in IVM delta application:
/// `(col_list, d_col_list, update_set)`.
fn build_column_lists(user_columns: &[String]) -> (String, String, String) {
    let col_list = user_columns
        .iter()
        .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");

    let d_col_list = user_columns
        .iter()
        .map(|c| format!("d.\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");

    let update_set = user_columns
        .iter()
        .map(|c| {
            let quoted = format!("\"{}\"", c.replace('"', "\"\""));
            format!("{quoted} = EXCLUDED.{quoted}")
        })
        .collect::<Vec<_>>()
        .join(", ");

    (col_list, d_col_list, update_set)
}

// ── Unit Tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── hash_str tests ──────────────────────────────────────────────

    #[test]
    fn test_hash_str_deterministic() {
        let h1 = hash_str("SELECT * FROM t");
        let h2 = hash_str("SELECT * FROM t");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_hash_str_different_inputs() {
        let h1 = hash_str("SELECT * FROM t");
        let h2 = hash_str("SELECT * FROM u");
        assert_ne!(h1, h2);
    }

    // ── is_simple_scan_chain tests ──────────────────────────────────

    #[test]
    fn test_scan_is_simple_chain() {
        use crate::dvm::parser::{Column, OpTree};
        let tree = OpTree::Scan {
            table_oid: 1,
            table_name: "t".into(),
            schema: "public".into(),
            columns: vec![Column {
                name: "id".into(),
                type_oid: 23,
                is_nullable: false,
            }],
            pk_columns: vec!["id".into()],
            alias: "t".into(),
        };
        assert!(IvmLockMode::is_simple_scan_chain(&tree));
    }

    #[test]
    fn test_filter_over_scan_is_simple_chain() {
        use crate::dvm::parser::{Column, Expr, OpTree};
        let tree = OpTree::Filter {
            predicate: Expr::Literal("true".into()),
            child: Box::new(OpTree::Scan {
                table_oid: 1,
                table_name: "t".into(),
                schema: "public".into(),
                columns: vec![Column {
                    name: "id".into(),
                    type_oid: 23,
                    is_nullable: false,
                }],
                pk_columns: vec![],
                alias: "t".into(),
            }),
        };
        assert!(IvmLockMode::is_simple_scan_chain(&tree));
    }

    #[test]
    fn test_project_over_filter_over_scan_is_simple_chain() {
        use crate::dvm::parser::{Column, Expr, OpTree};
        let scan = OpTree::Scan {
            table_oid: 1,
            table_name: "t".into(),
            schema: "public".into(),
            columns: vec![Column {
                name: "id".into(),
                type_oid: 23,
                is_nullable: false,
            }],
            pk_columns: vec![],
            alias: "t".into(),
        };
        let filter = OpTree::Filter {
            predicate: Expr::Literal("true".into()),
            child: Box::new(scan),
        };
        let project = OpTree::Project {
            expressions: vec![],
            aliases: vec![],
            child: Box::new(filter),
        };
        assert!(IvmLockMode::is_simple_scan_chain(&project));
    }

    #[test]
    fn test_aggregate_is_not_simple_chain() {
        use crate::dvm::parser::{Column, OpTree};
        let scan = OpTree::Scan {
            table_oid: 1,
            table_name: "t".into(),
            schema: "public".into(),
            columns: vec![Column {
                name: "id".into(),
                type_oid: 23,
                is_nullable: false,
            }],
            pk_columns: vec![],
            alias: "t".into(),
        };
        let agg = OpTree::Aggregate {
            group_by: vec![],
            aggregates: vec![],
            child: Box::new(scan),
        };
        assert!(!IvmLockMode::is_simple_scan_chain(&agg));
    }

    #[test]
    fn test_join_is_not_simple_chain() {
        use crate::dvm::parser::{Column, Expr, OpTree};
        let scan_a = OpTree::Scan {
            table_oid: 1,
            table_name: "a".into(),
            schema: "public".into(),
            columns: vec![Column {
                name: "id".into(),
                type_oid: 23,
                is_nullable: false,
            }],
            pk_columns: vec![],
            alias: "a".into(),
        };
        let scan_b = OpTree::Scan {
            table_oid: 2,
            table_name: "b".into(),
            schema: "public".into(),
            columns: vec![Column {
                name: "id".into(),
                type_oid: 23,
                is_nullable: false,
            }],
            pk_columns: vec![],
            alias: "b".into(),
        };
        let join = OpTree::InnerJoin {
            condition: Expr::Literal("true".into()),
            left: Box::new(scan_a),
            right: Box::new(scan_b),
        };
        assert!(!IvmLockMode::is_simple_scan_chain(&join));
    }

    #[test]
    fn test_distinct_is_not_simple_chain() {
        use crate::dvm::parser::{Column, OpTree};
        let scan = OpTree::Scan {
            table_oid: 1,
            table_name: "t".into(),
            schema: "public".into(),
            columns: vec![Column {
                name: "id".into(),
                type_oid: 23,
                is_nullable: false,
            }],
            pk_columns: vec![],
            alias: "t".into(),
        };
        let distinct = OpTree::Distinct {
            child: Box::new(scan),
        };
        assert!(!IvmLockMode::is_simple_scan_chain(&distinct));
    }

    // ── build_ivm_delete_sql tests ──────────────────────────────────

    #[test]
    fn test_keyed_delete_uses_simple_join() {
        let sql = build_ivm_delete_sql(r#""public"."my_st""#, "__delta_1", false);
        assert!(sql.contains("USING"), "keyed DELETE should use USING");
        assert!(
            sql.contains("__pgt_action = 'D'"),
            "should filter on action D"
        );
        assert!(
            !sql.contains("ROW_NUMBER"),
            "keyed DELETE should NOT use ROW_NUMBER"
        );
    }

    #[test]
    fn test_keyless_delete_uses_row_number() {
        let sql = build_ivm_delete_sql(r#""public"."my_st""#, "__delta_1", true);
        assert!(
            sql.contains("ROW_NUMBER"),
            "keyless DELETE should use ROW_NUMBER"
        );
        assert!(
            sql.contains("st_rn <= dc.del_count"),
            "should limit deletes by count"
        );
        assert!(
            sql.contains("__pgt_action = 'D'"),
            "should filter on action D"
        );
    }

    #[test]
    fn test_delete_sql_includes_table_names() {
        let sql = build_ivm_delete_sql(r#""myschema"."orders_st""#, "__delta_42", false);
        assert!(sql.contains(r#""myschema"."orders_st""#));
        assert!(sql.contains("__delta_42"));
    }

    // ── build_ivm_insert_sql tests ──────────────────────────────────

    #[test]
    fn test_keyed_insert_uses_on_conflict() {
        let cols = vec!["id".to_string(), "val".to_string()];
        let sql = build_ivm_insert_sql(r#""public"."st""#, "__delta", &cols, false);
        assert!(
            sql.contains("ON CONFLICT (__pgt_row_id) DO UPDATE SET"),
            "keyed INSERT should use ON CONFLICT"
        );
        assert!(sql.contains(r#""id""#));
        assert!(sql.contains(r#""val""#));
        assert!(sql.contains(r#""id" = EXCLUDED."id""#));
    }

    #[test]
    fn test_keyless_insert_no_on_conflict() {
        let cols = vec!["name".to_string()];
        let sql = build_ivm_insert_sql(r#""public"."st""#, "__delta", &cols, true);
        assert!(
            !sql.contains("ON CONFLICT"),
            "keyless INSERT should NOT have ON CONFLICT"
        );
        assert!(sql.contains("__pgt_action = 'I'"));
    }

    #[test]
    fn test_insert_sql_quotes_special_chars() {
        let cols = vec![r#"col"name"#.to_string()];
        let sql = build_ivm_insert_sql(r#""public"."st""#, "__delta", &cols, false);
        assert!(
            sql.contains(r#""col""name""#),
            "should double-quote embedded quotes: {sql}"
        );
    }

    // ── build_column_lists tests ────────────────────────────────────

    #[test]
    fn test_column_lists_basic() {
        let cols = vec!["id".to_string(), "val".to_string()];
        let (col_list, d_col_list, update_set) = build_column_lists(&cols);
        assert_eq!(col_list, r#""id", "val""#);
        assert_eq!(d_col_list, r#"d."id", d."val""#);
        assert!(update_set.contains(r#""id" = EXCLUDED."id""#));
        assert!(update_set.contains(r#""val" = EXCLUDED."val""#));
    }

    #[test]
    fn test_column_lists_empty() {
        let (col_list, d_col_list, update_set) = build_column_lists(&[]);
        assert_eq!(col_list, "");
        assert_eq!(d_col_list, "");
        assert_eq!(update_set, "");
    }

    #[test]
    fn test_column_lists_special_chars() {
        let cols = vec![r#"my"col"#.to_string()];
        let (col_list, d_col_list, _) = build_column_lists(&cols);
        assert_eq!(col_list, r#""my""col""#);
        assert_eq!(d_col_list, r#"d."my""col""#);
    }

    // ── IvmTriggerNames tests ───────────────────────────────────────

    #[test]
    fn test_trigger_names_before_triggers() {
        let names = IvmTriggerNames::new(42, 12345);
        assert_eq!(names.before_insert, "pgt_ivm_before_insert_42");
        assert_eq!(names.before_update, "pgt_ivm_before_update_42");
        assert_eq!(names.before_delete, "pgt_ivm_before_delete_42");
        assert_eq!(names.before_trunc, "pgt_ivm_before_trunc_42");
    }

    #[test]
    fn test_trigger_names_after_triggers() {
        let names = IvmTriggerNames::new(42, 12345);
        assert_eq!(names.after_ins, "pgt_ivm_after_ins_42");
        assert_eq!(names.after_upd, "pgt_ivm_after_upd_42");
        assert_eq!(names.after_del, "pgt_ivm_after_del_42");
        assert_eq!(names.after_trunc, "pgt_ivm_after_trunc_42");
    }

    #[test]
    fn test_trigger_names_functions_include_both_ids() {
        let names = IvmTriggerNames::new(7, 99999);
        assert_eq!(names.before_fn, "pgtrickle.pgt_ivm_before_fn_7_99999");
        assert_eq!(names.after_ins_fn, "pgtrickle.pgt_ivm_after_ins_fn_7_99999");
        assert_eq!(names.after_upd_fn, "pgtrickle.pgt_ivm_after_upd_fn_7_99999");
        assert_eq!(names.after_del_fn, "pgtrickle.pgt_ivm_after_del_fn_7_99999");
        assert_eq!(
            names.after_trunc_fn,
            "pgtrickle.pgt_ivm_after_trunc_fn_7_99999"
        );
    }

    #[test]
    fn test_trigger_names_all_triggers_returns_8() {
        let names = IvmTriggerNames::new(1, 100);
        let all = names.all_triggers();
        assert_eq!(all.len(), 8);
        // Verify no duplicates
        let set: std::collections::HashSet<&str> = all.iter().copied().collect();
        assert_eq!(set.len(), 8);
    }

    #[test]
    fn test_trigger_names_all_functions_returns_5() {
        let names = IvmTriggerNames::new(1, 100);
        let all = names.all_functions();
        assert_eq!(all.len(), 5);
        let set: std::collections::HashSet<&str> = all.iter().copied().collect();
        assert_eq!(set.len(), 5);
    }

    #[test]
    fn test_trigger_names_different_pgt_ids_differ() {
        let a = IvmTriggerNames::new(1, 100);
        let b = IvmTriggerNames::new(2, 100);
        assert_ne!(a.before_insert, b.before_insert);
        assert_ne!(a.after_ins_fn, b.after_ins_fn);
    }

    #[test]
    fn test_trigger_names_different_oids_differ() {
        let a = IvmTriggerNames::new(1, 100);
        let b = IvmTriggerNames::new(1, 200);
        // Trigger names only depend on pgt_id, so they match
        assert_eq!(a.before_insert, b.before_insert);
        // Function names include both IDs, so they differ
        assert_ne!(a.before_fn, b.before_fn);
    }

    #[test]
    fn test_trigger_names_all_triggers_matches_fields() {
        let names = IvmTriggerNames::new(5, 777);
        let all = names.all_triggers();
        assert!(all.contains(&names.before_insert.as_str()));
        assert!(all.contains(&names.before_update.as_str()));
        assert!(all.contains(&names.before_delete.as_str()));
        assert!(all.contains(&names.before_trunc.as_str()));
        assert!(all.contains(&names.after_ins.as_str()));
        assert!(all.contains(&names.after_upd.as_str()));
        assert!(all.contains(&names.after_del.as_str()));
        assert!(all.contains(&names.after_trunc.as_str()));
    }

    #[test]
    fn test_trigger_names_all_functions_matches_fields() {
        let names = IvmTriggerNames::new(5, 777);
        let all = names.all_functions();
        assert!(all.contains(&names.before_fn.as_str()));
        assert!(all.contains(&names.after_ins_fn.as_str()));
        assert!(all.contains(&names.after_upd_fn.as_str()));
        assert!(all.contains(&names.after_del_fn.as_str()));
        assert!(all.contains(&names.after_trunc_fn.as_str()));
    }
}
