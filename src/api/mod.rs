//! User-facing SQL API functions for pgtrickle.
//!
//! All functions are exposed in the `pg_trickle` schema and provide the primary
//! interface for creating, altering, dropping, and refreshing stream tables.

use pgrx::prelude::*;
use serde::Deserialize;
use std::collections::HashSet;
use std::time::Instant;

use crate::catalog::{CdcMode, RefreshRecord, StDependency, StreamTableMeta};
use crate::cdc;
use crate::config;
use crate::dag::{
    DagNode, DiamondConsistency, DiamondSchedulePolicy, NodeId, RefreshMode, StDag, StStatus,
};
use crate::error::PgTrickleError;
use crate::refresh;
use crate::shmem;
use crate::template_cache;
use crate::version;
use crate::wal_decoder;

pub(crate) mod outbox;
pub(crate) mod publication;
pub(crate) mod scheduler_control;
pub(crate) mod spec;
pub(crate) mod validation;

// ── G13-EH: Enriched error reporting ────────────────────────────────────────

/// UX-7: Resolve a relation OID to a human-readable `schema.table` name.
/// Falls back to `OID <n>` if the relation no longer exists.
fn resolve_oid_to_name(oid: u32) -> String {
    Spi::get_one_with_args::<String>(
        "SELECT format('%I.%I', n.nspname, c.relname) \
         FROM pg_catalog.pg_class c \
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.oid = $1",
        &[pgrx::pg_sys::Oid::from(oid).into()],
    )
    .unwrap_or(None)
    .unwrap_or_else(|| format!("OID {}", oid))
}

/// Raise a `PgTrickleError` as a PostgreSQL ERROR, adding DETAIL and HINT
/// fields for well-known error types that benefit from additional context.
///
/// For the four targeted variants (`UnsupportedOperator`, `CycleDetected`,
/// `UpstreamSchemaChanged`, `QueryParseError`) this uses `ErrorReport` with
/// `set_detail` / `set_hint` so the extra information appears in the
/// `DETAIL:` and `HINT:` sections of the PostgreSQL error message.
/// All other error types fall back to `pgrx::error!()`.
#[track_caller]
fn raise_error_with_context(e: PgTrickleError) -> ! {
    use pgrx::pg_sys::panic::ErrorReport;
    use pgrx::{PgLogLevel, PgSqlErrorCode};

    match &e {
        PgTrickleError::UnsupportedOperator(op) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                format!("unsupported operator for DIFFERENTIAL mode: {}", op),
                "",
            )
            .set_detail(
                "DIFFERENTIAL mode supports SELECT, WHERE, simple JOINs (INNER/LEFT/RIGHT), \
                 GROUP BY, HAVING, ORDER BY, and LIMIT. CTEs (WITH …), FULL OUTER JOINs, \
                 window functions without a writable equivalent, and certain aggregate forms \
                 are not supported."
                    .to_string(),
            )
            .set_hint(
                "Use refresh_mode = 'FULL' or refresh_mode = 'AUTO' to handle queries that \
                 contain unsupported constructs. AUTO will try DIFFERENTIAL first and fall \
                 back to FULL when needed."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::CycleDetected(path) => {
            let cycle_str = path.join(" -> ");
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_INVALID_SCHEMA_DEFINITION,
                format!("cycle detected in dependency graph: {}", cycle_str),
                "",
            )
            .set_detail(
                "Stream tables form a directed acyclic graph (DAG). Adding this stream \
                 table would introduce a cycle, which is not allowed."
                    .to_string(),
            )
            .set_hint(
                "Review the defining query and ensure it does not reference any stream \
                 table that transitively depends on this table. Use \
                 pgtrickle.pgt_dependencies to inspect the current dependency graph."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::UpstreamSchemaChanged(oid) => {
            let table_name = resolve_oid_to_name(*oid);
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_INVALID_TABLE_DEFINITION,
                format!("upstream table schema changed: {}", table_name),
                "",
            )
            .set_detail(
                "An upstream source table was modified (ALTER TABLE) after the stream table \
                 was created. The change buffer schema may no longer match the source."
                    .to_string(),
            )
            .set_hint(
                "Call pgtrickle.refresh_stream_table('<name>') which will automatically \
                 reinitialize the stream table, or call pgtrickle.alter_stream_table(\
                 '<name>') with the updated query."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::QueryParseError(msg) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
                format!("query parse error: {}", msg),
                "",
            )
            .set_detail(
                "The defining query could not be validated or rewritten. This can happen \
                 when the query references objects that do not exist yet, uses types \
                 incompatible with the storage schema, or triggers a validation check."
                    .to_string(),
            )
            .set_hint(
                "Verify the query runs successfully as a standalone SELECT before creating \
                 the stream table. Check that all referenced tables and columns exist."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        // UX-3: Enriched reporting for NotFound errors.
        PgTrickleError::NotFound(name) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_UNDEFINED_TABLE,
                format!("stream table not found: {}", name),
                "",
            )
            .set_hint(
                "Use pgtrickle.pgt_status() to list existing stream tables. \
                 Ensure the name is schema-qualified (e.g., 'public.my_table')."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        // UX-3: Enriched reporting for AlreadyExists errors.
        PgTrickleError::AlreadyExists(name) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_DUPLICATE_TABLE,
                format!("stream table already exists: {}", name),
                "",
            )
            .set_hint(
                "Use pgtrickle.alter_stream_table() to modify an existing stream table, \
                 or pgtrickle.drop_stream_table() to remove it first."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        // UX-3: Enriched reporting for InvalidArgument errors.
        PgTrickleError::InvalidArgument(msg) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
                format!("invalid argument: {}", msg),
                "",
            )
            .set_hint(
                "See docs/SQL_REFERENCE.md for valid parameter values and syntax.".to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        // UX-3: Enriched reporting for LockTimeout errors.
        PgTrickleError::LockTimeout(msg) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_LOCK_NOT_AVAILABLE,
                format!("lock timeout: {}", msg),
                "",
            )
            .set_hint(
                "The stream table may be locked by a concurrent refresh. Retry after \
                 the current refresh completes, or increase lock_timeout."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        // UX-3: Enriched reporting for QueryTooComplex errors.
        PgTrickleError::QueryTooComplex(msg) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED,
                format!("query too complex: {}", msg),
                "",
            )
            .set_hint(
                "Simplify the defining query or use refresh_mode = 'FULL'. \
                 The differential engine has limits on join depth and subquery nesting."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        // SEC-1: Ownership check on drop/alter stream table.
        PgTrickleError::PermissionDenied(msg) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE,
                format!("permission denied: {}", msg),
                "",
            )
            .set_hint(
                "Only the owner of the stream table's storage table (or a superuser) \
                 can drop or alter the stream table."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        // STAB-6: SQLSTATE coverage for remaining error variants.
        PgTrickleError::TypeMismatch(msg) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH,
                format!("type mismatch: {}", msg),
                "",
            )
            .set_hint(
                "Check that all column types in the defining query are compatible \
                 with the stream table schema. Explicit casts may be needed."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::UpstreamTableDropped(oid) => {
            let table_name = resolve_oid_to_name(*oid);
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_UNDEFINED_TABLE,
                format!("upstream table dropped: {}", table_name),
                "",
            )
            .set_detail(
                "A source table referenced by this stream table has been dropped.".to_string(),
            )
            .set_hint(
                "Drop the stream table with pgtrickle.drop_stream_table() or \
                 recreate it with an updated query."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::ReplicationSlotError(msg) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                format!("replication slot error: {}", msg),
                "",
            )
            .set_hint(
                "Ensure the replication slot exists and is not in use by another \
                 consumer. Check pg_replication_slots for details."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::WalTransitionError(msg) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                format!("WAL transition error: {}", msg),
                "",
            )
            .set_hint(
                "The trigger-to-WAL CDC transition could not complete. Check \
                 wal_level = 'logical' and that the replication slot is healthy."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::SpiError(msg) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("SPI error: {}", msg),
                "",
            )
            .set_hint(
                "An internal query failed. This may be transient — retry the \
                 operation. If it persists, check the PostgreSQL log for details."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::SpiErrorCode(code, msg) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("SPI error [{}]: {}", code, msg),
                "",
            )
            .set_hint(
                "An internal query failed with a specific SQLSTATE code. \
                 This may be transient — retry the operation. \
                 If it persists, check the PostgreSQL log for details."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::SpiPermissionError(msg) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE,
                format!("SPI permission error: {}", msg),
                "",
            )
            .set_detail(
                "The background worker role lacks required privileges on a \
                 referenced table."
                    .to_string(),
            )
            .set_hint(
                "GRANT SELECT on source tables and INSERT/UPDATE/DELETE on the \
                 stream table to the role running pg_trickle."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::WatermarkBackwardMovement(msg) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
                format!("watermark moved backward: {}", msg),
                "",
            )
            .set_hint(
                "Watermarks must advance monotonically. Ensure the source data \
                 timestamp or LSN is not moving backward."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::WatermarkGroupNotFound(msg) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
                format!("watermark group not found: {}", msg),
                "",
            )
            .set_hint(
                "Check the watermark group name. Use pgtrickle.pgt_watermark_groups() \
                 to list existing groups."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::WatermarkGroupAlreadyExists(msg) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_DUPLICATE_OBJECT,
                format!("watermark group already exists: {}", msg),
                "",
            )
            .set_hint("Choose a different group name or drop the existing group first.".to_string())
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::RefreshSkipped(msg) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                format!("refresh skipped: {}", msg),
                "",
            )
            .set_hint(
                "A concurrent refresh is still running. Wait for it to complete \
                 or check pgtrickle.pgt_status() for the current state."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::InternalError(msg) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("internal error: {}", msg),
                "",
            )
            .set_hint(
                "This is a bug in pg_trickle. Please report it at \
                 https://github.com/trickle-labs/pg-trickle/issues with the full error \
                 message and PostgreSQL log output."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::PublicationAlreadyExists(name) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_DUPLICATE_OBJECT,
                format!("publication already exists for stream table '{}'", name),
                "",
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::PublicationNotFound(name) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
                format!("no publication found for stream table '{}'", name),
                "",
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::SlaTooSmall(msg) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
                format!("SLA interval too small: {}", msg),
                "",
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::ChangedColsBitmaskFailed(msg) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("changed-columns bitmask failed: {}", msg),
                "",
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::PublicationRebuildFailed(msg) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("publication rebuild failed: {}", msg),
                "",
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::DiagnosticError(msg) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("diagnostic error: {}", msg),
                "",
            )
            .set_hint(
                "A diagnostic operation failed. Check the PostgreSQL log for details.".to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        // SNAP-1/2/3 (v0.27.0): Snapshot errors
        PgTrickleError::SnapshotAlreadyExists(target) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_DUPLICATE_TABLE,
                format!("snapshot already exists: {}", target),
                "",
            )
            .set_hint(
                "Choose a different target table name or drop the existing snapshot with \
                 pgtrickle.drop_snapshot()."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::SnapshotSourceNotFound(source) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_UNDEFINED_TABLE,
                format!("snapshot source not found: {}", source),
                "",
            )
            .set_hint(
                "Verify the snapshot table exists and is accessible. Use \
                 pgtrickle.list_snapshots() to see available snapshots."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::SnapshotSchemaVersionMismatch(msg) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_INVALID_TABLE_DEFINITION,
                format!("snapshot schema version mismatch: {}", msg),
                "",
            )
            .set_hint(
                "Snapshots taken with a different major version of pg_trickle may not be \
                 compatible. Re-create the snapshot with the current version."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        // v0.46.0: Outbox/pg_tide integration errors
        PgTrickleError::OutboxAlreadyEnabled(name) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_DUPLICATE_OBJECT,
                format!("outbox already attached for stream table: {}", name),
                "",
            )
            .set_hint(
                "Use pgtrickle.detach_outbox() first if you want to re-attach with \
                 different settings."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::OutboxNotEnabled(name) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
                format!("outbox not attached for stream table: {}", name),
                "",
            )
            .set_hint("Use pgtrickle.attach_outbox(name) to attach a pg_tide outbox.".to_string())
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::PgTideMissing => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
                "attach_outbox() requires the pg_tide extension",
                "",
            )
            .set_hint(
                "Install pg_tide first: CREATE EXTENSION pg_tide; \
                 See https://github.com/trickle-labs/pg-tide"
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::UnresolvedPlaceholder { token, context } => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("unresolved placeholder '{}' in SQL for {}", token, context),
                "",
            )
            .set_detail(
                "A delta SQL template still contained a __PGS_*__ or __PGT_*__ token \
                 after all substitution passes completed. This indicates an internal bug \
                 where a source OID or stream table ID was not mapped correctly."
                    .to_string(),
            )
            .set_hint(
                "This is a bug in pg_trickle. Please report it at \
                 https://github.com/trickle-labs/pg-trickle/issues with the full error message."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        // v0.54.0: DVM engine hardening errors.
        PgTrickleError::DiffDepthExceeded(limit) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED,
                format!(
                    "differential query depth exceeded limit of {} levels",
                    limit
                ),
                "",
            )
            .set_hint(
                "Reduce query nesting depth or raise pg_trickle.max_parse_depth. \
                 Alternatively, use refresh_mode = 'FULL'."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::DiffCteCountExceeded(limit) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED,
                format!("differential query CTE count exceeded limit of {}", limit),
                "",
            )
            .set_hint(
                "Simplify the defining query or raise pg_trickle.max_diff_ctes. \
                 Alternatively, use refresh_mode = 'FULL'."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        PgTrickleError::StSourceFrontierMissing(pgt_id) => {
            ErrorReport::new(
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                format!(
                    "upstream stream table (pgt_id={}) not found in refresh frontier",
                    pgt_id
                ),
                "",
            )
            .set_detail(
                "The upstream stream table may have been dropped while this stream table \
                 still references it. The consuming stream table cannot be refreshed \
                 until the dependency is resolved."
                    .to_string(),
            )
            .set_hint(
                "Call pgtrickle.reinitialize_stream_table('<name>') to rebuild the \
                 frontier, or drop and recreate the stream table with an updated query."
                    .to_string(),
            )
            .report(PgLogLevel::ERROR);
            unreachable!()
        }
        _ => pgrx::error!("{}", e),
    }
}

/// Metadata about which query rewrites were applied.
struct RewriteResult {
    /// The rewritten query string.
    query: String,
    /// Whether `rewrite_nested_window_exprs` lifted window functions out of
    /// expressions (e.g. `CASE WHEN ROW_NUMBER() OVER (...) ...`). This
    /// rewrite produces a subquery form that DVM parsing accepts but delta
    /// SQL generation cannot handle — so differential mode must fall back
    /// to full refresh (EC-03).
    had_nested_window_rewrite: bool,
}

/// Run the full query rewrite pipeline: view inlining, nested window
/// expressions, DISTINCT ON, GROUPING SETS, scalar subqueries, SubLinks
/// in OR, and ROWS FROM.
///
/// Returns the rewritten query and metadata about which rewrites fired.
/// The caller should keep the original query separately if needed for
/// `original_query` tracking.
fn run_query_rewrite_pipeline(query: &str) -> Result<RewriteResult, PgTrickleError> {
    // View inlining — must run first so view definitions containing
    // DISTINCT ON, GROUPING SETS, etc. get further rewritten.
    let query = crate::dvm::rewrite_views_inline(query)?;
    // Nested window expression lift — track whether the rewrite fired
    let pre_window = query.clone();
    let query = crate::dvm::rewrite_nested_window_exprs(&query)?;
    let had_nested_window_rewrite = query != pre_window;
    // DISTINCT ON → ROW_NUMBER() window
    let query = crate::dvm::rewrite_distinct_on(&query)?;
    // GROUPING SETS / CUBE / ROLLUP → UNION ALL of GROUP BY
    let query = crate::dvm::rewrite_grouping_sets(&query)?;
    // Scalar subquery in WHERE → CROSS JOIN
    let query = crate::dvm::rewrite_scalar_subquery_in_where(&query)?;
    // Correlated scalar subquery in SELECT → LEFT JOIN
    let query = crate::dvm::rewrite_correlated_scalar_in_select(&query)?;
    // SubLinks inside OR → UNION branches (with De Morgan normalization).
    // Multiple passes handle patterns exposed after the first rewrite,
    // e.g. NOT(AND(…)) → OR(…) → UNION in pass 2.  Cap at 3 iterations.
    let mut query = query;
    for _ in 0..3 {
        let prev = query.clone();
        let q = crate::dvm::rewrite_demorgan_sublinks(&query)?;
        let q = crate::dvm::rewrite_sublinks_in_or(&q)?;
        if q == prev {
            break;
        }
        query = q;
    }
    // ROWS FROM() multi-function rewrite
    let query = crate::dvm::rewrite_rows_from(&query)?;
    Ok(RewriteResult {
        query,
        had_nested_window_rewrite,
    })
}

/// Validated query metadata returned by [`validate_and_parse_query`].
struct ValidatedQuery {
    /// Output columns from `SELECT ... LIMIT 0`.
    columns: Vec<ColumnDef>,
    /// Full DVM parse result (only for DIFFERENTIAL/IMMEDIATE non-TopK).
    parsed_tree: Option<crate::dvm::ParseResult>,
    /// TopK metadata (if ORDER BY + LIMIT pattern detected).
    topk_info: Option<crate::dvm::TopKInfo>,
    /// The effective defining query (base query for TopK, original otherwise).
    effective_query: String,
    /// Whether the query needs `__pgt_count` (aggregate/distinct).
    needs_pgt_count: bool,
    /// Whether the query needs `__pgt_count_l`/`__pgt_count_r` (INTERSECT/EXCEPT).
    needs_dual_count: bool,
    /// Whether the query needs `__pgt_count` for UNION dedup.
    needs_union_dedup: bool,
    /// Whether any source table lacks a PRIMARY KEY (EC-06).
    has_keyless_source: bool,
    /// Source relation OIDs and types extracted from the query.
    source_relids: Vec<(pg_sys::Oid, String)>,
    /// AVG auxiliary columns: `(sum_col_name, count_col_name, arg_sql)` tuples
    /// for algebraic AVG maintenance. Empty if no non-DISTINCT AVG aggregates.
    avg_aux_columns: Vec<(String, String, String)>,
    /// Sum-of-squares auxiliary columns: `(sum2_col_name, arg_sql)` tuples
    /// for algebraic STDDEV/VAR maintenance. Empty if no non-DISTINCT STDDEV/VAR.
    sum2_aux_columns: Vec<(String, String)>,
    /// Cross-product auxiliary columns: `(col_name, arg_sql)` tuples
    /// for algebraic CORR/COVAR/REGR_* maintenance (P3-2).
    /// Columns: sumx, sumy, sumxy, sumx2, sumy2 per aggregate.
    covar_aux_columns: Vec<(String, String)>,
    /// Nonnull-count auxiliary columns: `(nonnull_col_name, arg_sql)` tuples
    /// for non-DISTINCT SUM aggregates above FULL JOIN children (P2-2).
    /// Empty if no such aggregates exist.
    nonnull_aux_columns: Vec<(String, String)>,
    /// PostgreSQL accumulator types for statistical auxiliary columns.
    statistical_aux_types: Vec<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CdcModeRequestSource {
    GlobalGuc,
    ExplicitOverride,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CdcRefreshModeInteraction {
    None,
    IgnoreWalForImmediate,
    RejectWalForImmediate,
}

fn classify_cdc_refresh_mode_interaction(
    refresh_mode: RefreshMode,
    requested_cdc_mode: &str,
    source: CdcModeRequestSource,
) -> CdcRefreshModeInteraction {
    if !refresh_mode.is_immediate() || !requested_cdc_mode.eq_ignore_ascii_case("wal") {
        return CdcRefreshModeInteraction::None;
    }

    match source {
        CdcModeRequestSource::GlobalGuc => CdcRefreshModeInteraction::IgnoreWalForImmediate,
        CdcModeRequestSource::ExplicitOverride => CdcRefreshModeInteraction::RejectWalForImmediate,
    }
}

fn enforce_cdc_refresh_mode_interaction(
    stream_table_name: &str,
    refresh_mode: RefreshMode,
    requested_cdc_mode: &str,
    source: CdcModeRequestSource,
) -> Result<(), PgTrickleError> {
    match classify_cdc_refresh_mode_interaction(refresh_mode, requested_cdc_mode, source) {
        CdcRefreshModeInteraction::None => Ok(()),
        CdcRefreshModeInteraction::IgnoreWalForImmediate => {
            pgrx::info!(
                "pg_trickle: cdc_mode 'wal' has no effect for IMMEDIATE refresh mode on {} — using IVM triggers instead of CDC.",
                stream_table_name,
            );
            Ok(())
        }
        CdcRefreshModeInteraction::RejectWalForImmediate => Err(PgTrickleError::InvalidArgument(
            "refresh_mode = 'IMMEDIATE' is incompatible with cdc_mode = 'wal'. \
             IMMEDIATE uses in-transaction IVM triggers; WAL-based CDC is async. \
             Use cdc_mode = 'trigger' or 'auto', or choose a deferred refresh_mode."
                .to_string(),
        )),
    }
}

fn normalize_requested_cdc_mode(
    requested_cdc_mode: Option<&str>,
) -> Result<Option<String>, PgTrickleError> {
    requested_cdc_mode
        .map(|mode| {
            let normalized = mode.trim().to_lowercase();
            match normalized.as_str() {
                "auto" | "trigger" | "wal" => Ok(normalized),
                other => Err(PgTrickleError::InvalidArgument(format!(
                    "invalid cdc_mode value: '{}' (expected 'auto', 'trigger', or 'wal')",
                    other
                ))),
            }
        })
        .transpose()
}

fn resolve_requested_cdc_mode(
    requested_cdc_mode: Option<&str>,
) -> Result<(Option<String>, String, CdcModeRequestSource), PgTrickleError> {
    let requested_override = normalize_requested_cdc_mode(requested_cdc_mode)?;
    let source = if requested_override.is_some() {
        CdcModeRequestSource::ExplicitOverride
    } else {
        CdcModeRequestSource::GlobalGuc
    };
    let effective = requested_override
        .clone()
        .unwrap_or_else(config::pg_trickle_cdc_mode);
    Ok((requested_override, effective, source))
}

fn resolve_requested_cdc_mode_for_st(
    st: &StreamTableMeta,
    requested_cdc_mode: Option<&str>,
) -> Result<(Option<String>, String, CdcModeRequestSource), PgTrickleError> {
    let requested_override = match normalize_requested_cdc_mode(requested_cdc_mode)? {
        Some(mode) => Some(mode),
        None => st.requested_cdc_mode.clone(),
    };
    let source = if requested_override.is_some() {
        CdcModeRequestSource::ExplicitOverride
    } else {
        CdcModeRequestSource::GlobalGuc
    };
    let effective = requested_override
        .clone()
        .unwrap_or_else(config::pg_trickle_cdc_mode);
    Ok((requested_override, effective, source))
}

fn validate_requested_cdc_mode_requirements(
    requested_cdc_mode: &str,
) -> Result<(), PgTrickleError> {
    if requested_cdc_mode != "wal" {
        return Ok(());
    }

    if !cdc::can_use_logical_replication_for_mode(requested_cdc_mode)? {
        return Err(PgTrickleError::InvalidArgument(
            "cdc_mode = 'wal' requires wal_level = logical and an available replication slot"
                .to_string(),
        ));
    }

    Ok(())
}

/// A warning produced by the shared create/preview analysis path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CreationWarning {
    pub(crate) code: &'static str,
    pub(crate) message: String,
    pub(crate) hint: String,
}

pub(crate) fn collect_creation_warnings(
    query: &str,
    requested_mode: &str,
    effective_mode: RefreshMode,
    source_relids: &[(pg_sys::Oid, String)],
) -> Vec<CreationWarning> {
    let mut warnings = Vec::new();

    if requested_mode.eq_ignore_ascii_case("FULL") || effective_mode == RefreshMode::Full {
        warnings.push(CreationWarning {
            code: "ALWAYS_FULL",
            message: if requested_mode.eq_ignore_ascii_case("FULL") {
                "The stream table is configured for FULL refreshes.".to_string()
            } else {
                "The query was admitted as FULL-only and cannot use differential maintenance.".to_string()
            },
            hint: "Use a supported rewrite for differential maintenance, or keep FULL explicitly if that is intentional.".to_string(),
        });
    }

    let join_source_count = source_relids
        .iter()
        .filter(|(_, source_type)| {
            matches!(source_type.as_str(), "TABLE" | "VIEW" | "STREAM_TABLE")
        })
        .count();
    let join_limit = config::pg_trickle_warn_join_sources();
    if join_limit > 0 && join_source_count > join_limit as usize {
        warnings.push(CreationWarning {
            code: "JOIN_SOURCE_COUNT_HIGH",
            message: format!(
                "The defining query references {join_source_count} joined sources, above the configured warning threshold of {join_limit}."
            ),
            hint: "Reduce or split the join graph, or accept the measured refresh and write cost.".to_string(),
        });
    }

    if crate::api::helpers::detect_select_star(query) {
        warnings.push(CreationWarning {
            code: "SELECT_STAR",
            message: "The defining query uses SELECT *, so source schema changes can require reinitialization.".to_string(),
            hint: "List output columns explicitly for a more stable stream-table schema.".to_string(),
        });
    }
    if let Some(pattern) = crate::api::helpers::detect_volatile_functions(query) {
        warnings.push(CreationWarning {
            code: "VOLATILE_FUNCTION",
            message: format!(
                "The defining query contains the volatile or non-deterministic function '{pattern}'."
            ),
            hint: "Use stable row inputs, or choose FULL refresh mode if this is intentional.".to_string(),
        });
    }

    for (oid, source_type) in source_relids {
        if source_type != "TABLE" {
            continue;
        }
        let source_name = Spi::get_one_with_args::<String>(
            "SELECT format('%I.%I', n.nspname, c.relname) \
             FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.oid = $1",
            &[(*oid).into()],
        )
        .unwrap_or(None)
        .unwrap_or_else(|| format!("OID {}", oid.to_u32()));

        if cdc::resolve_pk_columns(*oid)
            .map(|columns| columns.is_empty())
            .unwrap_or(false)
        {
            warnings.push(CreationWarning {
                code: "SOURCE_IDENTITY_INADEQUATE",
                message: format!("Source table {source_name} has no PRIMARY KEY; change identity falls back to content hashing."),
                hint: "Add a stable PRIMARY KEY or suitable replica identity for reliable, faster maintenance.".to_string(),
            });
        }

        let rls_enabled = Spi::get_one_with_args::<bool>(
            "SELECT relrowsecurity FROM pg_catalog.pg_class WHERE oid = $1",
            &[(*oid).into()],
        )
        .unwrap_or(Some(false))
        .unwrap_or(false);
        if rls_enabled {
            warnings.push(CreationWarning {
                code: "SOURCE_RLS_ENABLED",
                message: format!("Source table {source_name} has Row Level Security enabled."),
                hint: "The defining query runs as the stream owner with row_security enabled; verify that role's policies expose the intended rows.".to_string(),
            });
        }
    }

    warnings
}

pub(crate) fn emit_creation_warnings(warnings: &[CreationWarning]) {
    for warning in warnings {
        pgrx::warning!(
            "[pg_trickle] {}: {} HINT: {}",
            warning.code,
            warning.message,
            warning.hint,
        );
    }
}

/// Validate a rewritten query and parse it for DVM. This runs the LIMIT 0
/// check, TopK detection, unsupported construct rejection, DVM parsing,
/// volatility checks, and source relation extraction.
///
/// When `is_auto` is true and the query cannot be maintained incrementally,
/// `refresh_mode` is downgraded to `RefreshMode::Full` instead of returning
/// an error.
fn validate_and_parse_query(
    query: &str,
    refresh_mode: &mut RefreshMode,
    is_auto: bool,
    had_nested_window_rewrite: bool,
) -> Result<ValidatedQuery, PgTrickleError> {
    // Validate the defining query and extract output columns
    let (columns, query_volatility) = validate_defining_query(query)?;

    // TopK detection — must run BEFORE reject_limit_offset
    let topk_info = crate::dvm::detect_topk_pattern(query)?;
    if let Some(info) = &topk_info {
        let limit = validation::positive_i32("topk_limit", info.limit_value)?;
        let offset = info
            .offset_value
            .map(|value| validation::nonnegative_i32("topk_offset", value))
            .transpose()?;
        if let Some(offset) = offset
            && limit.checked_add(offset).is_none()
        {
            return Err(PgTrickleError::InvalidArgument(
                "topk_limit + topk_offset exceeds the supported integer range".to_string(),
            ));
        }
    }

    // TopK parsing removes ORDER BY/LIMIT from the DVM tree. Inspect the
    // original query first so volatile or uninspectable ordering expressions
    // cannot bypass incremental admission.
    if topk_info.is_some()
        && (*refresh_mode == RefreshMode::Differential || *refresh_mode == RefreshMode::Immediate)
    {
        let topk_issue = if query_volatility < 's' {
            None
        } else {
            Some(format!(
                "TopK ORDER BY contains {} expressions that can change between refreshes",
                if query_volatility == 'v' {
                    "volatile"
                } else {
                    "stable"
                }
            ))
        };
        if let Some(reason) = topk_issue {
            if is_auto && *refresh_mode == RefreshMode::Differential {
                pgrx::warning!(
                    "[pg_trickle] Falling back to FULL refresh [DVM-81-6-TOPK]: {}",
                    reason
                );
                *refresh_mode = RefreshMode::Full;
            } else {
                return Err(PgTrickleError::UnsupportedOperator(format!(
                    "{} mode is not safe for incremental maintenance: {}. \
                     Use FULL or AUTO, or use an immutable, inspectable ORDER BY expression.",
                    refresh_mode.as_str(),
                    reason
                )));
            }
        }
    }

    let (effective_query, topk_info) = if let Some(info) = topk_info {
        if let Some(offset) = info.offset_value {
            pgrx::info!(
                "pg_trickle: TopK pattern detected (ORDER BY {} LIMIT {} OFFSET {}). \
                 The stream table will maintain rows {}–{}.",
                info.order_by_sql,
                info.limit_value,
                offset,
                offset + 1,
                offset + info.limit_value,
            );
        } else {
            pgrx::info!(
                "pg_trickle: TopK pattern detected (ORDER BY {} LIMIT {}). \
                 The stream table will maintain the top {} rows.",
                info.order_by_sql,
                info.limit_value,
                info.limit_value,
            );
        }
        let base = info.base_query.clone();
        (base, Some(info))
    } else {
        // NS-1: Warn when the query has ORDER BY but no LIMIT.
        // ORDER BY without LIMIT has no effect on a stream table because
        // the underlying storage table row order is undefined.
        if crate::dvm::has_order_by_without_limit(query) {
            pgrx::warning!(
                "pg_trickle: ORDER BY without LIMIT has no effect on stream tables — \
                 storage row order is undefined. Use ORDER BY with LIMIT for TopK, \
                 or remove ORDER BY."
            );
        }
        (query.to_string(), None)
    };
    let q = if topk_info.is_some() {
        &effective_query
    } else {
        query
    };

    // Reject LIMIT / OFFSET (TopK already handled above).
    crate::dvm::reject_limit_offset(q)?;
    crate::dvm::warn_limit_without_order_in_subqueries(q);

    // Reject unsupported constructs (NATURAL JOIN, subquery expressions).
    // AUTO mode: downgrade to FULL instead of erroring.
    if let Err(e) = crate::dvm::reject_unsupported_constructs(q) {
        if is_auto {
            pgrx::warning!(
                "[pg_trickle] Falling back to FULL refresh: unsupported construct \
                 in defining query.\n\
                 Construct: {e}\n\
                 See docs/DVM_OPERATORS.md for the full list of supported operators."
            );
            *refresh_mode = RefreshMode::Full;
        } else {
            return Err(e);
        }
    }

    // Reject matviews/foreign tables in DIFFERENTIAL/IMMEDIATE.
    // AUTO mode: downgrade to FULL if matviews/foreign tables are present.
    if (*refresh_mode == RefreshMode::Differential || *refresh_mode == RefreshMode::Immediate)
        && let Err(e) = crate::dvm::reject_materialized_views(q)
    {
        if is_auto && *refresh_mode == RefreshMode::Differential {
            pgrx::warning!(
                "[pg_trickle] Falling back to FULL refresh: query references materialized \
                 views or foreign tables ({}).\n\
                 Suggestion: replace materialized view references with regular views or \
                 tables, or convert them to stream tables so changes can be tracked \
                 incrementally.",
                e
            );
            *refresh_mode = RefreshMode::Full;
        } else {
            return Err(e);
        }
    }

    // EC-03: Nested window expressions (e.g. CASE WHEN ROW_NUMBER() OVER (...) ...)
    // were rewritten into subqueries. DVM parsing succeeds on the rewritten form,
    // but delta SQL generation fails at refresh time. Downgrade to FULL in AUTO
    // mode; emit a warning in explicit DIFFERENTIAL mode.
    if had_nested_window_rewrite
        && (*refresh_mode == RefreshMode::Differential || *refresh_mode == RefreshMode::Immediate)
    {
        if is_auto {
            pgrx::warning!(
                "[pg_trickle] Falling back to FULL refresh: query contains window functions \
                 inside expressions (e.g. CASE WHEN ROW_NUMBER() OVER (...) ...).\n\
                 Suggestion: move the window function into a subquery or CTE and apply \
                 the surrounding expression (CASE, arithmetic, etc.) in the outer query."
            );
            *refresh_mode = RefreshMode::Full;
        } else {
            pgrx::warning!(
                "pg_trickle: Query contains window functions inside expressions \
                 (e.g. CASE WHEN ROW_NUMBER() OVER (...) ...). This pattern is not \
                 supported by differential maintenance and will fall back to full \
                 recomputation at refresh time.\n\
                 Suggestion: move the window function into a subquery or CTE and apply \
                 the surrounding expression in the outer query, or use FULL or AUTO \
                 refresh mode."
            );
        }
    }

    // IMMEDIATE mode: TopK limit threshold check
    if let (true, Some(info)) = (refresh_mode.is_immediate(), &topk_info) {
        let topk_limit = info.limit_value;
        let max_limit = crate::config::PGS_IVM_TOPK_MAX_LIMIT.get() as i64;
        if max_limit == 0 || topk_limit > max_limit {
            return Err(PgTrickleError::UnsupportedOperator(format!(
                "ORDER BY + LIMIT {topk_limit} (TopK) exceeds the IMMEDIATE mode threshold \
                 (pg_trickle.ivm_topk_max_limit = {max_limit}). TopK tables in IMMEDIATE mode \
                 recompute the top-K rows on every DML statement. Reduce LIMIT, raise the \
                 threshold, or use 'DIFFERENTIAL' mode for large TopK queries."
            )));
        }
        pgrx::warning!(
            "pg_trickle: TopK (LIMIT {}) in IMMEDIATE mode uses micro-refresh — \
             the top-{} rows are recomputed on every DML statement. \
             This adds latency proportional to the defining query cost.",
            topk_limit,
            topk_limit
        );
    }

    // IMMEDIATE mode: query restriction validation
    if refresh_mode.is_immediate() {
        crate::dvm::validate_immediate_mode_support(q)?;
    }

    // DVM parse for DIFFERENTIAL/IMMEDIATE (non-TopK).
    // AUTO mode: if DVM parsing fails, downgrade to FULL instead of erroring.
    let mut parsed_tree = if (*refresh_mode == RefreshMode::Differential
        || *refresh_mode == RefreshMode::Immediate)
        && topk_info.is_none()
    {
        match crate::dvm::parse_defining_query_full(q) {
            Ok(mut tree) => {
                // Emit advisory warnings (e.g. NATURAL JOIN column drift) exactly
                // once here — downstream parse calls (cache pre-warm, row-id
                // derivation) silently discard them via ParseResult.warnings.
                for msg in &tree.warnings {
                    pgrx::warning!("{}", msg);
                }
                // F4 (v0.37.0): Reclassify avg/sum on vector-typed columns to
                // VectorAvg/VectorSum (group-rescan strategy) so that
                // avg_aux_columns() does not create algebraic aux columns for
                // vector types (COALESCE(vec_col, 0) is not valid for vectors).
                if crate::config::pg_trickle_enable_vector_agg() {
                    let source_oids = {
                        let mut oids = tree.tree.source_oids();
                        oids.extend(tree.cte_registry.source_oids());
                        oids.sort_unstable();
                        oids.dedup();
                        oids
                    };
                    let vector_cols = crate::dvm::resolve_vector_columns_for_sources(&source_oids);
                    if !vector_cols.is_empty() {
                        crate::dvm::reclassify_vector_aggregates(&mut tree.tree, &vector_cols);
                    }
                }
                Some(tree)
            }
            Err(e) if is_auto && *refresh_mode == RefreshMode::Differential => {
                pgrx::warning!(
                    "[pg_trickle] Falling back to FULL refresh: query cannot use \
                     differential maintenance ({}).\n\
                     Suggestion: simplify the query to use supported SQL constructs, \
                     or break it into multiple stream tables chained together. \
                     Run SELECT * FROM pgtrickle.explain_refresh_mode('<table>') after \
                     creation for diagnostics. \
                     See docs/DVM_OPERATORS.md for the full list of supported operators.",
                    e
                );
                *refresh_mode = RefreshMode::Full;
                None
            }
            Err(e) => return Err(e),
        }
    } else {
        None
    };

    // RLS-3: A source with row-level security cannot be safely diffed.
    // The differential CDC staging window can only check whether a row is
    // visible in the *current* state of a source, so a delete/update image
    // for a row whose visibility changed (a hidden→visible UPDATE, a DELETE
    // of a row hidden from the owner, etc.) cannot be told apart from one
    // that never changed. FULL re-executes the defining query under the
    // owner's row_security from scratch every time, so it is the only mode
    // that is safe here.
    if let Some(pr) = parsed_tree.as_ref() {
        let mut source_oids = pr.tree.source_oids();
        source_oids.extend(pr.cte_registry.source_oids());
        source_oids.sort_unstable();
        source_oids.dedup();
        if let Some(rls_source) = crate::cdc::first_rls_enabled_source(&source_oids)? {
            if is_auto && *refresh_mode == RefreshMode::Differential {
                pgrx::warning!(
                    "[pg_trickle] Falling back to FULL refresh: source \"{}\" has row-level \
                     security enabled. Differential and IMMEDIATE maintenance cannot \
                     determine whether an old row image was visible to the stream table \
                     owner before a policy-relevant change, so only FULL refresh is \
                     supported while row-level security is enabled on this source.",
                    rls_source,
                );
                *refresh_mode = RefreshMode::Full;
                parsed_tree = None;
            } else {
                return Err(PgTrickleError::UnsupportedOperator(format!(
                    "{} mode is not supported for source \"{}\" because it has row-level \
                     security enabled: the differential CDC window cannot determine \
                     whether an old row image was visible to the stream table owner \
                     before a policy-relevant change. Use 'FULL' or 'AUTO' mode for \
                     RLS-protected sources.",
                    refresh_mode.as_str(),
                    rls_source,
                )));
            }
        }
    }

    // One admission matrix for CREATE, ALTER, and mode changes. Known unsafe
    // forms are FULL-only; explicit incremental modes fail before mutation.
    if let Some(pr) = parsed_tree.as_ref() {
        let admission = match crate::dvm::incremental_admission(
            pr,
            query_volatility,
            refresh_mode.is_immediate(),
        ) {
            Ok(admission) => admission,
            Err(e) if is_auto && *refresh_mode == RefreshMode::Differential => {
                pgrx::warning!(
                    "[pg_trickle] Falling back to FULL refresh: incremental inspection failed: {e}"
                );
                *refresh_mode = RefreshMode::Full;
                parsed_tree = None;
                crate::dvm::IncrementalAdmission::Proven
            }
            Err(e) => return Err(e),
        };
        let resolved = crate::dvm::resolve_incremental_mode(*refresh_mode, is_auto, &admission)?;
        if resolved == RefreshMode::Full && *refresh_mode != RefreshMode::Full {
            if let crate::dvm::IncrementalAdmission::FullOnly(issues) = admission {
                for issue in issues {
                    pgrx::warning!(
                        "[pg_trickle] Falling back to FULL refresh [{} / {}]: {} Hint: {}",
                        issue.code,
                        issue.operator,
                        issue.reason,
                        issue.hint,
                    );
                }
            }
            *refresh_mode = RefreshMode::Full;
            parsed_tree = None;
        }
    }

    // G12-AGG: Warn when DIFFERENTIAL mode is used with group-rescan aggregates.
    // These aggregates (STRING_AGG, ARRAY_AGG, JSON_AGG, etc.) require full
    // re-aggregation of affected groups on every refresh, which is correct but
    // slower than the algebraic path used by COUNT/SUM/AVG.
    if let Some(ref pr) = parsed_tree {
        let rescan_names = pr.tree.group_rescan_aggregate_names();
        if !rescan_names.is_empty() {
            let names_str = rescan_names.join(", ");
            pgrx::warning!(
                "pg_trickle: DIFFERENTIAL mode with group-rescan aggregates [{}]. \
                 These aggregates re-scan affected groups from source data on each \
                 refresh. This is correct but may be slower than algebraic aggregates \
                 (COUNT, SUM, AVG). Use FULL mode if group-rescan performance is \
                 unacceptable, or replace with algebraic alternatives where possible.",
                names_str,
            );
        }
    }

    // DIAG-2: Warn when algebraic aggregates are used with low-cardinality
    // GROUP BY columns — the DIFFERENTIAL overhead may not be justified.
    // (Placeholder — actual check runs after source_relids is computed below.)
    let diag2_info: Option<(Vec<String>, Vec<String>, i32)> = {
        let threshold = crate::config::pg_trickle_agg_diff_cardinality_threshold();
        if threshold > 0 {
            if let Some(ref pr) = parsed_tree {
                let alg_names = pr.tree.algebraic_aggregate_names();
                if !alg_names.is_empty() {
                    pr.tree
                        .group_by_columns()
                        .map(|gc| (alg_names, gc, threshold))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    };

    let needs_pgt_count = parsed_tree
        .as_ref()
        .is_some_and(|pr| pr.tree.needs_pgt_count());
    let needs_dual_count = parsed_tree
        .as_ref()
        .is_some_and(|pr| pr.tree.needs_dual_count());
    let needs_union_dedup = parsed_tree
        .as_ref()
        .is_some_and(|pr| pr.tree.needs_union_dedup_count());

    // Extract source dependencies
    let source_relids = normalize_source_relations(extract_source_relations(q)?);

    // DIAG-2: Now that source_relids is available, emit the low-cardinality warning.
    if let Some((alg_names, group_cols, threshold)) = diag2_info {
        let estimated_groups = estimate_group_cardinality(&source_relids, &group_cols);
        if let Some(est) = estimated_groups
            && est > 0
            && est < threshold as i64
        {
            pgrx::warning!(
                "pg_trickle: DIFFERENTIAL mode with algebraic aggregates [{}] \
                 and estimated GROUP BY cardinality {} (below threshold {}). \
                 Consider refresh_mode='full' or 'auto' for low-cardinality \
                 groupings. Adjust pg_trickle.agg_diff_cardinality_threshold \
                 to tune this warning.",
                alg_names.join(", "),
                est,
                threshold,
            );
        }
    }

    // EC-06: Detect keyless sources
    let has_keyless_source = source_relids.iter().any(|(oid, source_type)| {
        if source_type != "TABLE" && source_type != "FOREIGN_TABLE" && source_type != "MATVIEW" {
            return false;
        }
        cdc::resolve_pk_columns(*oid)
            .map(|pk| pk.is_empty())
            .unwrap_or(false)
    });

    // EC-06b: Join queries whose output does not include PK columns from
    // both sides cannot produce unique __pgt_row_id hashes. Treat them
    // as keyless so the storage table gets a non-unique index and the
    // refresh uses CTID-based deletion.
    let has_keyless_source = has_keyless_source
        || crate::dvm::query_has_incomplete_join_pk(query)
        // INTERSECT/EXCEPT ALL can legitimately materialize duplicate rows;
        // use the keyless/non-unique storage path for all guarded variants.
        || crate::dvm::query_needs_dual_count(query);

    let avg_aux_columns = parsed_tree
        .as_ref()
        .map(|pr| pr.tree.avg_aux_columns())
        .unwrap_or_default();

    let sum2_aux_columns = parsed_tree
        .as_ref()
        .map(|pr| pr.tree.sum2_aux_columns())
        .unwrap_or_default();

    let covar_aux_columns = parsed_tree
        .as_ref()
        .map(|pr| pr.tree.covar_aux_columns())
        .unwrap_or_default();

    let nonnull_aux_columns = parsed_tree
        .as_ref()
        .map(|pr| pr.tree.nonnull_aux_columns())
        .unwrap_or_default();
    let statistical_aux_types = parsed_tree
        .as_ref()
        .map(|pr| pr.tree.statistical_aux_types())
        .unwrap_or_default();

    Ok(ValidatedQuery {
        columns,
        parsed_tree,
        topk_info,
        effective_query,
        needs_pgt_count,
        needs_dual_count,
        needs_union_dedup,
        has_keyless_source,
        source_relids,
        avg_aux_columns,
        sum2_aux_columns,
        covar_aux_columns,
        nonnull_aux_columns,
        statistical_aux_types,
    })
}

/// Validate a stored defining query before switching an existing table into
/// an incremental mode. This is deliberately side-effect free.
pub(crate) fn validate_incremental_mode_for_query(
    defining_query: &str,
    mode: RefreshMode,
) -> Result<(), PgTrickleError> {
    if mode == RefreshMode::Full {
        return Ok(());
    }
    let (_, query_volatility) = validate_defining_query(defining_query)?;
    let result = crate::dvm::parse_defining_query_full(defining_query)?;
    crate::dvm::check_ivm_support_with_registry(&result)?;
    if mode.is_immediate() {
        crate::dvm::validate_immediate_mode_support(defining_query)?;
    }
    let admission =
        crate::dvm::incremental_admission(&result, query_volatility, mode.is_immediate())?;
    crate::dvm::resolve_incremental_mode(mode, false, &admission)?;
    Ok(())
}

/// F4: Post-fix vector aggregate output column types to include explicit
/// dimensions (e.g. `vector` → `vector(3)`).
///
/// `avg(embedding)` where `embedding` is `vector(3)` returns type `vector`
/// (undimensioned) at the PostgreSQL query-analysis level. HNSW / IVFFlat
/// indexes require the column to have an explicit dimension. This function
/// looks up each VectorAvg/VectorSum output column's source column typmod and
/// runs `ALTER TABLE … ALTER COLUMN … TYPE <type>(N)` when the dimension is
/// known.
///
/// VH-1 (v0.48.0): uses the actual pgvector type (`vector`, `halfvec`, or
/// `sparsevec`) so that `halfvec_avg` and `sparsevec_avg` output columns are
/// typed correctly (previously always used `vector(N)`).
fn fix_vector_aggregate_column_types(
    schema: &str,
    table_name: &str,
    tree: &crate::dvm::parser::OpTree,
) -> Result<(), PgTrickleError> {
    let dims = crate::dvm::extract_vector_agg_output_dims(tree);
    for (col_alias, typmod, typename) in dims {
        if typmod <= 0 {
            continue;
        }
        // Use the actual pgvector base type name to build the correct type expression.
        // typename is one of: "vector", "halfvec", "sparsevec".
        let alter_sql = format!(
            "ALTER TABLE {}.{} ALTER COLUMN {} TYPE {}({})",
            quote_identifier(schema),
            quote_identifier(table_name),
            quote_identifier(&col_alias),
            typename, // VH-1: use actual type, not always "vector"
            typmod,
        );
        Spi::run(&alter_sql).map_err(|e| {
            PgTrickleError::SpiError(format!(
                "F4: failed to set {} dimension for column '{}': {}",
                typename, col_alias, e
            ))
        })?;
    }
    Ok(())
}

/// Set up the storage table: CREATE TABLE, row_id index, DML guard trigger,
/// and optional GROUP BY composite index.
#[allow(clippy::too_many_arguments)]
fn setup_storage_table(
    schema: &str,
    table_name: &str,
    columns: &[ColumnDef],
    needs_pgt_count: bool,
    needs_dual_count: bool,
    has_keyless_source: bool,
    refresh_mode: RefreshMode,
    parsed_tree: Option<&crate::dvm::ParseResult>,
    avg_aux_columns: &[(String, String, String)],
    sum2_aux_columns: &[(String, String)],
    covar_aux_columns: &[(String, String)],
    nonnull_aux_columns: &[(String, String)],
    statistical_aux_types: &[(String, String)],
    // A1-1: partition key column name, or None for non-partitioned STs.
    partition_key: Option<&str>,
    // HOT-1: heap fillfactor for HOT-friendly differential updates.
    fillfactor: Option<i32>,
) -> Result<pg_sys::Oid, PgTrickleError> {
    let storage_needs_pgt_count = needs_pgt_count;
    // INTERSECT/EXCEPT admission is guarded for incremental modes.  FULL
    // storage must therefore contain only defining-query columns, even when
    // a legacy caller still reports the old dual-count requirement.
    let storage_needs_dual_count = needs_dual_count && refresh_mode != RefreshMode::Full;
    let storage_ddl = build_create_table_sql(
        schema,
        table_name,
        columns,
        storage_needs_pgt_count,
        storage_needs_dual_count,
        avg_aux_columns,
        sum2_aux_columns,
        covar_aux_columns,
        nonnull_aux_columns,
        statistical_aux_types,
        partition_key,
        fillfactor,
    );
    Spi::run(&storage_ddl)
        .map_err(|e| PgTrickleError::SpiError(format!("Failed to create storage table: {}", e)))?;

    // A1-1/A1-3b: For partitioned storage tables, create child partitions.
    // RANGE/LIST: create a catch-all default partition so rows are never rejected.
    // HASH: create N child partitions (no default allowed by PostgreSQL).
    if let Some(pk) = partition_key {
        let method = parse_partition_method(pk);
        match method {
            PartitionMethod::Hash => {
                let modulus = parse_hash_modulus(pk).unwrap_or(4);
                for remainder in 0..modulus {
                    let child_name = format!("{table_name}_p{remainder}");
                    let child_sql = format!(
                        "CREATE TABLE {}.{} PARTITION OF {}.{} \
                         FOR VALUES WITH (modulus {modulus}, remainder {remainder})",
                        quote_identifier(schema),
                        quote_identifier(&child_name),
                        quote_identifier(schema),
                        quote_identifier(table_name),
                    );
                    Spi::run(&child_sql).map_err(|e| {
                        PgTrickleError::SpiError(format!(
                            "Failed to create hash partition {child_name}: {e}"
                        ))
                    })?;
                }
            }
            PartitionMethod::Range | PartitionMethod::List => {
                let default_partition_sql = format!(
                    "CREATE TABLE {}.{} PARTITION OF {}.{} DEFAULT",
                    quote_identifier(schema),
                    quote_identifier(&format!("{table_name}_default")),
                    quote_identifier(schema),
                    quote_identifier(table_name),
                );
                Spi::run(&default_partition_sql).map_err(|e| {
                    PgTrickleError::SpiError(format!("Failed to create default partition: {}", e))
                })?;
            }
        }
    }

    let pgt_relid = get_table_oid(schema, table_name)?;

    // Create index on __pgt_row_id (EC-06: non-unique for keyless sources)
    //
    // A-4 / AUTO-IDX-2: When auto_index is enabled and the output schema has
    // <= 8 user columns, create a covering index with INCLUDE clause to enable
    // index-only scans during MERGE. This eliminates the heap fetch for matched
    // rows, giving 20-50% MERGE time reduction for small-delta / large-target
    // scenarios.
    //
    // A1-1: PostgreSQL does not support global UNIQUE indexes on partitioned
    // tables. Force a non-unique index when the storage table is partitioned.
    let auto_index = crate::config::pg_trickle_auto_index();
    const COVERING_INDEX_MAX_COLUMNS: usize = 8;
    let include_clause =
        if auto_index && columns.len() <= COVERING_INDEX_MAX_COLUMNS && !columns.is_empty() {
            let include_cols: Vec<String> = columns
                .iter()
                .map(|c| quote_identifier(&c.name).to_string())
                .collect();
            format!(" INCLUDE ({})", include_cols.join(", "))
        } else {
            String::new()
        };
    let is_partitioned = partition_key.is_some();
    let index_sql = if has_keyless_source || is_partitioned {
        format!(
            "CREATE INDEX ON {}.{} (__pgt_row_id){include_clause}",
            quote_identifier(schema),
            quote_identifier(table_name),
        )
    } else {
        format!(
            "CREATE UNIQUE INDEX ON {}.{} (__pgt_row_id){include_clause}",
            quote_identifier(schema),
            quote_identifier(table_name),
        )
    };
    Spi::run(&index_sql)
        .map_err(|e| PgTrickleError::SpiError(format!("Failed to create row_id index: {}", e)))?;

    // DML guard trigger
    install_dml_guard_trigger(schema, table_name)?;

    // AUTO-IDX-1: Auto-create indexes on GROUP BY / DISTINCT columns.
    // Gated behind pg_trickle.auto_index GUC (default true).
    if auto_index {
        // GROUP BY composite index for aggregate queries in DIFFERENTIAL mode
        if refresh_mode == RefreshMode::Differential
            && let Some(pr) = parsed_tree
            && let Some(group_cols) = pr.tree.group_by_columns()
            && !group_cols.is_empty()
        {
            let quoted_cols: Vec<String> = group_cols
                .iter()
                .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
                .collect();
            let group_index_sql = format!(
                "CREATE INDEX ON {}.{} ({})",
                quote_identifier(schema),
                quote_identifier(table_name),
                quoted_cols.join(", "),
            );
            Spi::run(&group_index_sql).map_err(|e| {
                PgTrickleError::SpiError(format!("Failed to create group-by index: {}", e))
            })?;
        }

        // DISTINCT composite index: when a DISTINCT query has no GROUP BY,
        // index the output columns to speed up deduplication lookups.
        if let Some(pr) = parsed_tree
            && pr.tree.group_by_columns().is_none()
            && pr.tree.has_distinct()
        {
            let distinct_cols = pr.tree.output_columns();
            if !distinct_cols.is_empty() && distinct_cols.len() <= COVERING_INDEX_MAX_COLUMNS {
                let quoted_cols: Vec<String> = distinct_cols
                    .iter()
                    .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
                    .collect();
                let distinct_index_sql = format!(
                    "CREATE INDEX ON {}.{} ({})",
                    quote_identifier(schema),
                    quote_identifier(table_name),
                    quoted_cols.join(", "),
                );
                Spi::run(&distinct_index_sql).map_err(|e| {
                    PgTrickleError::SpiError(format!("Failed to create distinct index: {}", e))
                })?;
            }
        }
    }

    Ok(pgt_relid)
}

/// Insert catalog entry and dependency edges for a new stream table.
#[allow(clippy::too_many_arguments)]
fn insert_catalog_and_deps(
    pgt_relid: pg_sys::Oid,
    schema: &str,
    table_name: &str,
    defining_query: &str,
    original_query: Option<&str>,
    schedule: Option<String>,
    refresh_mode: RefreshMode,
    vq: &ValidatedQuery,
    dc: DiamondConsistency,
    dsp: DiamondSchedulePolicy,
    requested_cdc_mode: Option<&str>,
    is_append_only: bool,
    pooler_compatibility_mode: bool,
    partition_by: Option<&str>,
    max_differential_joins: Option<i32>,
    max_delta_fraction: Option<f64>,
    // CORR-1/UX-1 (v0.36.0): temporal IVM mode
    temporal_mode: bool,
    // CORR-2/UX-3 (v0.36.0): columnar storage backend
    storage_backend: &str,
    // HOT-1 (v0.73.0): heap fillfactor
    storage_fillfactor: Option<i32>,
    // LSEC-3 (v0.87.7): the caller's search_path used to resolve `defining_query`
    defining_search_path: &str,
) -> Result<i64, PgTrickleError> {
    let topk_limit = vq
        .topk_info
        .as_ref()
        .map(|info| validation::positive_i32("topk_limit", info.limit_value))
        .transpose()?;
    let topk_offset = vq
        .topk_info
        .as_ref()
        .and_then(|info| info.offset_value)
        .map(|value| validation::nonnegative_i32("topk_offset", value))
        .transpose()?;
    if let (Some(limit), Some(offset)) = (topk_limit, topk_offset)
        && limit.checked_add(offset).is_none()
    {
        return Err(PgTrickleError::InvalidArgument(
            "topk_limit + topk_offset exceeds the supported integer range".to_string(),
        ));
    }

    let pgt_id = StreamTableMeta::insert(
        pgt_relid,
        table_name,
        schema,
        defining_query,
        original_query,
        schedule,
        refresh_mode,
        vq.parsed_tree.as_ref().map(|pr| pr.functions_used()),
        topk_limit,
        vq.topk_info.as_ref().map(|i| i.order_by_sql.as_str()),
        topk_offset,
        dc,
        dsp,
        vq.has_keyless_source,
        requested_cdc_mode,
        is_append_only,
        pooler_compatibility_mode,
        partition_by,
        max_differential_joins,
        max_delta_fraction,
        temporal_mode,
        storage_backend,
        storage_fillfactor,
        defining_search_path,
    )?;

    // Build per-source column usage map
    let columns_used_map = vq
        .parsed_tree
        .as_ref()
        .map(|pr| pr.source_columns_used())
        .unwrap_or_default();

    // Insert dependency edges with column snapshots
    for (source_oid, source_type) in &vq.source_relids {
        let cols = columns_used_map.get(&source_oid.to_u32()).cloned();

        let (snapshot, fingerprint) =
            if source_type == "TABLE" || source_type == "FOREIGN_TABLE" || source_type == "MATVIEW"
            {
                match crate::catalog::build_column_snapshot(*source_oid) {
                    Ok((s, f)) => (Some(s), Some(f)),
                    Err(e) => {
                        pgrx::debug1!(
                            "pg_trickle: failed to build column snapshot for source {}: {}",
                            source_oid.to_u32(),
                            e,
                        );
                        (None, None)
                    }
                }
            } else {
                (None, None)
            };

        StDependency::insert_with_snapshot(
            pgt_id,
            *source_oid,
            source_type,
            cols,
            snapshot,
            fingerprint,
        )?;
    }

    Ok(pgt_id)
}

/// Set up CDC or IVM trigger infrastructure on source tables.
fn setup_trigger_infrastructure(
    source_relids: &[(pg_sys::Oid, String)],
    refresh_mode: RefreshMode,
    pgt_id: i64,
    pgt_relid: pg_sys::Oid,
    defining_query: &str,
) -> Result<(), PgTrickleError> {
    let change_schema = config::pg_trickle_change_buffer_schema();
    if refresh_mode.is_immediate() {
        let lock_mode = crate::ivm::IvmLockMode::for_query(defining_query);
        for (source_oid, source_type) in source_relids {
            if source_type == "TABLE" {
                crate::ivm::setup_ivm_triggers(*source_oid, pgt_id, pgt_relid, lock_mode)?;
            }
        }
    } else {
        for (source_oid, source_type) in source_relids {
            if source_type == "TABLE" {
                setup_cdc_for_source(*source_oid, pgt_id, &change_schema)?;
            } else if source_type == "FOREIGN_TABLE" {
                cdc::setup_foreign_table_polling(*source_oid, pgt_id, &change_schema)?;
            } else if source_type == "MATVIEW" {
                cdc::setup_matview_polling(*source_oid, pgt_id, &change_schema)?;
            } else if source_type == "STREAM_TABLE" {
                // ST-ST-1: Ensure the upstream ST has a change buffer so
                // downstream STs can consume differential deltas.
                let upstream_pgt_id =
                    crate::catalog::StreamTableMeta::pgt_id_for_relid(*source_oid);
                if let Some(up_id) = upstream_pgt_id {
                    cdc::ensure_st_change_buffer(up_id, *source_oid, &change_schema)?;
                }
            }
        }
    }
    Ok(())
}

fn get_data_timestamp_str() -> String {
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{now_secs}Z")
}

pub(crate) mod cluster;

// ── v0.35.0 reactive-subscription API (UX-SUB) ──────────────────────────────

/// UX-SUB: Subscribe a named NOTIFY channel to a stream table.
///
/// After a non-empty differential or full refresh for `stream_table`, the
/// background worker will call `PERFORM pg_notify(channel, ...)` so clients
/// blocked on `LISTEN <channel>` wake up immediately.
///
/// The subscription is stored in `pgtrickle.pgt_subscriptions` and survives
/// restarts.
#[pg_extern(schema = "pgtrickle")]
fn subscribe(stream_table: &str, channel: &str) -> Result<(), PgTrickleError> {
    Spi::run_with_args(
        "INSERT INTO pgtrickle.pgt_subscriptions (stream_table, channel) \
         VALUES ($1, $2) ON CONFLICT DO NOTHING",
        &[stream_table.into(), channel.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))
}

/// UX-SUB: Remove a NOTIFY subscription for a stream table / channel pair.
#[pg_extern(schema = "pgtrickle")]
fn unsubscribe(stream_table: &str, channel: &str) -> Result<(), PgTrickleError> {
    Spi::run_with_args(
        "DELETE FROM pgtrickle.pgt_subscriptions \
         WHERE stream_table = $1 AND channel = $2",
        &[stream_table.into(), channel.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))
}

/// UX-SUB: List all active NOTIFY subscriptions.
///
/// Returns a table with columns (stream_table TEXT, channel TEXT,
/// created_at TIMESTAMPTZ).
#[allow(clippy::type_complexity)]
#[pg_extern(schema = "pgtrickle")]
fn list_subscriptions() -> TableIterator<
    'static,
    (
        name!(stream_table, Option<String>),
        name!(channel, Option<String>),
        name!(created_at, Option<pgrx::datum::TimestampWithTimeZone>),
    ),
> {
    let rows = Spi::connect(|client| {
        let tup_table = client.select(
            "SELECT stream_table, channel, created_at \
             FROM pgtrickle.pgt_subscriptions \
             ORDER BY stream_table, channel",
            None,
            &[],
        )?;
        let mut result: Vec<(
            Option<String>,
            Option<String>,
            Option<pgrx::datum::TimestampWithTimeZone>,
        )> = Vec::new();
        for row in tup_table {
            result.push((
                row["stream_table"].value::<String>()?,
                row["channel"].value::<String>()?,
                row["created_at"].value::<pgrx::datum::TimestampWithTimeZone>()?,
            ));
        }
        Ok::<_, pgrx::spi::Error>(result)
    })
    .unwrap_or_default();
    TableIterator::new(rows)
}

// ── v0.48.0 reactive distance-subscription API (VH-2) ────────────────────────

/// VH-2 (v0.48.0): Subscribe a NOTIFY channel to distance-predicate changes on
/// a stream table.
///
/// After each non-empty refresh the background worker evaluates
/// `<distance_col> <op> <threshold>` against the rows that changed and emits
/// `pg_notify(channel, payload)` when at least one row newly satisfies or
/// newly ceases to satisfy the predicate.  The payload is a JSON object with
/// `stream_table`, `schema`, `op`, `threshold`, and `matched_rows` fields.
///
/// `op` must be one of `<->` (L2), `<=>` (cosine), `<#>` (inner product),
/// `<+>` (L1), `<<->>` (Hamming), or `<<%>>` (Jaccard).
///
/// The subscription is stored in `pgtrickle.pgt_distance_subscriptions` and
/// survives restarts.
#[pg_extern(schema = "pgtrickle")]
fn subscribe_distance(
    stream_table: &str,
    channel: &str,
    vector_column: &str,
    query_vector: &str,
    op: &str,
    threshold: f64,
) -> Result<(), PgTrickleError> {
    const VALID_OPS: &[&str] = &["<->", "<=>", "<#>", "<+>", "<<->>", "<<%>>"];
    if !VALID_OPS.contains(&op) {
        return Err(PgTrickleError::InvalidArgument(format!(
            "subscribe_distance: invalid distance operator '{}'. \
             Must be one of: {}",
            op,
            VALID_OPS.join(", ")
        )));
    }
    if threshold <= 0.0 {
        return Err(PgTrickleError::InvalidArgument(
            "subscribe_distance: threshold must be positive".to_string(),
        ));
    }
    Spi::run_with_args(
        "INSERT INTO pgtrickle.pgt_distance_subscriptions \
         (stream_table, channel, vector_column, query_vector, op, threshold) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT (stream_table, channel) DO UPDATE \
           SET vector_column = EXCLUDED.vector_column, \
               query_vector  = EXCLUDED.query_vector, \
               op            = EXCLUDED.op, \
               threshold     = EXCLUDED.threshold",
        &[
            stream_table.into(),
            channel.into(),
            vector_column.into(),
            query_vector.into(),
            op.into(),
            threshold.into_datum().into(),
        ],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))
}

/// VH-2 (v0.48.0): Remove a distance-predicate subscription.
#[pg_extern(schema = "pgtrickle")]
fn unsubscribe_distance(stream_table: &str, channel: &str) -> Result<(), PgTrickleError> {
    Spi::run_with_args(
        "DELETE FROM pgtrickle.pgt_distance_subscriptions \
         WHERE stream_table = $1 AND channel = $2",
        &[stream_table.into(), channel.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))
}

/// VH-2 (v0.48.0): List all active distance-predicate subscriptions.
///
/// When `p_stream_table` is provided (e.g. `'public.ds3_st'`), only
/// subscriptions for that stream table are returned.  Pass NULL to list all.
#[allow(clippy::type_complexity)]
#[pg_extern(schema = "pgtrickle")]
fn list_distance_subscriptions(
    p_stream_table: pgrx::default!(Option<&str>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(stream_table, Option<String>),
        name!(channel, Option<String>),
        name!(vector_column, Option<String>),
        name!(op, Option<String>),
        name!(threshold, Option<f64>),
        name!(created_at, Option<pgrx::datum::TimestampWithTimeZone>),
    ),
> {
    let rows = Spi::connect(|client| {
        let tup_table = if let Some(st) = p_stream_table {
            client.select(
                "SELECT stream_table, channel, vector_column, op, threshold, created_at \
                 FROM pgtrickle.pgt_distance_subscriptions \
                 WHERE stream_table = $1 \
                 ORDER BY stream_table, channel",
                None,
                &[st.into()],
            )?
        } else {
            client.select(
                "SELECT stream_table, channel, vector_column, op, threshold, created_at \
                 FROM pgtrickle.pgt_distance_subscriptions \
                 ORDER BY stream_table, channel",
                None,
                &[],
            )?
        };
        let mut result: Vec<(
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<f64>,
            Option<pgrx::datum::TimestampWithTimeZone>,
        )> = Vec::new();
        for row in tup_table {
            result.push((
                row["stream_table"].value::<String>()?,
                row["channel"].value::<String>()?,
                row["vector_column"].value::<String>()?,
                row["op"].value::<String>()?,
                row["threshold"].value::<f64>()?,
                row["created_at"].value::<pgrx::datum::TimestampWithTimeZone>()?,
            ));
        }
        Ok::<_, pgrx::spi::Error>(result)
    })
    .unwrap_or_default();
    TableIterator::new(rows)
}

/// VH-2 (v0.48.0): Fire distance-predicate NOTIFY for a stream table after a
/// successful refresh.
///
/// Evaluates each distance subscription for the given stream table:
/// counts matching rows and emits `pg_notify(channel, payload)` when at
/// least one row satisfies the predicate.  Errors are logged but never
/// propagated \u2014 subscription notifications must not abort a refresh.
pub(crate) fn fire_distance_subscriptions(
    schema: &str,
    st_name: &str,
    storage_table: &str,
    skip_notify: bool,
) {
    if skip_notify {
        return;
    }
    // Load all distance subscriptions for this stream table.
    let full_name = format!("{schema}.{st_name}");
    let subs: Vec<(String, String, String, String, f64)> = Spi::connect(|client| {
        let tup = client.select(
            "SELECT channel, vector_column, query_vector, op, threshold \
             FROM pgtrickle.pgt_distance_subscriptions \
             WHERE stream_table = $1",
            None,
            &[full_name.as_str().into()],
        )?;
        let mut out: Vec<(String, String, String, String, f64)> = Vec::new();
        for row in tup {
            if let (Some(ch), Some(vc), Some(qv), Some(op), Some(thr)) = (
                row.get::<String>(1).ok().flatten(),
                row.get::<String>(2).ok().flatten(),
                row.get::<String>(3).ok().flatten(),
                row.get::<String>(4).ok().flatten(),
                row.get::<f64>(5).ok().flatten(),
            ) {
                out.push((ch, vc, qv, op, thr));
            }
        }
        Ok::<_, pgrx::spi::Error>(out)
    })
    .unwrap_or_default();

    for (channel, vector_col, query_vector, op, threshold) in subs {
        // Count rows that satisfy the distance predicate.
        // The query_vector is a user-supplied literal; cast to ::vector for pgvector.
        let quoted_storage = format!(
            "{}.{}",
            quote_identifier(schema),
            quote_identifier(storage_table),
        );
        let count_sql = format!(
            "SELECT COUNT(*)::bigint FROM {quoted_storage} \
             WHERE {} {} $1::vector < $2",
            quote_identifier(&vector_col),
            op,
        );
        let matched: i64 = Spi::get_one_with_args::<i64>(
            &count_sql,
            &[query_vector.as_str().into(), threshold.into_datum().into()],
        )
        .unwrap_or(None)
        .unwrap_or(0);

        if matched > 0 {
            let escaped_ch = channel.replace('\'', "''");
            let escaped_name = st_name.replace('\'', "''");
            let escaped_schema = schema.replace('\'', "''");
            let payload = format!(
                "{{\"stream_table\":\"{escaped_name}\",\"schema\":\"{escaped_schema}\",\
                 \"op\":\"{op}\",\"threshold\":{threshold},\"matched_rows\":{matched}}}"
            );
            let escaped_payload = payload.replace('\'', "''");
            let notify_sql = format!("NOTIFY {escaped_ch}, '{escaped_payload}'");
            if let Err(e) = Spi::run(&notify_sql) {
                pgrx::warning!(
                    "pg_trickle: distance subscription NOTIFY failed for {}.{}: {}",
                    schema,
                    st_name,
                    e
                );
            }
        }
    }
}

// ── v0.48.0 embedding_stream_table() ergonomic API (VA-1) ────────────────────

/// VA-1 (v0.48.0): One-call RAG corpus setup.
///
/// Auto-generates a denormalisation query, creates the stream table, provisions
/// HNSW indexes on vector/halfvec/sparsevec output columns, configures
/// `post_refresh_action = 'reindex_if_drift'`, and returns a summary of what
/// was created.
///
/// When `dry_run => true` the function returns the generated SQL without
/// executing it — useful for expert users who want to audit or customise the
/// generated definition before committing.
///
/// # Arguments
/// * `name`            — Name for the new stream table (schema-qualified optional).
/// * `source_table`    — Source table (schema-qualified optional).
/// * `vector_column`   — Column holding the embedding vector.
/// * `extra_columns`   — Comma-separated additional columns to include (default: all).
/// * `refresh_interval`— Refresh schedule (default: `'1m'`).
/// * `index_type`      — Index type: `'hnsw'` or `'ivfflat'` (default: `'hnsw'`).
/// * `dry_run`         — If true, return the SQL instead of executing it.
///
/// # Returns
/// A single-column table with one row per action taken (or SQL line for dry_run).
#[pg_extern(schema = "pgtrickle")]
fn embedding_stream_table(
    name: &str,
    source_table: &str,
    vector_column: &str,
    extra_columns: default!(Option<&str>, "NULL"),
    refresh_interval: default!(&str, "'1m'"),
    index_type: default!(&str, "'hnsw'"),
    dry_run: default!(bool, false),
) -> TableIterator<'static, (name!(action, Option<String>),)> {
    match embedding_stream_table_impl(
        name,
        source_table,
        vector_column,
        extra_columns,
        refresh_interval,
        index_type,
        dry_run,
    ) {
        Ok(rows) => TableIterator::new(rows.into_iter().map(|s| (Some(s),))),
        Err(e) => pgrx::error!("embedding_stream_table: {}", e),
    }
}

fn embedding_stream_table_impl(
    name: &str,
    source_table: &str,
    vector_col: &str,
    extra_columns: Option<&str>,
    refresh_interval: &str,
    index_type: &str,
    dry_run: bool,
) -> Result<Vec<String>, PgTrickleError> {
    // Validate index type.
    let idx_type_lc = index_type.trim_matches('\'').to_ascii_lowercase();
    if idx_type_lc != "hnsw" && idx_type_lc != "ivfflat" {
        return Err(PgTrickleError::InvalidArgument(format!(
            "embedding_stream_table: index_type must be 'hnsw' or 'ivfflat', got '{index_type}'"
        )));
    }

    // Resolve schema / table name for source.
    let src_parts: Vec<&str> = source_table.splitn(2, '.').collect();
    let (src_schema, src_tbl) = match src_parts.len() {
        2 => (src_parts[0].to_string(), src_parts[1].to_string()),
        _ => {
            let schema = Spi::get_one::<String>("SELECT current_schema()::text")
                .unwrap_or(None)
                .unwrap_or_else(|| "public".to_string());
            (schema, source_table.to_string())
        }
    };

    // Resolve destination schema / name.
    let dst_parts: Vec<&str> = name.splitn(2, '.').collect();
    let (dst_schema, dst_name) = match dst_parts.len() {
        2 => (dst_parts[0].to_string(), dst_parts[1].to_string()),
        _ => {
            let schema = Spi::get_one::<String>("SELECT current_schema()::text")
                .unwrap_or(None)
                .unwrap_or_else(|| "public".to_string());
            (schema, name.to_string())
        }
    };

    // Build SELECT list: extra columns + vector column.
    let select_list = match extra_columns {
        Some(cols) if !cols.trim().is_empty() => {
            format!("{}, {}", cols.trim(), quote_identifier(vector_col))
        }
        _ => {
            // Default: include all non-system columns from source.
            let cols_sql = "SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum) \
                 FROM pg_attribute pa \
                 JOIN pg_class pc ON pc.oid = pa.attrelid \
                 JOIN pg_namespace pn ON pn.oid = pc.relnamespace \
                 WHERE pn.nspname = $1 AND pc.relname = $2 \
                   AND pa.attnum > 0 AND NOT pa.attisdropped"
                .to_string();
            Spi::get_one_with_args::<String>(
                &cols_sql,
                &[src_schema.as_str().into(), src_tbl.as_str().into()],
            )
            .unwrap_or(None)
            .unwrap_or_else(|| format!("*, {}", quote_identifier(vector_col)))
        }
    };

    // Build the defining query.
    let defining_query = format!(
        "SELECT {} FROM {}.{}",
        select_list,
        quote_identifier(&src_schema),
        quote_identifier(&src_tbl),
    );

    // Build index access method string.
    let idx_access_method = idx_type_lc.as_str();

    // Generate the statements.
    let create_st_sql = format!(
        "SELECT pgtrickle.create_stream_table({}, $${defining_query}$$, {}, 'DIFFERENTIAL')",
        quote_literal(&format!("{}.{}", dst_schema, dst_name)),
        quote_literal(refresh_interval.trim_matches('\'')),
    );

    let alter_pra_sql = format!(
        "SELECT pgtrickle.alter_stream_table({}, post_refresh_action => 'reindex_if_drift')",
        quote_literal(&format!("{}.{}", dst_schema, dst_name)),
    );

    // Infer vector column type for index operator class.
    let type_sql = "SELECT t.typname::text FROM pg_attribute a \
         JOIN pg_type t ON t.oid = a.atttypid \
         JOIN pg_class c ON c.oid = a.attrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = $3 \
           AND a.attnum > 0 AND NOT a.attisdropped \
         LIMIT 1"
        .to_string();
    let vec_typename = Spi::get_one_with_args::<String>(
        &type_sql,
        &[
            src_schema.as_str().into(),
            src_tbl.as_str().into(),
            vector_col.into(),
        ],
    )
    .unwrap_or(None)
    .unwrap_or_else(|| "vector".to_string());

    // Select operator class based on index type and vector type.
    let opclass = match (idx_access_method, vec_typename.as_str()) {
        ("hnsw", "halfvec") => "halfvec_l2_ops",
        ("hnsw", "sparsevec") => "sparsevec_l2_ops",
        ("ivfflat", "halfvec") => "halfvec_l2_ops",
        ("ivfflat", "sparsevec") => {
            return Err(PgTrickleError::InvalidArgument(
                "embedding_stream_table: ivfflat does not support sparsevec; use hnsw".to_string(),
            ));
        }
        _ => "vector_l2_ops",
    };

    // Look up the source column's format_type (e.g. "vector(2)") so we can
    // build an explicit cast in the index expression.  pgvector HNSW/IVFFlat
    // indexes require the column expression to have explicit dimensions; when
    // pgtrickle creates the stream table it stores only the base type OID
    // (without type modifier), so the stream table column ends up as `vector`
    // (no dims).  Using `(col::vector(2))` in the index expression provides
    // the required dimensions without altering the stream table schema.
    let src_format_type: Option<String> = Spi::get_one_with_args::<String>(
        "SELECT format_type(a.atttypid, a.atttypmod) \
         FROM pg_attribute a \
         JOIN pg_class c ON c.oid = a.attrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = $3 \
           AND a.attnum > 0 AND NOT a.attisdropped \
         LIMIT 1",
        &[
            src_schema.as_str().into(),
            src_tbl.as_str().into(),
            vector_col.into(),
        ],
    )
    .unwrap_or(None);

    // Use an explicit cast when the source type carries dimension info
    // (e.g. "vector(2)" — contains a parenthesised modifier).
    let idx_col_expr = match &src_format_type {
        Some(fmt) if fmt.contains('(') => {
            format!("({}::{})", quote_identifier(vector_col), fmt)
        }
        _ => quote_identifier(vector_col).to_string(),
    };

    let create_idx_sql = format!(
        "CREATE INDEX IF NOT EXISTS {}_{}_idx ON {}.{} \
         USING {} ({idx_col_expr} {opclass})",
        dst_name,
        vector_col,
        quote_identifier(&dst_schema),
        quote_identifier(&dst_name),
        idx_access_method,
    );

    let mut actions: Vec<String> = Vec::new();

    if dry_run {
        actions.push(format!("-- 1. Create stream table\n{create_st_sql}"));
        actions.push(format!("-- 2. Set post-refresh action\n{alter_pra_sql}"));
        actions.push(format!(
            "-- 3. Create {idx_access_method} index\n{create_idx_sql}"
        ));
        return Ok(actions);
    }

    // Execute statements.
    Spi::run(&create_st_sql).map_err(|e| {
        PgTrickleError::SpiError(format!(
            "embedding_stream_table: failed to create stream table: {}",
            e
        ))
    })?;
    actions.push(format!("created stream table {}.{}", dst_schema, dst_name));

    // Set post-refresh action.
    if let Err(e) = Spi::run(&alter_pra_sql) {
        pgrx::warning!(
            "embedding_stream_table: failed to set post_refresh_action: {}",
            e
        );
    } else {
        actions.push("post_refresh_action = reindex_if_drift".to_string());
    }

    // Create vector index (best-effort — the ST may have no rows yet).
    if let Err(e) = Spi::run(&create_idx_sql) {
        pgrx::warning!(
            "embedding_stream_table: {} index creation deferred: {}",
            idx_access_method,
            e
        );
        actions.push(format!(
            "note: {} index creation deferred ({})",
            idx_access_method, e
        ));
    } else {
        actions.push(format!(
            "created {} index on {}.{} ({})",
            idx_access_method, dst_schema, dst_name, vector_col
        ));
    }

    Ok(actions)
}

/// Quote a string as a SQL literal (single-quote escaped).
fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

///
/// Returns per-stream-table statistics: p50/p99 refresh latency, freshness
/// lag, error rate, and remaining error budget.
#[allow(clippy::type_complexity)]
#[pg_extern(schema = "pgtrickle")]
fn sla_summary() -> TableIterator<
    'static,
    (
        name!(stream_table, Option<String>),
        name!(p50_ms, Option<f64>),
        name!(p99_ms, Option<f64>),
        name!(freshness_lag_s, Option<f64>),
        name!(error_rate, Option<f64>),
        name!(error_budget_remaining, Option<f64>),
    ),
> {
    let window_hours = crate::config::pg_trickle_sla_window_hours();
    let rows = Spi::connect(|client| {
        let tup_table = client.select(
            "SELECT
                 COALESCE(schema_name || '.' || stream_table_name, stream_table_name) AS stream_table,
                 PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_ms) AS p50_ms,
                 PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ms) AS p99_ms,
                 EXTRACT(EPOCH FROM (now() - MAX(finished_at))) AS freshness_lag_s,
                 ROUND(COUNT(*) FILTER (WHERE status = 'error')::numeric /
                       NULLIF(COUNT(*), 0), 4)::float8 AS error_rate,
                 GREATEST(0.0,
                     1.0 - ROUND(COUNT(*) FILTER (WHERE status = 'error')::numeric /
                                 NULLIF(COUNT(*), 0), 4))::float8 AS error_budget_remaining
             FROM pgtrickle.pgt_refresh_history
             WHERE start_time >= now() - ($1 || ' hours')::interval
             GROUP BY 1
             ORDER BY 1",
            None,
            &[window_hours.into()],
        )?;
        let mut result: Vec<(
            Option<String>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
        )> = Vec::new();
        for row in tup_table {
            result.push((
                row["stream_table"].value::<String>()?,
                row["p50_ms"].value::<f64>()?,
                row["p99_ms"].value::<f64>()?,
                row["freshness_lag_s"].value::<f64>()?,
                row["error_rate"].value::<f64>()?,
                row["error_budget_remaining"].value::<f64>()?,
            ));
        }
        Ok::<_, pgrx::spi::Error>(result)
    })
    .unwrap_or_default();
    TableIterator::new(rows)
}

// ── v0.35.0 stream table introspection API (A23) ────────────────────────────

/// A23: Explain the DVM operator tree for a stream table.
/// O39-9 (v0.39.0): Return a structured explanation of a stream table's
/// DVM configuration and refresh mode reasoning.
///
/// Returns a text representation of the cached operator tree that the
/// differential maintenance engine uses to evaluate incremental updates.
/// Useful for diagnosing unexpected fallback behaviour or understanding
/// which joins and filters pg_trickle resolves at each hop.
///
/// A44-8 (v0.43.0) extends the output to include:
/// - DVM plan: join strategy (L0/L1+Part3), aggregate mode (algebraic/GROUP_RESCAN)
/// - Fallback reasons (SUM(CASE), window functions, deep joins)
/// - Current GUC threshold values (part3_max_scan_count, deep_join_l0_scan_threshold)
/// - Estimated query complexity class
/// - Suggested tuning actions
///
/// v0.39.0 extends the output to include:
/// - Explicit DIFF/FULL fallback reason from the stream table catalog
/// - Whether `force_full_refresh` GUC is overriding the mode
/// - The effective refresh mode from the last completed refresh cycle
/// - Whether the backpressure or CDC-pause state is active
#[pg_extern(schema = "pgtrickle")]
fn explain_stream_table(name: &str) -> Result<String, PgTrickleError> {
    let (schema, table_name) = parse_qualified_name(name)?;
    let st = StreamTableMeta::get_by_name(&schema, &table_name)?;

    // Determine effective refresh mode and fallback reasoning.
    let force_full = crate::config::pg_trickle_force_full_refresh();
    let cdc_paused = crate::config::pg_trickle_cdc_paused();
    let capture_mode = crate::config::pg_trickle_cdc_capture_mode();
    let backpressure_note = if crate::config::pg_trickle_enforce_backpressure() {
        "enabled (CDC writes suppressed when WAL slot lag exceeds critical threshold)"
    } else {
        "disabled (alerts only)"
    };

    let refresh_mode_note = if force_full {
        "FULL — force_full_refresh GUC override is active (differential disabled globally)"
            .to_string()
    } else {
        format!("{:?} (configured)", st.refresh_mode)
    };

    let cdc_status = if cdc_paused {
        format!(
            "PAUSED (cdc_paused=on, capture_mode={}) — \
             {} while paused",
            capture_mode.as_str(),
            if capture_mode == crate::config::CdcCaptureMode::Discard {
                "changes are DISCARDED; reinitialize after un-pausing"
            } else {
                "changes are HELD in buffer"
            }
        )
    } else {
        "active".to_string()
    };

    // A44-8: Query complexity analysis — detect join depth and aggregate patterns.
    let query_upper = st.defining_query.to_uppercase();
    let join_count = query_upper.matches(" JOIN ").count();
    let has_aggregate = query_upper.contains(" GROUP BY ")
        || query_upper.contains("COUNT(")
        || query_upper.contains("SUM(")
        || query_upper.contains("AVG(")
        || query_upper.contains("MIN(")
        || query_upper.contains("MAX(");
    let has_case_in_agg =
        (query_upper.contains("SUM(CASE") || query_upper.contains("SUM( CASE")) && has_aggregate;
    let has_window = query_upper.contains(" OVER (") || query_upper.contains(" OVER(");
    let has_distinct_agg =
        query_upper.contains("COUNT(DISTINCT") || query_upper.contains("SUM(DISTINCT");
    let has_recursive = query_upper.contains("RECURSIVE ");

    let complexity_class = if has_recursive {
        "recursive"
    } else if join_count >= 6 {
        "very_deep_join"
    } else if join_count >= 4 {
        "deep_join"
    } else if join_count >= 2 {
        "multi_join"
    } else if join_count == 1 {
        "single_join"
    } else if has_aggregate {
        "aggregate_only"
    } else {
        "simple_scan"
    };

    let deep_threshold = crate::config::pg_trickle_deep_join_l0_scan_threshold();
    let part3_threshold = crate::config::pg_trickle_part3_max_scan_count();

    let join_strategy_note = if join_count == 0 {
        "no_join".to_string()
    } else if join_count > deep_threshold {
        format!(
            "L1+Part3 correction (join depth {join_count} > deep_join_l0_scan_threshold {deep_threshold})"
        )
    } else if join_count > part3_threshold {
        format!(
            "L1 without Part3 (join depth {join_count} > part3_max_scan_count {part3_threshold})"
        )
    } else {
        format!("L0 per-leaf snapshot (join depth {join_count} <= threshold {deep_threshold})")
    };

    let aggregate_strategy_note = if !has_aggregate {
        "no_aggregate".to_string()
    } else if has_case_in_agg {
        "GROUP_RESCAN — SUM(CASE) is not algebraically invertible (DI-8)".to_string()
    } else if has_window {
        "GROUP_RESCAN — window functions require full rescan".to_string()
    } else if has_distinct_agg {
        "GROUP_RESCAN — DISTINCT aggregates require full rescan".to_string()
    } else {
        "algebraic — COUNT/SUM algebraically invertible".to_string()
    };

    let mut fallback_reasons: Vec<String> = Vec::new();
    if has_case_in_agg {
        fallback_reasons.push("SUM(CASE) → GROUP_RESCAN (DI-8)".to_string());
    }
    if has_window {
        fallback_reasons.push("window functions → GROUP_RESCAN".to_string());
    }
    if has_distinct_agg {
        fallback_reasons.push("DISTINCT aggregate → GROUP_RESCAN".to_string());
    }
    if join_count > deep_threshold {
        fallback_reasons.push(format!(
            "deep join ({join_count} tables > {deep_threshold}) → L1+Part3"
        ));
    }

    let mut suggestions: Vec<String> = Vec::new();
    if has_case_in_agg {
        suggestions.push(
            "Rewrite SUM(CASE WHEN ...) as a conditional column to enable algebraic maintenance"
                .to_string(),
        );
    }
    if join_count > deep_threshold && join_count <= deep_threshold + 2 {
        suggestions.push(format!(
            "SET pg_trickle.deep_join_l0_scan_threshold = {join_count} \
             to use L1+Part3 for this query depth"
        ));
    }

    let explanation = format!(
        "Stream table: {schema}.{table_name}\n\
         Status:       {status:?}\n\
         Populated:    {populated}\n\
         Refresh mode: {refresh_mode}\n\
         CDC status:   {cdc_status}\n\
         Backpressure: {backpressure}\n\
         \n\
         === A44-8: DVM Plan Analysis ===\n\
         Complexity:        {complexity_class}\n\
         Join count:        {join_count}\n\
         Join strategy:     {join_strategy}\n\
         Aggregate strategy:{aggregate_strategy}\n\
         Fallback reasons:  {fallbacks}\n\
         Suggestions:       {suggestions_text}\n\
         \n\
         === GUC Thresholds ===\n\
         part3_max_scan_count:         {part3_max}\n\
         deep_join_l0_scan_threshold:  {deep_l0}\n\
         wal_max_changes_per_poll:     {wal_max_changes}\n\
         wal_max_lag_bytes:            {wal_max_lag}\n\
         cost_cache_capacity:          {cost_cap}\n\
         \n\
         Defining query:\n\
         {query}",
        schema = schema,
        table_name = table_name,
        status = st.status,
        populated = st.is_populated,
        refresh_mode = refresh_mode_note,
        cdc_status = cdc_status,
        backpressure = backpressure_note,
        complexity_class = complexity_class,
        join_count = join_count,
        join_strategy = join_strategy_note,
        aggregate_strategy = aggregate_strategy_note,
        fallbacks = if fallback_reasons.is_empty() {
            "none".to_string()
        } else {
            fallback_reasons.join("; ")
        },
        suggestions_text = if suggestions.is_empty() {
            "none".to_string()
        } else {
            suggestions.join("; ")
        },
        part3_max = crate::config::pg_trickle_part3_max_scan_count(),
        deep_l0 = crate::config::pg_trickle_deep_join_l0_scan_threshold(),
        wal_max_changes = crate::config::pg_trickle_wal_max_changes_per_poll(),
        wal_max_lag = crate::config::pg_trickle_wal_max_lag_bytes(),
        cost_cap = crate::config::pg_trickle_cost_cache_capacity(),
        query = st.defining_query,
    );
    Ok(explanation)
}

// ── v0.35.0 shadow-ST evolution status API (UX-STATUS) ──────────────────────

/// UX-STATUS: Return the shadow build status for all stream tables.
///
/// During a zero-downtime schema evolution (ALTER STREAM TABLE), pg_trickle
/// builds the new definition in a shadow table.  This function reports which
/// stream tables are currently in a shadow build and how far along they are.
#[allow(clippy::type_complexity)]
#[pg_extern(schema = "pgtrickle")]
fn view_evolution_status() -> TableIterator<
    'static,
    (
        name!(stream_table, Option<String>),
        name!(in_shadow_build, Option<bool>),
        name!(shadow_table_name, Option<String>),
        name!(status, Option<String>),
    ),
> {
    let rows = Spi::connect(|client| {
        let tup_table = client.select(
            "SELECT
                 COALESCE(pgt_schema || '.' || pgt_name, pgt_name) AS stream_table,
                 in_shadow_build,
                 shadow_table_name,
                 status::text
             FROM pgtrickle.pgt_stream_tables
             ORDER BY 1",
            None,
            &[],
        )?;
        let mut result: Vec<(Option<String>, Option<bool>, Option<String>, Option<String>)> =
            Vec::new();
        for row in tup_table {
            result.push((
                row["stream_table"].value::<String>()?,
                row["in_shadow_build"].value::<bool>()?,
                row["shadow_table_name"].value::<String>()?,
                row["status"].value::<String>()?,
            ));
        }
        Ok::<_, pgrx::spi::Error>(result)
    })
    .unwrap_or_default();
    TableIterator::new(rows)
}

// ── v0.36.0: Drain mode (A35) ─────────────────────────────────────────────

/// A35 (v0.36.0): Signal the scheduler to gracefully quiesce and wait for all
/// in-flight refreshes to complete before returning.
///
/// The function signals a drain by incrementing the shared `DRAIN_REQUESTED`
/// epoch counter, then polls `DRAIN_COMPLETED` at 100 ms intervals until it
/// matches the requested epoch or the `timeout_s` deadline expires.
///
/// Returns `true` if the scheduler completed the drain within the timeout,
/// `false` if the deadline was reached with refreshes still in flight.
///
/// # Example
/// ```sql
/// -- Quiesce before pg_upgrade or rolling restart:
/// SELECT pgtrickle.drain();
/// -- Confirm drained:
/// SELECT pgtrickle.is_drained();
/// -- Resume normal operation after maintenance:
/// SELECT pgtrickle.resume_after_drain();
/// ```
#[pg_extern(schema = "pgtrickle")]
fn drain(timeout_s: default!(i32, 60)) -> bool {
    let effective_timeout = if timeout_s == 0 {
        crate::config::pg_trickle_drain_timeout()
    } else {
        timeout_s
    };
    let effective_timeout = match validation::timeout_seconds("drain timeout", effective_timeout) {
        Ok(value) => value,
        Err(e) => raise_error_with_context(e),
    };

    let epoch = match crate::shmem::signal_drain() {
        Some(e) => e,
        None => {
            // Shared memory not available — trivially drained
            return true;
        }
    };

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(effective_timeout);

    loop {
        if crate::shmem::check_drain_completed(epoch) {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        pgrx::check_for_interrupts!();
        unsafe {
            // SAFETY: pg_usleep is PostgreSQL's bounded backend sleep helper.
            pgrx::pg_sys::pg_usleep(100_000);
        }
    }
}

/// A35 (v0.36.0): Returns `true` when the scheduler is drained (all in-flight
/// refreshes have completed and no new cycles are being dispatched).
///
/// A scheduler is considered drained when `DRAIN_COMPLETED >= DRAIN_REQUESTED`
/// in shared memory. This state is reset the next time the scheduler begins a
/// new tick.
#[pg_extern(schema = "pgtrickle")]
fn is_drained() -> Option<bool> {
    crate::shmem::drain_status()
}

/// v0.85.0: Explicitly resume dispatch after a persistent drain.
#[pg_extern(schema = "pgtrickle")]
fn resume_after_drain() -> bool {
    crate::shmem::resume_after_drain()
}

// ── v0.39.0: CDC pause status API (O39-8) ─────────────────────────────────

/// O39-8 (v0.39.0): Return the active CDC capture mode and pause status.
///
/// This function makes the CDC pause state operator-visible so that operators
/// know exactly whether changes are being captured, discarded, or held while
/// `pg_trickle.cdc_paused = on`.
///
/// Returns a table with one row containing:
/// - `paused` — `true` when `cdc_paused = on`
/// - `capture_mode` — `'discard'` or `'hold'`
/// - `note` — human-readable explanation of the current state
#[pg_extern(schema = "pgtrickle")]
fn cdc_pause_status() -> TableIterator<
    'static,
    (
        name!(paused, bool),
        name!(capture_mode, String),
        name!(note, String),
    ),
> {
    let paused = crate::config::pg_trickle_cdc_paused();
    let capture_mode = crate::config::pg_trickle_cdc_capture_mode();
    let mode_str = capture_mode.as_str().to_string();

    let note = if paused {
        match capture_mode {
            crate::config::CdcCaptureMode::Discard => {
                "CDC is PAUSED — changes are DISCARDED while paused. \
                 Stream tables that received DML during the pause will have stale data. \
                 Run pgtrickle.refresh_stream_table('<name>') or reinitialize after un-pausing."
                    .to_string()
            }
            crate::config::CdcCaptureMode::Hold => {
                "CDC is PAUSED in HOLD mode (reserved). Falling back to DISCARD semantics. \
                 Changes arriving now are being DROPPED."
                    .to_string()
            }
        }
    } else {
        "CDC is active — changes are being captured normally.".to_string()
    };

    TableIterator::new(vec![(paused, mode_str, note)])
}

// ── v0.36.0: Bulk operations (A25) ────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BulkAlterStreamTableParams {
    query: Option<String>,
    schedule: Option<String>,
    refresh_mode: Option<String>,
    status: Option<String>,
    diamond_consistency: Option<String>,
    diamond_schedule_policy: Option<String>,
    cdc_mode: Option<String>,
    append_only: Option<bool>,
    pooler_compatibility_mode: Option<bool>,
    tier: Option<String>,
    #[serde(alias = "fuse_mode")]
    fuse: Option<String>,
    fuse_ceiling: Option<i64>,
    fuse_sensitivity: Option<i32>,
    partition_by: Option<String>,
    max_differential_joins: Option<i32>,
    max_delta_fraction: Option<f64>,
    post_refresh_action: Option<String>,
    reindex_drift_threshold: Option<f64>,
}

impl BulkAlterStreamTableParams {
    fn is_empty(&self) -> bool {
        self.query.is_none()
            && self.schedule.is_none()
            && self.refresh_mode.is_none()
            && self.status.is_none()
            && self.diamond_consistency.is_none()
            && self.diamond_schedule_policy.is_none()
            && self.cdc_mode.is_none()
            && self.append_only.is_none()
            && self.pooler_compatibility_mode.is_none()
            && self.tier.is_none()
            && self.fuse.is_none()
            && self.fuse_ceiling.is_none()
            && self.fuse_sensitivity.is_none()
            && self.partition_by.is_none()
            && self.max_differential_joins.is_none()
            && self.max_delta_fraction.is_none()
            && self.post_refresh_action.is_none()
            && self.reindex_drift_threshold.is_none()
    }
}

fn canonicalize_bulk_stream_table_names(
    names: &[Option<String>],
    function_name: &str,
) -> Result<Vec<String>, PgTrickleError> {
    if names.is_empty() {
        return Err(PgTrickleError::InvalidArgument(format!(
            "{function_name}() names array is empty"
        )));
    }
    validation::cardinality(
        &format!("{function_name}() names"),
        names.len(),
        crate::config::pg_trickle_max_bulk_api_items(),
    )?;

    let mut seen = HashSet::with_capacity(names.len());
    let mut canonical_names = Vec::with_capacity(names.len());

    for (index, name_opt) in names.iter().enumerate() {
        let name = name_opt.as_deref().ok_or_else(|| {
            PgTrickleError::InvalidArgument(format!(
                "{function_name}() names[{index}] must not be NULL"
            ))
        })?;
        let (schema, table_name) = parse_qualified_name(name)?;
        let canonical_name = format!("{schema}.{table_name}");
        if !seen.insert(canonical_name.clone()) {
            return Err(PgTrickleError::InvalidArgument(format!(
                "{function_name}() contains duplicate target \"{canonical_name}\""
            )));
        }
        canonical_names.push(canonical_name);
    }

    Ok(canonical_names)
}

fn parse_bulk_alter_stream_table_params(
    params: serde_json::Value,
) -> Result<BulkAlterStreamTableParams, PgTrickleError> {
    let obj = params.as_object().ok_or_else(|| {
        PgTrickleError::InvalidArgument(
            "bulk_alter_stream_tables() expects params to be a JSON object".into(),
        )
    })?;

    if obj.is_empty() {
        return Err(PgTrickleError::InvalidArgument(
            "bulk_alter_stream_tables() params object is empty".into(),
        ));
    }

    if let Some((key, _)) = obj.iter().find(|(_, value)| value.is_null()) {
        return Err(PgTrickleError::InvalidArgument(format!(
            "bulk_alter_stream_tables() params field \"{key}\" must not be null"
        )));
    }

    let parsed: BulkAlterStreamTableParams = serde_json::from_value(params).map_err(|e| {
        PgTrickleError::InvalidArgument(format!(
            "bulk_alter_stream_tables() invalid params JSON: {e}"
        ))
    })?;

    if parsed.is_empty() {
        return Err(PgTrickleError::InvalidArgument(
            "bulk_alter_stream_tables() params object is empty".into(),
        ));
    }
    if let Some(value) = parsed.max_differential_joins {
        validation::nonnegative_i32("max_differential_joins", i64::from(value))?;
    }
    if let Some(value) = parsed.max_delta_fraction {
        validation::finite_fraction("max_delta_fraction", value)?;
    }
    if let Some(value) = parsed.fuse_ceiling
        && value <= 0
    {
        return Err(PgTrickleError::InvalidArgument(
            "fuse_ceiling must be a positive integer".into(),
        ));
    }
    if let Some(value) = parsed.fuse_sensitivity
        && value <= 0
    {
        return Err(PgTrickleError::InvalidArgument(
            "fuse_sensitivity must be a positive integer".into(),
        ));
    }
    if let Some(value) = parsed.reindex_drift_threshold
        && (!value.is_finite() || !(0.0..=1.0).contains(&value) || value == 0.0)
    {
        return Err(PgTrickleError::InvalidArgument(
            "reindex_drift_threshold must be finite and between 0.0 and 1.0".into(),
        ));
    }

    Ok(parsed)
}

fn build_bulk_alter_stream_table_options<'a>(
    name: &'a str,
    params: &'a BulkAlterStreamTableParams,
) -> alter::AlterStreamTableOptions<'a> {
    alter::AlterStreamTableOptions {
        name,
        query: params.query.as_deref(),
        schedule: params.schedule.as_deref(),
        refresh_mode: params.refresh_mode.as_deref(),
        status: params.status.as_deref(),
        diamond_consistency: params.diamond_consistency.as_deref(),
        diamond_schedule_policy: params.diamond_schedule_policy.as_deref(),
        cdc_mode: params.cdc_mode.as_deref(),
        append_only: params.append_only,
        pooler_compatibility_mode: params.pooler_compatibility_mode,
        tier: params.tier.as_deref(),
        fuse: params.fuse.as_deref(),
        fuse_ceiling: params.fuse_ceiling,
        fuse_sensitivity: params.fuse_sensitivity,
        partition_by: params.partition_by.as_deref(),
        max_differential_joins: params.max_differential_joins,
        max_delta_fraction: params.max_delta_fraction,
        post_refresh_action: params.post_refresh_action.as_deref(),
        reindex_drift_threshold: params.reindex_drift_threshold,
        target_freshness: None,
    }
}

fn bulk_alter_stream_tables_impl(
    names: Vec<Option<String>>,
    params: serde_json::Value,
) -> Result<i32, PgTrickleError> {
    let canonical_names = canonicalize_bulk_stream_table_names(&names, "bulk_alter_stream_tables")?;
    let params = parse_bulk_alter_stream_table_params(params)?;

    for name in &canonical_names {
        alter::prevalidate_stream_table_target(name)?;
    }

    for name in &canonical_names {
        alter::alter_stream_table_impl(build_bulk_alter_stream_table_options(name, &params))?;
    }

    Ok(canonical_names.len() as i32)
}

fn bulk_drop_stream_tables_impl(names: Vec<Option<String>>) -> Result<i32, PgTrickleError> {
    let canonical_names = canonicalize_bulk_stream_table_names(&names, "bulk_drop_stream_tables")?;
    let ordered_names = alter::plan_drop_stream_tables(&canonical_names)?;

    for name in &ordered_names {
        alter::drop_stream_table_impl(name, false)?;
    }

    Ok(ordered_names.len() as i32)
}

/// A25 (v0.36.0): Alter multiple stream tables in a single call.
///
/// Applies the same parameter set to each stream table in `names`. The
/// `params` JSONB object accepts the same keys as `alter_stream_table()`:
/// `schedule`, `refresh_mode`, `tier`, `fuse_mode`, `fuse_ceiling`, etc.
///
/// Validates the full batch before mutating any table and aborts atomically on
/// the first error.
///
/// Returns the number of stream tables altered.
///
/// # Example
/// ```sql
/// SELECT pgtrickle.bulk_alter_stream_tables(
///     ARRAY['public.orders_summary', 'public.daily_revenue'],
///     '{"schedule": "5m", "tier": "warm"}'::jsonb
/// );
/// ```
#[pg_extern(schema = "pgtrickle")]
fn bulk_alter_stream_tables(names: Vec<Option<String>>, params: pgrx::Json) -> i32 {
    match bulk_alter_stream_tables_impl(names, params.0) {
        Ok(count) => count,
        Err(e) => raise_error_with_context(e),
    }
}

/// A25 (v0.36.0): Drop multiple stream tables in a single call.
///
/// Drops each stream table in `names`, stopping CDC and removing the storage
/// table. The batch is validated up front and then executed in dependency-aware
/// order so a parent and its descendants can be dropped together with RESTRICT
/// semantics.
///
/// Returns the number of stream tables dropped.
///
/// # Example
/// ```sql
/// SELECT pgtrickle.bulk_drop_stream_tables(
///     ARRAY['public.orders_summary', 'public.stale_view']
/// );
/// ```
#[pg_extern(schema = "pgtrickle")]
fn bulk_drop_stream_tables(names: Vec<Option<String>>) -> i32 {
    match bulk_drop_stream_tables_impl(names) {
        Ok(count) => count,
        Err(e) => raise_error_with_context(e),
    }
}

// ── v0.36.0: CREATE STREAM TABLE SQL syntax (F11) ─────────────────────────

/// F11 (v0.36.0): Execute a `CREATE [OR REPLACE] STREAM TABLE` SQL statement.
///
/// Parses the custom `CREATE STREAM TABLE name AS SELECT ...` syntax (which
/// PostgreSQL's parser does not understand natively) and translates it to a
/// `pgtrickle.create_stream_table()` call.
///
/// Supported syntax variants:
/// ```sql
/// CREATE STREAM TABLE my_st AS SELECT ...;
/// CREATE OR REPLACE STREAM TABLE my_st AS SELECT ...;
/// DROP STREAM TABLE my_st;
/// ```
///
/// All `create_stream_table()` options default to their standard defaults.
/// For full control, call `pgtrickle.create_stream_table()` directly.
///
/// # Example
/// ```sql
/// SELECT pgtrickle.exec_stream_ddl(
///   'CREATE STREAM TABLE revenue AS SELECT SUM(amount) FROM orders'
/// );
/// ```
#[pg_extern(schema = "pgtrickle")]
fn exec_stream_ddl(cmd: &str) -> bool {
    let cmd = cmd.trim();
    let upper = cmd.to_uppercase();

    // ── DROP STREAM TABLE ──
    if upper.starts_with("DROP STREAM TABLE") {
        let rest = cmd[17..].trim(); // after "DROP STREAM TABLE"
        let name = rest.trim_end_matches(';').trim();
        if name.is_empty() {
            pgrx::error!("exec_stream_ddl: missing table name in DROP STREAM TABLE");
        }
        // Use $1 parameterized call to avoid SQL injection.
        Spi::run_with_args("SELECT pgtrickle.drop_stream_table($1)", &[name.into()])
            .unwrap_or_else(|e| pgrx::error!("exec_stream_ddl DROP: {}", e));
        return true;
    }

    // ── CREATE [OR REPLACE] STREAM TABLE ──
    let (or_replace, rest) = if upper.starts_with("CREATE OR REPLACE STREAM TABLE ") {
        (true, &cmd[31..]) // after "CREATE OR REPLACE STREAM TABLE "
    } else if upper.starts_with("CREATE STREAM TABLE ") {
        (false, &cmd[20..]) // after "CREATE STREAM TABLE "
    } else {
        pgrx::error!(
            "exec_stream_ddl: unrecognised syntax. Expected \
             'CREATE [OR REPLACE] STREAM TABLE name AS SELECT ...' \
             or 'DROP STREAM TABLE name'."
        );
    };

    // Split name from "AS SELECT ..."
    let upper_rest = rest.to_uppercase();
    let as_idx = upper_rest.find(" AS ").unwrap_or_else(|| {
        pgrx::error!(
            "exec_stream_ddl: missing AS keyword. Syntax: CREATE STREAM TABLE name AS SELECT ..."
        )
    });
    let name = rest[..as_idx].trim();
    let query = rest[as_idx + 4..].trim().trim_end_matches(';').trim();

    if name.is_empty() {
        pgrx::error!("exec_stream_ddl: missing table name");
    }
    if query.is_empty() {
        pgrx::error!("exec_stream_ddl: missing query after AS");
    }

    if or_replace {
        Spi::run_with_args(
            "SELECT pgtrickle.create_or_replace_stream_table($1, $2)",
            &[name.into(), query.into()],
        )
        .unwrap_or_else(|e| pgrx::error!("exec_stream_ddl CREATE OR REPLACE: {}", e));
    } else {
        Spi::run_with_args(
            "SELECT pgtrickle.create_stream_table($1, $2)",
            &[name.into(), query.into()],
        )
        .unwrap_or_else(|e| pgrx::error!("exec_stream_ddl CREATE: {}", e));
    }

    true
}

// ── v0.36.0: Column lineage (F12) ─────────────────────────────────────────

/// F12 (v0.36.0): Return the column lineage for a stream table.
///
/// Reports the `column_lineage` JSON recorded in `pgt_stream_tables` at
/// creation time, mapping each output column to its source table and column.
/// When no lineage information is available (e.g., complex expressions),
/// `source_table` and `source_col` are `NULL`.
///
/// # Example
/// ```sql
/// SELECT * FROM pgtrickle.stream_table_lineage('public.revenue_summary');
/// ```
#[pg_extern(schema = "pgtrickle")]
fn stream_table_lineage(
    name: &str,
) -> TableIterator<
    'static,
    (
        name!(output_col, Option<String>),
        name!(source_table, Option<String>),
        name!(source_col, Option<String>),
    ),
> {
    let (schema, tbl) = if let Some((s, t)) = name.split_once('.') {
        (s.to_string(), t.to_string())
    } else {
        ("public".to_string(), name.to_string())
    };

    let rows: Vec<(Option<String>, Option<String>, Option<String>)> = Spi::connect(|client| {
        // Retrieve the column_lineage JSON from the catalog.
        let result = client.select(
            "SELECT column_lineage FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_schema = $1 AND pgt_name = $2",
            None,
            &[schema.clone().into(), tbl.clone().into()],
        )?;

        if result.is_empty() {
            return Ok::<Vec<(Option<String>, Option<String>, Option<String>)>, pgrx::spi::SpiError>(
                Vec::new(),
            );
        }

        let first = result.first();
        let lineage_json: Option<pgrx::Json> = first.get::<pgrx::Json>(1)?;

        let lineage = match lineage_json {
            Some(j) => j.0,
            None => {
                return Ok::<
                    Vec<(Option<String>, Option<String>, Option<String>)>,
                    pgrx::spi::SpiError,
                >(Vec::new());
            }
        };

        // Expected JSON format:
        // [{"output_col": "revenue", "source_table": "orders", "source_col": "amount"}, ...]
        let mut rows = Vec::new();
        if let serde_json::Value::Array(entries) = lineage {
            for entry in entries {
                let output_col = entry["output_col"].as_str().map(str::to_owned);
                let source_table = entry["source_table"].as_str().map(str::to_owned);
                let source_col = entry["source_col"].as_str().map(str::to_owned);
                rows.push((output_col, source_table, source_col));
            }
        } else if let serde_json::Value::Object(obj) = lineage {
            // Also support {"col": {"source_table": ..., "source_col": ...}} format
            for (col, info) in obj {
                let source_table = info["source_table"].as_str().map(str::to_owned);
                let source_col = info["source_col"].as_str().map(str::to_owned);
                rows.push((Some(col), source_table, source_col));
            }
        }
        Ok(rows)
    })
    .unwrap_or_default();

    TableIterator::new(rows)
}

pub mod alter;
/// Return the pg_trickle extension version (from `Cargo.toml`).
///
/// This matches the version reported by `pg_extension.extversion`.
// ── Sub-modules ─────────────────────────────────────────────────────────────
pub mod create;
pub mod refresh_ops;

// Re-export from submodules for backward-compatible crate::api:: paths.
pub use refresh_ops::reinit_rewrite_if_needed;

mod diagnostics;
pub mod helpers;
pub(crate) mod metrics_ext;
pub(crate) mod planner;
// LSEC-1 (v0.87.7): exact caller-identity/search_path capture, used by
// create_stream_table and alter_stream_table's query migration below.
// LSEC-2's owner-context primitives are used by every refresh path from
// v0.87.8 onward and by lifecycle API hardening in v0.87.9+.
pub(crate) mod security_context;
mod self_monitoring;
pub(crate) mod snapshot;

// Re-export public items from sub-modules so external callers are unaffected.
pub use helpers::*;

/// Resolve a (possibly schema-qualified) table name to its OID.
#[cfg(test)]
mod tests {
    use super::create::{bulk_create_impl, compute_config_diff, normalize_sql_whitespace};
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn test_inject_pgt_count_distinct_basic() {
        let query = "SELECT DISTINCT color, size FROM prop_dist";
        let result = inject_pgt_count(query);
        assert_eq!(
            result,
            "SELECT color, size, COUNT(*) AS __pgt_count FROM prop_dist GROUP BY color, size"
        );
    }

    #[test]
    fn test_inject_pgt_count_distinct_lowercase() {
        let query = "select distinct color, size from prop_dist";
        let result = inject_pgt_count(query);
        // Note: "SELECT" is uppercased because detect_and_strip_distinct
        // reconstructs the prefix with literal "SELECT".
        assert_eq!(
            result,
            "SELECT color, size, COUNT(*) AS __pgt_count from prop_dist GROUP BY color, size"
        );
    }

    #[test]
    fn test_inject_pgt_count_distinct_with_where() {
        let query = "SELECT DISTINCT color FROM items WHERE active = true";
        let result = inject_pgt_count(query);
        assert_eq!(
            result,
            "SELECT color, COUNT(*) AS __pgt_count FROM items WHERE active = true GROUP BY color"
        );
    }

    #[test]
    fn test_inject_pgt_count_aggregate_no_distinct() {
        // Non-DISTINCT aggregate: just adds COUNT(*) before FROM
        let query = "SELECT region, SUM(amount) FROM orders GROUP BY region";
        let result = inject_pgt_count(query);
        assert_eq!(
            result,
            "SELECT region, SUM(amount), COUNT(*) AS __pgt_count FROM orders GROUP BY region"
        );
    }

    #[test]
    fn test_inject_pgt_count_distinct_three_cols() {
        let query = "SELECT DISTINCT a, b, c FROM t1";
        let result = inject_pgt_count(query);
        assert_eq!(
            result,
            "SELECT a, b, c, COUNT(*) AS __pgt_count FROM t1 GROUP BY a, b, c"
        );
    }

    #[test]
    fn test_detect_and_strip_distinct_none_for_non_distinct() {
        let query = "SELECT color, size FROM prop_dist";
        assert!(detect_and_strip_distinct(query).is_none());
    }

    #[test]
    fn test_detect_and_strip_distinct_basic() {
        let query = "SELECT DISTINCT color, size FROM prop_dist";
        let info = detect_and_strip_distinct(query).unwrap();
        assert_eq!(info.columns, vec!["color", "size"]);
        assert!(info.stripped.contains("SELECT"));
        assert!(!info.stripped.to_uppercase().contains("DISTINCT"));
    }

    #[test]
    fn test_split_top_level_commas() {
        let cols = split_top_level_commas("a, b, c");
        assert_eq!(cols, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_split_top_level_commas_with_parens() {
        let cols = split_top_level_commas("a, COALESCE(b, 0), c");
        assert_eq!(cols, vec!["a", "COALESCE(b, 0)", "c"]);
    }

    // ── quote_identifier tests ─────────────────────────────────────────

    #[test]
    fn test_quote_identifier_simple() {
        assert_eq!(quote_identifier("my_table"), "\"my_table\"");
    }

    #[test]
    fn test_quote_identifier_with_double_quotes() {
        // Embedded double quotes must be escaped by doubling
        assert_eq!(quote_identifier(r#"weird"name"#), r#""weird""name""#);
    }

    #[test]
    fn test_quote_identifier_reserved_word() {
        assert_eq!(quote_identifier("select"), "\"select\"");
    }

    // ── get_data_timestamp_str tests ───────────────────────────────────

    #[test]
    fn test_get_data_timestamp_str_suffix() {
        let ts = get_data_timestamp_str();
        assert!(ts.ends_with('Z'), "expected trailing Z, got: {}", ts);
    }

    #[test]
    fn test_get_data_timestamp_str_reasonable_epoch() {
        let ts = get_data_timestamp_str();
        let numeric = ts.trim_end_matches('Z');
        let secs: u64 = numeric.parse().expect("should be numeric epoch seconds");
        // Epoch should be > 2024-01-01 (1704067200) and < 2040-01-01 (2208988800)
        assert!(secs > 1_704_067_200, "epoch too old: {}", secs);
        assert!(secs < 2_208_988_800, "epoch too far in future: {}", secs);
    }

    // ── find_top_level_keyword tests ───────────────────────────────────

    #[test]
    fn test_find_top_level_keyword_basic() {
        let sql = "SELECT a, b FROM t1 WHERE x > 0";
        assert_eq!(find_top_level_keyword(sql, "FROM"), Some(12));
    }

    #[test]
    fn test_find_top_level_keyword_inside_parens_ignored() {
        // FROM inside parentheses should NOT be found
        let sql = "SELECT (SELECT x FROM y) AS sub FROM t1";
        let pos = find_top_level_keyword(sql, "FROM").unwrap();
        // Should find the outer FROM, not the inner one
        assert!(pos > 24, "found inner FROM at {}, expected outer", pos);
    }

    #[test]
    fn test_find_top_level_keyword_inside_string_ignored() {
        let sql = "SELECT 'FROM' AS label FROM t1";
        let pos = find_top_level_keyword(sql, "FROM").unwrap();
        // Should skip the 'FROM' string literal and find the real FROM
        assert!(pos > 20, "found FROM inside string at {}", pos);
    }

    #[test]
    fn test_find_top_level_keyword_case_insensitive() {
        let sql = "select a from t1";
        assert!(find_top_level_keyword(sql, "FROM").is_some());
    }

    #[test]
    fn test_find_top_level_keyword_not_found() {
        let sql = "SELECT a, b FROM t1";
        assert_eq!(find_top_level_keyword(sql, "WHERE"), None);
    }

    #[test]
    fn test_find_top_level_keyword_word_boundary() {
        // "FROMAGE" contains "FROM" but should not match due to word boundary
        let sql = "SELECT FROMAGE FROM t1";
        let pos = find_top_level_keyword(sql, "FROM").unwrap();
        // Should match the standalone FROM, not FROMAGE
        assert!(pos > 7, "matched FROM inside FROMAGE at {}", pos);
    }

    // ── split_top_level_commas additional edge cases ───────────────────

    #[test]
    fn test_split_top_level_commas_nested_function() {
        let cols = split_top_level_commas("SUM(CASE WHEN x > 0 THEN 1 ELSE 0 END), COUNT(*)");
        assert_eq!(
            cols,
            vec!["SUM(CASE WHEN x > 0 THEN 1 ELSE 0 END)", "COUNT(*)"]
        );
    }

    #[test]
    fn test_split_top_level_commas_quoted_string_with_comma() {
        let cols = split_top_level_commas("'hello, world', b");
        assert_eq!(cols, vec!["'hello, world'", "b"]);
    }

    #[test]
    fn test_split_top_level_commas_empty_input() {
        let cols = split_top_level_commas("");
        assert!(cols.is_empty());
    }

    #[test]
    fn test_split_top_level_commas_single_column() {
        let cols = split_top_level_commas("  a  ");
        assert_eq!(cols, vec!["a"]);
    }

    // ── parse_duration tests ───────────────────────────────────────────

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_duration("30s").unwrap(), 30);
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(parse_duration("5m").unwrap(), 300);
    }

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(parse_duration("2h").unwrap(), 7200);
    }

    #[test]
    fn test_parse_duration_days() {
        assert_eq!(parse_duration("1d").unwrap(), 86400);
    }

    #[test]
    fn test_parse_duration_weeks() {
        assert_eq!(parse_duration("1w").unwrap(), 604800);
    }

    #[test]
    fn test_parse_duration_compound() {
        assert_eq!(parse_duration("1h30m").unwrap(), 5400);
        assert_eq!(parse_duration("2m30s").unwrap(), 150);
        assert_eq!(parse_duration("1d12h").unwrap(), 129600);
    }

    #[test]
    fn test_parse_duration_bare_integer() {
        assert_eq!(parse_duration("60").unwrap(), 60);
        assert_eq!(parse_duration("0").unwrap(), 0);
    }

    #[test]
    fn test_parse_duration_zero() {
        assert_eq!(parse_duration("0s").unwrap(), 0);
        assert_eq!(parse_duration("0m").unwrap(), 0);
    }

    #[test]
    fn test_parse_duration_whitespace_trimmed() {
        assert_eq!(parse_duration("  5m  ").unwrap(), 300);
    }

    #[test]
    fn test_parse_duration_empty_fails() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("  ").is_err());
    }

    #[test]
    fn test_parse_duration_invalid_unit_fails() {
        assert!(parse_duration("5x").is_err());
    }

    #[test]
    fn test_parse_duration_no_number_before_unit_fails() {
        assert!(parse_duration("m").is_err());
    }

    #[test]
    fn test_parse_duration_trailing_digits_fails() {
        assert!(parse_duration("1h30").is_err());
    }

    #[test]
    fn test_parse_duration_negative_bare_fails() {
        assert!(parse_duration("-60").is_err());
    }

    // ── parse_schedule tests ────────────────────────────────────────────
    // Note: parse_schedule for *duration* strings calls validate_schedule,
    // which accesses a GUC. Duration-based scheduling is already tested via
    // the parse_duration tests above. Here we only test cron parsing and the
    // cron detection heuristic.

    #[test]
    fn test_parse_schedule_cron_every_5_min() {
        let schedule = parse_schedule("*/5 * * * *").unwrap();
        assert_eq!(schedule, Schedule::Cron("*/5 * * * *".to_string()));
    }

    #[test]
    fn test_parse_schedule_cron_hourly_alias() {
        let schedule = parse_schedule("@hourly").unwrap();
        assert_eq!(schedule, Schedule::Cron("@hourly".to_string()));
    }

    #[test]
    fn test_parse_schedule_cron_daily_alias() {
        let schedule = parse_schedule("@daily").unwrap();
        assert_eq!(schedule, Schedule::Cron("@daily".to_string()));
    }

    #[test]
    fn test_parse_schedule_cron_weekly_alias() {
        let schedule = parse_schedule("@weekly").unwrap();
        assert_eq!(schedule, Schedule::Cron("@weekly".to_string()));
    }

    #[test]
    fn test_parse_schedule_cron_specific_time() {
        let schedule = parse_schedule("0 6 * * 1-5").unwrap();
        assert_eq!(schedule, Schedule::Cron("0 6 * * 1-5".to_string()));
    }

    #[test]
    fn test_parse_schedule_cron_six_field() {
        // 6-field cron (with seconds)
        let schedule = parse_schedule("0 */5 * * * *").unwrap();
        assert_eq!(schedule, Schedule::Cron("0 */5 * * * *".to_string()));
    }

    #[test]
    fn test_parse_schedule_invalid_cron() {
        // Invalid cron expression (only 4 fields)
        assert!(parse_schedule("* * * *").is_err());
    }

    #[test]
    fn test_parse_schedule_invalid_duration() {
        assert!(parse_schedule("abc").is_err());
    }

    #[test]
    fn test_parse_schedule_empty_fails() {
        assert!(parse_schedule("").is_err());
    }

    #[test]
    fn test_parse_schedule_whitespace_trimmed() {
        let schedule = parse_schedule("  @hourly  ").unwrap();
        assert_eq!(schedule, Schedule::Cron("@hourly".to_string()));
    }

    // ── validate_cron tests ─────────────────────────────────────────────

    #[test]
    fn test_validate_cron_standard_expressions() {
        assert!(validate_cron("* * * * *").is_ok());
        assert!(validate_cron("0 0 * * *").is_ok());
        assert!(validate_cron("*/15 * * * *").is_ok());
        assert!(validate_cron("0 9-17 * * 1-5").is_ok());
    }

    #[test]
    fn test_validate_cron_aliases() {
        assert!(validate_cron("@yearly").is_ok());
        assert!(validate_cron("@monthly").is_ok());
        assert!(validate_cron("@weekly").is_ok());
        assert!(validate_cron("@daily").is_ok());
        assert!(validate_cron("@hourly").is_ok());
    }

    #[test]
    fn test_validate_cron_invalid() {
        assert!(validate_cron("not a cron").is_err());
        assert!(validate_cron("99 99 99 99 99").is_err());
    }

    // ── cron_is_due tests ───────────────────────────────────────────────

    #[test]
    fn test_cron_is_due_no_previous_refresh() {
        // If never refreshed, should always be due
        assert!(cron_is_due("* * * * *", None));
    }

    #[test]
    fn test_cron_is_due_recently_refreshed() {
        // If refreshed just now, should not be due (unless cron fires every second)
        let now_epoch = chrono::Utc::now().timestamp();
        // Cron = hourly → next occurrence is ~60 min from now
        assert!(!cron_is_due("@hourly", Some(now_epoch)));
    }

    #[test]
    fn test_cron_is_due_long_ago_refresh() {
        // If refreshed long ago, should be due
        let old_epoch = chrono::Utc::now().timestamp() - 86400; // 24h ago
        // Cron = hourly → should definitely be due
        assert!(cron_is_due("@hourly", Some(old_epoch)));
    }

    #[test]
    fn test_cron_is_due_invalid_expr_returns_false() {
        // Invalid cron should gracefully return false
        assert!(!cron_is_due("invalid cron", None));
    }

    // ── Additional parse_duration edge-case tests ────────────────────────

    #[test]
    fn test_parse_duration_large_compound() {
        // 1 week + 2 days + 3 hours + 4 minutes + 5 seconds
        assert_eq!(
            parse_duration("1w2d3h4m5s").unwrap(),
            604800 + 2 * 86400 + 3 * 3600 + 4 * 60 + 5
        );
    }

    #[test]
    fn test_parse_duration_repeated_units() {
        // Repeated same-unit segments accumulate
        assert_eq!(parse_duration("1m1m1m").unwrap(), 180);
    }

    #[test]
    fn test_parse_duration_only_hours_and_seconds() {
        assert_eq!(parse_duration("2h15s").unwrap(), 7215);
    }

    #[test]
    fn test_parse_duration_large_numbers() {
        assert_eq!(parse_duration("999s").unwrap(), 999);
        assert_eq!(parse_duration("100m").unwrap(), 6000);
    }

    #[test]
    fn test_parse_duration_single_digit_units() {
        assert_eq!(parse_duration("1s").unwrap(), 1);
        assert_eq!(parse_duration("1m").unwrap(), 60);
        assert_eq!(parse_duration("1h").unwrap(), 3600);
        assert_eq!(parse_duration("1d").unwrap(), 86400);
        assert_eq!(parse_duration("1w").unwrap(), 604800);
    }

    #[test]
    fn test_parse_duration_mixed_invalid_char_fails() {
        assert!(parse_duration("5m+3s").is_err());
        assert!(parse_duration("5m 3s").is_err()); // space treated as cron by parse_schedule
        assert!(parse_duration("5M").is_err()); // uppercase
    }

    // ── Additional validate_cron tests ───────────────────────────────────

    #[test]
    fn test_validate_cron_ranges_and_lists() {
        // Day-of-week range
        assert!(validate_cron("0 9 * * 1-5").is_ok());
        // Comma-separated list
        assert!(validate_cron("0 0 1,15 * *").is_ok());
        // Step + range
        assert!(validate_cron("0 */6 * * *").is_ok());
    }

    #[test]
    fn test_validate_cron_out_of_range() {
        // Minute 60 is invalid (range 0-59)
        assert!(validate_cron("60 * * * *").is_err());
        // Hour 25 is invalid
        assert!(validate_cron("0 25 * * *").is_err());
    }

    #[test]
    fn test_validate_cron_too_few_fields() {
        assert!(validate_cron("* *").is_err());
        assert!(validate_cron("*").is_err());
    }

    #[test]
    fn test_validate_cron_aliases_exhaustive() {
        assert!(validate_cron("@annually").is_ok());
    }

    // ── Additional parse_schedule tests ──────────────────────────────────

    #[test]
    fn test_parse_schedule_cron_with_ranges_and_steps() {
        let schedule = parse_schedule("*/10 9-17 * * 1-5").unwrap();
        assert_eq!(schedule, Schedule::Cron("*/10 9-17 * * 1-5".to_string()));
    }

    #[test]
    fn test_parse_schedule_cron_monthly_alias() {
        let schedule = parse_schedule("@monthly").unwrap();
        assert_eq!(schedule, Schedule::Cron("@monthly".to_string()));
    }

    #[test]
    fn test_parse_schedule_cron_yearly_alias() {
        let schedule = parse_schedule("@yearly").unwrap();
        assert_eq!(schedule, Schedule::Cron("@yearly".to_string()));
    }

    #[test]
    fn test_parse_schedule_cron_annually_alias() {
        let schedule = parse_schedule("@annually").unwrap();
        assert_eq!(schedule, Schedule::Cron("@annually".to_string()));
    }

    #[test]
    fn test_parse_schedule_cron_comma_list() {
        let schedule = parse_schedule("0 0 1,15 * *").unwrap();
        assert_eq!(schedule, Schedule::Cron("0 0 1,15 * *".to_string()));
    }

    #[test]
    fn test_parse_schedule_invalid_cron_bad_field_count() {
        // 3 fields — too few for cron
        assert!(parse_schedule("* * *").is_err());
    }

    #[test]
    fn test_parse_schedule_invalid_cron_bad_range() {
        // Minute 99 is out of range
        assert!(parse_schedule("99 * * * *").is_err());
    }

    // ── Additional cron_is_due tests ────────────────────────────────────

    #[test]
    fn test_cron_is_due_every_minute_recently_refreshed() {
        let now_epoch = chrono::Utc::now().timestamp();
        // Every-minute cron: next fire is ~60s away, so if refreshed
        // just now it should NOT be due (within the minute)
        // But if refreshed 2 minutes ago, should be due
        let old_epoch = now_epoch - 120;
        assert!(cron_is_due("* * * * *", Some(old_epoch)));
    }

    #[test]
    fn test_cron_is_due_yearly_recently_refreshed() {
        let now_epoch = chrono::Utc::now().timestamp();
        // Yearly cron: next fire is ~1 year away
        assert!(!cron_is_due("@yearly", Some(now_epoch)));
    }

    #[test]
    fn test_cron_is_due_with_alias() {
        let old_epoch = chrono::Utc::now().timestamp() - 7200; // 2 hours ago
        assert!(cron_is_due("@hourly", Some(old_epoch)));
    }

    // ── EC-15: detect_select_star unit tests ───────────────────────────

    #[test]
    fn test_detect_select_star_bare() {
        assert!(detect_select_star("SELECT * FROM t"));
    }

    #[test]
    fn test_detect_select_star_table_qualified() {
        assert!(detect_select_star("SELECT t.* FROM t"));
    }

    #[test]
    fn test_detect_select_star_explicit_columns_no_star() {
        assert!(!detect_select_star("SELECT id, name FROM t"));
    }

    #[test]
    fn test_detect_select_star_count_star_not_flagged() {
        assert!(!detect_select_star("SELECT count(*) FROM t"));
    }

    #[test]
    fn test_detect_select_star_sum_star_not_flagged() {
        assert!(!detect_select_star("SELECT sum(*) FROM t"));
    }

    #[test]
    fn test_detect_select_star_mixed_agg_and_bare_star() {
        // `count(*), *` has a bare `*` at top level
        assert!(detect_select_star("SELECT count(*), * FROM t"));
    }

    #[test]
    fn test_detect_select_star_no_asterisk_at_all() {
        assert!(!detect_select_star("SELECT id FROM t"));
    }

    #[test]
    fn test_detect_select_star_subquery_star_ignored() {
        // The outer SELECT has explicit columns; the inner SELECT * is inside parens
        assert!(!detect_select_star(
            "SELECT id, (SELECT * FROM b LIMIT 1) FROM t"
        ));
    }

    #[test]
    fn test_detect_select_star_case_insensitive() {
        assert!(detect_select_star("select * from t"));
        assert!(detect_select_star("SELECT * FROM t"));
        assert!(detect_select_star("Select * From t"));
    }

    #[test]
    fn test_detect_select_star_no_from() {
        // No FROM clause — should not crash, just return false
        assert!(!detect_select_star("SELECT 1"));
    }

    #[test]
    fn test_classify_cdc_refresh_mode_interaction_global_wal_immediate() {
        assert_eq!(
            classify_cdc_refresh_mode_interaction(
                RefreshMode::Immediate,
                "wal",
                CdcModeRequestSource::GlobalGuc,
            ),
            CdcRefreshModeInteraction::IgnoreWalForImmediate
        );
    }

    #[test]
    fn test_classify_cdc_refresh_mode_interaction_explicit_wal_immediate() {
        assert_eq!(
            classify_cdc_refresh_mode_interaction(
                RefreshMode::Immediate,
                "wal",
                CdcModeRequestSource::ExplicitOverride,
            ),
            CdcRefreshModeInteraction::RejectWalForImmediate
        );
    }

    #[test]
    fn test_classify_cdc_refresh_mode_interaction_non_immediate_is_none() {
        assert_eq!(
            classify_cdc_refresh_mode_interaction(
                RefreshMode::Differential,
                "wal",
                CdcModeRequestSource::GlobalGuc,
            ),
            CdcRefreshModeInteraction::None
        );
    }

    #[test]
    fn test_classify_cdc_refresh_mode_interaction_immediate_non_wal_is_none() {
        assert_eq!(
            classify_cdc_refresh_mode_interaction(
                RefreshMode::Immediate,
                "auto",
                CdcModeRequestSource::GlobalGuc,
            ),
            CdcRefreshModeInteraction::None
        );
    }

    // ── compute_config_diff tests ──────────────────────────────────────

    fn make_test_st() -> StreamTableMeta {
        StreamTableMeta {
            pgt_id: 1,
            pgt_relid: pg_sys::Oid::from(12345u32),
            pgt_name: "test_st".to_string(),
            pgt_schema: "public".to_string(),
            defining_query: "SELECT 1".to_string(),
            original_query: None,
            schedule: Some("1m".to_string()),
            refresh_mode: RefreshMode::Differential,
            status: StStatus::Active,
            is_populated: true,
            data_timestamp: None,
            consecutive_errors: 0,
            needs_reinit: false,
            auto_threshold: None,
            last_full_ms: None,
            functions_used: None,
            frontier: None,
            topk_limit: None,
            topk_order_by: None,
            topk_offset: None,
            diamond_consistency: DiamondConsistency::Atomic,
            diamond_schedule_policy: DiamondSchedulePolicy::Fastest,
            has_keyless_source: false,
            function_hashes: None,
            requested_cdc_mode: None,
            is_append_only: false,
            scc_id: None,
            last_fixpoint_iterations: None,
            pooler_compatibility_mode: false,
            refresh_tier: "hot".to_string(),
            fuse_mode: "off".to_string(),
            fuse_state: "armed".to_string(),
            fuse_ceiling: None,
            fuse_sensitivity: None,
            blown_at: None,
            blow_reason: None,
            st_partition_key: None,
            max_differential_joins: None,
            max_delta_fraction: None,
            last_error_message: None,
            last_error_at: None,
            downstream_publication_name: None,
            freshness_deadline_ms: None,
            st_placement: "local".to_string(),
            temporal_mode: false,
            storage_backend: "heap".to_string(),
            post_refresh_action: "none".to_string(),
            reindex_drift_threshold: None,
            rows_changed_since_last_reindex: 0,
            last_reindex_at: None,
            defining_query_hash: 0,
            storage_fillfactor: None,
            query_complexity_class: None,
            row_identity_version: Some(crate::hash::CURRENT_ROW_IDENTITY_VERSION),
            self_heal_work_mem_percent: 100,
            self_heal_lock_backoff_exponent: 0,
            self_heal_success_streak: 0,
            last_error_code: None,
            last_error_retryable: None,
            defining_search_path: "public".to_string(),
        }
    }

    // ── normalize_sql_whitespace tests ─────────────────────────────────

    #[test]
    fn test_normalize_whitespace_collapses_spaces() {
        assert_eq!(
            normalize_sql_whitespace("SELECT  id,  val  FROM  t"),
            "SELECT id, val FROM t"
        );
    }

    #[test]
    fn test_normalize_whitespace_tabs_and_newlines() {
        assert_eq!(
            normalize_sql_whitespace("SELECT\tid\n\tFROM\tt"),
            "SELECT id FROM t"
        );
    }

    #[test]
    fn test_normalize_whitespace_trims() {
        assert_eq!(
            normalize_sql_whitespace("  SELECT id FROM t  "),
            "SELECT id FROM t"
        );
    }

    #[test]
    fn test_normalize_whitespace_identical() {
        assert_eq!(
            normalize_sql_whitespace("SELECT id FROM t"),
            normalize_sql_whitespace("SELECT id FROM t")
        );
    }

    #[test]
    fn test_config_diff_all_identical() {
        let st = make_test_st();
        let diff = compute_config_diff(
            &st,
            Some("1m"),
            "DIFFERENTIAL",
            None,
            None,
            None,
            false,
            false,
        );
        assert!(diff.is_empty());
    }

    #[test]
    fn test_config_diff_schedule_changed() {
        let st = make_test_st();
        let diff = compute_config_diff(
            &st,
            Some("5m"),
            "DIFFERENTIAL",
            None,
            None,
            None,
            false,
            false,
        );
        assert!(!diff.is_empty());
        assert_eq!(diff.schedule, Some("5m"));
        assert!(diff.refresh_mode.is_none());
    }

    #[test]
    fn test_config_diff_schedule_to_calculated() {
        let st = make_test_st();
        let diff = compute_config_diff(
            &st,
            Some("calculated"),
            "DIFFERENTIAL",
            None,
            None,
            None,
            false,
            false,
        );
        assert!(!diff.is_empty());
        assert_eq!(diff.schedule, Some("calculated"));
    }

    #[test]
    fn test_config_diff_calculated_already_calculated() {
        let mut st = make_test_st();
        st.schedule = None; // NULL in catalog means CALCULATED
        let diff = compute_config_diff(
            &st,
            Some("calculated"),
            "DIFFERENTIAL",
            None,
            None,
            None,
            false,
            false,
        );
        assert!(diff.is_empty());
    }

    #[test]
    fn test_config_diff_mode_changed() {
        let st = make_test_st();
        let diff = compute_config_diff(&st, Some("1m"), "FULL", None, None, None, false, false);
        assert!(!diff.is_empty());
        assert_eq!(diff.refresh_mode, Some("FULL"));
    }

    #[test]
    fn test_config_diff_auto_vs_differential() {
        // AUTO resolves to DIFFERENTIAL — should be same as existing DIFFERENTIAL
        let st = make_test_st();
        let diff = compute_config_diff(&st, Some("1m"), "AUTO", None, None, None, false, false);
        assert!(diff.is_empty());
    }

    #[test]
    fn test_config_diff_diamond_consistency_changed() {
        let st = make_test_st(); // existing: Atomic
        let diff = compute_config_diff(
            &st,
            Some("1m"),
            "DIFFERENTIAL",
            Some("none"),
            None,
            None,
            false,
            false,
        );
        assert!(!diff.is_empty());
        assert_eq!(diff.diamond_consistency, Some("none"));
    }

    #[test]
    fn test_config_diff_append_only_changed() {
        let st = make_test_st(); // existing: false
        let diff = compute_config_diff(
            &st,
            Some("1m"),
            "DIFFERENTIAL",
            None,
            None,
            None,
            true,
            false,
        );
        assert!(!diff.is_empty());
        assert_eq!(diff.append_only, Some(true));
    }

    #[test]
    fn test_config_diff_cdc_mode_changed() {
        let st = make_test_st(); // existing: None
        let diff = compute_config_diff(
            &st,
            Some("1m"),
            "DIFFERENTIAL",
            None,
            None,
            Some("wal"),
            false,
            false,
        );
        assert!(!diff.is_empty());
        assert_eq!(diff.cdc_mode, Some("wal"));
    }

    #[test]
    fn test_config_diff_cdc_mode_both_none() {
        let st = make_test_st(); // existing: None
        let diff = compute_config_diff(
            &st,
            Some("1m"),
            "DIFFERENTIAL",
            None,
            None,
            None,
            false,
            false,
        );
        assert!(diff.is_empty());
    }

    // ── P2 property / fuzz tests ──────────────────────────────────────────

    proptest! {
        #[test]
        fn prop_detect_select_star_no_panic(input in ".*") {
            let _ = detect_select_star(&input);
        }

        #[test]
        fn prop_detect_select_star_false_without_star(input in "[^*]*") {
            prop_assert!(!detect_select_star(&input));
        }

        #[test]
        fn prop_split_top_level_commas_no_panic(input in ".*") {
            let _ = split_top_level_commas(&input);
        }

        #[test]
        fn prop_split_top_level_commas_nonempty_for_nonempty_input(input in ".+") {
            let parts = split_top_level_commas(&input);
            // Whitespace-only inputs trim to empty and produce no columns — that is correct
            // behaviour. Only assert non-empty output when the input contains non-whitespace.
            if input.chars().any(|c| c != ',' && !c.is_whitespace()) {
                prop_assert!(!parts.is_empty());
            }
        }

        #[test]
        fn prop_find_top_level_keyword_no_panic(
            sql in ".*",
            kw in "[A-Za-z]{1,15}"
        ) {
            let _ = find_top_level_keyword(&sql, &kw);
        }

        #[test]
        fn prop_find_top_level_keyword_pos_in_bounds(
            sql in ".*",
            kw in "[A-Za-z]{1,15}"
        ) {
            if let Some(pos) = find_top_level_keyword(&sql, &kw) {
                prop_assert!(pos < sql.len());
            }
        }

        #[test]
        fn prop_cron_is_due_no_panic(
            cron_expr in ".*",
            epoch in proptest::option::of(proptest::num::i64::ANY)
        ) {
            let _ = cron_is_due(&cron_expr, epoch);
        }
    }

    // ── A1-1b: parse_partition_key_columns tests ────────────────────────────

    #[test]
    fn test_parse_partition_key_single_column() {
        let cols = parse_partition_key_columns("event_day");
        assert_eq!(cols, vec!["event_day"]);
    }

    #[test]
    fn test_parse_partition_key_two_columns() {
        let cols = parse_partition_key_columns("event_day, customer_id");
        assert_eq!(cols, vec!["event_day", "customer_id"]);
    }

    #[test]
    fn test_parse_partition_key_whitespace_handling() {
        let cols = parse_partition_key_columns("  a , b , c  ");
        assert_eq!(cols, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_parse_partition_key_empty_string() {
        let cols = parse_partition_key_columns("");
        assert!(cols.is_empty());
    }

    #[test]
    fn test_parse_partition_key_trailing_comma() {
        let cols = parse_partition_key_columns("a,b,");
        assert_eq!(cols, vec!["a", "b"]);
    }

    #[test]
    fn test_validate_partition_key_multi_column_valid() {
        let columns = vec![
            ColumnDef {
                name: "region".to_string(),
                type_oid: PgOid::Invalid,
            },
            ColumnDef {
                name: "sale_date".to_string(),
                type_oid: PgOid::Invalid,
            },
            ColumnDef {
                name: "amount".to_string(),
                type_oid: PgOid::Invalid,
            },
        ];
        assert!(validate_partition_key("sale_date,region", &columns).is_ok());
    }

    #[test]
    fn test_validate_partition_key_multi_column_one_missing() {
        let columns = vec![
            ColumnDef {
                name: "region".to_string(),
                type_oid: PgOid::Invalid,
            },
            ColumnDef {
                name: "sale_date".to_string(),
                type_oid: PgOid::Invalid,
            },
        ];
        let err = validate_partition_key("sale_date,nonexistent", &columns);
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("nonexistent"),
            "Error should mention the missing column: {msg}"
        );
    }

    // ── A1-1d: LIST partitioning tests ──────────────────────────────────────

    #[test]
    fn test_parse_partition_method_range_default() {
        assert_eq!(parse_partition_method("sale_date"), PartitionMethod::Range);
    }

    #[test]
    fn test_parse_partition_method_range_multi() {
        assert_eq!(parse_partition_method("a,b"), PartitionMethod::Range,);
    }

    #[test]
    fn test_parse_partition_method_list_upper() {
        assert_eq!(parse_partition_method("LIST:region"), PartitionMethod::List,);
    }

    #[test]
    fn test_parse_partition_method_list_lower() {
        assert_eq!(parse_partition_method("list:region"), PartitionMethod::List,);
    }

    #[test]
    fn test_parse_partition_method_list_mixed_case() {
        assert_eq!(parse_partition_method("List:region"), PartitionMethod::List,);
    }

    #[test]
    fn test_strip_partition_mode_prefix_none() {
        assert_eq!(strip_partition_mode_prefix("sale_date"), "sale_date");
    }

    #[test]
    fn test_strip_partition_mode_prefix_list() {
        assert_eq!(strip_partition_mode_prefix("LIST:region"), "region");
    }

    #[test]
    fn test_strip_partition_mode_prefix_list_lower() {
        assert_eq!(strip_partition_mode_prefix("list:region"), "region");
    }

    #[test]
    fn test_strip_partition_mode_prefix_mixed_case() {
        assert_eq!(strip_partition_mode_prefix("LiSt:region"), "region");
    }

    #[test]
    fn test_parse_partition_key_columns_with_list_prefix() {
        let cols = parse_partition_key_columns("LIST:region");
        assert_eq!(cols, vec!["region"]);
    }

    #[test]
    fn test_validate_partition_key_list_single_column_ok() {
        let columns = vec![
            ColumnDef {
                name: "region".to_string(),
                type_oid: PgOid::Invalid,
            },
            ColumnDef {
                name: "amount".to_string(),
                type_oid: PgOid::Invalid,
            },
        ];
        assert!(validate_partition_key("LIST:region", &columns).is_ok());
    }

    #[test]
    fn test_validate_partition_key_list_multi_column_rejected() {
        let columns = vec![
            ColumnDef {
                name: "region".to_string(),
                type_oid: PgOid::Invalid,
            },
            ColumnDef {
                name: "category".to_string(),
                type_oid: PgOid::Invalid,
            },
        ];
        let err = validate_partition_key("LIST:region,category", &columns);
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("single column"),
            "Error should mention single column: {msg}"
        );
    }

    #[test]
    fn test_validate_partition_key_list_missing_column() {
        let columns = vec![ColumnDef {
            name: "amount".to_string(),
            type_oid: PgOid::Invalid,
        }];
        let err = validate_partition_key("LIST:region", &columns);
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("region"),
            "Error should mention the missing column: {msg}"
        );
    }

    // ── A1-3b: HASH partitioning tests ──────────────────────────────────────

    #[test]
    fn test_parse_partition_method_hash_upper() {
        assert_eq!(
            parse_partition_method("HASH:customer_id"),
            PartitionMethod::Hash,
        );
    }

    #[test]
    fn test_parse_partition_method_hash_lower() {
        assert_eq!(
            parse_partition_method("hash:customer_id"),
            PartitionMethod::Hash,
        );
    }

    #[test]
    fn test_parse_partition_method_hash_mixed_case() {
        assert_eq!(
            parse_partition_method("Hash:customer_id"),
            PartitionMethod::Hash,
        );
    }

    #[test]
    fn test_parse_partition_method_hash_with_modulus() {
        assert_eq!(parse_partition_method("HASH:id:8"), PartitionMethod::Hash,);
    }

    #[test]
    fn test_parse_hash_modulus_default() {
        assert_eq!(parse_hash_modulus("HASH:id"), Some(4));
    }

    #[test]
    fn test_parse_hash_modulus_explicit() {
        assert_eq!(parse_hash_modulus("HASH:id:8"), Some(8));
    }

    #[test]
    fn test_parse_hash_modulus_min_value() {
        assert_eq!(parse_hash_modulus("HASH:id:2"), Some(2));
    }

    #[test]
    fn test_parse_hash_modulus_large() {
        assert_eq!(parse_hash_modulus("HASH:id:256"), Some(256));
    }

    #[test]
    fn test_parse_hash_modulus_non_hash_returns_none() {
        assert_eq!(parse_hash_modulus("sale_date"), None);
    }

    #[test]
    fn test_parse_hash_modulus_list_returns_none() {
        assert_eq!(parse_hash_modulus("LIST:region"), None);
    }

    #[test]
    fn test_strip_partition_mode_prefix_hash() {
        assert_eq!(
            strip_partition_mode_prefix("HASH:customer_id"),
            "customer_id"
        );
    }

    #[test]
    fn test_strip_partition_mode_prefix_hash_lower() {
        assert_eq!(
            strip_partition_mode_prefix("hash:customer_id"),
            "customer_id"
        );
    }

    #[test]
    fn test_strip_partition_mode_prefix_hash_with_modulus() {
        assert_eq!(strip_partition_mode_prefix("HASH:id:8"), "id");
    }

    #[test]
    fn test_strip_partition_mode_prefix_hash_with_modulus_256() {
        assert_eq!(
            strip_partition_mode_prefix("HASH:customer_id:256"),
            "customer_id"
        );
    }

    #[test]
    fn test_parse_partition_key_columns_with_hash_prefix() {
        let cols = parse_partition_key_columns("HASH:customer_id");
        assert_eq!(cols, vec!["customer_id"]);
    }

    #[test]
    fn test_parse_partition_key_columns_with_hash_modulus() {
        let cols = parse_partition_key_columns("HASH:customer_id:8");
        assert_eq!(cols, vec!["customer_id"]);
    }

    #[test]
    fn test_validate_partition_key_hash_single_column_ok() {
        let columns = vec![
            ColumnDef {
                name: "customer_id".to_string(),
                type_oid: PgOid::Invalid,
            },
            ColumnDef {
                name: "amount".to_string(),
                type_oid: PgOid::Invalid,
            },
        ];
        assert!(validate_partition_key("HASH:customer_id", &columns).is_ok());
    }

    #[test]
    fn test_validate_partition_key_hash_with_modulus_ok() {
        let columns = vec![ColumnDef {
            name: "id".to_string(),
            type_oid: PgOid::Invalid,
        }];
        assert!(validate_partition_key("HASH:id:8", &columns).is_ok());
    }

    #[test]
    fn test_validate_partition_key_hash_multi_column_rejected() {
        let columns = vec![
            ColumnDef {
                name: "a".to_string(),
                type_oid: PgOid::Invalid,
            },
            ColumnDef {
                name: "b".to_string(),
                type_oid: PgOid::Invalid,
            },
        ];
        let err = validate_partition_key("HASH:a,b", &columns);
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("single column"),
            "Error should mention single column: {msg}"
        );
    }

    #[test]
    fn test_validate_partition_key_hash_missing_column() {
        let columns = vec![ColumnDef {
            name: "amount".to_string(),
            type_oid: PgOid::Invalid,
        }];
        let err = validate_partition_key("HASH:customer_id", &columns);
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("customer_id"),
            "Error should mention the missing column: {msg}"
        );
    }

    #[test]
    fn test_validate_partition_key_hash_modulus_too_low() {
        let columns = vec![ColumnDef {
            name: "id".to_string(),
            type_oid: PgOid::Invalid,
        }];
        let err = validate_partition_key("HASH:id:1", &columns);
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("modulus"),
            "Error should mention modulus: {msg}"
        );
    }

    #[test]
    fn test_validate_partition_key_hash_modulus_too_high() {
        let columns = vec![ColumnDef {
            name: "id".to_string(),
            type_oid: PgOid::Invalid,
        }];
        let err = validate_partition_key("HASH:id:257", &columns);
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("modulus"),
            "Error should mention modulus: {msg}"
        );
    }

    // ── G15-BC: bulk_create_impl JSONB validation tests ─────────────

    #[test]
    fn test_bulk_create_impl_rejects_non_array() {
        let input = serde_json::json!({"name": "t1", "query": "SELECT 1"});
        let err = bulk_create_impl(input).unwrap_err();
        assert!(err.to_string().contains("JSONB array"));
    }

    #[test]
    fn test_bulk_create_impl_rejects_empty_array() {
        let input = serde_json::json!([]);
        let err = bulk_create_impl(input).unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn test_bulk_create_impl_rejects_non_object_element() {
        let input = serde_json::json!(["not an object"]);
        let err = bulk_create_impl(input).unwrap_err();
        assert!(err.to_string().contains("not a JSON object"));
    }

    #[test]
    fn test_bulk_create_impl_rejects_missing_name() {
        let input = serde_json::json!([{"query": "SELECT 1"}]);
        let err = bulk_create_impl(input).unwrap_err();
        assert!(err.to_string().contains("missing required \"name\""));
    }

    #[test]
    fn test_bulk_create_impl_rejects_missing_query() {
        let input = serde_json::json!([{"name": "t1"}]);
        let err = bulk_create_impl(input).unwrap_err();
        assert!(err.to_string().contains("missing required \"query\""));
    }

    #[test]
    fn test_canonicalize_bulk_stream_table_names_canonicalizes_public_schema() {
        let names = vec![
            Some("orders".to_string()),
            Some("analytics.daily".to_string()),
        ];
        let canonical =
            canonicalize_bulk_stream_table_names(&names, "bulk_drop_stream_tables").unwrap();
        assert_eq!(canonical, vec!["public.orders", "analytics.daily"]);
    }

    #[test]
    fn test_canonicalize_bulk_stream_table_names_rejects_empty() {
        let err = canonicalize_bulk_stream_table_names(&[], "bulk_drop_stream_tables").unwrap_err();
        assert!(err.to_string().contains("names array is empty"));
    }

    #[test]
    fn test_canonicalize_bulk_stream_table_names_rejects_null_entry() {
        let err = canonicalize_bulk_stream_table_names(
            &[Some("orders".to_string()), None],
            "bulk_drop_stream_tables",
        )
        .unwrap_err();
        assert!(err.to_string().contains("must not be NULL"));
    }

    #[test]
    fn test_canonicalize_bulk_stream_table_names_rejects_duplicates() {
        let err = canonicalize_bulk_stream_table_names(
            &[
                Some("orders".to_string()),
                Some("public.orders".to_string()),
            ],
            "bulk_drop_stream_tables",
        )
        .unwrap_err();
        assert!(err.to_string().contains("duplicate target"));
    }

    #[test]
    fn test_canonicalize_bulk_stream_table_names_rejects_over_limit() {
        let names = vec![Some("orders".to_string()); 257];
        let err =
            canonicalize_bulk_stream_table_names(&names, "bulk_drop_stream_tables").unwrap_err();
        assert!(err.to_string().contains("configured limit"));
    }

    #[test]
    fn test_parse_bulk_alter_stream_table_params_rejects_non_object() {
        let err = parse_bulk_alter_stream_table_params(serde_json::json!(["bad"])).unwrap_err();
        assert!(
            err.to_string()
                .contains("expects params to be a JSON object")
        );
    }

    #[test]
    fn test_parse_bulk_alter_stream_table_params_rejects_empty_object() {
        let err = parse_bulk_alter_stream_table_params(serde_json::json!({})).unwrap_err();
        assert!(err.to_string().contains("params object is empty"));
    }

    #[test]
    fn test_parse_bulk_alter_stream_table_params_rejects_null_field() {
        let err = parse_bulk_alter_stream_table_params(serde_json::json!({"schedule": null}))
            .unwrap_err();
        assert!(err.to_string().contains("must not be null"));
    }

    #[test]
    fn test_parse_bulk_alter_stream_table_params_rejects_unknown_field() {
        let err = parse_bulk_alter_stream_table_params(serde_json::json!({"unknown": "value"}))
            .unwrap_err();
        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn test_parse_bulk_alter_stream_table_params_rejects_wrong_type() {
        let err = parse_bulk_alter_stream_table_params(serde_json::json!({"append_only": "yes"}))
            .unwrap_err();
        assert!(err.to_string().contains("invalid type"));
    }

    #[test]
    fn test_parse_bulk_alter_stream_table_params_accepts_alias() {
        let params =
            parse_bulk_alter_stream_table_params(serde_json::json!({"fuse_mode": "auto"})).unwrap();
        assert_eq!(params.fuse.as_deref(), Some("auto"));
    }
}
