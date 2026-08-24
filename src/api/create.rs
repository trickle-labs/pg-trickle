//! Create stream table API entry points (v0.55.0 decomposition).
// Extracted from src/api/mod.rs in v0.55.0 module decomposition.
// All shared helpers, types, and utilities are in api/mod.rs (use super::*).

use super::alter::{
    AlterStreamTableOptions, CreateStreamTableOptions, SearchPathSource, alter_stream_table_impl,
    create_stream_table_impl,
};
use super::*;
use serde::Deserialize;

/// Create a new stream table.
///
/// # Arguments
/// - `name`: Schema-qualified name (`'schema.table'`) or unqualified (`'table'`).
/// - `query`: The defining SELECT query.
/// - `schedule`: Desired maximum schedule. `'calculated'` for CALCULATED mode (inherits schedule from downstream dependents).
/// - `refresh_mode`: `'AUTO'` (default — DIFFERENTIAL with FULL fallback),
///   `'FULL'`, `'DIFFERENTIAL'`, or `'IMMEDIATE'`.
/// - `initialize`: Whether to populate the table immediately.
/// - `diamond_consistency`: `'atomic'` (default) or `'none'`.
/// - `diamond_schedule_policy`: `'fastest'` (default) or `'slowest'`.
/// - `output_distribution_column`: When non-NULL and Citus is loaded, the storage
///   table is converted to a Citus distributed table using this column as the
///   distribution key after creation. Use `'s'` when the stream table materializes
///   a view over pg_ripple VP tables and you want co-location with VP shards.
///   Has no effect when Citus is not installed.
#[allow(clippy::too_many_arguments)]
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
fn create_stream_table(
    name: &str,
    query: &str,
    schedule: default!(Option<&str>, "'calculated'"),
    refresh_mode: default!(&str, "'AUTO'"),
    initialize: default!(bool, true),
    diamond_consistency: default!(Option<&str>, "NULL"),
    diamond_schedule_policy: default!(Option<&str>, "NULL"),
    cdc_mode: default!(Option<&str>, "NULL"),
    append_only: default!(bool, false),
    pooler_compatibility_mode: default!(bool, false),
    partition_by: default!(Option<&str>, "NULL"),
    max_differential_joins: default!(Option<i32>, "NULL"),
    max_delta_fraction: default!(Option<f64>, "NULL"),
    // CITUS-7: Distribution column for the output (stream table storage) table.
    output_distribution_column: default!(Option<&str>, "NULL"),
    // CORR-1/UX-1 (v0.36.0): temporal IVM mode
    temporal: default!(bool, false),
    // CORR-2/UX-3 (v0.36.0): columnar storage backend
    storage_backend: default!(Option<&str>, "NULL"),
    // HOT-1 (v0.73.0): heap fillfactor for HOT-friendly differential refreshes
    fillfactor: default!(Option<i32>, "NULL"),
    // v0.86.0: declared freshness target
    target_freshness: default!(Option<&str>, "NULL"),
) {
    let result = create_stream_table_impl(CreateStreamTableOptions {
        name,
        query,
        schedule,
        refresh_mode_str: refresh_mode,
        initialize,
        diamond_consistency,
        diamond_schedule_policy,
        requested_cdc_mode: cdc_mode,
        append_only,
        pooler_compatibility_mode,
        partition_by,
        max_differential_joins,
        max_delta_fraction,
        output_distribution_column,
        temporal_mode: temporal,
        storage_backend,
        storage_fillfactor: fillfactor,
        target_freshness,
    });
    if let Err(e) = result {
        raise_error_with_context(e);
    }
}

/// Create a stream table if it does not already exist.
///
/// If a stream table with the given name already exists, this is a silent no-op
/// (an INFO message is logged). The existing definition is never modified.
///
/// This is useful for migration scripts that should be safe to re-run.
#[allow(clippy::too_many_arguments)]
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
fn create_stream_table_if_not_exists(
    name: &str,
    query: &str,
    schedule: default!(Option<&str>, "'calculated'"),
    refresh_mode: default!(&str, "'AUTO'"),
    initialize: default!(bool, true),
    diamond_consistency: default!(Option<&str>, "NULL"),
    diamond_schedule_policy: default!(Option<&str>, "NULL"),
    cdc_mode: default!(Option<&str>, "NULL"),
    append_only: default!(bool, false),
    pooler_compatibility_mode: default!(bool, false),
    partition_by: default!(Option<&str>, "NULL"),
    max_differential_joins: default!(Option<i32>, "NULL"),
    max_delta_fraction: default!(Option<f64>, "NULL"),
    // CITUS-7: Distribution column for the output (stream table storage) table.
    output_distribution_column: default!(Option<&str>, "NULL"),
    // CORR-1/UX-1 (v0.36.0): temporal IVM mode
    temporal: default!(bool, false),
    // CORR-2/UX-3 (v0.36.0): columnar storage backend
    storage_backend: default!(Option<&str>, "NULL"),
    // HOT-1 (v0.73.0): heap fillfactor for HOT-friendly differential refreshes
    fillfactor: default!(Option<i32>, "NULL"),
    // v0.86.0: declared freshness target
    target_freshness: default!(Option<&str>, "NULL"),
) {
    let result = create_stream_table_if_not_exists_impl(CreateStreamTableOptions {
        name,
        query,
        schedule,
        refresh_mode_str: refresh_mode,
        initialize,
        diamond_consistency,
        diamond_schedule_policy,
        requested_cdc_mode: cdc_mode,
        append_only,
        pooler_compatibility_mode,
        partition_by,
        max_differential_joins,
        max_delta_fraction,
        output_distribution_column,
        temporal_mode: temporal,
        storage_backend,
        storage_fillfactor: fillfactor,
        target_freshness,
    });
    if let Err(e) = result {
        raise_error_with_context(e);
    }
}

#[allow(clippy::too_many_arguments)]
fn create_stream_table_if_not_exists_impl(
    opts: CreateStreamTableOptions<'_>,
) -> Result<(), PgTrickleError> {
    let (schema, table_name) = parse_qualified_name(opts.name)?;

    match StreamTableMeta::get_by_name(&schema, &table_name) {
        Ok(_) => {
            pgrx::info!(
                "Stream table {}.{} already exists — skipping creation.",
                schema,
                table_name,
            );
            Ok(())
        }
        Err(PgTrickleError::NotFound(_)) => create_stream_table_impl(opts),
        Err(e) => Err(e),
    }
}

/// G15-BC: Create multiple stream tables in a single transaction.
///
/// Accepts a JSONB array of stream table definitions. Each element must be
/// an object with at least `name` and `query` keys; all other keys match
/// the parameters of [`create_stream_table`] (snake_case).
///
/// Returns a JSONB array of results, one per input definition:
/// ```json
/// [
///   {"name": "my_st", "status": "created", "pgt_id": 42},
///   {"name": "bad_st", "status": "error", "error": "query parse error: …"}
/// ]
/// ```
///
/// On any error, the entire transaction is rolled back (standard PostgreSQL
/// transactional semantics).
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
fn bulk_create(definitions: pgrx::JsonB) -> pgrx::JsonB {
    let result = bulk_create_impl(definitions.0);
    match result {
        Ok(results_json) => pgrx::JsonB(results_json),
        Err(e) => raise_error_with_context(e),
    }
}

pub(crate) fn bulk_create_impl(
    definitions: serde_json::Value,
) -> Result<serde_json::Value, PgTrickleError> {
    let defs = definitions.as_array().ok_or_else(|| {
        PgTrickleError::InvalidArgument(
            "bulk_create() expects a JSONB array of stream table definitions".into(),
        )
    })?;

    if defs.is_empty() {
        return Err(PgTrickleError::InvalidArgument(
            "bulk_create() definitions array is empty".into(),
        ));
    }
    validation::cardinality(
        "bulk_create() definitions",
        defs.len(),
        crate::config::pg_trickle_max_bulk_api_items(),
    )?;

    let mut parsed = Vec::with_capacity(defs.len());
    let mut targets = std::collections::HashSet::with_capacity(defs.len());
    for (i, def) in defs.iter().enumerate() {
        let object = def.as_object().ok_or_else(|| {
            PgTrickleError::InvalidArgument(format!(
                "bulk_create() element [{i}] is not a JSON object"
            ))
        })?;
        if !object.contains_key("name") {
            return Err(PgTrickleError::InvalidArgument(format!(
                "bulk_create() element [{i}] missing required \"name\" string"
            )));
        }
        if !object.contains_key("query") {
            return Err(PgTrickleError::InvalidArgument(format!(
                "bulk_create() element [{i}] missing required \"query\" string"
            )));
        }
        let definition: BulkCreateDefinition =
            serde_json::from_value(def.clone()).map_err(|e| {
                PgTrickleError::InvalidArgument(format!(
                    "bulk_create() element [{i}] has an invalid schema: {e}"
                ))
            })?;
        let (schema, table_name) = parse_qualified_name(&definition.name).map_err(|e| {
            PgTrickleError::InvalidArgument(format!(
                "bulk_create() element [{i}] has invalid name {:?}: {e}",
                definition.name
            ))
        })?;
        if !targets.insert((schema, table_name)) {
            return Err(PgTrickleError::InvalidArgument(format!(
                "bulk_create() element [{i}] duplicates a target"
            )));
        }
        let max_differential_joins = definition
            .max_differential_joins
            .map(|value| validation::nonnegative_i32("max_differential_joins", value))
            .transpose()?;
        let storage_fillfactor = definition
            .fillfactor
            .map(|value| validation::checked_i32("fillfactor", value))
            .transpose()?;
        if let Some(fillfactor) = storage_fillfactor
            && !(10..=100).contains(&fillfactor)
        {
            return Err(PgTrickleError::InvalidArgument(format!(
                "fillfactor must be between 10 and 100 (got {fillfactor})"
            )));
        }
        if let Some(value) = definition.max_delta_fraction {
            validation::finite_fraction("max_delta_fraction", value)?;
        }
        parsed.push((definition, max_differential_joins, storage_fillfactor));
    }

    let mut results = Vec::with_capacity(parsed.len());

    for (i, (definition, max_differential_joins, storage_fillfactor)) in
        parsed.into_iter().enumerate()
    {
        match create_stream_table_impl(CreateStreamTableOptions {
            name: &definition.name,
            query: &definition.query,
            schedule: definition.schedule.as_deref(),
            refresh_mode_str: &definition.refresh_mode,
            initialize: definition.initialize,
            diamond_consistency: definition.diamond_consistency.as_deref(),
            diamond_schedule_policy: definition.diamond_schedule_policy.as_deref(),
            requested_cdc_mode: definition.cdc_mode.as_deref(),
            append_only: definition.append_only,
            pooler_compatibility_mode: definition.pooler_compatibility_mode,
            partition_by: definition.partition_by.as_deref(),
            max_differential_joins,
            max_delta_fraction: definition.max_delta_fraction,
            output_distribution_column: definition.output_distribution_column.as_deref(),
            temporal_mode: definition.temporal,
            storage_backend: definition.storage_backend.as_deref(),
            storage_fillfactor,
            target_freshness: None,
        }) {
            Ok(()) => {
                // Look up pgt_id for the result
                let (schema, table_name) = parse_qualified_name(&definition.name).map_err(|e| {
                    PgTrickleError::InvalidArgument(format!(
                        "bulk_create() element [{i}] has invalid name: {e}"
                    ))
                })?;
                let pgt_id = StreamTableMeta::get_by_name(&schema, &table_name)?.pgt_id;

                results.push(serde_json::json!({
                    "name": definition.name,
                    "status": "created",
                    "pgt_id": pgt_id,
                }));
            }
            Err(e) => {
                // Abort the entire batch on error — the transaction will
                // be rolled back by PostgreSQL. Return immediate error
                // with context about which definition failed.
                return Err(PgTrickleError::InvalidArgument(format!(
                    "bulk_create() failed on element [{i}] {:?}: {e}",
                    definition.name
                )));
            }
        }
    }

    pgrx::info!(
        "pg_trickle: bulk_create created {} stream table(s)",
        results.len()
    );

    Ok(serde_json::Value::Array(results))
}

fn default_bulk_refresh_mode() -> String {
    "AUTO".to_string()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BulkCreateDefinition {
    name: String,
    query: String,
    #[serde(default)]
    schedule: Option<String>,
    #[serde(default = "default_bulk_refresh_mode")]
    refresh_mode: String,
    #[serde(default = "default_true")]
    initialize: bool,
    #[serde(default)]
    diamond_consistency: Option<String>,
    #[serde(default)]
    diamond_schedule_policy: Option<String>,
    #[serde(default)]
    cdc_mode: Option<String>,
    #[serde(default)]
    append_only: bool,
    #[serde(default)]
    pooler_compatibility_mode: bool,
    #[serde(default)]
    partition_by: Option<String>,
    #[serde(default)]
    max_differential_joins: Option<i64>,
    #[serde(default)]
    max_delta_fraction: Option<f64>,
    #[serde(default)]
    output_distribution_column: Option<String>,
    #[serde(default)]
    temporal: bool,
    #[serde(default)]
    storage_backend: Option<String>,
    #[serde(default)]
    fillfactor: Option<i64>,
}

fn default_true() -> bool {
    true
}

/// Create or replace a stream table.
///
/// If the stream table does not exist, it is created (identical to
/// [`create_stream_table`]).  If it already exists:
///
/// - **Identical definition** → no-op (INFO logged).
/// - **Query identical, config differs** → delegates to `alter_stream_table_impl`.
/// - **Query differs** → delegates to `alter_stream_table_impl` with query change
///   (ALTER QUERY path), plus any config changes.
///
/// This is the declarative API for idempotent deployments (dbt, migrations,
/// GitOps). Mirrors PostgreSQL's `CREATE OR REPLACE` convention.
/// SEC-1: `security_definer` — this is the fast-path lifecycle entry point
/// (the one `dbt-pgtrickle`'s `stream_table` materialization actually calls)
/// and was previously the only member of the create/alter/drop lifecycle
/// family left as SECURITY INVOKER, forcing callers to hold direct grants on
/// the `pgtrickle`/`pgtrickle_changes` catalog objects instead of just EXECUTE
/// on this function. Safe as a pure attribute change: this function is a thin
/// dispatcher with no SQL of its own — it delegates entirely to
/// `create_stream_table_impl` (new table) or `alter_stream_table_impl`
/// (existing table), both of which already re-derive the true caller via
/// `outer_user_id()`/`GetOuterUserId()` for their own authorization checks
/// (`validate_output_schema_create`/`validate_source_access`/
/// `transfer_output_table_ownership` on the create path,
/// `check_stream_table_ownership` on the alter path), so those checks keep
/// gating the real caller regardless of which entry point reached them.
#[allow(clippy::too_many_arguments)]
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
fn create_or_replace_stream_table(
    name: &str,
    query: &str,
    schedule: default!(Option<&str>, "'calculated'"),
    refresh_mode: default!(&str, "'AUTO'"),
    initialize: default!(bool, true),
    diamond_consistency: default!(Option<&str>, "NULL"),
    diamond_schedule_policy: default!(Option<&str>, "NULL"),
    cdc_mode: default!(Option<&str>, "NULL"),
    append_only: default!(bool, false),
    pooler_compatibility_mode: default!(bool, false),
    partition_by: default!(Option<&str>, "NULL"),
    max_differential_joins: default!(Option<i32>, "NULL"),
    max_delta_fraction: default!(Option<f64>, "NULL"),
    // CITUS-7: Distribution column for the output (stream table storage) table.
    output_distribution_column: default!(Option<&str>, "NULL"),
    // CORR-1/UX-1 (v0.36.0): temporal IVM mode
    temporal: default!(bool, false),
    // CORR-2/UX-3 (v0.36.0): columnar storage backend
    storage_backend: default!(Option<&str>, "NULL"),
) {
    let result = create_or_replace_stream_table_impl(
        name,
        query,
        schedule,
        refresh_mode,
        initialize,
        diamond_consistency,
        diamond_schedule_policy,
        cdc_mode,
        append_only,
        pooler_compatibility_mode,
        partition_by,
        max_differential_joins,
        max_delta_fraction,
        output_distribution_column,
        temporal,
        storage_backend,
    );
    if let Err(e) = result {
        raise_error_with_context(e);
    }
}

/// Tracks which config parameters differ between an existing stream table
/// and a `create_or_replace` call.  Fields are `Some` only when changed.
pub(crate) struct ConfigDiff<'a> {
    pub(crate) schedule: Option<&'a str>,
    pub(crate) refresh_mode: Option<&'a str>,
    pub(crate) diamond_consistency: Option<&'a str>,
    pub(crate) diamond_schedule_policy: Option<&'a str>,
    pub(crate) cdc_mode: Option<&'a str>,
    pub(crate) append_only: Option<bool>,
    pub(crate) pooler_compatibility_mode: Option<bool>,
}

impl ConfigDiff<'_> {
    pub(crate) fn is_empty(&self) -> bool {
        self.schedule.is_none()
            && self.refresh_mode.is_none()
            && self.diamond_consistency.is_none()
            && self.diamond_schedule_policy.is_none()
            && self.cdc_mode.is_none()
            && self.append_only.is_none()
            && self.pooler_compatibility_mode.is_none()
    }
}

/// Compare the requested config parameters against the existing catalog row.
/// Returns `Some` only for parameters that differ from the stored values.
#[allow(clippy::too_many_arguments)]
pub(crate) fn compute_config_diff<'a>(
    existing: &StreamTableMeta,
    new_schedule: Option<&'a str>,
    new_refresh_mode: &'a str,
    new_dc: Option<&'a str>,
    new_dsp: Option<&'a str>,
    new_cdc_mode: Option<&'a str>,
    new_append_only: bool,
    new_pooler_compat: bool,
) -> ConfigDiff<'a> {
    // Schedule: compare raw strings.  'calculated' in user input means NULL in catalog.
    let schedule_changed = match new_schedule {
        Some(s) if s.trim().eq_ignore_ascii_case("calculated") => existing.schedule.is_some(),
        Some(s) => existing
            .schedule
            .as_deref()
            .is_none_or(|cur| cur != s.trim()),
        None => existing.schedule.is_some(),
    };

    // Refresh mode: compare enum values.  AUTO is resolved to DIFFERENTIAL by from_str.
    let new_mode = RefreshMode::from_str(new_refresh_mode).unwrap_or(RefreshMode::Differential);
    let mode_changed = existing.refresh_mode != new_mode;

    // Diamond consistency: compare enum values.
    let new_dc_val = match new_dc {
        Some(s) => DiamondConsistency::from_sql_str(&s.to_lowercase()),
        None => DiamondConsistency::Atomic,
    };
    let dc_changed = existing.diamond_consistency != new_dc_val;

    // Diamond schedule policy: compare enum values.
    let new_dsp_val = match new_dsp {
        Some(s) => DiamondSchedulePolicy::from_sql_str(s).unwrap_or(DiamondSchedulePolicy::Fastest),
        None => DiamondSchedulePolicy::Fastest,
    };
    let dsp_changed = existing.diamond_schedule_policy != new_dsp_val;

    // CDC mode: compare Option<String>.
    let new_cdc_normalized = new_cdc_mode.map(|m| m.trim().to_lowercase());
    let cdc_changed = match (&existing.requested_cdc_mode, &new_cdc_normalized) {
        (None, None) => false,
        (Some(a), Some(b)) => a != b,
        _ => true,
    };

    // Append-only: compare bools.
    let ao_changed = existing.is_append_only != new_append_only;

    // PB2: Pooler compatibility mode.
    let pcm_changed = existing.pooler_compatibility_mode != new_pooler_compat;

    ConfigDiff {
        schedule: if schedule_changed {
            new_schedule.or(Some("calculated"))
        } else {
            None
        },
        refresh_mode: if mode_changed {
            Some(new_refresh_mode)
        } else {
            None
        },
        diamond_consistency: if dc_changed { new_dc } else { None },
        diamond_schedule_policy: if dsp_changed { new_dsp } else { None },
        cdc_mode: if cdc_changed { new_cdc_mode } else { None },
        append_only: if ao_changed {
            Some(new_append_only)
        } else {
            None
        },
        pooler_compatibility_mode: if pcm_changed {
            Some(new_pooler_compat)
        } else {
            None
        },
    }
}

/// Collapse all runs of whitespace (spaces, tabs, newlines) into a single
/// space and trim leading/trailing whitespace. Used for semantic query
/// comparison so cosmetic SQL formatting differences are treated as no-ops.
pub(crate) fn normalize_sql_whitespace(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[allow(clippy::too_many_arguments)]
fn create_or_replace_stream_table_impl(
    name: &str,
    query: &str,
    schedule: Option<&str>,
    refresh_mode_str: &str,
    initialize: bool,
    diamond_consistency: Option<&str>,
    diamond_schedule_policy: Option<&str>,
    cdc_mode: Option<&str>,
    append_only: bool,
    pooler_compatibility_mode: bool,
    partition_by: Option<&str>,
    max_differential_joins: Option<i32>,
    max_delta_fraction: Option<f64>,
    // CITUS-7: Distribution column for the output table (used only on first creation).
    output_distribution_column: Option<&str>,
    // CORR-1/UX-1 (v0.36.0): temporal IVM mode (used only on first creation).
    temporal_mode: bool,
    // CORR-2/UX-3 (v0.36.0): columnar storage backend (used only on first creation).
    storage_backend: Option<&str>,
) -> Result<(), PgTrickleError> {
    let (schema, table_name) = parse_qualified_name(name)?;

    match StreamTableMeta::get_by_name(&schema, &table_name) {
        Ok(existing) => {
            // SEC-1: wrap the ENTIRE existing-stream-table branch — not just
            // the query-diff-detection step — under the caller's own
            // search_path, including the delegated alter_stream_table_impl
            // call. Several rounds of CI failures each found one more
            // internal call along this path (query rewrite, TopK detection,
            // view-soft-dependency extraction, the stored defining query's
            // own incremental-mode admission check) that parses or resolves
            // the caller-supplied, unqualified query text — chasing them
            // piecemeal proved unreliable, so wrap the whole flow instead.
            // alter_stream_table_impl computes and applies its own
            // invoker_search_path independently (needed since it's also
            // reachable through bulk_alter_stream_tables_impl's plain
            // SECURITY INVOKER path, which this call is not), so nesting
            // here is redundant but harmless — with_invoker_search_path
            // restores whatever was actually active beforehand, so nested
            // calls compose correctly rather than clobbering each other.
            with_invoker_search_path(&invoker_search_path()?, || {
                // Stream table exists — determine what changed.
                let rw = run_query_rewrite_pipeline(query)?;
                let new_query_rewritten = rw.query;

                // TopK detection: if the new query is TopK, compare against
                // the base query (ORDER BY/LIMIT stripped) since that's
                // what is stored in `defining_query`.
                let topk_info = crate::dvm::detect_topk_pattern(&new_query_rewritten)?;
                let effective_new_query = match &topk_info {
                    Some(info) => &info.base_query,
                    None => &new_query_rewritten,
                };

                // Normalize whitespace before comparison so cosmetic
                // differences (extra spaces, newlines, tabs) are no-ops.
                let query_changed = normalize_sql_whitespace(&existing.defining_query)
                    != normalize_sql_whitespace(effective_new_query);

                let config_diff = compute_config_diff(
                    &existing,
                    schedule,
                    refresh_mode_str,
                    diamond_consistency,
                    diamond_schedule_policy,
                    cdc_mode,
                    append_only,
                    pooler_compatibility_mode,
                );

                if !query_changed && config_diff.is_empty() {
                    pgrx::info!(
                        "Stream table {}.{} already exists with identical definition — no changes made.",
                        schema,
                        table_name,
                    );
                    return Ok(());
                }

                // Delegate to alter_stream_table_impl with the appropriate
                // combination of query + config changes.
                alter_stream_table_impl(AlterStreamTableOptions {
                    name,
                    query: if query_changed { Some(query) } else { None },
                    schedule: config_diff.schedule,
                    refresh_mode: config_diff.refresh_mode,
                    status: None, // keep current
                    diamond_consistency: config_diff.diamond_consistency,
                    diamond_schedule_policy: config_diff.diamond_schedule_policy,
                    cdc_mode: config_diff.cdc_mode,
                    append_only: config_diff.append_only,
                    pooler_compatibility_mode: config_diff.pooler_compatibility_mode,
                    max_differential_joins,
                    max_delta_fraction,
                    search_path_source: SearchPathSource::SecurityDefinerCaller,
                    ..Default::default()
                })?;

                pgrx::info!(
                    "Stream table {}.{} replaced (query_changed={}, config_changed={}).",
                    schema,
                    table_name,
                    query_changed,
                    !config_diff.is_empty(),
                );

                Ok(())
            })
        }
        Err(PgTrickleError::NotFound(_)) => {
            // Does not exist — create from scratch.
            create_stream_table_impl(CreateStreamTableOptions {
                name,
                query,
                schedule,
                refresh_mode_str,
                initialize,
                diamond_consistency,
                diamond_schedule_policy,
                requested_cdc_mode: cdc_mode,
                append_only,
                pooler_compatibility_mode,
                partition_by,
                max_differential_joins,
                max_delta_fraction,
                output_distribution_column,
                temporal_mode,   // passed through from caller
                storage_backend, // passed through from caller
                storage_fillfactor: None,
                target_freshness: None,
            })
        }
        Err(e) => Err(e),
    }
}

// ── A-1 (v0.79.0): create_stream_table_fast_append_only ───────────────────

/// Create a stream table optimised for append-only sources.
///
/// A-1: Convenience wrapper around [`create_stream_table`] that presets:
/// - `append_only = true` — source rows are only ever inserted, never updated
///   or deleted.  The differential path can skip DELETE maintenance.
/// - `refresh_mode = 'DIFFERENTIAL'` — always use differential refresh.
/// - `initialize = true` — populate the table immediately.
///
/// All other parameters default to the same values as `create_stream_table`.
///
/// # Example
/// ```sql
/// SELECT pgtrickle.create_stream_table_fast_append_only(
///     'my_schema.event_counts',
///     'SELECT user_id, count(*) AS n FROM events GROUP BY user_id'
/// );
/// ```
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
fn create_stream_table_fast_append_only(
    name: &str,
    query: &str,
    schedule: default!(Option<&str>, "'calculated'"),
    cdc_mode: default!(Option<&str>, "NULL"),
    partition_by: default!(Option<&str>, "NULL"),
    max_differential_joins: default!(Option<i32>, "NULL"),
    max_delta_fraction: default!(Option<f64>, "NULL"),
) {
    let result = create_stream_table_impl(CreateStreamTableOptions {
        name,
        query,
        schedule,
        refresh_mode_str: "DIFFERENTIAL",
        initialize: true,
        append_only: true,
        requested_cdc_mode: cdc_mode,
        partition_by,
        max_differential_joins,
        max_delta_fraction,
        ..Default::default()
    });
    if let Err(e) = result {
        raise_error_with_context(e);
    }
}

// ── QW-10 (v0.81.0): Stream table presets ────────────────────────────────

/// Create a stream table optimised for real-time DIFFERENTIAL refresh.
///
/// Equivalent to `create_stream_table` with:
/// - `schedule = '1s'`
/// - `refresh_mode = 'DIFFERENTIAL'`
/// - `initialize = true`
///
/// Use this preset for latency-sensitive use cases where sub-second
/// freshness is required and the defining query is fully supported by the
/// DVM engine.
#[allow(clippy::too_many_arguments)]
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
fn create_stream_table_realtime(
    name: &str,
    query: &str,
    cdc_mode: default!(Option<&str>, "NULL"),
    append_only: default!(bool, false),
    partition_by: default!(Option<&str>, "NULL"),
    max_differential_joins: default!(Option<i32>, "NULL"),
    max_delta_fraction: default!(Option<f64>, "NULL"),
) {
    let result = create_stream_table_impl(CreateStreamTableOptions {
        name,
        query,
        schedule: Some("1s"),
        refresh_mode_str: "DIFFERENTIAL",
        initialize: true,
        append_only,
        requested_cdc_mode: cdc_mode,
        partition_by,
        max_differential_joins,
        max_delta_fraction,
        ..Default::default()
    });
    if let Err(e) = result {
        raise_error_with_context(e);
    }
}

/// Create a stream table optimised for batch refresh (every 5 minutes).
///
/// Equivalent to `create_stream_table` with:
/// - `schedule = '5m'`
/// - `refresh_mode = 'AUTO'`
/// - `initialize = true`
///
/// Use this preset for analytical workloads where moderate latency is
/// acceptable and cost efficiency matters more than freshness.
#[allow(clippy::too_many_arguments)]
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
fn create_stream_table_batch(
    name: &str,
    query: &str,
    cdc_mode: default!(Option<&str>, "NULL"),
    append_only: default!(bool, false),
    partition_by: default!(Option<&str>, "NULL"),
    max_differential_joins: default!(Option<i32>, "NULL"),
    max_delta_fraction: default!(Option<f64>, "NULL"),
) {
    let result = create_stream_table_impl(CreateStreamTableOptions {
        name,
        query,
        schedule: Some("5m"),
        refresh_mode_str: "AUTO",
        initialize: true,
        append_only,
        requested_cdc_mode: cdc_mode,
        partition_by,
        max_differential_joins,
        max_delta_fraction,
        ..Default::default()
    });
    if let Err(e) = result {
        raise_error_with_context(e);
    }
}

/// Create a cost-optimised stream table (refresh every 15 minutes).
///
/// Equivalent to `create_stream_table` with:
/// - `schedule = '15m'`
/// - `refresh_mode = 'AUTO'`
/// - `initialize = true`
///
/// Use this preset for reporting and BI queries where freshness can be
/// traded for lower CPU and I/O overhead.
#[allow(clippy::too_many_arguments)]
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
fn create_stream_table_cost_optimized(
    name: &str,
    query: &str,
    cdc_mode: default!(Option<&str>, "NULL"),
    append_only: default!(bool, false),
    partition_by: default!(Option<&str>, "NULL"),
    max_differential_joins: default!(Option<i32>, "NULL"),
    max_delta_fraction: default!(Option<f64>, "NULL"),
) {
    let result = create_stream_table_impl(CreateStreamTableOptions {
        name,
        query,
        schedule: Some("15m"),
        refresh_mode_str: "AUTO",
        initialize: true,
        append_only,
        requested_cdc_mode: cdc_mode,
        partition_by,
        max_differential_joins,
        max_delta_fraction,
        ..Default::default()
    });
    if let Err(e) = result {
        raise_error_with_context(e);
    }
}
