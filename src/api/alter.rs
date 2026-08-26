//! Alter, drop, resume, repair stream table API (v0.55.0 decomposition).
// Extracted from src/api/mod.rs in v0.55.0 module decomposition.
// All shared helpers, types, and utilities are in api/mod.rs (use super::*).

use super::refresh_ops::execute_manual_full_refresh;
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TargetFreshnessMode {
    Interval,
    OnCommit,
    Manual,
    Clear,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct TargetFreshness {
    pub(crate) mode: TargetFreshnessMode,
    pub(crate) milliseconds: Option<i64>,
}

/// Parse a PostgreSQL interval once and translate it to the existing deadline
/// control. Month-bearing intervals are rejected because their cadence is not
/// stable in milliseconds.
pub(crate) fn parse_target_freshness(
    value: Option<&str>,
) -> Result<Option<TargetFreshness>, PgTrickleError> {
    let Some(raw) = value.map(str::trim) else {
        return Ok(None);
    };
    if raw.is_empty() {
        return Ok(Some(TargetFreshness {
            mode: TargetFreshnessMode::Clear,
            milliseconds: None,
        }));
    }
    match raw.to_ascii_lowercase().as_str() {
        "manual" => {
            return Ok(Some(TargetFreshness {
                mode: TargetFreshnessMode::Manual,
                milliseconds: None,
            }));
        }
        "on_commit" | "on commit" => {
            return Ok(Some(TargetFreshness {
                mode: TargetFreshnessMode::OnCommit,
                milliseconds: None,
            }));
        }
        _ => {}
    }

    let months = Spi::get_one_with_args::<i32>(
        "SELECT (EXTRACT(YEAR FROM $1::interval) * 12 + EXTRACT(MONTH FROM $1::interval))::int",
        &[raw.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| PgTrickleError::InvalidArgument("target_freshness interval is NULL".into()))?;
    if months != 0 {
        return Err(PgTrickleError::InvalidArgument(
            "target_freshness cannot contain calendar months; use days or smaller units".into(),
        ));
    }
    let milliseconds = Spi::get_one_with_args::<f64>(
        "SELECT EXTRACT(EPOCH FROM $1::interval)::float8 * 1000",
        &[raw.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| PgTrickleError::InvalidArgument("target_freshness interval is NULL".into()))?;
    if !milliseconds.is_finite() || milliseconds <= 0.0 || milliseconds > i64::MAX as f64 {
        return Err(PgTrickleError::InvalidArgument(
            "target_freshness must be a positive finite interval".into(),
        ));
    }
    let milliseconds = milliseconds.round() as i64;
    if milliseconds <= 0 {
        return Err(PgTrickleError::InvalidArgument(
            "target_freshness must be at least one millisecond".into(),
        ));
    }
    Ok(Some(TargetFreshness {
        mode: TargetFreshnessMode::Interval,
        milliseconds: Some(milliseconds),
    }))
}

pub(crate) fn apply_target_freshness(
    pgt_id: i64,
    target: TargetFreshness,
) -> Result<(), PgTrickleError> {
    let (mode, milliseconds) = match target.mode {
        TargetFreshnessMode::Interval => (Some("INTERVAL"), target.milliseconds),
        TargetFreshnessMode::OnCommit => (Some("ON_COMMIT"), None),
        TargetFreshnessMode::Manual => (Some("MANUAL"), None),
        TargetFreshnessMode::Clear => (None, None),
    };
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_stream_tables
            SET target_freshness_mode = $1,
                freshness_deadline_ms = $2,
                schedule = CASE
                    WHEN $3::text = 'ON_COMMIT' THEN NULL
                    WHEN $3::text = 'MANUAL' THEN 'manual'
                    WHEN $3::text = 'INTERVAL'
                        AND (schedule IS NULL OR schedule = 'calculated')
                        THEN GREATEST(1, CEIL($2::double precision / 1000.0))::bigint::text || 's'
                    ELSE schedule
                END,
                updated_at = now()
          WHERE pgt_id = $4",
        &[mode.into(), milliseconds.into(), mode.into(), pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))
}

// ── Schema comparison for ALTER QUERY ──────────────────────────────────────

/// Classification of how the output schema changed between old and new query.
#[derive(Debug)]
enum SchemaChange {
    /// Column names, types, and count are identical — fast path.
    Same,
    /// Columns added or removed; surviving columns have compatible types.
    Compatible {
        added: Vec<ColumnDef>,
        removed: Vec<String>,
    },
    /// Column type changed incompatibly — requires full storage rebuild.
    Incompatible { reason: String },
}

/// Compare old vs new output column schemas to classify the change.
fn classify_schema_change(old: &[ColumnDef], new: &[ColumnDef]) -> SchemaChange {
    // Build lookup by name for old columns
    let old_map: std::collections::HashMap<&str, &ColumnDef> =
        old.iter().map(|c| (c.name.as_str(), c)).collect();
    let new_map: std::collections::HashMap<&str, &ColumnDef> =
        new.iter().map(|c| (c.name.as_str(), c)).collect();

    // Check for type incompatibilities on surviving columns
    for new_col in new {
        if let Some(old_col) = old_map.get(new_col.name.as_str())
            && old_col.type_oid != new_col.type_oid
        {
            // Check if PostgreSQL has an implicit cast
            let can_cast = Spi::get_one_with_args::<bool>(
                "SELECT EXISTS(SELECT 1 FROM pg_cast \
                 WHERE castsource = $1 AND casttarget = $2 \
                 AND castcontext = 'i')",
                &[
                    old_col.type_oid.value().into(),
                    new_col.type_oid.value().into(),
                ],
            )
            .unwrap_or(Some(false))
            .unwrap_or(false);

            if !can_cast {
                return SchemaChange::Incompatible {
                    reason: format!(
                        "column '{}' type changed from OID {} to {} (no implicit cast)",
                        new_col.name,
                        old_col.type_oid.value(),
                        new_col.type_oid.value(),
                    ),
                };
            }
        }
    }

    // Identify added and removed columns
    let added: Vec<ColumnDef> = new
        .iter()
        .filter(|c| !old_map.contains_key(c.name.as_str()))
        .cloned()
        .collect();
    let removed: Vec<String> = old
        .iter()
        .filter(|c| !new_map.contains_key(c.name.as_str()))
        .map(|c| c.name.clone())
        .collect();

    if added.is_empty() && removed.is_empty() {
        // Check ordering — if column order changed, treat as Compatible
        // (no DDL needed, but we track the difference)
        let same_order = old.len() == new.len()
            && old
                .iter()
                .zip(new.iter())
                .all(|(o, n)| o.name == n.name && o.type_oid == n.type_oid);
        if same_order {
            SchemaChange::Same
        } else {
            // Types compatible but order changed — treated as Same since
            // column order in storage doesn't affect correctness
            SchemaChange::Same
        }
    } else {
        SchemaChange::Compatible { added, removed }
    }
}

// ── Dependency diffing for ALTER QUERY ────────────────────────────────────

/// Result of diffing old vs new source dependencies.
struct DependencyDiff {
    /// Sources present in new query but not old.
    added: Vec<(pg_sys::Oid, String)>,
    /// Sources present in old query but not new.
    removed: Vec<(pg_sys::Oid, String)>,
    /// Sources present in both old and new queries.
    kept: Vec<(pg_sys::Oid, String)>,
}

/// Compute which source dependencies were added, removed, or kept.
fn diff_dependencies(
    old_deps: &[StDependency],
    new_sources: &[(pg_sys::Oid, String)],
) -> DependencyDiff {
    let old_oids: std::collections::HashSet<u32> =
        old_deps.iter().map(|d| d.source_relid.to_u32()).collect();
    let new_oids: std::collections::HashSet<u32> =
        new_sources.iter().map(|(o, _)| o.to_u32()).collect();

    let added = new_sources
        .iter()
        .filter(|(o, _)| !old_oids.contains(&o.to_u32()))
        .cloned()
        .collect();
    let removed = old_deps
        .iter()
        .filter(|d| !new_oids.contains(&d.source_relid.to_u32()))
        .map(|d| (d.source_relid, d.source_type.clone()))
        .collect();
    let kept = new_sources
        .iter()
        .filter(|(o, _)| old_oids.contains(&o.to_u32()))
        .cloned()
        .collect();

    DependencyDiff {
        added,
        removed,
        kept,
    }
}

// ── Storage table migration for ALTER QUERY ──────────────────────────────

/// Migrate the storage table schema for a Compatible schema change.
/// For Same schema, this is a no-op. For Incompatible, the caller
/// must drop and recreate the storage table.
fn migrate_storage_table_compatible(
    schema: &str,
    table_name: &str,
    added: &[ColumnDef],
    removed: &[String],
) -> Result<(), PgTrickleError> {
    let quoted_table = format!(
        "{}.{}",
        quote_identifier(schema),
        quote_identifier(table_name),
    );

    // Add new columns
    for col in added {
        let type_name = match col.type_oid {
            PgOid::Invalid => "text".to_string(),
            oid => {
                Spi::get_one_with_args::<String>("SELECT $1::regtype::text", &[oid.value().into()])
                    .unwrap_or(Some("text".to_string()))
                    .unwrap_or_else(|| "text".to_string())
            }
        };
        let add_sql = format!(
            "ALTER TABLE {} ADD COLUMN {} {}",
            quoted_table,
            quote_identifier(&col.name),
            type_name,
        );
        Spi::run(&add_sql).map_err(|e| {
            PgTrickleError::SpiError(format!("Failed to add column '{}': {}", col.name, e))
        })?;
    }

    // Drop removed columns
    for col_name in removed {
        let drop_sql = format!(
            "ALTER TABLE {} DROP COLUMN IF EXISTS {}",
            quoted_table,
            quote_identifier(col_name),
        );
        Spi::run(&drop_sql).map_err(|e| {
            PgTrickleError::SpiError(format!("Failed to drop column '{}': {}", col_name, e))
        })?;
    }

    Ok(())
}

/// Rebuild the `__pgt_row_id` index on a storage table.
///
/// The covering index created by `setup_storage_table` uses an INCLUDE clause
/// referencing user columns.  When `migrate_storage_table_compatible` drops
/// columns that appear in the INCLUDE list PostgreSQL silently drops the whole
/// index, leaving no unique constraint for `ON CONFLICT (__pgt_row_id)` in
/// differential refresh.  This function drops any surviving row-id index and
/// recreates it with the correct INCLUDE clause for the *new* column set.
fn rebuild_row_id_index(
    schema: &str,
    table_name: &str,
    new_columns: &[ColumnDef],
    has_keyless_source: bool,
    is_partitioned: bool,
) -> Result<(), PgTrickleError> {
    let quoted_table = format!(
        "{}.{}",
        quote_identifier(schema),
        quote_identifier(table_name),
    );

    // Drop any existing index on __pgt_row_id (may already be gone).
    let existing: Option<String> = Spi::get_one_with_args(
        "SELECT indexrelid::regclass::text FROM pg_index \
         JOIN pg_attribute ON attrelid = indrelid AND attnum = ANY(indkey) \
         WHERE indrelid = $1::regclass AND attname = '__pgt_row_id' \
         LIMIT 1",
        &[quoted_table.clone().into()],
    )
    .unwrap_or(None);

    if let Some(idx_name) = existing {
        Spi::run(&format!("DROP INDEX IF EXISTS {idx_name}")) // nosemgrep: rust.spi.run.dynamic-format — DROP INDEX DDL cannot be parameterized; idx_name is obtained from pg_index via ::regclass::text.
            .map_err(|e| {
                PgTrickleError::SpiError(format!("Failed to drop old row_id index: {e}"))
            })?;
    }

    // Rebuild with the new INCLUDE clause
    let auto_index = crate::config::pg_trickle_auto_index();
    const COVERING_INDEX_MAX_COLUMNS: usize = 8;
    let include_clause =
        if auto_index && new_columns.len() <= COVERING_INDEX_MAX_COLUMNS && !new_columns.is_empty()
        {
            let include_cols: Vec<String> = new_columns
                .iter()
                .map(|c| quote_identifier(&c.name).to_string())
                .collect();
            format!(" INCLUDE ({})", include_cols.join(", "))
        } else {
            String::new()
        };

    let index_sql = if has_keyless_source || is_partitioned {
        format!("CREATE INDEX ON {quoted_table} (__pgt_row_id){include_clause}",)
    } else {
        format!("CREATE UNIQUE INDEX ON {quoted_table} (__pgt_row_id){include_clause}",)
    };
    Spi::run(&index_sql)
        .map_err(|e| PgTrickleError::SpiError(format!("Failed to recreate row_id index: {e}")))?;

    Ok(())
}

/// Manage auxiliary columns (__pgt_count, __pgt_count_l/r, __pgt_aux_sum_*,
/// __pgt_aux_count_*, __pgt_aux_sum2_*, __pgt_aux_sumx_*, __pgt_aux_nonnull_*)
/// during ALTER QUERY when the query type or aggregate composition changes.
#[allow(clippy::too_many_arguments)]
fn migrate_aux_columns(
    schema: &str,
    table_name: &str,
    old_needs_pgt_count: bool,
    old_needs_dual_count: bool,
    new_needs_pgt_count: bool,
    new_needs_dual_count: bool,
    new_needs_union_dedup: bool,
    old_avg_aux: &[(String, String, String)],
    new_avg_aux: &[(String, String, String)],
    old_sum2_aux: &[(String, String)],
    new_sum2_aux: &[(String, String)],
    old_covar_aux: &[(String, String)],
    new_covar_aux: &[(String, String)],
    new_statistical_aux_types: &[(String, String)],
    old_nonnull_aux: &[(String, String)],
    new_nonnull_aux: &[(String, String)],
) -> Result<(), PgTrickleError> {
    let quoted_table = format!(
        "{}.{}",
        quote_identifier(schema),
        quote_identifier(table_name),
    );

    let new_storage_needs_pgt_count = new_needs_pgt_count || new_needs_union_dedup;

    // Transition: __pgt_count
    if !old_needs_pgt_count && new_storage_needs_pgt_count && !new_needs_dual_count {
        let sql = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS __pgt_count BIGINT NOT NULL DEFAULT 0",
            quoted_table
        );
        Spi::run(&sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    } else if old_needs_pgt_count && !new_storage_needs_pgt_count && !new_needs_dual_count {
        let sql = format!(
            "ALTER TABLE {} DROP COLUMN IF EXISTS __pgt_count",
            quoted_table
        );
        Spi::run(&sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    }

    // Transition: __pgt_count_l / __pgt_count_r
    if !old_needs_dual_count && new_needs_dual_count {
        let sql = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS __pgt_count_l BIGINT NOT NULL DEFAULT 0",
            quoted_table
        );
        Spi::run(&sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        let sql = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS __pgt_count_r BIGINT NOT NULL DEFAULT 0",
            quoted_table
        );
        Spi::run(&sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        // Drop __pgt_count if it was there and no longer needed
        if old_needs_pgt_count {
            let sql = format!(
                "ALTER TABLE {} DROP COLUMN IF EXISTS __pgt_count",
                quoted_table
            );
            Spi::run(&sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        }
    } else if old_needs_dual_count && !new_needs_dual_count {
        let sql = format!(
            "ALTER TABLE {} DROP COLUMN IF EXISTS __pgt_count_l",
            quoted_table
        );
        Spi::run(&sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        let sql = format!(
            "ALTER TABLE {} DROP COLUMN IF EXISTS __pgt_count_r",
            quoted_table
        );
        Spi::run(&sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        // Add __pgt_count if newly needed
        if new_storage_needs_pgt_count {
            let sql = format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS __pgt_count BIGINT NOT NULL DEFAULT 0",
                quoted_table
            );
            Spi::run(&sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        }
    }

    // Transition: AVG auxiliary columns (__pgt_aux_sum_*, __pgt_aux_count_*)
    let old_avg_names: std::collections::HashSet<(&str, &str)> = old_avg_aux
        .iter()
        .map(|(s, c, _)| (s.as_str(), c.as_str()))
        .collect();
    let new_avg_names: std::collections::HashSet<(&str, &str)> = new_avg_aux
        .iter()
        .map(|(s, c, _)| (s.as_str(), c.as_str()))
        .collect();
    // Add new AVG aux columns
    for (sum_col, count_col, _) in new_avg_aux {
        if !old_avg_names.contains(&(sum_col.as_str(), count_col.as_str())) {
            let sql = format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} NUMERIC NOT NULL DEFAULT 0",
                quoted_table,
                quote_identifier(sum_col),
            );
            Spi::run(&sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            let sql = format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} BIGINT NOT NULL DEFAULT 0",
                quoted_table,
                quote_identifier(count_col),
            );
            Spi::run(&sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        }
    }
    // Drop removed AVG aux columns
    for (sum_col, count_col, _) in old_avg_aux {
        if !new_avg_names.contains(&(sum_col.as_str(), count_col.as_str())) {
            let sql = format!(
                "ALTER TABLE {} DROP COLUMN IF EXISTS {}",
                quoted_table,
                quote_identifier(sum_col),
            );
            Spi::run(&sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            let sql = format!(
                "ALTER TABLE {} DROP COLUMN IF EXISTS {}",
                quoted_table,
                quote_identifier(count_col),
            );
            Spi::run(&sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        }
    }

    // Transition: sum-of-squares auxiliary columns (__pgt_aux_sum2_*)
    let old_sum2_names: std::collections::HashSet<&str> =
        old_sum2_aux.iter().map(|(n, _)| n.as_str()).collect();
    let new_sum2_names: std::collections::HashSet<&str> =
        new_sum2_aux.iter().map(|(n, _)| n.as_str()).collect();
    // Add new sum2 aux columns
    for (col_name, _) in new_sum2_aux {
        if !old_sum2_names.contains(col_name.as_str()) {
            let accumulator_type = new_statistical_aux_types
                .iter()
                .find(|(name, _)| name == col_name)
                .map(|(_, ty)| ty.as_str())
                .unwrap_or("numeric");
            let sql = format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {} NOT NULL DEFAULT 0",
                quoted_table,
                quote_identifier(col_name),
                accumulator_type,
            );
            Spi::run(&sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        }
    }
    // Drop removed sum2 aux columns
    for (col_name, _) in old_sum2_aux {
        if !new_sum2_names.contains(col_name.as_str()) {
            let sql = format!(
                "ALTER TABLE {} DROP COLUMN IF EXISTS {}",
                quoted_table,
                quote_identifier(col_name),
            );
            Spi::run(&sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        }
    }

    // Transition: cross-product auxiliary columns (__pgt_aux_sum{x,y,xy,x2,y2}_*)
    // for CORR/COVAR/REGR_* algebraic maintenance (P3-2).
    let old_covar_names: std::collections::HashSet<&str> =
        old_covar_aux.iter().map(|(n, _)| n.as_str()).collect();
    let new_covar_names: std::collections::HashSet<&str> =
        new_covar_aux.iter().map(|(n, _)| n.as_str()).collect();
    // Add new covar aux columns
    for (col_name, _) in new_covar_aux {
        if !old_covar_names.contains(col_name.as_str()) {
            let accumulator_type = new_statistical_aux_types
                .iter()
                .find(|(name, _)| name == col_name)
                .map(|(_, ty)| ty.as_str())
                .unwrap_or("numeric");
            let sql = format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {} NOT NULL DEFAULT 0",
                quoted_table,
                quote_identifier(col_name),
                accumulator_type,
            );
            Spi::run(&sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        }
    }
    // Drop removed covar aux columns
    for (col_name, _) in old_covar_aux {
        if !new_covar_names.contains(col_name.as_str()) {
            let sql = format!(
                "ALTER TABLE {} DROP COLUMN IF EXISTS {}",
                quoted_table,
                quote_identifier(col_name),
            );
            Spi::run(&sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        }
    }

    // Transition: nonnull-count auxiliary columns (__pgt_aux_nonnull_*)
    // for SUM NULL-transition correction (P2-2).
    let old_nonnull_names: std::collections::HashSet<&str> =
        old_nonnull_aux.iter().map(|(n, _)| n.as_str()).collect();
    let new_nonnull_names: std::collections::HashSet<&str> =
        new_nonnull_aux.iter().map(|(n, _)| n.as_str()).collect();
    // Add new nonnull aux columns
    for (col_name, _) in new_nonnull_aux {
        if !old_nonnull_names.contains(col_name.as_str()) {
            let sql = format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} BIGINT NOT NULL DEFAULT 0",
                quoted_table,
                quote_identifier(col_name),
            );
            Spi::run(&sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        }
    }
    // Drop removed nonnull aux columns
    for (col_name, _) in old_nonnull_aux {
        if !new_nonnull_names.contains(col_name.as_str()) {
            let sql = format!(
                "ALTER TABLE {} DROP COLUMN IF EXISTS {}",
                quoted_table,
                quote_identifier(col_name),
            );
            Spi::run(&sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        }
    }

    Ok(())
}

// ── Core ALTER QUERY implementation ──────────────────────────────────────

/// Perform an in-place query migration on an existing stream table.
/// Called from `alter_stream_table_impl` when `query` is `Some(...)`.
///
/// Executes Phases 0–5 from the ALTER QUERY design:
///   0. Validate & classify
///   1. Suspend & drain
///   2. Tear down old infrastructure
///   3. Migrate storage table
///   4. Update catalog & set up new infrastructure
///   5. Repopulate
fn alter_stream_table_query(
    st: &StreamTableMeta,
    schema: &str,
    table_name: &str,
    new_query: &str,
) -> Result<(), PgTrickleError> {
    // ── Phase 0: Validate & classify ──

    // LSEC-3 (v0.87.7): `alter_stream_table` is SECURITY INVOKER, so the
    // active search_path already is the caller's; only `$user` needs
    // expanding before it is persisted as the new defining_search_path.
    let caller_search_path =
        security_context::capture_caller_context(security_context::EntryContext::SecurityInvoker)?
            .search_path;

    // Run the full rewrite pipeline on the new query
    let original_new_query = new_query.to_string();
    let rw = run_query_rewrite_pipeline(new_query)?;
    let rewritten_query = rw.query;

    // Determine the effective refresh mode — use the ST's current mode
    let mut refresh_mode = st.refresh_mode;

    // Validate and parse the new query
    let vq = validate_and_parse_query(
        &rewritten_query,
        &mut refresh_mode,
        false,
        rw.had_nested_window_rewrite,
    )?;

    // Cycle detection on the new dependency set (ALTER-aware: replaces
    // the existing ST's edges rather than creating a sentinel node).
    // Pass the proposed query so monotonicity of the altered ST's new
    // query is checked when it participates in a cycle.
    check_for_cycles_alter(st.pgt_id, &vq.source_relids, &rewritten_query)?;

    // Get the current storage table columns (excluding internal __pgt_* columns)
    let old_columns = get_storage_table_columns(schema, table_name)?;

    // Classify schema change
    let schema_change = classify_schema_change(&old_columns, &vq.columns);

    // Diff source dependencies
    let old_deps = StDependency::get_for_st(st.pgt_id).unwrap_or_default();
    let dep_diff = diff_dependencies(&old_deps, &vq.source_relids);

    // Detect old auxiliary column state from current ST metadata
    let old_needs_pgt_count = crate::dvm::query_needs_pgt_count(&st.defining_query);
    let old_needs_dual_count = crate::dvm::query_needs_dual_count(&st.defining_query);
    let old_avg_aux = crate::dvm::query_avg_aux_columns(&st.defining_query);
    let old_sum2_aux = crate::dvm::query_sum2_aux_columns(&st.defining_query);
    let old_covar_aux = crate::dvm::query_covar_aux_columns(&st.defining_query);
    let old_nonnull_aux = crate::dvm::query_nonnull_aux_columns(&st.defining_query);

    // ── Phase 1: Suspend ──
    StreamTableMeta::update_status(st.pgt_id, StStatus::Suspended)?;

    // Flush pending deferred cleanups for sources being removed
    let removed_oids: Vec<u32> = dep_diff.removed.iter().map(|(o, _)| o.to_u32()).collect();
    if !removed_oids.is_empty() {
        crate::refresh::flush_pending_cleanups_for_oids(&removed_oids);
    }

    // ── Phase 2: Tear down old infrastructure ──

    // Remove CDC/IVM triggers from sources that are no longer needed
    for (source_oid, source_type) in &dep_diff.removed {
        if source_type == "TABLE" {
            if refresh_mode.is_immediate() {
                if let Err(e) = crate::ivm::cleanup_ivm_triggers(*source_oid, st.pgt_id) {
                    pgrx::warning!(
                        "Failed to clean up IVM triggers for removed source {}: {}",
                        source_oid.to_u32(),
                        e
                    );
                }
            } else {
                let old_dep = old_deps.iter().find(|d| d.source_relid == *source_oid);
                let cdc_mode = old_dep.map(|d| d.cdc_mode).unwrap_or(CdcMode::Trigger);
                if let Err(e) = cleanup_cdc_for_source(*source_oid, cdc_mode, Some(st.pgt_id)) {
                    pgrx::warning!(
                        "Failed to clean up CDC for removed source {}: {}",
                        source_oid.to_u32(),
                        e
                    );
                }
            }
        }
    }

    // Invalidate caches
    template_cache::invalidate(st.pgt_id);
    shmem::bump_cache_generation();

    // Flush MERGE template cache and deallocate prepared statements
    refresh::invalidate_merge_cache(st.pgt_id);

    // ── Phase 3: Migrate storage table ──

    let new_pgt_relid = match &schema_change {
        SchemaChange::Same => {
            // The output columns are unchanged, but set-operation FULL
            // storage may have a non-unique row-id index. Rebuild it when
            // the query's keyless requirement changes.
            if st.has_keyless_source != vq.has_keyless_source {
                rebuild_row_id_index(
                    schema,
                    table_name,
                    &vq.columns,
                    vq.has_keyless_source,
                    st.st_partition_key.is_some(),
                )?;
            }
            st.pgt_relid
        }
        SchemaChange::Compatible { added, removed } => {
            migrate_storage_table_compatible(schema, table_name, added, removed)?;
            // Dropping columns may destroy the covering INCLUDE index on
            // __pgt_row_id.  A query transition can also change whether
            // duplicate row IDs are valid (notably set-operation FULL
            // storage), so rebuild whenever the keyless flag changes.
            if !removed.is_empty() || st.has_keyless_source != vq.has_keyless_source {
                rebuild_row_id_index(
                    schema,
                    table_name,
                    &vq.columns,
                    vq.has_keyless_source,
                    st.st_partition_key.is_some(),
                )?;
            }
            st.pgt_relid
        }
        SchemaChange::Incompatible { reason } => {
            pgrx::warning!(
                "pg_trickle: ALTER QUERY requires full storage rebuild: {}. \
                 The storage table OID will change.",
                reason
            );

            // Detach pgt_relid before DROP so the sql_drop event trigger
            // does not recognise the table as ST storage and delete the
            // catalog row.
            Spi::run_with_args(
                "UPDATE pgtrickle.pgt_stream_tables \
                 SET pgt_relid = 0 WHERE pgt_id = $1",
                &[st.pgt_id.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

            // Drop existing storage table
            let drop_sql = format!(
                "DROP TABLE IF EXISTS {}.{} CASCADE",
                quote_identifier(schema),
                quote_identifier(table_name),
            );
            Spi::run(&drop_sql).map_err(|e| {
                PgTrickleError::SpiError(format!("Failed to drop storage table: {}", e))
            })?;

            // Recreate with new schema
            let storage_needs_pgt_count = vq.needs_pgt_count || vq.needs_union_dedup;
            setup_storage_table(
                schema,
                table_name,
                &vq.columns,
                storage_needs_pgt_count,
                vq.needs_dual_count,
                vq.has_keyless_source,
                refresh_mode,
                vq.parsed_tree.as_ref(),
                &vq.avg_aux_columns,
                &vq.sum2_aux_columns,
                &vq.covar_aux_columns,
                &vq.nonnull_aux_columns,
                &vq.statistical_aux_types,
                st.st_partition_key.as_deref(), // A1-1c: preserve partition key on query change
                st.storage_fillfactor,          // HOT-1: preserve fillfactor
            )?
        }
    };

    // For Same/Compatible, also handle auxiliary column transitions
    if !matches!(schema_change, SchemaChange::Incompatible { .. }) {
        migrate_aux_columns(
            schema,
            table_name,
            old_needs_pgt_count,
            old_needs_dual_count,
            vq.needs_pgt_count,
            vq.needs_dual_count,
            vq.needs_union_dedup,
            &old_avg_aux,
            &vq.avg_aux_columns,
            &old_sum2_aux,
            &vq.sum2_aux_columns,
            &old_covar_aux,
            &vq.covar_aux_columns,
            &vq.statistical_aux_types,
            &old_nonnull_aux,
            &vq.nonnull_aux_columns,
        )?;
    }

    // ── Phase 4: Update catalog & set up new infrastructure ──

    // Compute the effective defining query for storage — TopK stores the base query
    let defining_query = if vq.topk_info.is_some() {
        &vq.effective_query
    } else {
        &rewritten_query
    };

    // Update the pgt_stream_tables catalog row
    let original_query_opt = if original_new_query != *defining_query {
        Some(original_new_query.as_str())
    } else {
        None
    };

    let functions_used = vq.parsed_tree.as_ref().map(|pr| pr.functions_used());
    let topk_limit = vq
        .topk_info
        .as_ref()
        .map(|info| validation::positive_i32("topk_limit", info.limit_value))
        .transpose()?;
    let topk_order_by_owned = vq.topk_info.as_ref().map(|i| i.order_by_sql.clone());
    let topk_order_by = topk_order_by_owned.as_deref();
    let topk_offset = vq
        .topk_info
        .as_ref()
        .and_then(|i| i.offset_value)
        .map(|value| validation::nonnegative_i32("topk_offset", value))
        .transpose()?;
    if let (Some(limit), Some(offset)) = (topk_limit, topk_offset)
        && limit.checked_add(offset).is_none()
    {
        return Err(PgTrickleError::InvalidArgument(
            "topk_limit + topk_offset exceeds the supported integer range".to_string(),
        ));
    }

    // F5 (v0.36.0): Online schema evolution — when the GUC is enabled and the
    // schema change only adds columns (no removals, no incompatible changes),
    // preserve the existing frontier and is_populated flag so that the next
    // refresh continues incrementally rather than performing a full reinit.
    let preserve_frontier = config::pg_trickle_online_schema_evolution()
        && matches!(
            &schema_change,
            SchemaChange::Compatible { added, removed } if !added.is_empty() && removed.is_empty()
        );

    let (frontier_clause, populated_clause) = if preserve_frontier {
        // Keep existing frontier and is_populated — differential refresh continues
        pgrx::log!(
            "pg_trickle: online schema evolution enabled; preserving frontier for {}.{}",
            schema,
            table_name,
        );
        ("", "")
    } else {
        ("frontier = NULL,", "is_populated = false,")
    };

    Spi::run_with_args(
        &format!(
            "UPDATE pgtrickle.pgt_stream_tables SET \
             pgt_relid = $1, \
             defining_query = $2, \
             original_query = $3, \
             functions_used = $4, \
             topk_limit = $5, \
             topk_order_by = $6, \
             topk_offset = $7, \
             needs_reinit = false, \
             defining_query_hash = $10, \
             defining_search_path = $11, \
             {} \
             {} \
             has_keyless_source = $8, \
             updated_at = now() \
             WHERE pgt_id = $9",
            frontier_clause, populated_clause,
        ),
        &[
            new_pgt_relid.into(),
            defining_query.into(),
            original_query_opt.into(),
            functions_used.into(),
            topk_limit.into(),
            topk_order_by.into(),
            topk_offset.into(),
            vq.has_keyless_source.into(),
            st.pgt_id.into(),
            crate::catalog::compute_defining_query_hash(defining_query).into(),
            caller_search_path.into(),
        ],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    // Delete old dependency rows and insert new ones
    StDependency::delete_for_st(st.pgt_id)?;

    let columns_used_map = vq
        .parsed_tree
        .as_ref()
        .map(|pr| pr.source_columns_used())
        .unwrap_or_default();

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
            st.pgt_id,
            *source_oid,
            source_type,
            cols,
            snapshot,
            fingerprint,
        )?;
    }

    // Set up CDC/IVM triggers for newly added sources
    let change_schema = config::pg_trickle_change_buffer_schema();
    for (source_oid, source_type) in &dep_diff.added {
        if source_type == "TABLE" {
            if refresh_mode.is_immediate() {
                let lock_mode = crate::ivm::IvmLockMode::for_query(defining_query);
                crate::ivm::setup_ivm_triggers(*source_oid, st.pgt_id, new_pgt_relid, lock_mode)?;
            } else {
                setup_cdc_for_source(*source_oid, st.pgt_id, &change_schema)?;
            }
        } else if source_type == "FOREIGN_TABLE" && !refresh_mode.is_immediate() {
            cdc::setup_foreign_table_polling(*source_oid, st.pgt_id, &change_schema)?;
        } else if source_type == "MATVIEW" && !refresh_mode.is_immediate() {
            cdc::setup_matview_polling(*source_oid, st.pgt_id, &change_schema)?;
        } else if source_type == "STREAM_TABLE"
            && !refresh_mode.is_immediate()
            && let Some(upstream_pgt_id) = StreamTableMeta::pgt_id_for_relid(*source_oid)
        {
            cdc::ensure_st_change_buffer(upstream_pgt_id, *source_oid, &change_schema)?;
        }
    }

    // Sync CDC trigger functions and change buffer columns for kept sources.
    // When ALTER QUERY adds references to new source columns (e.g. a query
    // changing from `SELECT id, val` to `SELECT id, val, status`), the change
    // buffer for the unchanged source still lacks `new_status`/`old_status`.
    // Rebuilding the trigger function re-reads the updated catalog dependency
    // and calls sync_change_buffer_columns to add the missing columns.
    if !refresh_mode.is_immediate() {
        for (source_oid, source_type) in &dep_diff.kept {
            if source_type == "TABLE" {
                let cdc_mode = old_deps
                    .iter()
                    .find(|d| d.source_relid == *source_oid)
                    .map(|d| d.cdc_mode)
                    .unwrap_or(CdcMode::Trigger);
                if matches!(cdc_mode, CdcMode::Trigger)
                    && let Err(e) = cdc::rebuild_cdc_trigger_function(*source_oid, &change_schema)
                {
                    pgrx::warning!(
                        "pg_trickle: failed to sync CDC trigger for kept source {}: {}",
                        source_oid.to_u32(),
                        e
                    );
                }
            }
        }
    }

    // Register view soft-dependencies if view inlining was applied
    if original_query_opt.is_some()
        && let Ok(original_sources) = extract_source_relations(&original_new_query)
    {
        for (src_oid, src_type) in &original_sources {
            if src_type == "VIEW" {
                let already_registered = vq.source_relids.iter().any(|(o, _)| o == src_oid);
                if !already_registered {
                    StDependency::insert_with_snapshot(
                        st.pgt_id, *src_oid, src_type, None, None, None,
                    )?;
                }
            }
        }
    }

    // Signal DAG rebuild and cache invalidation
    shmem::signal_dag_invalidation(st.pgt_id);
    template_cache::invalidate(st.pgt_id);
    shmem::bump_cache_generation();

    // ── Phase 5: Repopulate ──

    // Execute a full refresh to populate the storage table with new query results
    let source_oids: Vec<pg_sys::Oid> = vq
        .source_relids
        .iter()
        .filter(|(_, t)| t == "TABLE")
        .map(|(o, _)| *o)
        .collect();

    // Re-load ST with updated metadata for the refresh
    let updated_st = StreamTableMeta::get_by_name(schema, table_name)?;
    execute_manual_full_refresh(&updated_st, schema, table_name, &source_oids)?;

    // ERR-1f: Clear any previous error state now that the query has been fixed
    // and the refresh succeeded. This ensures alter_stream_table with a fixed
    // query always produces a clean ACTIVE state, even when called directly on
    // an ERROR table (without a prior resume_stream_table call).
    let _ = StreamTableMeta::clear_error_state(st.pgt_id);

    // Re-activate the stream table
    StreamTableMeta::update_status(st.pgt_id, StStatus::Active)?;

    // Pre-warm delta SQL + MERGE template cache for DIFFERENTIAL mode
    if refresh_mode == RefreshMode::Differential {
        let st = StreamTableMeta::get_by_name(schema, table_name)?;
        refresh::prewarm_merge_cache(&st);
    }

    // CYC-6: Recompute SCC assignments — the query change may have created
    // or broken a cycle.
    if config::pg_trickle_allow_circular()
        && let Err(e) = assign_scc_ids_from_dag()
    {
        pgrx::warning!("Failed to recompute SCCs after ALTER QUERY: {}", e);
    }

    // ERG-F: warn so the client sees the full refresh regardless of log_min_messages.
    pgrx::warning!(
        "pg_trickle: stream table {}.{} ALTER QUERY applied a full refresh \
         (schema change: {}). This may take time on large tables.",
        schema,
        table_name,
        match &schema_change {
            SchemaChange::Same => "same",
            SchemaChange::Compatible { .. } => "compatible",
            SchemaChange::Incompatible { .. } => "incompatible (full rebuild)",
        }
    );

    Ok(())
}

/// A1-1c: Change the partition key on an existing stream table.
///
/// This is a destructive operation that:
/// 1. Validates the new partition key against the ST's output columns.
/// 2. Drops the old storage table (detaching pgt_relid first).
/// 3. Recreates it with the new partition scheme (or unpartitioned).
/// 4. Updates the catalog.
/// 5. Runs a full refresh to repopulate.
fn alter_stream_table_partition_key(
    st: &StreamTableMeta,
    schema: &str,
    table_name: &str,
    new_partition_key: Option<&str>,
) -> Result<(), PgTrickleError> {
    // Get current storage columns for validation.
    let columns = get_storage_table_columns(schema, table_name)?;

    // Validate new partition key against current columns.
    if let Some(pk) = new_partition_key {
        validate_partition_key(pk, &columns)?;
    }

    pgrx::warning!(
        "pg_trickle: ALTER partition_by on {schema}.{table_name} requires full storage rebuild. \
         The storage table will be recreated and a full refresh applied."
    );

    // Detach pgt_relid so the sql_drop event trigger does not delete the
    // catalog row when we drop the old table.
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET pgt_relid = 0 WHERE pgt_id = $1",
        &[st.pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    // Drop the old storage table (CASCADE drops child partitions too).
    let drop_sql = format!(
        "DROP TABLE IF EXISTS {}.{} CASCADE",
        quote_identifier(schema),
        quote_identifier(table_name),
    );
    Spi::run(&drop_sql)
        .map_err(|e| PgTrickleError::SpiError(format!("Failed to drop storage table: {e}")))?;

    // Recompute auxiliary column needs from the defining query.
    let needs_pgt_count = crate::dvm::query_needs_pgt_count(&st.defining_query);
    let needs_dual_count = crate::dvm::query_needs_dual_count(&st.defining_query);
    let avg_aux = crate::dvm::query_avg_aux_columns(&st.defining_query);
    let sum2_aux = crate::dvm::query_sum2_aux_columns(&st.defining_query);
    let covar_aux = crate::dvm::query_covar_aux_columns(&st.defining_query);
    let nonnull_aux = crate::dvm::query_nonnull_aux_columns(&st.defining_query);
    let statistical_aux_types = crate::dvm::query_statistical_aux_types(&st.defining_query);

    // Recreate the storage table with the new partition scheme.
    let new_pgt_relid = setup_storage_table(
        schema,
        table_name,
        &columns,
        needs_pgt_count,
        needs_dual_count,
        st.has_keyless_source,
        st.refresh_mode,
        None, // parsed_tree not needed for storage creation
        &avg_aux,
        &sum2_aux,
        &covar_aux,
        &nonnull_aux,
        &statistical_aux_types,
        new_partition_key,
        st.storage_fillfactor, // HOT-1: preserve fillfactor
    )?;

    // Update catalog: new relid + new partition key.
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET pgt_relid = $1, st_partition_key = $2, \
             is_populated = false, frontier = NULL, updated_at = now() \
         WHERE pgt_id = $3",
        &[
            new_pgt_relid.into(),
            new_partition_key.into(),
            st.pgt_id.into(),
        ],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    // Invalidate caches.
    template_cache::invalidate(st.pgt_id);
    shmem::bump_cache_generation();
    refresh::invalidate_merge_cache(st.pgt_id);

    // Full refresh to repopulate.
    let updated_st = StreamTableMeta::get_by_name(schema, table_name)?;
    let deps = StDependency::get_for_st(st.pgt_id).unwrap_or_default();
    let source_oids: Vec<pg_sys::Oid> = deps
        .iter()
        .filter(|d| d.source_type == "TABLE")
        .map(|d| d.source_relid)
        .collect();
    execute_manual_full_refresh(&updated_st, schema, table_name, &source_oids)?;

    pgrx::info!(
        "pg_trickle: partition key for {schema}.{table_name} changed to {}; full refresh applied.",
        new_partition_key.unwrap_or("(none)"),
    );

    Ok(())
}

/// Get the user-visible columns of a storage table (excluding __pgt_* internal columns).
fn get_storage_table_columns(
    schema: &str,
    table_name: &str,
) -> Result<Vec<ColumnDef>, PgTrickleError> {
    Spi::connect(|client| {
        let table = client
            .select(
                "SELECT a.attname::text, a.atttypid \
                 FROM pg_attribute a \
                 JOIN pg_class c ON c.oid = a.attrelid \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname = $2 \
                 AND a.attnum > 0 AND NOT a.attisdropped \
                 AND a.attname NOT LIKE '__pgt_%' \
                 ORDER BY a.attnum",
                None,
                &[schema.into(), table_name.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        let mut columns = Vec::new();
        for row in table {
            let map_spi = |e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string());
            let name = row.get::<String>(1).map_err(map_spi)?.unwrap_or_default();
            let type_oid_raw = row
                .get::<pg_sys::Oid>(2)
                .map_err(map_spi)?
                .unwrap_or(pg_sys::InvalidOid);
            columns.push(ColumnDef {
                name,
                type_oid: PgOid::from(type_oid_raw),
            });
        }
        Ok(columns)
    })
}

// A45-8: Centralized options struct for all create_stream_table variants.
/// All `create_stream_table` entry points construct this struct first and then
/// call [`create_stream_table_impl`], which takes ownership of it.  This
/// centralises defaults, validation, and documentation in one place.
#[derive(Debug, Default)]
pub(crate) struct CreateStreamTableOptions<'a> {
    pub(crate) name: &'a str,
    pub(crate) query: &'a str,
    pub(crate) schedule: Option<&'a str>,
    /// Raw refresh-mode string as passed by the caller (e.g. `"AUTO"`,
    /// `"DIFFERENTIAL"`, `"FULL"`).  Parsed inside `create_stream_table_impl`.
    pub(crate) refresh_mode_str: &'a str,
    pub(crate) initialize: bool,
    pub(crate) diamond_consistency: Option<&'a str>,
    pub(crate) diamond_schedule_policy: Option<&'a str>,
    pub(crate) requested_cdc_mode: Option<&'a str>,
    pub(crate) append_only: bool,
    pub(crate) pooler_compatibility_mode: bool,
    pub(crate) partition_by: Option<&'a str>,
    pub(crate) max_differential_joins: Option<i32>,
    pub(crate) max_delta_fraction: Option<f64>,
    /// CITUS-7: If set and Citus is loaded, convert the storage table to a
    /// Citus distributed table using this column as the distribution key.
    pub(crate) output_distribution_column: Option<&'a str>,
    /// CORR-1/UX-1 (v0.36.0): temporal IVM mode.
    pub(crate) temporal_mode: bool,
    /// CORR-2/UX-3 (v0.36.0): columnar storage backend
    /// (`"heap"`, `"citus"`, or `"none"`).
    pub(crate) storage_backend: Option<&'a str>,
    /// HOT-1 (v0.73.0): heap fillfactor (10–100). `None` = PostgreSQL default (100).
    pub(crate) storage_fillfactor: Option<i32>,
    /// v0.86.0: declared freshness target (`interval`, `on_commit`, `manual`).
    pub(crate) target_freshness: Option<&'a str>,
    /// Entry context used to capture the defining query's search path.
    pub(crate) entry_context: Option<security_context::EntryContext>,
}

impl<'a> CreateStreamTableOptions<'a> {
    fn new(name: &'a str, query: &'a str) -> Self {
        Self {
            name,
            query,
            refresh_mode_str: "AUTO",
            initialize: true,
            ..Default::default()
        }
    }
}

pub(crate) fn create_stream_table_impl(
    opts: CreateStreamTableOptions<'_>,
) -> Result<(), PgTrickleError> {
    let CreateStreamTableOptions {
        name,
        query,
        schedule,
        refresh_mode_str,
        initialize,
        diamond_consistency,
        diamond_schedule_policy,
        requested_cdc_mode,
        append_only,
        pooler_compatibility_mode,
        partition_by,
        max_differential_joins,
        max_delta_fraction,
        output_distribution_column,
        temporal_mode,
        storage_backend,
        storage_fillfactor,
        target_freshness,
        entry_context,
    } = opts;
    let is_auto = RefreshMode::is_auto_str(refresh_mode_str);
    let mut refresh_mode = RefreshMode::from_str(refresh_mode_str)?;
    let target = parse_target_freshness(target_freshness)?;
    if let Some(target) = target
        && target.mode == TargetFreshnessMode::OnCommit
    {
        refresh_mode = RefreshMode::Immediate;
    }
    // LSEC-1 (v0.87.7): capture the exact original-caller search_path (with
    // a bare `$user` expanded) instead of guessing `"<user>", public`.
    let invoker_search_path = security_context::capture_caller_context(
        entry_context.unwrap_or(security_context::EntryContext::SecurityDefiner),
    )?
    .search_path;

    // Parse diamond consistency — default to 'atomic' when not specified
    let dc = match diamond_consistency {
        Some(s) => {
            let val = s.to_lowercase();
            match val.as_str() {
                "none" | "atomic" => DiamondConsistency::from_sql_str(&val),
                other => {
                    return Err(PgTrickleError::InvalidArgument(format!(
                        "invalid diamond_consistency value: '{}' (expected 'none' or 'atomic')",
                        other
                    )));
                }
            }
        }
        None => DiamondConsistency::Atomic,
    };

    // Parse diamond schedule policy — default to 'fastest' when not specified
    let dsp = match diamond_schedule_policy {
        Some(s) => match DiamondSchedulePolicy::from_sql_str(s) {
            Some(p) => p,
            None => {
                return Err(PgTrickleError::InvalidArgument(format!(
                    "invalid diamond_schedule_policy value: '{}' (expected 'fastest' or 'slowest')",
                    s
                )));
            }
        },
        None => DiamondSchedulePolicy::Fastest,
    };

    // G15-PV: diamond_schedule_policy='slowest' only makes sense when
    // diamond_consistency='atomic'.  Without atomic reads the 'slowest'
    // policy delays refreshes at convergence nodes without providing any
    // consistency guarantee.
    if dsp == DiamondSchedulePolicy::Slowest && dc == DiamondConsistency::None {
        return Err(PgTrickleError::InvalidArgument(
            "diamond_schedule_policy = 'slowest' requires diamond_consistency = 'atomic'. \
             The 'slowest' policy is only meaningful when atomic cross-branch reads are \
             enabled. Set diamond_consistency = 'atomic' or use diamond_schedule_policy = 'fastest'."
                .to_string(),
        ));
    }

    // Parse schema.name
    let (schema, table_name) = parse_qualified_name(name)?;
    let qualified_name = format!("{schema}.{table_name}");
    validate_output_schema_create(&schema)?;

    // HOT-1: validate fillfactor range.
    if let Some(ff) = storage_fillfactor
        && !(10..=100).contains(&ff)
    {
        return Err(PgTrickleError::InvalidArgument(format!(
            "invalid fillfactor value: {} (expected 10–100)",
            ff
        )));
    }
    if max_differential_joins.is_some_and(|mdj| mdj < 0) {
        return Err(PgTrickleError::InvalidArgument(
            "max_differential_joins must be non-negative".to_string(),
        ));
    }
    if let Some(mdf) = max_delta_fraction {
        validation::finite_fraction("max_delta_fraction", mdf)?;
    }

    // Parse and validate schedule
    let schedule_str = if refresh_mode.is_immediate() {
        None
    } else {
        match schedule {
            Some(s) if s.trim().eq_ignore_ascii_case("calculated") => None,
            Some(s) => {
                let _schedule = parse_schedule(s)?;
                Some(s.trim().to_string())
            }
            None => {
                return Err(PgTrickleError::InvalidArgument(
                    "use 'calculated' instead of NULL to set CALCULATED schedule".to_string(),
                ));
            }
        }
    };

    if let Some(target) = target
        && target.mode != TargetFreshnessMode::Clear
        && schedule.is_some_and(|value| !value.trim().eq_ignore_ascii_case("calculated"))
    {
        return Err(PgTrickleError::InvalidArgument(
            "schedule and target_freshness are ambiguous; choose one control (HINT: use target_freshness or schedule, not both)".into(),
        ));
    }

    let (requested_cdc_mode_override, effective_requested_cdc_mode, cdc_mode_source) =
        resolve_requested_cdc_mode(requested_cdc_mode)?;
    enforce_cdc_refresh_mode_interaction(
        &qualified_name,
        refresh_mode,
        &effective_requested_cdc_mode,
        cdc_mode_source,
    )?;
    if !refresh_mode.is_immediate() {
        validate_requested_cdc_mode_requirements(&effective_requested_cdc_mode)?;
    }

    // ── Query rewrite, validation, and parse ───────────────────────
    let original_query = query.to_string();
    let (rw, vq) = with_invoker_search_path(&invoker_search_path, || {
        let rw = run_query_rewrite_pipeline(query)?;
        let vq = validate_and_parse_query(
            &rw.query,
            &mut refresh_mode,
            is_auto,
            rw.had_nested_window_rewrite,
        )?;
        Ok((rw, vq))
    })?;
    let query = &rw.query;
    validate_source_access(&vq.source_relids)?;
    // Warnings
    warn_source_table_properties(&vq.source_relids);
    emit_creation_warnings(&collect_creation_warnings(
        query,
        refresh_mode_str,
        refresh_mode,
        &vq.source_relids,
    ));

    // Summary warning when AUTO mode resulted in FULL refresh
    if is_auto && refresh_mode == RefreshMode::Full {
        pgrx::warning!(
            "[pg_trickle] Stream table '{}' will use FULL refresh instead of DIFFERENTIAL. \
             Each refresh will recompute the entire result set from scratch, which is slower \
             than incremental maintenance. See the warnings above for the specific reason \
             and how to fix it. \
             Use SELECT * FROM pgtrickle.explain_refresh_mode('{}') to check the effective \
             mode after the first refresh.",
            name,
            name,
        );
    }

    // Validate append_only flag
    if append_only {
        if refresh_mode == RefreshMode::Full {
            return Err(PgTrickleError::InvalidArgument(
                "append_only is not supported with FULL refresh mode. \
                 Use DIFFERENTIAL or AUTO refresh mode."
                    .to_string(),
            ));
        }
        if refresh_mode.is_immediate() {
            return Err(PgTrickleError::InvalidArgument(
                "append_only is not supported with IMMEDIATE refresh mode. \
                 Use DIFFERENTIAL or AUTO refresh mode."
                    .to_string(),
            ));
        }
        if vq.has_keyless_source {
            return Err(PgTrickleError::InvalidArgument(
                "append_only is not supported for stream tables with keyless sources. \
                 Add a PRIMARY KEY to all source tables first."
                    .to_string(),
            ));
        }
    }

    // Check for duplicate
    if StreamTableMeta::get_by_name(&schema, &table_name).is_ok() {
        return Err(PgTrickleError::AlreadyExists(format!(
            "{}.{}",
            schema, table_name
        )));
    }

    // A1-1: Validate partition_by if provided.
    if let Some(pk) = partition_by {
        validate_partition_key(pk, &vq.columns)?;
        // Partitioned stream tables with IMMEDIATE refresh are not supported —
        // IMMEDIATE triggers fire at DML time and the partition-key range is
        // not known until the delta is accumulated.
        if refresh_mode.is_immediate() {
            return Err(PgTrickleError::InvalidArgument(
                "partition_by is not supported with IMMEDIATE refresh mode. \
                 Use DIFFERENTIAL or AUTO refresh mode."
                    .to_string(),
            ));
        }
    }

    // Cycle detection
    check_for_cycles(&vq.source_relids)?;

    // ── Phase 1: DDL ──

    // Create storage table, indexes, and DML guard trigger
    let storage_needs_pgt_count = vq.needs_pgt_count || vq.needs_union_dedup;
    let pgt_relid = setup_storage_table(
        &schema,
        &table_name,
        &vq.columns,
        storage_needs_pgt_count,
        vq.needs_dual_count,
        vq.has_keyless_source,
        refresh_mode,
        vq.parsed_tree.as_ref(),
        &vq.avg_aux_columns,
        &vq.sum2_aux_columns,
        &vq.covar_aux_columns,
        &vq.nonnull_aux_columns,
        &vq.statistical_aux_types,
        partition_by,
        storage_fillfactor,
    )?;

    // F4: Fix vector aggregate column dimensions (VectorAvg/VectorSum output
    // columns need vector(N) type with explicit dimension for HNSW / IVFFlat
    // index support). avg(vector(3)) returns undimensioned `vector` at the
    // SQL type-inference level; we post-fix via ALTER COLUMN using the source
    // column's atttypmod.
    if crate::config::pg_trickle_enable_vector_agg()
        && let Some(ref pr) = vq.parsed_tree
    {
        with_invoker_search_path(&invoker_search_path, || {
            fix_vector_aggregate_column_types(&schema, &table_name, &pr.tree)
        })?;
    }

    // CITUS-7: Distribute the output storage table when requested and Citus is available.
    if let Some(dist_col) = output_distribution_column {
        if crate::citus::is_citus_loaded() {
            let qualified = format!(
                "{}.{}",
                quote_identifier(&schema),
                quote_identifier(&table_name),
            );
            Spi::run_with_args(
                "SELECT create_distributed_table($1, $2)",
                &[qualified.as_str().into(), dist_col.into()],
            )
            .map_err(|e| {
                PgTrickleError::SpiError(format!(
                    "create_distributed_table for {} on column '{}': {}",
                    qualified, dist_col, e
                ))
            })?;
            pgrx::info!(
                "pg_trickle: distributed stream table {} on column '{}'",
                qualified,
                dist_col,
            );
        } else {
            return Err(PgTrickleError::InvalidArgument(
                "output_distribution_column requires Citus to be installed and loaded".to_string(),
            ));
        }
    }

    // Insert catalog entry + dependency edges
    // For TopK, store the base query (ORDER BY/LIMIT stripped) as defining_query.
    // The ORDER BY, LIMIT, and OFFSET are stored separately as topk_order_by,
    // topk_limit, and topk_offset.
    let catalog_defining_query = if vq.topk_info.is_some() {
        &vq.effective_query
    } else {
        query
    };
    let original_query_opt = if original_query != *catalog_defining_query {
        Some(original_query.as_str())
    } else {
        None
    };

    // CORR-1/UX-1 (v0.36.0): Add __pgt_valid_from / __pgt_valid_to columns for temporal mode.
    if temporal_mode {
        let temporal_sql = format!(
            "ALTER TABLE {}.{} \
             ADD COLUMN IF NOT EXISTS __pgt_valid_from TIMESTAMPTZ NOT NULL DEFAULT now(), \
             ADD COLUMN IF NOT EXISTS __pgt_valid_to TIMESTAMPTZ",
            quote_identifier(&schema),
            quote_identifier(&table_name),
        );
        Spi::run(&temporal_sql).map_err(|e| {
            PgTrickleError::SpiError(format!(
                "Failed to add temporal columns to storage table: {}",
                e
            ))
        })?;
    }

    // CORR-2/UX-3 (v0.36.0): Normalize storage_backend value for catalog storage.
    let storage_backend_str = match storage_backend {
        Some(b) => {
            let b = b.to_lowercase();
            match b.as_str() {
                "heap" | "citus" | "none" => b,
                other => {
                    return Err(PgTrickleError::InvalidArgument(format!(
                        "invalid storage_backend '{}': expected 'heap', 'citus', or 'none'",
                        other
                    )));
                }
            }
        }
        None => {
            // Use GUC-configured columnar backend if set, otherwise heap
            use crate::config::{ColumnarBackend, pg_trickle_columnar_backend};
            match pg_trickle_columnar_backend() {
                ColumnarBackend::None => "heap".to_string(),
                ColumnarBackend::Citus => "citus".to_string(),
            }
        }
    };

    // Capture before schedule_str is moved into insert_catalog_and_deps.
    let is_calculated = schedule_str.is_none();
    let pgt_id = insert_catalog_and_deps(
        pgt_relid,
        &schema,
        &table_name,
        catalog_defining_query,
        original_query_opt,
        schedule_str,
        refresh_mode,
        &vq,
        dc,
        dsp,
        requested_cdc_mode_override.as_deref(),
        append_only,
        pooler_compatibility_mode,
        partition_by,
        max_differential_joins,
        max_delta_fraction,
        temporal_mode,
        &storage_backend_str,
        storage_fillfactor,
        &invoker_search_path,
    )?;

    if let Some(target) = target {
        apply_target_freshness(pgt_id, target)?;
    }
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_stream_tables SET requested_refresh_mode = $1 WHERE pgt_id = $2",
        &[refresh_mode_str.to_ascii_uppercase().into(), pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    // ── Phase 2: CDC / IVM trigger setup ──
    setup_trigger_infrastructure(&vq.source_relids, refresh_mode, pgt_id, pgt_relid, query)?;
    transfer_output_table_ownership(&schema, &table_name)?;

    // ── NS-5: Diamond consistency NOTICE ──
    // When the user explicitly opted out of atomic reads (diamond_consistency='none'),
    // check if this new ST is a diamond convergence point and advise.
    if dc == DiamondConsistency::None
        && let Ok(dag) = StDag::build_from_catalog(config::pg_trickle_default_schedule_seconds())
    {
        let diamonds = dag.detect_diamonds();
        if diamonds
            .iter()
            .any(|d| d.convergence == NodeId::StreamTable(pgt_id))
        {
            pgrx::notice!(
                "pg_trickle: Diamond dependency detected for \"{}\".\"{}\" and \
                 diamond_consistency is 'none' — cross-branch reads may be inconsistent. \
                 Consider diamond_consistency='atomic' for consistent results.",
                schema,
                table_name
            );
        }
    }

    // ── NS-7: CALCULATED schedule with no downstream NOTICE ──
    // CALCULATED stream tables inherit their schedule from downstream dependents.
    // If none exist yet, their schedule falls back to the default GUC and the user
    // may not realise rows won't be refreshed on their intended cadence.
    if is_calculated && !refresh_mode.is_immediate() {
        let has_downstream = Spi::get_one::<bool>(&format!(
            "SELECT EXISTS(\
               SELECT 1 FROM pgtrickle.pgt_dependencies d \
               WHERE d.source_relid = {relid} AND d.pgt_id != {pid}\
             )",
            relid = pgt_relid.to_u32(),
            pid = pgt_id,
        ))
        .unwrap_or(Some(false))
        .unwrap_or(false);
        if !has_downstream {
            let fallback_secs = config::pg_trickle_default_schedule_seconds();
            pgrx::notice!(
                "pg_trickle: Stream table \"{}\".\"{}\" uses CALCULATED schedule but has no \
                 downstream dependents yet — it will fall back to the default schedule \
                 (pg_trickle.default_schedule_seconds = {}s). Add a downstream stream table \
                 that references this one to activate the intended schedule.",
                schema,
                table_name,
                fallback_secs
            );
        }
    }

    // ── Phase 2a: CYC-6 — Assign SCC IDs when circular dependencies exist ──
    if config::pg_trickle_allow_circular() {
        assign_scc_ids_from_dag()?;
    }

    // ── Phase 2b: Register view soft-dependencies for DDL tracking ──
    if original_query_opt.is_some()
        && let Ok(original_sources) = with_invoker_search_path(&invoker_search_path, || {
            extract_source_relations(&original_query)
        })
    {
        for (src_oid, src_type) in &original_sources {
            if src_type == "VIEW" {
                let already_registered = vq.source_relids.iter().any(|(o, _)| o == src_oid);
                if !already_registered {
                    StDependency::insert_with_snapshot(
                        pgt_id, *src_oid, src_type, None, None, None,
                    )?;
                }
            }
        }
    }

    // Initialize if requested
    if initialize {
        let t_init = Instant::now();
        initialize_st(
            &schema,
            &table_name,
            query,
            pgt_id,
            &vq.columns,
            vq.needs_pgt_count,
            vq.needs_dual_count,
            vq.needs_union_dedup,
            vq.topk_info.as_ref(),
            &vq.avg_aux_columns,
            &vq.sum2_aux_columns,
            &vq.covar_aux_columns,
            &vq.nonnull_aux_columns,
            &vq.statistical_aux_types,
        )?;
        let init_ms = t_init.elapsed().as_secs_f64() * 1000.0;

        // Record initial full materialization time so the adaptive
        // threshold auto-tuner has a FULL baseline from the very first
        // differential refresh.  Without this, `last_full_ms` stays NULL
        // and the auto-tuner never activates for STs whose change rate
        // stays below the fallback threshold.
        if refresh_mode == RefreshMode::Differential
            && let Err(e) = StreamTableMeta::update_adaptive_threshold(pgt_id, None, Some(init_ms))
        {
            pgrx::debug1!("[pg_trickle] Failed to record initial last_full_ms: {}", e);
        }

        // G12-ERM-1: Record the effective refresh mode for the initial
        // population so monitoring and tests can observe the mode from
        // the very first cycle without waiting for a scheduler refresh.
        let initial_eff_mode = if vq.topk_info.is_some() {
            "TOP_K"
        } else {
            refresh_mode.as_str()
        };
        if let Err(e) = StreamTableMeta::update_effective_refresh_mode(pgt_id, initial_eff_mode) {
            pgrx::debug1!(
                "[pg_trickle] Failed to set initial effective_refresh_mode for {}.{}: {}",
                schema,
                table_name,
                e
            );
        }
    }

    // Pre-warm delta SQL + MERGE template cache for DIFFERENTIAL mode,
    // so the first refresh avoids the cold-start parsing penalty.
    if refresh_mode == RefreshMode::Differential && initialize {
        let st = StreamTableMeta::get_by_name(&schema, &table_name)?;
        refresh::prewarm_merge_cache(&st);
    }

    // Signal scheduler to rebuild DAG
    shmem::signal_dag_invalidation(pgt_id);

    pgrx::info!(
        "Stream table {}.{} created (pgt_id={}, mode={}, initialized={})",
        schema,
        table_name,
        pgt_id,
        refresh_mode.as_str(),
        initialize
    );

    Ok(())
}

/// Alter properties of an existing stream table.
#[allow(clippy::too_many_arguments)]
#[pg_extern(schema = "pgtrickle")]
fn alter_stream_table(
    name: &str,
    query: default!(Option<&str>, "NULL"),
    schedule: default!(Option<&str>, "NULL"),
    refresh_mode: default!(Option<&str>, "NULL"),
    status: default!(Option<&str>, "NULL"),
    diamond_consistency: default!(Option<&str>, "NULL"),
    diamond_schedule_policy: default!(Option<&str>, "NULL"),
    cdc_mode: default!(Option<&str>, "NULL"),
    append_only: default!(Option<bool>, "NULL"),
    pooler_compatibility_mode: default!(Option<bool>, "NULL"),
    tier: default!(Option<&str>, "NULL"),
    fuse: default!(Option<&str>, "NULL"),
    fuse_ceiling: default!(Option<i64>, "NULL"),
    fuse_sensitivity: default!(Option<i32>, "NULL"),
    partition_by: default!(Option<&str>, "NULL"),
    max_differential_joins: default!(Option<i32>, "NULL"),
    max_delta_fraction: default!(Option<f64>, "NULL"),
    // VP-1/VP-2 (v0.47.0): post-refresh action and drift threshold
    post_refresh_action: default!(Option<&str>, "NULL"),
    reindex_drift_threshold: default!(Option<f64>, "NULL"),
    target_freshness: default!(Option<&str>, "NULL"),
) {
    let result = alter_stream_table_impl(AlterStreamTableOptions {
        name,
        query,
        schedule,
        refresh_mode,
        status,
        diamond_consistency,
        diamond_schedule_policy,
        cdc_mode,
        append_only,
        pooler_compatibility_mode,
        tier,
        fuse,
        fuse_ceiling,
        fuse_sensitivity,
        partition_by,
        max_differential_joins,
        max_delta_fraction,
        post_refresh_action,
        reindex_drift_threshold,
        target_freshness,
    });
    if let Err(e) = result {
        raise_error_with_context(e);
    }
}

// Q-2 (v0.79.0): Centralized options struct for alter_stream_table entry points.
/// All `alter_stream_table` entry points construct this struct first and then
/// call [`alter_stream_table_impl`].  This mirrors the `CreateStreamTableOptions`
/// pattern and eliminates `#[allow(clippy::too_many_arguments)]` from the
/// business-logic function.  The pg_extern wrapper retains individual parameters
/// as required by pgrx.
#[derive(Debug, Default)]
pub(crate) struct AlterStreamTableOptions<'a> {
    pub(crate) name: &'a str,
    pub(crate) query: Option<&'a str>,
    pub(crate) schedule: Option<&'a str>,
    pub(crate) refresh_mode: Option<&'a str>,
    pub(crate) status: Option<&'a str>,
    pub(crate) diamond_consistency: Option<&'a str>,
    pub(crate) diamond_schedule_policy: Option<&'a str>,
    pub(crate) cdc_mode: Option<&'a str>,
    pub(crate) append_only: Option<bool>,
    pub(crate) pooler_compatibility_mode: Option<bool>,
    pub(crate) tier: Option<&'a str>,
    pub(crate) fuse: Option<&'a str>,
    pub(crate) fuse_ceiling: Option<i64>,
    pub(crate) fuse_sensitivity: Option<i32>,
    pub(crate) partition_by: Option<&'a str>,
    pub(crate) max_differential_joins: Option<i32>,
    pub(crate) max_delta_fraction: Option<f64>,
    /// VP-1/VP-2 (v0.47.0): post-refresh action.
    pub(crate) post_refresh_action: Option<&'a str>,
    /// VP-2 (v0.47.0): reindex drift threshold.
    pub(crate) reindex_drift_threshold: Option<f64>,
    /// v0.86.0: declared freshness target.
    pub(crate) target_freshness: Option<&'a str>,
}

pub(crate) fn alter_stream_table_impl(
    opts: AlterStreamTableOptions<'_>,
) -> Result<(), PgTrickleError> {
    let AlterStreamTableOptions {
        name,
        query,
        schedule,
        refresh_mode,
        status,
        diamond_consistency,
        diamond_schedule_policy,
        cdc_mode,
        append_only,
        pooler_compatibility_mode,
        tier,
        fuse,
        fuse_ceiling: fuse_ceiling_arg,
        fuse_sensitivity: fuse_sensitivity_arg,
        partition_by,
        max_differential_joins,
        max_delta_fraction,
        post_refresh_action,
        reindex_drift_threshold,
        target_freshness,
    } = opts;
    if let Some(value) = max_differential_joins {
        validation::nonnegative_i32("max_differential_joins", i64::from(value))?;
    }
    if let Some(value) = max_delta_fraction {
        validation::finite_fraction("max_delta_fraction", value)?;
    }
    if let Some(value) = fuse_ceiling_arg
        && value <= 0
    {
        return Err(PgTrickleError::InvalidArgument(
            "fuse_ceiling must be a positive integer".to_string(),
        ));
    }
    if let Some(value) = fuse_sensitivity_arg
        && value <= 0
    {
        return Err(PgTrickleError::InvalidArgument(
            "fuse_sensitivity must be a positive integer".to_string(),
        ));
    }
    if let Some(value) = reindex_drift_threshold
        && (!value.is_finite() || !(0.0..=1.0).contains(&value) || value == 0.0)
    {
        return Err(PgTrickleError::InvalidArgument(
            "reindex_drift_threshold must be finite and between 0.0 and 1.0".to_string(),
        ));
    }
    let (schema, table_name) = parse_qualified_name(name)?;
    let mut st = StreamTableMeta::get_by_name(&schema, &table_name)?;
    let qualified_name = format!("{schema}.{table_name}");

    // SEC-1: Ownership check — only the owner (or superuser) can alter.
    check_stream_table_ownership(st.pgt_relid, &schema, &table_name)?;

    if let Some(raw_target) = target_freshness {
        if schedule.is_some() {
            return Err(PgTrickleError::InvalidArgument(
                "schedule and target_freshness cannot be changed together (HINT: choose one control)".into(),
            ));
        }
        let target = parse_target_freshness(Some(raw_target))?
            .ok_or_else(|| PgTrickleError::InvalidArgument("target_freshness is missing".into()))?;
        if target.mode == TargetFreshnessMode::OnCommit && refresh_mode.is_some() {
            return Err(PgTrickleError::InvalidArgument(
                "on_commit target_freshness cannot be changed together with refresh_mode (HINT: choose IMMEDIATE or target_freshness => 'on_commit')".into(),
            ));
        }
        if target.mode == TargetFreshnessMode::OnCommit {
            crate::dvm::validate_immediate_mode_support(&st.defining_query)?;
            Spi::run_with_args(
                "UPDATE pgtrickle.pgt_stream_tables SET refresh_mode = 'IMMEDIATE', requested_refresh_mode = 'IMMEDIATE', updated_at = now() WHERE pgt_id = $1",
                &[st.pgt_id.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        }
        apply_target_freshness(st.pgt_id, target)?;
        st = StreamTableMeta::get_by_name(&schema, &table_name)?;
    }

    // ── Query migration (must run first, before other parameter changes) ──
    if let Some(new_query) = query {
        alter_stream_table_query(&st, &schema, &table_name, new_query)?;
        st = StreamTableMeta::get_by_name(&schema, &table_name)?;
    }

    // ── A1-1c: Partition key migration ──────────────────────────────────
    // partition_by => '' (empty string) removes partitioning.
    // partition_by => 'col' or 'LIST:col' adds/changes partitioning.
    // This requires storage table recreation + full refresh.
    if let Some(new_pk_raw) = partition_by {
        let new_pk = if new_pk_raw.trim().is_empty() {
            None
        } else {
            Some(new_pk_raw)
        };

        // Only act when the partition key is actually changing.
        let old_pk = st.st_partition_key.as_deref();
        if new_pk != old_pk {
            alter_stream_table_partition_key(&st, &schema, &table_name, new_pk)?;
            st = StreamTableMeta::get_by_name(&schema, &table_name)?;
        }
    }

    let (requested_cdc_mode_override, effective_requested_cdc_mode, cdc_mode_source) =
        resolve_requested_cdc_mode_for_st(&st, cdc_mode)?;
    let target_refresh_mode = match refresh_mode {
        Some(mode_str) => RefreshMode::from_str(mode_str)?,
        None => st.refresh_mode,
    };

    enforce_cdc_refresh_mode_interaction(
        &qualified_name,
        target_refresh_mode,
        &effective_requested_cdc_mode,
        cdc_mode_source,
    )?;
    if !target_refresh_mode.is_immediate() {
        validate_requested_cdc_mode_requirements(&effective_requested_cdc_mode)?;
    }

    // Validate the complete incremental admission before changing CDC mode,
    // schedule, catalog state, or trigger infrastructure.
    if target_refresh_mode != RefreshMode::Full {
        super::validate_incremental_mode_for_query(&st.defining_query, target_refresh_mode)?;
    }

    if requested_cdc_mode_override != st.requested_cdc_mode {
        StreamTableMeta::update_requested_cdc_mode(
            st.pgt_id,
            requested_cdc_mode_override.as_deref(),
        )?;
        st.requested_cdc_mode = requested_cdc_mode_override.clone();
    }

    if let Some(val) = schedule {
        if val.trim().eq_ignore_ascii_case("calculated") {
            // Switch to CALCULATED mode (NULL schedule in catalog)
            Spi::run_with_args(
                "UPDATE pgtrickle.pgt_stream_tables SET schedule = NULL, updated_at = now() WHERE pgt_id = $1",
                &[st.pgt_id.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        } else {
            let _schedule = parse_schedule(val)?;
            let trimmed = val.trim();
            Spi::run_with_args(
                "UPDATE pgtrickle.pgt_stream_tables SET schedule = $1, updated_at = now() WHERE pgt_id = $2",
                &[trimmed.into(), st.pgt_id.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        }
    }

    if let Some(mode_str) = refresh_mode {
        let new_mode = target_refresh_mode;
        let old_mode = st.refresh_mode;

        if new_mode != old_mode {
            // ── Validate mode switch ────────────────────────────────
            // TopK tables: check limit threshold for IMMEDIATE mode.
            if let (true, Some(topk_limit)) = (new_mode.is_immediate(), st.topk_limit) {
                let topk_limit = topk_limit as i64;
                let max_limit = crate::config::PGS_IVM_TOPK_MAX_LIMIT.get() as i64;
                if max_limit == 0 || topk_limit > max_limit {
                    return Err(PgTrickleError::UnsupportedOperator(format!(
                        "Cannot switch TopK stream table (LIMIT {topk_limit}) to IMMEDIATE mode. \
                         Exceeds pg_trickle.ivm_topk_max_limit = {max_limit}. Raise the threshold \
                         or keep using DIFFERENTIAL/FULL mode."
                    )));
                }
            }

            // Validate query restrictions for IMMEDIATE mode.
            if new_mode.is_immediate() {
                crate::dvm::validate_immediate_mode_support(&st.defining_query)?;
            }

            // Get dependencies for trigger migration.
            let deps = StDependency::get_for_st(st.pgt_id).unwrap_or_default();
            let change_schema = config::pg_trickle_change_buffer_schema();

            // ── Tear down OLD mode's infrastructure ─────────────────
            match old_mode {
                RefreshMode::Immediate => {
                    // Drop IVM triggers from source tables.
                    for dep in &deps {
                        if dep.source_type == "TABLE"
                            && let Err(e) =
                                crate::ivm::cleanup_ivm_triggers(dep.source_relid, st.pgt_id)
                        {
                            pgrx::warning!(
                                "Failed to clean up IVM triggers for oid {}: {}",
                                dep.source_relid.to_u32(),
                                e
                            );
                        }
                    }
                }
                RefreshMode::Full | RefreshMode::Differential => {
                    // Drop CDC triggers + change buffer tables from source
                    // tables (only if switching TO IMMEDIATE; FULL↔DIFF
                    // keeps CDC infrastructure).
                    if new_mode.is_immediate() {
                        for dep in &deps {
                            if dep.source_type == "TABLE"
                                && let Err(e) = cleanup_cdc_for_source(
                                    dep.source_relid,
                                    dep.cdc_mode,
                                    Some(st.pgt_id),
                                )
                            {
                                pgrx::warning!(
                                    "Failed to clean up CDC for oid {}: {}",
                                    dep.source_relid.to_u32(),
                                    e
                                );
                            }
                        }
                    }
                }
            }

            // ── Set up NEW mode's infrastructure ────────────────────
            match new_mode {
                RefreshMode::Immediate => {
                    // Install IVM triggers on source tables.
                    let lock_mode = crate::ivm::IvmLockMode::for_query(&st.defining_query);
                    for dep in &deps {
                        if dep.source_type == "TABLE" {
                            crate::ivm::setup_ivm_triggers(
                                dep.source_relid,
                                st.pgt_id,
                                st.pgt_relid,
                                lock_mode,
                            )?;
                        }
                    }
                    // Clear schedule for IMMEDIATE mode.
                    Spi::run_with_args(
                        "UPDATE pgtrickle.pgt_stream_tables SET schedule = NULL WHERE pgt_id = $1",
                        &[st.pgt_id.into()],
                    )
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
                }
                RefreshMode::Full | RefreshMode::Differential => {
                    // If switching FROM IMMEDIATE, recreate CDC triggers.
                    if old_mode.is_immediate() {
                        for dep in &deps {
                            if dep.source_type == "TABLE" {
                                setup_cdc_for_source(dep.source_relid, st.pgt_id, &change_schema)?;
                            }
                        }
                        // Restore a default schedule if none is set.
                        if schedule.is_none() {
                            Spi::run_with_args(
                                "UPDATE pgtrickle.pgt_stream_tables \
                                 SET schedule = COALESCE(schedule, '1m') \
                                 WHERE pgt_id = $1",
                                &[st.pgt_id.into()],
                            )
                            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
                        }
                    }
                }
            }

            // ── Update catalog ──────────────────────────────────────
            Spi::run_with_args(
                "UPDATE pgtrickle.pgt_stream_tables \
                 SET refresh_mode = $1, requested_refresh_mode = $1, updated_at = now() WHERE pgt_id = $2",
                &[mode_str.to_uppercase().into(), st.pgt_id.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

            // ── Full refresh to ensure consistency ──────────────────
            let source_oids: Vec<pg_sys::Oid> = deps
                .iter()
                .filter(|d| d.source_type == "TABLE")
                .map(|d| d.source_relid)
                .collect();
            // Re-load ST with updated mode for the refresh dispatch.
            let updated_st = StreamTableMeta::get_by_name(&schema, &table_name)?;
            execute_manual_full_refresh(&updated_st, &schema, &table_name, &source_oids)?;

            // ERG-F: warn so the client sees the implicit full refresh regardless of log_min_messages.
            pgrx::warning!(
                "pg_trickle: stream table {}.{} refresh mode changed from {} to {}; \
                 a full refresh was applied. This may take time on large tables.",
                schema,
                table_name,
                old_mode.as_str(),
                new_mode.as_str(),
            );
        } else {
            // Same mode — just update catalog (no-op but harmless).
            Spi::run_with_args(
                "UPDATE pgtrickle.pgt_stream_tables \
                 SET refresh_mode = $1, requested_refresh_mode = $1, updated_at = now() WHERE pgt_id = $2",
                &[mode_str.to_uppercase().into(), st.pgt_id.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

            // Normalize legacy set-operation storage even when the caller
            // explicitly reasserts FULL without requesting a refresh.
            if new_mode == RefreshMode::Full
                && crate::dvm::query_needs_dual_count(&st.defining_query)
            {
                crate::api::helpers::normalize_full_set_operation_storage(
                    &schema,
                    &table_name,
                    st.pgt_relid,
                    st.pgt_id,
                )?;
            }
        }
    }

    if cdc_mode.is_some() && !target_refresh_mode.is_immediate() {
        let deps = StDependency::get_for_st(st.pgt_id).unwrap_or_default();
        let change_schema = config::pg_trickle_change_buffer_schema();
        for dep in &deps {
            if dep.source_type == "TABLE" {
                setup_cdc_for_source(dep.source_relid, st.pgt_id, &change_schema)?;
            }
        }
        pgrx::info!(
            "Stream table {}.{} updated requested cdc_mode to {}",
            schema,
            table_name,
            effective_requested_cdc_mode,
        );
    }

    if let Some(status_str) = status {
        let new_status = StStatus::from_str(&status_str.to_uppercase())?;
        StreamTableMeta::update_status(st.pgt_id, new_status)?;
        if new_status == StStatus::Active {
            // Reset errors when resuming
            Spi::run_with_args(
                "UPDATE pgtrickle.pgt_stream_tables SET consecutive_errors = 0, updated_at = now() WHERE pgt_id = $1",
                &[st.pgt_id.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        }
    }

    if let Some(dc_str) = diamond_consistency {
        let val = dc_str.to_lowercase();
        match val.as_str() {
            "none" | "atomic" => {
                let dc = DiamondConsistency::from_sql_str(&val);
                // G15-PV: Validate combined diamond params before persisting.
                let effective_dsp = diamond_schedule_policy
                    .and_then(DiamondSchedulePolicy::from_sql_str)
                    .unwrap_or(st.diamond_schedule_policy);
                if effective_dsp == DiamondSchedulePolicy::Slowest && dc == DiamondConsistency::None
                {
                    return Err(PgTrickleError::InvalidArgument(
                        "diamond_schedule_policy = 'slowest' requires diamond_consistency = 'atomic'. \
                         The 'slowest' policy is only meaningful when atomic cross-branch reads are \
                         enabled. Set diamond_consistency = 'atomic' or use diamond_schedule_policy = 'fastest'."
                            .to_string(),
                    ));
                }
                StreamTableMeta::set_diamond_consistency(st.pgt_id, dc)?;
            }
            other => {
                return Err(PgTrickleError::InvalidArgument(format!(
                    "invalid diamond_consistency value: '{}' (expected 'none' or 'atomic')",
                    other
                )));
            }
        }
    }

    if let Some(dsp_str) = diamond_schedule_policy {
        match DiamondSchedulePolicy::from_sql_str(dsp_str) {
            Some(p) => {
                // G15-PV: Validate combined diamond params.  Only check when
                // dc is not also being changed (handled in the dc block above).
                if diamond_consistency.is_none() {
                    let effective_dc = st.diamond_consistency;
                    if p == DiamondSchedulePolicy::Slowest
                        && effective_dc == DiamondConsistency::None
                    {
                        return Err(PgTrickleError::InvalidArgument(
                            "diamond_schedule_policy = 'slowest' requires diamond_consistency = 'atomic'. \
                             The 'slowest' policy is only meaningful when atomic cross-branch reads are \
                             enabled. Set diamond_consistency = 'atomic' or use diamond_schedule_policy = 'fastest'."
                                .to_string(),
                        ));
                    }
                }
                StreamTableMeta::set_diamond_schedule_policy(st.pgt_id, p)?;
            }
            None => {
                return Err(PgTrickleError::InvalidArgument(format!(
                    "invalid diamond_schedule_policy value: '{}' (expected 'fastest' or 'slowest')",
                    dsp_str
                )));
            }
        }
    }

    if let Some(ao) = append_only {
        let effective_mode = match refresh_mode {
            Some(mode_str) => RefreshMode::from_str(mode_str)?,
            None => st.refresh_mode,
        };
        if ao {
            if effective_mode == RefreshMode::Full {
                return Err(PgTrickleError::InvalidArgument(
                    "append_only is not supported with FULL refresh mode.".to_string(),
                ));
            }
            if effective_mode.is_immediate() {
                return Err(PgTrickleError::InvalidArgument(
                    "append_only is not supported with IMMEDIATE refresh mode.".to_string(),
                ));
            }
            if st.has_keyless_source {
                return Err(PgTrickleError::InvalidArgument(
                    "append_only is not supported for stream tables with keyless sources."
                        .to_string(),
                ));
            }
        }
        StreamTableMeta::update_append_only(st.pgt_id, ao)?;
    }

    // PB2: Update pooler compatibility mode if explicitly set.
    if let Some(pcm) = pooler_compatibility_mode {
        StreamTableMeta::update_pooler_compatibility_mode(st.pgt_id, pcm)?;
        if pcm {
            // Deallocate any existing prepared MERGE statement for this ST,
            // since it will no longer be used.
            crate::refresh::invalidate_merge_cache(st.pgt_id);
        }
    }

    // G-7: Update refresh tier if explicitly set.
    if let Some(tier_str) = tier {
        use crate::scheduler::RefreshTier;
        if !RefreshTier::is_valid_str(tier_str) {
            return Err(PgTrickleError::InvalidArgument(format!(
                "invalid tier value: '{}' (expected 'hot', 'warm', 'cold', or 'frozen')",
                tier_str
            )));
        }
        let normalized = tier_str.to_lowercase();

        // C-1b: Emit NOTICE when demoting from Hot to Cold or Frozen so
        // operators are aware their configured interval will be multiplied.
        let old_tier = RefreshTier::from_sql_str(&st.refresh_tier);
        let new_tier = RefreshTier::from_sql_str(&normalized);
        if old_tier == RefreshTier::Hot
            && matches!(new_tier, RefreshTier::Cold | RefreshTier::Frozen)
        {
            let msg = match new_tier {
                RefreshTier::Cold => format!(
                    "stream table {}.{} demoted from hot to cold — effective refresh interval is now 10× the configured schedule",
                    st.pgt_schema, st.pgt_name
                ),
                RefreshTier::Frozen => format!(
                    "stream table {}.{} demoted from hot to frozen — refresh is suspended until the tier is changed back",
                    st.pgt_schema, st.pgt_name
                ),
                _ => unreachable!(),
            };
            pgrx::notice!("{}", msg);
        }

        StreamTableMeta::update_refresh_tier(st.pgt_id, &normalized)?;
    }

    // FUSE-2: Update fuse configuration if any fuse parameter is set.
    if fuse.is_some() || fuse_ceiling_arg.is_some() || fuse_sensitivity_arg.is_some() {
        let fuse_mode = match fuse {
            Some(mode_str) => {
                let normalized = mode_str.to_lowercase();
                match normalized.as_str() {
                    "off" | "on" | "auto" => normalized,
                    _ => {
                        return Err(PgTrickleError::InvalidArgument(format!(
                            "invalid fuse value: '{}' (expected 'off', 'on', or 'auto')",
                            mode_str
                        )));
                    }
                }
            }
            None => st.fuse_mode.clone(),
        };
        let ceiling = fuse_ceiling_arg.or(st.fuse_ceiling);
        let sensitivity = fuse_sensitivity_arg.or(st.fuse_sensitivity);

        if let Some(c) = ceiling
            && c <= 0
        {
            return Err(PgTrickleError::InvalidArgument(
                "fuse_ceiling must be a positive integer".into(),
            ));
        }
        if let Some(s) = sensitivity
            && s <= 0
        {
            return Err(PgTrickleError::InvalidArgument(
                "fuse_sensitivity must be a positive integer".into(),
            ));
        }

        StreamTableMeta::update_fuse_config(st.pgt_id, &fuse_mode, ceiling, sensitivity)?;
    }

    // DI-7: Update max_differential_joins if explicitly set.
    if let Some(mdj) = max_differential_joins {
        if mdj < 0 {
            return Err(PgTrickleError::InvalidArgument(
                "max_differential_joins must be a non-negative integer (0 disables the limit)"
                    .into(),
            ));
        }
        let val: Option<i32> = if mdj == 0 { None } else { Some(mdj) };
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET max_differential_joins = $1, updated_at = now() WHERE pgt_id = $2",
            &[val.into(), st.pgt_id.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    }

    // DI-7: Update max_delta_fraction if explicitly set.
    if let Some(mdf) = max_delta_fraction {
        validation::finite_fraction("max_delta_fraction", mdf)?;
        let val: Option<f64> = if mdf == 0.0 { None } else { Some(mdf) };
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET max_delta_fraction = $1, updated_at = now() WHERE pgt_id = $2",
            &[val.into(), st.pgt_id.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    }

    // VP-1/VP-2 (v0.47.0): Update post-refresh action and drift threshold if supplied.
    if let Some(pra) = post_refresh_action {
        let pra_lower = pra.to_lowercase();
        match pra_lower.as_str() {
            "none" | "analyze" | "reindex" | "reindex_if_drift" => {}
            other => {
                return Err(PgTrickleError::InvalidArgument(format!(
                    "invalid post_refresh_action '{}': expected 'none', 'analyze', \
                     'reindex', or 'reindex_if_drift'",
                    other
                )));
            }
        }
        StreamTableMeta::update_post_refresh_options(
            st.pgt_id,
            &pra_lower,
            reindex_drift_threshold,
        )?;
    } else if let Some(_threshold) = reindex_drift_threshold {
        // Drift threshold can be updated independently.
        StreamTableMeta::update_post_refresh_options(
            st.pgt_id,
            &st.post_refresh_action,
            reindex_drift_threshold,
        )?;
    }

    shmem::signal_dag_invalidation(st.pgt_id);
    // G14-SHC: Remove from catalog-backed template cache.
    template_cache::invalidate(st.pgt_id);
    // G8.1: Notify other backends to flush delta/MERGE template caches.
    shmem::bump_cache_generation();

    // ERR-1c: Clear error state when a pipeline-regenerating alter succeeds.
    // This lets ALTER STREAM TABLE with a fixed query reset an ERROR table.
    if st.status == StStatus::Error {
        let _ = StreamTableMeta::clear_error_state(st.pgt_id);
        let _ = StreamTableMeta::update_status(st.pgt_id, StStatus::Active);
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables SET consecutive_errors = 0, updated_at = now() WHERE pgt_id = $1",
            &[st.pgt_id.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    }

    Ok(())
}

/// Drop a stream table, removing the storage table and all catalog entries.
///
/// When `cascade` is `true` any downstream stream tables that depend on this
/// one are automatically dropped first.  When `cascade` is `false` (the
/// default) the function raises an error if any dependents exist, matching
/// the behaviour of PostgreSQL's own `DROP TABLE … RESTRICT`.
///
/// Changed in v0.19.0 (UX-6): default flipped from `true` to `false` to
/// prevent accidental cascading drops.
#[pg_extern(schema = "pgtrickle")]
fn drop_stream_table(name: &str, cascade: default!(bool, false)) {
    let result = drop_stream_table_impl(name, cascade);
    if let Err(e) = result {
        raise_error_with_context(e);
    }
}

pub(crate) fn prevalidate_stream_table_target(
    name: &str,
) -> Result<StreamTableMeta, PgTrickleError> {
    let (schema, table_name) = parse_qualified_name(name)?;
    let st = StreamTableMeta::get_by_name(&schema, &table_name)?;
    check_stream_table_ownership(st.pgt_relid, &schema, &table_name)?;
    Ok(st)
}

fn order_bulk_drop_target_ids(
    target_ids: &[i64],
    downstream_by_id: &std::collections::HashMap<i64, Vec<i64>>,
) -> Result<Vec<i64>, PgTrickleError> {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum VisitState {
        Visiting,
        Visited,
    }

    fn visit(
        pgt_id: i64,
        downstream_by_id: &std::collections::HashMap<i64, Vec<i64>>,
        states: &mut std::collections::HashMap<i64, VisitState>,
        ordered: &mut Vec<i64>,
    ) -> Result<(), PgTrickleError> {
        match states.get(&pgt_id) {
            Some(VisitState::Visited) => return Ok(()),
            Some(VisitState::Visiting) => {
                return Err(PgTrickleError::InternalError(format!(
                    "cycle detected while ordering bulk drop targets for pgt_id={pgt_id}"
                )));
            }
            None => {}
        }

        states.insert(pgt_id, VisitState::Visiting);
        for downstream_id in downstream_by_id.get(&pgt_id).into_iter().flatten() {
            visit(*downstream_id, downstream_by_id, states, ordered)?;
        }
        states.insert(pgt_id, VisitState::Visited);
        ordered.push(pgt_id);
        Ok(())
    }

    let mut states = std::collections::HashMap::with_capacity(target_ids.len());
    let mut ordered = Vec::with_capacity(target_ids.len());
    for pgt_id in target_ids {
        visit(*pgt_id, downstream_by_id, &mut states, &mut ordered)?;
    }
    Ok(ordered)
}

pub(crate) fn plan_drop_stream_tables(names: &[String]) -> Result<Vec<String>, PgTrickleError> {
    let mut targets = Vec::with_capacity(names.len());
    let mut requested_ids = HashSet::with_capacity(names.len());
    let mut names_by_id = std::collections::HashMap::with_capacity(names.len());

    for name in names {
        let st = prevalidate_stream_table_target(name)?;
        let qualified_name = format!("{}.{}", st.pgt_schema, st.pgt_name);
        requested_ids.insert(st.pgt_id);
        names_by_id.insert(st.pgt_id, qualified_name.clone());
        targets.push((st.pgt_id, st.pgt_relid, qualified_name));
    }

    let mut downstream_by_id = std::collections::HashMap::with_capacity(targets.len());
    for (pgt_id, relid, qualified_name) in &targets {
        let downstream_ids = StDependency::get_downstream_pgt_ids(*relid)?;
        let mut requested_downstream = Vec::new();
        let mut external_dependents = Vec::new();

        for downstream_id in downstream_ids {
            if requested_ids.contains(&downstream_id) {
                requested_downstream.push(downstream_id);
            } else if let Some(downstream_st) = StreamTableMeta::get_by_id(downstream_id)? {
                external_dependents.push(format!(
                    "{}.{}",
                    downstream_st.pgt_schema, downstream_st.pgt_name
                ));
            }
        }

        if !external_dependents.is_empty() {
            return Err(PgTrickleError::InvalidArgument(format!(
                "stream table {qualified_name} has dependent stream tables not included in this bulk drop: {}. \
                 Include them in the names array or call pgtrickle.drop_stream_table('{qualified_name}', cascade => true).",
                external_dependents.join(", ")
            )));
        }

        downstream_by_id.insert(*pgt_id, requested_downstream);
    }

    let ordered_ids = order_bulk_drop_target_ids(
        &targets
            .iter()
            .map(|(pgt_id, _, _)| *pgt_id)
            .collect::<Vec<_>>(),
        &downstream_by_id,
    )?;

    ordered_ids
        .into_iter()
        .map(|pgt_id| {
            names_by_id.get(&pgt_id).cloned().ok_or_else(|| {
                PgTrickleError::InternalError(format!(
                    "missing bulk drop target metadata for pgt_id={pgt_id}"
                ))
            })
        })
        .collect()
}

pub(crate) fn drop_stream_table_impl(name: &str, cascade: bool) -> Result<(), PgTrickleError> {
    let mut visited_pgt_ids = HashSet::new();
    drop_stream_table_impl_inner(name, cascade, &mut visited_pgt_ids)
}

fn drop_stream_table_impl_inner(
    name: &str,
    cascade: bool,
    visited_pgt_ids: &mut HashSet<i64>,
) -> Result<(), PgTrickleError> {
    let (schema, table_name) = parse_qualified_name(name)?;
    let st = StreamTableMeta::get_by_name(&schema, &table_name)?;

    if !visited_pgt_ids.insert(st.pgt_id) {
        return Ok(());
    }

    // SEC-1: Ownership check — only the owner (or superuser) can drop.
    // Only enforce on the top-level call (visited_pgt_ids has exactly 1 entry);
    // cascaded drops inherit the permission from the top-level check.
    if visited_pgt_ids.len() == 1 {
        check_stream_table_ownership(st.pgt_relid, &schema, &table_name)?;
    }

    // CASCADE: drop all stream tables that depend on this one first.
    // `get_downstream_pgt_ids` finds STs whose defining queries read from
    // this ST's storage table.  We iterate by pgt_id to avoid re-querying
    // after each recursive drop changes the catalog.
    let downstream_ids = StDependency::get_downstream_pgt_ids(st.pgt_relid)?;
    if !downstream_ids.is_empty() && !cascade {
        let names: Vec<String> = downstream_ids
            .iter()
            .filter_map(|id| StreamTableMeta::get_by_id(*id).ok().flatten())
            .map(|s| format!("{}.{}", s.pgt_schema, s.pgt_name))
            .collect();
        return Err(PgTrickleError::InvalidArgument(format!(
            "stream table {}.{} has dependent stream tables: {}. Use cascade => true to drop them automatically.",
            schema,
            table_name,
            names.join(", ")
        )));
    }
    for downstream_id in downstream_ids {
        if let Some(downstream_st) = StreamTableMeta::get_by_id(downstream_id)? {
            let qualified = format!("{}.{}", downstream_st.pgt_schema, downstream_st.pgt_name);
            drop_stream_table_impl_inner(&qualified, cascade, visited_pgt_ids)?;
        }
    }

    // Get dependencies before deleting catalog entries
    let deps = StDependency::get_for_st(st.pgt_id).unwrap_or_default();

    // Flush any deferred change-buffer cleanup entries that reference
    // source OIDs about to be cleaned up.  This prevents
    // `drain_pending_cleanups` on the next refresh from attempting to
    // access change-buffer tables that no longer exist.
    let dep_oids: Vec<u32> = deps
        .iter()
        .filter(|d| d.source_type == "TABLE")
        .map(|d| d.source_relid.to_u32())
        .collect();
    crate::refresh::flush_pending_cleanups_for_oids(&dep_oids);

    // CDC-PUB-2: Drop downstream publication if one exists.
    if let Some(pub_name) = &st.downstream_publication_name {
        let _ = Spi::run(&format!(
            "DROP PUBLICATION IF EXISTS {}",
            quote_identifier(pub_name),
        ));
    }

    // Drop the storage table
    let drop_sql = format!(
        "DROP TABLE IF EXISTS {}.{} CASCADE",
        quote_identifier(&schema),
        quote_identifier(&table_name),
    );
    Spi::run(&drop_sql)
        .map_err(|e| PgTrickleError::SpiError(format!("Failed to drop storage table: {}", e)))?;

    // Delete catalog entries (cascade handles pgt_dependencies)
    StreamTableMeta::delete(st.pgt_id)?;

    // Remove this ST's pgt_id from the tracked_by_pgt_ids arrays in
    // pgt_change_tracking so consumer counts stay accurate after drop.
    for dep in &deps {
        if dep.source_type == "TABLE" {
            let _ = Spi::run_with_args(
                "UPDATE pgtrickle.pgt_change_tracking \
                 SET tracked_by_pgt_ids = array_remove(tracked_by_pgt_ids, $1) \
                 WHERE source_relid = $2",
                &[st.pgt_id.into(), dep.source_relid.into()],
            );
        }
    }

    // Clean up CDC resources (triggers, WAL slots, publications) for
    // sources no longer tracked by any ST. For IMMEDIATE-mode STs, clean
    // up IVM triggers instead.
    for dep in &deps {
        if dep.source_type == "TABLE" {
            if st.refresh_mode.is_immediate() {
                if let Err(e) = crate::ivm::cleanup_ivm_triggers(dep.source_relid, st.pgt_id) {
                    pgrx::warning!(
                        "Failed to clean up IVM triggers for oid {}: {}",
                        dep.source_relid.to_u32(),
                        e
                    );
                }
            } else {
                cleanup_cdc_for_source(dep.source_relid, dep.cdc_mode, None)?;
            }
        } else if dep.source_type == "STREAM_TABLE" {
            // ST-ST-1: If this was the last downstream consumer of an
            // upstream ST's change buffer, drop the buffer.
            let upstream_pgt_id =
                crate::catalog::StreamTableMeta::pgt_id_for_relid(dep.source_relid);
            if let Some(up_id) = upstream_pgt_id {
                let consumers = cdc::count_downstream_st_consumers(up_id);
                if consumers == 0 {
                    let change_schema = config::pg_trickle_change_buffer_schema();
                    if let Err(e) = cdc::drop_st_change_buffer_table(up_id, &change_schema) {
                        pgrx::warning!(
                            "Failed to drop ST change buffer for upstream pgt_id {}: {}",
                            up_id,
                            e
                        );
                    }
                }
            }
        }
    }

    // ST-ST-1: Drop this ST's own change buffer (if it had downstream consumers).
    {
        let change_schema = config::pg_trickle_change_buffer_schema();
        if cdc::has_st_change_buffer(st.pgt_id, &change_schema)
            && let Err(e) = cdc::drop_st_change_buffer_table(st.pgt_id, &change_schema)
        {
            pgrx::warning!(
                "Failed to drop own ST change buffer for pgt_id {}: {}",
                st.pgt_id,
                e
            );
        }
    }

    // CYC-6: Recompute SCC assignments when a cycle member is dropped.
    // The dropped ST's catalog entry is already gone, so rebuild the DAG
    // from the remaining STs and reassign scc_id values. Former cycle
    // members that are no longer in a cycle will have their scc_id cleared.
    if st.scc_id.is_some()
        && let Err(e) = assign_scc_ids_from_dag()
    {
        pgrx::warning!("Failed to recompute SCCs after drop: {}", e);
    }

    // Signal scheduler
    shmem::signal_dag_invalidation(st.pgt_id);
    // G14-SHC: Remove from catalog-backed template cache.
    template_cache::invalidate(st.pgt_id);
    // G8.1: Notify other backends to flush delta/MERGE template caches.
    shmem::bump_cache_generation();

    pgrx::info!(
        "Stream table {}.{} dropped (pgt_id={})",
        schema,
        table_name,
        st.pgt_id
    );
    Ok(())
}

/// Resume a suspended stream table, clearing its consecutive error count and
/// re-enabling automated and manual refreshes.
#[pg_extern(schema = "pgtrickle")]
fn resume_stream_table(name: &str) {
    let result = resume_stream_table_impl(name);
    if let Err(e) = result {
        raise_error_with_context(e);
    }
}

fn resume_stream_table_impl(name: &str) -> Result<(), PgTrickleError> {
    let (schema, table_name) = parse_qualified_name(name)?;
    let st = StreamTableMeta::get_by_name(&schema, &table_name)?;

    if st.status != StStatus::Suspended && st.status != StStatus::Error {
        return Err(PgTrickleError::InvalidArgument(format!(
            "stream table {}.{} is not suspended or in error state (current status: {})",
            schema,
            table_name,
            st.status.as_str(),
        )));
    }

    let deps = StDependency::get_for_st(st.pgt_id)?;
    let source_oids: Vec<_> = deps
        .iter()
        .filter(|dep| matches!(dep.source_type.as_str(), "TABLE" | "FOREIGN_TABLE"))
        .map(|dep| dep.source_relid)
        .collect();
    cdc::lock_source_relations(&source_oids)?;
    // Restore capture before exposing the consumer as ACTIVE. Changes made
    // while the source was inactive are intentionally repaired by FULL refresh.
    for dep in &deps {
        if matches!(dep.source_type.as_str(), "TABLE" | "FOREIGN_TABLE") {
            cdc::refresh_capture_body_for_source(dep.source_relid, true)?;
        }
    }
    StreamTableMeta::mark_for_reinitialize(st.pgt_id)?;
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET status = 'ACTIVE', consecutive_errors = 0, \
         last_error_message = NULL, last_error_at = NULL, updated_at = now() \
         WHERE pgt_id = $1",
        &[st.pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    crate::monitor::alert_resumed(&schema, &table_name, st.pooler_compatibility_mode);

    pgrx::info!(
        "Stream table {}.{} resumed (pgt_id={})",
        schema,
        table_name,
        st.pgt_id
    );
    Ok(())
}

/// Repair a potentially broken stream table by reinitializing its storage,
/// rebuilding CDC infrastructure, and scheduling a full refresh.
///
/// A42-1: This function is the primary operational recovery tool for stream
/// tables that have been damaged by pg_dump/restore, storage corruption,
/// missing triggers, or inconsistent catalog state.
///
/// Steps performed (actions taken are summarized in the return text):
/// 1. Acquire a transaction-scoped advisory lock on the stream table.
/// 2. Verify the stream table exists in the catalog.
/// 3. Reinitialize the materialized storage table if it is missing.
/// 4. Reset CDC frontiers to force a full refresh on the next cycle.
/// 5. Rebuild any missing CDC triggers / change-buffer tables.
/// 6. Verify that all declared source dependencies still exist.
/// 7. Return a summary of all actions taken.
#[pg_extern(schema = "pgtrickle")]
fn repair_stream_table(name: &str) -> String {
    match repair_stream_table_impl(name) {
        Ok(summary) => summary,
        Err(e) => raise_error_with_context(e),
    }
}

fn repair_stream_table_impl(name: &str) -> Result<String, PgTrickleError> {
    let (schema, table_name) = parse_qualified_name(name)?;
    let st = StreamTableMeta::get_by_name(&schema, &table_name)?;

    // Step 1: Acquire a transaction-scoped advisory lock.
    let got_lock =
        Spi::get_one_with_args::<bool>("SELECT pg_try_advisory_xact_lock($1)", &[st.pgt_id.into()])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            .unwrap_or(false);

    if !got_lock {
        return Err(PgTrickleError::RefreshSkipped(format!(
            "{}.{} — another operation holds the advisory lock; retry later",
            schema, table_name,
        )));
    }

    let mut actions: Vec<String> = Vec::new();
    let change_schema = config::pg_trickle_change_buffer_schema();
    let deps = StDependency::get_for_st(st.pgt_id).unwrap_or_default();

    // Step 2: catalog verification is implicit — get_by_name() already
    // returned st above; if missing, it would have errored.

    // Step 3: Reinitialize materialized storage if missing.
    let storage_exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_class c \
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relname = $2)",
        &[schema.as_str().into(), table_name.as_str().into()],
    )
    .unwrap_or(None)
    .unwrap_or(false);

    if !storage_exists {
        // Rebuild the storage table from the catalog definition.
        // mark_for_reinitialize triggers full storage rebuild on next refresh.
        StreamTableMeta::mark_for_reinitialize(st.pgt_id)?;
        actions.push("storage_missing: marked for reinitialize".to_string());
    }

    // Step 4: Reset CDC frontiers (set frontier = NULL, needs_reinit = true)
    // so the next scheduled or manual refresh performs a full refresh.
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET frontier = NULL, needs_reinit = true, \
             is_populated = false, updated_at = now() \
         WHERE pgt_id = $1",
        &[st.pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(format!("Failed to reset frontier: {}", e)))?;
    actions.push("frontier reset: scheduled full refresh on next cycle".to_string());

    // Step 5: Rebuild missing CDC triggers / change-buffer tables.
    let mut cdc_rebuilt = false;
    for dep in &deps {
        if dep.source_type != "TABLE" {
            continue;
        }
        let source_oid = dep.source_relid;

        // Verify source still exists.
        let source_exists = Spi::get_one_with_args::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_class WHERE oid = $1)",
            &[source_oid.into()],
        )
        .unwrap_or(None)
        .unwrap_or(false);

        if !source_exists {
            actions.push(format!(
                "dependency missing: source OID {} no longer exists",
                source_oid.to_u32()
            ));
            continue;
        }

        // Rebuild change-buffer table if absent.
        let buf_name = cdc::buffer_base_name_for_oid(source_oid);
        let buf_exists = Spi::get_one_with_args::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2)",
            &[change_schema.as_str().into(), buf_name.as_str().into()],
        )
        .unwrap_or(None)
        .unwrap_or(false);

        if !buf_exists {
            let col_defs = cdc::resolve_referenced_column_defs(source_oid).unwrap_or_default();
            let stable_name = cdc::get_cdc_name_for_source(source_oid);
            if let Err(e) =
                cdc::create_change_buffer_table(source_oid, &change_schema, &col_defs, &stable_name)
            {
                actions.push(format!(
                    "change_buffer rebuild failed for OID {}: {}",
                    source_oid.to_u32(),
                    e
                ));
            } else {
                actions.push(format!(
                    "change_buffer rebuilt for OID {}",
                    source_oid.to_u32()
                ));
                cdc_rebuilt = true;
            }
        }

        // Rebuild CDC trigger if absent.
        if !cdc::trigger_exists(source_oid).unwrap_or(false) {
            let pk_columns = cdc::resolve_pk_columns(source_oid).unwrap_or_default();
            let col_defs = cdc::resolve_referenced_column_defs(source_oid).unwrap_or_default();
            let stable_name = cdc::get_cdc_name_for_source(source_oid);
            if let Err(e) = cdc::create_change_trigger(
                source_oid,
                &change_schema,
                &pk_columns,
                &col_defs,
                &stable_name,
            ) {
                actions.push(format!(
                    "trigger rebuild failed for OID {}: {}",
                    source_oid.to_u32(),
                    e
                ));
            } else {
                actions.push(format!("trigger rebuilt for OID {}", source_oid.to_u32()));
                cdc_rebuilt = true;
            }
        }
    }

    if !cdc_rebuilt && deps.iter().any(|d| d.source_type == "TABLE") {
        actions.push("cdc_infrastructure: verified OK".to_string());
    }

    // Step 6: Verify all dependencies still exist — already done in step 5
    // (any missing source OIDs were recorded above).

    // Reset consecutive errors so the ST is eligible for automatic refresh.
    StreamTableMeta::reset_fuse(st.pgt_id).unwrap_or(());
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET status = 'ACTIVE', consecutive_errors = 0, \
             last_error_message = NULL, last_error_at = NULL, \
             updated_at = now() \
         WHERE pgt_id = $1",
        &[st.pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(format!("Failed to reset status: {}", e)))?;
    actions.push("status reset: ACTIVE, errors cleared".to_string());

    shmem::signal_dag_invalidation(st.pgt_id);
    template_cache::invalidate(st.pgt_id);
    shmem::bump_cache_generation();

    let summary = format!(
        "repair_stream_table({}.{}): {}",
        schema,
        table_name,
        actions.join("; ")
    );
    pgrx::info!("{}", summary);
    Ok(summary)
}

// ── A-1 (v0.79.0): Convenience helpers ─────────────────────────────────────

/// Set the refresh policy (mode) for an existing stream table.
///
/// A-1: Convenience wrapper around `alter_stream_table` that only sets the
/// refresh mode, keeping all other settings unchanged.
///
/// # Example
/// ```sql
/// SELECT pgtrickle.set_stream_table_refresh_policy('my_schema.my_st', 'DIFFERENTIAL');
/// ```
#[pg_extern(schema = "pgtrickle")]
fn set_stream_table_refresh_policy(name: &str, refresh_mode: &str) {
    let result = alter_stream_table_impl(AlterStreamTableOptions {
        name,
        refresh_mode: Some(refresh_mode),
        ..Default::default()
    });
    if let Err(e) = result {
        raise_error_with_context(e);
    }
}

/// Set the storage policy for an existing stream table.
///
/// A-1: Convenience wrapper around `alter_stream_table` that sets both
/// `append_only` mode and the refresh `tier` in a single call.  Use this
/// to switch a stream table between normal and append-only storage behavior,
/// or to promote/demote it between scheduling tiers.
///
/// - `append_only`: `true` for append-only mode (no DELETEs/UPDATEs in output);
///   `false` for full UPSERT/DELETE behaviour.
/// - `tier`: One of `'hot'`, `'warm'`, `'frozen'`, or `NULL` to keep current.
///
/// # Example
/// ```sql
/// SELECT pgtrickle.set_stream_table_storage_policy('my_schema.my_st', true, 'hot');
/// ```
#[pg_extern(schema = "pgtrickle")]
fn set_stream_table_storage_policy(
    name: &str,
    append_only: default!(Option<bool>, "NULL"),
    tier: default!(Option<&str>, "NULL"),
) {
    let result = alter_stream_table_impl(AlterStreamTableOptions {
        name,
        append_only,
        tier,
        ..Default::default()
    });
    if let Err(e) = result {
        raise_error_with_context(e);
    }
}

// ── A-2 (v0.79.0): pause_stream_table ──────────────────────────────────────

/// Pause an active stream table, suspending automated and manual refreshes.
///
/// A-2: First-class wrapper that sets the stream table status to SUSPENDED.
/// Use `pgtrickle.resume_stream_table()` to re-enable refreshes.
///
/// Only ACTIVE stream tables can be paused. Already-SUSPENDED or ERROR
/// stream tables will return an error.
///
/// # Example
/// ```sql
/// SELECT pgtrickle.pause_stream_table('my_schema.my_st');
/// SELECT pgtrickle.resume_stream_table('my_schema.my_st');
/// ```
#[pg_extern(schema = "pgtrickle")]
fn pause_stream_table(name: &str) {
    let result = pause_stream_table_impl(name);
    if let Err(e) = result {
        raise_error_with_context(e);
    }
}

fn pause_stream_table_impl(name: &str) -> Result<(), PgTrickleError> {
    let (schema, table_name) = parse_qualified_name(name)?;
    let st = StreamTableMeta::get_by_name(&schema, &table_name)?;

    if st.status != StStatus::Active {
        return Err(PgTrickleError::InvalidArgument(format!(
            "stream table {}.{} is not ACTIVE (current status: {}); \
             only ACTIVE stream tables can be paused",
            schema,
            table_name,
            st.status.as_str(),
        )));
    }

    let deps = StDependency::get_for_st(st.pgt_id)?;
    let source_oids: Vec<_> = deps
        .iter()
        .filter(|dep| matches!(dep.source_type.as_str(), "TABLE" | "FOREIGN_TABLE"))
        .map(|dep| dep.source_relid)
        .collect();
    cdc::lock_source_relations(&source_oids)?;
    // Establish the repair contract before disabling capture. This keeps an
    // uncaptured interval from being mistaken for a differential catch-up.
    StreamTableMeta::mark_for_reinitialize(st.pgt_id)?;
    StreamTableMeta::update_status(st.pgt_id, StStatus::Suspended)?;
    for dep in &deps {
        if matches!(dep.source_type.as_str(), "TABLE" | "FOREIGN_TABLE") {
            cdc::sync_capture_body_for_source(dep.source_relid)?;
        }
    }

    pgrx::info!(
        "Stream table {}.{} paused (pgt_id={}); use pgtrickle.resume_stream_table() to re-enable.",
        schema,
        table_name,
        st.pgt_id
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_bulk_drop_target_ids_puts_dependents_first() {
        let target_ids = vec![1, 2, 3];
        let downstream_by_id =
            std::collections::HashMap::from([(1, vec![2, 3]), (2, vec![3]), (3, Vec::new())]);

        let ordered = order_bulk_drop_target_ids(&target_ids, &downstream_by_id).unwrap();
        assert_eq!(ordered, vec![3, 2, 1]);
    }

    #[test]
    fn test_order_bulk_drop_target_ids_handles_independent_targets() {
        let target_ids = vec![10, 20, 30];
        let downstream_by_id =
            std::collections::HashMap::from([(10, vec![20]), (20, Vec::new()), (30, Vec::new())]);

        let ordered = order_bulk_drop_target_ids(&target_ids, &downstream_by_id).unwrap();
        let pos_10 = ordered.iter().position(|id| *id == 10).unwrap();
        let pos_20 = ordered.iter().position(|id| *id == 20).unwrap();
        let pos_30 = ordered.iter().position(|id| *id == 30).unwrap();
        assert!(pos_20 < pos_10);
        assert!(pos_30 < ordered.len());
    }

    #[test]
    fn test_order_bulk_drop_target_ids_rejects_cycles() {
        let target_ids = vec![1, 2];
        let downstream_by_id = std::collections::HashMap::from([(1, vec![2]), (2, vec![1])]);

        let err = order_bulk_drop_target_ids(&target_ids, &downstream_by_id).unwrap_err();
        assert!(err.to_string().contains("cycle detected"));
    }
}
