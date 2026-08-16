//! Diagnostic, monitoring, and operational SQL functions.
//!
//! Status views, EXPLAIN output, fuse management, source gating,
//! watermarks, refresh groups, and recommendation endpoints.

use super::*;
use std::cmp::Ordering;

#[derive(Clone, Debug, PartialEq, Eq)]
struct SchemaVersionAuditRow {
    version: String,
    applied_at: Option<String>,
    description: Option<String>,
}

fn parse_release_version(input: &str) -> Option<(u64, u64, u64)> {
    let base = input.split('-').next()?;
    let mut parts = base.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor, patch))
}

fn compare_release_versions(lhs: &str, rhs: &str) -> Option<Ordering> {
    Some(parse_release_version(lhs)?.cmp(&parse_release_version(rhs)?))
}

fn latest_schema_version_audit_row() -> Option<SchemaVersionAuditRow> {
    let schema_table_exists =
        Spi::get_one::<bool>("SELECT to_regclass('pgtrickle.pgt_schema_version') IS NOT NULL")
            .unwrap_or(None)
            .unwrap_or(false);

    if !schema_table_exists {
        return None;
    }

    Spi::connect(|client| {
        let result = client
            .select(
                "SELECT version, applied_at::text, description \
                 FROM pgtrickle.pgt_schema_version \
                 ORDER BY applied_at DESC, version DESC LIMIT 1",
                None,
                &[],
            )
            .map_err(|e| crate::error::PgTrickleError::SpiError(e.to_string()))?;

        for row in result {
            let version = row.get::<String>(1).unwrap_or(None).unwrap_or_default();
            if version.is_empty() {
                continue;
            }
            return Ok::<_, crate::error::PgTrickleError>(Some(SchemaVersionAuditRow {
                version,
                applied_at: row.get::<String>(2).unwrap_or(None),
                description: row.get::<String>(3).unwrap_or(None),
            }));
        }

        Ok::<_, crate::error::PgTrickleError>(None)
    })
    .unwrap_or(None)
}

fn build_migrate_report(
    runtime_version: &str,
    extension_version: Option<&str>,
    schema_audit: Option<&SchemaVersionAuditRow>,
) -> serde_json::Value {
    let schema_version = schema_audit.map(|row| row.version.as_str());
    let runtime_matches_extension = extension_version == Some(runtime_version);
    let extension_matches_schema =
        extension_version.is_some() && extension_version == schema_version;
    let runtime_matches_schema = schema_version == Some(runtime_version);

    let (status, remediation, remediation_sql) = match extension_version {
        None => (
            "extension_not_installed",
            format!(
                "Install the packaged pg_trickle extension at version {runtime_version} before relying on migration diagnostics."
            ),
            Some(format!(
                "CREATE EXTENSION pg_trickle VERSION '{runtime_version}' CASCADE;"
            )),
        ),
        Some(ext_version)
            if ext_version == runtime_version && schema_version == Some(runtime_version) =>
        {
            ("ok", "No action required.".to_string(), None)
        }
        Some(ext_version) if ext_version == runtime_version => (
            "schema_version_mismatch",
            match schema_version {
                Some(version) => format!(
                    "The loaded library and SQL extension are both at {runtime_version}, but the latest pgtrickle.pgt_schema_version row is {version}. Reinstall or restore the packaged {runtime_version} SQL; pgtrickle.migrate() is read-only and will not advance the schema-version ledger."
                ),
                None => format!(
                    "The loaded library and SQL extension are both at {runtime_version}, but pgtrickle.pgt_schema_version is missing or empty. Reinstall or restore the packaged {runtime_version} SQL; pgtrickle.migrate() is read-only and will not seed the schema-version ledger."
                ),
            },
            None,
        ),
        Some(ext_version)
            if compare_release_versions(ext_version, runtime_version) == Some(Ordering::Less) =>
        {
            (
                "alter_extension_required",
                format!(
                    "The loaded library is {runtime_version}, but pg_extension is still at {ext_version}. Apply the packaged SQL upgrade before using the newer runtime."
                ),
                Some(format!(
                    "ALTER EXTENSION pg_trickle UPDATE TO '{runtime_version}';"
                )),
            )
        }
        Some(ext_version)
            if compare_release_versions(ext_version, runtime_version)
                == Some(Ordering::Greater)
                && schema_version == Some(ext_version) =>
        {
            (
                "restart_required",
                format!(
                    "The SQL extension and schema-version ledger are already at {ext_version}, but PostgreSQL is still running the older {runtime_version} shared library. Restart PostgreSQL to load the updated pg_trickle library."
                ),
                None,
            )
        }
        Some(ext_version) => (
            "version_mismatch",
            match schema_version {
                Some(version) => format!(
                    "Runtime/extension/schema versions disagree (runtime={runtime_version}, extension={ext_version}, schema={version}). If you already ran ALTER EXTENSION, restart PostgreSQL; otherwise apply the packaged upgrade ending at {runtime_version}."
                ),
                None => format!(
                    "Runtime and extension versions disagree (runtime={runtime_version}, extension={ext_version}), and pgtrickle.pgt_schema_version is missing or empty. If you already ran ALTER EXTENSION, restart PostgreSQL; otherwise apply the packaged upgrade ending at {runtime_version}."
                ),
            },
            Some(format!(
                "ALTER EXTENSION pg_trickle UPDATE TO '{runtime_version}';"
            )),
        ),
    };

    serde_json::json!({
        "runtime_version": runtime_version,
        "extension_version": extension_version,
        "schema_version": schema_version,
        "latest_schema_migration": schema_audit.map(|row| {
            serde_json::json!({
                "version": row.version,
                "applied_at": row.applied_at,
                "description": row.description,
            })
        }),
        "runtime_matches_extension": runtime_matches_extension,
        "extension_matches_schema": extension_matches_schema,
        "runtime_matches_schema": runtime_matches_schema,
        "up_to_date": runtime_matches_extension && extension_matches_schema,
        "status": status,
        "remediation": remediation,
        "remediation_sql": remediation_sql,
    })
}

#[pg_extern(schema = "pgtrickle", immutable, parallel_safe)]
pub(super) fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// STAB-7: Check for version mismatch between the compiled .so library
/// and the SQL-installed extension.
///
/// Returns a JSON string with library_version, extension_version, pg_version,
/// and a boolean `version_match`. Emits a WARNING if versions differ (e.g. after
/// `ALTER EXTENSION pg_trickle UPDATE` without restarting PostgreSQL).
#[pg_extern(schema = "pgtrickle")]
pub(super) fn version_check() -> String {
    let lib_version = env!("CARGO_PKG_VERSION");
    let ext_version: Option<String> = Spi::get_one::<String>(
        "SELECT extversion FROM pg_catalog.pg_extension WHERE extname = 'pg_trickle'",
    )
    .unwrap_or(None);
    let pg_version: Option<String> = Spi::get_one::<String>("SELECT version()").unwrap_or(None);

    let ext_ver = ext_version.as_deref().unwrap_or("unknown");
    let version_match = ext_ver == lib_version;

    if !version_match {
        pgrx::warning!(
            "pg_trickle: version mismatch — library (.so) is {} but SQL extension is {}. \
             Restart PostgreSQL after ALTER EXTENSION pg_trickle UPDATE to load the new library.",
            lib_version,
            ext_ver,
        );
    }

    format!(
        "{{\"library_version\":\"{}\",\"extension_version\":\"{}\",\"pg_version\":\"{}\",\"version_match\":{}}}",
        lib_version,
        ext_ver,
        pg_version
            .as_deref()
            .unwrap_or("unknown")
            .replace('"', "\\\""),
        version_match,
    )
}

/// DB-9: Report runtime, extension, and schema-version state without mutating.
///
/// Returns a JSON string describing the loaded pg_trickle shared library
/// version, `pg_extension.extversion`, and the latest
/// `pgtrickle.pgt_schema_version` audit row, plus the correct remediation when
/// they diverge.
///
/// This function performs no INSERT, UPDATE, DELETE, or DDL. Only packaged
/// install/upgrade SQL may advance `pgtrickle.pgt_schema_version`.
#[pg_extern(schema = "pgtrickle")]
pub(super) fn migrate() -> String {
    let runtime_version = env!("CARGO_PKG_VERSION");
    let extension_version: Option<String> = Spi::get_one::<String>(
        "SELECT extversion FROM pg_catalog.pg_extension WHERE extname = 'pg_trickle'",
    )
    .unwrap_or(None);
    let schema_audit = latest_schema_version_audit_row();

    serde_json::to_string(&build_migrate_report(
        runtime_version,
        extension_version.as_deref(),
        schema_audit.as_ref(),
    ))
    .unwrap_or_else(|_| {
        "{\"status\":\"serialization_error\",\"remediation\":\"Inspect pgtrickle version state manually.\"}".to_string()
    })
}

/// Bump the shared-memory DAG rebuild signal.
///
/// Called automatically at the end of `CREATE EXTENSION pg_trickle` so the
/// launcher background worker notices the new install and spawns a scheduler
/// immediately, rather than waiting for the 5-minute skip TTL.
///
/// Also safe to call manually if the launcher needs a nudge.
#[pg_extern(schema = "pgtrickle")]
pub(super) fn _signal_launcher_rescan() {
    crate::shmem::signal_dag_rebuild();
}

/// Rebuild all CDC triggers (function body + trigger DDL) for every source
/// table tracked by pg_trickle.
///
/// Called automatically during `ALTER EXTENSION pg_trickle UPDATE` (0.3.0 →
/// 0.4.0) to migrate existing row-level CDC triggers to statement-level.
/// Can also be called manually after changing `pg_trickle.cdc_trigger_mode`.
///
/// Returns `'done'` on success. Any rebuild failure aborts the transaction so
/// callers cannot mark a buffer as migrated while its writer still emits the
/// legacy identity encoding.
#[pg_extern(schema = "pgtrickle")]
pub(super) fn rebuild_cdc_triggers() -> &'static str {
    let change_schema = config::pg_trickle_change_buffer_schema();

    let source_oids: Vec<pg_sys::Oid> = Spi::connect(|client| {
        let result = client
            .select(
                "SELECT DISTINCT source_relid \
                 FROM pgtrickle.pgt_dependencies \
                 WHERE source_type IN ('TABLE', 'STREAM_TABLE')",
                None,
                &[],
            )
            .map_err(|e| crate::error::PgTrickleError::SpiError(e.to_string()))?;
        let mut oids = Vec::new();
        for row in result {
            let oid = row
                .get::<pg_sys::Oid>(1)
                .unwrap_or(None)
                .unwrap_or(pg_sys::InvalidOid);
            if oid != pg_sys::InvalidOid {
                oids.push(oid);
            }
        }
        Ok::<_, crate::error::PgTrickleError>(oids)
    })
    .unwrap_or_default();

    for source_oid in source_oids {
        if let Err(e) = cdc::rebuild_cdc_trigger(source_oid, &change_schema) {
            pgrx::error!(
                "pg_trickle: rebuild_cdc_triggers: failed for OID {}: {}",
                source_oid.to_u32(),
                e
            );
        }
    }

    "done"
}

/// Parse a Prometheus/GNU-style duration string and return seconds.
///
/// Used by SQL views to compare schedule. Returns NULL for invalid input
/// (cron expressions should not be passed).
#[pg_extern(schema = "pgtrickle", immutable, parallel_safe)]
pub(super) fn parse_duration_seconds(input: &str) -> Option<i64> {
    parse_duration(input).ok()
}

/// Get the status of all stream tables.
///
/// Returns a summary row per stream table including schedule configuration,
/// data timestamp, and computed staleness interval.
#[pg_extern(schema = "pgtrickle", name = "pgt_status")]
#[allow(clippy::type_complexity)]
pub(super) fn pgt_status() -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(status, String),
        name!(refresh_mode, String),
        name!(is_populated, bool),
        name!(consecutive_errors, i32),
        name!(schedule, Option<String>),
        name!(data_timestamp, Option<TimestampWithTimeZone>),
        name!(staleness, Option<pgrx::datum::Interval>),
        name!(scc_id, Option<i32>),
    ),
> {
    let rows: Vec<_> = Spi::connect(|client| {
        let result = client
            .select(
                "SELECT pgt_schema || '.' || pgt_name, status, refresh_mode, \
                 is_populated, consecutive_errors, schedule, data_timestamp, \
                 now() - data_timestamp AS staleness, scc_id \
                 FROM pgtrickle.pgt_stream_tables ORDER BY pgt_schema, pgt_name",
                None,
                &[],
            )
            .unwrap_or_else(|e| {
                pgrx::error!(
                    "{}",
                    crate::error::PgTrickleError::DiagnosticError(format!(
                        "st_list: SPI select failed: {e}"
                    ))
                )
            });

        let mut out = Vec::new();
        for row in result {
            let name = row.get::<String>(1).unwrap_or(None).unwrap_or_default();
            let status = row.get::<String>(2).unwrap_or(None).unwrap_or_default();
            let mode = row.get::<String>(3).unwrap_or(None).unwrap_or_default();
            let populated = row.get::<bool>(4).unwrap_or(None).unwrap_or(false);
            let errors = row.get::<i32>(5).unwrap_or(None).unwrap_or(0);
            let schedule = row.get::<String>(6).unwrap_or(None);
            let data_ts = row.get::<TimestampWithTimeZone>(7).unwrap_or(None);
            let staleness = row.get::<pgrx::datum::Interval>(8).unwrap_or(None);
            let scc_id = row.get::<i32>(9).unwrap_or(None);
            out.push((
                name, status, mode, populated, errors, schedule, data_ts, staleness, scc_id,
            ));
        }
        out
    });

    TableIterator::new(rows)
}

/// CYC-7: Show the status of all cyclic strongly connected components.
///
/// Returns one row per SCC, summarising its members, most recent fixpoint
/// iteration count, and last convergence time.
#[pg_extern(schema = "pgtrickle", name = "pgt_scc_status")]
#[allow(clippy::type_complexity)]
pub(super) fn pgt_scc_status() -> TableIterator<
    'static,
    (
        name!(scc_id, i32),
        name!(member_count, i32),
        name!(members, Vec<String>),
        name!(last_iterations, Option<i32>),
        name!(last_converged_at, Option<TimestampWithTimeZone>),
    ),
> {
    let rows: Vec<_> = Spi::connect(|client| {
        let result = client
            .select(
                "SELECT \
                     st.scc_id, \
                     count(*)::int AS member_count, \
                     array_agg(st.pgt_schema || '.' || st.pgt_name ORDER BY st.pgt_name) AS members, \
                     max(st.last_fixpoint_iterations) AS last_iterations, \
                     max(st.last_refresh_at) AS last_converged_at \
                 FROM pgtrickle.pgt_stream_tables st \
                 WHERE st.scc_id IS NOT NULL \
                 GROUP BY st.scc_id \
                 ORDER BY st.scc_id",
                None,
                &[],
            )
            .unwrap_or_else(|e| pgrx::error!("{}", crate::error::PgTrickleError::DiagnosticError(format!("pgt_scc_status: SPI select failed: {e}"))));

        let mut out = Vec::new();
        for row in result {
            let scc_id = row.get::<i32>(1).unwrap_or(None).unwrap_or(0);
            let member_count = row.get::<i32>(2).unwrap_or(None).unwrap_or(0);
            let members = row
                .get::<Vec<String>>(3)
                .unwrap_or(None)
                .unwrap_or_default();
            let last_iterations = row.get::<i32>(4).unwrap_or(None);
            let last_converged_at = row.get::<TimestampWithTimeZone>(5).unwrap_or(None);
            out.push((
                scc_id,
                member_count,
                members,
                last_iterations,
                last_converged_at,
            ));
        }
        out
    });

    TableIterator::new(rows)
}

/// G12-ERM-2: Explain the configured vs. effective refresh mode for a stream table.
///
/// Reports three pieces of information useful for diagnosing unexpected
/// FULL-refresh behaviour in AUTO mode:
///
/// - `configured_mode`  — the mode stored in `pgt_stream_tables.refresh_mode`
///   (`FULL`, `DIFFERENTIAL`, `IMMEDIATE`, or `AUTO`).
/// - `effective_mode`   — the mode that was **actually used** on the most
///   recent completed refresh. NULL until the first refresh fires.
///   One of `FULL`, `DIFFERENTIAL`, `APPEND_ONLY`, `TOP_K`, `NO_DATA`.
/// - `downgrade_reason` — a human-readable explanation when `effective_mode`
///   differs from the non-AUTO configured expectation, or when the
///   configured mode is `AUTO` and the effective mode is `FULL` (i.e. a
///   downgrade occurred).
///
/// Example:
/// ```sql
/// SELECT * FROM pgtrickle.explain_refresh_mode('public.orders_summary');
/// ```
#[pg_extern(schema = "pgtrickle")]
#[allow(clippy::type_complexity)]
pub(super) fn explain_refresh_mode(
    name: &str,
) -> TableIterator<
    'static,
    (
        name!(configured_mode, String),
        name!(effective_mode, Option<String>),
        name!(downgrade_reason, Option<String>),
    ),
> {
    let rows = explain_refresh_mode_impl(name);
    TableIterator::new(rows)
}

pub(super) fn explain_refresh_mode_impl(
    name: &str,
) -> Vec<(String, Option<String>, Option<String>)> {
    let (schema, table_name) = match parse_qualified_name(name) {
        Ok(pair) => pair,
        Err(e) => {
            raise_error_with_context(e);
        }
    };

    let row = Spi::connect(|client| {
        client
            .select(
                "SELECT refresh_mode, effective_refresh_mode \
                 FROM pgtrickle.pgt_stream_tables \
                 WHERE pgt_schema = $1 AND pgt_name = $2",
                None,
                &[schema.as_str().into(), table_name.as_str().into()],
            )
            .unwrap_or_else(|e| {
                pgrx::error!(
                    "{}",
                    crate::error::PgTrickleError::DiagnosticError(format!(
                        "explain_refresh_mode: SPI error: {e}"
                    ))
                )
            })
            .first()
            .get_two::<String, String>()
            .unwrap_or((None, None))
    });

    let (configured_opt, effective_opt) = row;
    let configured = configured_opt.unwrap_or_else(|| {
        pgrx::error!(
            "{}",
            crate::error::PgTrickleError::DiagnosticError(format!(
                "stream table '{}' not found",
                name
            ))
        );
    });

    let downgrade_reason: Option<String> = match (configured.as_str(), effective_opt.as_deref()) {
        // AUTO mode chose FULL — explain why we can't know for certain,
        // but point operators to the refresh history for details.
        ("AUTO" | "DIFFERENTIAL", Some("FULL")) => Some(
            "The most recent refresh used FULL mode. Possible causes: defining query \
             contains a CTE or unsupported operator, adaptive change-ratio threshold \
             was exceeded, or aggregate saturation occurred. \
             Check pgtrickle.pgt_refresh_history for details."
                .to_string(),
        ),
        // DIFFERENTIAL configured but actual was APPEND_ONLY — informational.
        ("DIFFERENTIAL" | "AUTO", Some("APPEND_ONLY")) => Some(
            "The most recent refresh used the APPEND_ONLY INSERT fast-path because \
             no DELETE or UPDATE was detected in the change buffer."
                .to_string(),
        ),
        // IMMEDIATE mode — effective_mode is always NULL (triggers handle it).
        ("IMMEDIATE", _) => Some(
            "IMMEDIATE mode is maintained by in-transaction IVM triggers; \
             the background scheduler does not run refreshes for this table."
                .to_string(),
        ),
        _ => None,
    };

    vec![(configured, effective_opt, downgrade_reason)]
}

// ── FUSE-3: reset_fuse() ───────────────────────────────────────────────────

// ── PROF-DLT: explain_delta() ──────────────────────────────────────────────

/// Show the delta SQL query plan for a stream table without executing a refresh.
///
/// Generates the differential delta SQL that would be used on the next refresh,
/// then runs `EXPLAIN (ANALYZE false, FORMAT <format>)` on it and returns the
/// plan lines. Useful for identifying slow joins, missing indexes, or
/// unexpected plan shapes in the auto-generated delta query.
///
/// Parameters:
/// - `name` — qualified stream table name (e.g. `'public.orders_summary'`)
/// - `format` — output format: `'text'` (default), `'json'`, `'xml'`, `'yaml'`
///
/// The delta SQL is generated against a hypothetical "scan all changes" window
/// (LSN 0/0 → FF/FFFFFFFF) so the plan shows full join/filter structure
/// even when the change buffer is currently empty.
///
/// Example:
/// ```sql
/// SELECT line FROM pgtrickle.explain_delta('public.orders_summary');
/// SELECT line FROM pgtrickle.explain_delta('public.orders_summary', 'json');
/// ```
#[pg_extern(schema = "pgtrickle", name = "explain_delta")]
pub(super) fn explain_delta_text(
    name: &str,
    format: default!(&str, "'text'"),
) -> SetOfIterator<'static, String> {
    let rows = match explain_delta_impl(name, format) {
        Ok(r) => r,
        Err(e) => raise_error_with_context(e),
    };
    SetOfIterator::new(rows)
}

pub(super) fn explain_delta_impl(name: &str, format: &str) -> Result<Vec<String>, PgTrickleError> {
    let (schema, table_name) = parse_qualified_name(name)?;
    let st = StreamTableMeta::get_by_name(&schema, &table_name)?;

    // Get source OIDs by parsing the defining query.
    let source_oids = crate::dvm::get_source_oids_for_query(&st.defining_query)?;

    // Build a max new-frontier so the change buffer filter covers all rows
    // (lsn > '0/0' AND lsn <= 'FF/FFFFFFFF'), giving a plan representative
    // of a real refresh against a fully-populated change buffer.
    let prev_frontier = crate::version::Frontier::new();
    let mut new_frontier = crate::version::Frontier::new();
    for &oid in &source_oids {
        new_frontier.set_source(oid, "FF/FFFFFFFF".to_string(), String::new());
    }

    // Generate delta SQL.
    let delta_result = crate::dvm::generate_delta_query(
        &st.defining_query,
        &prev_frontier,
        &new_frontier,
        &st.pgt_schema,
        &st.pgt_name,
    )?;

    let delta_sql = &delta_result.delta_sql;

    // Normalise format string.
    let fmt_upper = format.trim().to_uppercase();
    let fmt_kw = match fmt_upper.as_str() {
        "JSON" | "XML" | "YAML" | "TEXT" => fmt_upper.as_str(),
        other => {
            return Err(PgTrickleError::InvalidArgument(format!(
                "unsupported EXPLAIN format '{other}'; expected 'text', 'json', 'xml', or 'yaml'"
            )));
        }
    };

    // Run EXPLAIN without ANALYZE (no side effects).
    let explain_sql = format!(
        "EXPLAIN (ANALYZE false, FORMAT {fmt_kw}) \
         SELECT * FROM ({delta_sql}) __pgt_explain_d"
    );

    // For JSON/XML formats, PostgreSQL returns typed columns (json/xml)
    // which are not directly compatible with pgrx's String retrieval.
    let is_typed_format = fmt_kw == "JSON" || fmt_kw == "XML";

    let rows: Vec<String> = Spi::connect(|client| {
        let result = client
            .select(&explain_sql, None, &[])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        let mut lines = Vec::new();
        for row in result {
            let line: Option<String> = if is_typed_format {
                // JSON returns json type (Oid 114), XML returns xml type.
                // Use pgrx::Json to extract, then convert to String.
                match row.get::<pgrx::Json>(1) {
                    Ok(Some(json_val)) => Some(json_val.0.to_string()),
                    Ok(None) => None,
                    Err(_) => {
                        // Fall back to trying as xml/text
                        row.get::<String>(1)
                            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                    }
                }
            } else {
                row.get(1)
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            };
            if let Some(l) = line {
                lines.push(l);
            }
        }
        Ok::<Vec<String>, PgTrickleError>(lines)
    })?;

    Ok(rows)
}

// ── G14-MDED: dedup_stats() ────────────────────────────────────────────────

/// Show MERGE deduplication profiling counters accumulated since server start.
///
/// When the delta cannot be guaranteed to have at most one row per
/// `__pgt_row_id` (e.g. for aggregate queries or keyless sources), the MERGE
/// must group + aggregate the delta before merging. This is tracked as
/// "dedup needed". A high ratio indicates that pre-MERGE compaction in the
/// change buffer would reduce refresh latency.
///
/// Counters reset to zero on server restart. Only differential refreshes that
/// actually process rows (post no-data short-circuit) are counted.
///
/// Example:
/// ```sql
/// SELECT * FROM pgtrickle.dedup_stats();
/// ```
#[pg_extern(schema = "pgtrickle", name = "dedup_stats")]
pub(super) fn dedup_stats_fn() -> TableIterator<
    'static,
    (
        name!(total_diff_refreshes, i64),
        name!(dedup_needed, i64),
        name!(dedup_ratio_pct, f64),
    ),
> {
    let (total, dedup) = crate::shmem::read_dedup_stats();
    let ratio = if total == 0 {
        0.0_f64
    } else {
        (dedup as f64 / total as f64) * 100.0
    };
    TableIterator::new(vec![(total as i64, dedup as i64, ratio)])
}

/// D-4: Shared change buffer statistics.
///
/// Returns one row per shared change buffer (one per source table), showing
/// how many stream tables share the buffer, the columns tracked, the safe
/// cleanup frontier (MIN across all consumers), and the current buffer row count.
///
/// Example:
/// ```sql
/// SELECT * FROM pgtrickle.shared_buffer_stats();
/// ```
#[allow(clippy::type_complexity)]
#[pg_extern(schema = "pgtrickle", name = "shared_buffer_stats")]
pub(super) fn shared_buffer_stats_fn() -> TableIterator<
    'static,
    (
        name!(source_oid, i64),
        name!(source_table, String),
        name!(consumer_count, i32),
        name!(consumers, String),
        name!(columns_tracked, i32),
        name!(safe_frontier_lsn, Option<String>),
        name!(buffer_rows, i64),
        name!(is_partitioned, bool),
    ),
> {
    let rows = shared_buffer_stats_impl();
    TableIterator::new(rows)
}

#[allow(clippy::type_complexity)]
pub(super) fn shared_buffer_stats_impl()
-> Vec<(i64, String, i32, String, i32, Option<String>, i64, bool)> {
    let change_schema = "pgtrickle_changes";

    let query = "\
        SELECT ct.source_relid::bigint AS source_relid, \
               format('%I.%I', n.nspname, c.relname) AS source_table, \
               array_length(ct.tracked_by_pgt_ids, 1) AS consumer_count, \
               ct.tracked_by_pgt_ids \
        FROM pgtrickle.pgt_change_tracking ct \
        JOIN pg_class c ON c.oid = ct.source_relid \
        JOIN pg_namespace n ON n.oid = c.relnamespace \
        ORDER BY ct.source_relid";

    let mut rows = Vec::new();

    Spi::connect(|client| {
        if let Ok(table) = client.select(query, None, &[]) {
            for row in table {
                let source_oid: i64 = row
                    .get_by_name::<i64, _>("source_relid")
                    .ok()
                    .flatten()
                    .unwrap_or(0);
                let source_table: String = row
                    .get_by_name::<String, _>("source_table")
                    .ok()
                    .flatten()
                    .unwrap_or_default();
                let consumer_count: i32 = row
                    .get_by_name::<i32, _>("consumer_count")
                    .ok()
                    .flatten()
                    .unwrap_or(0);

                let consumers = Spi::get_one::<String>(&format!(
                    "SELECT string_agg(format('%I.%I', st.pgt_schema, st.pgt_name), ', ' \
                     ORDER BY st.pgt_name) \
                     FROM pgtrickle.pgt_stream_tables st \
                     WHERE st.pgt_id IN ( \
                       SELECT unnest(tracked_by_pgt_ids) FROM pgtrickle.pgt_change_tracking \
                        WHERE source_relid = {source_oid})",
                ))
                .unwrap_or(None)
                .unwrap_or_default();

                // v0.32.0+: buffer tables use stable (xxh64-derived) names.
                let buf_base =
                    crate::cdc::buffer_base_name_for_oid(pg_sys::Oid::from(source_oid as u32));

                // A44-10 (D+I schema): columns are now flat (no new_/old_ prefix).
                // Count user data columns by excluding the fixed system columns.
                let columns_tracked: i32 = Spi::get_one::<i64>(&format!(
                    "SELECT count(*)::bigint FROM information_schema.columns \
                     WHERE table_schema = '{change_schema}' \
                       AND table_name = '{buf_base}' \
                       AND column_name NOT IN (\
                         'change_id','lsn','action','pk_hash','changed_cols',\
                         '__pgt_trace_context')",
                ))
                .unwrap_or(None)
                .unwrap_or(0) as i32;

                let safe_frontier_lsn: Option<String> = Spi::get_one::<String>(&format!(
                    "SELECT MIN((st.frontier->'sources'->'{source_oid}'->>'lsn')::pg_lsn)::TEXT \
                     FROM pgtrickle.pgt_stream_tables st \
                     JOIN pgtrickle.pgt_dependencies dep ON dep.pgt_id = st.pgt_id \
                     WHERE dep.source_relid = {source_oid} \
                       AND st.frontier IS NOT NULL \
                       AND st.frontier->'sources'->'{source_oid}'->>'lsn' IS NOT NULL",
                ))
                .unwrap_or(None);

                let buffer_rows: i64 = Spi::get_one::<i64>(&format!(
                    // nosemgrep: rust.spi.query.dynamic-format
                    "SELECT count(*)::bigint FROM \"{change_schema}\".{buf_base}",
                ))
                .unwrap_or(None)
                .unwrap_or(0);

                let is_partitioned =
                    crate::cdc::is_buffer_partitioned(change_schema, source_oid as u32);

                rows.push((
                    source_oid,
                    source_table,
                    consumer_count,
                    consumers,
                    columns_tracked,
                    safe_frontier_lsn,
                    buffer_rows,
                    is_partitioned,
                ));
            }
        }
    });

    rows
}

/// Reset a blown fuse on a stream table.
///
/// The `action` parameter controls how pending changes are handled:
/// - `'apply'` (default): Re-arm the fuse and let the next scheduler tick
///   process the buffered changes normally. The stream table resumes as ACTIVE.
/// - `'reinitialize'`: Re-arm the fuse and mark the stream table for full
///   reinitialization. All buffered changes are consumed by the full refresh.
/// - `'skip_changes'`: Re-arm the fuse, drain (discard) all pending change
///   buffer rows for this stream table's sources, then resume as ACTIVE.
///
/// Returns nothing on success; raises an ERROR if the stream table does not
/// exist or the fuse is not blown.
#[pg_extern(schema = "pgtrickle")]
pub(super) fn reset_fuse(name: &str, action: default!(&str, "'apply'")) {
    let result = reset_fuse_impl(name, action);
    if let Err(e) = result {
        raise_error_with_context(e);
    }
}

pub(super) fn reset_fuse_impl(name: &str, action: &str) -> Result<(), PgTrickleError> {
    let (schema, table_name) = parse_qualified_name(name)?;
    let st = StreamTableMeta::get_by_name(&schema, &table_name)?;
    check_stream_table_ownership(st.pgt_relid, &schema, &table_name)?;

    if st.fuse_state != "blown" {
        return Err(PgTrickleError::InvalidArgument(format!(
            "fuse is not blown for {}.{} (current state: {})",
            schema, table_name, st.fuse_state
        )));
    }

    let normalized_action = action.trim().to_lowercase();
    match normalized_action.as_str() {
        "apply" => {
            // Re-arm fuse and set status to ACTIVE — changes processed on next tick.
            StreamTableMeta::reset_fuse(st.pgt_id)?;
        }
        "reinitialize" => {
            // Re-arm fuse, mark needs_reinit, set ACTIVE.
            StreamTableMeta::reset_fuse(st.pgt_id)?;
            StreamTableMeta::mark_for_reinitialize(st.pgt_id)?;
        }
        "skip_changes" => {
            // Re-arm fuse then drain all pending changes for this ST's sources.
            StreamTableMeta::reset_fuse(st.pgt_id)?;
            let change_schema = config::pg_trickle_change_buffer_schema();
            let deps = crate::catalog::StDependency::get_for_st(st.pgt_id)?;
            for dep in &deps {
                if dep.source_type == "TABLE" || dep.source_type == "FOREIGN_TABLE" {
                    let buf =
                        crate::cdc::buffer_qualified_name_for_oid(&change_schema, dep.source_relid);
                    let drain_sql = format!("TRUNCATE {buf}");
                    if let Err(e) = Spi::run(&drain_sql) {
                        // nosemgrep: rust.spi.run.dynamic-format — buffer name is derived from source OID.
                        // nosemgrep: rust.spi.run.dynamic-format — buffer name is derived from source OID.
                        pgrx::warning!(
                            "reset_fuse: failed to drain change buffer for source OID {}: {}",
                            dep.source_relid.to_u32(),
                            e,
                        );
                    }
                }
            }
        }
        _ => {
            return Err(PgTrickleError::InvalidArgument(format!(
                "invalid reset_fuse action: '{}' (expected 'apply', 'reinitialize', or 'skip_changes')",
                action
            )));
        }
    }

    pgrx::info!(
        "pg_trickle: fuse reset for {}.{} with action '{}'",
        schema,
        table_name,
        normalized_action,
    );
    Ok(())
}

// ── FUSE-4: fuse_status() ──────────────────────────────────────────────────

/// Show the fuse circuit breaker status for all stream tables.
///
/// Returns one row per stream table with fuse configuration and state.
#[pg_extern(schema = "pgtrickle")]
#[allow(clippy::type_complexity)]
pub(super) fn fuse_status() -> TableIterator<
    'static,
    (
        name!(stream_table, String),
        name!(fuse_mode, String),
        name!(fuse_state, String),
        name!(fuse_ceiling, Option<i64>),
        name!(effective_ceiling, Option<i64>),
        name!(fuse_sensitivity, Option<i32>),
        name!(blown_at, Option<TimestampWithTimeZone>),
        name!(blow_reason, Option<String>),
    ),
> {
    let rows = fuse_status_impl();
    TableIterator::new(rows)
}

#[allow(clippy::type_complexity)]
pub(super) fn fuse_status_impl() -> Vec<(
    String,
    String,
    String,
    Option<i64>,
    Option<i64>,
    Option<i32>,
    Option<TimestampWithTimeZone>,
    Option<String>,
)> {
    let sts = match StreamTableMeta::get_all() {
        Ok(sts) => sts,
        Err(e) => {
            pgrx::warning!("fuse_status: failed to load stream tables: {}", e);
            return Vec::new();
        }
    };

    let global_ceiling = config::pg_trickle_fuse_default_ceiling();

    sts.into_iter()
        .map(|st| {
            let qualified = format!("{}.{}", st.pgt_schema, st.pgt_name);
            let effective_ceiling = if st.fuse_mode == "off" {
                None
            } else {
                st.fuse_ceiling.or(if global_ceiling > 0 {
                    Some(global_ceiling)
                } else {
                    None
                })
            };
            (
                qualified,
                st.fuse_mode,
                st.fuse_state,
                st.fuse_ceiling,
                effective_ceiling,
                st.fuse_sensitivity,
                st.blown_at,
                st.blow_reason,
            )
        })
        .collect()
}

/// Show detected diamond consistency groups.
///
/// Returns one row per group member, indicating which group it belongs to,
/// whether it is a convergence (fan-in) node, the group's current epoch,
/// and the effective schedule policy.
#[pg_extern(schema = "pgtrickle", name = "diamond_groups")]
#[allow(clippy::type_complexity)]
pub(super) fn diamond_groups() -> TableIterator<
    'static,
    (
        name!(group_id, i32),
        name!(member_name, String),
        name!(member_schema, String),
        name!(is_convergence, bool),
        name!(epoch, i64),
        name!(schedule_policy, String),
    ),
> {
    let rows: Vec<_> = Spi::connect(|_client| {
        let dag = match StDag::build_from_catalog(config::pg_trickle_default_schedule_seconds()) {
            Ok(d) => d,
            Err(e) => {
                pgrx::warning!("diamond_groups: failed to build DAG: {}", e);
                return Vec::new();
            }
        };

        let groups = dag.compute_consistency_groups();
        let mut out = Vec::new();

        for (idx, group) in groups.iter().enumerate() {
            // Skip singletons — they are not in a diamond.
            if group.is_singleton() {
                continue;
            }

            let group_id = (idx + 1) as i32;
            let convergence_set: std::collections::HashSet<_> =
                group.convergence_points.iter().collect();

            // Compute effective schedule policy for the group.
            let effective_policy = group.convergence_points.iter().fold(
                DiamondSchedulePolicy::Fastest,
                |acc, node| {
                    if let NodeId::StreamTable(id) = node {
                        StreamTableMeta::get_all_active()
                            .ok()
                            .and_then(|sts| sts.into_iter().find(|s| s.pgt_id == *id))
                            .map(|st| acc.stricter(st.diamond_schedule_policy))
                            .unwrap_or(acc)
                    } else {
                        acc
                    }
                },
            );

            for member in &group.members {
                if let NodeId::StreamTable(pgt_id) = member {
                    // Load metadata for name/schema
                    let (name, schema) = match StreamTableMeta::get_all_active() {
                        Ok(sts) => {
                            if let Some(st) = sts.iter().find(|s| s.pgt_id == *pgt_id) {
                                (st.pgt_name.clone(), st.pgt_schema.clone())
                            } else {
                                (format!("pgt_id={}", pgt_id), "unknown".to_string())
                            }
                        }
                        Err(_) => (format!("pgt_id={}", pgt_id), "unknown".to_string()),
                    };

                    let is_convergence = convergence_set.contains(member);
                    out.push((
                        group_id,
                        name,
                        schema,
                        is_convergence,
                        group.epoch as i64,
                        effective_policy.as_str().to_string(),
                    ));
                }
            }
        }
        out
    });

    TableIterator::new(rows)
}

// ── Bootstrap Source Gating (v0.5.0, Phase 3) ─────────────────────────────

/// Mark a source table as "gated" so that all stream tables depending on it
/// are skipped by the scheduler until `ungate_source()` is called.
///
/// This is useful when loading bulk historical data into a source table:
/// gate it first, load the data, then ungate it so the scheduler picks it up
/// in one atomic refresh instead of refreshing repeatedly mid-load.
///
/// `source` is the source table name, optionally schema-qualified.
#[pg_extern(schema = "pgtrickle")]
pub(super) fn gate_source(source: &str) -> Result<(), PgTrickleError> {
    let source_relid = resolve_source_oid(source)?;
    crate::catalog::upsert_gate(source_relid, Some("gate_source"))?;

    // Signal the scheduler that the gate set has changed.
    let payload = format!("{}", source_relid.to_u32());
    // pg_notify does not support parameterized payloads; payload is source_relid.to_u32() (a plain integer).
    let gate_sql = format!("SELECT pg_notify('pgtrickle_source_gate', '{}')", payload);
    Spi::run(&gate_sql) // nosemgrep: rust.spi.run.dynamic-format — source identifier is catalog-resolved.
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    pgrx::info!(
        "pg_trickle: source {} (oid={}) is now gated",
        source,
        source_relid.to_u32()
    );
    Ok(())
}

/// Ungate a previously gated source table, allowing the scheduler to resume
/// refreshing stream tables that depend on it.
///
/// `source` is the source table name, optionally schema-qualified.
#[pg_extern(schema = "pgtrickle")]
pub(super) fn ungate_source(source: &str) -> Result<(), PgTrickleError> {
    let source_relid = resolve_source_oid(source)?;
    crate::catalog::set_ungated(source_relid)?;

    // Signal the scheduler that the gate set has changed.
    let payload = format!("{}", source_relid.to_u32());
    // pg_notify does not support parameterized payloads; payload is source_relid.to_u32() (a plain integer).
    let gate_sql = format!("SELECT pg_notify('pgtrickle_source_gate', '{}')", payload);
    Spi::run(&gate_sql) // nosemgrep: rust.spi.run.dynamic-format — source identifier is catalog-resolved.
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    pgrx::info!(
        "pg_trickle: source {} (oid={}) is now ungated",
        source,
        source_relid.to_u32()
    );
    Ok(())
}

/// Return the current gate status of all registered source gates.
///
/// Only rows that have ever been gated appear in this view (one row per
/// source_relid in `pgt_source_gates`).
#[pg_extern(schema = "pgtrickle", name = "source_gates")]
#[allow(clippy::type_complexity)]
pub(super) fn source_gates_fn() -> TableIterator<
    'static,
    (
        name!(source_table, String),
        name!(schema_name, String),
        name!(gated, bool),
        name!(gated_at, Option<TimestampWithTimeZone>),
        name!(ungated_at, Option<TimestampWithTimeZone>),
        name!(gated_by, Option<String>),
    ),
> {
    let rows: Vec<_> = Spi::connect(|client| {
        let table = client
            .select(
                "SELECT c.relname::text, n.nspname::text, \
                        g.gated, g.gated_at, g.ungated_at, g.gated_by \
                 FROM pgtrickle.pgt_source_gates g \
                 JOIN pg_class c ON c.oid = g.source_relid \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 ORDER BY n.nspname, c.relname",
                None,
                &[],
            )
            .unwrap_or_else(|e| {
                pgrx::warning!("source_gates: SPI error: {}", e);
                pgrx::error!(
                    "{}",
                    crate::error::PgTrickleError::DiagnosticError(format!(
                        "source_gates: SPI error: {e}"
                    ))
                )
            });

        let mut out = Vec::new();
        for row in table {
            let relname: String = row.get::<String>(1).unwrap_or(None).unwrap_or_default();
            let nspname: String = row.get::<String>(2).unwrap_or(None).unwrap_or_default();
            let gated: bool = row.get::<bool>(3).unwrap_or(None).unwrap_or(false);
            let gated_at = row.get::<TimestampWithTimeZone>(4).unwrap_or(None);
            let ungated_at = row.get::<TimestampWithTimeZone>(5).unwrap_or(None);
            let gated_by = row.get::<String>(6).unwrap_or(None);
            out.push((relname, nspname, gated, gated_at, ungated_at, gated_by));
        }
        out
    });

    TableIterator::new(rows)
}

/// Rich introspection of bootstrap gate status.
///
/// Returns one row per registered source gate with additional computed fields:
/// - `gate_duration`: how long the source has been in its current state
///   (if gated: `now() - gated_at`; if ungated: `ungated_at - gated_at`)
/// - `affected_stream_tables`: comma-separated list of stream tables whose
///   scheduler-driven refreshes are blocked by this gate
///
/// BOOT-F3: Designed for debugging "why isn't my stream table refreshing?"
/// situations by showing the full gate lifecycle at a glance.
#[pg_extern(schema = "pgtrickle", name = "bootstrap_gate_status")]
#[allow(clippy::type_complexity)]
pub(super) fn bootstrap_gate_status_fn() -> TableIterator<
    'static,
    (
        name!(source_table, String),
        name!(schema_name, String),
        name!(gated, bool),
        name!(gated_at, Option<TimestampWithTimeZone>),
        name!(ungated_at, Option<TimestampWithTimeZone>),
        name!(gated_by, Option<String>),
        name!(gate_duration, Option<pgrx::datum::Interval>),
        name!(affected_stream_tables, Option<String>),
    ),
> {
    let rows: Vec<_> = Spi::connect(|client| {
        let result = client
            .select(
                "SELECT c.relname::text, \
                        n.nspname::text, \
                        g.gated, \
                        g.gated_at, \
                        g.ungated_at, \
                        g.gated_by, \
                        CASE WHEN g.gated THEN now() - g.gated_at \
                             ELSE g.ungated_at - g.gated_at \
                        END AS gate_duration, \
                        ( \
                          SELECT string_agg(st.pgt_schema || '.' || st.pgt_name, ', ' ORDER BY st.pgt_schema, st.pgt_name) \
                          FROM pgtrickle.pgt_stream_tables st \
                          JOIN pgtrickle.pgt_dependencies d ON d.pgt_id = st.pgt_id \
                          WHERE d.source_relid = g.source_relid \
                        ) AS affected_stream_tables \
                 FROM pgtrickle.pgt_source_gates g \
                 JOIN pg_class c ON c.oid = g.source_relid \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 ORDER BY g.gated DESC, n.nspname, c.relname",
                None,
                &[],
            )
            .unwrap_or_else(|e| pgrx::error!("{}", crate::error::PgTrickleError::DiagnosticError(format!("bootstrap_gate_status: SPI select failed: {e}"))));

        let mut out = Vec::new();
        for row in result {
            let relname = row.get::<String>(1).unwrap_or(None).unwrap_or_default();
            let nspname = row.get::<String>(2).unwrap_or(None).unwrap_or_default();
            let gated = row.get::<bool>(3).unwrap_or(None).unwrap_or(false);
            let gated_at = row.get::<TimestampWithTimeZone>(4).unwrap_or(None);
            let ungated_at = row.get::<TimestampWithTimeZone>(5).unwrap_or(None);
            let gated_by = row.get::<String>(6).unwrap_or(None);
            let duration = row.get::<pgrx::datum::Interval>(7).unwrap_or(None);
            let affected = row.get::<String>(8).unwrap_or(None);
            out.push((
                relname, nspname, gated, gated_at, ungated_at, gated_by, duration, affected,
            ));
        }
        out
    });

    TableIterator::new(rows)
}

// ── Watermark Gating (v0.7.0) ─────────────────────────────────────────────

/// Advance the watermark for a source table.
///
/// The external process calls this after each load batch to signal "this
/// source's data is complete through timestamp `watermark`".
///
/// - **Monotonic:** rejects watermarks that go backward.
/// - **Idempotent:** re-advancing to the same value is a no-op.
/// - **Transactional:** the watermark is part of the caller's transaction.
#[pg_extern(schema = "pgtrickle")]
pub(super) fn advance_watermark(
    source: &str,
    watermark: TimestampWithTimeZone,
) -> Result<(), PgTrickleError> {
    let source_relid = resolve_source_oid(source)?;
    let advanced_by = Spi::get_one::<String>("SELECT current_user::text")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    crate::catalog::advance_watermark(source_relid, watermark, advanced_by.as_deref())?;

    // Notify the scheduler that watermark state changed.
    let payload = format!("wm:{}", source_relid.to_u32());
    // pg_notify does not support parameterized payloads; payload is "wm:" + source_relid.to_u32() (plain integer).
    let wm_sql = format!("SELECT pg_notify('pgtrickle_watermark', '{}')", payload);
    Spi::run(&wm_sql) // nosemgrep: rust.spi.run.dynamic-format — watermark identifier is catalog-resolved.
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    pgrx::info!(
        "pg_trickle: watermark for {} (oid={}) advanced",
        source,
        source_relid.to_u32()
    );
    Ok(())
}

/// Create a watermark group that declares a set of sources must be
/// temporally aligned before downstream stream tables refresh.
///
/// - `group_name`: unique name for this group.
/// - `sources`: array of source table names (optionally schema-qualified).
/// - `tolerance_secs`: maximum allowed lag in seconds between the most
///   advanced and least advanced watermark in the group (default 0 = strict).
#[pg_extern(schema = "pgtrickle")]
pub(super) fn create_watermark_group(
    group_name: &str,
    sources: Vec<String>,
    tolerance_secs: default!(f64, 0.0),
) -> Result<i32, PgTrickleError> {
    if sources.len() < 2 {
        return Err(PgTrickleError::InvalidArgument(
            "watermark group requires at least 2 sources".into(),
        ));
    }
    if tolerance_secs < 0.0 {
        return Err(PgTrickleError::InvalidArgument(
            "tolerance_secs must be non-negative".into(),
        ));
    }

    let mut source_oids = Vec::with_capacity(sources.len());
    for src in &sources {
        let oid = resolve_source_oid(src)?;
        source_oids.push(oid);
    }

    let group_id =
        crate::catalog::create_watermark_group(group_name, &source_oids, tolerance_secs)?;

    pgrx::info!(
        "pg_trickle: created watermark group '{}' (id={}) with {} sources, tolerance {:.1}s",
        group_name,
        group_id,
        sources.len(),
        tolerance_secs
    );
    Ok(group_id)
}

/// Drop a watermark group by name.
#[pg_extern(schema = "pgtrickle")]
pub(super) fn drop_watermark_group(group_name: &str) -> Result<(), PgTrickleError> {
    crate::catalog::drop_watermark_group(group_name)?;
    pgrx::info!("pg_trickle: dropped watermark group '{}'", group_name);
    Ok(())
}

/// Return the current watermark state for all registered sources.
#[pg_extern(schema = "pgtrickle", name = "watermarks")]
#[allow(clippy::type_complexity)]
pub(super) fn watermarks_fn() -> TableIterator<
    'static,
    (
        name!(source_table, String),
        name!(schema_name, String),
        name!(watermark, TimestampWithTimeZone),
        name!(updated_at, TimestampWithTimeZone),
        name!(advanced_by, Option<String>),
        name!(wal_lsn, Option<String>),
    ),
> {
    let rows: Vec<_> = Spi::connect(|client| {
        let table = client
            .select(
                "SELECT c.relname::text, n.nspname::text, \
                        w.watermark, w.updated_at, w.advanced_by, w.wal_lsn_at_advance \
                 FROM pgtrickle.pgt_watermarks w \
                 JOIN pg_class c ON c.oid = w.source_relid \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 ORDER BY n.nspname, c.relname",
                None,
                &[],
            )
            .unwrap_or_else(|e| {
                pgrx::error!(
                    "{}",
                    crate::error::PgTrickleError::DiagnosticError(format!(
                        "watermarks: SPI error: {e}"
                    ))
                )
            });

        let mut out = Vec::new();
        for row in table {
            let relname: String = row.get::<String>(1).unwrap_or(None).unwrap_or_default();
            let nspname: String = row.get::<String>(2).unwrap_or(None).unwrap_or_default();
            let watermark = row.get::<TimestampWithTimeZone>(3).unwrap_or(None);
            let updated_at = row.get::<TimestampWithTimeZone>(4).unwrap_or(None);
            let advanced_by = row.get::<String>(5).unwrap_or(None);
            let wal_lsn = row.get::<String>(6).unwrap_or(None);
            if let (Some(wm), Some(ua)) = (watermark, updated_at) {
                out.push((relname, nspname, wm, ua, advanced_by, wal_lsn));
            }
        }
        out
    });

    TableIterator::new(rows)
}

/// Return all watermark group definitions.
#[pg_extern(schema = "pgtrickle", name = "watermark_groups")]
#[allow(clippy::type_complexity)]
pub(super) fn watermark_groups_fn() -> TableIterator<
    'static,
    (
        name!(group_name, String),
        name!(source_count, i32),
        name!(tolerance_secs, f64),
        name!(created_at, TimestampWithTimeZone),
    ),
> {
    let rows: Vec<_> = match crate::catalog::get_all_watermark_groups() {
        Ok(groups) => groups
            .into_iter()
            .map(|g| {
                (
                    g.group_name,
                    g.source_relids.len() as i32,
                    g.tolerance_secs,
                    g.created_at,
                )
            })
            .collect(),
        Err(e) => {
            pgrx::warning!("watermark_groups: {}", e);
            Vec::new()
        }
    };
    TableIterator::new(rows)
}

/// Return live alignment status for each watermark group.
///
/// Shows per-group lag, whether the group is currently aligned, and the
/// effective minimum watermark.
#[pg_extern(schema = "pgtrickle", name = "watermark_status")]
#[allow(clippy::type_complexity)]
pub(super) fn watermark_status_fn() -> TableIterator<
    'static,
    (
        name!(group_name, String),
        name!(min_watermark, Option<TimestampWithTimeZone>),
        name!(max_watermark, Option<TimestampWithTimeZone>),
        name!(lag_secs, Option<f64>),
        name!(aligned, bool),
        name!(sources_with_watermark, i32),
        name!(sources_total, i32),
    ),
> {
    let rows: Vec<_> = Spi::connect(|client| {
        let groups = match crate::catalog::get_all_watermark_groups() {
            Ok(g) => g,
            Err(e) => {
                pgrx::warning!("watermark_status: {}", e);
                return Vec::new();
            }
        };

        let mut out = Vec::new();
        for group in &groups {
            let total = group.source_relids.len() as i32;
            let mut wm_values: Vec<TimestampWithTimeZone> = Vec::new();

            for oid in &group.source_relids {
                if let Ok(Some(wm)) = crate::catalog::get_watermark_for_source(*oid) {
                    wm_values.push(wm.watermark);
                }
            }

            let with_wm = wm_values.len() as i32;

            if wm_values.len() < 2 {
                out.push((
                    group.group_name.clone(),
                    wm_values.first().copied(),
                    wm_values.first().copied(),
                    Some(0.0),
                    true, // trivially aligned if <2 watermarks
                    with_wm,
                    total,
                ));
                continue;
            }

            // Compute min/max/lag via SQL.
            let mut min_wm = wm_values[0];
            let mut max_wm = wm_values[0];
            for wm in &wm_values[1..] {
                let is_less: bool = client
                    .select(
                        "SELECT $1::timestamptz < $2::timestamptz",
                        None,
                        &[(*wm).into(), min_wm.into()],
                    )
                    .ok()
                    .and_then(|t| t.into_iter().next())
                    .and_then(|row| row.get::<bool>(1).ok().flatten())
                    .unwrap_or(false);
                if is_less {
                    min_wm = *wm;
                }
                let is_greater: bool = client
                    .select(
                        "SELECT $1::timestamptz > $2::timestamptz",
                        None,
                        &[(*wm).into(), max_wm.into()],
                    )
                    .ok()
                    .and_then(|t| t.into_iter().next())
                    .and_then(|row| row.get::<bool>(1).ok().flatten())
                    .unwrap_or(false);
                if is_greater {
                    max_wm = *wm;
                }
            }

            let lag: f64 = client
                .select(
                    "SELECT EXTRACT(EPOCH FROM ($1::timestamptz - $2::timestamptz))::float8",
                    None,
                    &[max_wm.into(), min_wm.into()],
                )
                .ok()
                .and_then(|t| t.into_iter().next())
                .and_then(|row| row.get::<f64>(1).ok().flatten())
                .unwrap_or(0.0);

            let aligned = lag <= group.tolerance_secs;

            out.push((
                group.group_name.clone(),
                Some(min_wm),
                Some(max_wm),
                Some(lag),
                aligned,
                with_wm,
                total,
            ));
        }
        out
    });

    TableIterator::new(rows)
}

// ── Refresh Group API (A8) ──────────────────────────────────────────────────

/// Create a user-declared refresh group for cross-source snapshot consistency.
///
/// Stream tables in the same refresh group are refreshed together in a single
/// transaction, ensuring they all see consistent source data.
///
/// # Arguments
/// - `group_name`: Unique human-readable name for the group.
/// - `members`: Array of schema-qualified stream table names to include.
/// - `isolation`: Transaction isolation level (`'read_committed'` or `'repeatable_read'`).
#[pg_extern(schema = "pgtrickle")]
pub(super) fn create_refresh_group(
    group_name: &str,
    members: Vec<String>,
    isolation: default!(&str, "'read_committed'"),
) -> Result<i32, PgTrickleError> {
    // Validate group_name is not empty
    let group_name = group_name.trim();
    if group_name.is_empty() {
        return Err(PgTrickleError::InvalidArgument(
            "refresh group name must not be empty".into(),
        ));
    }

    // Validate at least 2 members
    if members.len() < 2 {
        return Err(PgTrickleError::InvalidArgument(
            "refresh group requires at least 2 members".into(),
        ));
    }

    // Validate isolation level
    let isolation = isolation.trim().to_lowercase();
    if isolation != "read_committed" && isolation != "repeatable_read" {
        return Err(PgTrickleError::InvalidArgument(format!(
            "invalid isolation level: '{}' (expected 'read_committed' or 'repeatable_read')",
            isolation
        )));
    }

    // Resolve member names to OIDs and validate they are stream tables
    let mut member_oids = Vec::with_capacity(members.len());
    for member in &members {
        let (schema, table_name) = parse_qualified_name(member)?;
        let st = StreamTableMeta::get_by_name(&schema, &table_name).map_err(|e| match e {
            PgTrickleError::NotFound(_) => {
                PgTrickleError::InvalidArgument(format!("stream table '{}' does not exist", member))
            }
            other => other,
        })?;
        member_oids.push(st.pgt_relid);
    }

    // Validate no member appears in another group
    for (i, oid) in member_oids.iter().enumerate() {
        if let Some(existing_group) = crate::catalog::find_group_containing_member(*oid)? {
            return Err(PgTrickleError::InvalidArgument(format!(
                "stream table '{}' is already a member of refresh group '{}'",
                members[i], existing_group
            )));
        }
    }

    // Insert the group
    let group_id = crate::catalog::create_refresh_group(group_name, &member_oids, &isolation)?;

    // Signal DAG rebuild so the scheduler picks up the new group
    shmem::signal_dag_rebuild();

    pgrx::info!(
        "pg_trickle: created refresh group '{}' (id={}) with {} members, isolation={}",
        group_name,
        group_id,
        members.len(),
        isolation
    );
    Ok(group_id)
}

/// Drop a refresh group by name.
#[pg_extern(schema = "pgtrickle")]
pub(super) fn drop_refresh_group(group_name: &str) -> Result<(), PgTrickleError> {
    crate::catalog::drop_refresh_group(group_name)?;

    // Signal DAG rebuild so the scheduler removes the group
    shmem::signal_dag_rebuild();

    pgrx::info!("pg_trickle: dropped refresh group '{}'", group_name);
    Ok(())
}

/// Return all user-declared refresh groups with member details.
#[pg_extern(schema = "pgtrickle", name = "refresh_groups")]
#[allow(clippy::type_complexity)]
pub(super) fn refresh_groups_fn() -> TableIterator<
    'static,
    (
        name!(group_id, i32),
        name!(group_name, String),
        name!(member_count, i32),
        name!(isolation, String),
        name!(created_at, TimestampWithTimeZone),
    ),
> {
    let rows: Vec<_> = match crate::catalog::get_all_refresh_groups() {
        Ok(groups) => groups
            .into_iter()
            .map(|g| {
                (
                    g.group_id,
                    g.group_name,
                    g.member_oids.len() as i32,
                    g.isolation,
                    g.created_at,
                )
            })
            .collect(),
        Err(e) => {
            pgrx::warning!("refresh_groups: {}", e);
            Vec::new()
        }
    };
    TableIterator::new(rows)
}

// ── PUB-2 (v0.25.0): Multi-DB worker allocation status ─────────────────

/// PUB-2: Return per-database worker allocation status.
///
/// Reports the current running-worker count, effective worker quota, and
/// queued-job count for the current database. Useful for capacity planning
/// and diagnosing scheduler starvation when multiple databases share the
/// cluster-wide worker pool.
///
/// Columns:
/// - `db_name`: The current database name.
/// - `workers_used`: Number of scheduler jobs currently RUNNING.
/// - `workers_quota`: Effective per-database worker quota (from GUC settings).
/// - `workers_queued`: Number of scheduler jobs currently QUEUED.
/// - `cluster_active`: Cluster-wide active worker count (across all DBs).
/// - `cluster_max`: Cluster-wide maximum worker count.
#[pg_extern(schema = "pgtrickle", name = "worker_allocation_status")]
#[allow(clippy::type_complexity)]
pub(super) fn worker_allocation_status_fn() -> TableIterator<
    'static,
    (
        name!(db_name, String),
        name!(workers_used, i64),
        name!(workers_quota, i64),
        name!(workers_queued, i64),
        name!(cluster_active, i64),
        name!(cluster_max, i64),
    ),
> {
    use crate::shmem;

    // Count RUNNING and QUEUED jobs in the current database.
    let (running, queued) = Spi::connect(|client| {
        let running: i64 = client
            .select(
                "SELECT COUNT(*) FROM pgtrickle.pgt_scheduler_jobs WHERE status = 'RUNNING'",
                None,
                &[],
            )
            .ok()
            .and_then(|r| {
                if r.is_empty() {
                    None
                } else {
                    r.first().get::<i64>(1).ok().flatten()
                }
            })
            .unwrap_or(0);
        let queued: i64 = client
            .select(
                "SELECT COUNT(*) FROM pgtrickle.pgt_scheduler_jobs WHERE status = 'QUEUED'",
                None,
                &[],
            )
            .ok()
            .and_then(|r| {
                if r.is_empty() {
                    None
                } else {
                    r.first().get::<i64>(1).ok().flatten()
                }
            })
            .unwrap_or(0);
        (running, queued)
    });

    let cluster_active = shmem::active_worker_count() as i64;
    let max_cluster = crate::config::pg_trickle_max_dynamic_refresh_workers().max(1) as i64;

    // Compute effective per-DB quota using the same logic as the scheduler.
    let quota = crate::scheduler::compute_per_db_quota(
        crate::config::pg_trickle_per_database_worker_quota(),
        crate::config::pg_trickle_max_concurrent_refreshes(),
        max_cluster as u32,
        cluster_active as u32,
    ) as i64;

    let db_name = Spi::get_one::<String>("SELECT current_database()")
        .unwrap_or(None)
        .unwrap_or_else(|| "unknown".to_string());

    TableIterator::new(vec![(
        db_name,
        running,
        quota,
        queued,
        cluster_active,
        max_cluster,
    )])
}

// ── A46-4/A46-5 (v0.45.0): Preflight and worker pool status ──────────────

/// A46-4: Run a pre-deployment readiness check for pg_trickle.
///
/// Checks and reports:
/// - `max_worker_processes` vs required slots
/// - `max_replication_slots` vs WAL-eligible sources
/// - `wal_level` is `logical` (if WAL CDC sources exist)
/// - `shared_preload_libraries` includes `pg_trickle`
/// - Scheduler is running and enabled
/// - Invalidation ring overflow count (capacity planning signal)
/// - Citus worker failure total (operational signal)
///
/// Returns a JSON string with one entry per check: `pass` (bool), `check` (name),
/// `detail` (human-readable message). An overall `ok` field is `true` only when
/// all checks pass.
#[pg_extern(schema = "pgtrickle")]
pub(super) fn preflight() -> String {
    use crate::config;
    use crate::shmem;

    // Check 1: shared_preload_libraries includes pg_trickle
    let preload_ok =
        Spi::get_one::<String>("SELECT current_setting('shared_preload_libraries', true)")
            .unwrap_or(None)
            .map(|s| s.contains("pg_trickle"))
            .unwrap_or(false);
    let shared_preload_libraries = serde_json::json!({
        "ok": preload_ok,
        "detail": if preload_ok {
            "pg_trickle is in shared_preload_libraries".to_string()
        } else {
            "pg_trickle is NOT in shared_preload_libraries — add it and restart".to_string()
        }
    });

    // Check 2: Scheduler is running
    let scheduler_running = shmem::scheduler_running();
    let scheduler_enabled = config::pg_trickle_enabled();
    let sched_ok = scheduler_running && scheduler_enabled;
    let scheduler_running_check = serde_json::json!({
        "ok": sched_ok,
        "detail": if !scheduler_enabled {
            "pg_trickle.enabled = off — scheduler is disabled".to_string()
        } else if !scheduler_running {
            "Scheduler background worker is not running — check PostgreSQL logs".to_string()
        } else {
            "Scheduler is running and enabled".to_string()
        }
    });

    // Check 3: persisted row-identity encoding matches this binary. Unknown
    // or stale catalog values are intentionally a hard failure for
    // incremental maintenance; the migration leaves them conservative.
    let expected_identity_version = crate::hash::CURRENT_ROW_IDENTITY_VERSION;
    let row_identity_check = match Spi::get_one_with_args::<bool>(
        "SELECT NOT EXISTS (\
             SELECT 1 FROM pgtrickle.pgt_stream_tables \
             WHERE row_identity_version IS DISTINCT FROM $1\
         ) AND NOT EXISTS (\
             SELECT 1 FROM pgtrickle.pgt_change_buffers \
             WHERE row_identity_version IS DISTINCT FROM $1\
         )",
        &[expected_identity_version.into()],
    ) {
        Ok(Some(true)) => serde_json::json!({
            "ok": true,
            "detail": format!("all persisted row-identity versions are {}", expected_identity_version)
        }),
        Ok(Some(false)) => serde_json::json!({
            "ok": false,
            "detail": format!(
                "one or more stream tables or CDC buffers do not use row-identity version {}; \
                 run the documented protected FULL/rebuild procedure before enabling incremental maintenance",
                expected_identity_version
            )
        }),
        Ok(None) | Err(_) => serde_json::json!({
            "ok": false,
            "detail": "row-identity catalog columns could not be verified; incremental maintenance is not safe"
        }),
    };

    // Check 4: max_worker_processes sufficiency
    let (max_workers, active_workers) = Spi::connect(|client| {
        let max: i64 = client
            .select(
                "SELECT current_setting('max_worker_processes')::int",
                None,
                &[],
            )
            .ok()
            .and_then(|r| {
                if r.is_empty() {
                    None
                } else {
                    r.first().get::<i64>(1).ok().flatten()
                }
            })
            .unwrap_or(8);
        let active: i64 = shmem::active_worker_count() as i64;
        (max, active)
    });
    // Recommended minimum: 8 base + worker pool size
    let pool_size = crate::config::pg_trickle_max_dynamic_refresh_workers() as i64;
    let recommended_min = 8 + pool_size;
    let workers_ok = max_workers >= recommended_min;
    let max_worker_processes = serde_json::json!({
        "ok": workers_ok,
        "detail": format!(
            "max_worker_processes={}, recommended_min={} (8 + pool_size={}), active_pg_trickle_workers={}",
            max_workers, recommended_min, pool_size, active_workers
        )
    });

    // Check 4: wal_level (advisory — only warn if WAL sources exist)
    let wal_level_val = Spi::get_one::<String>("SELECT current_setting('wal_level', true)")
        .unwrap_or(None)
        .unwrap_or_else(|| "unknown".to_string());
    // pgt_dependencies.cdc_mode stores the active CDC mechanism per source edge
    let wal_source_count: i64 = Spi::get_one::<i64>(
        "SELECT COUNT(DISTINCT source_relid) FROM pgtrickle.pgt_dependencies WHERE cdc_mode = 'WAL'",
    )
    .unwrap_or(None)
    .unwrap_or(0);
    let wal_ok = wal_level_val == "logical" || wal_source_count == 0;
    let wal_level = serde_json::json!({
        "ok": wal_ok,
        "detail": format!(
            "wal_level={}, wal_cdc_sources={} — {}",
            wal_level_val,
            wal_source_count,
            if wal_ok { "OK" } else { "wal_level must be 'logical' for WAL-mode CDC — run: ALTER SYSTEM SET wal_level = logical" }
        )
    });

    // Check 5: max_replication_slots vs WAL sources
    let max_slots: i64 =
        Spi::get_one::<i64>("SELECT current_setting('max_replication_slots')::int")
            .unwrap_or(None)
            .unwrap_or(0);
    let used_slots: i64 = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM pg_catalog.pg_replication_slots WHERE plugin IS NOT NULL",
    )
    .unwrap_or(None)
    .unwrap_or(0);
    let slots_available = max_slots - used_slots;
    let slots_ok = wal_source_count == 0 || slots_available > 0;
    let replication_slots = serde_json::json!({
        "ok": slots_ok,
        "detail": format!(
            "max_replication_slots={}, used={}, available={}, wal_sources={}",
            max_slots, used_slots, slots_available, wal_source_count
        )
    });

    // Check 6: Invalidation ring overflow (capacity signal, not a hard failure)
    let ring_overflows = shmem::invalidation_ring_overflow_count();
    let ring_capacity = config::pg_trickle_invalidation_ring_capacity();
    let ring_ok = ring_overflows == 0;
    let invalidation_ring_overflow = serde_json::json!({
        "ok": ring_ok,
        "count": ring_overflows,
        "detail": format!(
            "ring_capacity={}, overflow_count={} — {}",
            ring_capacity,
            ring_overflows,
            if ring_ok {
                "No overflows since startup".to_string()
            } else {
                format!("{} overflows detected — consider increasing pg_trickle.invalidation_ring_capacity", ring_overflows)
            }
        )
    });

    // Check 7: Citus failure total (advisory signal)
    let citus_failures = shmem::citus_worker_failure_total();
    let citus_ok = citus_failures == 0;
    let citus_worker_failures = serde_json::json!({
        "ok": true, // always advisory
        "detail": format!(
            "citus_worker_failure_total={} — {}",
            citus_failures,
            if citus_ok { "No Citus worker threshold crossings since startup" }
            else { "Citus worker failure threshold(s) crossed — check logs for COORD-14 warnings" }
        )
    });

    serde_json::json!({
        "shared_preload_libraries": shared_preload_libraries,
        "scheduler_running": scheduler_running_check,
        "row_identity": row_identity_check,
        "max_worker_processes": max_worker_processes,
        "wal_level": wal_level,
        "replication_slots": replication_slots,
        "invalidation_ring_overflow": invalidation_ring_overflow,
        "citus_worker_failures": citus_worker_failures,
    })
    .to_string()
}

// ── CACHE-3 (v0.25.0): Manual cache flush ──────────────────────────────

/// CACHE-3: Flush all delta-template cache levels in the current database.
///
/// Clears:
/// - **L1** (thread-local): The in-process `MERGE_TEMPLATE_CACHE` for the
///   current backend. Forces a re-parse on the next refresh in this session.
/// - **L2** (catalog table): `pgtrickle.pgt_template_cache` is truncated.
///   All backends will miss the L2 cache on their next cache-miss and will
///   re-populate it from a fresh DVM parse.
/// - **Generation bump**: `CACHE_GENERATION` is incremented so that all
///   other connected backends detect the invalidation on their next refresh
///   and flush their own L1 caches.
///
/// Returns the number of L2 entries that were removed.
///
/// Use during debugging, emergency migration rollback, or after a query
/// definition change that was not captured by the normal DDL invalidation path.
#[pg_extern(schema = "pgtrickle")]
pub(super) fn clear_caches() -> i64 {
    use crate::shmem;

    // Count L2 entries before clearing.
    let l2_count: i64 = Spi::get_one::<i64>("SELECT COUNT(*) FROM pgtrickle.pgt_template_cache")
        .unwrap_or(Some(0))
        .unwrap_or(0);

    // Flush L1: thread-local MERGE_TEMPLATE_CACHE in the current backend.
    crate::refresh::flush_local_template_cache();

    // P-2: Flush thread-local volatility caches so custom function/operator
    // changes take effect immediately without a backend restart.
    crate::dvm::parser::flush_volatility_cache();

    // Flush L2: truncate the shared catalog cache table.
    crate::template_cache::invalidate_all();

    // Advance CACHE_GENERATION so all other backends detect the invalidation
    // on their next refresh and flush their own L1 caches.
    shmem::bump_cache_generation();

    pgrx::info!(
        "pg_trickle: clear_caches() flushed L1 (thread-local) + L2 ({} entries) + bumped CACHE_GENERATION",
        l2_count
    );

    l2_count
}

/// VP-3 (v0.47.0): Vector status view — embedding lag, ANN age, drift percentage.
///
/// Returns one row per stream table that has a `post_refresh_action` other than
/// 'none', or that has any ANN-relevant index on its storage table. Operators
/// can use this to monitor the health of vector/embedding stream tables.
#[pg_extern(schema = "pgtrickle", name = "vector_status")]
#[allow(clippy::type_complexity)]
pub(super) fn vector_status() -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(post_refresh_action, String),
        name!(reindex_drift_threshold, Option<f64>),
        name!(rows_changed_since_last_reindex, i64),
        name!(last_reindex_at, Option<TimestampWithTimeZone>),
        name!(data_timestamp, Option<TimestampWithTimeZone>),
        name!(embedding_lag, Option<pgrx::datum::Interval>),
        name!(estimated_rows, Option<i64>),
        name!(drift_pct, Option<f64>),
    ),
> {
    let rows: Vec<_> = Spi::connect(|client| {
        let result = client
            .select(
                "SELECT \
                   s.pgt_schema || '.' || s.pgt_name AS name, \
                   COALESCE(s.post_refresh_action, 'none') AS post_refresh_action, \
                   s.reindex_drift_threshold, \
                   COALESCE(s.rows_changed_since_last_reindex, 0) AS rows_changed_since_last_reindex, \
                   s.last_reindex_at, \
                   s.data_timestamp, \
                   now() - s.data_timestamp AS embedding_lag, \
                   c.reltuples::BIGINT AS estimated_rows, \
                   CASE \
                     WHEN c.reltuples > 0 \
                     THEN ROUND( \
                       ((COALESCE(s.rows_changed_since_last_reindex, 0)::float / c.reltuples) * 100.0)::numeric, \
                       2)::float8 \
                     ELSE NULL \
                   END AS drift_pct \
                 FROM pgtrickle.pgt_stream_tables s \
                 JOIN pg_class c ON c.oid = s.pgt_relid \
                 WHERE COALESCE(s.post_refresh_action, 'none') <> 'none' \
                 ORDER BY s.pgt_schema, s.pgt_name",
                None,
                &[],
            )
            .unwrap_or_else(|e| {
                pgrx::error!(
                    "{}",
                    crate::error::PgTrickleError::DiagnosticError(format!(
                        "vector_status: SPI select failed: {e}"
                    ))
                )
            });

        let mut out = Vec::new();
        for row in result {
            let name = row.get::<String>(1).unwrap_or(None).unwrap_or_default();
            let post_refresh_action = row.get::<String>(2).unwrap_or(None).unwrap_or_default();
            let reindex_drift_threshold = row.get::<f64>(3).unwrap_or(None);
            let rows_changed = row.get::<i64>(4).unwrap_or(None).unwrap_or(0);
            let last_reindex_at = row.get::<TimestampWithTimeZone>(5).unwrap_or(None);
            let data_ts = row.get::<TimestampWithTimeZone>(6).unwrap_or(None);
            let embedding_lag = row.get::<pgrx::datum::Interval>(7).unwrap_or(None);
            let estimated_rows = row.get::<i64>(8).unwrap_or(None);
            let drift_pct = row.get::<f64>(9).unwrap_or(None);
            out.push((
                name,
                post_refresh_action,
                reindex_drift_threshold,
                rows_changed,
                last_reindex_at,
                data_ts,
                embedding_lag,
                estimated_rows,
                drift_pct,
            ));
        }
        out
    });

    TableIterator::new(rows)
}

// ── TEST-7 (v0.24.0): Unit tests for diagnostics pure-logic helpers ─────

// ── QW-2 (v0.81.0): GUC tune recommendations ────────────────────────────

// ── QW-1 (v0.81.0): Commit latency statistics ────────────────────────────

/// QW-1 (v0.81.0): Return per-stream-table commit-to-visible latency
/// statistics computed from `pgtrickle.pgt_refresh_history`.
///
/// Each row corresponds to one stream table and reports the minimum,
/// median (p50), p95, and maximum observed latency (in milliseconds)
/// between the scheduler tick start and the refresh end time.
///
/// When `pg_trickle.commit_timestamp_tracking = off` (the default), the
/// latency is estimated as `end_time - start_time` (refresh duration only).
/// Enabling `commit_timestamp_tracking` with `track_commit_timestamp = on`
/// in `postgresql.conf` would allow the extension to compute the true
/// commit-to-visible wall-clock latency; this is planned for a future
/// release.
///
/// Returns rows only for stream tables that have at least one completed
/// refresh in the history table.
#[pg_extern(schema = "pgtrickle", stable, parallel_safe)]
#[allow(clippy::type_complexity)]
pub(super) fn commit_latency_stats() -> TableIterator<
    'static,
    (
        name!(pgt_schema, String),
        name!(pgt_name, String),
        name!(samples, i64),
        name!(min_ms, f64),
        name!(p50_ms, f64),
        name!(p95_ms, f64),
        name!(max_ms, f64),
        name!(tracking_mode, String),
    ),
> {
    let tracking_enabled = crate::config::pg_trickle_commit_timestamp_tracking();
    let tracking_mode = if tracking_enabled {
        "commit_timestamp"
    } else {
        "refresh_duration"
    };

    // Query refresh latency stats per stream table from history.
    // For now, latency = EXTRACT(EPOCH FROM (end_time - start_time)) * 1000 ms.
    let rows: Vec<(String, String, i64, f64, f64, f64, f64, String)> = Spi::connect(|client| {
        let sql = "\
            SELECT \
                s.pgt_schema::text, \
                s.pgt_name::text, \
                count(*)::bigint AS samples, \
                min(EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000)::float8 AS min_ms, \
                percentile_disc(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000)::float8 AS p50_ms, \
                percentile_disc(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000)::float8 AS p95_ms, \
                max(EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000)::float8 AS max_ms \
            FROM pgtrickle.pgt_refresh_history h \
            JOIN pgtrickle.pgt_stream_tables s ON s.pgt_id = h.pgt_id \
            WHERE h.status = 'COMPLETED' \
              AND h.end_time IS NOT NULL \
              AND h.start_time IS NOT NULL \
            GROUP BY s.pgt_schema, s.pgt_name \
            ORDER BY s.pgt_schema, s.pgt_name";

        let tbl = client.select(sql, None, &[]).map_err(|_e| {
            pgrx::spi::SpiError::InvalidPosition
        })?;

        let mut out = Vec::new();
        for row in tbl {
            let schema = row.get::<String>(1).ok().flatten().unwrap_or_default();
            let name = row.get::<String>(2).ok().flatten().unwrap_or_default();
            let samples = row.get::<i64>(3).ok().flatten().unwrap_or(0);
            let min_ms = row.get::<f64>(4).ok().flatten().unwrap_or(0.0);
            let p50_ms = row.get::<f64>(5).ok().flatten().unwrap_or(0.0);
            let p95_ms = row.get::<f64>(6).ok().flatten().unwrap_or(0.0);
            let max_ms = row.get::<f64>(7).ok().flatten().unwrap_or(0.0);
            out.push((schema, name, samples, min_ms, p50_ms, p95_ms, max_ms, tracking_mode.to_string()));
        }
        Ok::<Vec<_>, pgrx::spi::SpiError>(out)
    }).unwrap_or_default();

    TableIterator::new(rows)
}

/// QW-2 (v0.81.0): Returns a set of GUC tuning recommendations based on
/// recent refresh history and current configuration.
///
/// Each row describes one recommendation:
/// - `guc_name`: the GUC parameter name (e.g. `pg_trickle.merge_work_mem_mb`)
/// - `current_value`: the current value as a string
/// - `recommended_value`: the suggested value
/// - `reason`: a human-readable explanation
///
/// Returns an empty result set when all observed metrics are within healthy
/// ranges. This function is read-only and safe to call at any time.
#[pg_extern(schema = "pgtrickle", stable, parallel_safe)]
pub(super) fn tune_recommendations() -> TableIterator<
    'static,
    (
        name!(guc_name, String),
        name!(current_value, String),
        name!(recommended_value, String),
        name!(reason, String),
    ),
> {
    let mut rows: Vec<(String, String, String, String)> = Vec::new();

    // ── Recommendation 1: merge_batch_size based on p99 delta size ──────
    // If the p99 delta row count in pgt_refresh_history exceeds 100 000 and
    // merge_batch_size is 0 (disabled), recommend enabling it.
    let merge_batch_size = crate::config::pg_trickle_merge_batch_size();
    let p99_delta: Option<i64> = Spi::get_one::<i64>(
        "SELECT percentile_disc(0.99) WITHIN GROUP (ORDER BY delta_rows) \
         FROM pgtrickle.pgt_refresh_history \
         WHERE delta_rows IS NOT NULL AND delta_rows > 0",
    )
    .unwrap_or(None);
    if merge_batch_size == 0
        && let Some(p99) = p99_delta.filter(|&p| p > 100_000)
    {
        rows.push((
            "pg_trickle.merge_batch_size".to_string(),
            "0 (disabled)".to_string(),
            "50000".to_string(),
            format!(
                "p99 delta size is {} rows — enabling chunked MERGE (QW-9) \
                 will route large deltas through DELETE+INSERT, \
                 reducing peak memory and MERGE join cost.",
                p99
            ),
        ));
    }

    // ── Recommendation 2: merge_work_mem_mb when p95 latency is high ────
    // If p95 refresh latency exceeds 10 s and work_mem_mb is already >= 512,
    // suggest lowering it (high work_mem can cause poor planner choices).
    let work_mem_mb = crate::config::pg_trickle_delta_work_mem_cap_mb();
    let p95_ms: Option<f64> = Spi::get_one::<f64>(
        "SELECT percentile_disc(0.95) WITHIN GROUP (ORDER BY duration_ms) \
         FROM pgtrickle.pgt_refresh_history \
         WHERE duration_ms IS NOT NULL AND status = 'COMPLETED'",
    )
    .unwrap_or(None);
    if work_mem_mb > 512
        && let Some(p95) = p95_ms.filter(|&p| p > 10_000.0)
    {
        rows.push((
            "pg_trickle.delta_work_mem_cap_mb".to_string(),
            work_mem_mb.to_string(),
            "256".to_string(),
            format!(
                "p95 refresh latency is {:.0}ms with delta_work_mem_cap_mb={}; \
                 a very high work_mem setting can cause the planner to choose \
                 suboptimal hash joins. Try reducing to 256 MB.",
                p95, work_mem_mb
            ),
        ));
    }

    // ── Recommendation 3: self_heal_oom when OOM errors are in history ───
    let self_heal_oom = crate::config::pg_trickle_self_heal_oom();
    if !self_heal_oom {
        let oom_count: Option<i64> = Spi::get_one::<i64>(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history \
             WHERE last_error_message ILIKE '%out of memory%' \
               AND refreshed_at > now() - INTERVAL '7 days'",
        )
        .unwrap_or(None);
        if oom_count.unwrap_or(0) > 0 {
            rows.push((
                "pg_trickle.self_heal_oom".to_string(),
                "off".to_string(),
                "on".to_string(),
                format!(
                    "Detected {} OOM error(s) in the past 7 days. Enabling \
                     self_heal_oom will intercept these errors before they \
                     count toward auto-suspension, preventing unnecessary \
                     stream table downtime.",
                    oom_count.unwrap_or(0)
                ),
            ));
        }
    }

    // ── Recommendation 4: concurrent_refreshes vs. worker_count ─────────
    let max_concurrent = crate::config::pg_trickle_max_concurrent_refreshes();
    let worker_count = crate::config::pg_trickle_worker_pool_size();
    if max_concurrent > worker_count && worker_count > 0 {
        rows.push((
            "pg_trickle.max_concurrent_refreshes".to_string(),
            max_concurrent.to_string(),
            worker_count.to_string(),
            format!(
                "max_concurrent_refreshes ({}) exceeds worker_count ({}). \
                 The extra concurrency slots will never be used. \
                 Lower max_concurrent_refreshes to match worker_count.",
                max_concurrent, worker_count
            ),
        ));
    }

    TableIterator::new(rows)
}

// ── QW-3 (v0.81.0): preview_stream_table ─────────────────────────────────

/// QW-3 (v0.81.0): Analyse a query to preview what a stream table would
/// look like if created with `create_stream_table`, without creating anything.
///
/// Returns a set of key/value analysis rows describing:
/// - `dvm_supported`: whether the DVM engine can handle this query
/// - `refresh_strategy`: the planned refresh strategy (DIFFERENTIAL / FULL)
/// - `source_tables`: comma-separated list of detected source table names
/// - `op_tree_root`: the root operator type of the parsed operator tree
/// - `warnings`: any advisory warnings from the DVM parser
/// - `complexity_estimate`: a rough complexity classification (depth)
///
/// # Example
/// ```sql
/// SELECT * FROM pgtrickle.preview_stream_table(
///     'SELECT o.id, SUM(i.amount) FROM orders o JOIN items i ON o.id = i.order_id GROUP BY o.id'
/// );
/// ```
#[pg_extern(schema = "pgtrickle")]
pub(super) fn preview_stream_table(
    query: &str,
) -> TableIterator<'static, (name!(property, String), name!(value, String))> {
    let mut rows: Vec<(String, String)> = Vec::new();

    let parse_result = crate::dvm::parser::parse_defining_query_full(query);

    match parse_result {
        Err(e) => {
            rows.push(("dvm_supported".to_string(), "false".to_string()));
            rows.push(("refresh_strategy".to_string(), "FULL".to_string()));
            rows.push(("parse_error".to_string(), e.to_string()));
            rows.push(("source_tables".to_string(), String::new()));
            rows.push(("op_tree_root".to_string(), "unknown".to_string()));
            rows.push(("warnings".to_string(), String::new()));
            rows.push(("complexity_estimate".to_string(), "unknown".to_string()));
        }
        Ok(result) => {
            let ivm_ok = crate::dvm::parser::check_ivm_support_with_registry(&result);
            let dvm_supported = ivm_ok.is_ok();
            rows.push(("dvm_supported".to_string(), dvm_supported.to_string()));
            rows.push((
                "refresh_strategy".to_string(),
                if dvm_supported {
                    "DIFFERENTIAL"
                } else {
                    "FULL"
                }
                .to_string(),
            ));
            if let Err(ref ivm_err) = ivm_ok {
                rows.push(("dvm_rejection_reason".to_string(), ivm_err.to_string()));
            }

            let sources = preview_collect_sources(&result.tree);
            rows.push(("source_tables".to_string(), sources.join(", ")));

            rows.push((
                "op_tree_root".to_string(),
                preview_root_type(&result.tree).to_string(),
            ));

            let warnings_str = result.warnings.join("; ");
            rows.push(("warnings".to_string(), warnings_str));

            if result.has_recursion {
                rows.push((
                    "recursive_cte".to_string(),
                    "true — FULL refresh only".to_string(),
                ));
            }

            let depth = preview_depth(&result.tree);
            let complexity = if depth <= 2 {
                "simple"
            } else if depth <= 5 {
                "moderate"
            } else {
                "complex"
            };
            rows.push((
                "complexity_estimate".to_string(),
                format!("{complexity} (depth={depth})"),
            ));
        }
    }

    TableIterator::new(rows)
}

fn preview_collect_sources(tree: &crate::dvm::parser::OpTree) -> Vec<String> {
    let mut sources = Vec::new();
    preview_collect_sources_rec(tree, &mut sources);
    sources.sort();
    sources.dedup();
    sources
}

fn preview_collect_sources_rec(tree: &crate::dvm::parser::OpTree, out: &mut Vec<String>) {
    use crate::dvm::parser::OpTree;
    match tree {
        OpTree::Scan { table_name, .. } => out.push(table_name.clone()),
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. }
        | OpTree::SemiJoin { left, right, .. }
        | OpTree::AntiJoin { left, right, .. }
        | OpTree::Intersect { left, right, .. }
        | OpTree::Except { left, right, .. } => {
            preview_collect_sources_rec(left, out);
            preview_collect_sources_rec(right, out);
        }
        OpTree::UnionAll { children } => {
            for child in children {
                preview_collect_sources_rec(child, out);
            }
        }
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Distinct { child }
        | OpTree::Aggregate { child, .. }
        | OpTree::Window { child, .. }
        | OpTree::Subquery { child, .. }
        | OpTree::LateralSubquery { child, .. }
        | OpTree::LateralFunction { child, .. }
        | OpTree::ScalarSubquery { child, .. } => preview_collect_sources_rec(child, out),
        OpTree::RecursiveCte {
            base, recursive, ..
        } => {
            preview_collect_sources_rec(base, out);
            preview_collect_sources_rec(recursive, out);
        }
        OpTree::CteScan { .. }
        | OpTree::RecursiveSelfRef { .. }
        | OpTree::ConstantSelect { .. } => {}
    }
}

fn preview_root_type(tree: &crate::dvm::parser::OpTree) -> &'static str {
    use crate::dvm::parser::OpTree;
    match tree {
        OpTree::Scan { .. } => "Scan",
        OpTree::InnerJoin { .. } => "InnerJoin",
        OpTree::LeftJoin { .. } => "LeftJoin",
        OpTree::FullJoin { .. } => "FullJoin",
        OpTree::SemiJoin { .. } => "SemiJoin",
        OpTree::AntiJoin { .. } => "AntiJoin",
        OpTree::Filter { .. } => "Filter",
        OpTree::Project { .. } => "Project",
        OpTree::Distinct { .. } => "Distinct",
        OpTree::Aggregate { .. } => "Aggregate",
        OpTree::Window { .. } => "Window",
        OpTree::UnionAll { .. } => "UnionAll",
        OpTree::Intersect { .. } => "Intersect",
        OpTree::Except { .. } => "Except",
        OpTree::Subquery { .. } => "Subquery",
        OpTree::LateralSubquery { .. } => "LateralSubquery",
        OpTree::LateralFunction { .. } => "LateralFunction",
        OpTree::CteScan { .. } => "CteScan",
        OpTree::ScalarSubquery { .. } => "ScalarSubquery",
        OpTree::RecursiveCte { .. } => "RecursiveCte",
        OpTree::RecursiveSelfRef { .. } => "RecursiveSelfRef",
        OpTree::ConstantSelect { .. } => "ConstantSelect",
    }
}

fn preview_depth(tree: &crate::dvm::parser::OpTree) -> usize {
    use crate::dvm::parser::OpTree;
    match tree {
        OpTree::Scan { .. }
        | OpTree::CteScan { .. }
        | OpTree::RecursiveSelfRef { .. }
        | OpTree::ConstantSelect { .. } => 1,
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Distinct { child }
        | OpTree::Aggregate { child, .. }
        | OpTree::Window { child, .. }
        | OpTree::Subquery { child, .. }
        | OpTree::LateralSubquery { child, .. }
        | OpTree::ScalarSubquery { child, .. }
        | OpTree::LateralFunction { child, .. } => 1 + preview_depth(child),
        OpTree::RecursiveCte {
            base, recursive, ..
        } => 1 + preview_depth(base).max(preview_depth(recursive)),
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. }
        | OpTree::SemiJoin { left, right, .. }
        | OpTree::AntiJoin { left, right, .. }
        | OpTree::Intersect { left, right, .. }
        | OpTree::Except { left, right, .. } => 1 + preview_depth(left).max(preview_depth(right)),
        OpTree::UnionAll { children } => 1 + children.iter().map(preview_depth).max().unwrap_or(0),
    }
}

#[cfg(test)]
mod tests {
    use super::{SchemaVersionAuditRow, build_migrate_report, compare_release_versions};
    use std::cmp::Ordering;

    #[test]
    fn test_version_returns_cargo_version() {
        let v = env!("CARGO_PKG_VERSION");
        assert!(!v.is_empty());
        assert!(v.contains('.'), "version should contain dots: {v}");
    }

    #[test]
    fn test_version_is_not_placeholder() {
        let v = env!("CARGO_PKG_VERSION");
        assert_ne!(v, "0.0.0");
        assert_ne!(v, "@CARGO_VERSION@");
    }

    #[test]
    fn test_compare_release_versions_orders_numeric_versions() {
        assert_eq!(
            compare_release_versions("0.83.0", "0.84.0"),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare_release_versions("0.84.0", "0.84.0"),
            Some(Ordering::Equal)
        );
        assert_eq!(
            compare_release_versions("0.85.0", "0.84.0"),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn test_migrate_report_marks_matching_versions_ok() {
        let report = build_migrate_report(
            "0.84.0",
            Some("0.84.0"),
            Some(&SchemaVersionAuditRow {
                version: "0.84.0".into(),
                applied_at: Some("2026-08-16 09:00:00+00".into()),
                description: Some("current".into()),
            }),
        );

        assert_eq!(report["status"], "ok");
        assert_eq!(report["up_to_date"], true);
        assert_eq!(report["remediation_sql"], serde_json::Value::Null);
    }

    #[test]
    fn test_migrate_report_requires_alter_for_older_sql() {
        let report = build_migrate_report(
            "0.84.0",
            Some("0.83.0"),
            Some(&SchemaVersionAuditRow {
                version: "0.83.0".into(),
                applied_at: None,
                description: None,
            }),
        );

        assert_eq!(report["status"], "alter_extension_required");
        assert_eq!(
            report["remediation_sql"],
            "ALTER EXTENSION pg_trickle UPDATE TO '0.84.0';"
        );
    }

    #[test]
    fn test_migrate_report_requires_restart_for_newer_sql() {
        let report = build_migrate_report(
            "0.83.0",
            Some("0.84.0"),
            Some(&SchemaVersionAuditRow {
                version: "0.84.0".into(),
                applied_at: None,
                description: None,
            }),
        );

        assert_eq!(report["status"], "restart_required");
        assert_eq!(report["remediation_sql"], serde_json::Value::Null);
    }

    #[test]
    fn test_migrate_report_is_read_only_diagnostic_for_schema_drift() {
        let report = build_migrate_report(
            "0.84.0",
            Some("0.84.0"),
            Some(&SchemaVersionAuditRow {
                version: "0.83.0".into(),
                applied_at: None,
                description: Some("stale".into()),
            }),
        );

        assert_eq!(report["status"], "schema_version_mismatch");
        assert_eq!(report["up_to_date"], false);
        assert_eq!(report["remediation_sql"], serde_json::Value::Null);
        assert!(
            report["remediation"]
                .as_str()
                .unwrap_or_default()
                .contains("read-only")
        );
    }

    #[test]
    fn test_explain_format_text_is_default() {
        let format = "text";
        assert!(["text", "json", "yaml", "xml"].contains(&format));
    }

    #[test]
    fn test_explain_format_json_is_valid() {
        let format = "json";
        assert!(["text", "json", "yaml", "xml"].contains(&format));
    }

    #[test]
    fn test_explain_format_unknown_is_rejected() {
        let format = "binary";
        assert!(!["text", "json", "yaml", "xml"].contains(&format));
    }

    #[test]
    fn test_parse_duration_helpers_seconds() {
        use crate::api::helpers::parse_duration;
        assert_eq!(parse_duration("30s").ok(), Some(30));
    }

    #[test]
    fn test_parse_duration_helpers_minutes() {
        use crate::api::helpers::parse_duration;
        assert_eq!(parse_duration("5m").ok(), Some(300));
    }

    #[test]
    fn test_parse_duration_helpers_hours() {
        use crate::api::helpers::parse_duration;
        assert_eq!(parse_duration("2h").ok(), Some(7200));
    }

    #[test]
    fn test_parse_duration_helpers_days() {
        use crate::api::helpers::parse_duration;
        assert_eq!(parse_duration("1d").ok(), Some(86400));
    }

    #[test]
    fn test_parse_duration_helpers_weeks() {
        use crate::api::helpers::parse_duration;
        assert_eq!(parse_duration("1w").ok(), Some(604800));
    }

    #[test]
    fn test_parse_duration_helpers_compound() {
        use crate::api::helpers::parse_duration;
        assert_eq!(parse_duration("1h30m").ok(), Some(5400));
    }

    #[test]
    fn test_parse_duration_helpers_bare_integer() {
        use crate::api::helpers::parse_duration;
        assert_eq!(parse_duration("60").ok(), Some(60));
    }

    #[test]
    fn test_parse_duration_helpers_empty() {
        use crate::api::helpers::parse_duration;
        assert!(parse_duration("").is_err());
    }

    #[test]
    fn test_parse_duration_helpers_invalid_unit() {
        use crate::api::helpers::parse_duration;
        assert!(parse_duration("5x").is_err());
    }

    #[test]
    fn test_parse_duration_helpers_negative() {
        use crate::api::helpers::parse_duration;
        assert!(parse_duration("-5").is_err());
    }

    #[test]
    fn test_parse_duration_helpers_zero() {
        use crate::api::helpers::parse_duration;
        assert_eq!(parse_duration("0").ok(), Some(0));
    }

    #[test]
    fn test_parse_duration_helpers_zero_seconds() {
        use crate::api::helpers::parse_duration;
        assert_eq!(parse_duration("0s").ok(), Some(0));
    }

    #[test]
    fn test_parse_duration_helpers_compound_all_units() {
        use crate::api::helpers::parse_duration;
        assert_eq!(
            parse_duration("1d2h30m15s").ok(),
            Some(86400 + 7200 + 1800 + 15)
        );
    }

    #[test]
    fn test_parse_duration_helpers_whitespace_trim() {
        use crate::api::helpers::parse_duration;
        assert_eq!(parse_duration("  30s  ").ok(), Some(30));
    }

    #[test]
    fn test_parse_duration_helpers_large_value() {
        use crate::api::helpers::parse_duration;
        assert_eq!(parse_duration("365d").ok(), Some(365 * 86400));
    }

    #[test]
    fn test_parse_duration_helpers_no_number_before_unit() {
        use crate::api::helpers::parse_duration;
        assert!(parse_duration("s").is_err());
    }

    #[test]
    fn test_parse_duration_helpers_48s_schedule() {
        use crate::api::helpers::parse_duration;
        assert_eq!(parse_duration("48s").ok(), Some(48));
    }

    #[test]
    fn test_parse_duration_helpers_96s_schedule() {
        use crate::api::helpers::parse_duration;
        assert_eq!(parse_duration("96s").ok(), Some(96));
    }
}
