-- pg_trickle 0.79.0 -> 0.80.0 upgrade migration
-- v0.80.0: Operational Excellence, Documentation Completeness & Final v1.0 Gate

-- Summary of changes in v0.80.0:
--
--   O-1: DVM fallback reason codes surfaced in health_check() output.
--        New 'dvm_fallbacks' check in health_check() queries recent
--        pgt_refresh_history for CORRELATED_SUBQUERY_DELTA_QUADRATIC,
--        CASE_IN_LIST_DVM_DRIFT_FULL_FALLBACK, and
--        REGEX_COMPLEXITY_CLASSIFIER_UNCERTAIN reason codes.
--        New REGEX_COMPLEXITY_CLASSIFIER_UNCERTAIN code set when CASE
--        aggregate contains EXISTS/subquery inside predicate.
--        (Rust code change; no catalog schema change.)
--
--   O-2: health_check() ring overflow alert.
--        New 'ring_overflow_trend' check in health_check().
--        (Rust code change; no catalog schema change.)
--
--   O-3: Cleanup backlog trend metrics in metrics_summary().
--        Two new columns: cleanup_backlog_count, cleanup_blocked_count.
--        The old function is dropped and recreated with the new signature.
--
--   DOC-1: docs lint — scripts/check_pg_extern_docs.py added.
--           Integrated into just docs-lint (CI gate).
--
--   DOC-2: docs/DVM_SUPPORT_MATRIX.md created.
--
--   U-1: docs/ROLLBACK_RUNBOOK.md created.
--
--   U-2: Upgrade E2E cutoff policy documented in CHANGELOG.md.
--
--   B-1: CONTRIBUTING.md CI gate workflow table expanded.
--
--   B-2: cargo-deny PR gate confirmed (dependency-policy.yml).
--
--   P-5: Fuzz target for DVM snapshot fingerprint classifiers added.
--
--   A-3: Event trigger function doc comments expanded in src/hooks.rs.

-- ── O-3: Drop and recreate metrics_summary() with two new columns ──────────
-- The return type changes (two new columns) so DROP + CREATE is required.

DROP FUNCTION IF EXISTS pgtrickle."metrics_summary"();

CREATE FUNCTION pgtrickle."metrics_summary"() RETURNS TABLE (
        "db_name" TEXT,
        "total_stream_tables" bigint,
        "active_stream_tables" bigint,
        "suspended_stream_tables" bigint,
        "total_refreshes" bigint,
        "successful_refreshes" bigint,
        "failed_refreshes" bigint,
        "total_rows_processed" bigint,
        "active_workers" INT,
        "ivm_lock_parse_error_count" bigint,
        "holdback_probe_calls" bigint,
        "holdback_probe_cache_hits" bigint,
        "holdback_probe_avg_ms" double precision,
        "cleanup_backlog_count" bigint,
        "cleanup_blocked_count" bigint
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'metrics_summary_wrapper';

COMMENT ON FUNCTION pgtrickle."metrics_summary"() IS
'Cluster-wide metrics summary for dashboards and monitoring.
Returns refresh counts, error counts, worker utilisation, IVM lock errors,
holdback probe metrics, and (v0.80.0) cleanup backlog trend counters.';

-- Upgrade complete.
-- Schema change: metrics_summary() return type extended with two new columns.
-- Apply with:
--   ALTER EXTENSION pg_trickle UPDATE TO '0.80.0';
-- Safe to apply with zero downtime.
