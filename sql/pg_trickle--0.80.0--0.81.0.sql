-- pg_trickle 0.80.0 -> 0.81.0 upgrade migration
-- v0.81.0: Observability, Self-Tuning & Quick Wins

-- Summary of changes in v0.81.0:
--
--   QW-1: commit_latency_stats() — per-stream-table commit/refresh
--         latency percentiles (min, p50, p95, max) from
--         pgt_refresh_history. Read-only, STABLE, PARALLEL SAFE.
--
--   QW-2: tune_recommendations() — rule-based GUC tuning advisor.
--         Returns actionable rows (guc_name, current_value,
--         recommended_value, reason) based on recent refresh history.
--         Read-only, STABLE, PARALLEL SAFE.
--
--   QW-3: preview_stream_table(query) — dry-run query analysis.
--         Returns key/value rows describing DVM support, refresh
--         strategy, source tables, op-tree root, and warnings.
--         No objects created. Read-only.
--
--   QW-4: OpenTelemetry span name constants added to src/otel.rs.
--         (Code-only; no catalog schema change.)
--
--   QW-5: LRU eviction added to DVM template cache (l1_insert_delta_template,
--         l1_insert_placeholder_resolver). Controlled by
--         pg_trickle.l1_cache_max_entries GUC.
--         (Code-only; no catalog schema change.)
--
--   QW-6: DeltaOperator trait extracted (src/dvm/operators/mod.rs).
--         (Code-only; no catalog schema change.)
--
--   QW-7: Config module split into src/config/{mod,scheduler,cdc,dvm,
--         monitoring}.rs. All GUCs unchanged.
--         (Code-only; no catalog schema change.)
--
--   QW-8: Self-heal OOM circuit breaker in the scheduler
--         (pg_trickle.self_heal_oom GUC).
--         (Code-only; no catalog schema change.)
--
--   QW-9: Chunked MERGE path (pg_trickle.merge_batch_size GUC).
--         (Code-only; no catalog schema change.)
--
--   QW-10: Stream table creation presets:
--          create_stream_table_realtime(name, query, …)  — 1s differential
--          create_stream_table_batch(name, query, …)     — 5m AUTO
--          create_stream_table_cost_optimized(name, query, …) — 15m AUTO

-- ── QW-1: commit_latency_stats() ─────────────────────────────────────────

CREATE OR REPLACE FUNCTION pgtrickle."commit_latency_stats"() RETURNS TABLE (
        "pgt_schema" TEXT,
        "pgt_name" TEXT,
        "samples" bigint,
        "min_ms" double precision,
        "p50_ms" double precision,
        "p95_ms" double precision,
        "max_ms" double precision,
        "tracking_mode" TEXT
)
STABLE PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'commit_latency_stats_wrapper';

COMMENT ON FUNCTION pgtrickle."commit_latency_stats"() IS
'Per-stream-table refresh latency percentiles (min, p50, p95, max) from
pgt_refresh_history. tracking_mode reflects whether commit_timestamp
tracking is enabled (commit_timestamp) or duration-based (refresh_duration).';

-- ── QW-2: tune_recommendations() ─────────────────────────────────────────

CREATE OR REPLACE FUNCTION pgtrickle."tune_recommendations"() RETURNS TABLE (
        "guc_name" TEXT,
        "current_value" TEXT,
        "recommended_value" TEXT,
        "reason" TEXT
)
STABLE PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'tune_recommendations_wrapper';

COMMENT ON FUNCTION pgtrickle."tune_recommendations"() IS
'Rule-based GUC tuning advisor. Returns one row per actionable recommendation
based on recent pgt_refresh_history data and current GUC settings. Returns an
empty result set when all metrics are within healthy ranges.';

-- ── QW-3: preview_stream_table(query) ────────────────────────────────────

CREATE OR REPLACE FUNCTION pgtrickle."preview_stream_table"(
        "query" TEXT
) RETURNS TABLE (
        "property" TEXT,
        "value" TEXT
)
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'preview_stream_table_wrapper';

COMMENT ON FUNCTION pgtrickle."preview_stream_table"(TEXT) IS
'Dry-run query analysis: reports DVM support, refresh strategy, source tables,
op-tree root type, warnings, and complexity estimate without creating anything.';

-- ── QW-10: Stream table creation presets ────────────────────────────────

CREATE OR REPLACE FUNCTION pgtrickle."create_stream_table_realtime"(
        "name" TEXT,
        "query" TEXT,
        "cdc_mode" TEXT DEFAULT NULL,
        "append_only" bool DEFAULT false,
        "partition_by" TEXT DEFAULT NULL,
        "max_differential_joins" INT DEFAULT NULL,
        "max_delta_fraction" double precision DEFAULT NULL
) RETURNS void

LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'create_stream_table_realtime_wrapper';

COMMENT ON FUNCTION pgtrickle."create_stream_table_realtime"(TEXT, TEXT, TEXT, bool, TEXT, INT, double precision) IS
'Preset: create a stream table with schedule=1s and refresh_mode=DIFFERENTIAL.
Use for latency-sensitive use cases where sub-second freshness is required.';

CREATE OR REPLACE FUNCTION pgtrickle."create_stream_table_batch"(
        "name" TEXT,
        "query" TEXT,
        "cdc_mode" TEXT DEFAULT NULL,
        "append_only" bool DEFAULT false,
        "partition_by" TEXT DEFAULT NULL,
        "max_differential_joins" INT DEFAULT NULL,
        "max_delta_fraction" double precision DEFAULT NULL
) RETURNS void

LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'create_stream_table_batch_wrapper';

COMMENT ON FUNCTION pgtrickle."create_stream_table_batch"(TEXT, TEXT, TEXT, bool, TEXT, INT, double precision) IS
'Preset: create a stream table with schedule=5m and refresh_mode=AUTO.
Use for analytical workloads where moderate latency is acceptable.';

CREATE OR REPLACE FUNCTION pgtrickle."create_stream_table_cost_optimized"(
        "name" TEXT,
        "query" TEXT,
        "cdc_mode" TEXT DEFAULT NULL,
        "append_only" bool DEFAULT false,
        "partition_by" TEXT DEFAULT NULL,
        "max_differential_joins" INT DEFAULT NULL,
        "max_delta_fraction" double precision DEFAULT NULL
) RETURNS void

LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'create_stream_table_cost_optimized_wrapper';

COMMENT ON FUNCTION pgtrickle."create_stream_table_cost_optimized"(TEXT, TEXT, TEXT, bool, TEXT, INT, double precision) IS
'Preset: create a stream table with schedule=15m and refresh_mode=AUTO.
Use for reporting and BI queries where freshness can be traded for lower overhead.';

-- Upgrade complete.
-- Schema changes: 6 new functions added (commit_latency_stats,
--   tune_recommendations, preview_stream_table, create_stream_table_realtime,
--   create_stream_table_batch, create_stream_table_cost_optimized).
-- All other changes (QW-4 through QW-9) are code-only with no DDL impact.
-- Apply with:
--   ALTER EXTENSION pg_trickle UPDATE TO '0.81.0';
-- Safe to apply with zero downtime.
