-- pg_trickle 0.69.0 -> 0.70.0 upgrade migration
--
-- v0.70.0 — Scheduler, Validator & Security Hardening
--
-- Changes in this release:
--
--   COR-002: LATERAL validator body scanning
--     - tree_collect_volatility() and check_ivm_support() now recurse into
--       LATERAL SRF bodies and LATERAL subquery SQL, so volatile expressions
--       inside LATERAL constructs are caught by the volatile_function_policy
--       check instead of silently bypassing it.
--
--   PERF-001: Batched buffer health checks
--     - check_slot_health_and_alert() now builds a single UNION ALL query
--       for all monitored buffers instead of one SPI call per buffer.
--       Reduces per-alert SPI overhead from O(n) to O(1).
--
--   PERF-002: Batched fused-chain eligibility lookups
--     - try_fused_chain_refresh() now calls StDependency::get_for_sts()
--       to fetch all dependency rows in a single query instead of one
--       per candidate stream table.
--
--   PERF-003: History prune interval now GUC-controlled
--     - pg_trickle.history_prune_interval_seconds (default 60) replaces
--       the hard-coded 24-hour constant. Zero disables automatic pruning.
--
--   PERF-004: delta_work_mem_cap_mb default raised to 256 MiB
--     - Previous default was 0 (disabled). Set to 256 MiB to prevent
--       unbounded memory growth during large differential refreshes.
--
--   SCAL-002: Launcher install-epoch fast-rescan
--     - A new shared-memory counter (LAUNCHER_INSTALL_EPOCH) is bumped
--       on every CREATE/DROP EXTENSION DDL event. The launcher detects
--       the change and switches to a 10-second polling interval for one
--       cycle before backing off to the steady-state 60-second interval.
--
--   SEC-001: Publication name parser unified
--     - create_publication() and alter_publication() now use the shared
--       helpers::parse_qualified_name_pub() instead of a local copy,
--       eliminating the duplicate parsing path.
--
--   OBS-002: Prune error visibility
--     - History prune failures now increment HISTORY_PRUNE_ERRORS (shared
--       memory), log a WARNING, and are exposed via the new
--       pgtrickle.history_prune_status() function.
--
--   TEST-001: LATERAL volatile body tests
--     - E2E tests verify that LATERAL subqueries/SRFs containing volatile
--       expressions are rejected in DIFFERENTIAL mode, and that immutable
--       SRFs continue to work.
--
--   TEST-002: cache_stats() shape and monotonicity tests
--     - E2E test verifies pgtrickle.cache_stats() returns exactly one row
--       with non-negative counters, and that cache accesses increase after
--       a DIFFERENTIAL refresh.

-- ── OBS-002: history_prune_status() ──────────────────────────────────────

-- Expose history prune error counter and last prune timing.
-- prune_error_count: cumulative SPI failures in the background prune loop.
-- last_prune_at:     timestamp of last successful prune (NULL if none).
-- last_rows_deleted: row count from the most recent prune run (NULL if none).

CREATE OR REPLACE FUNCTION pgtrickle."history_prune_status"()
RETURNS TABLE (
    "prune_error_count" bigint,
    "last_prune_at"     timestamp with time zone,
    "last_rows_deleted" bigint
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'history_prune_status_wrapper';

COMMENT ON FUNCTION pgtrickle."history_prune_status"() IS
    'OBS-002 (v0.70.0): Returns the cumulative prune error count and last prune '
    'timing from shared memory. A non-zero prune_error_count indicates the '
    'background cleanup loop is failing and pgt_refresh_history may grow unbounded.';
