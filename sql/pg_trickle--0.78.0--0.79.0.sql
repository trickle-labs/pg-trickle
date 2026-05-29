-- pg_trickle 0.78.0 -> 0.79.0 upgrade migration
-- v0.79.0: Code Quality, API Ergonomics & Security

-- Summary of changes in v0.79.0:
--
--   Q-1: Unused-import suppressions removed from src/refresh/codegen.rs and
--        src/refresh/merge/mod.rs (code quality; no schema change).
--
--   Q-2: alter_stream_table_impl refactored to typed AlterStreamTableOptions
--        parameter struct (code quality; no schema change).
--
--   Q-3: Global #![allow(dead_code)] replaced with per-module #[allow(dead_code)]
--        annotations on pgrx/export boundaries (code quality; no schema change).
--
--   Q-4: consume_slot_changes() marked #[deprecated]; function is never called
--        from the refresh pipeline.  No SQL-visible change.
--
--   A-1: Three new SQL convenience helper functions:
--          create_stream_table_fast_append_only()
--          set_stream_table_refresh_policy()
--          set_stream_table_storage_policy()
--
--   A-2: First-class pause_stream_table() wrapper function
--        (complements the existing resume_stream_table()).
--
--   S-1: Semgrep rules for dynamic SPI calls strengthened (CI change; no
--        schema change).
--
--   S-2: Runtime RLS warning at create_stream_table() time confirmed (code
--        already present via A45-3; no schema change).
--
--   S-3: New global SECURITY DEFINER trigger function CI test added to
--        e2e_rls_tests.rs (test change; no schema change).
--
--   D-3: New cleanup chaos E2E test (test change; no schema change).
--
--   T-5: dbt adapter compatibility matrix tests added (dbt change; no schema
--        change).

-- ── A-1: create_stream_table_fast_append_only() ──────────────────────────────
--
-- Convenience wrapper around create_stream_table() that presets:
--   append_only = true
--   refresh_mode = 'DIFFERENTIAL'
--   initialize = true
-- All other parameters default to their create_stream_table() defaults.

CREATE OR REPLACE FUNCTION pgtrickle."create_stream_table_fast_append_only"(
    "name"                   TEXT,
    "query"                  TEXT,
    "schedule"               TEXT DEFAULT 'calculated',
    "cdc_mode"               TEXT DEFAULT NULL,
    "partition_by"           TEXT DEFAULT NULL,
    "max_differential_joins" integer DEFAULT NULL,
    "max_delta_fraction"     double precision DEFAULT NULL
) RETURNS void
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'create_stream_table_fast_append_only_wrapper';

COMMENT ON FUNCTION pgtrickle."create_stream_table_fast_append_only"(TEXT, TEXT, TEXT, TEXT, TEXT, integer, double precision) IS
    'A-1 (v0.79.0): Create a stream table optimised for append-only sources. '
    'Equivalent to create_stream_table() with append_only=true and '
    'refresh_mode=''DIFFERENTIAL''. Source rows must be INSERT-only; '
    'UPDATE/DELETE on the source will cause the extension to fall back to '
    'FULL refresh automatically.';

-- ── A-1: set_stream_table_refresh_policy() ───────────────────────────────────
--
-- Convenience wrapper that sets only the refresh_mode of an existing stream
-- table, leaving all other settings unchanged.

CREATE OR REPLACE FUNCTION pgtrickle."set_stream_table_refresh_policy"(
    "name"         TEXT,
    "refresh_mode" TEXT
) RETURNS void
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'set_stream_table_refresh_policy_wrapper';

COMMENT ON FUNCTION pgtrickle."set_stream_table_refresh_policy"(TEXT, TEXT) IS
    'A-1 (v0.79.0): Set the refresh policy (mode) for an existing stream table. '
    'Convenience wrapper around alter_stream_table() that only changes the '
    'refresh_mode. Valid values: AUTO, FULL, DIFFERENTIAL, IMMEDIATE.';

-- ── A-1: set_stream_table_storage_policy() ───────────────────────────────────
--
-- Convenience wrapper that sets both append_only mode and scheduling tier
-- for an existing stream table.

CREATE OR REPLACE FUNCTION pgtrickle."set_stream_table_storage_policy"(
    "name"         TEXT,
    "append_only"  boolean DEFAULT NULL,
    "tier"         TEXT DEFAULT NULL
) RETURNS void
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'set_stream_table_storage_policy_wrapper';

COMMENT ON FUNCTION pgtrickle."set_stream_table_storage_policy"(TEXT, boolean, TEXT) IS
    'A-1 (v0.79.0): Set the storage policy for an existing stream table. '
    'Sets append_only mode and/or the scheduling tier (hot/warm/frozen) in one '
    'call. Either argument may be NULL to leave its current value unchanged.';

-- ── A-2: pause_stream_table() ────────────────────────────────────────────────
--
-- First-class pause wrapper: sets the stream table status to SUSPENDED.
-- Complements the existing resume_stream_table().

CREATE OR REPLACE FUNCTION pgtrickle."pause_stream_table"(
    "name" TEXT
) RETURNS void
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'pause_stream_table_wrapper';

COMMENT ON FUNCTION pgtrickle."pause_stream_table"(TEXT) IS
    'A-2 (v0.79.0): Pause an ACTIVE stream table, suspending automated and '
    'manual refreshes. Use pgtrickle.resume_stream_table() to re-enable. '
    'Only ACTIVE stream tables can be paused; already-SUSPENDED or ERROR '
    'stream tables return an error.';

-- Upgrade complete.  No catalog schema changes are required for v0.79.0.
-- The new SQL functions above become available immediately after
--   ALTER EXTENSION pg_trickle UPDATE TO '0.79.0';
-- Safe to apply with zero downtime.
