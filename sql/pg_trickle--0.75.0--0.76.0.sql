-- pg_trickle 0.75.0 -> 0.76.0 upgrade migration
-- v0.76.0: Complete DuckLake Integration Removal

-- ── pg_mooncake stub removal ───────────────────────────────────────────────
-- The pg_mooncake storage backend was never fully implemented — the string was
-- accepted and stored in pgt_stream_tables.storage_backend but no mooncake-
-- specific DDL was ever generated.  Any rows with storage_backend = 'pg_mooncake'
-- silently behaved as standard heap tables.
--
-- This migration normalises those rows to 'heap' (their actual behaviour) so
-- the catalog accurately reflects the storage in use.
--
-- If pg_trickle.columnar_backend was set to 'pg_mooncake' in postgresql.conf,
-- change it to 'none' (or 'citus' if Citus is available) before upgrading.

UPDATE pgtrickle.pgt_stream_tables
SET    storage_backend = 'heap'
WHERE  storage_backend = 'pg_mooncake';

-- ── DuckLake integration removal ──────────────────────────────────────────
-- All DuckLake sink and change-feed source integration has been removed in
-- v0.76.0. The pg_ducklake extension uses a native table AM (not FDW), making
-- the DUCKLAKE_CHANGE_FEED detection heuristic architecturally obsolete. The
-- outbound direction is covered by pg_duckpipe. Maintaining DuckLake-specific
-- relay code in an IVM extension is out of scope.
--
-- GUC deprecation notice: the following GUCs are no longer recognised and
-- will be silently ignored after upgrade:
--   pg_trickle.ducklake_compaction_policy
--   pg_trickle.ducklake_sink_bucket
--   pg_trickle.ducklake_sink_prefix
--   pg_trickle.ducklake_sink_max_retries
--   pg_trickle.ducklake_sink_failure_mode
--   pg_trickle.ducklake_catalog_schema
-- Remove these from postgresql.conf / ALTER SYSTEM after upgrading.

-- stream_tables_info in v0.75.0 is defined as SELECT st.* FROM
-- pgt_stream_tables. Drop/recreate it so ALTER TABLE can remove DuckLake
-- columns without dependency errors during extension upgrade.
DROP VIEW IF EXISTS pgtrickle.stream_tables_info;

-- Remove DuckLake columns from pgt_stream_tables.
ALTER TABLE pgtrickle.pgt_stream_tables
    DROP COLUMN IF EXISTS ducklake_compaction_policy;
ALTER TABLE pgtrickle.pgt_stream_tables
    DROP COLUMN IF EXISTS ducklake_sink_mode;
ALTER TABLE pgtrickle.pgt_stream_tables
    DROP COLUMN IF EXISTS ducklake_sink_path;
ALTER TABLE pgtrickle.pgt_stream_tables
    DROP COLUMN IF EXISTS ducklake_sink_table_id;

-- Update cdc_mode CHECK constraint on pgt_dependencies (drop + re-add).
ALTER TABLE pgtrickle.pgt_dependencies
    DROP CONSTRAINT IF EXISTS pgt_dependencies_cdc_mode_check;
ALTER TABLE pgtrickle.pgt_dependencies
    ADD CONSTRAINT pgt_dependencies_cdc_mode_check
        CHECK (cdc_mode IN ('TRIGGER', 'TRANSITIONING', 'WAL'));

-- Remove DuckLake catalog tables (provenance + sink delivery tracking).
DROP TABLE IF EXISTS pgtrickle.pgt_ducklake_sink_delivery;
DROP TABLE IF EXISTS pgtrickle.pgt_ducklake_provenance;

-- Remove the ducklake_sink_status() monitoring function (added in v0.69.0).
-- The C symbol (ducklake_sink_status_wrapper) was removed from the binary in
-- v0.76.0, so the function would fail if called. Drop it explicitly.
DROP FUNCTION IF EXISTS pgtrickle.ducklake_sink_status();

-- Recreate stream_tables_info with the v0.76.0 definition.
CREATE OR REPLACE VIEW pgtrickle.stream_tables_info AS
SELECT st.*,
      now() - st.last_refresh_at AS staleness,
      CASE WHEN st.schedule IS NOT NULL
              AND st.schedule !~ '[\s@]'
          THEN EXTRACT(EPOCH FROM (now() - st.last_refresh_at)) >
              pgtrickle.parse_duration_seconds(st.schedule)
          ELSE NULL::boolean
      END AS stale,
      CASE WHEN st.topk_limit IS NOT NULL THEN TRUE ELSE FALSE END AS is_topk
FROM pgtrickle.pgt_stream_tables st;

-- Migrate any DUCKLAKE_CHANGE_FEED rows back to TRIGGER (safe fallback).
UPDATE pgtrickle.pgt_dependencies
SET    cdc_mode = 'TRIGGER'
WHERE  cdc_mode = 'DUCKLAKE_CHANGE_FEED';
