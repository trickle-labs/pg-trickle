-- pg_trickle 0.73.0 -> 0.74.0 upgrade migration
-- v0.74.0: Test Coverage, CI Integrity & Security Hardening

-- This release contains only non-schema changes:
--   DEP-001: sqlx 0.8.6 → 0.9.0 upgrade
--   DEP-002: lru 0.16.4 → 0.18.0 upgrade
--   DEP-003: object_store 0.10.2 → 0.13.2 upgrade
--   TEST-002: Condition-based polling replacing fixed WAL/safety sleeps
--   CODE-002: Unit tests for refresh/codegen, refresh/merge, api/metrics_ext
--   SEC-001: Centralized advisory ignores and just security recipe
--   SEC-002: IVM AFTER trigger search path shadowing E2E tests
--   SEC-003: SQL builder audit script
--   TEST-004: Path-filtered full E2E CI security gate
--   TEST-005: just coverage-summary recipe
--   DEVEX-001: Re-enabled push-to-main benchmark baselines
--   DEVEX-002: just lint-ci recipe
--   DEVEX-003: Stale version tag updates

-- HOT-1 was introduced in v0.73.0 but the 0.73.0 archive SQL was generated
-- before the column was added. Apply idempotently so both fresh 0.73.0
-- installs (missing the column) and upgrade-path installs (already present)
-- succeed.
ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS storage_fillfactor INT
        CONSTRAINT pgt_storage_fillfactor_range
        CHECK (storage_fillfactor IS NULL OR
               (storage_fillfactor >= 10 AND storage_fillfactor <= 100));
