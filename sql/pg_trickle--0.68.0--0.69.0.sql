-- pg_trickle 0.68.0 -> 0.69.0 upgrade migration
--
-- v0.69.0 — DuckLake Sink Reliability & Security
--
-- Changes in this release:
--
--   ARCH-002 / REL-001: Sink delivery state machine
--     - New table pgtrickle.pgt_ducklake_sink_delivery tracks each delivery
--       attempt (PENDING → WRITING → DELIVERED / FAILED_RETRYABLE →
--       FAILED_PERMANENT).
--     - New GUCs: pg_trickle.ducklake_sink_max_retries (default 3) and
--       pg_trickle.ducklake_sink_failure_mode (default 'warn').
--
--   COR-005: View registration on query-only ALTER
--     - After alter_stream_table_query() returns, the DuckLake view entry is
--       re-registered with the updated defining_query so DuckLake clients see
--       the new definition immediately.
--
--   COR-006: Snapshot ID advisory lock
--     - register_ducklake_data_file() now acquires pg_advisory_xact_lock(table_id)
--       before reading MAX(snapshot_id) to prevent concurrent snapshot ID collisions.
--
--   SEC-002: Qualified schema resolution
--     - All DuckLake catalog writes now use the pg_trickle.ducklake_catalog_schema
--       GUC (default 'main') to avoid search_path-based misdirection.
--     - ducklake_view_table_exists() now queries pg_class JOIN pg_namespace
--       instead of information_schema.tables.
--
--   OBS-001: Sink health metrics
--     - New function pgtrickle.ducklake_sink_status() returns last delivery
--       status, timing, and error for each stream table with a DuckLake sink.
--
--   DEP-002: Dependency policy documentation
--     - New file docs/DEPENDENCIES.md documents the criteria for adding,
--       updating, and removing Rust crate dependencies.

-- ── ARCH-002 / REL-001: Delivery state machine table ─────────────────────

CREATE TABLE IF NOT EXISTS pgtrickle.pgt_ducklake_sink_delivery (
    delivery_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stream_table_id  bigint NOT NULL
                       REFERENCES pgtrickle.pgt_stream_tables(pgt_id)
                       ON DELETE CASCADE,
    refresh_id       bigint,
    status           text NOT NULL
                       CHECK (status IN (
                           'PENDING',
                           'WRITING',
                           'DELIVERED',
                           'FAILED_RETRYABLE',
                           'FAILED_PERMANENT'
                       )),
    attempt_count    int NOT NULL DEFAULT 0,
    bytes_written    bigint,
    rows_written     bigint,
    started_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz,
    last_error       text
);

CREATE INDEX IF NOT EXISTS pgt_ducklake_sink_delivery_st_started
    ON pgtrickle.pgt_ducklake_sink_delivery (stream_table_id, started_at DESC);

COMMENT ON TABLE pgtrickle.pgt_ducklake_sink_delivery IS
    'ARCH-002/REL-001 (v0.69.0): per-delivery tracking for the DuckLake sink write path.';
