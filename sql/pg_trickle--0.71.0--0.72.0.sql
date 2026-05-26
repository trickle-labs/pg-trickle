-- pg_trickle 0.71.0 -> 0.72.0 upgrade migration (v0.72.0 — Frontier Durability & Catalog Correctness)
--
-- v0.72.0 — Frontier Durability & Catalog Correctness
--
-- Changes in this release:
--
--   COR-001/REL-001/ARCH-001: Dead DUR-1 tentative-frontier functions removed.
--     The prepare_frontier, reconcile_tentative_frontiers, and the DUR-1 variant
--     of finalize_frontier_and_complete_refresh have been removed. The canonical
--     single-phase frontier path (store_frontier / store_frontier_and_complete_
--     refresh) is the only mechanism for advancing the frontier. The
--     tentative_frontier column is retained below but will always be NULL.
--
--   COR-002/API-001: pgt_outbox_config.stream_table_oid corrected to store
--     pgt_relid (actual pg_class OID) instead of pgt_id (internal counter).
--     The UPDATE below fixes existing rows.
--
--   COR-003: complete_wal_transition now updates catalog mode before dropping
--     the CDC trigger, wrapped in pg_advisory_lock. No schema change.
--
--   COR-004: create_replication_slot_pristine now checks for an XID-assigned
--     transaction before creating the slot. No schema change.

-- Ensure pgt_ducklake_sink_delivery exists for instances that were created
-- before v0.69.0 or whose archive was generated without this table.
-- Uses IF NOT EXISTS so it is a no-op on instances that already have it.
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

-- Correct any outbox rows that were written with the old pgt_id-as-OID bug.
-- Rows where stream_table_oid already equals pgt_relid are left untouched.
UPDATE pgtrickle.pgt_outbox_config oc
   SET stream_table_oid = st.pgt_relid
  FROM pgtrickle.pgt_stream_tables st
 WHERE oc.stream_table_oid = st.pgt_id::oid
   AND oc.stream_table_oid != st.pgt_relid;
