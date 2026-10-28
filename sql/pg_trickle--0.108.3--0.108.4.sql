-- pg_trickle v0.108.3 -> v0.108.4
-- Local WAL slots are removed only after their retirement has committed.
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_wal_slot_retirements (
    slot_name            TEXT PRIMARY KEY,
    source_relid         OID NOT NULL,
    capture_instance_id  TEXT NOT NULL,
    queued_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
REVOKE ALL ON TABLE pgtrickle.pgt_wal_slot_retirements FROM PUBLIC;
SELECT pg_catalog.pg_extension_config_dump(
    'pgtrickle.pgt_wal_slot_retirements', 'WHERE false'
);
