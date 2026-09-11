-- pg_trickle v0.102.0 -> v0.103.0
-- Durable WAL receipts are committed before a logical slot is acknowledged.

ALTER TABLE pgtrickle.pgt_change_tracking
    ADD COLUMN IF NOT EXISTS receipt_high_water_lsn PG_LSN;

ALTER TABLE pgtrickle.pgt_change_tracking
    ADD COLUMN IF NOT EXISTS acknowledged_high_water_lsn PG_LSN;

CREATE TABLE IF NOT EXISTS pgtrickle.pgt_wal_receipts (
    receipt_id       BIGSERIAL PRIMARY KEY,
    source_relid     OID NOT NULL,
    slot_name        TEXT NOT NULL,
    lsn              PG_LSN NOT NULL,
    source_xid       XID,
    data             TEXT NOT NULL,
    source_change    BOOLEAN NOT NULL DEFAULT false,
    status           TEXT NOT NULL DEFAULT 'RECEIVED'
                     CHECK (status IN ('RECEIVED', 'APPLIED', 'ACKNOWLEDGED')),
    received_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged_at  TIMESTAMPTZ,
    UNIQUE (source_relid, lsn, data)
);

CREATE INDEX IF NOT EXISTS pgt_wal_receipts_pending_idx
    ON pgtrickle.pgt_wal_receipts (source_relid, status, lsn, receipt_id);

REVOKE ALL ON TABLE pgtrickle.pgt_wal_receipts FROM PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('pgtrickle.pgt_wal_receipts', '');

INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES (
    '0.103.0',
    'Durable WAL receipts, replay watermarks, and bounded capture admission'
)
ON CONFLICT (version) DO NOTHING;
