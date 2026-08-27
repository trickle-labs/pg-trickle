-- pg_trickle 0.87.12 -> 0.87.13
-- LSEC-19..LSEC-21: caller-context pg_tide calls and immutable outbox
-- provenance. Existing mappings are adopted only when their live pg_tide row
-- can be identified exactly.

ALTER TABLE pgtrickle.pgt_outbox_config
    ADD COLUMN IF NOT EXISTS pg_tide_extension_oid OID,
    ADD COLUMN IF NOT EXISTS pg_tide_version TEXT,
    ADD COLUMN IF NOT EXISTS tide_outbox_created_at TIMESTAMPTZ;

DO $migration$
BEGIN
    IF EXISTS (SELECT 1 FROM pgtrickle.pgt_outbox_config) THEN
        IF NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_extension
            WHERE extname::text = 'pg_tide'
        ) OR to_regclass('tide.tide_outbox_config') IS NULL THEN
            RAISE EXCEPTION
                'v0.87.13 outbox migration cannot prove pg_tide provenance; install pg_tide and retry, or detach the affected outboxes first';
        END IF;

        UPDATE pgtrickle.pgt_outbox_config AS oc
           SET pg_tide_extension_oid = e.oid,
               pg_tide_version = e.extversion::text,
               tide_outbox_created_at = tc.created_at
          FROM pg_catalog.pg_extension AS e,
               tide.tide_outbox_config AS tc
         WHERE e.extname::text = 'pg_tide'
           AND tc.outbox_name = oc.tide_outbox_name;

        IF EXISTS (
            SELECT 1
            FROM pgtrickle.pgt_outbox_config
            WHERE pg_tide_extension_oid IS NULL
               OR pg_tide_version IS NULL
               OR tide_outbox_created_at IS NULL
        ) THEN
            RAISE EXCEPTION
                'v0.87.13 outbox migration found a missing or reused pg_tide outbox; detach and reattach the affected stream table';
        END IF;
    END IF;
END
$migration$;

ALTER TABLE pgtrickle.pgt_outbox_config
    ALTER COLUMN pg_tide_extension_oid SET NOT NULL,
    ALTER COLUMN pg_tide_version SET NOT NULL,
    ALTER COLUMN tide_outbox_created_at SET NOT NULL;

ALTER FUNCTION pgtrickle.attach_outbox(text, integer, integer)
    SECURITY DEFINER; -- nosemgrep: sql.security-definer.present — external pg_tide calls run as the captured caller.
ALTER FUNCTION pgtrickle.attach_outbox(text, integer, integer)
    SET search_path = pgtrickle, pg_catalog, pg_temp;
ALTER FUNCTION pgtrickle.detach_outbox(text, boolean)
    SECURITY DEFINER; -- nosemgrep: sql.security-definer.present — private mapping cleanup is definer-only.
ALTER FUNCTION pgtrickle.detach_outbox(text, boolean)
    SET search_path = pgtrickle, pg_catalog, pg_temp;
ALTER FUNCTION pgtrickle.attach_embedding_outbox(text, text, integer, integer)
    SECURITY DEFINER; -- nosemgrep: sql.security-definer.present — external pg_tide calls run as the captured caller.
ALTER FUNCTION pgtrickle.attach_embedding_outbox(text, text, integer, integer)
    SET search_path = pgtrickle, pg_catalog, pg_temp;
