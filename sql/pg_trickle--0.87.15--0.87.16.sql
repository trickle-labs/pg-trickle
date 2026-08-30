-- pg_trickle 0.87.15 -> 0.87.16
--
-- v0.87.16 changes newly created/rebuilt identity-bearing relations from the
-- legacy BIGINT digest to the complete V2 BYTEA encoding. Existing V1 state is
-- deliberately not converted in place: v0.87.17 recreates stream tables and
-- buffers from unchanged sources, so a failed upgrade cannot leave a partially
-- converted relation behind.

ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS row_probe_version SMALLINT;

ALTER TABLE pgtrickle.pgt_change_buffers
    ADD COLUMN IF NOT EXISTS row_probe_version SMALLINT;

-- Fail closed until the new binary has rebuilt every identity-bearing state
-- relation and has written both version markers atomically. The old physical
-- relations remain untouched and are rejected by the V2 runtime validator.
UPDATE pgtrickle.pgt_stream_tables
SET row_identity_version = NULL,
    row_probe_version = NULL;

UPDATE pgtrickle.pgt_change_buffers
SET row_identity_version = NULL,
    row_probe_version = NULL;

GRANT EXECUTE ON FUNCTION pgtrickle.row_probe_v1(bytea) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.encode_row_id_v2(text, anyelement) TO PUBLIC;
