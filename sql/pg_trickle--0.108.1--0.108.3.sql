-- pg_trickle v0.108.1 -> v0.108.2
-- V3 fixes bpchar identity canonicalization; V2 bytes remain immutable.

-- Only an explicitly fenced consumer may cross an identity-only rebuild.
CREATE OR REPLACE FUNCTION pgtrickle._reject_output_delta_contract_change()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pgtrickle, pg_catalog, pg_temp
AS $$
BEGIN
    IF (OLD.pgt_relid, OLD.defining_query, OLD.refresh_mode, OLD.orchestration_mode,
        OLD.row_identity_version) IS DISTINCT FROM
       (NEW.pgt_relid, NEW.defining_query, NEW.refresh_mode, NEW.orchestration_mode,
        NEW.row_identity_version)
       AND EXISTS (
           SELECT 1 FROM pgtrickle.pgt_output_delta_consumers
           WHERE pgt_id = OLD.pgt_id AND state <> 'DROPPED'
             AND NOT (
                 state IN ('RESNAPSHOT_REQUIRED', 'INVALIDATED')
                 AND (OLD.pgt_relid, OLD.defining_query, OLD.refresh_mode, OLD.orchestration_mode)
                     IS NOT DISTINCT FROM
                     (NEW.pgt_relid, NEW.defining_query, NEW.refresh_mode, NEW.orchestration_mode)
             )
       ) THEN
        RAISE EXCEPTION 'PGT_EXT_CONSUMER_BLOCKED: active output-delta consumers require an explicit resnapshot'
            USING ERRCODE = '55006';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION pgtrickle."encode_row_id_v3"(
    "domain" TEXT,
    "record" anyelement
) RETURNS bytea
STRICT STABLE PARALLEL SAFE
LANGUAGE c
AS 'MODULE_PATHNAME', 'encode_row_id_v3_wrapper';
GRANT EXECUTE ON FUNCTION pgtrickle.encode_row_id_v3(text, anyelement) TO PUBLIC;

UPDATE pgtrickle.pgt_stream_tables
   SET row_identity_version = NULL,
       row_probe_version = NULL,
       needs_reinit = TRUE,
       updated_at = now();

-- New writes use the V3 encoder, while all existing stream tables remain
-- blocked from differential maintenance until their protected FULL rebuild.
SELECT pgtrickle.rebuild_cdc_triggers();

-- Keep queued change rows intact. Only replace each synthetic CDC sentinel
-- and update the buffer metadata after the V3 encoder is installed.
DO $pgt$
DECLARE
    change_schema TEXT := COALESCE(
        NULLIF(current_setting('pg_trickle.change_buffer_schema', true), ''),
        'pgtrickle_changes'
    );
    buffer RECORD;
BEGIN
    FOR buffer IN
        SELECT buffer_key::text AS buffer_key, sentinel_token
        FROM pgtrickle.pgt_change_buffers
    LOOP
        EXECUTE format(
            'DELETE FROM %I.%I WHERE action = %L',
            change_schema, buffer.buffer_key, 'S'
        );
        EXECUTE format(
            'INSERT INTO %I.%I (lsn, action, __pgt_row_id) VALUES (%L::pg_lsn, %L, pgtrickle.encode_row_id_v3(''SYNTHETIC'', ROW(%L::bigint)))',
            change_schema, buffer.buffer_key, '0/0', 'S', buffer.sentinel_token
        );
    END LOOP;

    UPDATE pgtrickle.pgt_change_buffers
       SET row_identity_version = 3,
           row_probe_version = 1;
END
$pgt$;
