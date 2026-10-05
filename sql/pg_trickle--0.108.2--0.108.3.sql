-- pg_trickle v0.108.2 -> v0.108.3
-- Rebuild existing projected identities so FULL and DIFFERENTIAL refreshes agree.

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

UPDATE pgtrickle.pgt_stream_tables
   SET row_identity_version = NULL,
       row_probe_version = NULL,
       needs_reinit = TRUE,
       updated_at = now();
