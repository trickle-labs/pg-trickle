-- pg_trickle v0.107.0 -> v0.108.0
-- Preserve Delta V1 data while adding complete resnapshot fences and the
-- Graph V1.2 and Delta V1.1 public functions.

ALTER TABLE pgtrickle.pgt_output_delta_resnapshots
    ADD COLUMN IF NOT EXISTS database_instance_id TEXT,
    ADD COLUMN IF NOT EXISTS output_contract_digest BYTEA,
    ADD COLUMN IF NOT EXISTS row_identity_version SMALLINT;

UPDATE pgtrickle.pgt_output_delta_resnapshots AS r
SET database_instance_id = l.database_instance_id,
    output_contract_digest = l.output_contract_digest,
    row_identity_version = l.row_identity_version
FROM pgtrickle.pgt_output_delta_logs AS l
WHERE l.pgt_id = r.pgt_id
  AND (r.database_instance_id IS NULL
       OR r.output_contract_digest IS NULL
       OR r.row_identity_version IS NULL);

ALTER TABLE pgtrickle.pgt_output_delta_resnapshots
    ALTER COLUMN database_instance_id SET NOT NULL,
    ALTER COLUMN output_contract_digest SET NOT NULL,
    ALTER COLUMN row_identity_version SET NOT NULL;

ALTER TABLE pgtrickle.pgt_output_delta_resnapshots
    DROP CONSTRAINT IF EXISTS pgt_output_delta_resnapshots_row_identity_version_check;
ALTER TABLE pgtrickle.pgt_output_delta_resnapshots
    ADD CONSTRAINT pgt_output_delta_resnapshots_row_identity_version_check
    CHECK (row_identity_version > 0);

CREATE FUNCTION pgtrickle.request_output_delta_resnapshot(
    consumer_id UUID
) RETURNS TABLE (
    state TEXT,
    state_reason TEXT,
    acknowledged_batch_token BIGINT,
    log_head BIGINT,
    output_contract_digest BYTEA,
    row_identity_version SMALLINT
)
STRICT SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c
AS 'MODULE_PATHNAME', 'request_output_delta_resnapshot_wrapper';

CREATE FUNCTION pgtrickle.validate_output_delta_consumer(
    consumer_id UUID
) RETURNS TABLE (
    consumer_id UUID,
    delta_relation TEXT,
    state TEXT,
    state_reason TEXT,
    acknowledged_batch_token BIGINT,
    log_head BIGINT,
    batch_lag BIGINT,
    output_contract_digest BYTEA,
    row_identity_version SMALLINT,
    database_instance_id TEXT
)
STRICT SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c
AS 'MODULE_PATHNAME', 'validate_output_delta_consumer_wrapper';

CREATE FUNCTION pgtrickle.qualify_output_delta_recovery(
    consumer_id UUID,
    scenario TEXT
) RETURNS TEXT
STRICT SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c
AS 'MODULE_PATHNAME', 'qualify_output_delta_recovery_wrapper';

GRANT EXECUTE ON FUNCTION pgtrickle.request_output_delta_resnapshot(UUID) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.validate_output_delta_consumer(UUID) TO PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.qualify_output_delta_recovery(UUID, TEXT) FROM PUBLIC;

-- Regenerate existing trigger bodies so cdc_capture_mode='hold' is lossless
-- for sources created before this upgrade.
SELECT pgtrickle.rebuild_cdc_triggers();

INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES (
    '0.108.0',
    'Graph V1.2 differential admission and Delta V1.1 recovery'
)
ON CONFLICT (version) DO NOTHING;
