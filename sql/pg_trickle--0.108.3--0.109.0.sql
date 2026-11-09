-- pg_trickle v0.108.3 -> v0.109.0
ALTER TABLE pgtrickle.pgt_scheduler_jobs
    ADD COLUMN member_failure_recorded BOOLEAN NOT NULL DEFAULT false;
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

-- Add the metric-aware embedding helper without changing legacy calls.
CREATE TABLE pgtrickle.pgt_centroid_states (
    pgt_id BIGINT PRIMARY KEY REFERENCES pgtrickle.pgt_stream_tables(pgt_id) ON DELETE CASCADE,
    stream_relid OID NOT NULL,
    defining_query TEXT NOT NULL,
    contract_generation BIGINT NOT NULL,
    schema_version SMALLINT NOT NULL,
    materialized_xid XID8,
    baseline_frontier JSONB,
    last_change_limit_rebase BOOLEAN NOT NULL DEFAULT FALSE,
    cost_rescan_active BOOLEAN NOT NULL DEFAULT FALSE,
    cost_rescan_xid XID8,
    status TEXT NOT NULL CHECK (status IN ('BUILDING', 'READY'))
);
CREATE TABLE pgtrickle.pgt_centroid_groups (
    pgt_id BIGINT NOT NULL REFERENCES pgtrickle.pgt_centroid_states(pgt_id) ON DELETE CASCADE,
    row_id BYTEA NOT NULL,
    group_count BIGINT NOT NULL CHECK (group_count >= 0),
    state DOUBLE PRECISION[] NOT NULL,
    state_checksum TEXT NOT NULL,
    PRIMARY KEY (pgt_id, row_id),
    CHECK (array_ndims(state) = 1 AND array_lower(state, 1) = 1
           AND cardinality(state) BETWEEN 5 AND 32003
           AND (cardinality(state) - 3) % 2 = 0
           AND array_position(state, NULL::double precision) IS NULL
           AND state[1] >= 0 AND state[1] <= group_count AND state[1] = trunc(state[1]))
);
REVOKE ALL ON TABLE pgtrickle.pgt_centroid_states, pgtrickle.pgt_centroid_groups FROM PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('pgtrickle.pgt_centroid_states', 'WHERE false');
SELECT pg_catalog.pg_extension_config_dump('pgtrickle.pgt_centroid_groups', 'WHERE false');

CREATE FUNCTION pgtrickle._centroid_initialization_sql(text)
RETURNS text STABLE LANGUAGE c AS 'MODULE_PATHNAME', '_centroid_initialization_sql_wrapper';

CREATE FUNCTION pgtrickle._centroid_staging_sql(text)
RETURNS text STABLE LANGUAGE c AS 'MODULE_PATHNAME', '_centroid_staging_sql_wrapper';

CREATE FUNCTION pgtrickle._centroid_stage_state(oid)
RETURNS text SECURITY DEFINER SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c AS 'MODULE_PATHNAME', '_centroid_stage_state_wrapper';
REVOKE ALL ON FUNCTION pgtrickle._centroid_stage_state(oid) FROM PUBLIC;

CREATE FUNCTION pgtrickle._centroid_prepare_state(oid)
RETURNS text SECURITY DEFINER SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c AS 'MODULE_PATHNAME', '_centroid_prepare_state_wrapper';
REVOKE ALL ON FUNCTION pgtrickle._centroid_prepare_state(oid) FROM PUBLIC;

CREATE FUNCTION pgtrickle._centroid_delta_sql(oid, text, text)
RETURNS text SECURITY DEFINER SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c AS 'MODULE_PATHNAME', '_centroid_delta_sql_wrapper';
REVOKE ALL ON FUNCTION pgtrickle._centroid_delta_sql(oid, text, text) FROM PUBLIC;

CREATE FUNCTION pgtrickle._centroid_stage_delta(oid, text, text)
RETURNS text SECURITY DEFINER SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c AS 'MODULE_PATHNAME', '_centroid_stage_delta_wrapper';
REVOKE ALL ON FUNCTION pgtrickle._centroid_stage_delta(oid, text, text) FROM PUBLIC;

CREATE FUNCTION pgtrickle._centroid_stage_candidates(oid, text, text)
RETURNS text SECURITY DEFINER SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c AS 'MODULE_PATHNAME', '_centroid_stage_candidates_wrapper';
REVOKE ALL ON FUNCTION pgtrickle._centroid_stage_candidates(oid, text, text) FROM PUBLIC;

CREATE FUNCTION pgtrickle._centroid_candidate_sql(oid, text, text)
RETURNS text SECURITY DEFINER SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c AS 'MODULE_PATHNAME', '_centroid_candidate_sql_wrapper';
REVOKE ALL ON FUNCTION pgtrickle._centroid_candidate_sql(oid, text, text) FROM PUBLIC;

CREATE FUNCTION pgtrickle._centroid_delta_step(double precision[], real[], smallint)
RETURNS double precision[] IMMUTABLE PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_centroid_delta_step_wrapper';

CREATE AGGREGATE pgtrickle._centroid_delta(real[], smallint) (
    SFUNC = pgtrickle._centroid_delta_step,
    STYPE = double precision[],
    INITCOND = '{}',
    PARALLEL = UNSAFE
);

CREATE FUNCTION pgtrickle._centroid_init_raw(double precision[], integer)
RETURNS double precision[] IMMUTABLE PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_centroid_init_raw_wrapper';

CREATE FUNCTION pgtrickle._centroid_init(double precision[], integer)
RETURNS double precision[] LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
    SELECT pgtrickle._centroid_init_raw(
        CASE WHEN $1 IS NULL OR (pg_catalog.cardinality($1) > 0 AND
                 (pg_catalog.array_ndims($1) IS DISTINCT FROM 1 OR pg_catalog.array_lower($1, 1) IS DISTINCT FROM 1))
             THEN NULL::double precision[] ELSE $1 END, $2)
$$;

GRANT EXECUTE ON FUNCTION pgtrickle._centroid_delta_step(double precision[], real[], smallint) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle._centroid_delta(real[], smallint) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle._centroid_init_raw(double precision[], integer) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle._centroid_init(double precision[], integer) TO PUBLIC;

CREATE FUNCTION pgtrickle._centroid_finalize_raw(double precision[], integer)
RETURNS real[] IMMUTABLE PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_centroid_finalize_raw_wrapper';

CREATE FUNCTION pgtrickle._centroid_check_count(double precision[], bigint, integer)
RETURNS double precision[] IMMUTABLE PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_centroid_check_count_wrapper';
REVOKE ALL ON FUNCTION pgtrickle._centroid_check_count(double precision[], bigint, integer) FROM PUBLIC;

CREATE FUNCTION pgtrickle._centroid_finalize(double precision[], integer)
RETURNS real[] LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
    SELECT pgtrickle._centroid_finalize_raw(
        CASE WHEN $1 IS NULL OR pg_catalog.array_ndims($1) IS DISTINCT FROM 1
                   OR pg_catalog.array_lower($1, 1) IS DISTINCT FROM 1
             THEN NULL::double precision[] ELSE $1 END, $2)
$$;

CREATE FUNCTION pgtrickle._centroid_merge_raw(double precision[], double precision[], integer, boolean)
RETURNS double precision[] IMMUTABLE PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_centroid_merge_raw_wrapper';

CREATE FUNCTION pgtrickle._centroid_merge(double precision[], double precision[], integer, boolean)
RETURNS double precision[] LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
    SELECT pgtrickle._centroid_merge_raw(
        CASE WHEN $1 IS NULL OR $2 IS NULL
                   OR (pg_catalog.cardinality($1) > 0 AND
                       (pg_catalog.array_ndims($1) IS DISTINCT FROM 1 OR pg_catalog.array_lower($1, 1) IS DISTINCT FROM 1))
                   OR (pg_catalog.cardinality($2) > 0 AND
                       (pg_catalog.array_ndims($2) IS DISTINCT FROM 1 OR pg_catalog.array_lower($2, 1) IS DISTINCT FROM 1))
             THEN NULL::double precision[] ELSE $1 END,
        $2, $3, $4)
$$;

CREATE FUNCTION pgtrickle._centroid_rebase_reason_raw(
    bigint, bigint, bigint, double precision[], double precision[]
) RETURNS text IMMUTABLE PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_centroid_rebase_reason_raw_wrapper';

CREATE FUNCTION pgtrickle._centroid_rebase_reason(
    bigint, bigint, bigint, double precision[], double precision[]
) RETURNS text LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE WHEN pg_catalog.array_ndims($4) IS DISTINCT FROM 1
                      OR pg_catalog.array_ndims($5) IS DISTINCT FROM 1
                      OR pg_catalog.array_lower($4, 1) IS DISTINCT FROM 1
                      OR pg_catalog.array_lower($5, 1) IS DISTINCT FROM 1
                THEN 'INVALID_STATE'
                ELSE pgtrickle._centroid_rebase_reason_raw($1, $2, $3, $4, $5)
           END
$$;

REVOKE EXECUTE ON FUNCTION pgtrickle._centroid_initialization_sql(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle._centroid_staging_sql(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle._centroid_finalize_raw(double precision[], integer) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle._centroid_finalize(double precision[], integer) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle._centroid_merge_raw(double precision[], double precision[], integer, boolean) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle._centroid_merge(double precision[], double precision[], integer, boolean) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle._centroid_rebase_reason_raw(bigint, bigint, bigint, double precision[], double precision[]) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle._centroid_rebase_reason(bigint, bigint, bigint, double precision[], double precision[]) FROM PUBLIC;

ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS ann_refresh_epoch BIGINT DEFAULT 0
        CHECK (ann_refresh_epoch IS NULL OR ann_refresh_epoch >= 0),
    ADD COLUMN IF NOT EXISTS ann_churn_generation BIGINT DEFAULT 0
        CHECK (ann_churn_generation IS NULL OR ann_churn_generation >= 0),
    ADD COLUMN IF NOT EXISTS ann_churn_operations BIGINT DEFAULT 0
        CHECK (ann_churn_operations IS NULL OR ann_churn_operations >= 0);

CREATE TABLE pgtrickle.pgt_ann_maintenance_requests (
    request_id BIGSERIAL PRIMARY KEY,
    pgt_id BIGINT NOT NULL REFERENCES pgtrickle.pgt_stream_tables(pgt_id) ON DELETE CASCADE,
    database_oid OID NOT NULL,
    system_identifier TEXT NOT NULL,
    root_relid OID NOT NULL,
    leaf_relid OID NOT NULL,
    index_relid OID NOT NULL,
    index_filenode OID NOT NULL,
    index_definition TEXT NOT NULL,
    first_observed_epoch BIGINT,
    last_observed_epoch BIGINT,
    state TEXT NOT NULL DEFAULT 'REQUESTED' CHECK (state IN ('REQUESTED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
    claim_pid INTEGER,
    claim_backend_start TIMESTAMPTZ,
    claim_role OID,
    claim_epoch BIGINT,
    claim_churn_generation BIGINT,
    claim_churn_operations BIGINT,
    pending_during_run BOOLEAN NOT NULL DEFAULT FALSE,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    last_error TEXT,
    completed_at TIMESTAMPTZ,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_requested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (pgt_id, index_relid, index_filenode)
);
REVOKE ALL ON TABLE pgtrickle.pgt_ann_maintenance_requests FROM PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('pgtrickle.pgt_ann_maintenance_requests', 'WHERE false');

-- Serialize claims for the same leaf and index definition across REINDEX swaps.
CREATE TABLE pgtrickle.pgt_ann_maintenance_claims (
    pgt_id BIGINT NOT NULL REFERENCES pgtrickle.pgt_stream_tables(pgt_id) ON DELETE CASCADE,
    leaf_relid OID NOT NULL,
    index_key BYTEA NOT NULL,
    request_id BIGINT REFERENCES pgtrickle.pgt_ann_maintenance_requests(request_id) ON DELETE CASCADE,
    claim_pid INTEGER,
    claim_backend_start TIMESTAMPTZ,
    next_eligible_at TIMESTAMPTZ,
    reported_covered_epoch BIGINT CHECK (reported_covered_epoch IS NULL OR reported_covered_epoch >= 0),
    reported_churn_generation BIGINT,
    reported_churn_operations BIGINT,
    reported_covered_at TIMESTAMPTZ,
    consecutive_failures SMALLINT NOT NULL DEFAULT 0 CHECK (consecutive_failures BETWEEN 0 AND 6),
    PRIMARY KEY (pgt_id, leaf_relid, index_key)
);
REVOKE ALL ON TABLE pgtrickle.pgt_ann_maintenance_claims FROM PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('pgtrickle.pgt_ann_maintenance_claims', 'WHERE false');

CREATE FUNCTION pgtrickle.request_ann_maintenance(stream_table oid, index_oid oid)
RETURNS bigint STRICT SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c AS 'MODULE_PATHNAME', 'request_ann_maintenance_wrapper';
REVOKE EXECUTE ON FUNCTION pgtrickle.request_ann_maintenance(oid, oid) FROM PUBLIC;
CREATE FUNCTION pgtrickle.request_ann_maintenance_if_needed(
    stream_table oid, index_oid oid, estimated_rows bigint,
    estimate_generation bigint, estimate_operations bigint
) RETURNS jsonb STRICT SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c AS 'MODULE_PATHNAME', 'request_ann_maintenance_if_needed_wrapper';
REVOKE EXECUTE ON FUNCTION pgtrickle.request_ann_maintenance_if_needed(oid, oid, bigint, bigint, bigint) FROM PUBLIC;
CREATE FUNCTION pgtrickle.ann_maintenance_request(request_id bigint)
RETURNS jsonb STRICT SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c AS 'MODULE_PATHNAME', 'ann_maintenance_request_wrapper';
REVOKE EXECUTE ON FUNCTION pgtrickle.ann_maintenance_request(bigint) FROM PUBLIC;
CREATE FUNCTION pgtrickle.list_ann_maintenance_requests(after_request_id bigint, max_results integer)
RETURNS SETOF jsonb STRICT SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c AS 'MODULE_PATHNAME', 'list_ann_maintenance_requests_wrapper';
REVOKE EXECUTE ON FUNCTION pgtrickle.list_ann_maintenance_requests(bigint, integer) FROM PUBLIC;
CREATE FUNCTION pgtrickle.cancel_ann_maintenance(request_id bigint)
RETURNS jsonb STRICT SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c AS 'MODULE_PATHNAME', 'cancel_ann_maintenance_wrapper';
REVOKE EXECUTE ON FUNCTION pgtrickle.cancel_ann_maintenance(bigint) FROM PUBLIC;
CREATE FUNCTION pgtrickle.claim_ann_maintenance(request_id bigint)
RETURNS jsonb STRICT SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c AS 'MODULE_PATHNAME', 'claim_ann_maintenance_wrapper';
REVOKE EXECUTE ON FUNCTION pgtrickle.claim_ann_maintenance(bigint) FROM PUBLIC;
CREATE FUNCTION pgtrickle.finish_ann_maintenance(request_id bigint, succeeded boolean, error_message text)
RETURNS void STRICT SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c AS 'MODULE_PATHNAME', 'finish_ann_maintenance_wrapper';
REVOKE EXECUTE ON FUNCTION pgtrickle.finish_ann_maintenance(bigint, boolean, text) FROM PUBLIC;

CREATE FUNCTION pgtrickle.embedding_stream_table_v2(
    name TEXT, source_table TEXT, vector_column TEXT, metric TEXT,
    options JSONB DEFAULT NULL, dry_run BOOLEAN DEFAULT false
) RETURNS TABLE(action TEXT)
LANGUAGE c AS 'MODULE_PATHNAME', 'embedding_stream_table_v2_wrapper';
REVOKE EXECUTE ON FUNCTION pgtrickle.embedding_stream_table_v2(text, text, text, text, jsonb, boolean) FROM PUBLIC;

ALTER TABLE pgtrickle.pgt_distance_subscriptions
    ADD COLUMN pgt_id BIGINT REFERENCES pgtrickle.pgt_stream_tables(pgt_id) ON DELETE CASCADE,
    DROP CONSTRAINT pgt_distance_subscriptions_pkey,
    DROP CONSTRAINT pgt_distance_subscriptions_op_check,
    DROP CONSTRAINT pgt_distance_subscriptions_threshold_check,
    ADD CONSTRAINT pgt_distance_subscriptions_pgt_id_channel_key UNIQUE (pgt_id, channel);

-- Legacy threshold values may have been corrupted by the old parameter
-- binding. Preserve every row, but require explicit re-registration.
CREATE UNIQUE INDEX IF NOT EXISTS pgt_distance_subscriptions_legacy_name_channel_idx
    ON pgtrickle.pgt_distance_subscriptions (stream_table, channel) WHERE pgt_id IS NULL;

ALTER TABLE pgtrickle.pgt_distance_subscriptions
    ADD CONSTRAINT pgt_distance_subscriptions_op_check
        CHECK (op IN ('<->', '<=>', '<#>', '<+>', '<~>', '<%>')) NOT VALID,
    ADD CONSTRAINT pgt_distance_subscriptions_threshold_check
        CHECK (threshold NOT IN ('NaN'::float8, 'Infinity'::float8, '-Infinity'::float8)) NOT VALID;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pgtrickle.pgt_distance_subscriptions WHERE pgt_id IS NULL) THEN
        RAISE WARNING 'Legacy distance subscriptions are retained inactive because their thresholds may be incorrect; inspect and re-register them';
    END IF;
END $$;

ALTER FUNCTION pgtrickle.subscribe_distance(text, text, text, text, text, double precision)
    SECURITY DEFINER SET search_path = pgtrickle, pg_catalog, pg_temp; -- nosemgrep: sql.security-definer.present -- pinned path; caller ownership is checked.
ALTER FUNCTION pgtrickle.unsubscribe_distance(text, text)
    SECURITY DEFINER SET search_path = pgtrickle, pg_catalog, pg_temp; -- nosemgrep: sql.security-definer.present -- pinned path; caller ownership is checked.
ALTER FUNCTION pgtrickle.list_distance_subscriptions(text)
    SECURITY DEFINER SET search_path = pgtrickle, pg_catalog, pg_temp; -- nosemgrep: sql.security-definer.present -- pinned path; results are owner-filtered.

CREATE FUNCTION pgtrickle.vector_stream_status_v2() RETURNS SETOF jsonb
LANGUAGE c STRICT SECURITY DEFINER SET search_path = pgtrickle, pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'vector_stream_status_v2_wrapper';
CREATE FUNCTION pgtrickle.ann_index_status_v2() RETURNS SETOF jsonb
LANGUAGE c STRICT SECURITY DEFINER SET search_path = pgtrickle, pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ann_index_status_v2_wrapper';
GRANT EXECUTE ON FUNCTION pgtrickle.vector_stream_status_v2() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.ann_index_status_v2() TO PUBLIC;
