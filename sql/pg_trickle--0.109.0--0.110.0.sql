-- Add the metric-aware embedding helper without changing legacy calls.
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
    SECURITY DEFINER SET search_path = pgtrickle, pg_catalog, pg_temp;
ALTER FUNCTION pgtrickle.unsubscribe_distance(text, text)
    SECURITY DEFINER SET search_path = pgtrickle, pg_catalog, pg_temp;
ALTER FUNCTION pgtrickle.list_distance_subscriptions(text)
    SECURITY DEFINER SET search_path = pgtrickle, pg_catalog, pg_temp;

CREATE FUNCTION pgtrickle.vector_stream_status_v2() RETURNS SETOF jsonb
LANGUAGE c STRICT SECURITY DEFINER SET search_path = pgtrickle, pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'vector_stream_status_v2_wrapper';
CREATE FUNCTION pgtrickle.ann_index_status_v2() RETURNS SETOF jsonb
LANGUAGE c STRICT SECURITY DEFINER SET search_path = pgtrickle, pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ann_index_status_v2_wrapper';
GRANT EXECUTE ON FUNCTION pgtrickle.vector_stream_status_v2() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.ann_index_status_v2() TO PUBLIC;
