-- Add the metric-aware embedding helper without changing legacy calls.
ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS ann_refresh_epoch BIGINT DEFAULT 0
        CHECK (ann_refresh_epoch IS NULL OR ann_refresh_epoch >= 0);

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
    requested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_requested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (pgt_id, index_relid, index_filenode)
);
REVOKE ALL ON TABLE pgtrickle.pgt_ann_maintenance_requests FROM PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('pgtrickle.pgt_ann_maintenance_requests', 'WHERE false');

CREATE FUNCTION pgtrickle.request_ann_maintenance(stream_table oid, index_oid oid)
RETURNS bigint STRICT SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c AS 'MODULE_PATHNAME', 'request_ann_maintenance_wrapper';
REVOKE EXECUTE ON FUNCTION pgtrickle.request_ann_maintenance(oid, oid) FROM PUBLIC;
CREATE FUNCTION pgtrickle.ann_maintenance_request(request_id bigint)
RETURNS jsonb STRICT SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c AS 'MODULE_PATHNAME', 'ann_maintenance_request_wrapper';
REVOKE EXECUTE ON FUNCTION pgtrickle.ann_maintenance_request(bigint) FROM PUBLIC;

CREATE FUNCTION pgtrickle.embedding_stream_table_v2(
    name TEXT, source_table TEXT, vector_column TEXT, metric TEXT,
    options JSONB DEFAULT NULL, dry_run BOOLEAN DEFAULT false
) RETURNS TABLE(action TEXT)
LANGUAGE c AS 'MODULE_PATHNAME', 'embedding_stream_table_v2_wrapper';
REVOKE EXECUTE ON FUNCTION pgtrickle.embedding_stream_table_v2(text, text, text, text, jsonb, boolean) FROM PUBLIC;
