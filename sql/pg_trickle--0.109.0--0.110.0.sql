-- Add the metric-aware embedding helper without changing legacy calls.
CREATE FUNCTION pgtrickle.embedding_stream_table_v2(
    name TEXT, source_table TEXT, vector_column TEXT, metric TEXT,
    options JSONB DEFAULT NULL, dry_run BOOLEAN DEFAULT false
) RETURNS TABLE(action TEXT)
LANGUAGE c AS 'MODULE_PATHNAME', 'embedding_stream_table_v2_wrapper';
REVOKE EXECUTE ON FUNCTION pgtrickle.embedding_stream_table_v2(text, text, text, text, jsonb, boolean) FROM PUBLIC;

CREATE FUNCTION pgtrickle.vector_stream_status_v2() RETURNS SETOF jsonb
LANGUAGE c STRICT SECURITY DEFINER SET search_path = pgtrickle, pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'vector_stream_status_v2_wrapper';
CREATE FUNCTION pgtrickle.ann_index_status_v2() RETURNS SETOF jsonb
LANGUAGE c STRICT SECURITY DEFINER SET search_path = pgtrickle, pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ann_index_status_v2_wrapper';
GRANT EXECUTE ON FUNCTION pgtrickle.vector_stream_status_v2() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.ann_index_status_v2() TO PUBLIC;
