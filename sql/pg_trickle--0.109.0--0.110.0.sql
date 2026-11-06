-- Add the metric-aware embedding helper without changing legacy calls.
CREATE FUNCTION pgtrickle.embedding_stream_table_v2(
    name TEXT, source_table TEXT, vector_column TEXT, metric TEXT,
    options JSONB DEFAULT NULL, dry_run BOOLEAN DEFAULT false
) RETURNS TABLE(action TEXT)
LANGUAGE c AS 'MODULE_PATHNAME', 'embedding_stream_table_v2_wrapper';
REVOKE EXECUTE ON FUNCTION pgtrickle.embedding_stream_table_v2(text, text, text, text, jsonb, boolean) FROM PUBLIC;
