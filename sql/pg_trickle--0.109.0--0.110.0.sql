-- Add the metric-aware embedding helper without changing legacy calls.
ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS ann_refresh_epoch BIGINT DEFAULT 0
        CHECK (ann_refresh_epoch IS NULL OR ann_refresh_epoch >= 0);

CREATE FUNCTION pgtrickle.embedding_stream_table_v2(
    name TEXT, source_table TEXT, vector_column TEXT, metric TEXT,
    options JSONB DEFAULT NULL, dry_run BOOLEAN DEFAULT false
) RETURNS TABLE(action TEXT)
LANGUAGE c AS 'MODULE_PATHNAME', 'embedding_stream_table_v2_wrapper';
REVOKE EXECUTE ON FUNCTION pgtrickle.embedding_stream_table_v2(text, text, text, text, jsonb, boolean) FROM PUBLIC;
