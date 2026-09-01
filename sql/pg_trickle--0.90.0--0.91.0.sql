-- pg_trickle 0.90.0 -> 0.91.0

-- v0.91.0 keeps the catalog additions introduced by earlier releases and adds
-- the query-evolution explanation and explicit source-DDL repair entry points.
CREATE OR REPLACE FUNCTION pgtrickle."explain_alter"(
    "name" TEXT,
    "new_query" TEXT
) RETURNS JSONB
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'explain_alter_wrapper';

CREATE OR REPLACE FUNCTION pgtrickle."reinitialize_stream_table"(
    "name" TEXT
) RETURNS TEXT
STRICT SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c
AS 'MODULE_PATHNAME', 'reinitialize_stream_table_wrapper';

REVOKE EXECUTE ON FUNCTION pgtrickle.reinitialize_stream_table(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.explain_alter(text, text) TO PUBLIC;

INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES (
    '0.91.0',
    'Safe defining-query replacement and source schema-evolution recovery'
)
ON CONFLICT (version) DO NOTHING;
