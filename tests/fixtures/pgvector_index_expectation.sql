-- Load with \i in the same psql session; the helper exists only for that session.
CREATE FUNCTION pg_temp.ann_expectation(
    p_stream regclass, p_index text, p_opclass text, p_expression text, p_predicate text
) RETURNS text LANGUAGE sql AS $$
    WITH visible AS (
        SELECT 1 FROM pgtrickle.vector_stream_status_v2() v
        WHERE (v->>'table_oid')::oid = p_stream LIMIT 1
    ), observed AS (
        SELECT v FROM pgtrickle.ann_index_status_v2() v
        WHERE (v->>'stream_table_oid')::oid = p_stream
          AND (v->>'index_oid')::oid = pg_catalog.to_regclass(p_index)
          AND (v->>'key_number')::int = 1
        LIMIT 1
    )
    SELECT CASE
        WHEN p_index IS NULL THEN 'exact_search'
        WHEN p_opclass IS NULL OR p_expression IS NULL THEN 'incomplete_expectation'
        WHEN pg_catalog.to_regclass(p_index) IS NULL THEN 'missing_index'
        WHEN observed.v IS NULL THEN 'unavailable_or_wrong_stream'
        WHEN observed.v->>'valid' <> 'true' OR observed.v->>'ready' <> 'true' THEN 'invalid_index'
        WHEN observed.v->>'opclass' <> p_opclass
          OR observed.v->>'opclass_schema' IS DISTINCT FROM (
              SELECT n.nspname::text FROM pg_catalog.pg_extension e
              JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = 'vector'
          ) THEN 'metric_mismatch'
        WHEN observed.v->>'expression' <> p_expression THEN 'expression_mismatch'
        WHEN observed.v->>'predicate' IS DISTINCT FROM p_predicate THEN 'predicate_mismatch'
        ELSE 'ready'
    END
    FROM visible LEFT JOIN observed ON true
$$;
