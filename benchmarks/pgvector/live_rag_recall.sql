\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ;
SET LOCAL enable_seqscan = on;
SET LOCAL enable_indexscan = off;
SET LOCAL enable_indexonlyscan = off;
SET LOCAL enable_bitmapscan = off;
\echo LIVE_RAG_EXACT_PLAN_BEGIN
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT id, embedding::vector(3) <=> :'query_vector'::vector AS distance
FROM live_rag.:"corpus" WHERE tenant_id = :'tenant'::bigint
ORDER BY embedding::vector(3) <=> :'query_vector'::vector LIMIT :'k'::integer;
\echo LIVE_RAG_EXACT_PLAN_END
CREATE TEMP TABLE live_rag_exact ON COMMIT DROP AS
SELECT id, embedding::vector(3) <=> :'query_vector'::vector AS distance
FROM live_rag.:"corpus" WHERE tenant_id = :'tenant'::bigint
ORDER BY embedding::vector(3) <=> :'query_vector'::vector LIMIT :'k'::integer;

SET LOCAL enable_indexscan = on;
SET LOCAL enable_indexonlyscan = on;
SET LOCAL enable_bitmapscan = on;
SET LOCAL hnsw.iterative_scan = :'iterative_scan';
CREATE TEMP TABLE live_rag_candidate ON COMMIT DROP AS
SELECT id, embedding::vector(3) <=> :'query_vector'::vector AS distance
FROM live_rag.:"corpus" WHERE tenant_id = :'tenant'::bigint
ORDER BY embedding::vector(3) <=> :'query_vector'::vector LIMIT :'k'::integer;
\echo LIVE_RAG_PLANNER_PLAN_BEGIN
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT id, embedding::vector(3) <=> :'query_vector'::vector AS distance
FROM live_rag.:"corpus" WHERE tenant_id = :'tenant'::bigint
ORDER BY embedding::vector(3) <=> :'query_vector'::vector LIMIT :'k'::integer;
\echo LIVE_RAG_PLANNER_PLAN_END
SET LOCAL enable_seqscan = off;
\echo LIVE_RAG_ELIGIBILITY_PLAN_BEGIN
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT id, embedding::vector(3) <=> :'query_vector'::vector AS distance
FROM live_rag.:"corpus" WHERE tenant_id = :'tenant'::bigint
ORDER BY embedding::vector(3) <=> :'query_vector'::vector LIMIT :'k'::integer;
\echo LIVE_RAG_ELIGIBILITY_PLAN_END

SELECT 'LIVE_RAG_METRICS=' || jsonb_build_object(
    'tenant', :'tenant'::bigint, 'k', :'k'::integer,
    'exact_count', (SELECT count(*) FROM live_rag_exact),
    'candidate_count', (SELECT count(*) FROM live_rag_candidate),
    'matched', (SELECT count(*) FROM live_rag_candidate c JOIN live_rag_exact e USING (id)),
    'recall_at_k', CASE WHEN (SELECT count(*) FROM live_rag_exact) = 0 THEN NULL
        ELSE (SELECT count(*)::float8 FROM live_rag_candidate c JOIN live_rag_exact e USING (id))
             / (SELECT count(*) FROM live_rag_exact) END,
    'underfill', GREATEST(0, :'k'::integer - (SELECT count(*) FROM live_rag_candidate)),
    'exact_boundary_distance', (SELECT max(distance) FROM live_rag_exact),
    'boundary_tie_rows', (SELECT count(*) FROM live_rag.:"corpus"
        WHERE tenant_id = :'tenant'::bigint AND (embedding::vector(3) <=> :'query_vector'::vector)
            = (SELECT max(distance) FROM live_rag_exact)),
    'lexical_candidate_count', (SELECT count(*) FROM (
        SELECT id FROM live_rag.:"corpus" WHERE tenant_id = :'tenant'::bigint
          AND to_tsvector('simple', body) @@ plainto_tsquery('simple', :'terms') LIMIT 20
    ) lexical),
    'iterative_scan', current_setting('hnsw.iterative_scan'),
    'postgres', current_setting('server_version'),
    'pgvector', (SELECT extversion FROM pg_extension WHERE extname = 'vector'),
    'pg_trickle', (SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle')
)::text;
ROLLBACK;
