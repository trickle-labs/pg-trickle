-- Invoke with -v corpus=application_current (or periodic_full_current /
-- differential_current), -v tenant=10, -v query_vector='[1,0,0]', -v terms=alpha.
WITH vector_candidates AS MATERIALIZED (
    SELECT id, embedding::vector(3) <=> :'query_vector'::vector AS distance
    FROM live_rag.:"corpus"
    WHERE tenant_id = :'tenant'::bigint
    ORDER BY embedding::vector(3) <=> :'query_vector'::vector
    LIMIT 20
), lexical_candidates AS MATERIALIZED (
    SELECT id, ts_rank(to_tsvector('simple', body), plainto_tsquery('simple', :'terms')) AS score
    FROM live_rag.:"corpus"
    WHERE tenant_id = :'tenant'::bigint
      AND to_tsvector('simple', body) @@ plainto_tsquery('simple', :'terms')
    ORDER BY score DESC, id
    LIMIT 20
), ranked AS (
    SELECT id, 1.0 / (60 + row_number() OVER (ORDER BY distance, id)) AS score FROM vector_candidates
    UNION ALL
    SELECT id, 1.0 / (60 + row_number() OVER (ORDER BY score DESC, id)) AS score FROM lexical_candidates
)
SELECT id, sum(score)::double precision AS score FROM ranked
GROUP BY id ORDER BY score DESC, id LIMIT 10;
