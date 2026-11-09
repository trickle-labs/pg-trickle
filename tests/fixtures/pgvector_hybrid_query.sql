WITH vector_candidates AS MATERIALIZED (
    SELECT tenant_id, id, embedding::vector(3) <=> $2::text::vector AS distance
    FROM hybrid_search_corpus
    WHERE tenant_id = $1::text::uuid
    ORDER BY embedding::vector(3) <=> $2::text::vector
    LIMIT 20
), lexical_candidates AS MATERIALIZED (
    SELECT tenant_id, id, ts_rank(fts, plainto_tsquery('english', $3::text)) AS score
    FROM hybrid_search_corpus
    WHERE tenant_id = $1::text::uuid AND fts @@ plainto_tsquery('english', $3::text)
    ORDER BY score DESC, id
    LIMIT 20
), ranked AS (
    SELECT tenant_id, id, 1.0 / (60 + row_number() OVER (ORDER BY distance, id)) AS score
    FROM vector_candidates
    UNION ALL
    SELECT tenant_id, id, 1.0 / (60 + row_number() OVER (ORDER BY score DESC, id)) AS score
    FROM lexical_candidates
)
SELECT tenant_id::text, id, sum(score)::double precision AS score
FROM ranked
GROUP BY tenant_id, id
ORDER BY score DESC, id
LIMIT 10
