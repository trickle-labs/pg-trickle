WITH candidates AS MATERIALIZED (
    SELECT tenant_id, id, embedding
    FROM hybrid_search_corpus
    WHERE tenant_id = $1::text::uuid
    ORDER BY embedding::halfvec(3) <=> $2::text::halfvec
    LIMIT 20
)
SELECT tenant_id::text, id, embedding::vector(3) <=> $2::text::vector AS distance
FROM candidates
ORDER BY distance, id
LIMIT 10
