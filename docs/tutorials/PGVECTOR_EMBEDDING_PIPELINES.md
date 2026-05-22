# Maintaining Centroids with pgVectorMV

**pg_trickle** — F4: pgVectorMV incremental vector aggregate operators

## Overview

AI and RAG pipelines frequently compute per-entity centroid vectors for
retrieval, clustering, or personalized ranking. Recomputing centroids from
scratch on every refresh is expensive — `O(N)` in the number of source rows
rather than `O(delta)`.

pgVectorMV adds vector-aware aggregate support to the DVM engine. Stream
tables computing `avg(embedding)` or `sum(embedding)` over a
[pgvector](https://github.com/pgvector/pgvector) `vector` column are
maintained correctly under differential refresh.

## Prerequisites

```sql
-- Install pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Enable pgVectorMV in pg_trickle (Suset privilege required)
ALTER SYSTEM SET pg_trickle.enable_vector_agg = on;
SELECT pg_reload_conf();
```

## Example: User-taste centroid for RAG personalization

```sql
-- Source table: document embeddings per user
CREATE TABLE user_embeddings (
    id        BIGSERIAL PRIMARY KEY,
    user_id   INT NOT NULL,
    doc_id    BIGINT NOT NULL,
    embedding vector(1536) NOT NULL
);

-- Stream table: per-user centroid (incremental avg)
SELECT pgtrickle.create_stream_table(
    'user_centroid',
    $$
        SELECT user_id, avg(embedding) AS centroid
        FROM user_embeddings
        GROUP BY user_id
    $$,
    schedule => '5s',
    refresh_mode => 'DIFFERENTIAL'
);

-- HNSW index on the centroid for fast ANN search
CREATE INDEX user_centroid_hnsw_idx
    ON user_centroid USING hnsw (centroid vector_cosine_ops);
```

Now when documents are inserted or updated, `pg_trickle` re-aggregates only
the affected user groups (group-rescan strategy), leaving unchanged groups
untouched.

```sql
-- Insert new document embedding for user 42
INSERT INTO user_embeddings (user_id, doc_id, embedding)
VALUES (42, 999, '[0.1, 0.2, ...]');

-- Next scheduled refresh (or manual):
SELECT pgtrickle.refresh_stream_table('user_centroid');

-- Query: nearest users to a query embedding
SELECT user_id, centroid <=> '[0.05, 0.15, ...]'::vector AS distance
FROM user_centroid
ORDER BY centroid <=> '[0.05, 0.15, ...]'::vector
LIMIT 10;
```

## Example: Cluster sum for IVF index maintenance

```sql
CREATE TABLE doc_embeddings (
    id         BIGSERIAL PRIMARY KEY,
    cluster_id INT NOT NULL,
    embedding  vector(768) NOT NULL
);

SELECT pgtrickle.create_stream_table(
    'cluster_sum',
    $$
        SELECT cluster_id,
               sum(embedding)  AS vec_sum,
               count(*)        AS doc_count
        FROM doc_embeddings
        GROUP BY cluster_id
    $$,
    schedule => '1m',
    refresh_mode => 'DIFFERENTIAL'
);
```

## Refresh strategy

The current implementation uses the **group-rescan strategy** for vector aggregates:
any insert/delete/update affecting a group triggers a full re-aggregation
of that group using PostgreSQL's native `avg(vector)` or `sum(vector)`
aggregates from pgvector. Groups that were not affected are not touched.

This is:
- **Correct**: always produces the same result as a FULL refresh
- **Efficient for skewed workloads**: if only a few user groups are updated
  per refresh cycle, only those groups are re-computed
- **Safe under concurrent updates**: no state accumulation errors possible

A fully algebraic strategy (maintaining running `sum` + `count` auxiliary
columns without group rescans) is planned.

## Distance operators and ANN queries

pgvector distance operators (`<->`, `<=>`, `<#>`, `<+>`) in `WHERE`
predicates or `ORDER BY` clauses are **FULL-fallback safe**: pg_trickle
detects these operators and falls back to FULL refresh automatically. This
is expected and safe — distance operators are non-monotone and cannot be
differentiated.

```sql
-- This defining query uses a distance operator → FULL refresh mode only
SELECT pgtrickle.create_stream_table(
    'nearest_docs',
    $$
        SELECT id, embedding
        FROM doc_embeddings
        ORDER BY embedding <-> '[0.1, 0.2, ...]'
        LIMIT 20
    $$,
    refresh_mode => 'FULL'  -- always use FULL for ANN/KNN queries
);
```

## Monitoring

```sql
-- Check effective refresh mode (DIFFERENTIAL vs FULL fallback)
SELECT pgt_name, last_effective_mode, last_refresh_duration_ms
FROM pgtrickle.pgt_stream_tables
WHERE pgt_name IN ('user_centroid', 'cluster_sum');
```

## Cookbook: centroid similarity search

```sql
-- Find documents similar to a user's taste centroid
WITH user_vec AS (
    SELECT centroid FROM user_centroid WHERE user_id = $1
)
SELECT d.id, d.title, d.embedding <=> (SELECT centroid FROM user_vec) AS distance
FROM documents d
ORDER BY d.embedding <=> (SELECT centroid FROM user_vec)
LIMIT 10;
```

## See also

- [pgvector documentation](https://github.com/pgvector/pgvector)
- [PGVECTOR_TOOLING_LANDSCAPE.md](../../blog/pgvector-tooling-landscape.md) — pg_trickle in the pgvector ecosystem
- [Incremental pgvector](../../blog/incremental-pgvector.md)
- [HNSW recall distribution drift](../../blog/hnsw-recall-distribution-drift.md)
