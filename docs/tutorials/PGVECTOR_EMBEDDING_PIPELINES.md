# Maintaining Centroids with pgVectorMV

> **Status:** Vector aggregates are experimental and disabled by default. The
> `attach_embedding_outbox` integration is **Beta**. See the
> [support summary](../SUPPORT_SUMMARY.md) for capability-specific maturity.

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

pg_trickle targets PostgreSQL 18. The
[pgvector changelog](https://github.com/pgvector/pgvector/blob/master/CHANGELOG.md)
first lists PostgreSQL 18 support in pgvector 0.8.1. The bounded compatibility
suite tests 0.8.1 and 0.8.7; this is not a blanket support claim or a
recommendation to deploy the older release. Later pgvector releases fix index
and aggregate defects, so use a maintained release and review its release notes.

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

The current implementation uses the **group-rescan strategy** for vector
aggregates: an insert, delete or update re-aggregates affected groups using
pgvector's native `avg(vector)` or `sum(vector)`. For supported query shapes,
the group filter is pushed into the source query; other shapes may need to
recompute more of the defining query. Inspect the actual plan and workload
before relying on a particular cost.

The rescan does not accumulate a running vector sum across refreshes. Floating
point aggregation can still depend on input order, so a fresh FULL refresh and
a group rescan are not guaranteed to be bit-identical. The packaged regression
tests include cancellation, NULLs and repeated mutations, and compare each
component with native AVG using an absolute tolerance of `1e-5` plus a relative
tolerance of `1e-6`; those tests are evidence for their inputs, not a universal
error bound. A float32 `sum(vector)` can lose contributions that native AVG's
double-precision state retains. A fully algebraic strategy with a qualified
numerical and recovery contract is not yet implemented.

## Distance operators and ANN queries

Row-local distance projections and predicates are distinct from maintaining a
global nearest-neighbor ranking. A fixed-pivot distance can be evaluated on
changed rows; support still depends on the complete query and its expressions.
Inspect the effective refresh mode rather than assuming an operator forces FULL.
For `ORDER BY ... LIMIT`, the Top-K path re-evaluates the complete ordered
query on refresh; it is not an incremental ANN graph. See the
[Top-K contract](../SQL_REFERENCE.md).

```sql
-- Explicit FULL refresh for a bounded nearest-neighbor result
SELECT pgtrickle.create_stream_table(
    'nearest_docs',
    $$
        SELECT id, embedding
        FROM doc_embeddings
        ORDER BY embedding <-> '[0.1, 0.2, ...]'
        LIMIT 20
    $$,
    refresh_mode => 'FULL'
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
