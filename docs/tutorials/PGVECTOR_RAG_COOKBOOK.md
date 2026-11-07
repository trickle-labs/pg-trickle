# pgvector RAG Cookbook: Always-Fresh Embedding Pipelines with pg_trickle

> **Status: Stable** — pgvector RAG pipelines using stream tables are production-ready.

**Prerequisites:** PostgreSQL 18, `pg_trickle`, `pgvector` extension.

---

## Overview

This cookbook shows you how to build a production-ready Retrieval-Augmented
Generation (RAG) pipeline using `pg_trickle` and `pgvector`. The key insight:
embeddings are **derived state** — they must be recomputed whenever source
documents change. `pg_trickle` incremental view maintenance keeps that derived
state fresh automatically, without batch jobs or cron scripts.

Embeddings are **generated at the application layer** (via OpenAI, Cohere,
Ollama, or any embedding API) and stored in a source table. `pg_trickle` then
incrementally maintains the denormalised corpus, the vector index, and any
aggregate centroids.

---

## Pattern 1: Pre-computed Embeddings with Always-Fresh Search Corpus

### Setup

```sql
-- Source table: documents with embeddings set by your application
CREATE TABLE documents (
    id          BIGSERIAL PRIMARY KEY,
    title       TEXT NOT NULL,
    body        TEXT NOT NULL,
    embedding   vector(1536),    -- OpenAI text-embedding-3-small
    tags        TEXT[],
    owner_id    BIGINT NOT NULL,
    updated_at  TIMESTAMPTZ DEFAULT now()
);

-- Stream table: denormalized search corpus, always fresh
SELECT pgtrickle.create_stream_table(
    'search_corpus',
    $$
        SELECT
            d.id,
            d.title,
            d.body,
            d.embedding,
            d.tags,
            d.owner_id,
            d.updated_at
        FROM documents d
        WHERE d.embedding IS NOT NULL
    $$,
    schedule         => '5s',
    refresh_mode     => 'AUTO',
    post_refresh_action => 'analyze'   -- VP-1: keep statistics fresh
);
```

### Hybrid Search Query

```sql
-- Hybrid search: combine BM25 keyword + cosine vector similarity
SELECT
    sc.id,
    sc.title,
    ts_rank_cd(to_tsvector('english', sc.body), query) AS bm25_score,
    1 - (sc.embedding <=> $1::vector) AS cosine_score
FROM
    search_corpus sc,
    to_tsquery('english', $2) AS query
WHERE
    to_tsvector('english', sc.body) @@ query
ORDER BY
    cosine_score DESC, bm25_score DESC
LIMIT 20;
```

---

## Pattern 2: Tenant-Isolated Embedding Corpus with RLS

```sql
-- Per-tenant document table
CREATE TABLE tenant_docs (
    id         BIGSERIAL PRIMARY KEY,
    tenant_id  BIGINT NOT NULL,
    content    TEXT NOT NULL,
    embedding  vector(1536),
    created_at TIMESTAMPTZ DEFAULT now()
);

ALTER TABLE tenant_docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON tenant_docs
    USING (tenant_id = current_setting('app.tenant_id')::BIGINT);

-- Stream table: per-tenant corpus, differential refresh
SELECT pgtrickle.create_stream_table(
    'tenant_corpus',
    $$
        SELECT id, tenant_id, content, embedding
        FROM tenant_docs
        WHERE embedding IS NOT NULL
    $$,
    schedule => '10s',
    refresh_mode => 'AUTO'
);
```

---

## Pattern 3: Changed-Row-Triggered Reindexing (VP-1/VP-2)

`post_refresh_action = 'reindex_if_drift'` requests a table-wide, non-concurrent
`REINDEX` after the changed-row counter reaches a fraction of the estimated
table size. This ratio is a churn estimate, not a measurement of index quality,
semantic drift, or recall. Rebuilding every index on the table can block writes;
measure the effect on your workload before enabling it.

```sql
-- Create a vector stream table
SELECT pgtrickle.create_stream_table(
    'embedding_store',
    $$
        SELECT id, body, embedding
        FROM documents
        WHERE embedding IS NOT NULL
    $$,
    schedule => '30s'
);

-- Create the HNSW index on the stream table's storage table
CREATE INDEX idx_embedding_store_hnsw
    ON embedding_store USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- Configure drift-triggered REINDEX (20% threshold, the default)
SELECT pgtrickle.alter_stream_table(
    'embedding_store',
    post_refresh_action     => 'reindex_if_drift',
    reindex_drift_threshold => 0.20   -- REINDEX when 20% of rows have changed
);
```

### Inspect the Changed-Row Estimate

```sql
-- This function lists only stream tables with a non-'none' post-refresh action.
SELECT
    name,
    post_refresh_action,
    rows_changed_since_last_reindex,
    estimated_rows,
    drift_pct AS changed_rows_pct,
    last_reindex_at,
    embedding_lag AS time_since_data_timestamp
FROM pgtrickle.vector_status();
```

`drift_pct` is the changed-row counter divided by PostgreSQL's estimated row
count, expressed as a percentage; it is NULL when the estimate is not positive.
`embedding_lag` is the time since the stream's data timestamp. An idle corpus
can have an old timestamp even when it is current, and neither field reports
whether an embedding matches the latest document revision or model. Track that
freshness with application-provided revision and model metadata.

---

## Pattern 4: Centroid Maintenance for Cluster-Aware Search

`pg_trickle` supports `vector_avg()` for per-cluster centroid maintenance.
This enables fast cluster-first ANN search on large corpora.

```sql
-- Cluster assignments: updated by your ML pipeline
CREATE TABLE cluster_assignments (
    doc_id     BIGINT PRIMARY KEY,
    cluster_id INTEGER NOT NULL,
    embedding  vector(1536)
);

-- Stream table: per-cluster centroids, maintained incrementally
SELECT pgtrickle.create_stream_table(
    'cluster_centroids',
    $$
        SELECT
            cluster_id,
            vector_avg(embedding) AS centroid,
            count(*) AS member_count
        FROM cluster_assignments
        GROUP BY cluster_id
    $$,
    schedule => '1m',
    refresh_mode => 'DIFFERENTIAL'   -- incremental AVG maintenance
);
```

> **Note:** `vector_avg()` requires `pg_trickle.enable_vector_agg = on` in
> `postgresql.conf` and the `pgvector` extension to be installed.

---

## Pattern 5: Full Corpus ANALYZE After Every Refresh

For smaller tables that refresh frequently, running ANALYZE after each refresh
ensures the query planner always sees accurate row estimates, which is critical
for HNSW index-scan decisions:

```sql
SELECT pgtrickle.alter_stream_table(
    'embedding_store',
    post_refresh_action => 'analyze'
);
```

---

## Operational Sizing Guidance

Choose maintenance from measurements of your own query and update workload.
`analyze` updates planner statistics; `reindex_if_drift` can rebuild all indexes
on the table synchronously. With `none`, manage any index maintenance yourself.
Use `EXPLAIN` to check a query plan and an application-specific benchmark to
measure recall and latency.

---

## Monitoring

```sql
-- Inspect refresh timestamp age and changed-row estimates.
SELECT
    name,
    post_refresh_action,
    data_timestamp,
    embedding_lag AS time_since_data_timestamp,
    drift_pct AS changed_rows_pct,
    last_reindex_at
FROM pgtrickle.vector_status()
ORDER BY drift_pct DESC NULLS LAST;
```

This query does not establish refresh backlog, embedding freshness, index
validity, or recall. Tables with `post_refresh_action = 'none'` are omitted.

---

## Frequently Asked Questions

**Q: Can pg_trickle generate embeddings automatically?**
No. Embeddings are generated at the application layer (e.g., via the OpenAI
API, Ollama, or pgai). pg_trickle maintains the derived state once embeddings
are stored in the source table.

**Q: Should I use IVFFlat or HNSW with pg_trickle?**
Both support approximate search with different build, memory, and query
tradeoffs. Benchmark recall and latency for your data and workload. Rebuilding
may be useful as data changes, but the changed-row threshold does not measure
recall or establish when a rebuild is needed.

**Q: What is the `reindex_drift_threshold`?**
It is the fraction of estimated rows that must change since the last REINDEX
before a drift-triggered REINDEX fires. The default is 0.20 (20%). You can
set a per-table override via `ALTER STREAM TABLE ... reindex_drift_threshold`.

**Q: Does REINDEX block reads?**
pg_trickle uses the standard, non-concurrent `REINDEX TABLE` form, which can
block writes while indexes are rebuilt. If that is unsuitable, set
`post_refresh_action = 'none'` and manage index maintenance separately using
the PostgreSQL operation appropriate for your workload.
