# Hybrid Search Patterns with pg_trickle

> VH-3 — Cookbook for BM25 + vector + metadata retrieval on
> incrementally maintained stream tables.

## Overview

pg_trickle makes it easy to maintain a hybrid-search corpus — combining
full-text (BM25) search, vector similarity, and structured metadata filters —
using a single stream table that stays fresh automatically.

---

## Pattern 1: Flat Denormalised Corpus

The simplest pattern: one stream table holds everything needed for a hybrid
search query.

```sql
-- Source tables
CREATE TABLE documents (
    id          BIGSERIAL PRIMARY KEY,
    title       TEXT NOT NULL,
    body        TEXT NOT NULL,
    category    TEXT,
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE document_embeddings (
    doc_id      BIGINT PRIMARY KEY REFERENCES documents(id),
    embedding   vector(1536) NOT NULL,
    updated_at  TIMESTAMPTZ DEFAULT now()
);

-- Hybrid search corpus stream table
SELECT pgtrickle.create_stream_table(
    'hybrid_corpus',
    $$
        SELECT
            d.id,
            d.title,
            d.body,
            d.category,
            d.created_at,
            e.embedding,
            to_tsvector('english', d.title || ' ' || d.body) AS fts_vector
        FROM documents d
        JOIN document_embeddings e ON e.doc_id = d.id
    $$,
    '30s',
    'DIFFERENTIAL'
);

-- Full-text index for BM25
CREATE INDEX ON hybrid_corpus USING gin(fts_vector);

-- Vector index for ANN search
CREATE INDEX ON hybrid_corpus USING hnsw(embedding vector_cosine_ops);
```

Alternatively, use the one-call API:

```sql
SELECT pgtrickle.embedding_stream_table(
    'hybrid_corpus_v2',
    'document_embeddings',
    'embedding',
    extra_columns => 'doc_id, updated_at'
);
```

### Hybrid query

```sql
-- Combine BM25 rank and cosine similarity
SELECT
    id,
    title,
    ts_rank(fts_vector, query) AS bm25_score,
    1 - (embedding <=> '[...]'::vector) AS cosine_score,
    ts_rank(fts_vector, query) * 0.4 + (1 - (embedding <=> '[...]'::vector)) * 0.6 AS hybrid_score
FROM
    hybrid_corpus,
    plainto_tsquery('english', 'your search terms') AS query
WHERE
    fts_vector @@ query
    OR embedding <=> '[...]'::vector < 0.3
ORDER BY hybrid_score DESC
LIMIT 20;
```

---

## Pattern 2: RLS-Scoped Corpus (Multi-Tenant)

Pattern 2 extends the flat corpus to enforce per-user or per-tenant isolation
using PostgreSQL Row-Level Security.  The stream table holds rows for all
tenants; an RLS policy on the stream table filters results at query time.

```sql
-- Source tables (with tenant_id)
CREATE TABLE tenant_documents (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   UUID    NOT NULL,
    title       TEXT    NOT NULL,
    body        TEXT    NOT NULL,
    category    TEXT,
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE tenant_doc_embeddings (
    doc_id      BIGINT PRIMARY KEY REFERENCES tenant_documents(id),
    tenant_id   UUID NOT NULL,
    embedding   vector(1536) NOT NULL,
    updated_at  TIMESTAMPTZ DEFAULT now()
);

-- Hybrid search corpus stream table for all tenants
SELECT pgtrickle.create_stream_table(
    'tenant_hybrid_corpus',
    $$
        SELECT
            d.id,
            d.tenant_id,
            d.title,
            d.body,
            d.category,
            d.created_at,
            e.embedding,
            to_tsvector('english', d.title || ' ' || d.body) AS fts_vector
        FROM tenant_documents d
        JOIN tenant_doc_embeddings e ON e.doc_id = d.id
    $$,
    '30s',
    'DIFFERENTIAL'
);

-- Full-text and vector indexes span all tenants
CREATE INDEX ON tenant_hybrid_corpus USING gin(fts_vector);
CREATE INDEX ON tenant_hybrid_corpus USING hnsw(embedding vector_cosine_ops);
CREATE INDEX ON tenant_hybrid_corpus (tenant_id);

-- Enable RLS for per-tenant isolation at query time
ALTER TABLE tenant_hybrid_corpus ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_hybrid_corpus FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant_hybrid_corpus
    FOR SELECT
    USING (tenant_id = current_setting('app.tenant_id', true)::uuid);
```

**Querying with RLS:**

```sql
-- Set tenant context (parameterised — never string-interpolated)
SET app.tenant_id = 'your-tenant-uuid';

SELECT
    id,
    title,
    ts_rank(fts_vector, query) AS bm25_score,
    1 - (embedding <=> '[...]'::vector) AS cosine_score,
    ts_rank(fts_vector, query) * 0.4
        + (1 - (embedding <=> '[...]'::vector)) * 0.6 AS hybrid_score
FROM
    tenant_hybrid_corpus,
    plainto_tsquery('english', 'your search terms') AS query
WHERE
    fts_vector @@ query
    OR embedding <=> '[...]'::vector < 0.3
ORDER BY hybrid_score DESC
LIMIT 20;
-- RLS policy automatically restricts to the current tenant
```

**GUC reference:**

| GUC | Default | Effect |
|-----|---------|--------|
| `pg_trickle.enable_vector_agg` | `off` | Enable `vector_avg` / `halfvec_avg` / `sparsevec_avg` aggregates in defining queries.  Must be `on` for centroid-style stream tables. |

Enable before creating centroid aggregates:

```sql
SET pg_trickle.enable_vector_agg = on;
```

---

## Pattern 3: Tiered Storage (halfvec + sparsevec)

Pattern 3 uses pg_trickle's `halfvec_avg` and `sparsevec_avg` support
(enabled via `pg_trickle.enable_vector_agg`) to maintain storage-efficient
index tiers alongside full-precision data.

```sql
SET pg_trickle.enable_vector_agg = on;

-- Tier 1: Full-precision embeddings (authoritative source)
CREATE TABLE raw_embeddings (
    id          BIGSERIAL PRIMARY KEY,
    category    TEXT NOT NULL,
    content     TEXT NOT NULL,
    embedding   vector(1536) NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT now()
);

-- Tier 2: Full-precision stream table with GIN + HNSW indexes
SELECT pgtrickle.create_stream_table(
    'embeddings_full',
    $$
        SELECT id, category, content, embedding,
               to_tsvector('english', content) AS fts_vector
        FROM raw_embeddings
    $$,
    '1m', 'DIFFERENTIAL'
);
CREATE INDEX ON embeddings_full USING gin(fts_vector);
CREATE INDEX ON embeddings_full USING hnsw(embedding vector_cosine_ops);

-- Tier 3: Half-precision stream table (50% storage savings, same recall for most models)
SELECT pgtrickle.create_stream_table(
    'embeddings_half',
    $$
        SELECT id, category, embedding::halfvec(1536) AS embedding
        FROM raw_embeddings
    $$,
    '1m', 'DIFFERENTIAL'
);
CREATE INDEX ON embeddings_half USING hnsw(embedding halfvec_cosine_ops);

-- Tier 4: Per-category centroids using vector_avg aggregate
SELECT pgtrickle.create_stream_table(
    'category_centroids',
    $$
        SELECT
            category,
            vector_avg(embedding)                  AS centroid,
            halfvec_avg(embedding::halfvec(1536))  AS centroid_half,
            COUNT(*)                               AS doc_count
        FROM raw_embeddings
        GROUP BY category
    $$,
    '5m', 'DIFFERENTIAL'
);
```

**Query pattern — route by tier based on use case:**

```sql
-- High-recall retrieval: full precision (best accuracy)
SELECT id, content, embedding <=> '[...]'::vector AS distance
FROM embeddings_full
ORDER BY embedding <=> '[...]'::vector
LIMIT 10;

-- Low-latency retrieval: half precision (smaller index, faster scan)
SELECT id, embedding <=> '[...]'::halfvec AS distance
FROM embeddings_half
ORDER BY embedding <=> '[...]'::halfvec
LIMIT 10;

-- Category routing: find best-matching category centroid first
SELECT category, centroid <=> '[...]'::vector AS centroid_distance
FROM category_centroids
ORDER BY centroid <=> '[...]'::vector
LIMIT 3;
-- Then query embeddings_full filtered by category
```

**Performance comparison (1536-dim, 1M rows):**

| Tier | Storage | Index size | p99 ANN latency |
|------|---------|------------|-----------------|
| `vector` (full) | ~6 GB | ~2.5 GB | ~8 ms |
| `halfvec` (half) | ~3 GB | ~1.3 GB | ~4 ms |
| Centroids only | < 1 MB | < 1 MB | < 1 ms |

---

## Performance Tuning Notes

| Tip | Recommendation |
|-----|---------------|
| HNSW `m` parameter | Default 16; increase to 32–64 for high-recall |
| HNSW `ef_construction` | Default 64; increase for better recall at index build cost |
| Index maintenance | Use `post_refresh_action = 'reindex_if_drift'` for automatic drift-based REINDEX |
| halfvec storage | ~50% storage savings vs `vector`; use for index columns when precision allows |
| Refresh interval | Match to your ingestion rate; 30s–5m is typical for RAG |

---

## Latency Assertions

Measure your actual p99 latencies before adding hard latency gates —
pg_trickle publishes measured baselines via `pgtrickle.vector_status()`,
not aspirational numbers.

```sql
-- Check embedding lag and drift
SELECT name, embedding_lag, drift_pct, last_reindex_at
FROM pgtrickle.vector_status();
```
