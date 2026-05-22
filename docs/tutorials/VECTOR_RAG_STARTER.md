# pg_trickle Starter: Vector RAG Corpus

> VA-5 — Quick-start for building a production RAG corpus with
> pg_trickle and pgvector.

## Prerequisites

- PostgreSQL 18+ with pg_trickle installed
- pgvector extension

## 5-Minute Quick Start

```sql
-- 1. Enable required extensions
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trickle;

-- 2. Create source table
CREATE TABLE documents (
    id         BIGSERIAL PRIMARY KEY,
    content    TEXT NOT NULL,
    metadata   JSONB,
    embedding  vector(1536),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- 3. Create embedding stream table (one call does everything)
SELECT pgtrickle.embedding_stream_table(
    'doc_corpus',           -- stream table name
    'documents',            -- source table
    'embedding',            -- vector column
    refresh_interval => '1m'
);

-- 4. Insert documents (populate embeddings from your model)
INSERT INTO documents (content, embedding) VALUES
    ('Hello world', '[0.1,0.2,...]'),
    ('Another doc', '[0.3,0.4,...]');

-- 5. Query with vector similarity
SELECT id, content, embedding <=> '[0.1,0.2,...]'::vector AS distance
FROM doc_corpus
ORDER BY embedding <=> '[0.1,0.2,...]'::vector
LIMIT 5;
```

## Next Steps

- **Hybrid search**: Add a `tsvector` column — see [HYBRID_SEARCH_PATTERNS.md](../tutorials/HYBRID_SEARCH_PATTERNS.md)
- **Multi-tenant isolation**: See [PER_TENANT_ANN_PATTERNS.md](../tutorials/PER_TENANT_ANN_PATTERNS.md)
- **Distance alerts**: Use `subscribe_distance()` for real-time anomaly detection
- **Embedding outbox**: Use `attach_embedding_outbox()` to publish embedding changes downstream

## `pgtrickle.embedding_stream_table()` Parameter Reference

`pgtrickle.embedding_stream_table()` is a convenience wrapper around
`pgtrickle.create_stream_table()` that automatically generates the stream table
query, creates the HNSW index, and wires optional outbox integration.

```
pgtrickle.embedding_stream_table(
    name             TEXT,          -- (1) stream table name
    source_table     TEXT,          -- (2) source table (schema-qualified or current schema)
    vector_column    TEXT,          -- (3) vector column name in source table
    extra_columns    TEXT DEFAULT NULL,  -- (4) comma-separated extra columns to include
    refresh_interval TEXT DEFAULT '1m',  -- (5) refresh schedule (duration or cron)
    refresh_mode     TEXT DEFAULT 'DIFFERENTIAL',  -- (6) DIFFERENTIAL | FULL | AUTO | IMMEDIATE
    hnsw_m           INT  DEFAULT 16,   -- (7) HNSW index m parameter
    hnsw_ef          INT  DEFAULT 64,   -- (8) HNSW ef_construction parameter
    distance_ops     TEXT DEFAULT 'vector_cosine_ops',  -- (9) pgvector distance operator class
    with_outbox      BOOL DEFAULT FALSE  -- (10) attach an embedding outbox via pg_tide
) RETURNS TEXT
```

| # | Parameter | Default | Description |
|---|-----------|---------|-------------|
| 1 | `name` | required | Name of the stream table to create (optionally schema-qualified). |
| 2 | `source_table` | required | Source table containing the vector column. Must be accessible to the calling role. |
| 3 | `vector_column` | required | Name of the `vector` / `halfvec` / `sparsevec` column in the source table. |
| 4 | `extra_columns` | `NULL` | Comma-separated list of additional columns to include in the stream table, e.g. `'id, content, metadata'`. If `NULL`, all columns are included. |
| 5 | `refresh_interval` | `'1m'` | Refresh schedule: a duration string (`'30s'`, `'5m'`) or a cron expression. |
| 6 | `refresh_mode` | `'DIFFERENTIAL'` | Refresh strategy. `DIFFERENTIAL` is correct for most embedding workflows. |
| 7 | `hnsw_m` | `16` | HNSW graph degree. Higher values improve recall at the cost of index build time and memory. |
| 8 | `hnsw_ef` | `64` | HNSW `ef_construction`. Higher values improve index quality. |
| 9 | `distance_ops` | `'vector_cosine_ops'` | pgvector index operator class. Use `'vector_l2_ops'` for Euclidean distance or `'vector_ip_ops'` for inner product. |
| 10 | `with_outbox` | `FALSE` | If `TRUE`, attaches a pg_tide transactional outbox to publish embedding changes downstream. Requires pg_tide installed. |

**Example with explicit parameters:**

```sql
SELECT pgtrickle.embedding_stream_table(
    'doc_corpus',
    'documents',
    'embedding',
    extra_columns    => 'id, content, metadata',
    refresh_interval => '30s',
    refresh_mode     => 'DIFFERENTIAL',
    hnsw_m           => 32,
    hnsw_ef          => 128,
    distance_ops     => 'vector_cosine_ops',
    with_outbox      => FALSE
);
```

For a full SQL_REFERENCE entry see
[SQL_REFERENCE.md](../SQL_REFERENCE.md#embedding_stream_table).

---

## Architecture Diagram

```
┌─────────────┐   INSERT/UPDATE   ┌──────────────────┐
│  documents  │ ──────────────── ▶│  doc_corpus (ST) │
│  (source)   │                   │  HNSW index      │
└─────────────┘                   └────────┬─────────┘
                                            │
                                  ┌─────────▼──────────┐
                                  │  Your application  │
                                  │  (vector search)   │
                                  └────────────────────┘
```

## Ecosystem

pg_trickle works alongside:

| Tool | Role |
|------|------|
| pgvector | Vector storage and ANN search |
| pg_tide | Transactional outbox for embedding events |
| LangChain / LlamaIndex | RAG framework integration |
| pgai | Automated embedding generation from model APIs |

See the [pg_trickle blog](https://pg-trickle.dev/blog) for in-depth integration guides.
