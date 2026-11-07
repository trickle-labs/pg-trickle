# pgvector RAG Cookbook: Always-Fresh Embedding Pipelines with pg_trickle

> **Status:** Vector aggregates are experimental and disabled by default. See
> the [support summary](../SUPPORT_SUMMARY.md) for capability-specific maturity.

**Prerequisites:** PostgreSQL 18, `pg_trickle`, `pgvector` extension.

---

## Overview

This cookbook describes Retrieval-Augmented Generation (RAG) patterns using
`pg_trickle` and `pgvector`. Applications must recompute embeddings when model
inputs change. pg_trickle maintains the stored query result from committed
inputs; scheduled refresh adds propagation lag and does not generate vectors.

Embeddings are **generated at the application layer** (via OpenAI, Cohere,
Ollama, or any embedding API) and stored in a source table. `pg_trickle` then
incrementally maintains the denormalised corpus, the vector index, and any
aggregate centroids.

The first pattern below does not track whether an embedding matches the latest
document content. For asynchronous generation, retries and model changes, use
the [revision-safe embedding workflow](REVISION_SAFE_EMBEDDINGS.md): a stream
refresh alone cannot make a stale provider result current.

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

`pg_trickle` supports pgvector's `avg(vector)` for per-cluster centroid maintenance.
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
            avg(embedding) AS centroid,
            count(*) AS member_count
        FROM cluster_assignments
        GROUP BY cluster_id
    $$,
    schedule => '1m',
    refresh_mode => 'DIFFERENTIAL'   -- incremental AVG maintenance
);
```

> **Note:** Vector aggregates are experimental and disabled by default. Install
> pgvector and enable `pg_trickle.enable_vector_agg` in the sessions that create
> and refresh the stream table, including scheduler workers.

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
The legacy `reindex` and `reindex_if_drift` actions run a non-concurrent,
table-wide `REINDEX` inside the refresh transaction. It blocks writes and can
also block reads while the indexes are locked. A failure is isolated, but the
rebuild still extends the refresh transaction. The threshold is not evidence
of index quality. Leave `post_refresh_action = 'none'` to manage maintenance
independently.

For manual concurrent maintenance, the tested
[candidate query](../../tests/fixtures/pgvector_ann_maintenance_candidates.sql)
accepts a stream-table name as its `$1` parameter and returns valid HNSW and
IVFFlat leaf indexes for which the caller has `MAINTAIN` privilege. Run it as a
trusted maintenance role with access to the pg_trickle stream-table catalog and
the stream table. The role must also have schema usage and `MAINTAIN` on each
leaf table; PostgreSQL checks privileges again at execution. The returned index
names are quoted identifiers, not SQL supplied by another user. For each chosen
index, recheck its identity and access method immediately before executing
`REINDEX INDEX CONCURRENTLY` on an ordinary connection in autocommit mode, with
appropriate lock and statement timeouts. Do not run concurrent DDL inside a
function or transaction block, or assume the original index OID survives a
successful concurrent rebuild. Coordinate operators to prevent duplicate work,
and inspect invalid `_ccnew`/`_ccold` indexes after a failed operation as
described in the [PostgreSQL REINDEX documentation](https://www.postgresql.org/docs/18/sql-reindex.html).
To detect refreshes that committed during an operation, an operator can record
the stream identity and epoch immediately before and after it:

```sql
SELECT pgt_id, pgt_relid, ann_refresh_epoch
FROM pgtrickle.pgt_stream_tables
WHERE pgt_relid = 'search_corpus'::regclass;
```

If the identity or epoch changes, or the epoch is `NULL`, the observation is
not evidence that the rebuild covered the latest refresh. An unchanged epoch
also does not rule out a refresh still in flight.

An authorized operator can persist an explicit intent for a selected leaf:

```sql
SELECT pgtrickle.request_ann_maintenance(
    'search_corpus'::regclass::oid, 'search_corpus_embedding_idx'::regclass::oid
);
```

The function records intent only. It does not start a worker, perform the
rebuild, or mark it complete; an administrator must grant `EXECUTE` explicitly.
The returned ID can be inspected with `pgtrickle.ann_maintenance_request(id)`
after an administrator grants `EXECUTE` on that function too. The inspection
rechecks privileges and reports whether the captured identity still matches;
it is not a lock and cannot prevent later DDL. Requests are not restored
across a logical dump/restore because their OIDs are database-local. The
session-bound claim and completion APIs can record a manually executed rebuild
and its failure. The repository's one-request psql worker is documented in
[the SQL reference](../SQL_REFERENCE.md#pgtrickleclaim_ann_maintenance-and-pgtricklefinish_ann_maintenance),
including its connection, identity and timeout requirements. A supervisor is
responsible for selecting requests and retries. Continue normal VACUUM and
autovacuum care.
