# pg_trickle Starter: Vector RAG Corpus

This example uses a three-dimensional vector so its SQL can be run without an
embedding provider. Applications generate embeddings; pg_trickle maintains
the corpus, and pgvector performs search. Scheduled refresh adds propagation
lag and does not establish that an embedding matches the latest document text.

Run the [tested SQL fixture](../../tests/fixtures/pgvector_l2_starter.sql) in
a fresh, configured database. For example, from the repository root in `psql`:

```sql
\i tests/fixtures/pgvector_l2_starter.sql
```

The legacy helper builds an L2 HNSW index. The fixture's query repeats the
dimensional cast and uses the matching `<->` operator.

## Helper contract

The installed `pgtrickle.embedding_stream_table` signature is:

```text
embedding_stream_table(name text, source_table text, vector_column text,
    extra_columns text DEFAULT NULL, refresh_interval text DEFAULT '1m',
    index_type text DEFAULT 'hnsw', dry_run boolean DEFAULT false)
RETURNS TABLE(action text)
```

`index_type` accepts `hnsw` or `ivfflat`; the helper uses the L2 operator
class for the source vector type. `extra_columns` is trusted SQL text, not a
parameterized expression: do not pass untrusted input. Source relations are
resolved through PostgreSQL's search path; quoted schema, relation, and column
names are supported. A pgvector extension type with known dimensions is
required. An expression index can require the query to repeat the dimensional
cast.

Set `dry_run => true` to inspect the generated SQL without creating objects.
The call succeeds only if the requested index is created; a conflicting index
name or a failed build aborts the statement and stream-table creation. New
index names are deterministic hashes; existing indexes are not renamed on
upgrade. The helper does not treat an existing same-name index as proof that it
matches the requested table and operator class. IVFFlat
indexes need representative training data for useful recall; creating one on
an empty corpus is generally unsuitable.

New helper-created tables now keep the default `post_refresh_action = 'none'`.
Older versions silently selected `reindex_if_drift`, which can rebuild indexes
with blocking locks. Existing tables retain their stored policy on upgrade;
inspect and change it explicitly if needed. The helper does not attach an
outbox or generate embeddings.

The metric-aware helper takes an explicit metric and structured options:

```sql
SELECT * FROM pgtrickle.embedding_stream_table_v2(
    'corpus_cosine', 'documents', 'embedding', 'cosine',
    '{"columns": ["id"], "hnsw_m": 16, "hnsw_ef_construction": 64}'::jsonb
);
-- Match the index expression and cosine operator in the search query.
SELECT id FROM corpus_cosine
ORDER BY embedding::vector(3) <=> '[1,0,0]'::vector LIMIT 5;
```

Use `inner_product` with `<#>` or `l2` with `<->`. The new helper does not
alter existing indexes: migrate by creating a new stream table and switching
queries to it. See the [SQL reference](../SQL_REFERENCE.md#pgtrickleembedding_stream_table_v2)
for options and [hybrid search](HYBRID_SEARCH_PATTERNS.md) and
[tenant isolation](PER_TENANT_ANN_PATTERNS.md) for additional patterns.
