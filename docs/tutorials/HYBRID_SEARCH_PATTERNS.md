# Hybrid Search Patterns with pg_trickle

This example maintains a tenant-scoped corpus and combines independent vector
and lexical candidate lists using reciprocal-rank fusion. PostgreSQL's
`ts_rank` is native full-text ranking, not BM25. Applications generate the
embeddings; pg_trickle maintains the joined corpus and pgvector performs the
similarity search.

## Setup

Install pg_trickle and pgvector in a PostgreSQL instance configured for
pg_trickle. Run the [setup fixture](../../tests/fixtures/pgvector_hybrid_setup.sql)
in a fresh database. It creates source tables, a differential stream table,
a GIN index, a cosine HNSW expression index and a row-security policy. The
three-dimensional vectors keep the example small; use the actual model
dimension consistently in a real corpus.

From the repository root, for example, run these commands in `psql`:

```sql
CREATE EXTENSION pg_trickle;
\i tests/fixtures/pgvector_hybrid_setup.sql
```

```sql
INSERT INTO hybrid_documents VALUES
  ('00000000-0000-0000-0000-000000000001', 1, 'red apple', 'fruit', true),
  ('00000000-0000-0000-0000-000000000001', 2, 'blue berry', 'fruit', true),
  ('00000000-0000-0000-0000-000000000002', 1, 'secret apple', 'fruit', true);
INSERT INTO hybrid_embeddings VALUES
  ('00000000-0000-0000-0000-000000000001', 1, '[1,0,0]'),
  ('00000000-0000-0000-0000-000000000001', 2, '[0,1,0]'),
  ('00000000-0000-0000-0000-000000000002', 1, '[1,0,0]');
SELECT pgtrickle.refresh_stream_table('hybrid_search_corpus');
```

Create a restricted reader and grant it `SELECT` on the corpus. Do not grant
it source-table or stream-table write access. The corpus owner and application
reader should be different roles; the reader must not be a superuser or have
`BYPASSRLS`:

```sql
CREATE ROLE hybrid_reader NOLOGIN;
GRANT USAGE ON SCHEMA public TO hybrid_reader;
GRANT SELECT ON hybrid_search_corpus TO hybrid_reader;
```

A trusted application authenticates the tenant and sets the
context locally in the same transaction as the query:

```sql
BEGIN;
-- Bind $1 to the authenticated tenant UUID as text.
SELECT set_config('app.tenant_id', $1, true);
-- Execute pgvector_hybrid_query.sql with the parameters below.
COMMIT;
```

Use the [query fixture](../../tests/fixtures/pgvector_hybrid_query.sql) as a
prepared statement, binding `$1` to the authenticated tenant UUID as text,
`$2` to a vector literal such as `[1,0,0]`, and `$3` to search terms such as
`apple`. Both candidate branches filter by tenant before fusion, and the RLS
policy independently restricts corpus reads. A client allowed to issue
arbitrary SQL can set its own custom GUC; the GUC is not authentication.
See [tenant security](PER_TENANT_ANN_PATTERNS.md) for owner, connection-pool,
and partition boundaries.

The vector branch orders directly by the same cosine expression as the HNSW
index and applies its limit before fusion. The lexical branch uses GIN for
matching where the planner finds it useful, then ranks matches; GIN does not
provide rank order. A document found by both branches contributes both ranks
but appears once in the final result. Candidate budgets and the fusion
constant are examples to tune, not calibrated relevance guarantees.

## Half-precision candidates and full-precision reranking

The setup also builds `hybrid_search_half_cosine_idx` on
`embedding::halfvec(3)` with `halfvec_cosine_ops`. The separate
[rerank query](../../tests/fixtures/pgvector_halfvec_rerank_query.sql) binds the
same tenant and vector parameters as the hybrid query. It first retrieves a
bounded set ordered by that exact half-precision expression, then orders those
candidates by full-precision cosine distance. The full vector remains in the
corpus. Reranking cannot recover a neighbor omitted by approximate or
half-precision candidate selection; measure recall for the model and budget.

For higher dimensions, use the model's actual dimension in both the index and
candidate expression. The current pgvector HNSW limits are 2,000 dimensions
for `vector` indexes and 4,000 for `halfvec` indexes, distinct from storage
limits. Verify the [limits for your pgvector version](https://github.com/pgvector/pgvector#hnsw).

For filtered ANN, `ef_search` can be tuned transaction-locally. On pgvector
0.8.0 and later, iterative scans can examine additional candidates up to their
configured scan limits. Check the installed `vector` extension version before
using the second setting:

```sql
SELECT extversion FROM pg_extension WHERE extname = 'vector';
BEGIN;
SET LOCAL hnsw.ef_search = 100;
SET LOCAL hnsw.iterative_scan = strict_order; -- pgvector 0.8.0+
-- Bind tenant context and execute a candidate query in this transaction.
COMMIT;
```

These settings are workload choices, not guarantees of a full result set or
perfect recall. For a selective tenant filter, an exact search may be preferable.

## Verify the plan and results

The [E2E test](../../tests/e2e_pgvector_hybrid_tests.rs) executes both fixtures,
checks differential refresh after changes, and tests two tenants with shared
document IDs. It also inspects the cosine index's validity and an `EXPLAIN`
plan as a restricted reader. Its transaction-local planner settings establish
index eligibility on a tiny fixture; they are not production tuning advice.
Use `EXPLAIN (ANALYZE, BUFFERS)` on representative data to see the normal
planner choice.

ANN candidates are approximate. Tenant filtering can underfill a candidate
set; pgvector 0.8+ iterative scans and their limits can be tuned when useful.
Measure recall against an exact query with the same metric and tenant filter,
and consider exact filtered search when the tenant subset is small. A stream
refresh timestamp does not prove that an application's embeddings match its
latest source revisions or model version.
