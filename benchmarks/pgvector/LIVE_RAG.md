# Live RAG comparison fixture

This developing fixture compares a batched application-maintained table,
periodic full recomputation and a differential stream against the same
revision-safe source. It uses the existing revision-safe example's job lease,
claim token, stale-completion checks and two model versions. The local fake
provider is deterministic and requires no credentials beyond PostgreSQL.
The setup and worker support `--dimensions` up to 1536; both must use the same
dimension. The example below uses three dimensions.

Use a disposable database with the packaged v0.109.0 candidate, PostgreSQL 18
and pgvector installed. Record the candidate commit, package digest, image,
PostgreSQL and pgvector versions before collecting evidence. Configure libpq
with a service or environment variables, then:

```sh
psql -X -v ON_ERROR_STOP=1 -c 'CREATE EXTENSION pg_trickle CASCADE'
python3 benchmarks/pgvector/setup_live_rag.py --dimensions 3
psql -X -v ON_ERROR_STOP=1 -c "INSERT INTO embedding_demo.documents(id, body) VALUES (1, 'alpha'), (2, 'beta'); INSERT INTO live_rag.access(document_id, tenant_id) VALUES (1, 10), (2, 20); SELECT embedding_demo.enqueue(1, 'model-v1'); SELECT embedding_demo.enqueue(2, 'model-v1')"
python3 benchmarks/pgvector/fake_embedding_worker.py --model model-v1 --duplicate
psql -X -v ON_ERROR_STOP=1 -c "SELECT live_rag.sync_application(ARRAY[1,2]::bigint[]); SELECT live_rag.recompute_full(); SELECT pgtrickle.refresh_stream_table('live_rag.differential')"
```

After source changes, enqueue generation in the same transaction as each
change, include every changed or deleted ID in the application batch, then
refresh each corpus. The worker claims before generating, and completion
rechecks the document's incarnation, revision, chunk version, publication and
lease. `--delay-seconds` can hold a claim while another session edits or
deletes a document; a late completion is rejected. `--duplicate` verifies that
a second completion cannot publish again. Use `--model model-v2` to drain the
existing example's second model queue; these comparison tables currently select
only model-v1 and do not implement a model cutover.

Compare complete corpus snapshots separately from retrieval quality:

```sql
SELECT NOT EXISTS ((TABLE live_rag.application EXCEPT ALL TABLE live_rag.truth)
                   UNION ALL (TABLE live_rag.truth EXCEPT ALL TABLE live_rag.application)) AS application_exact,
       NOT EXISTS ((TABLE live_rag.periodic_full EXCEPT ALL TABLE live_rag.truth)
                   UNION ALL (TABLE live_rag.truth EXCEPT ALL TABLE live_rag.periodic_full)) AS full_exact,
       NOT EXISTS ((TABLE live_rag.differential EXCEPT ALL TABLE live_rag.truth)
                   UNION ALL (TABLE live_rag.truth EXCEPT ALL TABLE live_rag.differential)) AS differential_exact;
```

The `*_current` views recheck source visibility and exact revision/embedding
identity at query time, so an outdated corpus entry is excluded before its
next refresh. They are example correctness guards, not a complete tenant
authorization system: apply the RLS and role guidance in the
[hybrid-search recipe](../../docs/tutorials/PGVECTOR_RAG_COOKBOOK.md) and
filter every query by its authorized tenant. The extra source join has a cost
that must be included in any comparison.

The same hybrid candidate-fusion query can target each guarded view for the
three-dimensional setup:

```sh
psql -X -v ON_ERROR_STOP=1 -v corpus=application_current -v tenant=10 \
  -v query_vector='[1,0,0]' -v terms=alpha \
  -f benchmarks/pgvector/live_rag_hybrid_query.sql
```

Repeat with `periodic_full_current` and `differential_current`. Indexes are
identical in kind, but the guard joins can affect planner choice. Inspect the
actual plan; the existence of an HNSW index does not prove it was used.

This is a functional fixture, not the finished #1396 benchmark. Wider setup
is supported, but the sample hybrid query is currently three-dimensional. It
does not yet provide the 384/768/1536-dimensional scale matrix,
hybrid recall and underfill analysis, concurrent workload, latency and resource
measurements, or a decision table. Do not infer a performance advantage from
this example.
