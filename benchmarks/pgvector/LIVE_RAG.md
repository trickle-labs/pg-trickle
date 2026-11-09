# Live RAG comparison fixture

This developing fixture compares a batched application-maintained table,
periodic full recomputation and a differential stream against the same
revision-safe source. It uses the existing revision-safe example's job lease,
claim token, stale-completion checks and two model versions. The local fake
provider is deterministic and requires no credentials beyond PostgreSQL.
The setup and worker support `--dimensions` up to 1536; both must use the same
dimension. The example below uses three dimensions.
The fixture CI exercises setup, generation and corpus convergence at 384 and
1536 dimensions; those small tests do not establish scale or retrieval quality.

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
next refresh. They are administrative views and do not authorize tenants.
For the fixture's restricted role-per-tenant example, an administrator can
grant only the security-barrier `*_authorized` views:

```sql
CREATE ROLE rag_tenant_10 NOLOGIN;
INSERT INTO live_rag.tenant_readers(role_oid, tenant_id)
  SELECT oid, 10 FROM pg_roles WHERE rolname = 'rag_tenant_10';
GRANT USAGE ON SCHEMA live_rag TO rag_tenant_10;
GRANT SELECT ON live_rag.application_authorized,
  live_rag.periodic_full_authorized, live_rag.differential_authorized TO rag_tenant_10;
```

Connect as that role or grant it only to trusted login roles. Do not give
readers direct access to the corpus, current views, mapping table, or source
tables. The mapping is keyed by the active database role OID; remove its row
when dropping a role, and update it transactionally when access changes. A
client-supplied tenant parameter alone is not authorization. Owners and
superusers are trusted administrators. This fixture is not a general identity
system; see the [hybrid-search recipe](../../docs/tutorials/PGVECTOR_RAG_COOKBOOK.md)
for production role and RLS guidance. The source and authorization joins have
a cost that must be included in any comparison.

The same hybrid candidate-fusion query can target each guarded view for the
three-dimensional setup:

```sh
PGOPTIONS='-c role=rag_tenant_10' psql -X -v ON_ERROR_STOP=1 -v corpus=application_authorized -v tenant=10 \
  -v query_vector='[1,0,0]' -v terms=alpha \
  -f benchmarks/pgvector/live_rag_hybrid_query.sql
```

Repeat with `periodic_full_authorized` and `differential_authorized`. Indexes are
identical in kind, but the guard joins can affect planner choice. Inspect the
actual plan; the existence of an HNSW index does not prove it was used.

To compare filtered vector results with an exact scan on the same tenant and
snapshot, supply the candidate's recorded commit and package SHA-256:

```sh
PGOPTIONS='-c role=rag_tenant_10' python3 benchmarks/pgvector/measure_live_rag_recall.py \
  --corpus differential_authorized --tenant 10 --dimensions 3 \
  --query-vector '[1,0,0]' --terms alpha --k 10 --expected-pgvector 0.8.7 \
  --candidate-commit "$CANDIDATE_COMMIT" --package-sha256 "$PACKAGE_SHA256" \
  > recall.json
```

The report records recall@K against a scan with index paths disabled, result
underfill, lexical candidate count, boundary ties, installed versions, and
EXPLAIN ANALYZE/BUFFERS plans for the exact query, normal planner choice and
forced index eligibility. The forced plan is diagnostic and is not used to
compute recall. The candidate and plan queries execute separately in the same
repeatable-read snapshot. Boundary ties can make ID-based recall ambiguous.
The supplied commit and digest are declarations: this script does not attest
the installed package. Retain separate installation and environment evidence.

This is a functional fixture, not the finished #1396 benchmark. Wider setup
is supported, but the sample hybrid query is currently three-dimensional. It
does not yet provide the 384/768/1536-dimensional scale matrix,
hybrid-fusion recall analysis, concurrent workload, end-to-end latency and resource
measurements, or a decision table. Do not infer a performance advantage from
this example.
