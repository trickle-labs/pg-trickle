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
The periodic-full baseline bulk-loads the rebuilt corpus, then rebuilds its
HNSW and text indexes; index creation is included in maintenance latency and
can block readers during the full refresh.

Use a disposable database with the packaged v0.109.0 candidate, PostgreSQL 18
and pgvector installed. Record the candidate commit, package digest, image,
PostgreSQL and pgvector versions before collecting evidence. Configure libpq
with a service or environment variables, then:

```sh
psql -X -v ON_ERROR_STOP=1 -c 'CREATE EXTENSION vector; CREATE EXTENSION pg_trickle'
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
deletes a document; a late completion is rejected. The batch runner also
supports deterministic per-job delay samples with
`--delay-profile uniform --delay-seed <seed>` and reports their p50/p95/p99
and maximum. It records persisted stale and absent rows after source commit,
generation and maintenance, plus p50/p95/p99 for retrieval query plans.
`--duplicate` verifies that
a second completion cannot publish again. Use `--model model-v2` to drain the
existing example's second model queue. Each document's selected model is in
`live_rag.access.model_version` (default `model-v1`). To roll a document over,
enqueue and complete its new-model embedding, update that column, then include
the document ID in the application batch and refresh both other corpora. The
guarded views immediately exclude the old model after the update, including
when the new embedding or a corpus refresh is still pending. For an atomic
group cutover, update the selected model for that group in one transaction;
the guarded views may temporarily omit rows until maintenance catches up.

Compare complete corpus snapshots separately from retrieval quality:

```sql
SELECT NOT EXISTS ((TABLE live_rag.application EXCEPT ALL TABLE live_rag.truth)
                   UNION ALL (TABLE live_rag.truth EXCEPT ALL TABLE live_rag.application)) AS application_exact,
       NOT EXISTS ((TABLE live_rag.periodic_full EXCEPT ALL TABLE live_rag.truth)
                   UNION ALL (TABLE live_rag.truth EXCEPT ALL TABLE live_rag.periodic_full)) AS full_exact,
       NOT EXISTS ((SELECT id, tenant_id, body, incarnation, revision, chunk_version, model_version, embedding
                    FROM live_rag.differential EXCEPT ALL TABLE live_rag.truth)
                   UNION ALL (TABLE live_rag.truth EXCEPT ALL
                    SELECT id, tenant_id, body, incarnation, revision, chunk_version, model_version, embedding
                    FROM live_rag.differential)) AS differential_exact;
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

The report records vector recall@K against a scan with index paths disabled,
result underfill, lexical candidate count, boundary ties, installed versions,
and EXPLAIN ANALYZE/BUFFERS plans for the exact query, normal planner choice
and forced index eligibility. It also reports hybrid recall@10 and underfill
for the shared fusion query against the same query with exact vector candidates
and index paths disabled. Both hybrid queries use up to 20 vector and 20 lexical
candidates; `--k` changes only the vector-only measurement. Their exact and
normal-planner plans are included, with ANN index use reported separately.
The `forced_*` metrics separately report recall and underfill with sequential
scans discouraged. Treat these as ANN measurements only when
`forced_plan_used_ann_index` is true; another index or a sequential scan can
still be selected. The candidate and plan queries execute separately in the
same repeatable-read snapshot. The forced path is diagnostic and does not
represent the normal planner's choice. The overall measurement-command duration
includes this extra diagnostic query and is not an individual ANN latency.
Exact-baseline queries disable index scans and discourage nested loops to avoid
repeatedly scanning the corpus; the original nested-loop setting is restored before the
normal-planner and forced-index queries.
Boundary ties can make ID-based recall ambiguous.
The supplied commit and digest are declarations: this script does not attest
the installed package. Retain separate installation and environment evidence.

For a batched-update comparison, start with another empty disposable database
with both `pg_trickle` and `vector` installed. The benchmark creates the two
fixture schemas and refuses to run if either is already present:

For larger Docker runs, allocate sufficient container shared memory (for
example, `--shm-size=1g`); the default 64 MiB failed the 100,000-row exact
corpus comparison in one local run.

```sh
python3 benchmarks/pgvector/run_live_rag_batches.py \
  --rows 10000 --dimensions 384 --batch 10 --cycles 3 --tenant 1 \
  --candidate-commit "$CANDIDATE_COMMIT" --package-sha256 "$PACKAGE_SHA256" \
  --candidate-archive "$CANDIDATE_ARCHIVE" --attest-container "$CONTAINER" \
  --checkpoint batch-progress.json \
  > batch-results.json
```

For long runs, `--checkpoint` atomically records setup progress and each
completed maintenance measurement. A completed run has `status: "complete"`;
an error records `status: "failed"`, and an externally interrupted process
leaves the last `status: "running"` checkpoint with its current phase and any
completed samples. The checkpoint preserves evidence; it does not resume the
database workload. Keep stdout redirected separately for the complete report.

For Docker runs, `--candidate-archive` and `--attest-container` together check
the archive digest and candidate manifest, the installed extension files, and
that the benchmark connection reaches the same PostgreSQL instance as the
container. The checks run before and after the benchmark; a mismatch or restart
fails the run. This requires Docker access and permission to read PostgreSQL's
control-system identity. It verifies files on disk, not which library bytes a
previously started backend mapped. Omit both options for other environments;
the report then explicitly labels the package identity as operator-declared.
Use a direct connection: the endpoint checks do not prove that a load balancer
routed every intermediate query to the same instance. The manifest is not a
signature; verify the archive digest and provenance independently.

`--rows` supports up to one million, and `--dimensions` accepts 384, 768 or
1536. The deterministic initial backfill bypasses the job worker; subsequent
updates enqueue generation transactionally and use the fake worker. Add
`--workload mixed` to apply an update, a visibility toggle, a deletion and an
insertion in each group of four batch slots. Mixed batches must contain at
least four slots and `rows >= batch * cycles`; each cycle targets fresh IDs so
the runner does not silently reuse deleted rows. Every cycle gives all three
approaches the same changed IDs, rotates their serial
execution order, checks each entire corpus against the defining SQL, records
the effective differential refresh state, and runs vector and hybrid recall
measurements. The report includes individual batch and maintenance times,
nearest-rank p50/p95/p99 across batches, index-inclusive relation sizes and
cluster-wide WAL insert-LSN movement. Startup of each `psql` process is
included. Changed-row eligible and searchable counts are sampled immediately
after commit, after generation and after each maintenance step. Generation
duration is recorded separately; commit-to-eligible also includes the
post-commit availability query. Eligible-to-searchable time includes earlier
approaches and availability queries in that cycle; use the individual
maintenance duration for a less order-dependent comparison. Other cluster
activity contaminates WAL measurements. Retrieval
uses administrative current views after all approaches converge, and the
EXPLAIN plans contain per-query timing and buffer information.

The batch runner populates all three corpora before creating their ANN and
lexical indexes, so the large initial copy does not pay per-row index updates.
The reported initial setup time includes those index builds. The standalone
`setup_live_rag.py` command still creates indexes immediately for interactive
and E2E fixture setup.

Add `--concurrent-readers 1` (up to four) to issue repeated tenant-filtered
hybrid reads against each approach while its maintenance runs. Each reader
starts a new `psql` process per query and pauses 50 ms between queries. The
report includes client-side read latency, query counts, and the count whose
database statement intervals overlapped the maintenance interval. A zero
overlap count is not evidence of concurrent query performance. These readers
use administrative current views and do not generate concurrent writes. A read
is cancelled by PostgreSQL after 29 seconds and counted separately; its
client-window overlap does not prove that the database executed it throughout
that interval. A 35-second client timeout fails the run because server cleanup
cannot be confirmed.

This runner uses a fixed delay or deterministic uniform per-job delays
(`--delay-profile uniform --delay-seed <seed>`); it does not run concurrent
writers or report per-document freshness percentiles. The run itself does not
collect host CPU or lock metrics; a separate Docker stats sample records
database-container CPU and memory for the 100k case. See the
[candidate-bounded scale report](results/2026-11-15-rag-scale-bd0813-arm64/README.md)
for the 10k dimension matrix and 100k/384 sample, including its unfavorable
filtered ANN result. These observations use candidate `bd0813e8`, before the
current package fix, and are not final v0.109.0 qualification or a general
decision model.

For an opt-in concurrent three-approach exercise, run the ignored
`test_live_rag_unblocked_writes_and_reads_during_refresh` E2E test. It uses
fresh databases, one paced visibility writer, a restricted tenant reader and
a 200 ms target maintenance cadence. `PGT_RAG_LOAD_ROWS` selects 1,000 to
100,000 documents (default 1,000), `PGT_RAG_LOAD_DIMENSIONS` selects 384,
768 or 1536 (default 384), and `PGT_RAG_LOAD_CHANGES` selects 1 to 1,000
ordered changes (default 100), subject to the test's tenant-size bound.
For example, after preparing the E2E image and pinned pgvector image:

```sh
just build-e2e-image
./tests/build_e2e_pgvector_image.sh 0.8.7
PGS_E2E_IMAGE=pg_trickle_e2e_pgvector:0.8.7 PGVECTOR_EXPECTED_VERSION=0.8.7 \
PGT_RAG_LOAD_ROWS=10000 PGT_RAG_LOAD_DIMENSIONS=384 PGT_RAG_LOAD_CHANGES=100 \
  ./scripts/run_e2e_tests.sh --retries 0 --test e2e_pgvector_revision_tests \
    --run-ignored ignored-only --no-capture \
    -E 'test(test_live_rag_unblocked_writes_and_reads_during_refresh)'
```

The input uses deterministic varied vectors and ordered visibility changes.
Application batching uses a watermark valid for that ordered trace only.
The test records client and server intervals, not ANN recall or full RAG
freshness, and runs the three databases serially. Larger sizes require suitable
memory, disk and execution time; the accepted limit is not a measured capacity.
One 10k/384 local-source run passed on PostgreSQL 18.6, pgvector 0.8.7 and
pg_trickle 0.109.0; its server/client overlap counts and local-build provenance
are recorded in the [concurrency report](results/2026-11-16-rag-concurrency-local-arm64/README.md).
Set `PGS_E2E_PLATFORM` to the built image's platform when Docker's default
differs. This run is not an attestation of the release archive.

This ignored E2E case is a functional concurrency fixture, not the scale
benchmark: it does not measure ANN recall or per-document freshness and runs
the three databases serially. The batch runner substitutes the configured
vector dimensions into the hybrid query; the standalone SQL example above
remains three-dimensional. The retained candidate-bounded results cover
10k rows at 384/768/1536 dimensions and 100k rows at 384 dimensions. They are
not final-package qualification. The release gate reruns both 10k and 100k at
384/768/1536 against the exact package; corpora above 100k are out of scope and
unvalidated.
Larger-load recovery-to-searchability and final integrated-package reruns remain
outstanding. The report records a scoped comparison and limitations; do not
infer a universal performance advantage from these synthetic cases.

The exact-package runner retains a machine-readable JSON report, a Markdown
decision table and two SVG plots with the qualification artifact. The table
compares p50 maintenance time against the batched application and periodic-full
baselines, and separately reports exact corpus checks, planner HNSW use,
forced-plan ANN recall and hybrid recall. Forced recall is shown only when the
forced plan actually uses HNSW. The table and plots are partial if the six-case
matrix fails or is interrupted; use the JSON and raw checkpoints for full
per-cycle results. The maintenance comparison does not measure concurrent
writer throughput or host lock behavior.
