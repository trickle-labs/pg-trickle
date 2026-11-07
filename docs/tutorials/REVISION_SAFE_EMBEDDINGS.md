# Revision-safe asynchronous embeddings

Applications generate embeddings; pg_trickle maintains a corpus from committed
database rows. A recent stream refresh does not establish that a provider has
processed the current document revision. The
[executable SQL fixture](../../tests/fixtures/pgvector_revision_pipeline.sql)
shows a provider-neutral protocol with a deterministic fake provider in its
[E2E test](../../tests/e2e_pgvector_revision_tests.rs). It uses three-dimensional
vectors to keep the example small. Install both extensions, then run the fixture
in a fresh database; it creates the `embedding_demo` schema.

Each document has a random incarnation token and a revision token. The trigger
assigns fresh tokens on insertion, preserves the incarnation on update, and
changes the revision when `body` or `chunk_version` changes. Metadata-only
updates leave it unchanged. A real application must include **every** input
to the model and chunker in its revision rule, including any title, prompt,
normalization, and chunk identity. Assign a distinct incarnation to a
delete/recreate even if the external document ID is reused. The example's
single chunk is represented by one `chunk_version`; a multi-chunk system must
also key and reconcile individual chunks.

The job and result keys include incarnation, revision, chunk version and model
version. The corpus joins on all of them and filters by model version. After a
refresh, a document whose current result is missing is omitted until generation
finishes. Until the next scheduled refresh a previous, internally consistent
text/vector pair can remain visible; it is not evidence of current source data.
If an application serves stale results intentionally, it must keep the old
text together with its old vector rather than pairing a new body with it.

## Application and worker transactions

Only trusted application and worker roles should write these tables or execute
the example functions. Restrict their privileges and validate model identifiers,
vectors, retry settings and sanitized error messages at the application
boundary. This fixture is a protocol example, not a multi-tenant authorization
scheme.

1. In the **same transaction** as an insert or model-input update, call
   `embedding_demo.enqueue(document_id, model_version)` for each model that
   should be generated. The unique key makes duplicate enqueue calls harmless.
   A metadata-only update does not need a new request. On deletion the foreign
   keys remove jobs and results; document and chunk replacement follow the same
   rule with new keys.
2. A worker calls `embedding_demo.claim(model_version, positive_lease)` and
   commits the transaction immediately. It records the returned job ID, claim
   token and body, and generates the embedding **outside** the database
   transaction. The fixture uses `FOR UPDATE SKIP LOCKED` and issues a fresh
   token for every lease, including a reclaimed expired lease.
3. On success, call `embedding_demo.complete(job_id, claim_token, embedding)`
   in a short transaction. It locks the job and current document and writes a
   result only if the lease, incarnation, revision, chunk version and published
   state still match. A false return is a stale, expired, deleted or already
   completed claim; discard that output. A duplicate completion does not replace
   another result.
4. On failure, call `embedding_demo.fail(job_id, claim_token, sanitized_error,
   retry_delay)` in a short transaction. Use bounded exponential backoff and
   operational retry limits in the worker. A crash leaves the lease to expire;
   a new token prevents the old worker from completing the reclaimed job.

Do not call a network provider from a refresh query. The optional outbox can
publish changes downstream; it does not generate embeddings, schedule provider
retries, or coordinate model versions.

This query reports states only for jobs for the current published revision;
an absent job is not a successful generation:

```sql
SELECT j.model_version, j.state, count(*)
FROM embedding_demo.jobs j
JOIN embedding_demo.documents d ON d.id = j.document_id
 AND (j.incarnation, j.revision, j.chunk_version) =
     (d.incarnation, d.revision, d.chunk_version)
WHERE d.published
GROUP BY j.model_version, j.state;
```

Track published documents without a matching result separately, and alert on
failed or expired jobs. A refresh timestamp is not an embedding-generation
freshness measurement. Retain completed jobs for current revisions as the
deduplication record. A cleanup job can remove results and non-running jobs for
superseded keys after a retention period; account for leases, in-flight workers,
backup and audit requirements before deleting anything. Never remove the only
current result as routine cleanup.

## Model rollover

The fixture maintains separate `corpus_v1` and `corpus_v2` streams and indexes,
even though both fake models use three dimensions. Enqueue and backfill every
published current revision for the new model; a global model change is a full
backfill, not a small delta. Use separate result types and indexes when models
have different dimensions. Do not mix embedding spaces just because their
dimensions match.

Before switching, require no missing current results for the new model, a
successful corpus refresh, a valid and ready index, and a result comparison or
retrieval check for that model. For example, the missing-result count is:

```sql
SELECT count(*) AS missing_v2
FROM embedding_demo.documents d
WHERE d.published AND NOT EXISTS (
    SELECT 1 FROM embedding_demo.results e
    WHERE e.document_id = d.id AND e.incarnation = d.incarnation
      AND e.revision = d.revision AND e.chunk_version = d.chunk_version
      AND e.model_version = 'model-v2'
);
```

Inspect `pg_index.indisvalid` and `indisready` for `corpus_v2_ann` and compare
the refreshed corpus with the current source/result join before switching the
application's view or configuration. The test switches the stable view from
v1 to v2, verifies the distinct model result, then switches it back. Keep the
old corpus and model available through the rollback window; changing the view
does not itself make an incomplete backfill ready.
