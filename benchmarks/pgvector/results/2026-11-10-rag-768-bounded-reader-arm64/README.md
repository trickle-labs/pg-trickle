# Live RAG at 768 dimensions with bounded readers

On 2026-11-10, runner commit `f4a29ce32b85a3b4ff6f53e7e67e3d409532630a`
repeated the 10,000-document, 768-dimensional mixed workload with one hybrid
reader and three 12-slot batches. PostgreSQL 18.6 and pgvector 0.8.7 ran in
Linux ARM64 Docker with 1 GiB shared memory on an Apple M1 Pro. The image was
`sha256:f1a28666a96308630d613d012b3889628ab8f295d7e2f0a436c000011ba31332`.
The earlier v0.109.0 package was candidate
`1e291536bf0380b37a509b730155ca3d3a0ccea4`, archive SHA-256
`2cd9d52c1d9d744557e2f16a7faf1cb5c44213fdc72c72beebed37122b34f73b`.
The [report](results.json.gz) records matching archive, 157 installed files
and database identity before and after the run. The [log](run.log.gz) includes
the initialization fallback and differential refresh details.

The reader helper set a 29-second PostgreSQL statement timeout before its
35-second client timeout, correcting the abandoned-reader problem in the
[earlier run](../2026-11-10-rag-768-concurrent-read-arm64/README.md).
During one full-maintenance step, an activity sample showed a single waiting
reader; this was not continuous observation. Each full step recorded two
reader statement timeouts overlapping the client maintenance window.
Application and differential steps had completed database-time overlaps and
no reader timeouts. Timed-out queries have no completed database interval.

All three corpora matched the defining SQL after initialization and every
batch. Initial differential population fell back to FULL; all three measured
refreshes were differential without fallback. Application maintenance took
0.109–0.118 seconds, differential 1.125–1.216 seconds and periodic full
67.579–81.981 seconds. Initial setup took 187.1 seconds. Median generation
time was 0.425 seconds and median commit-to-eligible time was 0.464 seconds,
including the post-commit availability check. Times include `psql` startup.

No normal retrieval plan selected an ANN index. Forced differential HNSW
recall@10 was 0.7, 0.6 and 0.6, with no underfill; the forced application and
full plans did not select ANN indexes. These serial synthetic samples use
administrative views, repeat the same query, and contain no concurrent
writers or generation-delay distribution. They do not establish a general
performance ranking, and the earlier archive is not the final candidate.
Attestation checks disk files and endpoint identity, not mapped library bytes.
