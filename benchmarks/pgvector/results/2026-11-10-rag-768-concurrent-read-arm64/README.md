# Live RAG mixed batches at 768 dimensions

On 2026-11-10, runner commit `76ecd0f3744ba12debea4849391e84c34ca1bd94`
completed three mixed 12-slot batches over 10,000 seeded documents, with one
concurrent hybrid reader during each approach's maintenance. The workload ran
on PostgreSQL 18.6 / pgvector 0.8.7 in Linux ARM64 Docker with 1 GiB shared
memory on an Apple M1 Pro. The image was
`sha256:f1a28666a96308630d613d012b3889628ab8f295d7e2f0a436c000011ba31332`.
The earlier v0.109.0 archive, candidate
`1e291536bf0380b37a509b730155ca3d3a0ccea4`, had SHA-256
`2cd9d52c1d9d744557e2f16a7faf1cb5c44213fdc72c72beebed37122b34f73b`.
The [report](results.json.gz) records successful before-and-after attestation
of 157 installed files and database identity. The [log](run.log.gz) includes
the initial full-refresh fallback and measured differential refreshes.

All three corpora matched the defining query after initialization and each
batch; the measured stream refreshes were differential without full fallback.
For the three serial, rotated maintenance samples, application maintenance
took 0.104–0.113 seconds, differential 1.125–1.157 seconds and periodic full
60.933–61.345 seconds. These times include `psql` startup. Initial setup took
181.6 seconds. Generation median was 0.406 seconds, and commit-to-eligible
median was 0.447 seconds including the post-commit availability check.

Application and differential maintenance each had completed database-time
reader overlaps and no client timeouts. Every periodic-full step had two
30-second client timeouts overlapping its maintenance window; during the run,
database activity showed readers waiting on its relation lock. Client timeout
overlap is measured against the client window, not a completed database
interval. The report retains individual samples and query plans.

The normal planner selected no ANN index for these tenant-filtered queries.
The forced differential HNSW path returned recall@10 of 0.6 with no underfill
in each sample; forced application and full plans did not use ANN indexes.
These are repeated queries on one synthetic workload, not independent recall
trials or a performance ranking. The approaches run serially, views are
administrative, and there are no concurrent writers or generation-delay
distribution. The package is an earlier candidate, not final release evidence;
attestation checks disk files and endpoint identity, not mapped library bytes.
