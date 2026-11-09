# Live RAG mixed batches with concurrent reads

On 2026-11-09, a fresh 10,000-document, 384-dimensional workload ran three
12-slot mixed batches on PostgreSQL 18.6 and pgvector 0.8.7 in Linux ARM64
Docker on an Apple M1 Pro. One tenant-filtered hybrid reader ran against each
approach during its maintenance step. The server had 1 GiB shared memory;
the image was `sha256:f1a28666a96308630d613d012b3889628ab8f295d7e2f0a436c000011ba31332`.
The v0.109.0 archive was from [qualification run 42600697708](https://github.com/trickle-labs/pg-trickle/actions/runs/42600697708),
candidate `1e291536bf0380b37a509b730155ca3d3a0ccea4`, archive SHA-256
`2cd9d52c1d9d744557e2f16a7faf1cb5c44213fdc72c72beebed37122b34f73b`.
The attestation in [results.json.gz](results.json.gz) matched 157 installed
files and the database identity before and after the run.

All three corpora matched defining SQL initially and after every batch. Initial
differential population fell back to FULL; the three measured small-batch
refreshes were differential. The [run log](run.log.gz) preserves notices and
the result contains the per-query plans, availability and timing samples.

| Cycle | Approach | Maintenance seconds | Completed reads | Database-time overlaps | Client timeouts overlapping maintenance window |
| --- | --- | ---: | ---: | ---: | ---: |
| 0 | application | 0.076 | 1 | 1 | 0 |
| 0 | periodic full | 31.899 | 2 | 2 | 1 |
| 0 | differential | 0.671 | 7 | 7 | 0 |
| 1 | periodic full | 33.134 | 2 | 2 | 1 |
| 1 | differential | 0.662 | 7 | 7 | 0 |
| 1 | application | 0.077 | 1 | 1 | 0 |
| 2 | differential | 0.665 | 7 | 7 | 0 |
| 2 | application | 0.067 | 1 | 1 | 0 |
| 2 | periodic full | 33.416 | 2 | 2 | 1 |

Completed-read overlap uses the PostgreSQL statement interval. A timed-out
read has no completed database interval; its overlap is only with the
client-side maintenance window. Client latency includes `psql` startup and
the 30-second timeouts are excluded from completed-read percentiles. The
initial [warmup attempt](warmup-failure.log.gz) stopped after a reader exceeded
30 seconds before measurement; the reported run used a fresh container and a
revised harness that records timeouts.

These are three serial, synthetic samples with one reader and no concurrent
writers, not a capacity or general speedup claim. They use administrative
views and an earlier package, not the final release candidate. Attestation
checks on-disk files and endpoint identity, not already mapped library bytes.
The run used an uncommitted harness based on `dde2adcf`; afterward, timeout
cleanup was tightened and the same summary calculation was factored into a
unit-tested helper. The final helper was tested separately, but this result
was not rerun after those source-only changes.
