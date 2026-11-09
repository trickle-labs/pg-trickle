# 100,000-document RAG rehearsal: interrupted setup and plan investigation

On 2026-11-09, a 100,000-document, 384-dimensional mixed-churn run used the
ARM64 v0.109.0 archive from [qualification run 42600697708](https://github.com/trickle-labs/pg-trickle/actions/runs/42600697708),
candidate `1e291536bf0380b37a509b730155ca3d3a0ccea4`, archive SHA-256
`2cd9d52c1d9d744557e2f16a7faf1cb5c44213fdc72c72beebed37122b34f73b`.
The pgvector 0.8.7 image was
`sha256:f1a28666a96308630d613d012b3889628ab8f295d7e2f0a436c000011ba31332`.
Its [installation observation](installation.json.gz) matched all 157 candidate
PostgreSQL payload files. The host was an Apple M1 Pro using Linux ARM64 Docker
with 10 CPUs and 8,319,504,384 bytes available; PostgreSQL was 18.6.

The original three-cycle run built the three indexed corpora but
[failed](initial-failure.log) at its initial exact-corpus check because the
container's default 64 MiB shared memory was exhausted. Retrying that read-only
check with `max_parallel_workers_per_gather=0` returned
[true for all three corpora](initial-exact.json). Initial setup timing was not
recovered.

The first mixed-churn batch was then committed and all three approaches were
maintained. Its original exact retrieval plan was canceled during a long
nested-loop execution; the [interrupted log](interrupted-cycle.log) is retained.
An [EXPLAIN without ANALYZE](original-exact-plan.log), using the same corpus,
tenant and planner settings with a different synthetic query vector, showed
the no-index exact baseline repeatedly scanning the application corpus. It is
a diagnostic plan, not a completed latency sample.

The updated benchmark temporarily disables nested loops only for exact
baselines, then restores the caller's setting for normal and forced-index
plans. On the populated 100,000-row database, the revised query completed
with [nested loops initially enabled](hash-join-check.json.gz) and
[initially disabled](hash-join-original-off.json.gz). Both exact and hybrid
exact plans used hash joins and no index scans. The normal-planner plans
respected each caller setting. These checks use the updated query at commit
`394e0484226e34878b125b150519b2e25227144d`.

At 13:56:32 UTC, the three hourly fixture schedulers were
[paused](scheduler-control.txt) during the interrupted cycle's retrieval
checks to prevent background refreshes during the long serial comparison.
Manual refresh remains enabled. A one-off [resume patch](resume.patch.gz) checks
the expected post-cycle-0 document count, IDs and seed rows and rechecks exact
corpus equality before running cycles 1 and 2. It reports no setup time and uses
`PGOPTIONS='-c max_parallel_workers_per_gather=0'`. This intervention differs
from the ordinary fresh-database command and is not a controlled comparison
with the earlier 10,000-row run.

The resumed cycles are still running. No completed 100,000-row performance
claim or final release qualification follows from this partial evidence.
