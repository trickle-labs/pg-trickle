# Varied-vector concurrent RAG workload

On 2026-11-10, test source `81f1411b` exercised the three maintenance
approaches with deterministic varied vectors on PostgreSQL 18.6 in Linux
ARM64 Docker on an Apple M1 Pro. It installed the earlier v0.109.0 package
from commit `1e291536bf0380b37a509b730155ca3d3a0ccea4`; the archive SHA-256
was `2cd9d52c1d9d744557e2f16a7faf1cb5c44213fdc72c72beebed37122b34f73b`.
For each run, the accompanying compressed installation record lists the same
157 candidate and installed files and matching payload digests at container
initialization. This is not final-candidate release qualification.

| Rows | Dimensions | pgvector | Test log | Installation record |
| ---: | ---: | --- | --- | --- |
| 1,000 | 384 | 0.8.1 | [log](1k-081.log) | [record](attest-1k-081.json.gz) |
| 1,000 | 384 | 0.8.7 | [log](1k-087.log) | [record](attest-1k-087.json.gz) |
| 1,000 | 768 | 0.8.7 | [log](1k-768-087.log) | [record](attest-1k-768-087.json.gz) |
| 1,000 | 1,536 | 0.8.7 | [log](1k-1536-087.log) | [record](attest-1k-1536-087.json.gz) |
| 10,000 | 384 | 0.8.7 | [log](10k-087.log) | [record](attest-10k-087.json.gz) |

Each approach ran in a fresh database in fixed order: application batching,
periodic full recomputation, then differential refresh. The corpus was split
90/10 between tenants, and 100 ordered visibility changes were committed by
one writer with a 50 ms pause; one restricted tenant reader checked visibility
and issued vector-ordered queries. Maintenance targeted a 200 ms cadence,
skipping late ticks. Initial and final results were checked against the exact
defining query, and the differential refreshes reported no full fallback.
The application watermark is valid for this ordered trace, not arbitrary or
out-of-order changes. The logs retain client and server intervals and samples.

The 10,000-row, 384-dimensional pgvector 0.8.7 run had one periodic-full
maintenance call, taking 32.8 seconds, during which one guard call waited
32.7 seconds client-side. Its differential phase made six maintenance calls
and 109 guard calls; client guard p95 was 6.05 ms. These are single local
observations with very different sample counts, not a general speedup or SLA.
Client timing includes waiting that server statement timing may omit.

The 1,000-row pgvector 0.8.7 and 768-dimensional runs overlapped in time, so
their timing may be affected by contention. This synthetic workload measures
visibility-only changes, uses one writer and reader, and does not measure ANN
recall, CPU, or WAL. Autovacuum and scheduler effects were not controlled.
The supported parameter range is not a tested capacity limit.
