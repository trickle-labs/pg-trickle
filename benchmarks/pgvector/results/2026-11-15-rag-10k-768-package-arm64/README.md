# Concurrent RAG workload, 10,000 documents and 768 dimensions

On 2026-11-15, integrated test source `33a55f44220403eeac71ba779fc63cd771b9f12c`
ran the three approaches in fixed order on PostgreSQL 18.6 and pgvector 0.8.7,
in Linux ARM64 Docker on an Apple M1 Pro. Each fresh database installed the
v0.109.0 package built from `7e7b51c2a7f31cdf24d06408cd815e4b6b147a77`,
archive SHA-256 `90d121bfa63fb30197ce535a78be93367b0f8e72dad2ae52d06b0b9a9e94591b`.
The retained [installation record](attestation.json.gz) is for the final
differential database: all 157 candidate and installed files and both payload
digests match. The record path was reused and overwrote the earlier records.
This is not the final release candidate.

The [log](run.log) contains the raw server and client intervals. Each approach
received the same 100 ordered visibility changes, one restricted tenant reader,
and maintenance targeting a 200 ms cadence. Initial and final corpora matched
the defining query; differential refresh did not fall back to full refresh.
The application watermark is valid only for this ordered trace. The corpus was
90/10 tenant-skewed and had deterministic varied vectors.

| Approach | Maintenance calls | Guard calls | Client guard p95 | Client maintenance p50 |
| --- | ---: | ---: | ---: | ---: |
| Application batching | 28 | 50 | 3.80 ms | 1.88 ms |
| Periodic full | 1 | 1 | 62.08 s | 62.16 s |
| Differential | 3 | 63 | 4.49 ms | 3.16 s |

The periodic-full guard waited for its single concurrent maintenance call;
one observation does not provide a meaningful percentile. These sequential,
single-run results have different sample counts and are not a speedup or SLA.
This synthetic visibility-only workload does not establish ANN recall, CPU or
WAL costs, embedding-generation latency, or production-scale capacity.
