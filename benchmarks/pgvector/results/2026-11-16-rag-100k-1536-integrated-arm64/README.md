# Integrated v0.109.0 live RAG run

This is a local qualification run of the integrated candidate at
`8c40838705e4ee29a2c6aa4dd9cffbf76b7aa960`. It is not an official release
artifact. The local package archive used to build the test image had SHA-256
`c1c45a3d8fb2b593b42cb2e6968e9158c6dd1877e2afb66adf950686dc404e30`; the
installed four-file payload was checked before and after the run and had SHA-256
`2b7a99e082fc765056937f8bd5027177d61d3f5c6521004480b0bc077676b559`.

## Setup

- PostgreSQL 18.6, pg_trickle 0.109.0, pgvector 0.8.7.
- Apple M1 Pro; Docker Linux/ARM64, 8,319,504,384 bytes (7.748 GiB) available.
- 100,000 rows, 1,536 dimensions, mixed workload, batch size 12, three cycles,
  tenant 2, no concurrent readers, zero fake-worker delay.
- The worker accepted six jobs per cycle, with no stale completions.
- Initial setup and backfill took 5,732.94 seconds (95.5 minutes).
- The Docker image ID was `sha256:1009256a0345ef4aa05fdd396be2a53e20a7cc5659920a1423ca4821046a25cb`.

The full machine-readable results and setup/refresh log are in
[`results.json`](results.json) and [`run.log`](run.log). The JSON records the
candidate archive and installed payload attestations. The container remained
healthy without restart or OOM. Intermittent Docker memory samples ranged from
about 0.7 to 4.3 GiB; sampling was not continuous.

## Correctness and timings

All three corpora matched the authoritative truth at initialization and after
each cycle. The run's p50/p95 timings (three samples) were:

| Operation | p50 | p95 |
|---|---:|---:|
| Source transaction | 57 ms | 66 ms |
| Commit to eligible embeddings | 481 ms | 483 ms |
| Fake embedding generation | 435 ms | 435 ms |
| Application maintenance | 1.57 s | 2.34 s |
| Differential maintenance | 20.96 s | 31.37 s |
| Periodic full maintenance | 2,032.92 s | 2,034.22 s |

The three approaches ran serially in a rotating order. Maintenance timings are
per operation and include psql startup; the separate commit-to-searchable fields
in the JSON include earlier serial work and should not be compared as isolated
per-approach latency. WAL counts are cluster-wide and can include unrelated
activity.

The integrated ANN supervisor E2E filter also passed all four selected tests
against the local candidate image with pgvector 0.8.7; three longer cooldown
and sustained-load cases were ignored by that filter.

## Retrieval limitation

The normal planner and hybrid planner did not use the HNSW index for any corpus.
Their vector recall@10 and hybrid recall@10 were 1.0 in all three cycles, so
those results show correct exact/planner retrieval but do not establish ANN
recall. The forced path used `differential_ann` and returned 10 rows, but only
one matched the exact top 10 (recall 0.1) in each cycle. The forced path used
`hnsw.iterative_scan=strict_order`, `hnsw.ef_search=40` (the default), and
`hnsw.max_scan_tuples=20000` for the highly selective tenant-2 filter. The
application and periodic-full forced plans did not use their ANN indexes.

A post-run, session-only probe on the same differential current view and query
raised `hnsw.ef_search` to 1000; recall improved to only 3/10. The probe was
diagnostic and is not part of the benchmark samples. This synthetic, selective
tenant query remains a material ANN-quality gap; the exact planner result must
not be presented as proof of HNSW quality.

## Scope

A 1M-document RAG run is outside the v0.109.0 scope: the required corpus
matrix stops at 100,000 documents, and larger corpora remain unvalidated.
Earlier 1M-source-row subscription samples are historical evidence from a
separate workload and do not qualify this RAG run. Initial setup alone took
about 95 minutes at 100k rows. The official qualification workflow was run
separately on package SHA
`3ac73e99674ee291acd7b73a79945e09974dd3a6`; it failed the Criterion regression
budget and published no release artifacts. That package SHA is not this
integrated benchmark SHA.
