# Live RAG scale sample

**Status:** candidate-bounded development evidence, not final v0.109.0
qualification. Every run below used commit `bd0813e8202d82b9186457122465887d12b2ca50`
and the same Linux ARM64 package archive (SHA-256
`7974fd80d337ee05f2531158fdcd4a7649d8e4968b3593118099de4c3c1087a5`,
157 files; payload SHA-256
`3b546cc406b48111dffa7dabc59eaed71c3020c78d9e6df0f512aefddd69aea1`).
The package reports pg_trickle 0.109.0, PostgreSQL 18.6 and pgvector 0.8.7.
The candidate predates the integer-aggregate fix now on the package PR, so these
numbers must not be attributed to that updated candidate.

The runner used deterministic synthetic embeddings and revisions, a fake
embedding worker, tenant 2, a 0.02-second mean per-job delay sampled uniformly
with seed 1396, mixed update/visibility-toggle/delete/insert batches, and one
hybrid reader during maintenance. Application writes were batched. Each cycle
gave all three corpus implementations the same changes and compared the full
corpus to the defining SQL. The 10k matrix ran five cycles at each dimension;
the 100k case ran three cycles at 384 dimensions with batch size 1,000.

## Maintenance results

Median individual maintenance latency, seconds:

| Rows / dimensions | Cycles | Application | Differential | Periodic full |
|---|---:|---:|---:|---:|
| 10k / 384 | 5 | 0.448 | 1.071 | 31.029 |
| 10k / 768 | 5 | 0.919 | 1.984 | 69.297 |
| 10k / 1536 | 5 | 1.880 | 3.523 | 132.446 |
| 100k / 384 | 3 | 5.653 | 11.110 | 548.336 |

Every corpus matched its defining SQL at setup and after every measured cycle.
All three 100k post-backfill stream refreshes completed as differential refreshes
without fallback. The initial 100k differential initialization did cross the
15,000-change threshold and fell back to FULL; setup took 1,742.9 seconds.

At 100k/384, median cluster-wide WAL movement was 61.4 MB for application,
62.3 MB for differential and 891.6 MB for periodic full. Final index-inclusive
relation sizes were 426.2 MB, 434.2 MB and 417.5 MB, respectively. WAL is
cluster-wide and can include unrelated activity. Docker sampled 1,109 database
container observations over 61m39s: CPU median 113%, p95 229%, peak 438%; memory
peaked at 548.8 MiB. Host CPU model, host-wide utilization, locks and a
controlled resource limit were not recorded.

## Retrieval and concurrent-reader results

The exact filtered vector and hybrid baselines returned the expected 10 rows
with recall 1.0 and no underfill. The ordinary planner did not choose HNSW for
any approach or hybrid query. With sequential scans discouraged, the measured
query used HNSW only for the differential corpus; its recall@10 was 0.1, 0.0
and 0.0 across the three cycles, with no underfill. The application and
periodic-full diagnostics instead used their ID B-tree paths and therefore are
not ANN recall measurements. Hybrid plans also did not use HNSW. The benchmark
thus found a severe selective-filter recall problem in this synthetic case and
does not establish comparable ANN quality across all three approaches.

A read-only parameter spot check on the retained 100k/384 database used tenant
2 and the same `seed document 1` query vector after the measured workload. On
the differential corpus, forced HNSW recall@10 was 0.0 with
`ef_search=40`/`strict_order`, 0.2 with `ef_search=400`/`strict_order`, and 0.7
with `ef_search=400`/`relaxed_order`; `ef_search=1000`/`relaxed_order` also
returned 0.7. Each returned ten rows. This single post-run query shows that
pgvector search settings affect the result, but it still missed three exact
neighbors and recorded no latency comparison. It does not qualify a default
setting or resolve filtered ANN quality; multiple query vectors and settings
need measurement on the final package.

During the three maintenance cycles, application and differential readers had
zero timeouts. The periodic-full reader had 56 timed-out queries (18, 18 and
20 per cycle). Client read intervals overlapped maintenance, but this is one
reader and no concurrent writer was used.

## Concurrent-write functional check

A separate [10k/384 local-source E2E run](../2026-11-16-rag-concurrency-local-arm64/README.md)
passed one paced 100-write trace with a restricted tenant reader. It completed
all writes during each approach. Differential maintenance had 99 of 100 writes,
99 of 100 guard reads and 99 of 100 vector reads overlap its server intervals;
periodic full had 100 of 100 writes overlap its 30.916-second refresh, while
its only guard and vector read waited until refresh ended. This is functional
evidence from a local source build, not an exact release archive. It does not
extend the scale matrix's ANN or cost measurements.

## Scoped decision

For these synthetic cases, application batching had the shortest maintenance
time; differential refresh cost about twice as much as application batching
but was roughly 29–49 times faster than periodic full recomputation. The
100k selective-filter HNSW result is not acceptable evidence of good ANN
quality: the ordinary planner avoided HNSW, and the forced differential HNSW
path had very low recall. These measurements support no universal throughput,
freshness or retrieval-quality claim. They only show that differential
maintenance can reduce recomputation work for the measured batches while
leaving filtered ANN quality and final-candidate behavior unresolved.

The per-run JSON includes installation attestations, exact row outcomes,
latencies, WAL, relation sizes, reader overlap, recall and complete EXPLAIN
plans. `*-run.log` files retain command output; Docker CPU/memory samples are
in the corresponding `*-docker-stats.csv` files. Rerun the required cases on
the final integrated v0.109.0 package before using this as release sign-off.
