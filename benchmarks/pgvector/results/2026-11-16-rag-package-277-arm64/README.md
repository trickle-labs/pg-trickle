# Exact-package live RAG sample

**Status:** exact-package 10k/384 and 100k/768 mixed-workload runs; not release sign-off.

The run used the Linux ARM64 archive built from candidate `277efecf7305246092c549e1e643f9449ecdf9af` (v0.109.0), PostgreSQL 18.6 and pgvector 0.8.7. Archive SHA-256 is
`9b04a50a25ecde66dc0cfeed28c4885e0bd03d2d1d8bc3ccbbd25d708392249f`;
the runner verified the manifest and all 157 installed package files before
and after the run. The installed payload SHA-256 was
`5e61176f38df99eb216fbc94c771276f5c671e68c3cfc7c9a45e970f45714ad9`.

The deterministic workload used 10,000 rows, 384 dimensions, tenant 2, three
mixed cycles of 1,000 slots, one hybrid reader per approach, and a uniform fake
embedding delay with mean 0.02 seconds and seed 1396. Initial setup took 90.96
seconds. Its 10,000-row initialization exceeded the 1,500-change threshold and
used a FULL fallback; all three subsequent differential refreshes were
incremental.

All three corpora matched their defining SQL at setup and after every cycle.
Median maintenance times were:

| Approach | Median seconds |
|---|---:|
| Application batching | 2.06 |
| Differential | 2.63 |
| Periodic full | 29.01 |

During maintenance, 63/64 application reads and 80/81 differential reads
overlapped the database maintenance intervals. The periodic-full reader had one
timeout across the three cycles. This is a single-reader local run; it does not
measure concurrent writers or host-wide CPU and lock behavior.

The guarded-current view's normal planner did not use HNSW for any approach.
The forced diagnostic selected HNSW only for the differential corpus, with
recall@10 of 0.9, 0.8 and 0.8 across the three cycles and no underfill. The
application and periodic-full diagnostics used B-tree paths, so their recall
values are not ANN measurements.

The normalized report and full plans are in [`10k-384.json`](10k-384.json);
the complete run output is in [`10k-384-run.log`](10k-384-run.log).

## Exact-package 100k/768 run

This run used 100,000 rows, 768 dimensions, batch size 1,000, three mixed
cycles, tenant 2, one reader, and the same uniform fake embedding delay and
seed. It used the exact candidate archive and installed-file attestation
described above. Setup took 3,216.31 seconds (53.6 minutes); all three corpora
matched their defining SQL at setup and after every cycle. The initial
differential initialization used its FULL fallback at 15,001 changes against
the 15,000-row threshold. All three changed-cycle differential refreshes were
incremental with `explicit_dml_reconciled`.

| Approach | Median seconds | p95 seconds |
|---|---:|---:|
| Application batching | 8.50 | 8.85 |
| Differential | 18.11 | 20.77 |
| Periodic full | 1,146.53 | 1,405.48 |

The source transaction p95 was 343.29 seconds; commit-to-eligible p50/p95 was
48.76/58.61 seconds. The periodic-full reader timed out 48, 39, and 38 times
in the three cycles (two completed queries per cycle). Application and
differential readers had no timeouts. All retrievals returned exact top-10
results without underfill. The normal planner did not use HNSW; forced HNSW was
selected only for differential and had recall@10 of zero for this query.

Final relation sizes including indexes were 860 MB for application, 841 MB for
periodic full, and 877 MB for differential. Docker sampling observed a peak of
7.099 GiB out of 7.748 GiB; the run completed without an OOM event. The full
report, run log, and resource samples are in [`100k-768.json`](100k-768.json),
[`100k-768-run.log`](100k-768-run.log), and
[`100k-768-docker-stats.csv`](100k-768-docker-stats.csv).

Exact-package 100k/1536, package-bound concurrent-write and recovery
measurements remain outstanding. A 1m/384 RAG run is outside the v0.109.0
scope.
