# Exact-package live RAG sample

**Status:** one exact-package 10k/384 mixed-workload run; not release sign-off.

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
the complete run output is in [`10k-384-run.log`](10k-384-run.log). Larger
exact-package runs at 100k/768, 100k/1536 and 1m remain outstanding, as do
package-bound concurrent-write and recovery measurements.
