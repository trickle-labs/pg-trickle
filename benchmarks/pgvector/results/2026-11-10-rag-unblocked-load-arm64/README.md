# Unblocked RAG writes, tenant reads and differential refresh

On 2026-11-10, two runs each with pgvector 0.8.1 and 0.8.7 passed on
PostgreSQL 18.6 / Linux ARM64 Docker on an Apple M1 Pro. The images used the
earlier v0.109.0 candidate `1e291536bf0380b37a509b730155ca3d3a0ccea4`;
no separate installation attestation was collected for these runs.

Each isolated run seeded 1,000 documents with identical 384-dimensional
vectors and a 900/100 tenant split. One writer committed 100 visibility
changes with a 50 ms pause after each. A separate restricted tenant reader
repeated visibility checks and vector-ordered queries while a third session
ran differential refreshes without a test barrier. Every measured refresh
and the final reconciliation reported differential completion without full
fallback; the final stream matched its defining SQL. The reader checked that
already acknowledged hidden documents and foreign-tenant rows did not appear.

| pgvector / run | Write statements overlapping refresh | Vector reads overlapping refresh | Refresh calls | Wall seconds |
| --- | ---: | ---: | ---: | ---: |
| [0.8.1 / 1](pgvector-081-1.log) | 83 / 100 | 177 / 208 | 38 | 5.496 |
| [0.8.1 / 2](pgvector-081-2.log) | 82 / 100 | 183 / 210 | 40 | 5.558 |
| [0.8.7 / 1](pgvector-087-1.log) | 84 / 100 | 177 / 213 | 43 | 5.615 |
| [0.8.7 / 2](pgvector-087-2.log) | 82 / 100 | 179 / 210 | 41 | 5.556 |

Each log includes individual PostgreSQL timestamp intervals and p50/p95/p99
summaries. Overlap means the write statement or vector-read interval intersects
the instrumented refresh-function interval; it does not prove that a write's
commit occurred within that interval. Write timings exclude commit, vector-read
timings exclude the separate guard check, and refresh timings exclude the
outer transaction commit. Wall time includes deliberate writer pacing.

This is an opt-in overlap test, not a universal timing gate. It covers one
writer, one reader, visibility updates and the differential approach only;
it does not compare application and full maintenance under the same load,
measure varied vectors, prove ANN recall, or qualify the final package. Two
short synthetic runs per version are not a capacity or SLA claim. To repeat:

```sh
cargo test --features pg18 --test e2e_pgvector_revision_tests \
  test_live_rag_unblocked_writes_and_reads_during_refresh -- --ignored --nocapture
```
