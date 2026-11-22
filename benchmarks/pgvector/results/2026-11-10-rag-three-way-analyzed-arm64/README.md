# Three-way RAG maintenance under concurrent reads and writes

On 2026-11-10, committed test source
`93a76ee8577a5c813e27aa2e260395719e61dec3` passed two trials each with
pgvector 0.8.1 ([1](pgvector-081-1.log), [2](pgvector-081-2.log)) and 0.8.7
([1](pgvector-087-1.log), [2](pgvector-087-2.log)). PostgreSQL 18.6 ran in
Linux ARM64 Docker on an Apple M1 Pro. Images used the earlier v0.109.0
candidate `1e291536bf0380b37a509b730155ca3d3a0ccea4`; these runs did not
collect separate installed-file attestations.

Each trial ran application batching, periodic full recomputation and
differential refresh in that order, each in a fresh database with 1,000
documents, identical 384-dimensional vectors, a 900/100 tenant split and
the same ordered sequence of 100 visibility changes. Each writer paused 50 ms
after a commit; one restricted tenant reader repeatedly checked visibility
and ran a vector-ordered query. Each maintenance loop targeted a 200 ms cadence
and skipped missed ticks. Initial source and target tables were vacuumed and
analyzed. Unused demo streams were dropped in every database; the application
and full databases had no stream table, while the differential database kept
only its target stream. The extension remained installed in all three.

The application baseline batches newly hidden IDs using a transactional
watermark. That watermark is valid for this ordered single-writer trace; it is
not a general change queue for out-of-order or arbitrary updates. All three
target corpora matched their defining SQL after final reconciliation; every
measured differential refresh and its final reconciliation reported a
completed differential action without full fallback.

Across the four trials, client-side p95 guard-query latency was 4.05–4.60 ms
for application, 162.75–376.48 ms for periodic full, and 1.26–1.60 ms for
differential. Client p95 maintenance-call latency was 2.51–5.77 ms,
325.92–528.79 ms, and 169.94–174.52 ms, respectively. These are ranges of
four per-trial percentiles, not pooled percentiles. The logs retain every
client and server interval, call counts, and p50/p95/p99 summaries. Full
maintenance missed more target ticks in some trials. Application maintenance
was brief enough that zero or one write statement overlapped it in server
time; client windows recorded four to seven overlapping write requests.

Server and client intervals measure different boundaries. In particular,
server statement timestamps can omit time waiting before execution; the
client guard interval includes that wait and network overhead. Client-window
overlap does not prove simultaneous database execution, and the write
statement interval ends before commit. The raw samples allow the reported
overlap counts to be recomputed.

The [exploratory archive](exploratory-prototypes.tar.gz) retains earlier
uncommitted prototype logs: `load` revisited previously hidden IDs and kept
unused streams, `watermark` fixed the former but retained unused streams,
and `isolated` removed unused streams without the final vacuum/analyze step.
Those runs are not the comparison above. One `load` 0.8.7 trial recorded
81.068 ms writer-client p95 during differential maintenance; its immediate
repeat recorded 2.982 ms. The cause is unknown and those prototype sources
were not frozen as commits.

This is a short, ordered synthetic visibility workload, not a representative
production RAG trace or an SLA. It uses equal vectors, no embedding delay,
one writer and one reader; it does not measure ANN recall, CPU or WAL, or
establish a general speedup. Trial order was fixed, random revision IDs differ
between databases, and scheduler/autovacuum effects were not controlled.
The package is an earlier candidate, not final-release qualification. The
test is opt-in because actual overlap is scheduling-dependent.

```sh
cargo test --features pg18 --test e2e_pgvector_revision_tests \
  test_live_rag_unblocked_writes_and_reads_during_refresh -- --ignored --nocapture
```
