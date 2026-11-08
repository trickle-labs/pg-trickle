# Filtered pgvector workload

## Version conformance

The PR compatibility workflow packages pg_trickle for PostgreSQL 18.6 and
tests pgvector 0.8.1 and 0.8.7 from digest-pinned upstream images. The first
is a historical PostgreSQL 18 compatibility target, not a deployment
recommendation; see the [pgvector changelog](https://github.com/pgvector/pgvector/blob/master/CHANGELOG.md)
for fixes in later releases. The selected tests assert the installed versions
and exercise a bounded set of helper, index, aggregate, security, and example
paths. They do not prove every pgvector capability.

To build a matching image locally, run `just build-e2e-image` followed by
`./tests/build_e2e_pgvector_image.sh 0.8.7`. Set
`PGS_E2E_IMAGE=pg_trickle_e2e_pgvector:0.8.7` and
`PGVECTOR_EXPECTED_VERSION=0.8.7` when running the cases in
`.github/workflows/pgvector-conformance.yml`.

## Workload measurements

This opt-in database benchmark runs against the packaged extension in a
Testcontainers PostgreSQL instance. It emits one `PGVECTOR_BENCHMARK_JSON=`
line containing raw observations; it does not impose universal latency or
recall thresholds. The scheduler is paused for each benchmark stream so the
workload controls its refreshes. The weekly E2E Benchmarks workflow runs dimensions 384,
768 and 1536 and uploads the JSON, complete output, commit, host CPU and Docker
version as artifacts.

```sh
just build-e2e-image
PGVECTOR_BENCH_DIM=384 PGVECTOR_BENCH_ROWS=1000 PGVECTOR_BENCH_SAMPLES=20 \
  ./scripts/run_e2e_tests.sh --retries 0 --test e2e_bench_tests \
    --run-ignored ignored-only --no-capture \
    -E 'test(test_pgvector_filtered_ann_workload)' | tee pgvector-bench.txt
```

Coordinates derive deterministically from `md5(id:dimension)`; ten evenly
sized tenants share the corpus. For HNSW and IVFFlat (10 lists), the test
measures sequential client-observed cosine top-10 queries on one tenant and
compares returned IDs to an exact scan with the same vector, filter and metric.
It forces ANN index eligibility and checks its `EXPLAIN ANALYZE` plan. It also
requires a sequential scan of the corpus for the exact baseline and records
both plans, buffer counts, index/corpus sizes, individual recalls and
latencies, and nearest-rank p50/p95/p99. The report includes server and
extension versions and the actual HNSW/IVFFlat search settings. ANN construction
can use randomness despite deterministic input; do not infer production
latency or recall from one run or treat forced eligibility as the default plan.

The refresh section measures a one-row vector update, a 10% vector update,
and a 10% delete on separate differential and full-refresh stream tables with
equivalent HNSW indexes. Both must match their complete defining query and the
recorded history must confirm the intended strategy. The differential refresh
always runs first, so shared cache and other ordering effects can bias the
comparison. WAL is measured between cluster-wide LSN reads, excludes the source
mutation and can include unrelated activity; refresh latencies include client
overhead. A plan is recorded for retrieval, not for the refresh internals.
The centroid baseline additionally compares one changed vector in roughly 100
small groups and two large groups across group-rescan, opt-in algebraic, and
FULL refresh. It records the selected strategy, affected group size, refresh
latency, cluster-wide WAL, stream storage, estimated logical auxiliary-state
bytes, and maximum component error against native AVG. Each refresh runs under
`EXPLAIN (ANALYZE, BUFFERS, WAL)`; its top-level counters include the call but
do not necessarily expose plans for statements executed through SPI. A separate
affected-group query illustrates a possible rescan. Same-transaction PostgreSQL
tuple counters record sequential returns and index fetches from the source during
each refresh, including repeated visits; they are not counts of distinct rows.
This is one observation per
strategy in the order listed; EXPLAIN overhead and cache order affect timings.
The workload also performs five repeated one-row changes by default, rotating
the group and strategy order. Set `PGVECTOR_CENTROID_REWRITE_ROWS` to update
more rows per round, up to the surviving source rows less two fixed inputs.
It checks each selected strategy and all affected groups against native AVG,
and records individual refresh times and percentiles without EXPLAIN. Set
`PGVECTOR_CENTROID_SAMPLES` from 1 to 50 to change the number of rounds; a
small sample cannot establish tail latency. The algebraic measurement asserts
that every round uses algebraic state, so batches that trigger a rebase fail
the benchmark rather than being silently counted as algebraic. The workload does not
measure source-write overhead or concurrent load. Use a dedicated host and
more repetitions before drawing performance conclusions.

The scheduled run also measures distance subscriptions separately with 1,000
and 10,000 rows and zero, one, or ten subscriptions. Five one-row changes per
case alternate across the threshold. It verifies the affected-row snapshot and
differential strategy, and records refresh-call time plus same-transaction
source and stream tuple counters. The stream is analyzed before measurement;
the counters include repeated visits and are not distinct row counts. A listener
verifies each event after commit; those timings exclude client delivery. The
[local report](results/2026-11-08-subscription-scaling-arm64/README.md) also
records an unanalysed control and its limitations.

A [recorded emulated run](results/2026-11-07-emulated/README.md) includes the
raw JSON for all three dimensions and documents its limitations.
The later [local centroid comparison](results/2026-11-07-centroid-arm64/README.md)
includes the experimental algebraic path and its raw observations.
The [source-read comparison](results/2026-11-08-source-reads-arm64/README.md)
adds same-transaction source tuple counters and larger affected groups.
The [repeated centroid comparison](results/2026-11-08-repeated-centroid-arm64/README.md)
records 20 rotated-order refreshes per group and strategy at each dimension.
The [rewrite comparison](results/2026-11-08-rewrite-centroid-arm64/README.md)
repeats 200-row updates and checks every affected group.
The [scheduler-isolated rewrite run](results/2026-11-08-paused-rewrite-arm64/README.md)
repeats that 384-dimensional workload with manually controlled refreshes.
