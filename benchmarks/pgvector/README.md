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
recall thresholds. The weekly E2E Benchmarks workflow runs dimensions 384,
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
It forces ANN index eligibility, checks the actual `EXPLAIN ANALYZE` plan, and
stores that plan, buffer counts, index/corpus sizes, individual recalls and
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
the group and strategy order. It checks each selected strategy and affected
group against native AVG, and records individual refresh times and percentiles
without EXPLAIN. Set `PGVECTOR_CENTROID_SAMPLES` from 1 to 50 to change that
count; a small sample cannot establish tail latency. The workload does not
measure source-write overhead or concurrent load. Use a dedicated host and
more repetitions before drawing performance conclusions.

A [recorded emulated run](results/2026-11-07-emulated/README.md) includes the
raw JSON for all three dimensions and documents its limitations.
The later [local centroid comparison](results/2026-11-07-centroid-arm64/README.md)
includes the experimental algebraic path and its raw observations.
The [source-read comparison](results/2026-11-08-source-reads-arm64/README.md)
adds same-transaction source tuple counters and larger affected groups.
The [repeated centroid comparison](results/2026-11-08-repeated-centroid-arm64/README.md)
records 20 rotated-order refreshes per group and strategy at each dimension.
