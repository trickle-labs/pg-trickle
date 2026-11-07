# Filtered pgvector workload

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
and a 10% delete with an HNSW index present. Each refresh must be differential
and match its complete defining query. WAL is measured between cluster-wide
LSN reads, excludes the source mutation and can include unrelated activity;
refresh latencies include client overhead. A plan is recorded for retrieval,
not for the refresh internals. This initial workload does not compare full
refresh, centroid strategies or group sizes, measure source-write overhead,
or characterize concurrent load. Use a dedicated host and more repetitions
before drawing performance conclusions.
