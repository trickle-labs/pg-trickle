# v0.88 vector aggregate gate

This directory freezes the primary v0.88.0 aggregate workload. Do not change
`contract.json` after recording the v0.87.17 baseline. A benchmark bug fix
requires review and a new baseline.

The workload uses PostgreSQL 18.3, 1,000,000 source rows, 10,000 integer
groups, and a deterministic 100,000-row mixed delta. The stream table computes
`SUM(int4)`, `COUNT(*)`, and `AVG(int4)`. Run one warm-up and five measured
refreshes. Compare the median changed rows per second and validate the exact
multiset after every refresh.

Run the checked-in E2E gate with one test thread:

```bash
PGS_VECTOR_BENCH_BASELINE_JSON=benchmarks/vector-aggregate-v0.88/baseline-v0.87.17.json \
PGS_VECTOR_BENCH_JSON=/tmp/vector-agg-v0.88.0.json \
cargo test --test e2e_bench_tests --features pg18 -- \
  --ignored --test-threads=1 --nocapture bench_vector_aggregate_v088
```

Record the host CPU, memory limit, Rust toolchain, extension commit, container
image digest, p50 and p95 refresh time, peak RSS, temporary bytes, page count,
emitted groups, and apply time with each result. The v0.88.0 result must be at
least 5 times the saved v0.87.17 throughput. An eligible production-like
workload may not regress by more than 10 percent.

`baseline-v0.87.17.json`, `result-v0.88.0.json`, and `comparison.json` are the
saved release evidence. The release gate validates their versions, sample
counts, exact checks, selected strategy, and throughput ratio.
