# Emulated PostgreSQL 18 pgvector workload, 2026-11-07

These observations are a reproducibility example, not a production benchmark.
They were collected from commit `e941e139b416130974a55864e396e8e3e72381c6`
using the workload described in the [benchmark README](../../README.md).
The host was an Apple M1 Pro with 32 GiB RAM; Docker Desktop 29.9.2 exposed
10 CPUs and approximately 7.75 GiB RAM to a Linux ARM64 VM. The PostgreSQL
container was **x86-64 under emulation**, so neither latency nor throughput
should be compared with native deployments.

The packaged extension was pg_trickle 0.110.0 with PostgreSQL 18.6 and
pgvector 0.8.7. The test image was
`sha256:4857c2702080433d0a69b31fdd0812a98fdaa812a8fe94c4a2b5b9d52d60711f`.
Each file contains the complete JSON output for 1,000 deterministic vectors,
ten tenants, 20 query samples, and the indicated vector dimension. The test
forces ANN index eligibility; default search settings were `hnsw.ef_search=40`
and `ivfflat.probes=1`, with ten IVFFlat lists. Recall compares IDs against an
exact cosine scan with the same tenant filter. These results illustrate that
filtering after ANN candidate selection can return fewer than ten neighbors.

| Dimensions | HNSW mean recall@10 | IVFFlat mean recall@10 | HNSW p50 / p95 ms | IVFFlat p50 / p95 ms |
| --- | ---: | ---: | ---: | ---: |
| [384](384.json) | 0.505 | 0.205 | 3.71 / 13.41 | 3.50 / 14.28 |
| [768](768.json) | 0.455 | 0.190 | 3.07 / 13.86 | 2.78 / 7.62 |
| [1536](1536.json) | 0.430 | 0.225 | 6.33 / 12.61 | 6.68 / 16.58 |

Each JSON file also contains per-sample recall and latency, p99, index and
corpus sizes, the actual retrieval plan, refresh latency and cluster-wide WAL,
and one-row centroid refresh observations for small and large groups. The
centroid observations compare group rescan with FULL refresh; algebraic vector
state is not implemented or measured. Each refresh observation is a single
run with differential first. The separate group query plan is illustrative,
not the actual refresh plan. The benchmark excludes source-write overhead and
concurrent load, and does not establish a numerical error bound for an
algebraic implementation.

Run each dimension with `PGVECTOR_BENCH_DIM` set to `384`, `768`, or `1536`,
and the other settings from the benchmark README. Index construction can be
randomized, and cache and scheduling effects can change repeated results.
