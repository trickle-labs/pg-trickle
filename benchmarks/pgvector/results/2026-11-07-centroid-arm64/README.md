# Local centroid comparison, 2026-11-07

These are single observations from the packaged database workload, not a
production performance qualification. The three runs used PostgreSQL 18.6,
pgvector 0.8.7, and pg_trickle 0.110.0 in Linux arm64 containers on an Apple M1
Pro with 32 GiB host memory, macOS 27.2, and Docker Server 29.9.2. Container
resource limits and other host activity were not controlled. The image ID was
`sha256:cbb24fd207eb8b37a04931e7fe30feb4db083feaf5d3eb12ee38f8979578f1fd`.

The source checkout was dirty, based on commit
`8b73de89f8d145d31fe65ccf82876728d5c843f3`. Immediately after the runs, the
tracked binary diff had SHA-256
`309f4c579bad3dd9c53a950d6a663fe72487a1c49138da3e17cc51c480873f90`;
the untracked `src/api/centroid_stage.rs` and `src/dvm/centroid_numerics.rs`
had SHA-256 `54374e91fb8af821a98b9dbbc3054a33e13f332f91aeb14ba43fd1a5ed83256c`
and `e8d17cc9fb1e7c3c6bcb471e2aef2563786ffe782c6cf6838d7930abda30aeb9`,
respectively. The image ID identifies the actual packaged binary; the dirty
checkout is not a reproducible commit.

Each run started with 1,000 deterministic vectors and 20 retrieval samples.
The preceding refresh workload deleted 100 source rows, leaving 900 rows for
the centroid comparison. A single vector changed; the affected small and
large groups had 9 and 450 rows, respectively. Refreshes ran in the fixed
order group rescan, algebraic, FULL, under `EXPLAIN ANALYZE`. Client timings
include configuration and EXPLAIN overhead. Each cell below is one elapsed
time in milliseconds; it is not a percentile.

| Dimensions | Group shape | Group rescan | Algebraic | FULL |
|---:|---|---:|---:|---:|
| 384 | small | 15.15 | 22.52 | 14.30 |
| 384 | large | 16.29 | 22.64 | 12.59 |
| 768 | small | 14.88 | 22.70 | 21.00 |
| 768 | large | 19.04 | 22.89 | 17.46 |
| 1536 | small | 15.12 | 26.35 | 35.98 |
| 1536 | large | 22.28 | 24.23 | 22.19 |

The largest observed component error against native AVG was zero in these
samples. The algebraic state payload estimate was 0.93–3.70 MB across the 100
small groups and 18.7–74.0 kB across the two large groups. Estimates are
logical payloads, not allocated relation or TOAST sizes. For the 384-dimensional
run, cluster-wide WAL during refresh ranged from 40.8–43.2 kB for group rescans
and 156.1–157.5 kB for algebraic refreshes; it excludes the source update and
may include unrelated activity.

The [384](384.json), [768](768.json), and [1536](1536.json) reports contain
the raw plans, settings, latencies, WAL and size observations. `EXPLAIN` does
not necessarily expose nested SPI plans, so these results do not prove how
many source rows the refresh read. The workload does not measure concurrent
writers, source-write overhead, repeated centroid refresh distributions, or
automatic rebase frequency. The measured algebraic path did not beat group
rescan in these observations; a cost-based policy and broader measurements
remain necessary before promoting the strategy.
