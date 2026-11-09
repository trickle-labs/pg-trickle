# One-row centroid refreshes at two table sizes, 2026-11-08

These observations explore group size; they do not establish a general cost
threshold. PostgreSQL 18.6, pgvector 0.8.7 and pg_trickle 0.110.0 ran in Linux
arm64 containers on an Apple M1 Pro with 32 GiB host memory. Host activity and
container resources were not controlled. The Docker image manifest ID was
`sha256:4b674e3d60732f3cd250379e0a5b66bca90fe147ec285a8bdf28e7cf51befb8c`;
its arm64 image ID was
`sha256:8a481cd18588ef2b1f645e583f13538f987254782b571c1ba0ed20257d50967c`.
The benchmark harness was run from clean commit `064ac6a7`; the image has no
source revision label, so its exact correspondence to that commit is unverified.

Each run generated 384-dimensional deterministic vectors. After deleting 10%
of the rows in the preceding refresh workload, 50 rounds alternated one
source vector between two distinct values. There were 100 small groups and two
large groups, with 9 and 450 rows per group in the 1,000-row run, and 90 and
4,500 in the 10,000-row run. Separate stream tables used group rescans,
algebraic state, and FULL refresh. Group and strategy order rotated between
rounds. Each refresh asserted its actual strategy where applicable and compared
the affected group against native AVG within `1e-5 + 1e-6 * |AVG|`.

Times are client-observed milliseconds for the refresh call, excluding mutation,
setting statements, validation, and EXPLAIN. Each cell contains 50 observations.

| Initial rows | Group | Rescan p50 / p95 | Algebraic p50 / p95 | FULL p50 / p95 |
|---:|---|---:|---:|---:|
| 1,000 | small | 12.99 / 15.01 | 20.63 / 23.52 | 12.21 / 13.95 |
| 1,000 | large | 14.05 / 16.09 | 20.42 / 21.64 | 11.47 / 12.86 |
| 10,000 | small | 15.18 / 16.42 | 21.25 / 22.71 | 34.99 / 36.11 |
| 10,000 | large | 27.58 / 28.98 | 21.12 / 22.09 | 34.24 / 35.22 |

These sequential runs had no explicit warmup or controlled concurrency; their
validation queries also affect the cache. Each size was measured once and p95
is not a reliable tail estimate. They do not measure concurrent writers,
rebase costs or repeated rewrites. In particular, switching to a rescan can
leave private state unbound, so a temporary per-refresh timing advantage does
not by itself define a safe long-term strategy. The earlier unexplained smoke
failures documented in the [rewrite report](../2026-11-08-rewrite-centroid-arm64/README.md)
remain unresolved.

The raw [1,000-row](1000.json) and [10,000-row](10000.json) reports contain
the observations, plans, source counters and initial storage/WAL measurements.
