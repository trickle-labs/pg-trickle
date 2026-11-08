# Repeated centroid rewrites, 2026-11-08

These local observations characterize one workload; they do not establish a
general cost threshold. PostgreSQL 18.6, pgvector 0.8.7 and pg_trickle 0.110.0
ran in Linux arm64 containers on the Apple M1 Pro with 32 GiB host memory
described in the earlier local report. The host reported macOS 27.2 and Docker
Server 29.9.2. Resource limits and other host activity were not controlled.
The packaged image ID was
`sha256:a1dae9c3ee99269afbe1b5e6048f2e2e8b4133fdfecc4ebed0328fb0bb16ac0f`.

The dirty checkout was based on `6a7e8fc9a01cb3899410568fbedafa1c8c623a3a`;
its tracked binary diff had SHA-256
`9148fd0199e1af85de3bdb3465556dccc409fd60cf0c342003fab18ec0fbefc4`.
The benchmark source at measurement time had SHA-256
`d28991f66bea46acf076276ea940ef412093205bfc28d51b7344b1aca8dcd76a`.
The image identifies the installed binary; the dirty benchmark checkout is not
a reproducible commit.

Each dimension started with 5,000 deterministic vectors and 20 ANN samples.
After the preceding refresh workload deleted 500 rows, twenty rounds each
updated 200 surviving source rows. Those rows touched all 100 small groups and
both large groups. Separate stream tables used group rescans, algebraic state,
and FULL refresh; group and strategy order rotated between rounds. Each refresh
verified its recorded strategy and compared every affected group componentwise
with native AVG within `1e-5 + 1e-6 * |AVG|`. The test forced differential
mode for the two differential variants to measure them even when automatic
policy might select FULL. It asserted that every algebraic sample actually used
algebraic state; no rebase was counted as an algebraic sample.

Times are client-observed milliseconds for the refresh call, excluding the
source mutation, setting statements, correctness checks, and EXPLAIN.

| Dimensions | Group shape | Rescan p50 / p95 | Algebraic p50 / p95 | FULL p50 / p95 |
|---:|---|---:|---:|---:|
| 384 | 100 small groups | 59.44 / 66.51 | 130.15 / 132.55 | 23.83 / 24.87 |
| 384 | 2 large groups | 36.35 / 37.05 | 42.15 / 47.21 | 22.83 / 24.76 |
| 768 | 100 small groups | 76.88 / 79.93 | 233.98 / 247.39 | 44.75 / 46.68 |
| 768 | 2 large groups | 63.56 / 65.38 | 60.71 / 61.69 | 43.45 / 45.95 |
| 1536 | 100 small groups | 151.90 / 190.02 | 563.14 / 616.77 | 104.75 / 149.91 |
| 1536 | 2 large groups | 133.41 / 153.63 | 120.50 / 148.07 | 96.64 / 121.01 |

Each cell has 20 observations; p95 is the nineteenth sorted value and is not a
reliable tail-latency estimate. There was no explicit warmup, validation itself
affects the cache, and runs were sequential by dimension. The workload does not
measure concurrent writers or source-write overhead. The separate initial
single-row sample includes source tuple counters, WAL, state-size estimates,
and plans; those are not per-round measurements of the 200-row rewrite.

The [384](384.json), [768](768.json), and [1536](1536.json) reports preserve all
observations and execution order. Larger batches can trigger the numerical
rebase guard and are deliberately rejected as algebraic samples by this
benchmark; repeated-rebase behavior is covered separately by an E2E regression.

During development, two preliminary one-row smoke runs at 384 dimensions and
1,000 input rows failed: one reported an affected-group mismatch and another
did not use the expected algebraic strategy. They occurred before detailed
failure diagnostics were added, so the cause is unknown. Thirteen subsequent
sequential runs and forty subsequent runs in pairs of isolated containers passed;
those passes do not explain or rule out the earlier failures.
