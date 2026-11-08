# Repeated centroid refreshes, 2026-11-08

These local measurements are evidence for this workload, not a production cost
model. PostgreSQL 18.6, pgvector 0.8.7 and pg_trickle 0.110.0 ran in Linux
arm64 containers on the Apple M1 Pro with 32 GiB host memory described in the
previous local report. The host reported macOS 27.2 and Docker Server 29.9.2.
Resource limits and other host activity were not controlled. The packaged image
ID was `sha256:1ff6a798c7e818fb55a080d84b4c787e3076ed3256f99579f9bebd0f77974f08`.

The dirty checkout was based on `8b73de89f8d145d31fe65ccf82876728d5c843f3`;
its tracked binary diff had SHA-256
`33e56ab8a6d278e8575c483328962b539a7f3d066be4ebecee6bae1cc4c38199`.
The benchmark source had SHA-256
`4f55e5840783c16e36ddfea35a9eeafc5707f3ab13734efcc6bdedb085ed3fa3`.
The untracked centroid stage and numerical source files had SHA-256
`90ef31c90fa496a237ef8b66bb46eac5575304a218faa163e7b67f46e59a7ef1`
and `e8d17cc9fb1e7c3c6bcb471e2aef2563786ffe782c6cf6838d7930abda30aeb9`.
The image identifies the installed binary; the dirty checkout is not a
reproducible commit.

Each dimension started with 5,000 deterministic vectors and 20 ANN samples.
After the preceding refresh workload deleted 500 rows, the affected small and
large groups contained 45 and 2,250 rows. Twenty rounds alternated one source
vector between two values. Each round refreshed separate small- and large-group
tables using group rescan, algebraic state, and FULL. Group and strategy order
rotated across rounds. Every refresh checked its recorded strategy and compared
the affected group componentwise with native AVG within `1e-5 + 1e-6 * |AVG|`.
The times below are client-observed milliseconds for the refresh call, excluding
the source mutation, strategy-setting statements, validation, and EXPLAIN.

| Dimensions | Group | Rescan p50 / p95 | Algebraic p50 / p95 | FULL p50 / p95 |
|---:|---|---:|---:|---:|
| 384 | small | 15.00 / 19.87 | 23.88 / 25.39 | 23.64 / 24.88 |
| 384 | large | 21.94 / 23.05 | 24.04 / 25.07 | 22.90 / 23.91 |
| 768 | small | 31.03 / 53.54 | 45.83 / 74.79 | 72.24 / 119.89 |
| 768 | large | 55.07 / 69.23 | 43.33 / 84.07 | 65.27 / 157.20 |
| 1536 | small | 19.55 / 38.31 | 37.56 / 76.08 | 104.10 / 161.45 |
| 1536 | large | 63.17 / 114.88 | 37.02 / 89.75 | 97.62 / 189.73 |

Each cell summarizes only 20 observations; p95 is the nineteenth sorted value,
not a reliable tail-latency estimate. There was no explicit warmup, and prior
validation and refreshes affected cache state. The runs were sequential by
dimension and the shared host exhibited considerable timing variation. They do
not measure concurrent writers, rebase frequency, source-write overhead, or
frequently rewritten groups. Auxiliary bytes, WAL, source tuple counters and
plans are included for the separate initial instrumented sample in each report,
not for every repeated observation.

The [384](384.json), [768](768.json), and [1536](1536.json) reports include all
rounds, their execution order, strategies and observations. More diverse group
sizes, rewrite rates and controlled repetitions are needed before choosing or
calibrating an automatic strategy-selection policy.
