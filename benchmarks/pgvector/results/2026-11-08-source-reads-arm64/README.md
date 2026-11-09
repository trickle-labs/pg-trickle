# Source reads during centroid refresh, 2026-11-08

These are single observations, not a production qualification or a basis for a
universal cost threshold. The packaged workload ran PostgreSQL 18.6, pgvector
0.8.7 and pg_trickle 0.110.0 in Linux arm64 containers on the same Apple M1 Pro
with 32 GiB host memory described in the previous local run. The host reported
macOS 27.2 and Docker Server 29.9.2. Container limits and other host activity
were not controlled. The image ID was
`sha256:1ff6a798c7e818fb55a080d84b4c787e3076ed3256f99579f9bebd0f77974f08`.

The dirty checkout was based on commit `8b73de89f8d145d31fe65ccf82876728d5c843f3`.
Its tracked binary diff had SHA-256
`44c37e7890b8c1daa34321df41e96b3166096d8881cb0fbe374de81ad7c16d6a`.
The benchmark source had SHA-256
`b52cb94b2b9299c728942b56f1bd3241b015ba0ac42cf561810e206b9bdd05d2`;
the untracked centroid stage and numerical source files had SHA-256
`90ef31c90fa496a237ef8b66bb46eac5575304a218faa163e7b67f46e59a7ef1`
and `e8d17cc9fb1e7c3c6bcb471e2aef2563786ffe782c6cf6838d7930abda30aeb9`.
The image ID identifies the packaged binary; this checkout is not a
reproducible commit.

Each run generated 5,000 deterministic vectors and 20 ANN query samples. The
preceding refresh workload deleted 500 rows, leaving 4,500 source rows. The
centroid comparison changed one vector: its small group contained 45 rows and
its large group contained 2,250. Refreshes ran in the fixed order group rescan,
algebraic, FULL, under `EXPLAIN ANALYZE`. Each cell is one client-observed
elapsed time in milliseconds, including settings, transaction, counter-query,
and EXPLAIN overhead.

| Dimensions | Group | Group rescan | Algebraic | FULL |
|---:|---|---:|---:|---:|
| 384 | small | 18.17 | 24.13 | 24.46 |
| 384 | large | 23.00 | 23.03 | 23.41 |
| 768 | small | 18.04 | 26.33 | 46.03 |
| 768 | large | 37.93 | 25.69 | 43.55 |
| 1536 | small | 21.97 | 30.64 | 81.46 |
| 1536 | large | 49.40 | 27.12 | 78.21 |

The same-transaction PostgreSQL source counters reported 4,500 sequential
tuples returned and zero index tuples fetched for every group-rescan and FULL
observation. Every algebraic observation reported zero for both counters. These
are tuple visits, including repeats, rather than distinct rows; the observed
rescan plan scanned the source even for the small group. The largest observed
component error against native AVG was zero in these samples. Logical
auxiliary-state payload estimates ranged from 0.93 to 3.70 MB across 100 small
groups and 18.7 to 74.0 kB across two large groups, excluding relation and TOAST
overhead. Algebraic refresh WAL ranged from 155.8 to 222.8 kB, versus 41.3 to
61.8 kB for group rescans. WAL is cluster-wide and excludes the source mutation.

The [384](384.json), [768](768.json), and [1536](1536.json) reports contain raw
observations and plans. These runs do not measure repeated-refresh distributions,
concurrent writers, source-write overhead, or rebase frequency; fixed execution
order, EXPLAIN and the shared host can bias the timings. An independently
validated cost policy and broader measurements remain necessary.
