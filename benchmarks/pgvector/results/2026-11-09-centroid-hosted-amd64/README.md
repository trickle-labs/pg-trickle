# Hosted centroid comparison, 2026-11-09

These are development-image measurements from the successful [hosted
pgvector benchmark run 42588168262](https://github.com/trickle-labs/pg-trickle/actions/runs/42588168262),
not final v0.109.0 artifact qualification. The run used commit
`1f1517ba77dcc8d7d47dccccba0d936e644db34a`, PostgreSQL 18.6, pgvector 0.8.7
and pg_trickle 0.109.0. Each dimension ran in a separate Linux amd64 runner;
the full benchmark reports, host details, image IDs and package-file hashes
are alongside this file. The extension library hash was identical across the
three builds, while the generated installation SQL hashes differed. These
independently built images are not proof of a single identical release payload.

The benchmark starts with 1,000 corpus rows and tests one-row changes. In its
centroid phase the affected small and large groups held 9 and 450 rows. It
performs five measured refresh calls per strategy and group, rotating order,
and checks each resulting centroid against native AVG. The table shows the
observed median refresh-call milliseconds; it is a small smoke comparison,
not a latency guarantee or a comparison across the separate runners.

| Dimensions | Group rows | Group rescan | Algebraic | FULL |
|---:|---:|---:|---:|---:|
| 384 | 9 | 22.46 | 38.10 | 19.76 |
| 384 | 450 | 24.70 | 38.06 | 18.85 |
| 768 | 9 | 12.30 | 21.25 | 13.67 |
| 768 | 450 | 13.85 | 21.43 | 12.68 |
| 1536 | 9 | 14.34 | 26.28 | 20.34 |
| 1536 | 450 | 18.40 | 27.16 | 17.67 |

In these specific cases algebraic maintenance was slower than group rescan;
FULL was sometimes fastest. The one-row baseline comparisons reported zero
maximum absolute component error at output precision. Five samples, warm
cache and a small dataset are insufficient to calibrate the selector or infer
general crossover points. The run does not exercise concurrent writers,
recovery, cold-cache repetitions, skewed large groups or rebase frequency.
The experimental centroid state remains opt-in.
