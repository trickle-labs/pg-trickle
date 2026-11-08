# Centroid selector overhead, 2026-11-08

One sequential baseline/candidate comparison ran PostgreSQL 18.6, pgvector
0.8.7 and pg_trickle 0.110.0 in Linux arm64 containers on an Apple M1 Pro
with 32 GiB host memory. Host activity and container resources were not
controlled. The baseline image ID was
`sha256:c113860bed539cf451428e0008ea498842ab71c357faf626769c1ceab719775e`;
the candidate image ID was
`sha256:efe758a5bcadcf85d01eb72480276d013082ab4ca5bcb635373e7e0f3320fe1f`.
Neither image carries a source revision label. The candidate was packaged from
the source later committed as `90886370`; the baseline's exact source identity
was not independently verified.

Each run used 5,000 deterministic 384-dimensional vectors, one general
workload sample, and 20 centroid rounds changing two source rows each. Separate
stream tables used group rescans, algebraic state and FULL refresh; the group
and strategy order rotated. Every round checked affected groups against native
AVG. The policy did not switch strategies in this workload: the separate
streams did not supply comparable observations for both strategies. Thus this
measures overhead in this setup, not the benefit or cost of switching.

Median client-observed refresh time in milliseconds, excluding mutation and
validation:

| Group | Strategy | Baseline | Candidate |
|---|---|---:|---:|
| Small | Group rescan | 14.42 | 14.81 |
| Small | Algebraic | 21.94 | 22.64 |
| Small | FULL | 22.06 | 22.43 |
| Large | Group rescan | 25.61 | 26.09 |
| Large | Algebraic | 21.94 | 22.50 |
| Large | FULL | 21.18 | 21.79 |

These are single unpaired runs without warmup or controlled concurrency; the
differences cannot be attributed solely to the selector. They do not measure
switching, rebase cost, or a cold-start policy, and do not establish a general
threshold. Earlier intermittent smoke failures remain unexplained. Raw reports:
[baseline](baseline.json) and [candidate](candidate.json).

As an additional smoke check, the candidate image passed ten sequential fresh
container runs each at 100 and 1,000 initial rows, 384 dimensions, one ANN
sample, one centroid round and one rewritten row. Each run checked its refresh
strategy and native AVG result. The 1,000-row case covers the size of the
earlier preliminary failures, but those failures used an older development
image and their cause remains unknown; these passes do not establish that the
underlying cause was fixed.
