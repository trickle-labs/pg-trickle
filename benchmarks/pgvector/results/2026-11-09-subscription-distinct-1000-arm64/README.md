# Distinct subscription pivots at 768 dimensions

These opt-in measurements used PostgreSQL 18.6, pgvector 0.8.7 and
pg_trickle 0.109.0 on Linux ARM64 Docker on an Apple M1 Pro. The benchmark
source was `0dfe6873c44582383c790394f7f84f7bc9c27586`. The development image
was `sha256:0fa00f6d9c9951be4ea6e975c86993c1e129a7718ac6365bd4bf5d09383f08f0`,
with pg_trickle library SHA-256
`564930aa0cec232ea21a5b6acd084a467c92cdae2bde7946443d35b57f0bd8ee`.
This is the image containing the duplicate-predicate optimization described in
the [384-dimensional report](../2026-11-09-candidate-subscription-arm64/README.md),
not a final candidate release archive. Docker resource limits were not set.

Each case used 10,000 rows, 768-dimensional vectors and 0/1/10/100/1,000
subscriptions with up to 1,000 distinct nearby pivots. Two one-row updates
crossed the same threshold in each case. Every subscribed channel received its
expected real LISTEN event, with no duplicates; both tests passed. The forced
run used a no-op row trigger and `explicit_dml` in every case. The subsequent
trigger-free run used bounded snapshots for subscribed cases; its zero-control
used `delete_insert`, so it is not a same-strategy timing control.

Mean refresh time in milliseconds over two calls, with nested `auto_explain`
and buffer instrumentation enabled:

| Subscriptions | Forced, unanalyzed | Forced, analyzed | Bounded, unanalyzed | Bounded, analyzed |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 26.18 | 19.97 | 27.02 | 20.40 |
| 1 | 184.75 | 183.64 | 24.32 | 23.42 |
| 10 | 832.24 | 802.18 | 29.81 | 28.95 |
| 100 | 6,884.95 | 6,934.28 | 102.20 | 100.87 |
| 1,000 | 66,771.34 | 72,949.84 | 932.04 | 934.79 |

At 1,000 subscriptions, each forced unanalyzed refresh returned 10,020,000
sequential stream tuples. The forced analyzed case returned 10,000 sequential
tuples; that counter does not include all other plan work. The bounded case
captured one affected identity and one old row. Its unanalyzed stream counter
returned 10,000 sequential tuples and its analyzed counter returned zero.
At 1,000 subscriptions, listener-drain times after commit ranged from 8.37 to
20.68 ms in the forced run and 8.75 to 21.36 ms in the bounded run; delivery
may already have begun before that measurement. Full per-call measurements,
including commit and delivery observations, are in [forced.json](forced.json)
and [bounded.json](bounded.json).

The [forced](forced-plans.json) and [bounded](bounded-plans.json) files retain
representative analyzed 1,000-subscription count and distance plans. Complete
nested logs remain local: forced SHA-256
`2c4c5920fb099690c39110f85f853a6e84a0440138ad5a088affb4ee775878fc`, bounded
SHA-256 `c83b78f94fd9a1766087fede5efdf87fd24b479b57367a39f343d7a37f61a46c`.
[extract_plans.py](extract_plans.py) extracts the retained examples.

The forced run preceded the bounded run without randomized ordering; briefly
compressing the first log overlapped the second run. There was no controlled
concurrent workload, and instrumentation adds overhead. Two samples do not
support percentiles or a general capacity limit. The full-snapshot cost with
distinct predicates remains a measured optimization target; this evidence does
not justify weakening trigger or notification semantics.

Reproduce with the command in the linked 384-dimensional report, setting
`PGS_E2E_IMAGE=pg_trickle_e2e_pgvector:issue1399-cache-087`,
`PGVECTOR_SUB_ROWS=10000`, `PGVECTOR_SUB_COUNTS=0,1,10,100,1000`,
`PGVECTOR_SUB_DIM=768`, `PGVECTOR_SUB_ROUNDS=2`, `PGVECTOR_SUB_PIVOTS=1000`,
`PGVECTOR_SUB_STATISTICS=both` and running once each with
`PGVECTOR_SUB_FORCE_EXPLICIT=1` and `0`. The [forced](forced-test.log) and
[bounded](bounded-test.log) test logs include the raw measurements.
