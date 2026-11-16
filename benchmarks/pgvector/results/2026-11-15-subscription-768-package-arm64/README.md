# 768-dimensional subscription matrix on a newer package

On 2026-11-15, PostgreSQL 18.6 with pgvector 0.8.7 ran on Linux ARM64 Docker
on an Apple M1 Pro. The test source was integrated commit
`1e1435649baf38596dc7054e3cd17be1499869b0`. The installed v0.109.0 archive
came from [qualification run 43710039862](https://github.com/trickle-labs/pg-trickle/actions/runs/43710039862),
commit `7e7b51c2a7f31cdf24d06408cd815e4b6b147a77`, artifact `14161540423`,
SHA-256 `90d121bfa63fb30197ce535a78be93367b0f8e72dad2ae52d06b0b9a9e94591b`.
The image ID was
`sha256:f1a28666a96308630d613d012b3889628ab8f295d7e2f0a436c000011ba31332`.
Both [forced](attest-forced.json.gz) and [bounded](attest-bounded.json.gz)
installation observations matched all 157 candidate and installed files and
payload digests at startup. This is not the final integrated package.

Each run used 10,000 rows, 768-dimensional vectors, 0/1/10/100/1,000
subscriptions and up to 1,000 distinct nearby pivots. For analyzed and
unanalyzed tables, two one-row changes crossed the threshold. Every expected
LISTEN notification was received without duplicates; both tests passed. The
forced case used a no-op user trigger and `explicit_dml` for every subscription
count. The subsequent trigger-free case used bounded snapshots with
subscriptions; its zero-subscription control could use another merge path.
Nested `auto_explain` and buffer instrumentation were enabled.

Mean refresh milliseconds over two calls:

| Subscriptions | Forced, unanalyzed | Forced, analyzed | Bounded, unanalyzed | Bounded, analyzed |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 25.19 | 20.48 | 30.38 | 20.09 |
| 1 | 182.03 | 177.59 | 26.17 | 23.29 |
| 10 | 477.09 | 474.10 | 33.40 | 31.16 |
| 100 | 540.37 | 544.41 | 115.39 | 106.10 |
| 1,000 | 1,245.87 | 1,225.56 | 961.60 | 971.02 |

For 1,000 subscriptions, the first forced refresh returned 30,000 sequential
stream tuples without ANALYZE and 20,000 with ANALYZE; the forced zero controls
returned 10,000 and zero. These are repeated tuple visits, not distinct rows,
and omit work through other paths. A retained [forced predicate plan](forced-predicate-plan.json)
reads compacted old/new temporary rows; a retained [bounded predicate plan](bounded-predicate-plan.json)
uses an identity-index probe. [Forced](forced-stream-plans.json) and
[bounded](bounded-stream-plans.json) stream-plan excerpts are also retained.

The complete [forced](forced.json) and [bounded](bounded.json) measurements
include individual refresh, commit and post-commit listener-wait times,
strategies and tuple counters; the [forced test log](forced-test.log.gz) and
[bounded test log](bounded-test.log.gz) retain their output. Container log
retention truncated older nested plans during these high-volume cases: the
captured tails retain only one start marker each, so they cannot represent every
case. Their local SHA-256 values are
`28e060640b74c3fcc671a120f30d7196c358d414ec707a8c92c3ab56b468c4a8`
and `35dc9caccffa48d353cbcb0bab398ba80780dbcd65fba66534139e6f45bf791a`.
The plan excerpts are examples from the retained tails, not a complete matrix.

Runs were serial and forced preceded bounded. There was no controlled
concurrent traffic, instrumentation adds overhead, and two samples do not
support percentiles or a general capacity limit. The earlier
[distinct-pivot investigation](../2026-11-09-subscription-distinct-1000-arm64/README.md)
used a different development image; comparing timings across the runs does not
by itself establish a controlled speedup.
