# Shared changed-row evaluation for distinct subscriptions

For a full distance snapshot with multiple distinct predicates on a pgvector
column, the change compares old and new binary vector values once. Identical
row-identity/value multiplicities cancel; all rows for changed identities are
retained for the existing per-subscription membership calculation. The full
pre-refresh snapshot remains in place, including user-trigger side effects.
Unsupported types and errors preparing the comparison fall back to the original
full evaluation. A single predicate and the bounded-delta path keep their
existing behavior.

The [before measurements](../2026-11-09-subscription-distinct-1000-arm64/README.md)
used 10,000 rows, 768 dimensions, up to 1,000 distinct nearby pivots, two
one-row crossing updates, analyzed and unanalyzed statistics, a user trigger,
and nested `auto_explain`. The after run used the same parameters and verified
the same real LISTEN notifications. It used a locally rebuilt development
library (SHA-256 `9f01fc174a5f6f872483434fe7ff57dbc2b595d4fea85ea0a414ca67ba5ac104`)
and matching installation SQL (SHA-256
`97315459236ad1f18d08db64a802dfd7ead282fcab9d88db797103ba7311186d`), on
PostgreSQL 18.6 and pgvector 0.8.7, Linux ARM64 Docker on the same Apple M1
Pro. The image ID was
`sha256:7250b7a328410d116cbe26d42103133ed069a54c2a8db74249ebe00fa43e0fe9`.
The source was a working-tree change based on `b69eb0a6`; its `src/api/mod.rs`
diff SHA-256 was `7829390e7d18c732a5b7a879158f28fff22fad190ea4514a30768a3241ff90b5`.
This is not an attested final release artifact.

Mean refresh milliseconds over two calls:

| Subscriptions | Before, unanalyzed | After, unanalyzed | Before, analyzed | After, analyzed |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 26.18 | 24.77 | 19.97 | 19.06 |
| 1 | 184.75 | 178.24 | 183.64 | 170.34 |
| 10 | 832.24 | 452.80 | 802.18 | 464.42 |
| 100 | 6,884.95 | 542.12 | 6,934.28 | 520.15 |
| 1,000 | 66,771.34 | 1,208.39 | 72,949.84 | 1,206.79 |

For 1,000 subscriptions, each unanalyzed refresh returned 10,020,000 stream
sequential tuples before and 30,000 after. The analyzed after case returned
20,000; sequential counters alone omit other access paths. The [after plans](final-plans.json)
include one shared binary comparison and a predicate reading compacted rows.
The [full measurements](final.json) and [test log](final-test.log) retain
per-call refresh, commit and listener-drain observations. The full nested log
remains local with SHA-256
`5839887b6d42de5e73234c6996ecaae7c3bcb343389666b3cd7466a47d090c7f`;
[extract_final_plans.py](extract_final_plans.py) extracts the representative
plans.

An additional [100,000-row run](final2-100k.json) used 384 dimensions and up
to 100 distinct pivots with the same forced-trigger and instrumentation
settings. At 100 subscriptions, the earlier
[mixed-pivot run](../2026-11-09-candidate-subscription-arm64/mixed-pivots.json)
averaged 24.65 s unanalyzed and 32.33 s analyzed; the new run averaged 2.70 s
and 2.57 s respectively. Unanalyzed sequential stream tuple returns fell from
10,200,000 to 300,000 per refresh. The earlier run overlapped an image build,
so this is not a controlled speedup estimate. The new [test log](final2-100k-test.log)
contains all samples and listener checks; its full nested log remains local
with SHA-256 `7373266df8260d1bb840455a7c1e83d8b16949702bd867bc546db557a3e494f4`.

The runs were ordered, not randomized. Nested instrumentation adds overhead;
background host activity was not controlled. Two samples do not establish tail
latency or a production capacity limit. This workload uses
nearby pivots and one-row changes; broad rewrites, other dimensions and final
candidate qualification remain necessary.

The complete distance-subscription E2E selection passed 21 tests with one
larger test ignored on both [pgvector 0.8.1](final-suite-081.log) and
[0.8.7](final-suite-087.log). Coverage includes an unrelated row modified by
a user trigger, duplicate and distinct predicates, NULL and row-identity
transitions, FULL refresh, typed vectors, a non-superuser stream owner, and
fallback when scratch-table preparation is blocked. A new concurrent-writer
and rollback test is included in both logs and required in both release suites.
The 2,707-test Rust unit suite, formatting, lint and release-contract checks
also passed. These checks do not replace exact final-package qualification.
