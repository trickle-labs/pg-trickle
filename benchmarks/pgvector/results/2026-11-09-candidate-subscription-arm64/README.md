# Candidate subscription scaling and scan attribution

On 2026-11-09, two instrumented runs passed against the v0.109.0 ARM64 archive
from [qualification run 42600697708](https://github.com/trickle-labs/pg-trickle/actions/runs/42600697708),
commit `1e291536bf0380b37a509b730155ca3d3a0ccea4`, archive SHA-256
`2cd9d52c1d9d744557e2f16a7faf1cb5c44213fdc72c72beebed37122b34f73b`.
The test used the verified pgvector 0.8.7 candidate image
`sha256:f1a28666a96308630d613d012b3889628ab8f295d7e2f0a436c000011ba31332`;
see the [archive installation evidence](../2026-11-09-integrated-package-arm64/README.md).
The benchmark source was integrated commit `018039a9f9e6a82c7db69fd1915ebd37c1235d89`.
The host was an Apple M1 Pro running Linux ARM64 Docker, PostgreSQL 18.6 and
pgvector 0.8.7; Docker resource allocations were not recorded.

Each run measured 1,000, 10,000 and 100,000 rows, 0, 1, 10 and 100
subscriptions, 384 dimensions, analyzed and unanalyzed tables, and two
alternating one-row updates per case. Each subscribed refresh produced the
expected real LISTEN notifications. The [forced-trigger samples](forced.json)
and [trigger-free samples](bounded.json) preserve the complete measurements.
Nested `auto_explain` instrumentation was enabled throughout; timings include
its overhead. The forced run preceded the trigger-free run. There was no
controlled concurrent load, randomized run order, CPU or WAL measurement, and
two samples are insufficient for percentiles or general performance claims.

At 100,000 rows, mean refresh times in milliseconds for the two samples were:

| Subscriptions | Forced trigger, unanalyzed | Forced trigger, analyzed | Trigger-free, unanalyzed | Trigger-free, analyzed |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 46.56 | 20.72 | 48.61 | 20.21 |
| 1 | 490.37 | 549.71 | 49.51 | 23.34 |
| 10 | 2,513.41 | 3,498.14 | 97.28 | 30.80 |
| 100 | 22,427.58 | 31,619.24 | 194.06 | 101.93 |

The no-op row trigger forces `explicit_dml` in every forced control and causes
a full pre-refresh stream snapshot when subscriptions exist. With no
subscriptions, the unanalyzed case still returned 100,000 sequential stream
tuples. Its [captured plan](forced-plans.json) identifies a fallback `COUNT(*)`
used to estimate stream size; with analyzed statistics this subplan did not
execute. Thus a whole-stream visit can occur without subscription evaluation.

The forced 100-subscription case exposes a separate real cost: its first
round's [captured plans](forced-plans.json) show a full snapshot and an
evaluation query over both the snapshot and the current 100,000-row stream.
The unanalyzed transaction returned 10,200,000 sequential stream tuples per
refresh. The analyzed plan instead used a stream index scan for the evaluation;
sequential tuple counters alone therefore understate its visits. This expensive
trigger path and a safe targeted remediation remain open; the measurements do
not justify weakening trigger or event semantics.

Without the trigger, subscribed cases used a bounded one-row identity snapshot;
their [plans](bounded-plans.json) show index probes for the affected row. The
zero-subscription control used `delete_insert`, whereas subscribed cases used
`explicit_dml`, so the trigger-free zero control is not a same-strategy timing
comparison. These runs do not establish an operating limit for 1,000
subscriptions, one million rows, multiple pivots, concurrent traffic or other
query shapes.

To reproduce, build the candidate test image as described in the linked
archive evidence, then run the following twice, setting
`PGVECTOR_SUB_FORCE_EXPLICIT` to `1` and `0` respectively:

```sh
PGS_E2E_IMAGE=pg_trickle_e2e_pgvector:qualification-1e-087 \
PGVECTOR_EXPECTED_VERSION=0.8.7 PGVECTOR_SUB_ROWS=1000,10000,100000 \
PGVECTOR_SUB_COUNTS=0,1,10,100 PGVECTOR_SUB_DIM=384 PGVECTOR_SUB_ROUNDS=2 \
PGVECTOR_SUB_STATISTICS=both PGVECTOR_SUB_FORCE_EXPLICIT=1 \
PGVECTOR_SUB_PLAN_LOG=subscription-postgres.log \
  ./scripts/run_e2e_tests.sh --retries 0 --test e2e_bench_tests \
  --run-ignored ignored-only --no-capture \
  -E 'test(test_pgvector_subscription_scaling_workload)'
```

The JSON plan files retain representative first-round plans for 100,000-row,
zero- and 100-subscription cases in both statistics states; they are excerpts,
not the full nested-plan logs. The full logs were retained locally, with
SHA-256 `59fd7f9636229380e1fcda2bf4ac9db14d4f384be22f9889f8f7bfdf9365e982`
(forced) and `acb0803811fb5dd3b4725cac84cb7accc2adc18327995dc3ac89cb7073e82bad`
(trigger-free).
