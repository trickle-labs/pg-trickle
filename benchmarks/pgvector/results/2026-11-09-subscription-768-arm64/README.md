# 768-dimensional subscription smoke measurements

On an Apple M1 Pro using Linux ARM64 Docker, PostgreSQL 18.6 and pgvector
0.8.7, these two runs used the v0.109.0 candidate archive at commit
`1e291536bf0380b37a509b730155ca3d3a0ccea4`, SHA-256
`2cd9d52c1d9d744557e2f16a7faf1cb5c44213fdc72c72beebed37122b34f73b`.
The benchmark source was `e03f0f8f85e39f32ef8eae53cbf21c3e1a08df8b`.
Both installation observations match all 157 candidate payload files.

Each run exercised 10,000 rows with 768-dimensional vectors, one pivot,
0/1/10/100 identical subscriptions, analyzed and unanalyzed tables, and two
alternating one-row updates per case. The subscribed cases checked real
LISTEN notifications. Nested `auto_explain` was enabled. The forced-trigger
run preceded the trigger-free run; there was no randomized order or controlled
concurrent load. Two samples do not support percentile or general performance
claims, and timings include instrumentation overhead.

Mean refresh time in milliseconds across the two calls:

| Subscriptions | Forced, unanalyzed | Forced, analyzed | Trigger-free, unanalyzed | Trigger-free, analyzed |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 25.02 | 22.88 | 28.29 | 20.91 |
| 1 | 190.37 | 198.98 | 25.22 | 23.91 |
| 10 | 823.52 | 829.21 | 32.01 | 31.78 |
| 100 | 7,255.29 | 7,322.38 | 109.26 | 108.76 |

The unanalyzed forced-trigger 100-subscription case returned 1,020,000
sequential stream tuples on its first refresh, compared with 10,000 in the
forced zero-subscription case. The analyzed forced case returned 10,000
sequential tuples at both counts; this counter alone does not include other
access paths. The trigger-free zero-subscription case may use a different
merge strategy, so its timing is not a same-strategy control. The archive
predates the duplicate-predicate cache described in the
[384-dimensional report](../2026-11-09-candidate-subscription-arm64/README.md).

The full measurements, test logs, matching installation observations, and
representative first matching count and distance plans are retained here. The
full nested-plan logs remain local, with SHA-256
`8c26e04e02cd08e1b87de4a7bc4958ca3b763ce451d04ab26a7c65c2893e2f0a`
(forced) and `367e5473a93d46d80027be9dbbf2e404f2748e388b494c02bbe45901a70c4241`
(trigger-free). `extract_plans.py` extracts the retained plans from those logs.
These results do not qualify the final integrated package, larger workloads,
distinct pivots, or concurrent traffic.

Use the command in the linked 384-dimensional report with
`PGVECTOR_SUB_ROWS=10000` and `PGVECTOR_SUB_DIM=768` to reproduce these cases.
