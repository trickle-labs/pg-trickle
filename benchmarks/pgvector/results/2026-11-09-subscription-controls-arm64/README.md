# Subscription scan controls, 2026-11-09

This is a diagnostic run, not release qualification or a scaling limit. It
uses one measured one-row update per case, with 384-dimensional vectors,
1,000 or 10,000 stream rows, 0, 1 or 10 subscriptions, and separate
unanalyzed and analyzed streams. Both runs use `auto_explain` with nested
plans, actual rows and buffers; instrumentation affects timing. The scheduler
is paused. Each subscription uses the same pivot and threshold.

The development image was built from the extension sources at commit
`1f1517ba77dcc8d7d47dccccba0d936e644db34a`, with PostgreSQL 18.6, pgvector 0.8.7 and
pg_trickle 0.109.0. Its Docker image ID was
`sha256:36731f289865bbd35c7f591ebad9641c6546dd91a2be2693a88ab5e6ff12bab9`;
the extension library SHA-256 was
`0b236a96c80eaae16e855f5133d993481c77cd0acecef0d1de98d0fba4696f86`.
The host was an Apple M1 Pro with 32 GiB RAM, running a Linux arm64 Docker
container. This image is not the final release artifact.

`control.json` records the common `explicit_dml` control with a no-op user
trigger on every stream. `trigger-free.json` records the same cases without
that trigger; its zero-subscription cases used `delete_insert`, while its
subscribed cases used `explicit_dml`. The corresponding `*-plans.json` files
retain the complete captured JSON plans for queries mentioning the measured
stream or distance-snapshot tables, tagged with their case. Other nested plans
and the original server log are not included here.

- With no subscriptions and the same explicit merge path, the unanalyzed
  1,000- and 10,000-row controls returned 1,000 and 10,000 stream tuples;
  the analyzed controls returned zero. The captured plan for the refresh
  row-count estimate shows that when `reltuples < 1`, its `COUNT(*)` fallback
  performs a sequential scan of the stream. This is refresh work and occurs
  without subscriptions. The same fallback appears in the trigger-free run.
- In the trigger-free subscribed cases, the measured distance snapshot and
  affected-identity table each held one row. In the triggered subscribed
  cases, the snapshot held the entire stream: user triggers disable the
  bounded path. For unanalyzed triggered cases, sequential tuple returns were
  3,000/12,000 at 1,000 rows and 30,000/120,000 at 10,000 rows for 1/10
  subscriptions. The plans show both the full pre-refresh snapshot and
  subscription evaluation scanning the stream. Analyzed plans differ, so
  these observations should not be generalized to every plan or workload.

The controls explain the corresponding scans in this instrumented
reproduction; they do not prove that every visit in the earlier exploratory
report had the same cause. They also expose a real full-snapshot cost when
user triggers are present. Before changing that safety boundary, compare a
targeted remedy with the current behavior and verify trigger and notification
semantics.

`scale-100k.json` records a separate uninstrumented, analyzed, trigger-free
run at 100,000 rows and 0, 1, 10 and 100 subscriptions. The subscribed cases
used one-row bounded snapshots and delivered the expected transition to every
listener. Each case has only one sample and the zero-subscription case used a
different merge strategy; its timings are not a controlled overhead estimate.

The larger scale matrix, repeated timings, concurrent load,
restart/retry behavior, and final candidate-artifact evidence remain open.
