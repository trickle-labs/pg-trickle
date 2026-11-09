# Filtered pgvector workload

### Long-running centroid drift

The `centroid-drift` job in the manually dispatched E2E Benchmarks workflow runs 2,000 seeded
mutation and refresh calls against a development image with pgvector 0.8.7.
It compares every group to native `avg(vector)` after each call and records
the largest component error and fraction of the absolute-plus-relative
tolerance at 100-call checkpoints. The run includes updates, deletes, NULL
transitions and group moves; it uses three dimensions and does not qualify
large dimensions, concurrent workloads, crash recovery or release artifacts.
The uploaded results identify the image and its extension files.

To run it locally after building the version-specific image:

```sh
PGS_E2E_IMAGE=pg_trickle_e2e_pgvector:0.8.7 PGVECTOR_EXPECTED_VERSION=0.8.7 \
  ./scripts/run_e2e_tests.sh --retries 0 --test e2e_pgvector_tests \
  --run-ignored ignored-only --no-capture \
  -E 'test(test_centroid_long_running_drift_matches_native_avg)'
```

Use `test_centroid_high_dimensional_drift_matches_native_avg` in the same
command to run the 1,536-dimensional variant. It uses the same seed and a
small corpus, so it does not establish a general numerical operating envelope.

### Controlled distance-subscription measurements

The opt-in subscription workload accepts comma-separated `PGVECTOR_SUB_ROWS`
(1–1,000,000) and `PGVECTOR_SUB_COUNTS` (0–1,000), plus
`PGVECTOR_SUB_DIM` (2–1,536) and `PGVECTOR_SUB_ROUNDS` (1–100).
`PGVECTOR_SUB_PIVOTS` selects 1–1,000 distinct nearby pivot vectors (default 1);
all remain on the same side of each measured threshold transition.
`PGVECTOR_SUB_STATISTICS=both` runs separate unanalyzed and analyzed streams;
the harness disables autovacuum on each and rejects an unanalyzed control if
statistics were already collected. `PGVECTOR_SUB_FORCE_EXPLICIT=1` adds the
same no-op row trigger to every stream, including the zero-subscription
control, and requires the recorded merge path to be `explicit_dml` throughout.
Trigger overhead is part of every control measurement, so these timings do not
represent trigger-free production workloads. User triggers also disable the
bounded distance snapshot, so subscribed cases take a full stream snapshot and
scan the stream during evaluation. The report records the observed snapshot
kind and row count; run without the trigger to investigate the bounded path.

For a bounded control run on a packaged candidate image:

```sh
PGS_E2E_IMAGE=pg_trickle_e2e_pgvector:0.8.7 \
PGVECTOR_SUB_ROWS=1000,10000 PGVECTOR_SUB_COUNTS=0,1,10 \
PGVECTOR_SUB_DIM=384 PGVECTOR_SUB_STATISTICS=both PGVECTOR_SUB_FORCE_EXPLICIT=1 \
PGVECTOR_SUB_PLAN_LOG=subscription-postgres.log \
  ./scripts/run_e2e_tests.sh --retries 0 --test e2e_bench_tests \
  --run-ignored ignored-only --no-capture \
  -E 'test(test_pgvector_subscription_scaling_workload)'
```

The report separates refresh, commit, and post-commit listener wait; listener
wait is not commit-to-delivery latency because delivery may begin earlier.
When `PGVECTOR_SUB_PLAN_LOG` is set, PostgreSQL `auto_explain` records nested
JSON plans with actual rows and buffers. START and END log markers identify each
case and refresh. The file combines Docker stdout and stderr; use timestamps
to interpret their ordering. Capture is also attempted when a test panics.
Instrumentation adds overhead, so compare timings separately from uninstrumented
runs. The scheduled job records its image ID and pg_trickle file hashes; these
identify the development image, not a published release artifact. The harness
does not yet measure controlled concurrent load, WAL, or CPU, and a run alone
does not establish a general scaling limit. Larger cases remain opt-in.

The [v0.109.0 candidate measurements](results/2026-11-09-candidate-subscription-arm64/README.md)
compare the forced-trigger and bounded paths through 100,000 rows and 100
subscriptions, with representative nested plans and explicit limitations.

## Version conformance

The PR compatibility workflow packages pg_trickle for PostgreSQL 18.6 and
tests pgvector 0.8.1 and 0.8.7 from digest-pinned upstream images. The first
is a historical PostgreSQL 18 compatibility target, not a deployment
recommendation; see the [pgvector changelog](https://github.com/pgvector/pgvector/blob/master/CHANGELOG.md)
for fixes in later releases. The selected tests assert the installed versions
and exercise a bounded set of helper, index, aggregate, security, and example
paths. They do not prove every pgvector capability.

To build a matching image locally, run `just build-e2e-image` followed by
`./tests/build_e2e_pgvector_image.sh 0.8.7`. Set
`PGS_E2E_IMAGE=pg_trickle_e2e_pgvector:0.8.7` and
`PGVECTOR_EXPECTED_VERSION=0.8.7` when running the cases in
`.github/workflows/pgvector-conformance.yml`.

## Workload measurements

This opt-in database benchmark runs against the packaged extension in a
Testcontainers PostgreSQL instance. It emits one `PGVECTOR_BENCHMARK_JSON=`
line containing raw observations; it does not impose universal latency or
recall thresholds. The scheduler is paused for each benchmark stream so the
workload controls its refreshes. The weekly E2E Benchmarks workflow runs dimensions 384,
768 and 1536 and uploads the JSON, complete output, commit, host CPU and Docker
version as artifacts.

```sh
just build-e2e-image
PGVECTOR_BENCH_DIM=384 PGVECTOR_BENCH_ROWS=1000 PGVECTOR_BENCH_SAMPLES=20 \
  ./scripts/run_e2e_tests.sh --retries 0 --test e2e_bench_tests \
    --run-ignored ignored-only --no-capture \
    -E 'test(test_pgvector_filtered_ann_workload)' | tee pgvector-bench.txt
```

Coordinates derive deterministically from `md5(id:dimension)`; ten evenly
sized tenants share the corpus. For HNSW and IVFFlat (10 lists), the test
measures sequential client-observed cosine top-10 queries on one tenant and
compares returned IDs to an exact scan with the same vector, filter and metric.
It forces ANN index eligibility and checks its `EXPLAIN ANALYZE` plan. It also
requires a sequential scan of the corpus for the exact baseline and records
both plans, buffer counts, index/corpus sizes, individual recalls and
latencies, and nearest-rank p50/p95/p99. The report includes server and
extension versions and the actual HNSW/IVFFlat search settings. ANN construction
can use randomness despite deterministic input; do not infer production
latency or recall from one run or treat forced eligibility as the default plan.

The refresh section measures a one-row vector update, a 10% vector update,
and a 10% delete on separate differential and full-refresh stream tables with
equivalent HNSW indexes. Both must match their complete defining query and the
recorded history must confirm the intended strategy. The differential refresh
always runs first, so shared cache and other ordering effects can bias the
comparison. WAL is measured between cluster-wide LSN reads, excludes the source
mutation and can include unrelated activity; refresh latencies include client
overhead. A plan is recorded for retrieval, not for the refresh internals.
The centroid baseline additionally compares one changed vector in roughly 100
small groups and two large groups across group-rescan, opt-in algebraic, and
FULL refresh. It records the selected strategy, affected group size, refresh
latency, cluster-wide WAL, stream storage, estimated logical auxiliary-state
bytes, and maximum component error against native AVG. Each refresh runs under
`EXPLAIN (ANALYZE, BUFFERS, WAL)`; its top-level counters include the call but
do not necessarily expose plans for statements executed through SPI. A separate
affected-group query illustrates a possible rescan. Same-transaction PostgreSQL
tuple counters record sequential returns and index fetches from the source during
each refresh, including repeated visits; they are not counts of distinct rows.
This is one observation per
strategy in the order listed; EXPLAIN overhead and cache order affect timings.
The workload also performs five repeated one-row changes by default, rotating
the group and strategy order. Set `PGVECTOR_CENTROID_REWRITE_ROWS` to update
more rows per round, up to the surviving source rows less two fixed inputs.
It checks each selected strategy and all affected groups against native AVG
with an absolute bound of `1e-5` plus a relative bound of `1e-6`, rejects
missing components, and records each round's maximum absolute component error,
refresh time and percentiles without EXPLAIN. Set
`PGVECTOR_CENTROID_SAMPLES` from 1 to 50 to change the number of rounds; a
small sample cannot establish tail latency. The algebraic measurement asserts
that every round uses algebraic state, so batches that trigger a rebase fail
the benchmark rather than being silently counted as algebraic. The workload does not
measure source-write overhead or concurrent load. Use a dedicated host and
more repetitions before drawing performance conclusions.

The scheduled run also measures distance subscriptions separately with 1,000
and 10,000 rows and zero, one, or ten subscriptions. Five one-row changes per
case alternate across the threshold using 384-dimensional vectors. It runs
analyzed and unanalyzed controls, forces the same explicit-DML path with a
no-op row trigger, verifies the affected-row snapshot and differential strategy,
and records refresh, commit, listener-wait and same-transaction source and
stream tuple counters. The counters include repeated visits and are not distinct
row counts. A listener verifies each event after commit. The
[local report](results/2026-11-08-subscription-scaling-arm64/README.md) also
records an unanalysed control and its limitations.

A [recorded emulated run](results/2026-11-07-emulated/README.md) includes the
raw JSON for all three dimensions and documents its limitations.
The later [local centroid comparison](results/2026-11-07-centroid-arm64/README.md)
includes the experimental algebraic path and its raw observations.
The [source-read comparison](results/2026-11-08-source-reads-arm64/README.md)
adds same-transaction source tuple counters and larger affected groups.
The [repeated centroid comparison](results/2026-11-08-repeated-centroid-arm64/README.md)
records 20 rotated-order refreshes per group and strategy at each dimension.
The [rewrite comparison](results/2026-11-08-rewrite-centroid-arm64/README.md)
repeats 200-row updates and checks every affected group.
The [scheduler-isolated rewrite run](results/2026-11-08-paused-rewrite-arm64/README.md)
repeats that 384-dimensional workload with manually controlled refreshes.
