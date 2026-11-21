# One-million-row subscription refresh samples

These runs exercise the subscription scaling benchmark in
`tests/e2e/pgvector_bench_workload.rs` with a one-million-row stream and
384- and 768-dimensional vectors. This is the **1M source-row subscription
case**; it is not the RAG benchmark. The RAG corpus remains capped at 100,000
documents.

The test used PostgreSQL 18.7, pgvector 0.8.7, and pg_trickle 0.109.0 in the
Docker image `sha256:3fa654fab411194eb6b9102eecac0ceb115030d5199fcb66237e2db1ee4ee3b8`.
The runtime image was built from integrated candidate `cfb965df`. The 384D
harness came from `fafc6af9`; the 768D harness came from `9fd80566`. The
`src/` tree is identical across those commits. The host was Apple Silicon with
32 GiB RAM. Docker Desktop 29.9.2 provided 10 CPUs and 8,319,504,384 bytes of
memory to the `linux/amd64` test container. Both dimensions completed within
that allocation.

Each case used an analyzed stream, two alternating one-row updates, and nested
`auto_explain` capture. At both dimensions the trigger-free cases covered 0, 1,
10, 100, and 1,000 subscriptions; the 1,000-subscription case used 10 distinct
pivots. The 384D forced-explicit controls added a no-op row trigger to compare 0
and 1 subscription. The test asserted differential refresh, propagated the
changed row, and checked expected notifications. Times include plan
instrumentation. Raw samples and exact plan-log checksums are in
[measurements.json](measurements.json) and
[measurements-768.json](measurements-768.json); complete nested plan logs are
compressed under [plans/](plans/).

| Trigger path | Subscriptions | Refresh ms (rounds 1, 2) | Commit ms (rounds 1, 2) | Snapshot | Stream sequential tuples returned (rounds 1, 2) |
| --- | ---: | ---: | ---: | --- | ---: |
| Trigger-free | 0 | 567.113, 95.106 | 5.970, 2.520 | None | 0, 0 |
| Trigger-free | 1 | 576.889, 105.470 | 6.408, 4.310 | Bounded, 1 row / 1 identity | 0, 0 |
| Trigger-free | 1,000 | 1,254.591, 866.062 | 6.844, 5.318 | Bounded, 1 row / 1 identity | 0, 0 |
| Forced explicit-DML | 0 | 623.085, 165.441 | 7.030, 4.709 | None | 0, 0 |
| Forced explicit-DML | 1 | 10,386.820, 6,628.518 | 124.024, 121.399 | Full, 1,000,000 rows | 1,000,000, 1,000,000 |

| Dimension | Trigger path | Subscriptions | Refresh ms (rounds 1, 2) | Commit ms (rounds 1, 2) | Notification wait ms | Snapshot | Stream sequential tuples returned |
| ---: | --- | ---: | ---: | ---: | ---: | --- | ---: |
| 768 | Trigger-free | 0 | 263.981, 96.066 | 6.300, 2.305 | — | None | 0, 0 |
| 768 | Trigger-free | 1 | 240.506, 88.880 | 6.930, 2.899 | 0.139, 0.134 | Bounded, 1 row / 1 identity | 0, 0 |
| 768 | Trigger-free | 10 | 103.252, 166.249 | 3.459, 5.080 | 0.705, 0.443 | Bounded, 1 row / 1 identity | 0, 0 |
| 768 | Trigger-free | 100 | 269.053, 167.759 | 6.031, 2.762 | 4.930, 3.653 | Bounded, 1 row / 1 identity | 0, 0 |
| 768 | Trigger-free | 1,000 | 965.949, 950.739 | 6.980, 3.769 | 8.712, 13.769 | Bounded, 1 row / 1 identity | 0, 0 |

On this workload, trigger-free subscribed refreshes retained a one-row bounded
snapshot through 1,000 subscriptions at both dimensions. The forced
no-op-trigger 384D case with one subscription took a full snapshot and returned
one million stream tuples per round. These are two instrumented samples per
case, not general performance estimates. No controlled concurrent load,
randomized run order, CPU or WAL collection, percentile estimate, or reliable
peak-memory measurement was made. These runs do not qualify 1536-dimensional
vectors or other workloads.

Nested plan evidence shows one membership-predicate plan per round at 1, 10,
and 100 subscriptions with one pivot, and ten plans per round at 1,000
subscriptions with ten pivots. Those trigger-free plans reported no shared
reads; their root shared-hit counters were 5/4 blocks per round for the 384D
one-pivot case, 50/40 for its ten-pivot case, 9/8 for the corresponding 768D
one-pivot cases, and 90/80 for 768D with ten pivots. The forced no-op-trigger
384D case with one subscription reported 203,830 and 203,829 shared-read
blocks across its two membership plans, consistent with its full snapshot.
These are sums of `auto_explain` plan-root buffer counters, not unique pages or
total workload I/O; see the JSON files and compressed logs for raw evidence.

To reproduce the trigger-free 0/1 subscription cases, run the ignored test with
`PGVECTOR_SUB_ROWS=1000000`, `PGVECTOR_SUB_COUNTS=0,1`,
`PGVECTOR_SUB_DIM=384`, `PGVECTOR_SUB_ROUNDS=2`,
`PGVECTOR_SUB_STATISTICS=analyzed`, `PGVECTOR_SUB_FORCE_EXPLICIT=0`, and
`PGVECTOR_SUB_PLAN_LOG` set to a writable path. For the 1,000-subscription case,
use `PGVECTOR_SUB_COUNTS=1000` and `PGVECTOR_SUB_PIVOTS=10`. For the forced
explicit-DML controls, use `PGVECTOR_SUB_COUNTS=0,1` and
`PGVECTOR_SUB_FORCE_EXPLICIT=1`. Run each command separately because each test
creates its own one-million-row fixture.

For the 768D matrix, use `PGVECTOR_SUB_DIM=768` and separately run each of
`PGVECTOR_SUB_COUNTS=0`, `1`, `10`, `100`, and `1000` with
`PGVECTOR_SUB_FORCE_EXPLICIT=0`. For the 1,000-subscription case also set
`PGVECTOR_SUB_PIVOTS=10`. A fresh test container per count avoids retaining
multiple million-row fixtures at once.
