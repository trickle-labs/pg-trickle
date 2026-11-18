# One-million-row subscription refresh samples

These runs exercise the subscription scaling benchmark in
`tests/e2e/pgvector_bench_workload.rs` with a one-million-row stream and
384-dimensional vectors. This is the **1M source-row subscription case**; it is
not the RAG benchmark. The RAG corpus remains capped at 100,000 documents.

The test used PostgreSQL 18.7, pgvector 0.8.7, and pg_trickle 0.109.0 in the
Docker image `sha256:3fa654fab411194eb6b9102eecac0ceb115030d5199fcb66237e2db1ee4ee3b8`.
The runtime image was built from integrated candidate `cfb965df`; the benchmark
harness came from `fafc6af9`. Later changes between those commits did not alter
the pg_trickle library. The host was Apple Silicon with 32 GiB RAM. Docker
Desktop 29.9.2 provided 10 CPUs and 8,319,504,384 bytes of memory to the
`linux/amd64` test container. The benchmark completed within that allocation.

Each case used an analyzed stream, two alternating one-row updates, and nested
`auto_explain` capture. The trigger-free cases used 0, 1, or 1,000 subscriptions;
the 1,000-subscription case used 10 distinct pivots. The forced-explicit cases
added a no-op row trigger to select the explicit-DML path and compare 0 and 1
subscription. The test asserted differential refresh, propagated the changed
row, and checked expected notifications. Times include plan instrumentation.
Raw samples and exact plan-log checksums are in [measurements.json](measurements.json);
complete nested plan logs are compressed under [plans/](plans/).

| Trigger path | Subscriptions | Refresh ms (rounds 1, 2) | Commit ms (rounds 1, 2) | Snapshot | Stream sequential tuples returned (rounds 1, 2) |
| --- | ---: | ---: | ---: | --- | ---: |
| Trigger-free | 0 | 567.113, 95.106 | 5.970, 2.520 | None | 0, 0 |
| Trigger-free | 1 | 576.889, 105.470 | 6.408, 4.310 | Bounded, 1 row / 1 identity | 0, 0 |
| Trigger-free | 1,000 | 1,254.591, 866.062 | 6.844, 5.318 | Bounded, 1 row / 1 identity | 0, 0 |
| Forced explicit-DML | 0 | 623.085, 165.441 | 7.030, 4.709 | None | 0, 0 |
| Forced explicit-DML | 1 | 10,386.820, 6,628.518 | 124.024, 121.399 | Full, 1,000,000 rows | 1,000,000, 1,000,000 |

On this workload, trigger-free subscribed refreshes retained a one-row bounded
snapshot through 1,000 subscriptions. The forced no-op-trigger case with one
subscription took a full snapshot and returned one million stream tuples per
round. These are two instrumented samples per case, not general performance
estimates. No controlled concurrent load, randomized run order, CPU or WAL
collection, percentile estimate, or reliable peak-memory measurement was made.
The run covers only analyzed 384-dimensional vectors; it does not qualify
768/1536 dimensions or other workloads.

To reproduce the trigger-free 0/1 subscription cases, run the ignored test with
`PGVECTOR_SUB_ROWS=1000000`, `PGVECTOR_SUB_COUNTS=0,1`,
`PGVECTOR_SUB_DIM=384`, `PGVECTOR_SUB_ROUNDS=2`,
`PGVECTOR_SUB_STATISTICS=analyzed`, `PGVECTOR_SUB_FORCE_EXPLICIT=0`, and
`PGVECTOR_SUB_PLAN_LOG` set to a writable path. For the 1,000-subscription case,
use `PGVECTOR_SUB_COUNTS=1000` and `PGVECTOR_SUB_PIVOTS=10`. For the forced
explicit-DML controls, use `PGVECTOR_SUB_COUNTS=0,1` and
`PGVECTOR_SUB_FORCE_EXPLICIT=1`. Run each command separately because each test
creates its own one-million-row fixture.
