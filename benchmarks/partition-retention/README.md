# Partition retention benchmark

For the full partition regression suite, build a test-only image with the same
feature enabled in both the extension and test binary:

```sh
PGS_E2E_FEATURES=e2e-unsafe-test-hooks ./tests/build_e2e_image.sh
cargo test --features e2e-unsafe-test-hooks --test e2e_partition_tests -- --test-threads=2
```

The default image omits these unsafe test helpers.

Run against a disposable PostgreSQL database with pg_trickle installed and
`psql`, `pgbench`, and Python 3 on `PATH`. The connection must be allowed to
create schemas, tables, and stream tables. The script creates and drops two
randomly named schemas; use a database dedicated to this benchmark. Ensure
there is enough disk space for the data, indexes, change buffers, and WAL.

```sh
python3 benchmarks/partition-retention/benchmark.py \
  --database 'postgresql://localhost/pgtrickle_benchmark' \
  --events 9000000 --output /tmp/retention-9m.json
python3 benchmarks/partition-retention/benchmark.py \
  --database 'postgresql://localhost/pgtrickle_benchmark' \
  --events 27000000 --output /tmp/retention-27m.json
```

Each trial loads 30 retained daily partitions with 1,000 accounts per day
(30,000 retained groups), plus one expired daily partition. It compares a
stream without retention, which reinitializes after direct partition DROP,
with an otherwise equivalent stream configured for TRIM. One pgbench writer
inserts into a retained partition while one reader repeatedly aggregates the
stream. They run during DROP and the first manual refresh. After the writer
stops, the harness waits for its database sessions to exit and refreshes until
an `EXCEPT ALL` comparison matches the entire source query in both directions.
The number of drain refresh calls is recorded. A connection string that
overrides `PGAPPNAME` cannot be used, because the harness must identify its
writer and reader sessions.

JSON output includes wall-clock DROP and refresh-call times, reader and writer
transaction counts and p95/max latency, actual refresh-history actions and
durations, and the mismatch count. `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`
plans capture a complete source aggregation and an output range scan before
the timed operation, including actual rows, partition scans, and buffers. These
are diagnostic proxy plans, not instrumentation of the internal DROP itself.
The script rejects mismatches, incomplete refreshes, or a TRIM trial that
reinitialized, fell back to FULL, or never performed differential work.
It also requires a completed baseline reinitialization. With instrumentation,
it rejects a baseline if the timed refresh did not scan all retained
partitions, which can happen when the scheduler reinitializes first. Without
instrumentation, refresh history alone cannot attribute that work to the timed
call.
Connection startup is included in wall times. Reader and writer latency
samples cover the whole concurrent workload, not only the blocked interval;
inspect the maximum and p95 as stall indicators, not precise lock wait times.

For actual internal scans, repeat with `--instrument-internal`. This loads
PostgreSQL's `auto_explain` module and captures analyzed nested plans during
DROP and refresh, requiring permission to load that module. The JSON includes
executed source partitions and rows examined in source and output scans,
alongside the relevant plans. Counts sum plan-node observations and nested
plans can overlap, so they are not necessarily unique physical reads. Timing
with instrumentation includes plan logging and should not be compared directly
with the uninstrumented times below. PostgreSQL reports per-loop averages, so
counts can be approximate when a node executes more than once.
For this workload, an instrumented TRIM trial fails if its DROP scans a
retained source partition or uses an output scan without an index or TID
condition. If this check fails, the captured plan is saved beside the output
with a `.failed-plan.json` suffix. To exercise
caller settings that disable indexed plans, add
`PGOPTIONS='-c enable_indexscan=off -c enable_bitmapscan=off'` before the
instrumented command. Retention temporarily enables those scan types and
restores the caller's settings afterward.

Add `--downstream` to create a dependent stream, verify its complete result
against the source query after refresh, and record its effective refresh
actions. The TRIM trial rejects a downstream FULL fallback or reinitialization.
Use `--keyless` to exercise a source without a primary key. For example:

```sh
PGOPTIONS='-c random_page_cost=1000000 -c cpu_index_tuple_cost=100000000' python3 benchmarks/partition-retention/benchmark.py \
  --database 'postgresql://localhost/pgtrickle_benchmark' --events 30000 \
  --instrument-internal --downstream --keyless --pending-events 20000 \
  --output /tmp/retention-keyless.json
```

`--pending-events` inserts additional live-partition events immediately before
DROP, so the protected catch-up must process a substantial CDC backlog. These
events are additional to the requested retained event count.
Add `--partitioned-output` to use four hash partitions for stream storage;
instrumentation includes scans of their child tables.

The results depend on cache, storage, CPU, database settings, and concurrent
load. Run several times and report the environment and individual results;
do not compare a smoke run or proxy plan as a production measurement.

## Local measurements

One run per size on 2026-11-17 used PostgreSQL 18.6 on arm64 in Docker
29.9.2, with 10 visible CPUs and approximately 7.75 GiB container memory.
The test image included the E2E test hooks. The full JSON results and plans
are in [results](results/). Times below are milliseconds; refresh is the
measured SQL call, while the recorded reinitialization durations were 3,402
and 9,154 ms respectively.

| Retained events | Mode | DROP | Refresh call | Writer p95 / max | Reader p95 / max |
|---:|---|---:|---:|---:|---:|
| 9,000,000 | baseline | 36 | 3,441 | 0.56 / 4.23 | 1.77 / 3.56 |
| 9,000,000 | TRIM | 108 | 45 | 0.61 / 68.70 | 1.90 / 7.40 |
| 27,000,000 | baseline | 40 | 9,192 | 0.59 / 6.62 | 1.92 / 7.49 |
| 27,000,000 | TRIM | 109 | 48 | 0.63 / 67.01 | 2.09 / 7.35 |

Each trial had zero mismatches. The baseline recorded `REINITIALIZE`; TRIM
recorded only completed `DIFFERENTIAL` actions and no FULL fallback. The
pre-operation diagnostic source plans scanned 31 partitions and 9.3 million
or 27.9 million rows, including the expiring partition. The diagnostic output
range plans used bitmap heap scans and returned 1,000 affected groups. These
plans do not measure the internal DROP statement's scans. The higher maximum
writer latencies in both TRIM runs are visible in the results; this
single sample does not establish a latency distribution or isolate lock time.

Instrumented runs after the sealed-history fallback fix produced these results:

| Retained events | Mode | DROP | Refresh call | Writer p95 / max | Reader p95 / max |
|---:|---|---:|---:|---:|---:|
| 9,000,000 | baseline | 41 | 3,465 | 0.39 / 10.95 | 1.64 / 3.79 |
| 9,000,000 | TRIM | 120 | 55 | 0.51 / 75.27 | 1.70 / 5.93 |
| 27,000,000 | baseline | 65 | 9,731 | 4.55 / 18.05 | 5.84 / 12.51 |
| 27,000,000 | TRIM | 144 | 576 | 0.50 / 86.72 | 1.82 / 6.27 |

Both comparisons had zero mismatches. Baseline refresh scanned all 30 retained
partitions and 9,003,254 or 27,000,679 rows including concurrent inserts.
TRIM's DROP and following refresh scanned no source partitions; DROP observed
2,858 or 2,882 output rows across indexed lookups and range deletion. At 27M,
the following ordinary differential refresh observed 120,000 output rows
across nested plans. These sums can include overlapping plans. PostgreSQL
planner preferences do not guarantee a particular plan, and ordinary refreshes
can still scan the full output. Instrumented times include plan logging and
should not be compared directly with the uninstrumented table. An earlier
instrumented 27M attempt failed after a storage I/O error and restart.

In a separate 30,000-event diagnostic with the caller's index and bitmap scans
disabled, the earlier implementation's TRIM DROP took 13,362 ms and observed
four sequential output scans (124,000 rows total). With scoped index scans it
took 112 ms and observed 2,910 rows across indexed scans. Both results matched
their source queries. This single small run tests the planner-setting edge case;
it is not a production-scale timing comparison. The raw plans are in the
`index-disabled-before` and `index-disabled-after` result files.
After adding the row-identity index check, the same diagnostic passed with
zero mismatches and 2,880 observed output rows; see `identity-index-smoke`.
After tightening keyed-index uniqueness validation, it passed with zero
mismatches and 2,912 observed output rows; see `unique-identity-smoke`.

In a 30,000-event diagnostic with `random_page_cost=1000000` and a downstream
stream, the earlier capture path took 2,703 ms and observed 95,884 output
rows, including unbounded scans. The scoped capture took 226 ms and observed
5,680 output rows through bounded scans; upstream and downstream both matched
the source query, and the downstream refresh was DIFFERENTIAL. These are
single diagnostic runs with plan logging, not production-scale timings.
Without a downstream stream at the same planner cost, the diagnostic changed
from 12,587 ms and 124,000 observed output rows to 115 ms and 2,868 rows.
The bounded-plan check also passed in separate runs with a keyless source and
with the aggregate fast path disabled. These settings are planner tests, not
production-scale measurements.
One combined high-cost keyless/downstream diagnostic failed the unbounded-scan
check. Its captured plan read the entire 31,000-row output index in each of
three statements; two join nodes each rejected about 31 million row pairs.
After retention scoped `random_page_cost` to 1, 40 repetitions passed. An
additional 30,000-event diagnostic with 20,000 deliberately pending events
observed 7,000 output rows across bounded scans and no retained-source scans;
upstream and downstream matched their independent query. These diagnostics ran
on PostgreSQL 18.7 arm64 on macOS with a debug build; they are not comparable
to the Docker scale measurements. The captured before and after plans are in
`results/`. PostgreSQL planner choices can still violate the affected-groups
performance target, even with these hints.

With only the page-cost fix, an extreme `cpu_index_tuple_cost` also caused
the catch-up to scan all 31,000 output rows. After scoping that cost as well,
a run with both extreme caller settings and 1,000 pending events observed
7,000 output rows in bounded scans, no retained-source scans, and exact
upstream and downstream results. Caller settings were restored after the cut.
Retention catch-up now also uses a correlated aggregate lookup so the delta
group drives the storage probe. A 20,000-event backlog with both extreme costs
and a separate 1,000-event backlog with `cpu_operator_cost=1000000` each passed
the bounded-scan check and both result oracles; the nested plans are retained
in `results/`. Other catch-up statements still depend on planner choices.

With `cpu_operator_cost=100000000`, a separate 1,000-change keyless backlog
caused each of two downstream capture statements to revisit 500,500 delta rows.
After scoping that planner cost to its default during retention, each visited
1,000 delta rows; neither run scanned retained source partitions, and both
upstream and downstream results matched their oracles. The instrumented TRIM
DROP times were 318 ms and 289 ms in these single debug-build runs. The raw
plans are `results/2026-11-17-pg18-macos-operator-cost-before-100m.json` and
`results/2026-11-17-pg18-macos-operator-cost-fixed.json`. The benchmark now
rejects repeated sequential scans of the delta in downstream capture. Two
earlier post-change attempts produced no reader latency samples and were
discarded. Both plans still compared 999,000 row pairs in each downstream
capture join, which the earlier scan-only guard did not detect.

Retention capture now compares its affected pre- and post-refresh multisets
with `EXCEPT ALL`. In a 30,000-event diagnostic with 1,000 pending events and
hash joins, merge joins, materialization, memoization, sorting, hash aggregation,
and index-only scans disabled by the caller, the previous plan examined
1,007,000 output rows and repeatedly compared affected rows. The new plan
examined 8,000 output rows, scanned no retained source partitions, and matched
both full-result oracles. Its instrumented DROP took 254 ms in one debug-build
run. The diagnostic restored normal planner settings for follow-up refreshes;
the concurrent reader and writer retained the restrictive settings. Raw plans
are `results/2026-11-17-pg18-macos-join-flags-before.json` and
`results/2026-11-17-pg18-macos-multiset-after.json`. The benchmark also rejects
nested capture joins that discard more than 100 times their output rows, with
a 1,000-row floor. This heuristic does not prove a hard planner bound.

Instrumented runs with `--downstream` on the final candidate produced these
results (milliseconds):

| Retained events | Mode | DROP | First refresh call | Writer p95 / max | Reader p95 / max |
|---:|---|---:|---:|---:|---:|
| 9,000,000 | baseline | 45 | 3,769 | 0.52 / 11.91 | 1.75 / 3.10 |
| 9,000,000 | TRIM | 231 | 60 | 0.52 / 185.08 | 1.79 / 3.36 |
| 27,000,000 | baseline | 50 | 9,948 | 0.45 / 10.34 | 1.77 / 2.76 |
| 27,000,000 | TRIM | 233 | 630 | 0.63 / 183.14 | 1.91 / 2.88 |

Both upstream and downstream results matched the source in each trial. The
baseline reinitialized; TRIM and its downstream completed DIFFERENTIAL work
without fallback. During TRIM DROP, no retained source partitions were scanned,
and 5,760 or 5,665 output rows were observed across nested plans. Baseline
refresh scanned 30 retained partitions and 9,003,154 or 27,003,453 rows,
including concurrent inserts. Each baseline required two post-writer drain
calls; each TRIM trial required one. At 27M the TRIM drain refresh itself took
about 557 ms, outside the first-call time shown. Plan logging adds overhead;
latency samples cover the entire concurrent workload and do not isolate locks.

After the correlated aggregate change, one instrumented run per size on
PostgreSQL 18.7 arm64 macOS with a debug build produced the following times
in milliseconds. These runs are separate from the Docker measurements above.

| Retained events | Mode | DROP | First refresh | Writer p95 / max | Reader p95 / max |
|---:|---|---:|---:|---:|---:|
| 9,000,000 | baseline | 41 | 3,824 | 0.11 / 10.69 | 2.04 / 3.26 |
| 9,000,000 | TRIM | 582 | 234 | 1.95 / 437.22 | 10.72 / 27.89 |
| 27,000,000 | baseline | 41 | 10,587 | 0.12 / 9.71 | 2.04 / 3.36 |
| 27,000,000 | TRIM | 1,294 | 207 | 0.13 / 1,249.30 | 2.13 / 2.99 |

Both oracles matched. Baseline reinitialized and scanned 30 retained
partitions with 9,012,813 or 27,012,605 observed source rows; each baseline
needed two drain calls. TRIM and downstream completed DIFFERENTIAL work, with
one drain call each. TRIM DROP scanned no retained source partitions and
observed 5,660 or 6,000 output rows across nested plans. These single samples
include instrumentation overhead and do not isolate lock waits.

After scoping the unpartitioned output DML, a further single instrumented
macOS debug-build run per size produced these times in milliseconds:

| Retained events | Mode | DROP | First refresh | Writer p95 / max | Reader p95 / max |
|---:|---|---:|---:|---:|---:|
| 9,000,000 | baseline | 40 | 3,768 | 0.13 / 10.91 | 1.96 / 3.15 |
| 9,000,000 | TRIM | 288 | 207 | 0.13 / 245.22 | 2.10 / 3.46 |
| 27,000,000 | baseline | 40 | 10,032 | 0.10 / 9,988.44 | 2.04 / 3.20 |
| 27,000,000 | TRIM | 377 | 238 | 1.93 / 270.66 | 8.72 / 34.26 |

The timed baseline refresh scanned all 30 retained partitions, observing
9,013,437 or 27,013,324 source rows. TRIM DROP scanned no retained source
partitions and observed 7,000 or 6,202 output rows in nested plans. Both
upstream and downstream matched their complete result oracles; TRIM and its
downstream completed differential refreshes. The baseline needed two drain
calls and TRIM one. The writer and reader samples cover the whole workload,
not just the lock wait, and instrumentation adds overhead.

For partitioned and unpartitioned output, retention catch-up correlates delete, update,
and existence probes with the affected identities, and enables TID scans in
its restored planner scope. A keyless insert/update/delete check and a
duplicate-identity multiset check passed against PostgreSQL. Captured plans
show indexed probes and TID scans. Partitioned output can still visit multiple
storage partitions per identity, and the planner can choose other plans, so
the hard performance bound remains unproven.

A 30,000-event HASH-output diagnostic with 20,000 pending events, a keyless
source, and a downstream stream reproduced missing downstream updates before
the HASH path was corrected to honor explicit DML. Afterward, ten runs with
the scheduler disabled and `pg_trickle.refresh_strategy=differential` passed
the bounded-scan check and both result oracles. One representative run is in
`results/`; it observed 11,000 output rows during TRIM DROP and no retained
source scans. This is a small diagnostic, not a scale comparison.

With the scheduler enabled, a separate repetition hit a PostgreSQL deadlock
between manual upstream refresh and a downstream rebuild. PostgreSQL aborted
the manual transaction. Protected FULL baselines now try-lock upstream
metadata before taking source storage locks; a two-session test confirmed a
prompt retryable error, successful retry, and no replay after the FULL
baseline. A deterministic scheduler test also passed against PostgreSQL 18 in
Testcontainers: it held the upstream catalog lock while the scheduler reached
its upstream check, then verified the scheduled reinitialization and exact
downstream rows after the upstream refresh committed. An additional high-cost
run used a normal FULL fallback after the retention cut, which this benchmark
rejected.

After merging the 0.110.2 changes from `main`, single instrumented macOS
PostgreSQL 18.7 debug-build runs with the scheduler disabled gave the
following times in milliseconds:

| Retained events | Mode | DROP | First refresh | Writer p95 / max | Reader p95 / max |
|---:|---|---:|---:|---:|---:|
| 9,000,000 | baseline | 41 | 3,814 | 0.12 / 3,769.87 | 2.08 / 7.17 |
| 9,000,000 | TRIM | 289 | 57 | 0.13 / 244.68 | 2.01 / 6.32 |
| 27,000,000 | baseline | 38 | 10,097 | 0.10 / 8.67 | 2.04 / 3.12 |
| 27,000,000 | TRIM | 286 | 61 | 0.12 / 243.95 | 2.12 / 2.99 |

Both complete-result oracles matched. The timed baseline refresh scanned all
30 retained partitions, observing 9,012,353 or 27,013,237 source rows. TRIM
DROP scanned no retained source partitions and observed 7,000 output rows;
the upstream and downstream completed DIFFERENTIAL work. Each trial needed
one post-writer drain refresh. These are single instrumented samples, and the
writer and reader latency distributions cover the whole workload rather than
isolating lock waits. A separate HASH-output backlog diagnostic on the merged
candidate also passed both oracles with no retained-source scan.
Those earlier large-run plans also used quadratic downstream capture joins;
the following runs use the multiset capture and the stronger plan guard.

After replacing the downstream capture join with multiset differences, a new
single instrumented run per size used the same 30,000 retained groups and
concurrent writer and reader. Times are milliseconds:

| Retained events | Mode | DROP | First refresh | Writer p95 / max | Reader p95 / max |
|---:|---|---:|---:|---:|---:|
| 9,000,000 | baseline | 52 | 3,743 | 0.10 / 16.84 | 2.09 / 3.39 |
| 9,000,000 | TRIM | 169 | 56 | 0.10 / 126.38 | 2.05 / 3.46 |
| 27,000,000 | baseline | 63 | 11,471 | 0.19 / 11.86 | 2.74 / 4.20 |
| 27,000,000 | TRIM | 163 | 59 | 0.11 / 120.57 | 2.16 / 3.08 |

Both upstream and downstream oracles matched. Baseline reinitialization
scanned all 30 retained partitions and observed 9,014,091 or 27,009,576 source
rows; TRIM DROP scanned no retained source partitions and observed 7,000
output rows. TRIM and downstream completed differential actions. The new
join-amplification guard passed. Results and executed plans are in
`results/2026-11-17-pg18-macos-multiset-9m.json` and
`results/2026-11-17-pg18-macos-multiset-27m.json`. These debug-build samples
include plan-logging overhead; latencies span the workload and do not isolate
lock waits.

After adding the deterministic-collation guard, the same native PostgreSQL 18.7
arm64 setup was rerun once at each size, with instrumentation and both oracles:

| Retained events | Mode | DROP | First refresh | Writer p95 / max | Reader p95 / max |
|---:|---|---:|---:|---:|---:|
| 9,000,000 | baseline | 41 | 3,738 | 0.10 / 10.32 | 1.99 / 3.09 |
| 9,000,000 | TRIM | 170 | 59 | 0.11 / 125.05 | 2.12 / 3.32 |
| 27,000,000 | baseline | 37 | 10,072 | 0.10 / 9.45 | 2.12 / 2.98 |
| 27,000,000 | TRIM | 175 | 63 | 0.12 / 131.74 | 2.19 / 3.01 |

Times are milliseconds. Baseline reinitialization scanned 30 retained
partitions and observed 9,014,341 and 27,014,435 source rows, respectively.
TRIM scanned no retained source partitions during DROP and observed 7,000
output rows; TRIM and downstream refreshes completed differentially. The
raw plans and latency samples are in `results/2026-11-17-pg18-macos-collation-9m.json`
and `results/2026-11-17-pg18-macos-collation-27m.json`. These are single
instrumented debug-build runs, not a guaranteed planner bound.

The following instrumented native PostgreSQL 18.7 arm64 debug-build runs were
repeated after the floating-point eligibility guard. Each includes a concurrent
writer, reader, and downstream stream. Times are milliseconds.

| Retained events | Sample | Mode | DROP | First refresh | Writer p95 / max | Reader p95 / max |
|---:|---|---|---:|---:|---:|---:|
| 9,000,000 | 1 | baseline | 50 | 3,853 | 0.14 / 10.09 | 2.08 / 3.17 |
| 9,000,000 | 1 | TRIM | 1,492 | 63 | 0.13 / 1,448.43 | 1.99 / 2.75 |
| 9,000,000 | 2 | baseline | 40 | 3,726 | 0.10 / 3,682.42 | 1.99 / 3.10 |
| 9,000,000 | 2 | TRIM | 166 | 205 | 0.11 / 123.71 | 2.05 / 3.29 |
| 27,000,000 | 1 | baseline | 41 | 10,270 | 0.11 / 9.93 | 2.10 / 3.24 |
| 27,000,000 | 1 | TRIM | 172 | 206 | 0.13 / 126.69 | 2.08 / 2.95 |

Both result oracles matched in every sample. Baseline refreshes scanned all 30
retained partitions and observed 9,013,205, 9,014,424, and 27,013,287 source
rows respectively. TRIM DROP scanned no retained source partitions and observed
7,000 output rows per sample; TRIM and downstream completed differential work.
The first 9M TRIM DROP and the repeat baseline each showed a long writer latency.
The server logged frequent WAL-driven checkpoints during the benchmark period,
but the measurements do not establish the cause of either stall. Latencies cover
the workload and do not isolate lock waits; these samples are not a hard planner
bound. The plans are in `results/2026-11-17-pg18-macos-head-9m.json`,
`results/2026-11-17-pg18-macos-head-9m-repeat.json`, and
`results/2026-11-17-pg18-macos-head-27m.json`.
