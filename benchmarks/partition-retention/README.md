# Partition retention benchmark

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
PGOPTIONS='-c random_page_cost=1000000' python3 benchmarks/partition-retention/benchmark.py \
  --database 'postgresql://localhost/pgtrickle_benchmark' --events 30000 \
  --instrument-internal --downstream --keyless --pending-events 20000 \
  --output /tmp/retention-keyless.json
```

`--pending-events` inserts additional live-partition events immediately before
DROP, so the protected catch-up must process a substantial CDC backlog. These
events are additional to the requested retained event count.

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
