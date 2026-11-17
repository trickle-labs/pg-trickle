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
stops, a final refresh drains its changes and an `EXCEPT ALL` comparison checks
the entire materialization against the source query in both directions.

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

A separate 9M run with internal instrumentation captured no source partition
scans during TRIM's DROP or following refresh; the baseline refresh scanned
all 30 retained partitions and about 9,002,045 rows, including concurrent
inserts. TRIM's DROP observed 125,001 output rows across nested scan plans,
including several full scans of the 31,000-row output, and the following
refresh observed 90,084 output rows. These sums can include overlapping plans.
Thus the range deletion itself is indexed, but the existing differential
catch-up can still scale with the complete materialized output rather than
only affected groups. The instrumented 27M rerun did not complete: PostgreSQL
reported a storage I/O error and restarted. The uninstrumented 27M run above
completed successfully.
