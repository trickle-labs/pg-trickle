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

The results depend on cache, storage, CPU, database settings, and concurrent
load. Run several times and report the environment and individual results;
do not compare a smoke run or proxy plan as a production measurement.

## Local measurements

One run per size on 2026-11-16 used PostgreSQL 18.6 on arm64 in Docker
29.9.2, with 10 visible CPUs and approximately 7.75 GiB container memory.
The test image included the E2E test hooks. The full JSON results and plans
are in [results](results/). Times below are milliseconds; refresh is the
measured SQL call, while the recorded reinitialization durations were 3,838
and 13,825 ms respectively.

| Retained events | Mode | DROP | Refresh call | Writer p95 / max | Reader p95 / max |
|---:|---|---:|---:|---:|---:|
| 9,000,000 | baseline | 49 | 3,890 | 2.9 / 12.5 | 3.6 / 7.6 |
| 9,000,000 | TRIM | 108 | 149 | 2.8 / 7.2 | 3.7 / 6.1 |
| 27,000,000 | baseline | 47 | 13,868 | 1.8 / 21.4 | 3.2 / 22.6 |
| 27,000,000 | TRIM | 101 | 584 | 2.1 / 63.4 | 2.6 / 7.0 |

Each trial had zero mismatches. The baseline recorded `REINITIALIZE`; TRIM
recorded only completed `DIFFERENTIAL` actions and no FULL fallback. The
pre-operation diagnostic source plans scanned 31 partitions and 9.3 million
or 27.9 million rows, including the expiring partition. The diagnostic output
range plans used bitmap heap scans and returned 1,000 affected groups. These
plans do not measure the internal DROP statement's scans. The higher maximum
writer latency in the 27-million-row TRIM run is visible in the results; this
single sample does not establish a latency distribution or isolate lock time.
