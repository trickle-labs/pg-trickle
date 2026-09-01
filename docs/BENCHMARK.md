# pg_trickle — Benchmark Guide

This document explains how the database-level refresh benchmarks work and how to interpret their output.

---

## Overview

The benchmark suite in `tests/e2e_bench_tests.rs` measures **wall-clock refresh time** for **FULL** vs **DIFFERENTIAL** mode across a matrix of table sizes, change rates, and query complexities. Each benchmark spawns an isolated PostgreSQL 18.x container via Testcontainers, ensuring reproducible and interference-free measurements.

The core question the benchmarks answer:

> **How much faster is an DIFFERENTIAL refresh compared to a FULL refresh, given a specific workload?**

## v0.87 low-impact overhead gate

`benchmarks/pgbench-v0.87/run.sh` is the versioned three-configuration gate
for the v0.87 write-path contract. It recreates each database for every
paired repetition and rotates the configuration order deterministically:

| Configuration | PostgreSQL contents |
|---|---|
| `absent` | Stock PostgreSQL, without pg_trickle |
| `installed` | pg_trickle installed and preloaded, with no stream tables |
| `active` | pg_trickle with projection, aggregate, and join stream tables |

The blocking profile uses PostgreSQL 18.3, scale 10, 8 clients, 2 jobs, a
10-second warm-up, a 30-second measurement, and 3 paired repetitions. It
samples pgbench transaction logs at 10%, records p50/p95/p99, checks active
stream-table correctness and refresh history, and samples Linux `/proc`
jiffies for the scheduler/refresh workers. The publication profile is a
five-repetition, 60-second run.

```bash
./tests/build_e2e_image.sh
benchmarks/pgbench-v0.87/run.sh --profile blocking
```

The machine-readable result is `target/pgbench-v0.87/result.json`; the
versioned product budgets are in
[`benchmarks/pgbench-v0.87/budgets.json`](../benchmarks/pgbench-v0.87/budgets.json).
The gate fails closed for missing repetitions, malformed/non-finite values,
missing latency samples, unavailable Linux CPU samples, failed correctness,
or a budget regression. The initial v0.87 product floor preserves the
roadmap requirement of at least 99% installed/no-stream TPS; active p99 and
refresh CPU share are separately budgeted so CI noise cannot silently change
the product contract.

## v0.88 vector aggregate gate

`benchmarks/vector-aggregate-v0.88/contract.json` freezes the primary v0.88
workload. It uses 1,000,000 rows, 10,000 groups, and a deterministic
100,000-row mixed delta with `SUM(int4)`, `COUNT(*)`, and `AVG(int4)`. The gate
runs one warm-up and five measured differential refreshes and checks exact
multiset equality after every refresh.

```bash
cargo test --test e2e_bench_tests --features pg18 -- \
  --ignored --test-threads=1 --nocapture bench_vector_aggregate_v088
```

The release result must reach 5 times the saved v0.87.17 median changed rows
per second. Eligible production-like workloads may not regress by more than
10 percent.

## v0.89 window admission artifact

`benchmarks/window-v0.89/admission.json` records the v0.89 decision for every
candidate family. All entries have `runtime_enabled = false` and use
`partition_recompute`. The narrow built-in `ROW_NUMBER` candidate failed the
performance gate.

The measured query used one direct keyed scan, exact same-name projections,
and the complete non-null identity in `ORDER BY`. The candidate filtered
changed output rows but still recomputed the affected partition. Each cell has
five measured repetitions after one warm-up.

| Rows | Change | State candidate median ms [range] | Partition recompute median ms [range] | Recompute/state ratio |
|---:|---|---:|---:|---:|
| 1,000 | Tail insert | 29.941625 [28.490875, 31.011292] | 12.065875 [11.525541, 13.216917] | 0.4030 |
| 1,000 | Front insert | 92.454083 [44.982209, 152.869667] | 20.670208 [18.595209, 38.816750] | 0.2236 |
| 10,000 | Tail insert | 553.613125 [143.272875, 594.418208] | 24.684708 [23.715125, 25.846125] | 0.0446 |
| 10,000 | Front insert | 691.848292 [685.656292, 762.309333] | 112.537125 [99.531458, 123.249584] | 0.1627 |
| 100,000 | Tail insert | 1556.859583 [1311.842167, 1831.429541] | 164.637041 [158.997375, 192.374791] | 0.1057 |
| 100,000 | Front insert | 4190.417875 [4085.014458, 4430.121000] | 1299.224083 [1229.935416, 1420.041042] | 0.3100 |

The ratio is the partition-recompute median divided by the state-candidate
median. Every ratio is below 1.0. The candidate therefore failed the required
20 percent improvement gate.

| Rows | Change | State candidate WAL bytes / output rows | Partition recompute WAL bytes / output rows |
|---:|---|---:|---:|
| 1,000 | Tail insert | 439,056 / 1 | 38,840 / 1 |
| 1,000 | Front insert | 1,239,248 / 1,010 | 416,336 / 2,019 |
| 10,000 | Tail insert | 4,360,624 / 1 | 41,272 / 1 |
| 10,000 | Front insert | 11,951,736 / 10,010 | 3,752,704 / 20,019 |
| 100,000 | Tail insert | 43,259,896 / 1 | 40,880 / 1 |
| 100,000 | Front insert | 124,280,840 / 100,010 | 37,409,824 / 200,019 |

These are medians. The benchmark did not sample state relation size. v0.89
publishes no incremental speedup claim.

Run the offline contract check with:

```bash
python3 scripts/v0_89_release_gate.py
```

```bash
bash scripts/run_light_e2e_tests.sh \
  --package --test e2e_window_incremental_tests --ignored
```

---

## Prerequisites

Build the E2E test Docker image before running any benchmarks:

```bash
./tests/build_e2e_image.sh
```

Docker must be running on the host.

---

## Running Benchmarks

All benchmark tests are tagged `#[ignore]` so they are skipped during normal CI. The `--nocapture` flag is required to see the printed output tables.

### Quick Spot Checks (~5–10 seconds each)

```bash
# Simple scan, 10K rows, 1% change rate
cargo test --test e2e_bench_tests --features pg18 -- --ignored --nocapture bench_scan_10k_1pct

# Aggregate query, 100K rows, 1% change rate
cargo test --test e2e_bench_tests --features pg18 -- --ignored --nocapture bench_aggregate_100k_1pct

# Join + aggregate, 100K rows, 10% change rate
cargo test --test e2e_bench_tests --features pg18 -- --ignored --nocapture bench_join_agg_100k_10pct
```

### Zero-Change Latency (~5 seconds)

```bash
cargo test --test e2e_bench_tests --features pg18 -- --ignored --nocapture bench_no_data_refresh_latency
```

### Full Matrix (~15–30 minutes)

Runs all 30 combinations and prints a consolidated summary:

```bash
cargo test --test e2e_bench_tests --features pg18 -- --ignored --nocapture bench_full_matrix
```

### Run All Benchmarks in Parallel

```bash
cargo test --test e2e_bench_tests --features pg18 -- --ignored --nocapture
```

Note: each test starts its own container, so parallel execution requires sufficient Docker resources.

---

## Benchmark Dimensions

### Table Sizes

| Size | Rows | Purpose |
|------|------|---------|
| Small | 10,000 | Fast iteration; measures per-row overhead |
| Medium | 100,000 | More realistic; reveals scaling characteristics |

### Change Rates

| Rate | Description |
|------|-------------|
| 1% | Low churn — the sweet spot for differential refresh |
| 10% | Moderate churn — tests delta query scalability |
| 50% | High churn — stress test; approaches full-refresh cost |

### Query Complexities

| Scenario | Defining Query | Operators Tested |
|----------|---------------|------------------|
| **scan** | `SELECT id, region, category, amount, score FROM src` | Table scan only |
| **filter** | `SELECT id, region, amount FROM src WHERE amount > 5000` | Scan + filter (WHERE) |
| **aggregate** | `SELECT region, SUM(amount), COUNT(*) FROM src GROUP BY region` | Scan + group-by aggregate |
| **join** | `SELECT s.id, s.region, s.amount, d.region_name FROM src s JOIN dim d ON ...` | Scan + inner join |
| **join_agg** | `SELECT d.region_name, SUM(s.amount), COUNT(*) FROM src s JOIN dim d ON ... GROUP BY ...` | Scan + join + aggregate |

### DML Mix per Cycle

Each change cycle applies a realistic mix of operations:

| Operation | Fraction | Example at 10K rows, 10% rate |
|-----------|----------|-------------------------------|
| UPDATE | 70% | 700 rows have `amount` incremented |
| DELETE | 15% | 150 rows removed |
| INSERT | 15% | 150 new rows added |

---

## What Each Benchmark Does

```
1. Start a fresh PostgreSQL 18.x container
2. Install the pg_trickle extension
3. Create and populate the source table (10K or 100K rows)
4. Create dimension table if needed (for join scenarios)
5. ANALYZE for stable query plans

── FULL mode ──
6. Create a Stream Table in FULL refresh mode
7. For each of 3 cycles:
   a. Apply random DML (updates + deletes + inserts)
   b. ANALYZE
   c. Time the FULL refresh (TRUNCATE + re-execute entire query)
   d. Record refresh_ms and ST row count
8. Drop the FULL-mode ST

── DIFFERENTIAL mode ──
9. Reset source table to same starting state
10. Create a Stream Table in DIFFERENTIAL refresh mode
11. For each of 3 cycles:
    a. Apply random DML (same parameters)
    b. ANALYZE
    c. Time the DIFFERENTIAL refresh (delta query + MERGE)
    d. Record refresh_ms and ST row count

12. Print results table and summary
```

Both modes start from the same data to ensure a fair comparison. The 3-cycle design captures warm-up effects (cycle 1 may be slower due to plan caching).

---

## Reading the Output

### Detail Table

```
╔══════════════════════════════════════════════════════════════════════════════════════╗
║                    pg_trickle Refresh Benchmark Results                      ║
╠════════════╤══════════╤════════╤═════════════╤═══════╤════════════╤═════════════════╣
║ Scenario   │ Rows     │ Chg %  │ Mode        │ Cycle │ Refresh ms │ ST Rows         ║
╠════════════╪══════════╪════════╪═════════════╪═══════╪════════════╪═════════════════╣
║ aggregate  │    10000 │     1% │ FULL        │     1 │       22.1 │               5 ║
║ aggregate  │    10000 │     1% │ FULL        │     2 │        4.8 │               5 ║
║ aggregate  │    10000 │     1% │ FULL        │     3 │        5.3 │               5 ║
║ aggregate  │    10000 │     1% │ DIFFERENTIAL │     1 │        8.4 │               5 ║
║ aggregate  │    10000 │     1% │ DIFFERENTIAL │     2 │        4.4 │               5 ║
║ aggregate  │    10000 │     1% │ DIFFERENTIAL │     3 │        4.6 │               5 ║
╚════════════╧══════════╧════════╧═════════════╧═══════╧════════════╧═════════════════╝
```

| Column | Meaning |
|--------|---------|
| **Scenario** | Query complexity level (scan, filter, aggregate, join, join_agg) |
| **Rows** | Number of rows in the base table |
| **Chg %** | Percentage of rows changed per cycle |
| **Mode** | FULL (truncate + recompute) or DIFFERENTIAL (delta + merge) |
| **Cycle** | Which of the 3 measurement rounds (cycle 1 often includes warm-up) |
| **Refresh ms** | Wall-clock time for the refresh operation |
| **ST Rows** | Row count in the Stream Table after refresh (sanity check) |

### Summary Table

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Summary (avg ms per cycle)                       │
├────────────┬──────────┬────────┬─────────────────┬──────────────────────┤
│ Scenario   │ Rows     │ Chg %  │ FULL avg ms     │ DIFFERENTIAL avg ms   │
├────────────┼──────────┼────────┼─────────────────┼──────────────────────┤
│ aggregate  │    10000 │     1% │       10.7       │        5.8 (  1.8x) │
└────────────┴──────────┴────────┴─────────────────┴──────────────────────┘
```

The **Speedup** value in parentheses is `FULL avg / DIFFERENTIAL avg` — how many times faster the differential refresh is compared to a full refresh.

---

## Interpreting the Speedup

### What to Expect

| Change Rate | Table Size | Expected Speedup | Explanation |
|-------------|------------|-------------------|-------------|
| 1% | 10K | 1.5–5x | Small table; overhead is similar, delta is tiny |
| 1% | 100K | 5–50x | Larger table amplifies full-refresh cost |
| 10% | 100K | 2–10x | Moderate delta; still significantly faster |
| 50% | any | 1–2x | Delta is nearly as large as full table |

### Rules of Thumb

| Speedup | Interpretation |
|---------|---------------|
| **> 10x** | Strong win for DIFFERENTIAL — typical at low change rates on larger tables |
| **5–10x** | Clear advantage for DIFFERENTIAL |
| **2–5x** | Moderate advantage — DIFFERENTIAL is the right choice |
| **1–2x** | Marginal gain — either mode is acceptable |
| **~1x** | Break-even — change rate is too high for incremental to help |
| **< 1x** | DIFFERENTIAL is slower — would indicate overhead exceeds savings (investigate) |

### Key Patterns to Look For

1. **Scaling with table size**: For the same change rate, speedup should increase with table size. FULL must re-process all rows; DIFFERENTIAL processes only the delta.

2. **Degradation with change rate**: As change rate rises from 1% → 50%, speedup should decrease. At 50%, DIFFERENTIAL processes half the table which approaches FULL cost.

3. **Query complexity amplifies speedup**: Aggregate and join queries benefit more from DIFFERENTIAL because they avoid expensive re-computation. A join_agg at 1% changes should show higher speedup than a simple scan at the same parameters.

4. **Cycle 1 warm-up**: The first cycle in each mode may be slower due to PostgreSQL plan cache population. Use cycles 2–3 for the steadiest numbers.

5. **ST Rows consistency**: The ST row count should be similar between FULL and DIFFERENTIAL for the same scenario (accounting for random DML). Large discrepancies indicate a correctness issue.

---

## Zero-Change Latency

The `bench_no_data_refresh_latency` test measures the overhead of a refresh when **no data has changed** — the NO_DATA code path.

```
┌──────────────────────────────────────────────┐
│ NO_DATA Refresh Latency (10 iterations)      │
├──────────────────────────────────────────────┤
│ Avg:     3.21 ms                             │
│ Max:     5.10 ms                             │
│ Target: < 10 ms                              │
│ Status: ✅ PASS                              │
└──────────────────────────────────────────────┘
```

| Metric | Meaning |
|--------|---------|
| **Avg** | Average wall-clock time across 10 no-op refreshes |
| **Max** | Worst-case single iteration |
| **Target** | The [PLAN.md](https://github.com/trickle-labs/pg-trickle/blob/main/plans/PLAN.md) goal: < 10 ms per no-op refresh |
| **Status** | PASS if avg < 10 ms, SLOW otherwise |

A passing result confirms the scheduler's per-cycle overhead is negligible. Values > 10 ms in containerized environments may be acceptable due to Docker overhead; bare-metal PostgreSQL should comfortably meet the target.

---

## Available Tests

### Individual Tests (10K rows)

| Test Name | Scenario | Change Rate |
|-----------|----------|-------------|
| `bench_scan_10k_1pct` | scan | 1% |
| `bench_scan_10k_10pct` | scan | 10% |
| `bench_scan_10k_50pct` | scan | 50% |
| `bench_filter_10k_1pct` | filter | 1% |
| `bench_aggregate_10k_1pct` | aggregate | 1% |
| `bench_join_10k_1pct` | join | 1% |
| `bench_join_agg_10k_1pct` | join_agg | 1% |

### Individual Tests (100K rows)

| Test Name | Scenario | Change Rate |
|-----------|----------|-------------|
| `bench_scan_100k_1pct` | scan | 1% |
| `bench_scan_100k_10pct` | scan | 10% |
| `bench_scan_100k_50pct` | scan | 50% |
| `bench_aggregate_100k_1pct` | aggregate | 1% |
| `bench_aggregate_100k_10pct` | aggregate | 10% |
| `bench_join_agg_100k_1pct` | join_agg | 1% |
| `bench_join_agg_100k_10pct` | join_agg | 10% |

### Special Tests

| Test Name | Description |
|-----------|-------------|
| `bench_full_matrix` | All 30 combinations (5 queries × 2 sizes × 3 rates) |
| `bench_no_data_refresh_latency` | Zero-change overhead (10 iterations) |

---

## Nexmark Streaming Benchmark

The Nexmark benchmark validates correctness against a sustained high-frequency
DML workload modelling an online auction system. It is adapted from the
[Nexmark benchmark specification](https://datalab.cs.pdx.edu/niagara/NEXMark/)
used by streaming systems like Flink, Feldera, and Materialize.

### Data Model

| Table | Description | Default Size |
|-------|-------------|--------------|
| `person` | Registered users (sellers/bidders) | 100 rows |
| `auction` | Items listed for sale | 500 rows |
| `bid` | Bids placed on auctions | 2,000 rows |

### Queries

| Query | Features | Description |
|-------|----------|-------------|
| Q0 | Passthrough | Identity projection of all bids |
| Q1 | Projection + arithmetic | Currency conversion |
| Q2 | Filter | Bids on specific auctions |
| Q3 | JOIN + filter | Local item suggestion (person-auction join) |
| Q4 | JOIN + GROUP BY + AVG | Average selling price by category |
| Q5 | GROUP BY + COUNT | Hot items (bid count per auction) |
| Q6 | JOIN + GROUP BY + AVG | Average bid price per seller |
| Q7 | Aggregate (MAX) | Highest bid price |
| Q8 | JOIN | Person-auction join (new users monitoring) |
| Q9 | JOIN + DISTINCT ON | Winning bid per auction with bidder info |

### Running Nexmark Tests

```bash
# Default scale (100 persons, 500 auctions, 2000 bids, 3 cycles)
cargo test --test e2e_nexmark_tests -- --ignored --test-threads=1 --nocapture

# Larger scale
NEXMARK_PERSONS=1000 NEXMARK_AUCTIONS=5000 NEXMARK_BIDS=50000 NEXMARK_CYCLES=5 \
  cargo test --test e2e_nexmark_tests -- --ignored --test-threads=1 --nocapture
```

### What Each Cycle Does

Each refresh cycle applies three mutation functions (RF1-RF3) then refreshes
all stream tables and asserts multiset equality:

1. **RF1 (INSERT):** New persons, auctions, and bids
2. **RF2 (DELETE):** Remove oldest bids, orphaned auctions, orphaned persons
3. **RF3 (UPDATE):** Price changes, reserve adjustments, city moves
4. **Refresh + Assert:** Differential refresh → EXCEPT ALL correctness check

### Correctness Validation

The test uses the same DBSP invariant as TPC-H: after every differential
refresh, the stream table must be **multiset-equal** to re-executing the
defining query from scratch (symmetric `EXCEPT ALL`). Additionally, negative
`__pgt_count` values (over-retraction bugs) are detected.

---

## DAG Topology Benchmarks

The DAG topology benchmark suite in `tests/e2e_dag_bench_tests.rs` measures **end-to-end propagation latency and throughput** through multi-level DAG topologies. While the single-ST benchmarks above measure per-operator refresh speed, these benchmarks measure how efficiently changes propagate through chains, fan-outs, diamonds, and mixed topologies with 5–100+ stream tables.

The core questions these benchmarks answer:

> **How long does it take for a source-table INSERT to propagate through an entire DAG to the leaf stream tables?**
>
> **How does PARALLEL refresh mode compare to CALCULATED mode across different topology shapes?**

### Running DAG Benchmarks

```bash
# Full suite (rebuilds Docker image)
just test-dag-bench

# Skip Docker image rebuild
just test-dag-bench-fast

# Individual topology tests
cargo test --test e2e_dag_bench_tests --features pg18 -- --ignored bench_latency_linear_5 --test-threads=1 --nocapture
cargo test --test e2e_dag_bench_tests --features pg18 -- --ignored bench_throughput_diamond --test-threads=1 --nocapture
```

### Topology Patterns

| Topology | Shape | Description |
|----------|-------|-------------|
| **Linear Chain** | `src → st_1 → st_2 → ... → st_N` | Sequential pipeline; L1 aggregate, L2+ alternating project/filter |
| **Wide DAG** | `src → [W parallel chains × D deep]` | W independent chains of depth D from a shared source; tests parallel refresh mode |
| **Fan-Out Tree** | `src → root → [b children] → [b² grandchildren] → ...` | Exponential fan-out; each parent spawns b children with filter/project variants |
| **Diamond** | `src → [fan-out aggregates] → JOIN → [extension]` | Fan-out to independent aggregates (SUM/COUNT/MAX/MIN/AVG) then converge via JOIN |
| **Mixed** | Two sources, 4 layers, ~15 STs | Realistic e-commerce scenario with chains, fan-out, cross-source joins, and alerts |

### Measurement Modes

**Latency benchmarks** (auto-refresh): The scheduler is enabled with a 200 ms interval. The test INSERTs into the source table and polls `pgt_refresh_history` until the leaf stream table has a new COMPLETED entry. This measures the full propagation latency including scheduler overhead.

**Throughput benchmarks** (manual refresh): The scheduler is disabled. The test applies mixed DML (70% UPDATE, 15% DELETE, 15% INSERT) then manually refreshes all STs in topological order. This isolates pure refresh cost from scheduler overhead.

### Theoretical Comparison

Each latency benchmark computes the theoretical prediction from [PLAN_DAG_PERFORMANCE.md](https://github.com/trickle-labs/pg-trickle/blob/main/plans/performance/PLAN_DAG_PERFORMANCE.md) and reports the delta:

| Mode | Formula |
|------|---------|
| CALCULATED | L = I_s + N × T_r |
| PARALLEL(C) | L = Σ ⌈W_l / C⌉ × max(I_p, T_r) per level |

Where T_r is the measured average per-ST refresh time, I_s = 200 ms (scheduler interval), and C is the concurrency limit.

### Reading the Output

#### Per-Cycle Machine-Parseable Lines (stderr)

```
[DAG_BENCH] topology=linear_chain mode=CALCULATED sts=10 depth=10 width=1 cycle=1 actual_ms=820.3 theory_ms=700.0 overhead_pct=17.2 per_hop_ms=82.0
```

#### ASCII Summary Table (stdout)

```
╔══════════════════════════════════════════════════════════════════════════════════════════════════════╗
║                         pg_trickle DAG Topology Benchmark Results                                 ║
╠═══════════════╤═══════════════╤══════╤═══════╤═══════╤════════════╤════════════╤═══════════════════╣
║ Topology      │ Mode          │ STs  │ Depth │ Width │ Actual ms  │ Theory ms  │ Overhead          ║
╠═══════════════╪═══════════════╪══════╪═══════╪═══════╪════════════╪════════════╪═══════════════════╣
║ linear_chain  │ CALCULATED    │   10 │    10 │     1 │      820.3 │      700.0 │ +17.2%            ║
║ wide_dag      │ PARALLEL_C8   │   60 │     3 │    20 │     2430.1 │     1800.0 │ +35.0%            ║
╚═══════════════╧═══════════════╧══════╧═══════╧═══════╧════════════╧════════════╧═══════════════════╝
```

#### Per-Level Breakdown

```
  Per-Level Breakdown (linear_chain D=10, CALCULATED):
  Level  1: avg  52.3ms  [st_lc_1]
  Level  2: avg  48.7ms  [st_lc_2]
  ...
  Level 10: avg  51.2ms  [st_lc_10]
  Total:       513.5ms  (scheduler overhead: 306.8ms)
```

#### JSON Export

Results are written to `target/dag_bench_results/<timestamp>.json` (overridable via `PGS_DAG_BENCH_JSON_DIR` env var) for cross-run comparison.

### Available DAG Benchmark Tests

#### Latency Tests (Auto-Refresh)

| Test Name | Topology | Mode | STs |
|-----------|----------|------|-----|
| `bench_latency_linear_5_calc` | Linear, D=5 | CALCULATED | 5 |
| `bench_latency_linear_10_calc` | Linear, D=10 | CALCULATED | 10 |
| `bench_latency_linear_20_calc` | Linear, D=20 | CALCULATED | 20 |
| `bench_latency_linear_10_par4` | Linear, D=10 | PARALLEL(4) | 10 |
| `bench_latency_wide_3x20_calc` | Wide, D=3 W=20 | CALCULATED | 60 |
| `bench_latency_wide_3x20_par4` | Wide, D=3 W=20 | PARALLEL(4) | 60 |
| `bench_latency_wide_3x20_par8` | Wide, D=3 W=20 | PARALLEL(8) | 60 |
| `bench_latency_wide_5x20_calc` | Wide, D=5 W=20 | CALCULATED | 100 |
| `bench_latency_wide_5x20_par8` | Wide, D=5 W=20 | PARALLEL(8) | 100 |
| `bench_latency_fanout_b2d5_calc` | Fan-out, b=2 d=5 | CALCULATED | 31 |
| `bench_latency_fanout_b2d5_par8` | Fan-out, b=2 d=5 | PARALLEL(8) | 31 |
| `bench_latency_diamond_4_calc` | Diamond, fan=4 | CALCULATED | 5 |
| `bench_latency_mixed_calc` | Mixed, ~15 STs | CALCULATED | ~15 |
| `bench_latency_mixed_par8` | Mixed, ~15 STs | PARALLEL(8) | ~15 |

#### Throughput Tests (Manual Refresh)

| Test Name | Topology | STs | Delta Sizes |
|-----------|----------|-----|-------------|
| `bench_throughput_linear_5` | Linear, D=5 | 5 | 10, 100, 1000 |
| `bench_throughput_linear_10` | Linear, D=10 | 10 | 10, 100, 1000 |
| `bench_throughput_linear_20` | Linear, D=20 | 20 | 10, 100, 1000 |
| `bench_throughput_wide_3x20` | Wide, D=3 W=20 | 60 | 10, 100, 1000 |
| `bench_throughput_fanout_b2d5` | Fan-out, b=2 d=5 | 31 | 10, 100, 1000 |
| `bench_throughput_diamond_4` | Diamond, fan=4 | 5 | 10, 100, 1000 |
| `bench_throughput_mixed` | Mixed, ~15 STs | ~15 | 10, 100, 1000 |

### What to Look For

1. **Linear chain: CALCULATED faster than PARALLEL.** For width=1 DAGs, PARALLEL adds poll overhead without parallelism benefit. CALCULATED should be faster.

2. **Wide DAG: PARALLEL(C=8) speedup over CALCULATED.** For width ≥ 20, PARALLEL should show measurable improvement — it refreshes up to C STs concurrently per level instead of sequentially.

3. **Overhead < 100%.** Theoretical vs actual overhead should stay below 100% across all topologies — the formulas should be in the right ballpark.

4. **DIFFERENTIAL action in per-ST breakdown.** ST-on-ST hops should show `DIFFERENTIAL` rather than `FULL`, confirming differential propagation is working.

5. **Throughput scaling with delta size.** Smaller deltas (10 rows) should yield lower per-cycle wall-clock time than larger deltas (1000 rows).

---

## In-Process Micro-Benchmarks (Criterion.rs)

In addition to the E2E database benchmarks, the project includes two **Criterion.rs** benchmark suites that measure pure Rust computation time without database overhead. These are useful for tracking performance regressions in the internal query-building and IVM differentiation logic.

### Benchmark Suites

#### `refresh_bench` — Utility Functions

`benches/refresh_bench.rs` benchmarks the low-level helper functions used during refresh operations:

| Benchmark Group | What It Measures |
|----------------|------------------|
| **quote_ident** | PostgreSQL identifier quoting speed |
| **col_list** | Column list SQL generation |
| **prefixed_col_list** | Prefixed column list generation (e.g., `NEW.col`) |
| **expr_to_sql** | AST expression → SQL string conversion |
| **output_columns** | Output column extraction from parsed queries |
| **source_oids** | Source table OID resolution |
| **lsn_gt** | LSN comparison expression generation |
| **frontier_json** | Frontier state JSON serialization |
| **canonical_period** | Interval parsing and canonicalization |
| **dag_operations** | DAG topological sort and cycle detection |
| **xxh64** | xxHash-64 hashing throughput |

#### `diff_operators` — IVM Operator Differentiation

`benches/diff_operators.rs` benchmarks the delta SQL generation for every IVM operator. Each benchmark creates a realistic operator tree and measures `differentiate()` throughput:

| Benchmark Group | What It Measures |
|----------------|------------------|
| **diff_scan** | Table scan differentiation (3, 10, 20 columns) |
| **diff_filter** | Filter (WHERE) differentiation |
| **diff_project** | Projection (SELECT subset) differentiation |
| **diff_aggregate** | GROUP BY aggregate differentiation (simple + complex) |
| **diff_inner_join** | Inner join differentiation |
| **diff_left_join** | Left outer join differentiation |
| **diff_distinct** | DISTINCT differentiation |
| **diff_union_all** | UNION ALL differentiation (2, 5, 10 children) |
| **diff_window** | Window function differentiation |
| **diff_join_aggregate** | Composite join + aggregate pipeline |
| **differentiate_full** | Full `differentiate()` call for scan-only and filter+scan trees |

### Running Micro-Benchmarks

```bash
# Run all Criterion benchmarks
just bench

# Run only refresh utility benchmarks
cargo bench --bench refresh_bench --features pg18

# Run only IVM diff operator benchmarks
just bench-diff
# or equivalently:
cargo bench --bench diff_operators --features pg18

# Output in Bencher-compatible format (for CI integration)
just bench-bencher
```

### Output and Reports

Criterion produces statistical analysis for each benchmark including:

- **Mean** and **standard deviation** of execution time
- **Throughput** (iterations/sec)
- **Comparison with previous run** — reports improvements/regressions with confidence intervals

HTML reports are generated in `target/criterion/` with interactive charts showing distributions and regression history. Open `target/criterion/report/index.html` to browse all results.

Sample output:

```
diff_scan/3_columns   time:   [11.834 µs 12.074 µs 12.329 µs]
diff_scan/10_columns  time:   [16.203 µs 16.525 µs 16.869 µs]
diff_aggregate/simple time:   [21.447 µs 21.862 µs 22.301 µs]
diff_inner_join       time:   [25.919 µs 26.421 µs 26.952 µs]
```

---

## Continuous Benchmarking with Bencher

[Bencher](https://bencher.dev) provides continuous benchmark tracking in CI, detecting performance regressions on pull requests before they merge.

### How It Works

The `.github/workflows/benchmarks.yml` workflow:

1. **On `main` pushes** — runs both Criterion suites and uploads results to Bencher as the baseline. This establishes the expected performance for each benchmark.

2. **On pull requests** — runs the same benchmarks and compares against the `main` baseline using a **Student's t-test** with a 99% upper confidence boundary. If any benchmark regresses beyond the threshold, the PR check fails.

### Setup

To enable Bencher for your fork or deployment:

1. **Create a Bencher account** at [bencher.dev](https://bencher.dev) and create a project.

2. **Add the API token** as a GitHub Actions secret:
   - Go to **Settings → Secrets and variables → Actions**
   - Add `BENCHER_API_TOKEN` with your Bencher API token

3. **Update the project slug** in `.github/workflows/benchmarks.yml` if your Bencher project name differs from `pg-trickle`.

The workflow gracefully degrades — if `BENCHER_API_TOKEN` is not set, benchmarks still run and upload artifacts but skip Bencher tracking.

### Local Bencher-Format Output

To see what Bencher would receive from CI:

```bash
just bench-bencher
```

This runs both suites with `--output-format bencher`, producing JSON output compatible with `bencher run`.

### Dashboard

Once configured, the Bencher dashboard shows:

- **Historical trends** for every benchmark across commits
- **Statistical thresholds** with configurable alerting
- **PR annotations** highlighting which benchmarks regressed and by how much

---

## Troubleshooting

| Issue | Resolution |
|-------|-----------|
| `docker: command not found` | Install Docker Desktop and ensure it is running |
| Container startup timeout | Increase Docker memory allocation (≥ 4 GB recommended) |
| `image not found` | Run `./tests/build_e2e_image.sh` to build the test image |
| Highly variable timings | Close other workloads; use `--test-threads=1` to avoid container contention |
| SLOW status on latency test | Expected in Docker; bare-metal should pass < 10 ms |

---

## CDC Write-Side Overhead Benchmarks

The CDC write-overhead benchmark suite in `tests/e2e_cdc_write_overhead_tests.rs` measures the DML throughput cost of pg_trickle's CDC triggers on source tables. This quantifies the "write amplification factor" — how much slower DML becomes when a stream table is attached.

The core question this benchmark answers:

> **How much write throughput do you sacrifice by attaching a stream table to a source table?**

### Running CDC Write Overhead Benchmarks

```bash
# Full suite (all 5 scenarios)
cargo test --test e2e_cdc_write_overhead_tests --features pg18 -- --ignored --nocapture bench_cdc_write_overhead_full

# Individual scenarios
cargo test --test e2e_cdc_write_overhead_tests --features pg18 -- --ignored --nocapture bench_cdc_single_row_insert
cargo test --test e2e_cdc_write_overhead_tests --features pg18 -- --ignored --nocapture bench_cdc_bulk_insert
cargo test --test e2e_cdc_write_overhead_tests --features pg18 -- --ignored --nocapture bench_cdc_bulk_update
cargo test --test e2e_cdc_write_overhead_tests --features pg18 -- --ignored --nocapture bench_cdc_bulk_delete
cargo test --test e2e_cdc_write_overhead_tests --features pg18 -- --ignored --nocapture bench_cdc_concurrent_writers
```

### Scenarios

| Scenario | Description | Rows per Cycle |
|----------|-------------|----------------|
| **Single-row INSERT** | One `INSERT` statement per row, 1,000 rows total | 1,000 |
| **Bulk INSERT** | Single `INSERT ... SELECT generate_series(...)` | 10,000 |
| **Bulk UPDATE** | Single `UPDATE ... WHERE id <= N` | 10,000 |
| **Bulk DELETE** | Single `DELETE ... WHERE id <= N` | 10,000 |
| **Concurrent writers** | 4 parallel sessions each inserting 5,000 rows | 20,000 total |

### Reading the Output

```
╔═══════════════════════════════════════════════════════════════════════════════════╗
║               pg_trickle CDC Write-Side Overhead Benchmark                       ║
╠═══════════════════════╤═══════════════╤═══════════════╤═════════════════════════╣
║ Scenario              │ Baseline (ms) │ With CDC (ms) │ Write Amplification     ║
╠═══════════════════════╪═══════════════╪═══════════════╪═════════════════════════╣
║ single-row INSERT     │         450.2 │         890.5 │       1.98×             ║
║ bulk INSERT (10K)     │          35.1 │          72.3 │       2.06×             ║
║ bulk UPDATE (10K)     │          48.7 │         105.2 │       2.16×             ║
║ bulk DELETE (10K)     │          22.4 │          51.8 │       2.31×             ║
║ concurrent (4×5K)     │          65.3 │         142.1 │       2.18×             ║
╚═══════════════════════╧═══════════════╧═══════════════╧═════════════════════════╝
```

| Column | Meaning |
|--------|---------|
| **Scenario** | DML pattern being measured |
| **Baseline** | Average wall-clock time with no stream table (no CDC trigger) |
| **With CDC** | Average wall-clock time with an active stream table (CDC trigger fires) |
| **Write Amplification** | `With CDC / Baseline` — how many times slower the write path becomes |

### Machine-Readable Output

```
[CDC_BENCH] scenario=single-row_INSERT baseline_avg_ms=450.2 cdc_avg_ms=890.5 write_amplification=1.98
```

### Interpreting Write Amplification

| Write Amplification | Interpretation |
|----|----------------|
| **1.0–1.5×** | Minimal overhead — triggers add negligible cost. Typical for bulk DML with statement-level triggers. |
| **1.5–2.5×** | Expected range for statement-level CDC triggers. Each DML statement incurs one additional INSERT into the change buffer. |
| **2.5–4.0×** | Moderate overhead — acceptable for most workloads. Common with row-level triggers or single-row DML. |
| **4.0–10×** | High overhead — consider `pg_trickle.cdc_trigger_mode = 'statement'` if using row-level triggers, or reduce DML frequency. |
| **> 10×** | Investigate — may indicate lock contention on the change buffer or pathological trigger interaction. |

### Key Patterns to Look For

1. **Statement-level triggers vs row-level**: Statement-level triggers (the default) should show significantly lower overhead for bulk DML compared to row-level triggers.

2. **Bulk DML advantage**: Bulk INSERT/UPDATE/DELETE should show lower write amplification than single-row INSERT because the trigger fires once per statement, not once per row.

3. **Concurrent writer safety**: The concurrent scenario should complete without deadlocks or errors, and the write amplification should be similar to the serial bulk INSERT case.

4. **DELETE overhead**: DELETE triggers tend to be slightly more expensive than INSERT triggers because the trigger must capture the `OLD` row values.

---

## CI Benchmark Workflows

All benchmark jobs run only on weekly schedule and `workflow_dispatch` — never on PR or push — to avoid blocking the merge gate with long-running tests.

### `e2e-benchmarks.yml` — E2E Benchmark Tracking

Produces the numbers in `README.md` and this document. Each job posts a summary table to the GitHub Actions run page and uploads artifacts at **90-day retention**. Manual dispatch accepts a `job` input (`refresh` \| `latency` \| `cdc` \| `tpch` \| `all`) to re-run a single job.

| Job | Test(s) | README Section | Timeout | `just` command |
|-----|---------|---------------|---------|----------------|
| `bench-refresh` | `bench_full_matrix` | Differential vs Full Refresh | 60 min | `just test-bench-e2e-fast` |
| `bench-latency` | `bench_no_data_refresh_latency` | Zero-Change Latency | 20 min | `just test-bench-e2e-fast` |
| `bench-cdc` | `bench_cdc_trigger_overhead` | Write-Path Overhead | 30 min | `just test-bench-e2e-fast` |
| `bench-tpch` | `test_tpch_performance_comparison` | TPC-H per-query table | 30 min | `just bench-tpch-fast` |

### `ci.yml` — Benchmark Jobs

Criterion micro-benchmarks and DAG topology benchmarks. Run on the **daily** schedule and `workflow_dispatch`.

| Job | Test Suite | What It Measures | Timeout | `just` command |
|-----|-----------|-----------------|---------|----------------|
| `benchmarks` | `benches/refresh_bench.rs`, `benches/diff_operators.rs` | In-process Rust: query building, delta SQL generation (sub-µs) | 20 min | `just bench` |
| `dag-bench-calc` | `e2e_dag_bench_tests` (excl. `par*`) | DAG propagation latency + throughput, CALCULATED mode | 30 min | `just test-dag-bench-fast` |
| `dag-bench-parallel` | `e2e_dag_bench_tests` (`par*`) | DAG propagation with 4–8 parallel workers | 120 min | `just test-dag-bench-fast` |

### `benchmarks.yml` — Bencher Integration (opt-in)

Disabled by default (no scheduled trigger). Re-enable by restoring `push`/`pull_request` triggers and adding a `BENCHER_API_TOKEN` secret. When active, it annotates PRs with regressions detected via Student’s t-test at a 99% upper confidence boundary.

| Job | Test Suite | What It Measures | Tracking |
|-----|-----------|-----------------|----------|
| `benchmark` | `benches/refresh_bench.rs`, `benches/diff_operators.rs` | Same as `ci.yml` `benchmarks` job | Bencher (regression alert on PR) |

### Artifact Retention Summary

| Workflow | Artifact | Retention |
|----------|----------|-----------|
| `e2e-benchmarks.yml` | `bench-{refresh,latency,cdc,tpch}-results` (stdout + JSON) | **90 days** |
| `ci.yml` `benchmarks` | `benchmark-results` (Criterion HTML + JSON) | 7 days |
| `benchmarks.yml` | `criterion-results` (Criterion HTML + JSON) | 7 days |
