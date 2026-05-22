# Tutorial: Tuning Refresh Mode

This tutorial walks you through using pg_trickle's built-in diagnostics to
determine whether your stream tables are running in the most efficient
refresh mode (FULL vs DIFFERENTIAL), and how to act on the recommendations.

## Prerequisites

- pg_trickle or later
- At least one stream table with several completed refresh cycles (the
  diagnostics become more accurate with more history)

## Step 1: Check Current Refresh Efficiency

Start by reviewing how your stream tables are performing with their current
refresh mode:

```sql
SELECT pgt_name, refresh_mode, diff_count, full_count,
       avg_diff_ms, avg_full_ms, diff_speedup
FROM pgtrickle.refresh_efficiency();
```

Example output:

| pgt_name | refresh_mode | diff_count | full_count | avg_diff_ms | avg_full_ms | diff_speedup |
|----------|-------------|-----------|-----------|------------|------------|-------------|
| order_totals | DIFFERENTIAL | 142 | 3 | 12.4 | 850.2 | 68.6x |
| user_stats | FULL | 0 | 145 | — | 320.1 | — |
| daily_metrics | DIFFERENTIAL | 98 | 47 | 425.8 | 410.3 | 1.0x |

Key observations:
- **order_totals**: DIFFERENTIAL is 68× faster — this is a great fit.
- **user_stats**: Running in FULL mode with no DIFFERENTIAL history — worth
  checking if DIFFERENTIAL would be faster.
- **daily_metrics**: DIFFERENTIAL and FULL take about the same time (1.0×
  speedup). FULL might actually be simpler and more predictable here.

## Step 2: Get Recommendations

Use `recommend_refresh_mode()` to get AI-weighted recommendations:

```sql
SELECT pgt_name, current_mode, recommended_mode, confidence, reason
FROM pgtrickle.recommend_refresh_mode();
```

Example output:

| pgt_name | current_mode | recommended_mode | confidence | reason |
|----------|-------------|-----------------|-----------|--------|
| order_totals | DIFFERENTIAL | KEEP | high | DIFFERENTIAL is 68.6× faster than FULL with low latency variance |
| user_stats | FULL | DIFFERENTIAL | medium | Query is simple (no complex joins), change ratio is low (2.1%), target table is large |
| daily_metrics | DIFFERENTIAL | FULL | medium | DIFFERENTIAL shows no speedup over FULL (1.0×); high latency variance (p95/p50 = 4.2) suggests unstable performance |

For a single table with full signal details:

```sql
SELECT recommended_mode, confidence, reason,
       jsonb_pretty(signals) AS signal_details
FROM pgtrickle.recommend_refresh_mode('daily_metrics');
```

## Step 3: Understand the Signals

The `signals` JSONB column contains the detailed breakdown of all seven
weighted signals that contributed to the recommendation:

```json
{
  "composite_score": -0.22,
  "signals": [
    { "name": "change_ratio_avg", "score": -0.1, "weight": 0.30 },
    { "name": "empirical_timing", "score": -0.3, "weight": 0.35 },
    { "name": "change_ratio_current", "score": -0.2, "weight": 0.25 },
    { "name": "query_complexity", "score": 0.0, "weight": 0.10 },
    { "name": "target_size", "score": 0.1, "weight": 0.10 },
    { "name": "index_coverage", "score": 0.0, "weight": 0.05 },
    { "name": "latency_variance", "score": -0.4, "weight": 0.05 }
  ]
}
```

Positive scores favour DIFFERENTIAL; negative scores favour FULL. A
composite score above +0.15 recommends DIFFERENTIAL; below −0.15
recommends FULL; in between, the current mode is near-optimal (KEEP).

**Why ±0.15?**  The thresholds create a *dead zone* between −0.15 and +0.15
where the engine considers the two modes equivalent.  Without this dead zone,
small fluctuations in the `change_ratio` signal would cause the engine to
oscillate between FULL and DIFFERENTIAL every few cycles — burning scheduling
overhead with no net benefit.  The +0.15 threshold means DIFFERENTIAL needs
a clear edge (roughly a 15% advantage in combined signal weight) before the
engine switches away from FULL, and vice versa.

You can widen or narrow the dead zone:

```sql
-- Wider dead zone (less switching) — good for stable, predictable workloads
ALTER SYSTEM SET pg_trickle.cost_model_safety_margin = 0.25;
SELECT pg_reload_conf();

-- Narrower dead zone (faster mode switching) — good for highly variable workloads
ALTER SYSTEM SET pg_trickle.cost_model_safety_margin = 0.05;
SELECT pg_reload_conf();
```

The default is `0.15`.  If you see frequent mode oscillation in
`pgtrickle.pgt_refresh_history`, increase the margin.

**Confidence levels:**

| Level | Meaning |
|-------|---------|
| `high` | 10+ completed refresh cycles; strong signal agreement |
| `medium` | 5–10 cycles or mixed signals |
| `low` | Fewer than 5 cycles; recommendation is speculative |

## Step 4: Apply the Recommendation

If you decide to follow a recommendation, use `ALTER STREAM TABLE`:

```sql
-- Switch daily_metrics from DIFFERENTIAL to FULL
SELECT pgtrickle.alter_stream_table('daily_metrics',
    refresh_mode => 'FULL'
);
```

Or switch a table to DIFFERENTIAL:

```sql
-- Switch user_stats to DIFFERENTIAL mode
SELECT pgtrickle.alter_stream_table('user_stats',
    refresh_mode => 'DIFFERENTIAL'
);
```

The change takes effect on the next refresh cycle. No data is lost during
the transition.

## Step 5: Monitor After the Change

After switching modes, wait for several refresh cycles and re-check:

```sql
-- Wait a few minutes, then re-check efficiency
SELECT pgt_name, refresh_mode, diff_count, full_count,
       avg_diff_ms, avg_full_ms, diff_speedup
FROM pgtrickle.refresh_efficiency()
WHERE pgt_name = 'daily_metrics';
```

Run the recommendation function again to verify the change was beneficial:

```sql
SELECT recommended_mode, confidence, reason
FROM pgtrickle.recommend_refresh_mode('daily_metrics');
```

If the recommendation now says KEEP, the new mode is working well.

## Common Scenarios

### High-cardinality aggregates

Stream tables with `SUM`/`COUNT`/`AVG` over high-cardinality GROUP BY keys
(1000+ groups) are almost always better in DIFFERENTIAL mode. pg_trickle
warns about low-cardinality groups at creation time (DIAG-2).

### Small tables with frequent full rewrites

If the source table is small (< 10,000 rows) and changes affect > 30% of
rows per cycle, FULL refresh is often faster because it avoids the overhead
of change tracking and delta application.

### Complex multi-join queries

Queries with 4+ JOINs may have high DIFFERENTIAL overhead due to the
delta propagation rules. If `diff_speedup` is below 2×, consider FULL mode.

### Tables with volatile functions

Stream tables using volatile functions (e.g., `now()`, `random()`) must use
FULL mode. pg_trickle rejects volatile functions in DIFFERENTIAL mode at
creation time.

## See Also

- [SQL Reference: recommend_refresh_mode()](../SQL_REFERENCE.md) — Full
  function documentation
- [SQL Reference: refresh_efficiency()](../SQL_REFERENCE.md) — Efficiency
  metrics documentation
- [Configuration: agg_diff_cardinality_threshold](../CONFIGURATION.md) —
  Cardinality warning threshold
- [DVM Operators](../DVM_OPERATORS.md) — Full operator support matrix
