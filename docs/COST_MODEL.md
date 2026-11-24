# Predictive Cost Model

pg_trickle's `AUTO` refresh mode does not just toggle between FULL
and DIFFERENTIAL by hand-tuned thresholds. It runs a **predictive
cost model** that estimates the expected cost of each mode for the
*next* refresh, given the current change ratio and historical
runtimes, and picks the cheaper one.

This page explains how the model works, the levers you can pull,
and when it is safe to ignore the model and pin a mode by hand.

---

## Why a cost model?

Differential refresh is dramatically faster than FULL refresh — when
the change ratio is small. As the change ratio grows, the *delta
overhead* (computing ΔQ, scanning change buffers, planning the
MERGE) starts to dominate, and at some point a full recomputation
wins.

A static threshold ("switch at 50%") is a reasonable default, but
it is wrong in either direction for many real queries:

- Aggregates with a few groups recompute trivially in FULL — DIFF
  has to do work for nothing.
- Wide joins with selective filters benefit from DIFF even at very
  high change ratios.
- A query whose source has just doubled in size will see different
  trade-offs from yesterday's plan.

The authoritative cost check runs inside differential execution for the
actual CDC window. The scheduler does not independently preempt to FULL.

## Inputs the model uses

- Complete input CDC event counts, not output rows or whole-buffer estimates.
- Recent successful differential durations within a factor of two of the
  current input size, for the same query identity and contract generation.
- The median of the last five eligible completed FULL durations.
- The configured prediction window and minimum sample count (at least three).
- The statistics-reset boundary.

Use the median comparable differential duration directly; it already includes
fixed overhead and query complexity. Do not extrapolate across distant batch
sizes or multiply by a complexity coefficient. Cost-based FULL requires

```
median_diff_ms > median_full_ms × max(1.0, prediction_ratio, cost_model_safety_margin)
```

The default effective margin is 1.5. Missing or incomplete evidence never
authorizes a cost-driven FULL. Configured change-ratio and resource limits,
explicit FULL requests, and correctness recovery remain distinct decisions.
Monitoring summaries are not authoritative decision inputs.

---

## Inspect what the model would do

```sql
-- Recommendation and reasoning for a single stream table
SELECT * FROM pgtrickle.recommend_refresh_mode('order_totals');
-- recommended_mode | reason                           | composite_score
-- DIFFERENTIAL     | change ratio 0.018, est. 22 ms   | 0.31

-- Rolling efficiency: refresh durations vs source-table size
SELECT * FROM pgtrickle.refresh_efficiency('order_totals');

-- Or for the whole catalogue
SELECT pgt_name, recommended_mode, change_ratio, est_diff_ms, est_full_ms
FROM pgtrickle.recommend_refresh_mode_all();
```

---

## Tuning levers

| GUC | Default | Effect |
|---|---|---|
| `pg_trickle.prediction_window` | `60` | Age limit in minutes for differential observations. |
| `pg_trickle.prediction_min_samples` | `5` | Required comparable samples; zero disables cost fallback. |
| `pg_trickle.prediction_ratio` | `1.5` | Minimum duration advantage for elective FULL. |
| `pg_trickle.cost_model_safety_margin` | `0.8` | Additional lower bound on the effective margin. |
| `pg_trickle.differential_max_change_ratio` | `0.15` | Separate configured change-ratio guard; zero disables that guard. |
| `pg_trickle.refresh_strategy` | `auto` | Set to differential to bypass elective ratio and cost fallback. |

Higher margins favor differential execution. FULL still holds source and
target locks, so its duration alone is not an availability guarantee.

---

## When to override the model

There are good reasons to pin a mode manually:

- **Queries the model can't see well.** Holistic aggregates
  (`PERCENTILE_*`, `STRING_AGG`) often plan badly under DIFF; pin
  to FULL.
- **Workloads with large but cheap full refreshes.** Tiny aggregates
  that re-aggregate from a small source — the FULL plan is so
  cheap that the model's DIFF estimate cannot beat it.
- **Predictable bursts.** If you know that 9–10 a.m. is your
  upload window, pre-emptively `ALTER STREAM TABLE … SET refresh_mode
  = 'FULL'` for that window.

Pin a mode with:

```sql
SELECT pgtrickle.alter_stream_table('order_totals', refresh_mode => 'FULL');
```

The FULL mode is explicitly configured. Use the cluster-wide `refresh_strategy = 'differential'` to suppress elective differential-to-FULL fallback; correctness and resource guards still apply.

---

## Verifying the model in production

```sql
-- Compare actual mode vs recommended mode over the last 1000 refreshes
WITH recent AS (
    SELECT pgt_name, refresh_mode, started_at, finished_at,
           EXTRACT(epoch FROM (finished_at - started_at)) * 1000 AS ms
    FROM pgtrickle.pgt_refresh_history
    WHERE started_at > now() - interval '1 hour'
)
SELECT pgt_name,
       refresh_mode,
       AVG(ms) AS avg_ms,
       COUNT(*) AS n
FROM recent
GROUP BY pgt_name, refresh_mode
ORDER BY pgt_name;
```

If a stream table is consistently slow in `AUTO` mode, look at the
distribution of modes chosen. Often the answer is "the model is
flapping" — increase `cost_model_safety_margin` or pin a mode.

---

**See also:**
[Performance Cookbook](PERFORMANCE_COOKBOOK.md) ·
[tuning-refresh-mode](tutorials/tuning-refresh-mode.md) ·
[Configuration – Refresh Performance](CONFIGURATION.md#refresh-performance) ·
[SQL Reference – Diagnostics](SQL_REFERENCE.md#diagnostics)
