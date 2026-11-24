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

The cost model uses *measured* `last_full_ms` and `last_diff_ms`
together with the *current* change ratio to make a per-refresh
decision.

---

## Inputs the model uses

For each stream table, on each scheduler tick:

| Input | Source |
|---|---|
| `change_ratio_current` | `pending_changes / source_row_count` |
| `last_full_ms` | most recent full refresh duration |
| `last_diff_ms` | most recent differential refresh duration |
| `pending_rows` | size of the change buffer |
| `delta_amplification_factor` | learned multiplier: estimated delta volume given pending changes |
| `cost_model_safety_margin` (GUC) | bias toward FULL or DIFF |

It then computes:

```
predicted_diff_ms = base_diff_overhead
                  + per_row_diff_cost × pending_rows × delta_amplification_factor

predicted_full_ms = last_full_ms × source_growth_factor
```

The execution-time B-4 fallback check reads committed history for the current stream table on each decision. It uses the median duration of up to ten completed differential samples within a factor of two of the current change count, requiring at least three comparable samples, and the median of up to five completed FULL samples. Both windows honor `stat_reset`; an absent comparable sample set falls back to the ordinary ratio heuristic. This deliberately avoids extrapolating fixed overhead or a pathological plan from small batches to large ones. The periodic cost summary remains a monitoring estimate, not the authoritative B-4 decision input. FULL still holds source and target locks; duration alone is not an availability guarantee.

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
| `pg_trickle.cost_model_safety_margin` | `1.20` | Multiplier on the predicted DIFF cost. > 1.0 biases toward FULL. |
| `pg_trickle.differential_max_change_ratio` | `0.50` | Hard cap: never pick DIFF above this ratio. |
| `pg_trickle.adaptive_full_threshold` | `0.50` | Force FULL when change ratio exceeds this (legacy fallback). |
| `pg_trickle.delta_amplification_threshold` | `5.0` | When `delta_volume / pending_changes > T`, prefer FULL. |
| `pg_trickle.max_delta_estimate_rows` | `10000000` | Cap on the model's estimate; above this, prefer FULL. |
| `pg_trickle.planner_aggressive` | `off` | Allow the model to override per-table refresh-mode hints. |

A typical "trust the model" setup:

```sql
ALTER SYSTEM SET pg_trickle.cost_model_safety_margin = '1.0';
ALTER SYSTEM SET pg_trickle.planner_aggressive       = 'on';
SELECT pg_reload_conf();
```

A typical "be conservative" setup (prefer FULL on uncertainty):

```sql
ALTER SYSTEM SET pg_trickle.cost_model_safety_margin       = '1.5';
ALTER SYSTEM SET pg_trickle.differential_max_change_ratio  = '0.30';
```

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

The model will not override a manually-pinned mode unless
`planner_aggressive = on`.

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
