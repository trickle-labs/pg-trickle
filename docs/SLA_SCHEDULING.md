# SLA-based Smart Scheduling

Instead of manually tuning refresh schedules, you can tell pg_trickle what
your **data freshness requirement** is and let it figure out the rest.

Set a target — "this stream table must never be more than 10 seconds stale"
— and pg_trickle assigns the right scheduling tier, monitors whether it can
meet the target based on real refresh history, and alerts you before an SLA
breach happens.

> **`set_stream_table_sla`, `recommend_schedule`, `predicted_sla_breach` alerts available**

---

## The problem with manual scheduling

A manually configured schedule like `schedule => '5s'` works when your source
tables are quiet, but it can easily become wrong over time:

- Source tables grow → refreshes take longer → the 5-second schedule no
  longer completes in time.
- A brief write surge hits → a single refresh takes 4×  the normal time →
  the SLA is quietly broken with no warning.
- You add a complex JOIN → differential refresh cost jumps → you never notice
  until a user complains about stale data.

SLA-based scheduling solves this by tying the refresh schedule to an
observable outcome (data freshness) instead of an assumed refresh duration.

---

## Quickstart

### Set an SLA on a stream table

```sql
SELECT pgtrickle.set_stream_table_sla('public.order_totals', interval '10 seconds');
```

This does two things immediately:
1. Stores `10000` ms as `freshness_deadline_ms` in the catalog.
2. Assigns a tier based on the SLA value (see [Tier assignment](#tier-assignment)).

pg_trickle will then actively monitor whether each refresh is on track to
meet the target, and alert you if it predicts a breach.

### Check the current SLA

```sql
SELECT pgt_name, freshness_deadline_ms, refresh_tier, staleness
FROM pgtrickle.stream_tables_info
WHERE pgt_name = 'order_totals';
```

---

## Tier assignment

`set_stream_table_sla` maps your freshness target to one of three scheduler
tiers:

| SLA target | Tier assigned | Description |
|-----------|---------------|-------------|
| ≤ 5 seconds | **Hot** | Maximum priority; refreshes as fast as the worker pool allows |
| 6–30 seconds | **Warm** | Standard priority |
| > 30 seconds | **Cold** | Background priority; other tables take precedence |

You can still override the tier manually after setting an SLA:

```sql
-- Force to hot regardless of SLA
SELECT pgtrickle.set_stream_table_tier('public.order_totals', 'hot');
```

---

## Schedule recommendations

Once a stream table has accumulated enough refresh history, pg_trickle can
recommend an optimal schedule based on observed refresh durations using a
median+MAD (Median Absolute Deviation) statistical model.

### Single table recommendation

```sql
SELECT pgtrickle.recommend_schedule('public.order_totals');
```

Returns JSONB:

```json
{
  "recommended_interval_seconds": 3.8,
  "current_interval_seconds": 5.0,
  "delta_pct": -24.0,
  "peak_window_cron": null,
  "confidence": 0.87,
  "reasoning": "median=1247ms mad=183ms p95_estimate=1796ms recommended=2.7s confidence=0.87"
}
```

| Field | Meaning |
|-------|---------|
| `recommended_interval_seconds` | Suggested new schedule, with a 1.5× headroom over p95 refresh duration |
| `current_interval_seconds` | Current configured schedule |
| `delta_pct` | How much the recommendation differs from the current schedule (negative = speed up) |
| `confidence` | 0.0–1.0; reflects how consistent refresh times are; `0.0` means insufficient history |
| `reasoning` | Human-readable explanation of how the recommendation was computed |

### All tables at once

```sql
SELECT name, current_interval_seconds, recommended_interval_seconds, delta_pct, confidence
FROM pgtrickle.schedule_recommendations()
ORDER BY ABS(delta_pct) DESC;
```

This is particularly useful for a periodic review of your entire deployment.
Sort by `delta_pct DESC` to find tables where the schedule is too aggressive
(recommendation is longer → reducing unnecessary CPU cost), or by
`delta_pct ASC` to find tables where the schedule is too relaxed (refresh
is taking too long to stay within SLA).

### Minimum sample threshold

The planner requires at least `pg_trickle.schedule_recommendation_min_samples`
completed refreshes (default: 20) before computing a non-zero confidence score.
Until then, `confidence = 0.0` and the recommendation reflects the last known
full refresh duration. You can lower this during initial setup:

```sql
ALTER SYSTEM SET pg_trickle.schedule_recommendation_min_samples = 10;
SELECT pg_reload_conf();
```

---

## Predictive SLA breach alerts

After every refresh, the scheduler checks whether the predicted next refresh
duration will exceed the stream table's `freshness_deadline_ms` by more than
20%. If so, a `predicted_sla_breach` alert is emitted via `LISTEN/NOTIFY` on
the `pg_trickle_alert` channel.

This gives you advance warning **before** the breach happens — not after.

### Listening for alerts

```sql
LISTEN pg_trickle_alert;
```

A breach alert payload looks like:

```json
{
  "event": "predicted_sla_breach",
  "stream_table": "public.order_totals",
  "predicted_duration_ms": 12800,
  "deadline_ms": 10000,
  "overage_pct": 28.0,
  "timestamp": "2025-04-23T14:32:00Z"
}
```

### Debouncing

To avoid flooding your alerting system during a temporary spike, alerts are
debounced per stream table:

```
pg_trickle.schedule_alert_cooldown_seconds = 300   # 5 minutes (default)
```

Only one `predicted_sla_breach` alert fires per stream table per cooldown
window, even if every refresh during that window predicts a breach.

### Bridging alerts to external systems

See [Monitoring & Alerting](tutorials/MONITORING_AND_ALERTING.md#bridging-to-external-systems)
for examples of routing `pg_trickle_alert` notifications to PagerDuty, Slack,
Prometheus alertmanager, and other systems.

---

## Workflow: setting up SLA-based scheduling from scratch

### 1. Create the stream table with a rough initial schedule

```sql
SELECT pgtrickle.create_stream_table(
    'public.order_totals',
    $$SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id$$,
    schedule => '5s'
);
```

### 2. Let it run for a while to build history

Wait for at least 20 refreshes (typically a minute or two with a 5-second schedule):

```sql
SELECT COUNT(*) FROM pgtrickle.pgt_refresh_history
WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'order_totals')
  AND status = 'COMPLETED';
```

### 3. Set an SLA

```sql
SELECT pgtrickle.set_stream_table_sla('public.order_totals', interval '8 seconds');
```

### 4. Get a data-driven recommendation

```sql
SELECT pgtrickle.recommend_schedule('public.order_totals');
```

### 5. Apply the recommendation

```sql
SELECT pgtrickle.alter_stream_table(
    'public.order_totals',
    p_schedule => '3s'   -- use the recommended value
);
```

### 6. Monitor for predicted breaches

```sql
LISTEN pg_trickle_alert;
```

Or query the alert history:

```sql
SELECT event_type, stream_table, payload, created_at
FROM pgtrickle.pgt_alert_history
WHERE event_type = 'predicted_sla_breach'
ORDER BY created_at DESC
LIMIT 10;
```

---

## Checking current SLA status across all tables

```sql
SELECT
    pgt_name,
    freshness_deadline_ms,
    staleness,
    CASE WHEN staleness > (freshness_deadline_ms || ' milliseconds')::interval
         THEN 'BREACHED' ELSE 'OK' END AS sla_status
FROM pgtrickle.stream_tables_info
WHERE freshness_deadline_ms IS NOT NULL
ORDER BY sla_status DESC, staleness DESC;
```

---

## Removing an SLA

To remove an SLA target without changing the schedule:

```sql
UPDATE pgtrickle.pgt_stream_tables
SET freshness_deadline_ms = NULL
WHERE pgt_name = 'order_totals';
```

No predictive breach alerts will fire after this.

---

## When recommendations have low confidence

A low `confidence` score (< 0.5) means refresh durations are highly variable.
Common causes:

| Cause | Fix |
|-------|-----|
| Not enough history | Wait for more refreshes, or lower `schedule_recommendation_min_samples` |
| Highly variable write load | Widen the prediction window; consider a cron schedule for peak hours |
| Source table growing rapidly | The current schedule may already be too slow; reduce it manually |
| Mix of FULL and DIFFERENTIAL refreshes | Check that the differential threshold is tuned correctly |

---

## See also

- [Tiered Scheduling](tutorials/TIERED_SCHEDULING.md) — manual tier assignment and freeze controls
- [Monitoring & Alerting](tutorials/MONITORING_AND_ALERTING.md) — full NOTIFY-based alerting setup
- [Tuning Refresh Mode](tutorials/tuning-refresh-mode.md) — when to use FULL vs. DIFFERENTIAL
- [SQL Reference: set\_stream\_table\_sla](SQL_REFERENCE.md#pgtrickleset_stream_table_sla)
- [Configuration: schedule\_recommendation\_min\_samples](CONFIGURATION.md#pg_trickleschedule_recommendation_min_samples)
