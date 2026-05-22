# Tutorial: Tiered Scheduling

Tiered scheduling (v0.12.0+) lets you assign refresh priorities to stream
tables using four tiers: **Hot**, **Warm**, **Cold**, and **Frozen**. This
reduces CPU and I/O overhead by refreshing less-critical tables less
frequently.

## When to Use It

- You have many stream tables (50+) and want to reduce scheduler load
- Some tables power real-time dashboards (need hot refresh) while others
  serve weekly reports (can be cold)
- You want to freeze tables during maintenance windows without dropping them

## Tier Overview

| Tier | Multiplier | Effect |
|------|-----------|--------|
| `hot` | 1× | Refresh at the configured schedule (default) |
| `warm` | 2× | Refresh at 2× the configured interval |
| `cold` | 10× | Refresh at 10× the configured interval |
| `frozen` | skip | Never refreshed until manually promoted |

For a stream table with `schedule => '1m'`:

| Tier | Effective Interval |
|------|-------------------|
| hot | 1 minute |
| warm | 2 minutes |
| cold | 10 minutes |
| frozen | never |

> **Note:** Cron-based schedules are **not** affected by the tier
> multiplier. They always fire at the configured cron time.

## Step-by-Step Example

### 1. Enable tiered scheduling

Tiered scheduling is enabled by default. Verify:

```sql
SHOW pg_trickle.tiered_scheduling;
-- Should return: on
```

### 2. Create stream tables with different priorities

```sql
-- Real-time dashboard — stays hot (default)
SELECT pgtrickle.create_stream_table(
    name     => 'live_order_count',
    query    => 'SELECT COUNT(*) AS total FROM orders WHERE status = ''active''',
    schedule => '30s'
);

-- Important but not latency-critical
SELECT pgtrickle.create_stream_table(
    name     => 'daily_revenue',
    query    => 'SELECT DATE_TRUNC(''day'', created_at) AS day, SUM(amount) AS revenue
                 FROM orders GROUP BY 1',
    schedule => '1m'
);

-- Weekly report — rarely queried
SELECT pgtrickle.create_stream_table(
    name     => 'customer_lifetime_value',
    query    => 'SELECT customer_id, SUM(amount) AS lifetime_value
                 FROM orders GROUP BY customer_id',
    schedule => '5m'
);
```

### 3. Assign tiers

```sql
-- live_order_count stays at 'hot' (default) — refreshes every 30s

-- daily_revenue: 2× multiplier → effective interval = 2 minutes
SELECT pgtrickle.alter_stream_table('daily_revenue', tier => 'warm');

-- customer_lifetime_value: 10× multiplier → effective interval = 50 minutes
SELECT pgtrickle.alter_stream_table('customer_lifetime_value', tier => 'cold');
```

### 4. Verify effective schedules

```sql
SELECT pgt_name, schedule, refresh_tier,
       CASE refresh_tier
           WHEN 'hot'  THEN schedule
           WHEN 'warm' THEN schedule || ' ×2'
           WHEN 'cold' THEN schedule || ' ×10'
           WHEN 'frozen' THEN 'never'
       END AS effective
FROM pgtrickle.pgt_stream_tables
ORDER BY refresh_tier;
```

### 5. Freeze a table during maintenance

```sql
-- Freeze before a schema migration
SELECT pgtrickle.alter_stream_table('customer_lifetime_value', tier => 'frozen');

-- ... perform migration ...

-- Promote back when ready
SELECT pgtrickle.alter_stream_table('customer_lifetime_value', tier => 'warm');
```

## Choosing the Right Tier

| Use Case | Recommended Tier |
|----------|-----------------|
| Real-time dashboards, alerting tables | **hot** |
| Operational reports queried hourly | **warm** |
| Weekly/monthly analytics, batch consumers | **cold** |
| Tables under maintenance, seasonal reports | **frozen** |

**Rules of thumb:**

- Start with everything at **hot** (the default). Move tables to **warm**
  or **cold** as you identify which ones can tolerate more staleness.
- **Warm** halves the refresh CPU cost compared to hot.
- **Cold** reduces refresh overhead by 90%.
- Use **frozen** sparingly — changes accumulate in the buffer and will be
  processed when you promote the table back.

## Monitoring Tiers

```sql
-- Check which tables are in which tier
SELECT pgt_name, refresh_tier, status, staleness
FROM pgtrickle.stream_tables_info
ORDER BY refresh_tier, staleness DESC;

-- Find frozen tables (these are NOT being refreshed)
SELECT pgt_name, refresh_tier
FROM pgtrickle.pgt_stream_tables
WHERE refresh_tier = 'frozen';
```

## Troubleshooting

**All tables are frozen and nothing is refreshing:**  
If every stream table is set to `frozen`, the scheduler has nothing to do.
Promote at least one table back to `hot` or `warm`.

**Staleness exceeds expectations for cold tables:**  
Remember that `cold` applies a 10× multiplier. A 5-minute schedule becomes
a 50-minute effective interval. If this is too stale, use `warm` instead.

## Further Reading

- [Configuration — pg_trickle.tiered_scheduling](../CONFIGURATION.md#pg_trickletiered_scheduling)
- [FAQ — Troubleshooting stalled data flow](../FAQ.md#how-do-i-diagnose-stalled-data-flow-through-stream-tables)
