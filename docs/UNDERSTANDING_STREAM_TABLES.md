# Understanding Your Stream Table

Use the bounded explanation API when a stream table is slower, stale, or
unexpectedly using FULL refreshes:

```sql
SELECT pgtrickle.explain('public.orders_summary');
SELECT pgtrickle.explain_json('public.orders_summary');
```

The text form is for operators; the JSON form keeps numeric values, nullable
unknowns, evidence sources, and sample counts for dashboards.

The snapshot answers eight practical questions:

1. **Refresh mode** — the requested mode and the concrete mode currently used.
   `AUTO → FULL` means admission selected a FULL-only path.
2. **Estimated changed rows** — the pending CDC rows that the next refresh may
   process. `null` means the buffer cannot be inspected safely.
3. **Dominant cost** — the largest exclusive PostgreSQL plan node when a safe
   `EXPLAIN` is available; otherwise the OpTree complexity classifier.
4. **Expected refresh time** — an observed FULL duration or a differential
   per-delta-row estimate multiplied by pending rows. It stays unknown until
   compatible samples exist.
5. **Current lag** — time since the last successful verification, not a
   commit-to-visible freshness guarantee.
6. **Next scheduled refresh** — the configured schedule, `on_commit`, `manual`,
   or an explicit unknown state.
7. **FULL fallback threshold/reason** — the AUTO threshold and the durable
   reason code/detail for the latest FULL rebuild.
8. **Write-path overhead** — reserved for compatible PostgreSQL function-statistics
   evidence; unknown is preferable to an invented estimate.

For cumulative monitoring, prefer `pgtrickle.pg_stat_pgtrickle`. It reads
summary tables rather than scanning refresh history on every scrape. Reset
statistics with `pgtrickle.stat_reset(pgt_id)`; history and operational error
state are retained.

Declared freshness targets are one-time translations, not a v0.90 feedback
controller:

- `target_freshness => '5 seconds'` stores an interval declaration and a
  duration schedule.
- `target_freshness => 'on_commit'` uses IMMEDIATE maintenance.
- `target_freshness => 'manual'` removes the table from scheduler eligibility,
  while authorized manual refreshes remain available.

Choose either `schedule` or `target_freshness` in one create/alter call.
