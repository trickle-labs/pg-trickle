# CDC Modes

pg_trickle captures changes from source tables using **Change Data Capture (CDC)**.
Two mechanisms are available: trigger-based CDC and WAL-based logical decoding.
Trigger-based CDC uses statement-level triggers by default and can fall back to
row-level triggers when explicitly configured.

---

## Quick decision guide

| Situation | Recommended mode |
|-----------|-----------------|
| Just getting started / unsure | `auto` (default) — triggers now, upgrades to WAL automatically |
| High-write tables where trigger overhead matters | `auto` or `wal` |
| `wal_level = logical` not available (managed PG, read replica) | `trigger` |
| You want strict control — no automatic transitions | `trigger` or `wal` |
| Per-table override (e.g. one hot table on WAL, rest on triggers) | Pass `cdc_mode` to `create_stream_table` |

---

## How trigger-based CDC works

When you create a stream table, pg_trickle installs trigger-based CDC on every
source table. The default `pg_trickle.cdc_trigger_mode = 'statement'` creates
one `AFTER` statement trigger per DML event with transition tables:

```
AFTER INSERT FOR EACH STATEMENT REFERENCING NEW TABLE AS __pgt_new
AFTER UPDATE FOR EACH STATEMENT REFERENCING OLD TABLE AS __pgt_old NEW TABLE AS __pgt_new
AFTER DELETE FOR EACH STATEMENT REFERENCING OLD TABLE AS __pgt_old
```

Each trigger fires **synchronously within the user's transaction** and bulk
writes one change-buffer row per changed source row to
`pgtrickle_changes.changes_<stable_name>`. The buffer rows are in the same
transaction as the user's change — if the transaction rolls back, the buffer
rows also disappear.

```
User transaction:
  INSERT INTO orders …
   → statement trigger fires
   → INSERT INTO pgtrickle_changes.changes_12345 (action, typed row columns)
  COMMIT
        │
        ▼
  Scheduler picks up buffer rows → computes delta → refreshes stream table
```

**Write-side cost:** statement-level triggers amortize trigger invocation cost
across the whole statement, while still writing one typed change-buffer row per
changed source row. Row-level triggers remain available with
`pg_trickle.cdc_trigger_mode = 'row'` for diagnostic compatibility.

---

## How WAL-based CDC works

WAL-based CDC uses PostgreSQL's built-in logical decoding to capture changes
**asynchronously** from the write-ahead log, eliminating trigger overhead
entirely.

```
User transaction:
  INSERT INTO orders …
  COMMIT  (no trigger overhead)
        │
        ▼
  WAL written to disk
        │
        ▼
  pg_trickle WAL decoder background worker
  calls pg_logical_slot_get_changes()
        │
        ▼
  Decoded changes written to pgtrickle_changes.changes_<oid>
        │
        ▼
  Scheduler refreshes stream table
```

The change capture is decoupled from the user transaction. Users see no added
latency on commits.

**Trade-off:** WAL decoding introduces a small additional replication lag
(typically < 1 second). Changes committed by the user are visible to the
stream table slightly later than with triggers.

### Prerequisites for WAL-based CDC

1. `wal_level = logical` in `postgresql.conf`
2. Sufficient replication slots: `max_replication_slots ≥ (number of tracked source tables) + existing slots`
3. Source table has `REPLICA IDENTITY DEFAULT` (primary key) or `REPLICA IDENTITY FULL`
4. PostgreSQL 18.x (required for the pg_trickle extension)

---

## The `auto` mode: transparent transition

The default `cdc_mode = 'auto'` starts with triggers and automatically upgrades
to WAL-based CDC when the prerequisites are met.

```
TRIGGER ──► TRANSITIONING ──► WAL
   ▲                           │
   └───────── (fallback) ──────┘
```

### Transition lifecycle

1. **TRIGGER** — pg_trickle installs statement-level triggers on the source table by default.
2. When `wal_level = logical` becomes available, pg_trickle starts the transition:
   - Creates a publication (`pgtrickle_cdc_<oid>`) and replication slot (`pgtrickle_<oid>`)
   - Sets the source's CDC state to **TRANSITIONING**
   - Both the trigger and WAL decoder write to the buffer (deduplication happens at refresh)
3. **WAL** — once the WAL decoder confirms it has caught up, the trigger is dropped.
4. **Fallback** — if the transition times out or errors (e.g. `wal_level` reverts to
   `replica`), the slot and publication are dropped and CDC reverts to triggers.

The transition is transparent — stream tables remain current throughout and
there is no window of data loss.

---

## Configuring CDC mode

### Global setting

In `postgresql.conf`:

```
pg_trickle.cdc_mode = 'auto'     # default: start with triggers, upgrade to WAL
pg_trickle.cdc_mode = 'trigger'  # always use triggers; never create replication slots
pg_trickle.cdc_mode = 'wal'      # require WAL; error if wal_level != logical
```

Apply without restart:

```sql
ALTER SYSTEM SET pg_trickle.cdc_mode = 'auto';
SELECT pg_reload_conf();
```

### Per-stream-table override

Override for a single stream table at creation time:

```sql
SELECT pgtrickle.create_stream_table(
    'public.order_totals',
    $$SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id$$,
    cdc_mode => 'wal'    -- force WAL for this table's sources only
);
```

Or after the fact:

```sql
SELECT pgtrickle.alter_stream_table('public.order_totals', p_cdc_mode => 'trigger');
```

The per-table override is stored in `pgtrickle.pgt_stream_tables.requested_cdc_mode`
and takes precedence over the global GUC.

---

## Checking the current CDC mode

```sql
-- Per-stream-table CDC state for all sources
SELECT source_table, cdc_mode, pending_rows, buffer_bytes
FROM pgtrickle.change_buffer_sizes()
ORDER BY source_table;

-- Full health check including WAL lag
SELECT * FROM pgtrickle.check_cdc_health();

-- Which triggers are installed
SELECT source_table, trigger_type, trigger_name, present, enabled
FROM pgtrickle.trigger_inventory()
ORDER BY source_table;
```

`check_cdc_health()` returns one row per source table with:

| Column | Description |
|--------|-------------|
| `source_table` | Qualified source table name |
| `cdc_mode` | Current effective mode: `trigger`, `wal`, or `transitioning` |
| `slot_lag_bytes` | WAL slot lag (NULL for trigger mode) |
| `slot_lag_warn` | `true` if lag exceeds `publication_lag_warn_bytes` |
| `alert` | Human-readable status / warning message |

---

## Enabling WAL-based CDC

If you start with `cdc_mode = 'trigger'` and later want to switch to WAL:

### Step 1 — Configure PostgreSQL

```
# postgresql.conf
wal_level = logical
max_replication_slots = 20    # allow enough slots for all tracked sources
```

**Requires a PostgreSQL restart:**

```bash
pg_ctl restart -D $PGDATA
```

### Step 2 — Set the GUC

```sql
ALTER SYSTEM SET pg_trickle.cdc_mode = 'auto';
SELECT pg_reload_conf();
```

pg_trickle will automatically begin transitioning existing stream tables to
WAL-based CDC over the next few scheduler ticks. No manual intervention is
needed per stream table.

### Step 3 — Monitor the transition

```sql
SELECT source_table, cdc_mode FROM pgtrickle.check_cdc_health();
```

Tables will cycle through `trigger` → `transitioning` → `wal` over the next
1–2 minutes depending on write volume.

---

## Reverting to trigger-based CDC

To revert globally:

```sql
ALTER SYSTEM SET pg_trickle.cdc_mode = 'trigger';
SELECT pg_reload_conf();
```

pg_trickle will drop all CDC replication slots and publications on the next
scheduler tick and reinstall trigger-based CDC. Stream tables remain current
throughout — the transition is safe.

To revert a single table:

```sql
SELECT pgtrickle.alter_stream_table('public.order_totals', p_cdc_mode => 'trigger');
```

---

## Trigger mode details

### Statement-level vs. row-level triggers

By default, pg_trickle uses statement-level `AFTER` triggers with transition
tables. This avoids firing the PL/pgSQL trigger body once per row on bulk DML
while still recording each changed source row in the change buffer. Row-level
triggers are available if you need the legacy behavior:

```
pg_trickle.cdc_trigger_mode = 'statement'   # default
pg_trickle.cdc_trigger_mode = 'row'         # legacy/diagnostic mode
```

Note: `cdc_trigger_mode` is ignored when WAL-based CDC is active.

### REPLICA IDENTITY and triggers

Trigger-based CDC captures the full `NEW` and `OLD` row. For `DELETE` and
`UPDATE` to capture the old row values, the source table needs a primary key
or `REPLICA IDENTITY FULL`. Without a primary key, pg_trickle detects this
and may fall back to full refresh for affected stream tables.

---

## WAL mode details

### Replication slot naming

Each tracked source table gets its own replication slot:

```
pgtrickle_<source_table_oid>
```

And a publication:

```
pgtrickle_cdc_<source_table_oid>
```

These are internal to pg_trickle and should not be modified manually.

### Slot lag management

If a subscriber (or pg_trickle itself) falls behind, the replication slot
holds WAL on disk until it is consumed. This can grow unboundedly if pg_trickle
is stopped for an extended period.

pg_trickle monitors slot lag and warns when it exceeds
`pg_trickle.publication_lag_warn_bytes` (default: 64 MB). In `auto` mode,
change-buffer cleanup is paused for lagging slots to prevent data loss.

If a slot grows dangerously large while pg_trickle is down, you can drop and
recreate it:

```sql
-- 1. Temporarily switch to trigger mode
ALTER SYSTEM SET pg_trickle.cdc_mode = 'trigger';
SELECT pg_reload_conf();

-- 2. Manually drop the stale slot if needed
SELECT pg_drop_replication_slot('pgtrickle_12345');

-- 3. Switch back to auto (pg_trickle recreates the slot)
ALTER SYSTEM SET pg_trickle.cdc_mode = 'auto';
SELECT pg_reload_conf();
```

### Partitioned source tables

WAL-based CDC for partitioned tables uses `publish_via_partition_root = true`
so that child partition changes are published under the parent table name.
This matches trigger-mode behaviour and ensures the stream table sees a
unified change stream.

If a table is converted to partitioned after CDC is set up, pg_trickle detects
the inconsistency on the next health check and rebuilds the publication with
the correct setting automatically.

---

## Monitoring slot lag in Prometheus

If you use the [Prometheus & Grafana integration](integrations/prometheus.md),
pg_trickle exports per-source slot lag as:

```
pgtrickle_replication_slot_lag_bytes{slot_name="pgtrickle_12345", source_table="orders"}
```

Set an alert at 80% of your disk space budget for WAL retention.

---

## Performance comparison

| | Trigger | WAL |
|--|---------|-----|
| Write-side overhead | ~2–15 µs per row | Zero (async) |
| Change latency | Sub-millisecond | Up to ~1 second |
| Prerequisites | None | `wal_level = logical`, replication slot |
| Works on managed PG (e.g. RDS without logical replication) | Yes | No |
| Works on physical read replicas | No | No |
| Handles bulk inserts efficiently | Statement mode optional | Yes (batch decoded) |
| Replication slot disk usage | None | Yes — grows if consumer lags |

---

## Troubleshooting

### Trigger CDC: changes not appearing

1. Verify triggers are installed:
   ```sql
   SELECT * FROM pgtrickle.trigger_inventory() WHERE NOT present OR NOT enabled;
   ```
2. If missing, rebuild:
   ```sql
   SELECT pgtrickle.rebuild_cdc_triggers('public.source_table');
   ```

### WAL CDC: slot not advancing

1. Check slot lag:
   ```sql
   SELECT slot_name, active, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS lag_bytes
   FROM pg_replication_slots WHERE slot_name LIKE 'pgtrickle_%';
   ```
2. Check that the pg_trickle background worker is running:
   ```sql
   SELECT * FROM pg_stat_activity WHERE application_name LIKE 'pg_trickle%';
   ```
3. Check `pg_trickle.cdc_mode` is set to `'auto'` or `'wal'`.

### Stuck in TRANSITIONING state

If a source table stays in `transitioning` for more than a few minutes:

```sql
SELECT source_table, cdc_mode FROM pgtrickle.check_cdc_health();
```

The transition has a timeout (`wal_transition_timeout`, default: 300 s). After
the timeout it falls back to triggers automatically. If it keeps failing:

1. Check `wal_level = logical` is still set.
2. Check `max_replication_slots` has not been exceeded.
3. Force revert: `ALTER SYSTEM SET pg_trickle.cdc_mode = 'trigger'`.

---

## See also

- [Configuration: pg\_trickle.cdc\_mode](CONFIGURATION.md#pg_tricklecdc_mode)
- [Architecture Overview](ARCHITECTURE.md) — CDC architecture and WAL decoder design
- [Downstream Publications](PUBLICATIONS.md) — expose stream table output via logical replication
- [Tutorials: What Happens on INSERT](tutorials/WHAT_HAPPENS_ON_INSERT.md) — trigger-mode CDC deep dive
