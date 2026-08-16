# Snapshots

A **snapshot** is a point-in-time copy of a stream table's contents,
stored as an ordinary PostgreSQL table. Snapshots let you back up
derived state, bootstrap a replica, build deterministic test
fixtures, or compare two refresh runs without having to re-derive
the data.

> **Supported feature.** Snapshot mutation is accepted only for a relation
> recorded in `pgtrickle.pgt_snapshots` with matching OID, provenance token,
> relation kind, schema, and stream-table owner.

---

## Why snapshots?

A stream table's contents are *derived* — pg_trickle can always
recompute them from the source tables. But recomputation is not
free, and there are operational situations where having a frozen
copy is cheaper, safer, or simpler:

- **Replica bootstrap.** When you stand up a new read replica or a
  fresh environment, you can restore from a snapshot in seconds
  instead of waiting for an initial full refresh that may take
  minutes or hours on a large dataset.
- **Point-in-time forensics.** Take a snapshot before a risky
  migration or a suspicious incident; compare it to the live stream
  table later.
- **Test fixtures.** Snapshot a stream table from a representative
  environment and check it into a test database.
- **Cheap rollback.** If a defining-query change goes wrong,
  restore from the most recent snapshot while you investigate.

---

## Quickstart

### Take a snapshot

```sql
SELECT pgtrickle.snapshot_stream_table('order_totals');
-- pgtrickle.snapshot_order_totals_1735689421000
```

The function returns the fully-qualified name of the new snapshot
table. By default snapshots live in the `pgtrickle` schema and are
named `snapshot_<table>_<epoch_ms>`.

Each new snapshot stores an independent provenance token, exact relation OID,
creator role, source `pgt_id`, and compatibility metadata in the catalog.
Names alone are not proof of snapshot identity.

You can choose your own name with the optional second argument:

```sql
SELECT pgtrickle.snapshot_stream_table(
    'order_totals',
    'archive.order_totals_2026_q1'
);
```

### List snapshots

```sql
SELECT * FROM pgtrickle.list_snapshots();
```

Or filter to a single stream table:

```sql
SELECT * FROM pgtrickle.list_snapshots('order_totals');
```

### Restore from a snapshot

```sql
SELECT pgtrickle.restore_from_snapshot(
    'order_totals',                                       -- stream table to restore into
    'pgtrickle.snapshot_order_totals_1735689421000'       -- snapshot table
);
```

After a restore, pg_trickle reinitialises the stream table's frontier
and validates the cataloged snapshot identity before changing storage. A
drop/recreate lookalike, unresolved legacy row, incompatible column layout, or
ownership mismatch is rejected before locking or truncating the stream table.
Logical cluster restore rebinding is separate: it resets restored frontiers and
rebuilds CDC before a protected FULL baseline.

### Drop an old snapshot

```sql
SELECT pgtrickle.drop_snapshot('pgtrickle.snapshot_order_totals_1735689421000');
```

---

## What's in a snapshot

The snapshot table is a plain PostgreSQL heap table with the same
columns as the stream table, **including** the hidden `__pgt_row_id`
column. That is what allows a restore to map snapshot rows back to
their stable identities.

Because the snapshot is an ordinary table, you can:

- Back it up with `pg_dump`, copy it elsewhere with `pg_dump -t`, or
  move it across databases with `\copy`.
- Inspect it freely with regular SQL.
- Add indexes for read-side workloads (the snapshot is independent
  of the live stream table).

---

## Operational patterns

### Periodic archival

```sql
-- Every night, snapshot a slowly-changing dimension
SELECT pgtrickle.snapshot_stream_table(
    'customer_360',
    format('archive.customer_360_%s', to_char(now(), 'YYYY_MM_DD'))
);

-- Keep only the last 30 days
SELECT pgtrickle.drop_snapshot(snapshot_table)
FROM pgtrickle.list_snapshots('customer_360')
WHERE created_at < now() - interval '30 days';
```

### Replica bootstrap

```sql
-- On the source: pg_dump the snapshot
pg_dump -t pgtrickle.snapshot_order_totals_1735689421000 mydb > snap.sql

-- On the replica: restore the snapshot, then reattach
psql replicadb < snap.sql
SELECT pgtrickle.restore_from_snapshot(
    'order_totals',
    'pgtrickle.snapshot_order_totals_1735689421000'
);
```

### Disaster recovery

Combine snapshots with regular `pg_dump` of the source tables.
After a restore, pg_trickle's frontier tracking ensures the stream
table will catch up correctly when CDC resumes.

---

## Caveats

- Snapshots are not coordinated across multiple stream tables. If
  you need a *consistent* view across several stream tables, take
  them inside a single transaction and rely on PostgreSQL's MVCC
  isolation.
- Snapshots do not freeze the source tables. The "as-of" time is
  determined by the most recent refresh of the stream table at the
  moment you take the snapshot.
- A restore reinitialises the frontier — if you want the stream
  table to *replay* changes between the snapshot time and now, the
  source CDC slots / change buffers must still hold those entries.
  Otherwise, expect a full refresh on the next cycle.
- Legacy snapshots whose OID and provenance cannot be proven remain cataloged
  but are read-only until explicitly repaired; an ordinary table with a
  plausible name is never accepted.

---

**See also:**
[Backup & Restore](BACKUP_AND_RESTORE.md) ·
[Replica Bootstrap & PITR Alignment (Patterns)](PATTERNS.md#replica-bootstrap--pitr-alignment-v0270) ·
[SQL Reference – Lifecycle](SQL_REFERENCE.md)
