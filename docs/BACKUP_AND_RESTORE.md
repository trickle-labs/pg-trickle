# Backup and Restore

pg_trickle plays nicely with every standard PostgreSQL backup
mechanism — `pg_dump`, `pg_basebackup`, pgBackRest, WAL archiving,
PITR, and pre-built tools like CloudNativePG and Crunchy Operator.
The catalog, change buffers, and stream-table contents are all
ordinary PostgreSQL relations, so they get backed up like anything
else.

This page walks through the recommended workflows, the gotchas, and
how the [Snapshots](SNAPSHOTS.md) API fits in.

> **TL;DR.** Physical backups (pgBackRest, `pg_basebackup`) just
> work. `pg_dump` works too, with one small ordering rule. Snapshots
> are an *application-level* tool for derived state, not a backup
> replacement.

---

## Choosing the right tool

| Tool | Best for | Notes |
|---|---|---|
| **pgBackRest / WAL-G / pg_basebackup** | Production backup & PITR | Full-fidelity; no special pg_trickle steps |
| **`pg_dump` / `pg_restore`** | Logical copies, dev environments, schema migration | Works; restore order matters slightly |
| **Stream-table [snapshots](SNAPSHOTS.md)** | Replica bootstrap, archival of derived state, fast rollback of one stream table | Not a substitute for a real backup |

---

## Physical backups (pgBackRest, pg_basebackup, WAL-G)

Physical backups copy the data directory at the file-system level.
Everything is captured: source tables, stream-table storage, the
`pgtrickle.*` catalog, the `pgtrickle_changes.*` change buffers,
and (in WAL CDC mode) the replication slots' on-disk state.

**Restore procedure:**

1. Restore the data directory exactly as you would for any
   PostgreSQL database.
2. Start PostgreSQL.
3. The pg_trickle launcher discovers each database on the next
   tick (~10 s) and resumes the per-database scheduler.

There is nothing pg_trickle-specific to do.

**Point-in-time recovery (PITR).** PITR works as expected. If you
recover to a point in the middle of a refresh, that refresh is
marked failed in `pgtrickle.pgt_refresh_history` on first start;
the next scheduler tick re-runs it. No data loss.

**WAL CDC slots after restore.** If you were running in
`pg_trickle.cdc_mode = 'wal'` and the restored cluster came up
without the original slots (e.g. a logical-decoding replica that
did not inherit slots), pg_trickle's scheduler detects the absence
and re-bootstraps trigger CDC for the affected sources. You will
see one `WARNING` per source; the system continues to work.

---

## Logical backups (`pg_dump` / `pg_restore`)

`pg_dump` produces a portable SQL script (or directory archive)
that can be replayed into a fresh database. pg_trickle objects are
included automatically because they are normal extension objects.

**The one ordering rule:** restore must follow the standard
PostgreSQL "schema, then data, then constraints/indexes" order.
`pg_restore --section=pre-data --section=data --section=post-data`
does this for you. Avoid hand-editing the dump to interleave
sections.

### Recommended workflow

```bash
# Create the dump (custom or directory format)
pg_dump --format=custom --file=mydb.dump mydb

# Restore into a fresh database
createdb mydb_restored
pg_restore --dbname=mydb_restored --jobs=4 mydb.dump
```

Then, if you want to verify everything came back:

```sql
-- Should list every stream table
SELECT * FROM pgtrickle.pgt_status();

-- Force a refresh on each one to confirm CDC is wired
SELECT pgtrickle.refresh_stream_table(pgt_name)
FROM pgtrickle.stream_tables_info;
```

### What `pg_dump` does and does not capture

| Object | Captured by `pg_dump`? |
|---|---|
| Source tables (your data) | ✅ |
| Stream-table storage (your derived data) | ✅ |
| `pgtrickle.*` catalog rows | ✅ |
| CDC trigger definitions | ✅ (recreated when the extension reapplies them) |
| `pgtrickle_changes.*` change buffers | ✅ — but typically empty after a clean dump |
| WAL replication slots (WAL CDC mode) | ✕ (slots are not dumpable; the scheduler recreates them) |
| Refresh history | ✅ |

If you do not need the audit history, you can shrink the dump with
`pg_dump --exclude-table='pgtrickle.pgt_refresh_history'`.

---

## Stream-table snapshots vs. backups

[Snapshots](SNAPSHOTS.md) are an **application-level**
mechanism for capturing the contents of *one* stream table at a
*chosen* point. They are great for:

- Bootstrapping a replica without re-running a slow full refresh.
- Archiving a slowly-changing dimension daily.
- Rolling one stream table back after a defining-query mistake.

They are **not** a backup of your database. Use them in addition to,
not instead of, pgBackRest / `pg_dump`.

A reasonable production posture:

- Daily pgBackRest backup.
- Snapshots of your most important stream tables on the cadence
  that matches your business RPO.
- WAL retention sized to PITR window.

---

## Backup and restore on Kubernetes (CNPG)

CloudNativePG handles backup orchestration via Barman / object
storage. pg_trickle is fully compatible:

- Use `Cluster.spec.backup` exactly as you would for any other
  PG cluster.
- After a `Cluster.spec.bootstrap.recovery` operation, the
  pg_trickle launcher resumes automatically.
- For very large stream tables, consider taking pre-backup
  snapshots and restoring them on the new cluster to skip an
  initial full refresh.

See [CloudNativePG integration](integrations/cloudnativepg.md).

---

## Disaster-recovery checklist

- [ ] Backup tool of choice configured (pgBackRest / WAL-G / CNPG /
      managed service).
- [ ] WAL retention window ≥ your PITR target.
- [ ] If using WAL CDC: alerting on
      `pg_trickle.slot_lag_critical_threshold_mb`.
- [ ] Periodic snapshot of business-critical stream tables.
- [ ] Documented restore procedure tested at least once
      (snapshot → fresh database → `pg_trickle.health_check()`).
- [ ] Off-site copy of backups (managed service, S3 with
      cross-region replication, etc.).
- [ ] Monitoring on `pg_trickle.pgt_refresh_history` for restore
      drift.

---

**See also:**
[Snapshots](SNAPSHOTS.md) ·
[High Availability and Replication](HA_AND_REPLICATION.md) ·
[CloudNativePG integration](integrations/cloudnativepg.md) ·
[Capacity Planning](CAPACITY_PLANNING.md)
