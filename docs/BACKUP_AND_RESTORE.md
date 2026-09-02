# Backup and Restore

pg_trickle supports physical PostgreSQL backups directly and validates durable
stream-table state after restore, promotion, and cloning. Logical dumps retain
the durable configuration, but restored database identity, relation OIDs,
frontiers, and CDC infrastructure are not trusted until the v0.92 recovery
checks pass.

This page walks through the recommended workflows, the gotchas, and
how the [Snapshots](SNAPSHOTS.md) API fits in.

> **TL;DR.** Physical backups preserve runtime state, but validate the restored
> capture owner and frontier before accepting writes. Logical restores start
> quarantined when their database identity changes; explicitly adopt the
> database and protected-rebuild affected stream tables. Snapshots are
> provenance-checked derived data, not a backup replacement.

---

## Choosing the right tool

| Tool | Best for | Notes |
|---|---|---|
| **pgBackRest / WAL-G / pg_basebackup** | Production backup & PITR | Full-fidelity; no special pg_trickle steps |
| **`pg_dump` / `pg_restore`** | Source-data copies and schema migration | Stream-table refresh remains disabled after restore; recreate stream tables |
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
3. Before resuming application writes, validate the capture owner and all
   source frontiers:

   ```sql
   SELECT pgtrickle.capture_instance_status();
   SELECT pgtrickle.validate_recovery();
   ```

4. Resume the scheduler only after the report is `SAFE`. A promoted or
   point-in-time-restored cluster may require protected reinitialization when
   the recoverable WAL position is behind a persisted frontier.

**Point-in-time recovery (PITR).** The recovery validator compares each
persisted frontier with the restored WAL position. If the frontier is ahead,
the affected stream table is suspended with `RECOVERY_FRONTIER_UNPROVEN` and
must be protected-rebuilt; it is never replayed from an unproven position.

**WAL CDC slots after restore.** If a WAL slot is missing or has lost required
history, `validate_recovery()` reports `CDC_SLOT_MISSING` or
`CDC_WAL_UNAVAILABLE` and suspends the affected stream tables. Recreate the
capture state and protected-rebuild before resuming; the scheduler does not
silently switch to a new frontier.

---

## Logical backups (`pg_dump` / `pg_restore`)

`pg_dump` produces a portable SQL script (or directory archive). Durable
pg_trickle configuration and the registered dependency/CDC catalog rows are
included according to the extension's `pg_extension_config_dump` policy. Those
rows retain source-cluster identities and are not trusted after restore. The
private window-state registry is excluded, and the current release does not
reconcile the remaining state automatically.

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

Before restore, stop scheduling and avoid application writes. After restore,
the capture owner is checked against the current database identity:

```sql
-- Inspect durable configuration and reconciliation state
SELECT * FROM pgtrickle.pgt_status();
SELECT pgtrickle.capture_instance_status();
SELECT pgtrickle.validate_recovery();
```

If the database identity changed, run the explicit superuser adoption command
and then rebuild every affected stream table:

```sql
SELECT pgtrickle.recover_capture_instance();
SELECT pgtrickle.reinitialize_stream_table('public.my_stream_table');
```

Do not guess from similarly named relations or reuse the source database's
capture slots and frontiers.

### What `pg_dump` does and does not capture

| Object | Captured by `pg_dump`? |
|---|---|
| Source tables (your data) | ✅ |
| Stream-table storage (your derived data) | ✅ |
| Durable `pgtrickle.*` configuration | ✅ |
| Dependency and CDC registries | ✅ — validated before resume |
| CDC trigger definitions | Not a supported resume contract; do not trust after restore |
| `pgtrickle_changes.*` change buffers | Not a supported resume contract; do not trust after restore |
| `pgt_stream_tables.window_strategy` | ✅ |
| `pgt_window_states` rows and private window state | ✕ — derived and excluded; v0.89 production plans create none |
| WAL replication slots (WAL CDC mode) | ✕ — missing slots require protected recovery |
| Refresh history and runtime summaries | ✕ — operational history is excluded |

`pgt_window_states` uses an always-false `pg_extension_config_dump` filter.
Logical restore therefore cannot reuse relation OIDs from the source cluster.
The durable `window_strategy` plan remains available for diagnostics. Every
v0.89 window plan is runtime-disabled, and restored stream-table refresh stays
blocked until identity, CDC infrastructure, and frontier validation succeed.

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
- After a `Cluster.spec.bootstrap.recovery` operation, the pg_trickle launcher
  starts normally, but run `capture_instance_status()` and
  `validate_recovery()` before resuming application writes.
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
- [ ] Documented logical-restore procedure tested with changed relation OIDs,
      explicit ownership adoption, protected reinitialization, and post-restore
      DML.
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
