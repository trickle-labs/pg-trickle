# Operational Rollback Runbook

<!-- U-1 (v0.80.0): Operational rollback runbook — backup requirements, snapshot
     recommendation, restore path, and why downgrades are unsafe. -->

This runbook describes the steps required to roll back a pg_trickle upgrade,
the pre-conditions that must be in place before starting, and why downgrades
are considered unsafe by default.

> **TL;DR**: Take a full base-backup *before* every upgrade.  Downgrading
> after the first refresh on the new version is unsafe and unsupported without
> restoring from that backup.

---

## Table of Contents

1. [Why downgrades are unsafe](#why-downgrades-are-unsafe)
2. [Pre-upgrade backup requirements](#pre-upgrade-backup-requirements)
3. [Recommended snapshot workflow](#recommended-snapshot-workflow)
4. [Restore path (rollback procedure)](#restore-path-rollback-procedure)
5. [Upgrade E2E cutoff policy](#upgrade-e2e-cutoff-policy)

---

## Why downgrades are unsafe

pg_trickle upgrades may make **irreversible forward-only changes** to the
PostgreSQL catalog and the extension's own metadata tables:

### 1. Schema migrations applied at upgrade time

Each version ships a migration SQL file (`sql/pg_trickle--X.Y--X.Z.sql`) that
runs automatically when `ALTER EXTENSION pg_trickle UPDATE` is executed.
Migrations may:
- Add new columns to `pgt_stream_tables`, `pgt_refresh_history`, etc.
- Create new catalog tables.
- Drop columns or rename objects that existed in the previous version.

Running the old version's code against the new schema will fail immediately
or produce silent data corruption.

### 2. WAL decoder and replication slot format

The CDC pipeline writes binary change records into the change-buffer tables
(`pgtrickle_changes.changes_<oid>`).  The record format may change between
versions.  Running the old decoder against records written by the new encoder
will produce garbled deltas.

### 3. Shared memory layout

The background worker uses shared memory segments for counters, ring buffers,
and worker coordination.  The layout is version-specific.  Running mismatched
versions causes crashes or undefined behaviour in the worker pool.

### 4. Stream table frontier state

After the first DIFFERENTIAL refresh on the upgraded version, the LSN
frontier written to `pgtrickle.pgt_stream_tables.last_frontier_lsn` is in
the new format.  The old version cannot read this frontier and will either
refuse to start or perform an incorrect full recompute.

---

## Pre-upgrade backup requirements

**A full base-backup is required before every pg_trickle upgrade.**

At minimum, back up:
1. The entire `pgtrickle` schema (all catalog tables).
2. All `pgtrickle_changes` schema tables (CDC buffer state).
3. All stream table materialized views (`pgtrickle_<pgt_id>_mv` or equivalent).
4. The `pg_trickle` extension control file and `.so` / `.dylib` binary
   (so you can restore the old version).

### Using pg_dump (minimum viable backup)

```bash
# Dump the pgtrickle catalog and change-buffer schemas.
pg_dump \
  --schema=pgtrickle \
  --schema=pgtrickle_changes \
  --schema=pgtrickle_mv \
  -F c -f pg_trickle_pre_upgrade_$(date +%Y%m%d).dump \
  "$DATABASE_URL"
```

### Using filesystem snapshot (recommended)

A filesystem-level snapshot of `$PGDATA` taken while PostgreSQL is in a
**backup-start** state is the only reliable way to guarantee a consistent
restore:

```bash
# 1. Start the backup label.
psql -c "SELECT pg_backup_start('pre_pgtrickle_upgrade')"

# 2. Snapshot the entire PGDATA volume (LVM, cloud snapshot, ZFS send, etc.)
lvcreate --snapshot --size 20G --name pgdata_snap /dev/vg0/pgdata

# 3. Stop the backup label.
psql -c "SELECT pg_backup_stop()"
```

### Using CNPG (CloudNativePG)

If you are running under CNPG, schedule a manual backup before the upgrade:

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Backup
metadata:
  name: pre-upgrade-snapshot
spec:
  cluster:
    name: pg-cluster
  method: barmanObjectStore
```

---

## Recommended snapshot workflow

The recommended workflow before any pg_trickle major version upgrade is:

```
1. Drain traffic to read-only or put the application in maintenance mode.
2. Wait for all background refresh jobs to complete:
     SELECT pgtrickle.drain();
3. Take a filesystem snapshot (see above).
4. Record the current extension version:
     SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle';
5. Apply the upgrade:
     ALTER EXTENSION pg_trickle UPDATE TO '0.80.0';
6. Verify the upgrade succeeded:
     SELECT pgtrickle.version();
     SELECT pgtrickle.version_check();
7. Run health check:
     SELECT * FROM pgtrickle.health_check() WHERE severity != 'OK';
8. Resume traffic.
```

If step 5 or later fails, restore from the snapshot taken in step 3.

---

## Restore path (rollback procedure)

If a rollback is necessary, the only supported path is to **restore from the
pre-upgrade snapshot** taken before step 4 above.

### Step-by-step rollback

```bash
# 1. Stop the PostgreSQL service.
systemctl stop postgresql

# 2. Restore PGDATA from the snapshot.
#    (Exact command depends on your snapshot technology.)
lvconvert --merge /dev/vg0/pgdata_snap
# OR: restore from barman, cloud snapshot, etc.

# 3. Restore the old pg_trickle .so / .dylib binary.
#    If you upgraded the binary, re-install the old package:
apt-get install pg-trickle=0.79.0

# 4. Start PostgreSQL.
systemctl start postgresql

# 5. Verify the version is back to the pre-upgrade version.
psql -c "SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle'"

# 6. Run health check.
psql -c "SELECT * FROM pgtrickle.health_check() WHERE severity != 'OK'"
```

### What you lose

After restoring from a pre-upgrade snapshot:
- All data changes that occurred between the snapshot and the failure are lost.
- All stream table refreshes that occurred between the snapshot and the failure
  are lost — stream tables will re-initialize from the last frontier in the snapshot.
- CDC buffer state is restored to the snapshot point — changes captured after
  the snapshot time but before the snapshot was taken may have been lost
  depending on snapshot consistency.

---

## Upgrade E2E cutoff policy

See [CHANGELOG.md](../CHANGELOG.md) and the individual version release notes
in [roadmap/](../roadmap/) for the Upgrade E2E test cutoff policy.

**Summary**: The upgrade E2E test suite validates migrations from the **last
two released versions** to the current version.  Upgrades from older versions
are not regression-tested but may work — always follow this runbook and take a
backup before attempting them.

Example: v0.80.0 ships upgrade E2E tests for:
- v0.78.0 → v0.80.0
- v0.79.0 → v0.80.0

Upgrades from v0.77.0 and earlier are not covered by automated testing.
