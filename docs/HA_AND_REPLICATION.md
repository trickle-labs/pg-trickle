# High Availability and Replication

This page covers running pg_trickle in production with PostgreSQL
replication: how stream tables behave on physical (streaming)
replicas, on logical replicas, during failover, and across
read-write splits.

> Looking for **backups** instead? See
> [Backup & Restore](BACKUP_AND_RESTORE.md). Looking for
> **disaster-recovery snapshots**? See [Snapshots](SNAPSHOTS.md).

---

## Quick answers

| Question | Answer |
|---|---|
| Can I run pg_trickle on a physical replica? | The extension can be installed, but the **scheduler does not refresh on a hot standby**. Stream tables on the standby reflect what was promoted from the primary. |
| Will my stream tables survive a failover? | Yes — they are ordinary heap tables. On the new primary, the scheduler resumes from the last persisted frontier. |
| Can I logically replicate a stream table? | Yes — including the hidden `__pgt_row_id` column. See [Downstream Publications](PUBLICATIONS.md). |
| Does pg_trickle work on a logical replica? | Yes — the replica is its own database with its own pg_trickle scheduler. |
| What about CNPG (Kubernetes)? | Fully supported; see [CloudNativePG integration](integrations/cloudnativepg.md). |

---

## Physical (streaming) replication

PostgreSQL's streaming replication ships WAL from primary to
replica. Stream tables, change buffers, and the pg_trickle catalog
are all WAL-logged, so they are byte-for-byte identical on the
replica.

**On the primary:** the scheduler runs and refreshes as normal.

**On the replica (hot standby):** the scheduler **does not refresh**
— the database is read-only. Stream-table contents are exactly the
contents that were on the primary at the replica's replay LSN. Reads
from the replica are perfectly valid; writes (and refreshes) are
not.

After a failover:

1. The new primary's pg_trickle launcher detects the database is
   writable.
2. The scheduler resumes refreshing from the last persisted
   frontier.
3. Any change-buffer rows that arrived between the last refresh
   and the failover are processed in the next cycle.

**Recommended GUCs on a streaming-replica role you might promote:**

```ini
shared_preload_libraries = 'pg_trickle'
pg_trickle.enabled       = on    # safe to leave on; refresh is gated by writability
```

---

## Logical replication

A logical replica is a separate database that subscribes to one or
more publications on the primary. Each logical replica has its own
pg_trickle catalog and its own scheduler.

This makes logical replication a **good answer for an analytics
replica**: replicate the source tables to the analytics replica,
install pg_trickle there, and define your stream tables on the
replica. Heavy DIFFERENTIAL workloads are isolated from the OLTP
primary.

```
┌──────────────┐        logical                 ┌────────────────────┐
│   primary    │  publication: source tables    │  analytics replica │
│   (writes)   │ ─────────────────────────────▶ │  (heavy STs here)  │
└──────────────┘                                 └────────────────────┘
```

Stream tables on the analytics replica are independent of any
stream tables on the primary.

---

## Replicating stream tables themselves

If you want a *downstream* system to receive stream-table changes
(another PostgreSQL, Debezium, Kafka, …), use
[downstream publications](PUBLICATIONS.md):

```sql
SELECT pgtrickle.stream_table_to_publication('order_totals');
```

The publication exposes the storage table's `INSERT`, `UPDATE`, and
`DELETE` operations, including the `__pgt_row_id` column. A reinitialize
also sends `TRUNCATE` followed by refill `INSERT` events. Subscribers
receive the materialised data only — they do not need to know that
pg_trickle is generating it. Give each destination table its own
stream-table publication unless the consumer explicitly handles the
table-wide effects of `TRUNCATE`; see the
[publication limitations](PUBLICATIONS.md#limitations).

---

## Failover behaviour in detail

When the primary fails and a replica is promoted:

| Stage | What happens |
|---|---|
| Promotion | Replica becomes writable; its `pg_trickle` launcher detects this. |
| Scheduler restart | Scheduler resumes from the last persisted frontier (catalog row). |
| Change-buffer rows | Any rows captured before the failover are still in the buffers (they're WAL-logged). They are processed in the next refresh. |
| In-flight refresh | An interrupted refresh is marked failed in `pgt_refresh_history` and retried automatically (subject to the fuse). |
| WAL CDC slots | If using WAL CDC, the slots exist on the replica (slots are WAL-logged in PG ≥ 17 with `failover_slots`). On older versions, the scheduler recreates them and falls back briefly to triggers. |

---

## CNPG (Kubernetes) specifics

- Use the OCI extension image: `ghcr.io/trickle-labs/pg_trickle-ext:<version>`.
- CNPG's standby cluster topology is supported — pg_trickle behaves
  exactly as on bare-metal streaming replication.
- `Cluster.spec.postgresql.shared_preload_libraries` must include
  `pg_trickle`.
- Use `Cluster.spec.postgresql.parameters` for `pg_trickle.*` GUCs.

Full example: see
[integrations/cloudnativepg.md](integrations/cloudnativepg.md) and
the [`cnpg/`](https://github.com/trickle-labs/pg-trickle/tree/main/cnpg)
directory in the repository.

---

## Read-write splits with PgBouncer

pg_trickle's background workers connect directly to PostgreSQL,
not through a pooler. Your application can use PgBouncer (in
transaction-pool mode, including Supabase / Railway / Neon) freely.

For a read-only replica behind a pooler:

- Reads from stream tables work exactly as reads from any table.
- `IMMEDIATE` stream tables only update on the primary; on the
  replica they reflect what's been replayed.

See [PgBouncer integration](integrations/pgbouncer.md) for tuning.

---

## Geographic / cross-region replication

The recommended pattern for cross-region:

1. Stream tables on the **regional primary** (low-latency CDC).
2. [Downstream publications](PUBLICATIONS.md) for the materialised
   results.
3. PostgreSQL logical replication carries them to the remote
   region.
4. Optional: pg_trickle in the remote region builds further
   stream tables on the replicated data.

This keeps the heavy DIFFERENTIAL maintenance close to its source
data, and ships only the final materialised diffs over the WAN.

---

## Caveats

- pg_trickle does **not** participate in synchronous replication
  decisions. It is data, not infrastructure.
- A logical replica that subscribes to source tables but
  *not* to the pg_trickle catalog will need to define its own
  stream tables — they are not auto-created.
- Promoting a standby with a stale `pgtrickle_changes` schema (e.g.
  after a long replication lag) is fine; the next refresh catches
  up. If the lag was very long, the model may pick FULL instead of
  DIFFERENTIAL for the catch-up — by design.

---

**See also:**
[Backup & Restore](BACKUP_AND_RESTORE.md) ·
[Snapshots](SNAPSHOTS.md) ·
[Downstream Publications](PUBLICATIONS.md) ·
[CloudNativePG integration](integrations/cloudnativepg.md) ·
[PgBouncer integration](integrations/pgbouncer.md)
