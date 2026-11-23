# Downstream Publications

pg_trickle can expose the live content of any stream table as a PostgreSQL
**logical replication publication**. This lets any tool that understands
PostgreSQL logical replication — Debezium, Kafka Connect, Spark Structured
Streaming, a read replica, a custom consumer — subscribe to stream table
changes in real time, without needing to poll the table or set up a separate
CDC pipeline.

> **Supported feature**

---

## Why use downstream publications?

Stream tables are already the result of incremental view maintenance — every
refresh applies changes to the stream table's storage. Exposing that storage
through logical replication lets external systems consume the resulting SQL
row changes without a separate CDC pipeline. The published events reflect the
storage operations, which are not necessarily the same as pg_trickle's internal
delta representation.

| Use case | Tool |
|----------|------|
| Push stream table changes to Kafka | Debezium, Kafka Connect |
| Replicate to a read replica or standby | PostgreSQL physical/logical replica |
| Build event-driven microservices | Any logical replication consumer |
| Feed a data warehouse incrementally | Spark, Flink, Airbyte |
| Archive change history | Custom WAL consumer |

---

## How it works

When you call `stream_table_to_publication`, pg_trickle creates a standard
PostgreSQL publication named `pgt_pub_<stream_table_name>` that covers the
stream table's underlying storage table.

```
Stream table refresh (MERGE) or reinitialize (TRUNCATE + refill)
        │
        ▼
  Storage INSERT / UPDATE / DELETE / TRUNCATE operations
        │
        ▼
  PostgreSQL logical replication (pgoutput)
        │
        ▼
  Subscribers receive the published storage operations
```

A differential refresh may insert a new group, update an existing group's
values, or delete a group that disappears. Although internal delta processing
may represent replacements as delete/insert pairs, **the replication stream is
not guaranteed to contain delete/insert pairs**: a storage `UPDATE` is emitted
as an `UPDATE`. A full refresh or reinitialization that truncates storage emits
`TRUNCATE`, followed by `INSERT` events as the table is refilled. These events
are subject to the publication's `publish` settings; filtering out `TRUNCATE`
does not make the subscriber's existing rows disappear.

The publication is named `pgt_pub_<stream_table_name>`. The caller must own the
stream table and have `CREATE` on the current database. PostgreSQL creates the
publication with the caller as owner. Stream-table ownership does not grant
database publication privileges.

## Security contract

Publication APIs run in caller context for public PostgreSQL DDL. They do not
borrow the extension owner's authority. The extension keeps the private
binding in the same transaction as the publication change.

Each binding is validated against the live publication identity, owner, stream
relation, and relation set. A renamed, replaced, transferred, or otherwise
changed publication is rejected before the extension changes private state.

The caller needs:

- ownership of the stream table;
- `USAGE` on its schema and `CREATE` on the current database;
- `EXECUTE` on the exact publication function overload.

The caller does not need access to `pgtrickle` catalog tables or
`pgtrickle_changes`.

The binding table stores database-local PostgreSQL OIDs and is intentionally
excluded from extension configuration dumps. Detach downstream publications
before `pg_dump`; after restore, recreate them through this API so pg_trickle
records the new object identities.

---

## Quickstart

### Step 1 — Verify PostgreSQL is configured

Logical replication requires `wal_level = logical` in `postgresql.conf`:

```sql
SHOW wal_level;
-- Should return: logical
```

If it returns `replica` or `minimal`, update `postgresql.conf`:

```
wal_level = logical
```

Then restart PostgreSQL. You also need enough replication slots:

```
max_replication_slots = 10   # at least 1 per subscriber
```

### Step 2 — Create the publication

```sql
SELECT pgtrickle.stream_table_to_publication('public.order_totals');
-- INFO: pg_trickle: created publication 'pgt_pub_order_totals' for stream table 'public.order_totals'
```

This creates the publication immediately. Any subscriber can connect right away.

### Step 3 — Create a subscriber

#### PostgreSQL logical replication subscriber

```sql
-- On a downstream PostgreSQL instance:
CREATE SUBSCRIPTION order_totals_sub
    CONNECTION 'host=primary port=5432 dbname=mydb user=replicator password=secret'
    PUBLICATION pgt_pub_order_totals;
```

#### Debezium (via Kafka Connect)

```json
{
  "name": "order-totals-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "primary",
    "database.port": "5432",
    "database.user": "replicator",
    "database.password": "secret",
    "database.dbname": "mydb",
    "publication.name": "pgt_pub_order_totals",
    "table.include.list": "public.order_totals",
    "plugin.name": "pgoutput"
  }
}
```

#### Kafka Connect (without Debezium)

```json
{
  "name": "order-totals-source",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "publication.name": "pgt_pub_order_totals"
  }
}
```

---

## Checking whether a publication exists

```sql
-- Via pg_trickle catalog
SELECT pgt_name, downstream_publication_name
FROM pgtrickle.pgt_stream_tables
WHERE pgt_name = 'order_totals';

-- Via PostgreSQL catalog
SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete, pubtruncate
FROM pg_publication
WHERE pubname = 'pgt_pub_order_totals';
```

---

## Monitoring subscriber lag

Slow or stalled subscribers can cause the WAL to grow unboundedly. Monitor
replication slot lag:

```sql
SELECT slot_name, database, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag
FROM pg_replication_slots
WHERE slot_name LIKE 'pgt_%'
ORDER BY restart_lsn;
```

pg_trickle also watches subscriber lag automatically via
`pg_trickle.publication_lag_warn_bytes`. When a slot exceeds the
configured byte lag:

1. A warning is logged.
2. Change-buffer cleanup is **paused** for that slot until it catches up —
   preventing data loss for slow consumers.

Configure the threshold:

```
pg_trickle.publication_lag_warn_bytes = 67108864   # 64 MB
```

---

## Declarative deployments with publications

A defining-query change in `create_or_replace_stream_table` may replace the
stream table's **storage relation OID** using an atomic shadow swap. PostgreSQL
publications and subscribers are bound to relation identities, so pg_trickle
**rejects replacement while a downstream publication is attached** instead of
silently migrating a binding or leaving subscribers pointing at old storage.
A configuration-only change or unchanged (including whitespace-only) query
does not require detaching the publication.

To decide before changing anything, call the read-only
`pgtrickle.would_replace_stream_table_storage(name, query)`. It matches the
declarative API's rewrite and change detection, returning `true` only when the
new query would require storage replacement. Invalid changed queries error
instead of returning a misleading result. This example assumes the stream
table already has a publication and the caller owns both objects:

```sql
DO $$
DECLARE
    definition text := $query$
        SELECT grp, sum(value) AS total, count(*) AS n,
               max(value) AS peak
        FROM event GROUP BY grp
    $query$;
    replace_storage boolean;
BEGIN
    replace_storage :=
        pgtrickle.would_replace_stream_table_storage('public.totals', definition);
    IF replace_storage THEN
        PERFORM pgtrickle.drop_stream_table_publication('public.totals');
    END IF;

    PERFORM pgtrickle.create_or_replace_stream_table(
        'public.totals', definition, '1h', 'DIFFERENTIAL');

    IF replace_storage THEN
        PERFORM pgtrickle.stream_table_to_publication('public.totals');
    END IF;
END $$;
```

This leaves the publication attached on routine no-op and config-only deployments.
When re-attachment is necessary, consumers may need a subscription publication
refresh, compatible destination schema, or resnapshot for changed columns;
dropping and recreating a publication is **not** transparent replication of
shadow-table contents. Coordinate the change with subscribers. A preflight is
advisory when concurrent DDL can change the definition before the subsequent
replace.

---

## Removing a publication

```sql
SELECT pgtrickle.drop_stream_table_publication('public.order_totals');
```

Publications are also automatically dropped when the stream table is dropped:

```sql
SELECT pgtrickle.drop_stream_table('public.order_totals');
-- Also drops pgt_pub_order_totals
```

Automatic cleanup is fail-closed: if the recorded publication was renamed,
recreated, transferred, or retargeted, the stream-table drop is rejected until
the binding is reconciled.

---

## Multiple subscribers on the same publication

A single publication can support multiple subscribers (e.g. both Debezium and
a PostgreSQL logical replica). Each subscriber gets its own replication slot
and offset — they progress independently.

```sql
-- One publication, multiple consumers:
-- Consumer 1: Debezium → Kafka
-- Consumer 2: PostgreSQL read replica
-- Consumer 3: Spark Structured Streaming

SELECT pgtrickle.stream_table_to_publication('public.order_totals');
-- All three consumers can subscribe to pgt_pub_order_totals
```

---

## Partitioned stream tables

If your stream table is backed by a partitioned source, pg_trickle
automatically sets `publish_via_partition_root = true` on the publication so
that child partition changes are published under the parent table's identity.
This matches the behaviour of trigger-based CDC and ensures subscribers see a
consistent stream regardless of partitioning scheme.

---

## Permissions

The role that creates or drops a publication must own the stream table and
have the database-level `CREATE` privilege:

```sql
GRANT CREATE ON DATABASE mydb TO stream_owner;
GRANT EXECUTE ON FUNCTION pgtrickle.stream_table_to_publication(text)
    TO stream_owner;
GRANT EXECUTE ON FUNCTION pgtrickle.drop_stream_table_publication(text)
    TO stream_owner;
```

The role consuming the publication needs the `REPLICATION` attribute (or
superuser):

```sql
CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'secret';
```

For Debezium and Kafka Connect, grant SELECT on the stream table too:

```sql
GRANT SELECT ON public.order_totals TO replicator;
```

---

## Limitations

- Only one publication per stream table. Calling `stream_table_to_publication`
  twice returns an error. Use a single publication with multiple subscribers
  instead.
- A query change requiring replacement storage is refused while a downstream
  publication is attached. Use `would_replace_stream_table_storage` to guard
  only the deployments that need detach, replace, and re-attach.
- `wal_level = logical` is required. This is not the default in all managed
  PostgreSQL providers — check your provider's documentation.
- Subscribers must handle `INSERT`, `UPDATE`, and `DELETE` events from
  differential refreshes. For example, a new aggregate group produces an
  `INSERT`, a changed total for an existing group can produce a single
  `UPDATE`, and removing the last source row of a group produces a `DELETE`.
  Do not assume that a changed row is always a delete/insert pair.
- A reinitialization (and any full storage replacement using `TRUNCATE`)
  publishes `TRUNCATE` followed by the refill's `INSERT` events. Make sure the
  publication publishes truncates (the default for `CREATE PUBLICATION`) and
  the consumer supports them. Excluding `truncate` from the publication's
  `publish` option does **not** turn a reinitialization into row-level deletes:
  the subscriber retains stale rows, and refill inserts may conflict with
  existing keys and stop replication.
- For PostgreSQL subscribers, provision a compatible destination table with
  an appropriate primary key or replica identity for `UPDATE` and `DELETE`.
  Verify the publisher's replica identity is suitable for the emitted row
  changes, especially when using a custom logical decoding consumer.
- Prefer **one stream-table publication per destination table**. A replicated
  `TRUNCATE` clears the *entire* destination table, including rows populated
  by other publications; it is not scoped to the originating publication.
  Sharing a destination table requires explicit consumer-side isolation or
  safe handling of `TRUNCATE` and refill.

---

## Relationship to WAL-based CDC

Downstream publications are a separate feature from pg_trickle's own
WAL-based CDC mode. pg_trickle uses WAL internally (when `cdc_mode = 'wal'`)
to capture source table changes — the downstream publication feature exposes
the *output* (stream table) to external consumers.

See [CDC Modes](CDC_MODES.md) for an explanation of how pg_trickle captures
changes from source tables.

---

## See also

- [SQL Reference: stream\_table\_to\_publication](SQL_REFERENCE.md#pgtricklestreamtabletopublication)
- [CDC Modes](CDC_MODES.md) — WAL-based change capture for source tables
- [Prometheus & Grafana integration](integrations/prometheus.md) — monitor replication lag
