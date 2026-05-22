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
refresh produces a well-defined diff of inserted and deleted rows. Exposing
that diff via logical replication means external systems get exactly the same
granular change events that pg_trickle computes internally, without extra work.

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
Stream table refresh (MERGE)
        │
        ▼
  Rows inserted / deleted in stream table storage
        │
        ▼
  PostgreSQL logical replication
        │
        ▼
  Subscribers receive INSERT / DELETE events
  (standard pgoutput protocol)
```

The publication is named `pgt_pub_<stream_table_name>` and is owned by the
same role that created the stream table.

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
SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete
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
`pg_trickle.publication_lag_warn_bytes` (v0.25.0). When a slot exceeds the
configured byte lag:

1. A warning is logged.
2. Change-buffer cleanup is **paused** for that slot until it catches up —
   preventing data loss for slow consumers.

Configure the threshold:

```
pg_trickle.publication_lag_warn_bytes = 67108864   # 64 MB
```

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
- `wal_level = logical` is required. This is not the default in all managed
  PostgreSQL providers — check your provider's documentation.
- Subscribers must be able to handle `INSERT` and `DELETE` events (stream
  tables do not use `UPDATE` — every change is expressed as a delete + insert
  pair in the logical replication stream).

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
