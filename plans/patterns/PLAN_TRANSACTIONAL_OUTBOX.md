# Transactional Outbox Pattern with pg_trickle

> **Status:** Research Report  
> **Created:** 2026-04-17  
> **Category:** Integration Pattern

---

## Table of Contents

- [Executive Summary](#executive-summary)
- [What Is the Transactional Outbox Pattern?](#what-is-the-transactional-outbox-pattern)
- [The Problem It Solves](#the-problem-it-solves)
- [How pg_trickle Enables the Transactional Outbox](#how-pg_trickle-enables-the-transactional-outbox)
  - [Architecture Overview](#architecture-overview)
  - [Approach 1: Stream Table as the Outbox View](#approach-1-stream-table-as-the-outbox-view)
  - [Approach 2: CDC Buffer as the Outbox](#approach-2-cdc-buffer-as-the-outbox)
  - [Approach 3: IMMEDIATE Mode for Zero-Lag Outbox](#approach-3-immediate-mode-for-zero-lag-outbox)
- [Message Relay Strategies](#message-relay-strategies)
  - [Strategy A: Polling Publisher via Stream Table](#strategy-a-polling-publisher-via-stream-table)
  - [Strategy B: LISTEN/NOTIFY + Stream Table](#strategy-b-listennotify--stream-table)
  - [Strategy C: Transaction Log Tailing via WAL CDC](#strategy-c-transaction-log-tailing-via-wal-cdc)
- [Worked Example: Order Service Outbox](#worked-example-order-service-outbox)
- [Complementary PostgreSQL Extensions](#complementary-postgresql-extensions)
  - [pgmq — PostgreSQL Message Queue](#pgmq--postgresql-message-queue)
  - [pg_amqp — AMQP Publishing from PostgreSQL](#pg_amqp--amqp-publishing-from-postgresql)
  - [pgflow — Durable Workflow Engine](#pgflow--durable-workflow-engine)
  - [Debezium — External CDC Platform](#debezium--external-cdc-platform)
  - [NATS / JetStream — Lightweight Messaging Fabric](#nats--jetstream--lightweight-messaging-fabric)
- [Potential pg_trickle Extensions](#potential-pg_trickle-extensions)
  - [Extension 1: Outbox Table Helper](#extension-1-outbox-table-helper)
  - [Extension 2: Message Relay Background Worker](#extension-2-message-relay-background-worker)
  - [Extension 3: pgmq Integration](#extension-3-pgmq-integration)
  - [Extension 4: Webhook Dispatcher](#extension-4-webhook-dispatcher)
- [Design Considerations](#design-considerations)
- [Event Sourcing vs. Transactional Outbox](#event-sourcing-vs-transactional-outbox)
- [CloudEvents Standard](#cloudevents-standard)
- [Exactly-Once Semantics — Clarified](#exactly-once-semantics--clarified)
- [Schema Evolution & Message Versioning](#schema-evolution--message-versioning)
- [Observability & Distributed Tracing](#observability--distributed-tracing)
- [Testing Strategies](#testing-strategies)
- [Saga Pattern & Compensating Transactions](#saga-pattern--compensating-transactions)
- [Multi-Tenancy](#multi-tenancy)
- [Cost Analysis](#cost-analysis)
- [Security Considerations](#security-considerations)
- [Disaster Recovery & Backup-Restore](#disaster-recovery--backup-restore)
- [Operational Runbooks](#operational-runbooks)
- [Migration Guide](#migration-guide)
- [Comparison with Traditional Approaches](#comparison-with-traditional-approaches)
- [References](#references)

---

## Executive Summary

The **Transactional Outbox Pattern** guarantees that domain events (messages)
are published if and only if the originating database transaction commits. It
decouples services in a microservice architecture while preventing data
inconsistency caused by partial failures between the database and a message
broker.

pg_trickle is a natural fit for implementing this pattern because:

1. **CDC triggers already capture every DML change** atomically within the
   source transaction — no additional outbox trigger is needed.
2. **Stream tables can materialize the "pending events" view** with sub-second
   latency via DIFFERENTIAL refresh.
3. **IMMEDIATE mode** can provide zero-lag outbox materialization within the
   same transaction.
4. **Refresh alerts** on `pgtrickle_alert` can wake a relay after a scheduled
   refresh. The scheduler polls; CDC capture does not send write-time NOTIFY.
5. **WAL-based CDC** can act as a built-in transaction log tailer, avoiding
   external tools like Debezium for simple topologies.

---

## What Is the Transactional Outbox Pattern?

In a microservice architecture, a service often needs to:

1. Update its own database (e.g., create an order).
2. Publish an event to a message broker (e.g., `OrderCreated`).

These two operations must be **atomic** — either both succeed or neither does.
Using a distributed transaction (2PC) across the database and broker is
fragile, slow, and often unsupported.

The **Transactional Outbox** solves this by:

1. Writing the business entity **and** the event into the **same database
   transaction** (the event goes into an "outbox" table).
2. A separate **message relay** process reads the outbox and publishes events
   to the external broker.
3. Events are published **at least once** — consumers must be idempotent.

```
┌─────────────────────────────────────────────────────────┐
│                    PostgreSQL                            │
│                                                         │
│  BEGIN;                                                 │
│    INSERT INTO orders (...) VALUES (...);               │
│    INSERT INTO outbox_events (event_type, payload, ...) │
│      VALUES ('OrderCreated', '{"order_id": 42}');       │
│  COMMIT;                                                │
│                                                         │
│  ┌──────────────────┐    ┌──────────────────────────┐   │
│  │  outbox_events   │───→│ Message Relay (worker)   │───┼──→ Kafka / RabbitMQ / pgmq
│  │  (pending rows)  │    │ polls or tails changes   │   │
│  └──────────────────┘    └──────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

---

## The Problem It Solves

| Failure Scenario                        | Without Outbox             | With Outbox                  |
|-----------------------------------------|----------------------------|------------------------------|
| DB commits, broker publish fails        | Event lost forever         | Relay retries from outbox    |
| Service crashes after DB commit         | Event lost (was in memory) | Event persisted in outbox    |
| DB commits, service crashes before ack  | Possible duplicate         | Relay re-publishes (idempotent) |
| Broker down for maintenance             | Events lost during outage  | Events queue in outbox table |

**Key guarantees:**

- **No event loss:** If the transaction commits, the event is durably stored.
- **At-least-once delivery:** The relay may publish duplicates, but never drops.
- **Ordering preserved:** Events from a single aggregate are ordered by their
  database sequence (outbox PK or LSN).

---

## How pg_trickle Enables the Transactional Outbox

### Architecture Overview

pg_trickle's CDC infrastructure already captures row-level changes into buffer
tables (`pgtrickle_changes.changes_<oid>`) within the same transaction as the
source DML. This means **the CDC buffer is itself a transactional outbox** —
the only missing piece is the relay to an external broker.

```
┌──────────────────────────────────────────────────────────────────┐
│                         PostgreSQL                                │
│                                                                   │
│  ┌──────────────┐  CDC trigger   ┌──────────────────────────┐    │
│  │ orders       │ ──────────────→│ pgtrickle_changes.       │    │
│  │ (base table) │  (same txn)    │   changes_<oid>          │    │
│  └──────────────┘                └────────────┬─────────────┘    │
│                                               │                   │
│  ┌──────────────┐                             │ DIFFERENTIAL      │
│  │ outbox_events│  (optional,                 │ refresh           │
│  │ (explicit)   │   user-managed)             ↓                   │
│  └──────┬───────┘                ┌──────────────────────────┐    │
│         │ CDC trigger            │ pending_outbox_events    │    │
│         ↓                        │ (stream table)           │    │
│  ┌──────────────────────────┐    └────────────┬─────────────┘    │
│  │ pgtrickle_changes.       │                 │                   │
│  │   changes_<oid>          │                 │ NOTIFY             │
│  └──────────────────────────┘                 ↓                   │
│                                  ┌──────────────────────────┐    │
│                                  │ Message Relay             │    │
│                                  │ (app worker or pg worker) │───┼──→ Kafka / pgmq
│                                  └──────────────────────────┘    │
└──────────────────────────────────────────────────────────────────┘
```

### Approach 1: Stream Table as the Outbox View

The simplest approach — use an explicit outbox table and a pg_trickle stream
table to materialize the "pending events" view. The stream table gives you a
fast, pre-filtered, always-fresh view of unpublished events.

```sql
-- Step 1: Create the outbox table
CREATE TABLE outbox_events (
    event_id     BIGSERIAL PRIMARY KEY,
    event_type   TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ          -- NULL = not yet published
);

CREATE INDEX idx_outbox_pending ON outbox_events (event_id)
    WHERE published_at IS NULL;

-- Step 2: Create a stream table for pending events
SELECT pgtrickle.create_stream_table(
    'pending_outbox_events',
    $$SELECT event_id, event_type, aggregate_id, aggregate_type, payload, created_at
      FROM outbox_events
      WHERE published_at IS NULL$$,
    schedule => '1s',
    refresh_mode => 'DIFFERENTIAL'
);

-- Step 3: Application writes business entity + outbox in one transaction
BEGIN;
  INSERT INTO orders (user_id, total, status)
      VALUES (123, 99.95, 'confirmed');
  INSERT INTO outbox_events (event_type, aggregate_id, aggregate_type, payload)
      VALUES ('OrderCreated', currval('orders_id_seq')::text, 'Order',
              jsonb_build_object('user_id', 123, 'total', 99.95, 'status', 'confirmed'));
COMMIT;

-- Step 4: Relay reads from the stream table (always fresh within ~1s)
-- Application-side pseudocode:
--   rows = SELECT * FROM pending_outbox_events ORDER BY event_id;
--   for each row:
--     publish(row) to Kafka/RabbitMQ
--     UPDATE outbox_events SET published_at = now() WHERE event_id = row.event_id;
```

**Advantages:**
- Stream table is pre-filtered (only pending events), no full-table scan.
- DIFFERENTIAL refresh means only changed rows are recomputed.
- `pg_trickle_alert` channel notifies when the stream table refreshes.
- Multiple relays can read the stream table (but need coordination — see below).

**When to use:** Standard outbox with moderate throughput (< 10K events/sec).

### Approach 2: CDC Buffer as the Outbox

For higher throughput, skip the explicit outbox table entirely. pg_trickle's
CDC triggers already capture every INSERT/UPDATE/DELETE into typed buffer
tables. Your relay can read directly from the CDC buffers.

```sql
-- The application just writes to the business table
BEGIN;
  INSERT INTO orders (user_id, total, status)
      VALUES (123, 99.95, 'confirmed');
COMMIT;

-- pg_trickle's CDC trigger has already captured this INSERT in:
--   pgtrickle_changes.changes_<orders_oid>
-- with columns: change_id, lsn, action='I', new_user_id, new_total, new_status, ...

-- Create a stream table that transforms CDC changes into domain events
SELECT pgtrickle.create_stream_table(
    'order_events',
    $$SELECT o.id AS aggregate_id,
             CASE
               WHEN o.status = 'confirmed' THEN 'OrderCreated'
               WHEN o.status = 'shipped'   THEN 'OrderShipped'
               WHEN o.status = 'cancelled' THEN 'OrderCancelled'
             END AS event_type,
             jsonb_build_object(
               'order_id', o.id,
               'user_id', o.user_id,
               'total', o.total,
               'status', o.status
             ) AS payload
      FROM orders o$$,
    schedule => '1s',
    refresh_mode => 'DIFFERENTIAL'
);
```

**Advantages:**
- No explicit outbox table — zero additional write overhead.
- CDC captures happen within the same transaction automatically.
- Delta-based: only changed rows flow through the stream table.

**When to use:** When the business table itself is the source of events and
you don't need custom event shaping beyond what a SQL query can express.

### Approach 3: IMMEDIATE Mode for Zero-Lag Outbox

For scenarios requiring **sub-millisecond event freshness**, IMMEDIATE mode
updates the stream table within the same transaction as the source write.

```sql
-- Create the outbox with IMMEDIATE refresh
SELECT pgtrickle.create_stream_table(
    'realtime_outbox',
    $$SELECT event_id, event_type, aggregate_id, payload, created_at
      FROM outbox_events
      WHERE published_at IS NULL$$,
    refresh_mode => 'IMMEDIATE'
);

-- Now, within the same transaction:
BEGIN;
  INSERT INTO orders (...) VALUES (...);
  INSERT INTO outbox_events (...) VALUES (...);
  -- At COMMIT time, realtime_outbox is already updated
COMMIT;

-- A LISTEN-based relay sees the change immediately
LISTEN pg_trickle_alert;
-- Payload: {"event":"refresh_completed","pgt_name":"realtime_outbox",...}
```

**Advantages:**
- Zero lag between business write and outbox materialization.
- Relay can use LISTEN/NOTIFY for push-based publishing.

**Trade-offs:**
- Adds latency to the source transaction (statement-level trigger overhead).
- Best for low-to-moderate write throughput (< 1K writes/sec).

---

## Message Relay Strategies

The outbox pattern requires a **message relay** — a process that reads pending
events and publishes them to an external broker. pg_trickle supports three
relay strategies.

### Strategy A: Polling Publisher via Stream Table

The relay polls the stream table at regular intervals.

```python
import psycopg2
import json
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='kafka:9092')
conn = psycopg2.connect("postgresql://localhost/mydb")

while True:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT event_id, event_type, aggregate_id, payload
            FROM pending_outbox_events
            ORDER BY event_id
            LIMIT 100
        """)
        rows = cur.fetchall()

        for event_id, event_type, aggregate_id, payload in rows:
            producer.send(
                topic=event_type,
                key=aggregate_id.encode(),
                value=json.dumps(payload).encode(),
                headers=[('event_id', str(event_id).encode())]
            )
            cur.execute(
                "UPDATE outbox_events SET published_at = now() WHERE event_id = %s",
                (event_id,)
            )
        conn.commit()

    time.sleep(0.5)  # or match the stream table schedule
```

**Throughput:** Moderate. Limited by polling interval.  
**Latency:** ~1–2s (stream table schedule + polling interval).

### Strategy B: LISTEN/NOTIFY + Stream Table

Push-based relay using PostgreSQL's built-in notification system.

```python
import psycopg2
import psycopg2.extensions
import select

conn = psycopg2.connect("postgresql://localhost/mydb")
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

cur = conn.cursor()
cur.execute("LISTEN pg_trickle_alert;")

while True:
    if select.select([conn], [], [], 5.0) != ([], [], []):
        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            payload = json.loads(notify.payload)

            if (payload.get('event') == 'refresh_completed' and
                payload.get('pgt_name') == 'pending_outbox_events'):
                # Fetch and publish pending events
                publish_pending_events()
```

**Throughput:** High. Wakes immediately on refresh.  
**Latency:** Stream table poll schedule + notification delivery after refresh.

### Strategy C: Transaction Log Tailing via WAL CDC

When pg_trickle transitions a source table to WAL-based CDC, the logical
replication slot already captures changes in commit order. An external tool or
custom relay can tail this stream.

This is conceptually identical to using Debezium, but pg_trickle manages the
replication slot lifecycle automatically.

**When to use:** Very high throughput (> 10K events/sec) where polling overhead
is unacceptable and you need strict commit-order guarantees.

---

## Worked Example: Order Service Outbox

A complete example showing an e-commerce order service with pg_trickle-powered
outbox.

```sql
-- === Schema ===

CREATE TABLE customers (
    id    SERIAL PRIMARY KEY,
    name  TEXT NOT NULL,
    email TEXT NOT NULL
);

CREATE TABLE orders (
    id          SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(id),
    total       NUMERIC(10,2) NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE outbox_events (
    event_id       BIGSERIAL PRIMARY KEY,
    event_type     TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    payload        JSONB NOT NULL,
    metadata       JSONB DEFAULT '{}',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ
);

-- === Trigger to auto-populate outbox on order changes ===

CREATE OR REPLACE FUNCTION fn_order_outbox() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO outbox_events (event_type, aggregate_type, aggregate_id, payload)
        VALUES (
            'OrderCreated', 'Order', NEW.id::text,
            jsonb_build_object(
                'order_id', NEW.id,
                'customer_id', NEW.customer_id,
                'total', NEW.total,
                'status', NEW.status
            )
        );
    ELSIF TG_OP = 'UPDATE' AND OLD.status <> NEW.status THEN
        INSERT INTO outbox_events (event_type, aggregate_type, aggregate_id, payload)
        VALUES (
            'OrderStatusChanged', 'Order', NEW.id::text,
            jsonb_build_object(
                'order_id', NEW.id,
                'old_status', OLD.status,
                'new_status', NEW.status
            )
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_order_outbox
    AFTER INSERT OR UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION fn_order_outbox();

-- === pg_trickle Stream Tables ===

-- Pending events for the relay to publish
SELECT pgtrickle.create_stream_table(
    'pending_outbox_events',
    $$SELECT event_id, event_type, aggregate_type, aggregate_id,
             payload, metadata, created_at
      FROM outbox_events
      WHERE published_at IS NULL
      ORDER BY event_id$$,
    schedule => '1s',
    refresh_mode => 'DIFFERENTIAL'
);

-- Published event audit log (for monitoring dashboards)
SELECT pgtrickle.create_stream_table(
    'outbox_stats',
    $$SELECT event_type,
             COUNT(*) AS total_events,
             COUNT(*) FILTER (WHERE published_at IS NULL) AS pending,
             COUNT(*) FILTER (WHERE published_at IS NOT NULL) AS published,
             MAX(created_at) FILTER (WHERE published_at IS NULL) AS oldest_pending
      FROM outbox_events
      GROUP BY event_type$$,
    schedule => '10s',
    refresh_mode => 'DIFFERENTIAL'
);

-- === Monitoring ===

-- Alert when outbox is backing up
-- pg_trickle_alert channel will emit stale_data events if the stream table
-- falls behind schedule, giving you a built-in health signal.
SELECT pgtrickle.get_staleness('pending_outbox_events');
```

### Competing Consumers with `FOR UPDATE SKIP LOCKED`

For multiple relay instances processing the same outbox:

```sql
-- Each relay instance runs:
BEGIN;
  SELECT event_id, event_type, aggregate_id, payload
  FROM outbox_events
  WHERE published_at IS NULL
  ORDER BY event_id
  LIMIT 50
  FOR UPDATE SKIP LOCKED;

  -- Publish batch to broker...

  UPDATE outbox_events
  SET published_at = now()
  WHERE event_id = ANY($published_ids);
COMMIT;
```

This provides safe concurrent processing without duplicate publishing.

---

## Complementary PostgreSQL Extensions

### pgmq — PostgreSQL Message Queue

[pgmq](https://github.com/pgmq/pgmq) (4.8K+ stars) is a lightweight
PostgreSQL extension that implements an SQS-like message queue entirely within
PostgreSQL.

**How it works with pg_trickle:**

```sql
CREATE EXTENSION pgmq;

-- Create a queue for order events
SELECT pgmq.create('order_events');

-- Use a trigger on the pg_trickle stream table (or outbox table)
-- to enqueue messages automatically
CREATE OR REPLACE FUNCTION fn_enqueue_outbox() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pgmq.send(
        'order_events',
        jsonb_build_object(
            'event_id', NEW.event_id,
            'event_type', NEW.event_type,
            'aggregate_id', NEW.aggregate_id,
            'payload', NEW.payload
        )
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Consumers read from the pgmq queue
SELECT * FROM pgmq.read('order_events', vt => 30, qty => 10);

-- After processing, delete or archive
SELECT pgmq.delete('order_events', msg_id => 1);
```

**Advantages of pgmq + pg_trickle:**
- Both run inside PostgreSQL — no external broker needed.
- pgmq provides visibility timeouts, exactly-once delivery semantics, and
  message archival.
- pg_trickle provides the real-time materialized view layer on top.
- Combined: write to business table → CDC → stream table → pgmq queue →
  consumer.

**Use case:** Small-to-medium deployments that want to avoid operating Kafka or
RabbitMQ.

### pg_amqp — AMQP Publishing from PostgreSQL

[pg_amqp](https://github.com/omniti-labs/pg_amqp) allows publishing messages
to an AMQP broker (RabbitMQ) directly from PostgreSQL triggers or functions.

```sql
-- Configure the broker connection
INSERT INTO amqp.broker (host, port, vhost, username, password)
VALUES ('rabbitmq', 5672, '/', 'guest', 'guest');

-- Publish from a trigger on the outbox table
CREATE OR REPLACE FUNCTION fn_publish_to_amqp() RETURNS TRIGGER AS $$
BEGIN
    PERFORM amqp.publish(1, 'events', NEW.event_type, NEW.payload::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

**Trade-off:** Publishing happens within the transaction — if the broker is
slow or down, it blocks the transaction. Better suited as a relay step after
the outbox commit.

### pgflow — Durable Workflow Engine

[pgflow](https://pgflow.dev/) builds durable, multi-step workflows on top of
pgmq. It can orchestrate complex event processing pipelines with retry logic,
timeouts, and DAG-based step dependencies.

**Synergy with pg_trickle:** Use pg_trickle stream tables as the "sensing"
layer that detects changes, and pgflow as the "acting" layer that orchestrates
side effects.

### Debezium — External CDC Platform

[Debezium](https://debezium.io/) is the industry-standard CDC platform for
tailing database WALs and publishing changes to Kafka. When pg_trickle
transitions to WAL-based CDC, the logical replication slot it creates is
conceptually similar to what Debezium uses.

**When to choose Debezium over pg_trickle-native relay:**
- You need Kafka Connect ecosystem integration.
- You need to capture changes from multiple databases into a central event bus.
- You need schema registry integration (Avro/Protobuf serialization).

**When pg_trickle-native is sufficient:**
- Single-database topology.
- Events are consumed by services that can query PostgreSQL directly.
- You want to avoid operating an additional infrastructure component.

### NATS / JetStream — Lightweight Messaging Fabric

[NATS](https://nats.io/) is a high-performance, cloud-native messaging system
(CNCF incubating project) that provides pub/sub, request/reply, and persistent
streaming via **JetStream** — all through a single ~10 MB binary with
sub-millisecond latency. It is a compelling alternative to Kafka or RabbitMQ
for the outbox relay target, especially in edge, IoT, or low-ops environments.

**Key JetStream features relevant to the outbox relay:**

| Feature | Benefit for Outbox Relay |
|---------|-------------------------|
| Built-in deduplication (`Nats-Msg-Id` header) | Prevents duplicate event delivery without consumer-side logic |
| Exactly-once semantics | Double-ack protocol between publisher and consumer |
| Per-subject ordering | Events for the same aggregate stay ordered |
| Durable consumers | Consumers resume from last acknowledged position after restart |
| Work queue mode | Automatic load balancing across competing relay consumers |
| Wildcard subscriptions | `orders.>` matches `orders.created`, `orders.shipped`, etc. |
| Stream retention policies | Limits-based, interest-based, or work-queue retention |

**Relay pattern with NATS JetStream:**

```python
import nats
from nats.js import JetStreamContext
import psycopg2
import json
import asyncio

async def outbox_relay():
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()

    # Ensure the stream exists (idempotent)
    await js.add_stream(name="ORDERS", subjects=["orders.>"])

    conn = psycopg2.connect("postgresql://localhost/mydb")

    while True:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT event_id, event_type, aggregate_id, payload
                FROM pending_outbox_events
                ORDER BY event_id
                LIMIT 100
            """)
            rows = cur.fetchall()

            for event_id, event_type, aggregate_id, payload in rows:
                subject = f"orders.{event_type.lower()}"
                await js.publish(
                    subject,
                    json.dumps(payload).encode(),
                    headers={
                        "Nats-Msg-Id": str(event_id),  # deduplication key
                        "Aggregate-Id": aggregate_id,
                    },
                )
                cur.execute(
                    "UPDATE outbox_events SET published_at = now() WHERE event_id = %s",
                    (event_id,),
                )
            conn.commit()

        await asyncio.sleep(0.5)
```

**Subject-based routing example:**

```
orders.ordercreated       → Order Service consumers
orders.orderstatuschanged → Fulfillment Service consumers
orders.>                  → Audit Service (wildcard: receives everything)
```

**`pgnats` — Direct NATS publishing from PostgreSQL SQL:**

[pgnats](https://github.com/luxms/pgnats) (by [luxms](https://github.com/luxms),
MIT license, written in Rust with pgrx) is a real PostgreSQL extension that
provides direct NATS publishing from SQL — analogous to `pg_amqp` for
RabbitMQ. It eliminates the external relay process entirely for simple
topologies.

**pgnats features:**

| Feature | API |
|---------|-----|
| Core NATS publish (text/binary/JSON/JSONB) | `nats_publish_text/binary/json/jsonb()` |
| JetStream persistent publish | `nats_publish_text/json/jsonb_stream()` |
| JetStream publish with headers | `nats_publish_jsonb_stream(subj, payload, headers)` |
| NATS subscriptions (invoke PG functions) | `nats_subscribe(subject, handler::regproc)` |
| Request/reply | `nats_request_json/text/binary/jsonb()` |
| Key-Value storage | `nats_put/get_json/text/binary/jsonb()` |
| Object storage | `nats_put/get_file()` |
| TLS / mTLS | Via FDW server options |

**Configuration via Foreign Data Wrapper:**

```sql
CREATE EXTENSION pgnats;

-- One-time connection setup
CREATE SERVER nats_server
    FOREIGN DATA WRAPPER pgnats_fdw
    OPTIONS (
        host 'localhost',
        port '4222',
        -- Optional TLS:
        -- tls_ca_path   '/etc/ssl/nats-ca.pem',
        -- tls_cert_path '/etc/ssl/nats-client.pem',
        -- tls_key_path  '/etc/ssl/nats-client-key.pem'
    );
```

**Publishing to NATS JetStream directly from a trigger:**

```sql
-- Publish to JetStream (durable, at-least-once) with deduplication header
CREATE OR REPLACE FUNCTION fn_publish_to_nats() RETURNS TRIGGER AS $$
BEGIN
    PERFORM nats_publish_jsonb_stream(
        'orders.' || lower(NEW.event_type),           -- subject
        NEW.payload || jsonb_build_object(            -- payload + event metadata
            'event_id',      NEW.event_id,
            'aggregate_id',  NEW.aggregate_id,
            'created_at',    NEW.created_at
        ),
        json_build_object(
            'Nats-Msg-Id', NEW.event_id::text         -- JetStream deduplication key
        )
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_outbox_nats
    AFTER INSERT ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION fn_publish_to_nats();
```

**Subscribing to NATS subjects from PostgreSQL (inbox use case):**

```sql
-- PostgreSQL function called for every inbound NATS message
CREATE OR REPLACE FUNCTION public.handle_inbound_event(payload bytea)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    msg jsonb := convert_from(payload, 'UTF8')::jsonb;
BEGIN
    INSERT INTO inbox_events (event_id, event_type, payload)
    VALUES (
        msg->>'event_id',
        msg->>'event_type',
        msg
    )
    ON CONFLICT (event_id) DO NOTHING;
END;
$$;

-- Subscribe: NATS delivers messages to this PG function via a background worker
SELECT nats_subscribe('orders.>', 'public.handle_inbound_event'::regproc);

-- Unsubscribe when no longer needed
-- SELECT nats_unsubscribe('orders.>', 'public.handle_inbound_event'::regproc);
```

> **Caution:** In-transaction publishing (trigger-based) couples the
> transaction to NATS availability. If NATS is down or slow, the source
> transaction blocks or fails. The relay pattern (async, outside the
> transaction) is safer for production use. The trigger approach is best
> suited for scenarios where NATS is co-located and highly available
> (e.g., sidecar or leaf node in a Kubernetes pod). pgnats also supports
> TLS and mTLS to mitigate availability risk.

**When to choose NATS over Kafka/RabbitMQ:**
- Low operational overhead (single binary, zero external dependencies).
- Sub-millisecond publish latency is critical.
- Edge or IoT deployments where a full Kafka cluster is impractical.
- You need both pub/sub and request/reply in the same system.
- Built-in deduplication simplifies consumer logic.

**When Kafka is still preferred:**
- You need the Kafka Connect ecosystem (hundreds of connectors).
- Multi-datacenter replication with MirrorMaker.
- Schema registry and Avro/Protobuf serialization.
- Existing Kafka infrastructure and team expertise.

---

## Potential pg_trickle Extensions

### Extension 1: Outbox Table Helper

A convenience function that creates the outbox table, trigger, and stream
table in one call.

```sql
-- Proposed API
SELECT pgtrickle.create_outbox(
    source_table => 'orders',
    outbox_name  => 'order_outbox',
    event_types  => ARRAY['OrderCreated', 'OrderUpdated', 'OrderDeleted'],
    schedule     => '1s'
);

-- This would create:
-- 1. outbox_events_<source> table with standard schema
-- 2. AFTER INSERT/UPDATE/DELETE trigger on source_table
-- 3. Stream table: pending_<outbox_name> (WHERE published_at IS NULL)
-- 4. Index on (event_id) WHERE published_at IS NULL
```

**Effort:** Medium. Mostly SQL generation and metadata management.

### Extension 2: Message Relay Background Worker

A pg_trickle background worker that reads from the outbox stream table and
publishes to an external system via a configurable connector.

```sql
-- Proposed GUC configuration
SET pg_trickle.outbox_relay_enabled = true;
SET pg_trickle.outbox_relay_target = 'pgmq';  -- or 'http', 'kafka'
SET pg_trickle.outbox_relay_queue = 'order_events';
SET pg_trickle.outbox_relay_batch_size = 100;
SET pg_trickle.outbox_relay_interval_ms = 500;
```

**Effort:** High. Requires connector framework, error handling, and
backpressure management.

### Extension 3: pgmq Integration

First-class integration with pgmq so that stream table refreshes can
automatically enqueue changed rows into a pgmq queue.

```sql
-- Proposed API
SELECT pgtrickle.create_stream_table(
    'order_notifications',
    $$SELECT id, status, total FROM orders WHERE status = 'confirmed'$$,
    schedule => '1s',
    refresh_mode => 'DIFFERENTIAL',
    on_change => 'pgmq:order_events'  -- auto-enqueue deltas
);
```

**Effort:** Medium. Hook into the refresh completion path to call
`pgmq.send_batch()` with the delta rows.

### Extension 4: Webhook Dispatcher

A built-in HTTP webhook dispatcher that posts delta rows to an external URL
after each refresh.

```sql
-- Proposed API
SELECT pgtrickle.create_stream_table(
    'order_webhooks',
    $$SELECT id, status, total FROM orders$$,
    schedule => '5s',
    refresh_mode => 'DIFFERENTIAL',
    on_change_webhook => 'https://api.example.com/webhooks/orders'
);
```

**Effort:** High. Requires HTTP client, retry logic, authentication, and
circuit breaker patterns. Consider using pg_net extension as the HTTP layer.

---

## Design Considerations

### Ordering Guarantees

| Scope | Guarantee |
|-------|-----------|
| Single aggregate (same `aggregate_id`) | Ordered by `event_id` (monotonic PK) |
| Across aggregates | No global ordering; each aggregate ordered independently |
| Across source tables | Independent refresh schedules; no cross-table ordering |
| Within a diamond dependency group | Atomic refresh group ensures consistency |

#### Strict Total Ordering Edge Cases

**Global ordering is fundamentally impossible** with the outbox pattern without
a single global lock — which defeats the purpose of microservice decoupling.
Specific edge cases to be aware of:

1. **PK gaps:** PostgreSQL sequences can produce gaps (transaction rollback,
   `nextval()` without `INSERT`). Gaps in `event_id` are normal and consumers
   must tolerate them. Never use "next expected ID" logic for gap detection
   on the outbox side.

2. **Relay batch duplication:** If the relay publishes events 1–10, crashes
   before marking them as published, then restarts and re-publishes 1–10,
   the broker now has two copies. Meanwhile events 11–20 arrive normally.
   Consumers see: 1–10, 1–10 (deduped), 11–20. With broker-level dedup
   (`Nats-Msg-Id`), the second 1–10 batch is silently dropped.

3. **Cross-aggregate causal ordering:** Order A creates a payment that
   triggers an inventory check. The events for Order and Payment have
   independent sequences. If you need causal ordering, include a
   `causation_id` (the event ID that triggered this event) in the payload
   and let the consumer reconstruct the causal chain.

4. **Competing relays:** Multiple relay instances using `FOR UPDATE SKIP
   LOCKED` process different batches concurrently. Events within each batch
   are ordered, but batches may arrive at the broker out of order (relay A
   publishes 11–20 before relay B publishes 1–10). **Fix:** Use a single
   relay instance per aggregate key, or accept per-aggregate ordering only.

### Idempotency

The outbox pattern provides **at-least-once** delivery. Consumers MUST handle
duplicates. Recommended strategies:

1. **Event ID deduplication:** Store processed `event_id` values in a
   `processed_events` table and check before processing.
2. **Idempotent operations:** Design state mutations so that applying the same
   event twice has no additional effect (e.g., `UPDATE ... SET status = 'x'`
   is naturally idempotent).
3. **Version vectors:** Include an `expected_version` in the event payload and
   reject stale events.

### Garbage Collection

Published events should be cleaned up to prevent outbox table bloat:

```sql
-- Option A: Delete published events older than 7 days
DELETE FROM outbox_events
WHERE published_at IS NOT NULL
  AND published_at < now() - INTERVAL '7 days';

-- Option B: Partition the outbox table by month
CREATE TABLE outbox_events (
    event_id BIGSERIAL,
    ...
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
) PARTITION BY RANGE (created_at);
```

### Backpressure

When the relay falls behind:

1. `pg_trickle_alert` emits `stale_data` events when the stream table
   staleness exceeds 2× its schedule.
2. Monitor `pending_outbox_events` row count via the `outbox_stats` stream
   table.
3. Adjust `schedule` or increase relay parallelism.
4. pg_trickle's adaptive refresh can automatically switch to FULL refresh if
   the change ratio exceeds `differential_max_change_ratio`.

### Failure Modes

| Failure | Impact | Recovery |
|---------|--------|----------|
| Relay crashes after publish, before marking published | Duplicate event published | Consumer deduplicates via event_id |
| PostgreSQL crashes after COMMIT | Events durable in WAL | Normal crash recovery; outbox intact |
| Stream table refresh fails | Relay reads stale data | pg_trickle retries with backoff; alert emitted |
| Broker unreachable | Relay retries | Events queue in outbox; monitor staleness |

---

## Event Sourcing vs. Transactional Outbox

These two patterns are frequently confused but serve fundamentally different
purposes. Understanding the distinction prevents over-engineering.

| Aspect | Transactional Outbox | Event Sourcing |
|--------|---------------------|----------------|
| **Purpose** | Notify other services of state changes | Store state *as* a sequence of events |
| **Source of truth** | Traditional tables (orders, users) | The event log itself |
| **Event role** | Notification — can be lost and re-derived | Authoritative — losing events = losing state |
| **Retention** | Temporary (garbage-collect after publishing) | Permanent (append-only, never delete) |
| **Replay** | Not required; events are disposable | Essential; state is rebuilt from replay |
| **Schema** | JSONB payload, loosely structured | Strongly typed, versioned events |
| **pg_trickle role** | CDC + stream table + relay | CDC captures the append to the event store |

**Can they coexist?** Yes. A common architecture:

1. **Event Store** (e.g., `order_events` append-only table) is the source of
   truth.
2. **Projection tables** (e.g., `orders_current_state`) are pg_trickle stream
   tables that materialize the latest state from the event store.
3. **Outbox** publishes selected events to external services — the stream
   table over the event store acts as the outbox view.

```sql
-- Event store: append-only, never UPDATE or DELETE
CREATE TABLE order_events (
    event_id    BIGSERIAL PRIMARY KEY,
    order_id    UUID NOT NULL,
    event_type  TEXT NOT NULL,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Projection: current order state (derived from events)
SELECT pgtrickle.create_stream_table(
    'orders_current_state',
    $$SELECT DISTINCT ON (order_id)
            order_id, event_type AS last_event, payload, created_at
      FROM order_events
      ORDER BY order_id, event_id DESC$$,
    schedule => '1s',
    refresh_mode => 'DIFFERENTIAL'
);

-- Outbox: events not yet published to external services
SELECT pgtrickle.create_stream_table(
    'pending_external_events',
    $$SELECT event_id, order_id, event_type, payload
      FROM order_events
      WHERE event_id > (SELECT COALESCE(MAX(last_published_id), 0)
                        FROM outbox_watermark)$$,
    schedule => '1s',
    refresh_mode => 'DIFFERENTIAL'
);
```

> **Rule of thumb:** If your team debates "should we event-source this?",
> start with the outbox pattern. It's strictly simpler. You can always
> migrate to event sourcing later — the outbox trigger captures the same
> change events either way.

---

## CloudEvents Standard

[CloudEvents](https://cloudevents.io/) (CNCF graduated) is a vendor-neutral
specification for describing event data in a common way. Adopting it makes
outbox events interoperable across brokers, languages, and frameworks.

### Why CloudEvents?

- Standard envelope format understood by NATS, Kafka, Azure Event Grid,
  AWS EventBridge, Knative, and many others.
- SDKs available for Go, Java, Python, Rust, JavaScript, .NET, and more.
- Schema validation tools (`ce-schema`) ensure payload correctness.
- [AsyncAPI](https://www.asyncapi.com/) can document CloudEvents contracts
  (like OpenAPI but for event-driven APIs).

### CloudEvents-Formatted Outbox

```sql
-- CloudEvents v1.0 envelope stored in the outbox
CREATE TABLE outbox_events (
    event_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),  -- 'id'
    source         TEXT NOT NULL DEFAULT '/orders',             -- 'source'
    spec_version   TEXT NOT NULL DEFAULT '1.0',                 -- 'specversion'
    event_type     TEXT NOT NULL,                               -- 'type'
    subject        TEXT,                                        -- 'subject'
    time           TIMESTAMPTZ NOT NULL DEFAULT now(),          -- 'time'
    data_content_type TEXT NOT NULL DEFAULT 'application/json', -- 'datacontenttype'
    data           JSONB NOT NULL,                              -- 'data'
    -- Extension attributes
    aggregate_id   UUID,
    trace_id       UUID,
    schema_version INTEGER NOT NULL DEFAULT 1,
    -- Relay state
    published_at   TIMESTAMPTZ
);

-- Trigger that writes CloudEvents-formatted events
CREATE OR REPLACE FUNCTION fn_order_outbox_ce() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO outbox_events (event_type, subject, aggregate_id, data)
    VALUES (
        'com.example.orders.' || CASE TG_OP
            WHEN 'INSERT' THEN 'created'
            WHEN 'UPDATE' THEN 'updated'
            WHEN 'DELETE' THEN 'deleted'
        END,
        '/orders/' || COALESCE(NEW.id, OLD.id)::text,
        COALESCE(NEW.id, OLD.id),
        CASE TG_OP
            WHEN 'DELETE' THEN to_jsonb(OLD)
            ELSE to_jsonb(NEW)
        END
    );
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;
```

### Publishing as CloudEvents Wire Format

```python
# Relay publishes in CloudEvents JSON format
import json

def to_cloudevent_json(row: dict) -> bytes:
    ce = {
        "specversion": row["spec_version"],
        "id":          str(row["event_id"]),
        "source":      row["source"],
        "type":        row["event_type"],
        "time":        row["time"].isoformat(),
        "datacontenttype": row["data_content_type"],
        "data":        row["data"],
    }
    if row.get("subject"):
        ce["subject"] = row["subject"]
    return json.dumps(ce).encode()

# With NATS + pgnats:
# SELECT nats_publish_jsonb_stream(
#     'orders.created',
#     to_jsonb(outbox_row),  -- already CloudEvents-shaped
#     json_build_object('Nats-Msg-Id', event_id::text,
#                       'ce-specversion', '1.0')
# );
```

### Documenting with AsyncAPI

AsyncAPI provides an OpenAPI-like contract for event-driven APIs:

```yaml
# asyncapi.yaml (v3.0)
asyncapi: 3.0.0
info:
  title: Order Service Events
  version: 1.0.0
channels:
  orderCreated:
    address: orders.created
    messages:
      OrderCreated:
        payload:
          $ref: '#/components/schemas/Order'
        headers:
          type: object
          properties:
            ce-specversion: { type: string, const: '1.0' }
            ce-type: { type: string }
            ce-source: { type: string }
            Nats-Msg-Id: { type: string }
components:
  schemas:
    Order:
      type: object
      required: [order_id, amount, status]
      properties:
        order_id: { type: string, format: uuid }
        amount: { type: number }
        status: { type: string, enum: [pending, confirmed, shipped] }
```

---

## Exactly-Once Semantics — Clarified

The term "exactly-once" is used loosely in messaging. There are three distinct
levels, and confusing them leads to over- or under-engineering.

### Three Levels of Exactly-Once

| Level | What It Means | Who Provides It |
|-------|--------------|------------------|
| **Broker-level dedup** | The broker stores a message at most once per dedup key | NATS `Nats-Msg-Id`, Kafka idempotent producer |
| **Consumer-level dedup** | The consumer processes a message at most once per ID | Inbox `ON CONFLICT (event_id) DO NOTHING` |
| **End-to-end exactly-once** | From source write to final side-effect, the event has exactly one effect | Both broker-level + consumer-level together |

### How Each pg_trickle Approach Maps

| Approach | Broker-Level | Consumer-Level | End-to-End |
|----------|-------------|----------------|------------|
| Stream table + polling relay to Kafka | Kafka idempotent producer (`acks=all`) | Consumer dedup table | Yes (with careful config) |
| Stream table + NATS relay | `Nats-Msg-Id` = `event_id` | Consumer dedup table | Yes (built-in) |
| pgnats trigger → NATS JetStream | `Nats-Msg-Id` header in trigger | Consumer dedup table | Yes (simplest path) |
| CDC buffer → Debezium → Kafka | Debezium event ID | Consumer dedup table | Yes |
| pgmq (intra-PostgreSQL) | `pgmq.send()` is transactional | Same-DB dedup | Yes (single DB boundary) |

### Common Pitfalls

1. **Relay crash between publish and `UPDATE published_at`:** The event is in
   the broker but not marked as published. On restart, the relay re-publishes
   → duplicate. **Fix:** Use broker-level dedup (`Nats-Msg-Id` or Kafka
   idempotent producer).
2. **Consumer crash between processing and ack:** The broker redelivers.
   **Fix:** Inbox `ON CONFLICT DO NOTHING` or idempotent state mutation.
3. **`event_id` is unique but not deterministic:** If the same business
   operation can generate multiple events with different IDs (e.g., retried
   application logic), dedup by `event_id` won't help. **Fix:** Use a
   deterministic idempotency key derived from business state (e.g.,
   `order_id || status || version`).

> **Recommendation:** Always implement *both* broker-level and consumer-level
> dedup. The cost is minimal (one `ON CONFLICT` clause + one header), and it
> makes the system resilient to any single-component failure.

---

## Schema Evolution & Message Versioning

As services evolve, event schemas change. A robust outbox implementation must
handle versioning gracefully to avoid breaking downstream consumers.

### Versioning Strategies

| Strategy | Description | Pros | Cons |
|----------|-------------|------|------|
| **Embedded version field** | `event_type = 'OrderCreated.v2'` | Easy to route, clear in logs | Type proliferation |
| **Semantic versioning in payload** | `{"schema_version": 2, ...}` in JSONB | Single event type, flexible | Consumers must inspect payload |
| **Envelope pattern** | Wrap payload in `{"v": 2, "data": {...}}` | Uniform structure | Extra wrapper object |
| **Content-type header** | `application/vnd.orders.created+json; version=2` | Standards-based | Requires rich broker support |

### Compatibility Rules

Follow the **Postel's Law** principle for schema changes:

- **Backward-compatible** (safe): Add optional fields, widen types, add enum values.
- **Forward-compatible** (safe with care): Remove optional fields, rename with alias.
- **Breaking** (avoid or version bump): Remove required fields, change field types, alter semantics.

### Implementation with pg_trickle

```sql
-- Version the outbox table columns
ALTER TABLE outbox_events
    ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1;

-- Stream table filtered by version for v1 consumers only
SELECT pgtrickle.create_stream_table(
    'pending_outbox_v1',
    $$SELECT event_id, aggregate_id, event_type, payload
      FROM outbox_events
      WHERE published_at IS NULL
        AND schema_version = 1$$,
    schedule => '1s',
    refresh_mode => 'DIFFERENTIAL'
);

-- Separate view for v2+ consumers
SELECT pgtrickle.create_stream_table(
    'pending_outbox_v2',
    $$SELECT event_id, aggregate_id, event_type, payload
      FROM outbox_events
      WHERE published_at IS NULL
        AND schema_version >= 2$$,
    schedule => '1s',
    refresh_mode => 'DIFFERENTIAL'
);
```

### Rolling Schema Migration

1. **Deploy** the producer with new schema alongside old schema (dual-write).
2. **Migrate** consumers to accept both versions.
3. **Cut over** the producer to write only the new schema.
4. **Drain** the old `pending_outbox_v1` view to zero.
5. **Drop** old version support after all consumers are updated.

---

## Observability & Distributed Tracing

Distributed systems require end-to-end visibility to diagnose issues. A
well-instrumented outbox pipeline correlates events from database → relay →
broker → consumer.

### Correlation IDs

Include a `trace_id` in every event to correlate across service boundaries:

```sql
ALTER TABLE outbox_events
    ADD COLUMN trace_id UUID NOT NULL DEFAULT gen_random_uuid(),
    ADD COLUMN span_id  UUID;

-- Embed trace context in payload for downstream services
CREATE OR REPLACE FUNCTION outbox_embed_trace() RETURNS TRIGGER AS $$
BEGIN
    NEW.payload := NEW.payload ||
        jsonb_build_object(
            '_trace_id', NEW.trace_id::text,
            '_span_id',  NEW.span_id::text
        );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER outbox_embed_trace_tg
    BEFORE INSERT ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION outbox_embed_trace();
```

### Monitoring Dashboard Stream Table

```sql
-- Real-time outbox observability
SELECT pgtrickle.create_stream_table(
    'outbox_observability',
    $$SELECT
        event_type,
        COUNT(*) FILTER (WHERE published_at IS NULL)          AS pending_count,
        COUNT(*) FILTER (WHERE published_at IS NOT NULL)      AS published_count,
        AVG(EXTRACT(EPOCH FROM (published_at - created_at)))
            FILTER (WHERE published_at IS NOT NULL)           AS avg_publish_latency_sec,
        MAX(EXTRACT(EPOCH FROM (now() - created_at)))
            FILTER (WHERE published_at IS NULL)               AS max_pending_age_sec,
        COUNT(*) FILTER (
            WHERE published_at IS NULL
              AND created_at < now() - INTERVAL '5 minutes')  AS stuck_events_count
      FROM outbox_events
      GROUP BY event_type$$,
    schedule => '10s',
    refresh_mode => 'DIFFERENTIAL'
);
```

### pg_trickle Alert Integration

Subscribe to `pg_trickle_alert` for push-based relay health notifications:

```python
import asyncio, asyncpg, json

async def monitor_outbox_pipeline(dsn: str):
    conn = await asyncpg.connect(dsn)

    async def handle_alert(conn, pid, channel, payload):
        alert = json.loads(payload)
        match alert["type"]:
            case "stale_data":
                if alert["stream_table"] in ("pending_outbox", "outbox_observability"):
                    sentry.capture_message(f"Outbox lag: {alert['staleness_seconds']}s")
            case "refresh_failed":
                pagerduty.trigger(f"Outbox stream table failed: {alert['detail']}")
            case "buffer_growth_warning":
                slack.post(f"CDC buffer growing: {alert['stream_table']}")

    await conn.add_listener("pg_trickle_alert", handle_alert)
    await asyncio.sleep(float("inf"))  # run indefinitely
```

### OpenTelemetry Spans in the Relay

```python
from opentelemetry import trace

tracer = trace.get_tracer("outbox-relay")

async def relay_event(event: dict):
    trace_id = event.get("payload", {}).get("_trace_id")
    with tracer.start_as_current_span(
        "outbox.relay",
        context=trace_context_from_id(trace_id),
        attributes={
            "event.id":      event["event_id"],
            "event.type":    event["event_type"],
            "aggregate.id":  str(event["aggregate_id"]),
        }
    ) as span:
        await broker.publish(event)
        span.set_attribute("broker.subject", event["event_type"])
```

### Key Metrics to Track

| Metric | How to Measure | Alert Threshold |
|--------|---------------|------------------|
| **Outbox depth** | `pending_count` in `outbox_observability` | > 1 000 events |
| **Publish latency p99** | `avg_publish_latency_sec` | > 30 s |
| **Stuck events** | `stuck_events_count > 0` for 5+ min | Any stuck > 5 min |
| **CDC buffer size** | `pg_trickle_alert` → `buffer_growth_warning` | Built-in |
| **Stream table refresh failure** | `pg_trickle_alert` → `refresh_failed` | Built-in |
| **Relay process liveness** | External health-check endpoint | 1 missed heartbeat |

---

## Testing Strategies

Testing the transactional outbox requires verifying both the atomicity guarantee
and the relay pipeline end-to-end.

### Unit Tests — Event Generation

Verify events are inserted atomically and rolled back correctly:

```sql
-- pgTAP: event generated on INSERT
BEGIN;
INSERT INTO orders (customer_id, amount, status) VALUES (42, 100.00, 'pending');
SELECT is(
    (SELECT COUNT(*)::int FROM outbox_events WHERE event_type = 'OrderCreated'),
    1,
    'OrderCreated event generated on INSERT'
);
ROLLBACK;

-- After ROLLBACK, event must not exist
SELECT is(
    (SELECT COUNT(*)::int FROM outbox_events WHERE event_type = 'OrderCreated'),
    0,
    'No event persisted after ROLLBACK'
);
```

### Integration Tests — Stream Table Refresh

```rust
#[tokio::test]
async fn test_outbox_stream_table_reflects_new_events() {
    let ctx = TestContext::new().await;

    // Insert order — CDC trigger fires
    ctx.execute("INSERT INTO orders (customer_id, amount, status)
                 VALUES (1, 99.99, 'pending')").await;

    // Trigger immediate refresh
    ctx.execute("SELECT pgtrickle.refresh('pending_outbox')").await;

    let count: i64 = ctx.query_one(
        "SELECT COUNT(*) FROM pending_outbox WHERE event_type = 'OrderCreated'"
    ).await;
    assert_eq!(count, 1, "Stream table must contain the new event");
}
```

### End-to-End Tests — Full Relay Pipeline

```python
import pytest, asyncio, json

@pytest.mark.asyncio
async def test_event_reaches_broker_after_db_commit(pg_conn, nats_client):
    """Full pipeline: INSERT → stream table → relay → NATS → subscriber."""
    received: asyncio.Future = asyncio.get_event_loop().create_future()

    async def on_message(msg):
        received.set_result(json.loads(msg.data))
        await msg.ack()

    sub = await nats_client.subscribe("orders.created", cb=on_message)
    await pg_conn.execute(
        "INSERT INTO orders (customer_id, amount, status) VALUES ($1, $2, $3)",
        1, 100.0, "pending"
    )
    event = await asyncio.wait_for(received, timeout=5.0)
    assert event["event_type"] == "OrderCreated"
    await sub.unsubscribe()

@pytest.mark.asyncio
async def test_no_event_on_rollback(pg_conn, nats_client):
    """Events within a rolled-back transaction must NOT reach the broker."""
    events_received = []

    async def on_message(msg):
        events_received.append(msg)

    sub = await nats_client.subscribe("orders.created", cb=on_message)
    try:
        async with pg_conn.transaction():
            await pg_conn.execute(
                "INSERT INTO orders (customer_id, amount, status) VALUES ($1, $2, $3)",
                2, 50.0, "pending"
            )
            raise Exception("Forced rollback")
    except Exception:
        pass

    await asyncio.sleep(2.0)  # wait for a relay cycle
    assert len(events_received) == 0, "No events should reach broker after ROLLBACK"
    await sub.unsubscribe()
```

### Chaos Tests — Relay Crash Recovery

```python
@pytest.mark.asyncio
async def test_relay_recovers_after_crash(pg_conn, relay_process):
    """Events queued while relay is down must be delivered after restart."""
    await relay_process.stop()

    for i in range(10):
        await pg_conn.execute(
            "INSERT INTO orders (customer_id, amount) VALUES ($1, $2)", i, 10.0
        )

    await relay_process.start()
    await asyncio.sleep(5.0)  # allow relay to catch up

    count = await pg_conn.fetchval(
        "SELECT COUNT(*) FROM outbox_events WHERE published_at IS NOT NULL"
    )
    assert count == 10, "All events delivered after relay restart"
```

---

## Saga Pattern & Compensating Transactions

The Transactional Outbox pattern is the foundation of the **Saga Pattern** for
distributed transactions. A saga is a sequence of local transactions, each
publishing an event that triggers the next step. If any step fails, compensating
transactions roll back prior steps.

### Choreography-Based Saga

Each service reacts to events and publishes its own next-step events:

```sql
-- Order service: saga step 1 — emit InventoryReservationRequested on OrderCreated
CREATE OR REPLACE FUNCTION trigger_saga_step_after_order()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO outbox_events (aggregate_id, event_type, payload)
    VALUES (
        NEW.id,
        'InventoryReservationRequested',
        jsonb_build_object(
            'order_id', NEW.id,
            'items',    NEW.items,
            'saga_id',  NEW.saga_id,
            'step',     1
        )
    );
    RETURN NEW;
END;
$$;
```

### Compensating Transaction Events

```sql
-- Track which saga steps need compensation if a later step fails
CREATE TABLE saga_compensation_events (
    saga_id        UUID NOT NULL,
    step           INTEGER NOT NULL,
    aggregate_id   UUID NOT NULL,
    compensate_fn  TEXT NOT NULL,  -- e.g., 'ReleaseInventory'
    compensated_at TIMESTAMPTZ,
    PRIMARY KEY (saga_id, step)
);

-- Stream table: sagas awaiting compensation (in reverse step order)
SELECT pgtrickle.create_stream_table(
    'sagas_needing_compensation',
    $$SELECT s.saga_id, s.step, s.compensate_fn, s.aggregate_id
      FROM saga_compensation_events s
      JOIN orders o ON o.saga_id = s.saga_id
      WHERE o.status = 'failed'
        AND s.compensated_at IS NULL
      ORDER BY s.step DESC$$,
    schedule => '5s',
    refresh_mode => 'DIFFERENTIAL'
);
```

### Orchestration-Based Saga

A central orchestrator drives the saga state machine:

```sql
CREATE TABLE order_saga_state (
    saga_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id     UUID REFERENCES orders(id),
    current_step TEXT NOT NULL DEFAULT 'STARTED',
    -- STARTED → INVENTORY_RESERVED → PAYMENT_CHARGED → COMPLETED
    -- STARTED → INVENTORY_RESERVATION_FAILED → COMPENSATING → COMPENSATED
    failed_step  TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Orchestrator polls this stream table to decide the next action
SELECT pgtrickle.create_stream_table(
    'saga_pending_actions',
    $$SELECT s.saga_id, s.order_id, s.current_step, o.amount, o.customer_id
      FROM order_saga_state s
      JOIN orders o ON o.id = s.order_id
      WHERE s.current_step NOT IN ('COMPLETED', 'COMPENSATED')$$,
    schedule => '2s',
    refresh_mode => 'DIFFERENTIAL'
);
```

### Saga Timeout Detection

```sql
SELECT pgtrickle.create_stream_table(
    'stalled_sagas',
    $$SELECT saga_id, order_id, current_step,
             EXTRACT(EPOCH FROM (now() - updated_at)) AS stuck_seconds
      FROM order_saga_state
      WHERE current_step NOT IN ('COMPLETED', 'COMPENSATED')
        AND updated_at < now() - INTERVAL '10 minutes'$$,
    schedule => '60s',
    refresh_mode => 'DIFFERENTIAL'
);
```

---

## Multi-Tenancy

In multi-tenant SaaS applications, the outbox pipeline must isolate tenant data
and allow independent scaling per tenant.

### Tenant-Per-Row Isolation

```sql
-- Add tenant_id to all relevant tables
ALTER TABLE orders        ADD COLUMN tenant_id UUID NOT NULL;
ALTER TABLE outbox_events ADD COLUMN tenant_id UUID NOT NULL;

-- Row-Level Security to prevent cross-tenant access
ALTER TABLE outbox_events ENABLE ROW LEVEL SECURITY;
CREATE POLICY outbox_tenant_isolation ON outbox_events
    USING (tenant_id = current_setting('app.tenant_id')::UUID);

-- Tenant-scoped stream table (relay sets app.tenant_id per connection)
SELECT pgtrickle.create_stream_table(
    'pending_outbox_tenant_scoped',
    $$SELECT event_id, tenant_id, aggregate_id, event_type, payload
      FROM outbox_events
      WHERE published_at IS NULL$$,
    schedule => '1s',
    refresh_mode => 'DIFFERENTIAL'
);
```

### Tenant-Per-Schema Isolation

For stronger isolation, give each tenant its own schema:

```sql
-- Create a tenant-specific outbox
CREATE SCHEMA tenant_abc;
CREATE TABLE tenant_abc.outbox_events (LIKE public.outbox_events INCLUDING ALL);

-- Tenant-specific stream table
SELECT pgtrickle.create_stream_table(
    'tenant_abc.pending_outbox',
    $$SELECT * FROM tenant_abc.outbox_events WHERE published_at IS NULL$$,
    schedule => '1s',
    refresh_mode => 'DIFFERENTIAL'
);
```

### Multi-Tenant Relay Routing

Route events to tenant-specific broker subjects:

```python
async def relay_event(event: dict):
    tenant_id = event["tenant_id"]
    subject = f"tenant.{tenant_id}.{event['event_type'].lower()}"
    # e.g., "tenant.abc123.ordercreated"
    await js.publish(subject, json.dumps(event).encode(), headers={
        "Nats-Msg-Id": str(event["event_id"]),  # JetStream deduplication
    })
```

---

## Cost Analysis

Understanding the operational cost of the outbox pipeline helps choose the
right implementation approach.

### Storage Cost

| Component | Typical Size per Event | Notes |
|-----------|----------------------|-------|
| `outbox_events` row | ~200–500 bytes | JSONB payload dominates |
| CDC buffer row | ~100–300 bytes | Typed columns, no JSONB overhead |
| Stream table | ~same as source rows | Materialized view |
| Index overhead | ~40% of table size | Typical for BTREE indexes |

**Example:** 1 million events/day × 400 bytes = ~400 MB/day. With 7-day
retention: ~2.8 GB. With monthly partitioning, expired partitions drop
instantly via `DETACH PARTITION` + `DROP TABLE`.

### Refresh CPU Overhead

| Mode | CPU Impact | Latency | Use When |
|------|------------|---------|----------|
| DIFFERENTIAL | Low (only changed rows) | ~50–200 ms | Default; > 99% of cases |
| FULL | High (full table scan) | seconds–minutes | Forced by complex aggregations |
| IMMEDIATE | Minimal (synchronous) | ~0 ms additional | Zero-lag requirement |

### Infrastructure Comparison

| Setup | Additional Infrastructure | Estimated Monthly Cost (small–medium) |
|-------|--------------------------|----------------------------------------|
| PostgreSQL only | None | $0 additional |
| PostgreSQL + NATS | ~10 MB binary, single process | ~$5–20/mo (small VM or managed) |
| PostgreSQL + Kafka | KRaft cluster + brokers | $50–500/mo (managed) |
| PostgreSQL + Debezium | Kafka + Kafka Connect | $100–1 000/mo (managed) |

> **Note:** NATS offers near-Kafka throughput at a fraction of the operational
> cost. For fewer than 100 000 events/sec, NATS JetStream is usually sufficient.

---

## Security Considerations

Protecting event data across the outbox pipeline requires attention at every
boundary.

### Payload Encryption for Sensitive Data

Never store PII or secrets in plaintext within the outbox payload:

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Encrypt sensitive fields in the outbox capture trigger
CREATE OR REPLACE FUNCTION outbox_capture_order() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO outbox_events (aggregate_id, event_type, payload)
    VALUES (
        NEW.id,
        'OrderCreated',
        jsonb_build_object(
            'order_id',    NEW.id,
            'amount',      NEW.amount,
            -- Encrypt PII at rest; relay decrypts before publishing
            'customer_pii', encode(
                pgp_sym_encrypt(
                    (NEW.customer_email || '|' || NEW.shipping_address)::bytea,
                    current_setting('app.encryption_key')
                ),
                'base64'
            )
        )
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

### Database-Level Access Controls

```sql
-- Dedicated role for the relay process — minimal privileges
CREATE ROLE outbox_relay LOGIN PASSWORD '...';
GRANT USAGE ON SCHEMA public TO outbox_relay;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM outbox_relay;
GRANT SELECT, UPDATE ON outbox_events TO outbox_relay;
GRANT SELECT ON pending_outbox TO outbox_relay;
```

### Broker Credential Management

- Never hardcode broker credentials in application code.
- Use environment variables, HashiCorp Vault, AWS Secrets Manager, or
  Kubernetes Secrets.
- Rotate credentials regularly. NATS supports
  [NKey authentication](https://docs.nats.io/running-a-nats-service/configuration/securing_nats/auth_intro/nkey_auth)
  with Ed25519 keypairs for zero-secret broker authentication.

### TLS in Transit

```python
import ssl, nats

# Always use TLS for broker connections in production
nc = await nats.connect(
    servers=["tls://nats.example.com:4222"],
    tls=ssl.create_default_context(cafile="/etc/ssl/nats-ca.pem"),
    tls_hostname="nats.example.com",
    user_credentials="/etc/nats/relay.creds",
)
```

### Immutable Audit Log

```sql
-- Record every relay attempt; never UPDATE or DELETE rows in this table
CREATE TABLE outbox_relay_log (
    id          BIGSERIAL PRIMARY KEY,
    event_id    BIGINT NOT NULL REFERENCES outbox_events(event_id),
    relay_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    outcome     TEXT NOT NULL CHECK (outcome IN ('success', 'failure', 'duplicate')),
    broker_ack  TEXT,   -- broker-returned message ID or error
    relay_host  TEXT    -- for multi-relay-process setups
);

-- Append-only policy: deny UPDATE and DELETE
ALTER TABLE outbox_relay_log ENABLE ROW LEVEL SECURITY;
CREATE POLICY relay_log_insert_only ON outbox_relay_log
    FOR INSERT WITH CHECK (TRUE);
-- No UPDATE or DELETE policy = denied by default
```

---

## Disaster Recovery & Backup-Restore

The outbox pattern has a critical edge case during disaster recovery that
can silently cause duplicate event publishing.

### The Backup-Restore Trap

When restoring PostgreSQL from a backup (e.g., `pg_basebackup`, PITR, or
a cloud snapshot):

1. Backup was taken at time T₀.
2. Between T₀ and the restore point, the relay published events E₁..Eₙ to
   the broker and marked them `published_at IS NOT NULL`.
3. After restore, the database is back at T₀ — those events show
   `published_at IS NULL` again.
4. The relay re-publishes E₁..Eₙ → **duplicates at the broker**.

```
Timeline:
  T₀ (backup)  ────── T₁ (crash) ────── T₂ (restore to T₀)
                 ↑                         ↑
          Events E₁..Eₙ published    E₁..Eₙ now appear
          and marked done            unpublished again
                                     → relay re-publishes
```

### Mitigation Strategies

| Strategy | How | Trade-off |
|----------|-----|----------|
| **Broker-level dedup** | `Nats-Msg-Id` / Kafka idempotent producer | Best defense; requires deterministic event IDs |
| **Consumer idempotency** | `ON CONFLICT (event_id) DO NOTHING` at inbox | Always needed anyway |
| **High-watermark table** | Store `last_published_event_id` in a broker-side checkpoint | Requires broker-to-DB feedback loop |
| **Outbox watermark in WAL** | After restore, compare broker's latest known ID with DB state | Complex but definitive |

### Recommended: Deterministic Event IDs

Make event IDs deterministic so that re-publishing after restore produces the
same IDs:

```sql
-- Instead of BIGSERIAL (which resets on restore), use a content-derived UUID:
CREATE OR REPLACE FUNCTION fn_deterministic_event_id(
    aggregate_id UUID,
    event_type   TEXT,
    version      INTEGER
) RETURNS UUID AS $$
    SELECT uuid_generate_v5(
        'a1b2c3d4-e5f6-7890-abcd-ef1234567890'::UUID,  -- namespace
        aggregate_id::text || event_type || version::text
    );
$$ LANGUAGE SQL IMMUTABLE;
```

With deterministic IDs, re-publishing after restore produces the same events
that the broker already has — dedup handles them transparently.

### Point-in-Time Recovery (PITR) Considerations

- **Recovery target:** Always recover to a consistent point *after* the last
  completed relay batch, if possible.
- **WAL archiving:** Ensure WAL segments are archived frequently enough that
  recovery can reach a point close to the crash time.
- **Logical replication slots:** If using WAL CDC mode, the replication slot
  position is part of the database state — it also rolls back on restore.
  Debezium/WAL-tailing relays need to handle this.

### Replica Promotion / Failover

When promoting a standby to primary:

1. The new primary's outbox state matches the last replicated position.
2. If the relay was connected to the old primary, it must reconnect.
3. **Risk:** Async replication lag means the new primary might be *behind*
   the old primary — some published events may not exist yet.
4. **Fix:** Use synchronous replication for the outbox table, or accept
   duplicate re-publishing with broker-level dedup.

---

## Operational Runbooks

Practical procedures for common operational scenarios.

### Graceful Relay Shutdown

```python
import signal, asyncio

class OutboxRelay:
    def __init__(self):
        self.running = True
        self.current_batch = None
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        """Drain current batch before exiting."""
        self.running = False
        # Do NOT abort mid-batch — let the current batch complete

    async def run(self):
        while self.running:
            self.current_batch = await self.fetch_batch()
            if self.current_batch:
                await self.publish_and_mark(self.current_batch)
            else:
                await asyncio.sleep(0.5)

        # Final drain: publish any remaining batch
        if self.current_batch:
            await self.publish_and_mark(self.current_batch)
        print("Relay shut down gracefully.")
```

### pg_trickle Upgrade Procedure

When upgrading pg_trickle, stream tables may need recreation:

```bash
# 1. Stop the relay process
systemctl stop outbox-relay

# 2. Upgrade pg_trickle
ALTER EXTENSION pg_trickle UPDATE;

# 3. Verify stream tables are intact
SELECT pgt_name, pgt_status, pgt_refresh_mode
  FROM pgtrickle.pgt_stream_tables;

# 4. If stream tables need recreation (rare, noted in release notes):
SELECT pgtrickle.drop_stream_table('pending_outbox_events');
SELECT pgtrickle.create_stream_table('pending_outbox_events', ...);

# 5. Restart the relay
systemctl start outbox-relay
```

### Kubernetes Deployment Model

| Model | Pros | Cons |
|-------|------|------|
| **Sidecar** (relay in same pod as app) | Shares DB connection pool, simple lifecycle | Scales with app, not with outbox depth |
| **Standalone Deployment** (relay as separate service) | Scales independently, multiple replicas | Extra service to manage |
| **CronJob** (relay as periodic batch job) | Simple, no long-running process | Higher latency, no event-driven wake |
| **pg_trickle BGW** (proposed Extension 2) | Zero external processes | Requires pg_trickle extension support |

**Recommended:** Standalone Deployment with `SKIP LOCKED` for competing
instances, plus a Kubernetes `readinessProbe` that checks the relay's
health endpoint.

### Stuck Events Investigation

```sql
-- Find events stuck for more than 5 minutes
SELECT event_id, event_type, aggregate_id, created_at,
       EXTRACT(EPOCH FROM (now() - created_at)) AS age_seconds
FROM outbox_events
WHERE published_at IS NULL
  AND created_at < now() - INTERVAL '5 minutes'
ORDER BY created_at
LIMIT 20;

-- Check if the stream table is refreshing
SELECT pgtrickle.get_staleness('pending_outbox_events');

-- Force a manual refresh
SELECT pgtrickle.refresh('pending_outbox_events');
```

---

## Migration Guide

Migrating from a hand-rolled outbox to pg_trickle without downtime.

### Prerequisites

- Existing outbox table (e.g., `domain_events` with `published` column)
- Existing relay process (e.g., cron job or custom poller)
- pg_trickle installed and `pg_trickle.enabled = true`

### Step 1: Add pg_trickle CDC (Non-Destructive)

```sql
-- Register the existing outbox table as a pg_trickle source
-- This adds a CDC trigger but does NOT change existing behavior
SELECT pgtrickle.create_stream_table(
    'pending_outbox_v2',
    $$SELECT event_id, event_type, aggregate_id, payload, created_at
      FROM domain_events
      WHERE published = false$$,
    schedule => '1s',
    refresh_mode => 'DIFFERENTIAL'
);
```

Your existing relay still works — `domain_events` is unchanged.

### Step 2: Shadow Read (Validate)

Run both the old relay and a new relay reading from `pending_outbox_v2` in
**read-only mode** (log events but don't publish):

```python
async def shadow_relay():
    rows = await conn.fetch("SELECT * FROM pending_outbox_v2 ORDER BY event_id")
    for row in rows:
        logger.info(f"Shadow relay would publish: {row['event_id']}")
        # Verify: does this match what the old relay published?
```

### Step 3: Cut Over

```bash
# Stop the old relay
systemctl stop old-outbox-relay

# Start the new pg_trickle-based relay
systemctl start new-outbox-relay
```

### Step 4: Clean Up

```sql
-- Once confident, drop old indexes/columns that are no longer needed
-- (keep the table structure if it's still the source of events)
```

### Blue/Green Migration

For zero-downtime migration with verification:

1. **Green:** New relay reads from pg_trickle stream table.
2. **Blue:** Old relay reads from `domain_events` directly.
3. Run both simultaneously — green publishes, blue validates.
4. Once green is stable for 24h, decommission blue.
5. Both relays use broker-level dedup, so parallel publishing is safe.

---

## Comparison with Traditional Approaches

| Aspect | Traditional Outbox | pg_trickle Outbox | Debezium CDC | pg_trickle + NATS JetStream |
|--------|--------------------|-------------------|--------------|-----------------------------|
| Additional write overhead | 1 extra INSERT per event | Zero (CDC trigger is automatic) | Zero (WAL tail) | Zero (CDC trigger is automatic) |
| Relay mechanism | Custom polling worker | Stream table + NOTIFY | Kafka Connect | Stream table + NATS publish |
| Latency | Depends on poll interval | ~1s (DIFFERENTIAL) or ~0ms (IMMEDIATE) | ~1-5s | ~1s (DIFFERENTIAL) + <1ms (NATS) |
| Ordering | PK-ordered | PK-ordered + LSN-ordered | LSN-ordered | Per-subject ordered (JetStream) |
| Infrastructure | PostgreSQL only | PostgreSQL only | PostgreSQL + Kafka + Debezium | PostgreSQL + NATS (~10 MB binary) |
| Monitoring | Custom | Built-in (staleness, alerts, stats) | Kafka + Debezium metrics | pg_trickle alerts + NATS metrics |
| Competing consumers | FOR UPDATE SKIP LOCKED | FOR UPDATE SKIP LOCKED or pgmq | Kafka consumer groups | JetStream work queues |
| Garbage collection | Manual DELETE/partition | Manual or partitioned | Kafka retention | JetStream retention policies |
| Deduplication | Manual | Manual | Kafka idempotent producer | Built-in (`Nats-Msg-Id`) |

---

## References

- Chris Richardson, [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)
- Microsoft, [Implement the Transactional Outbox Pattern](https://learn.microsoft.com/en-us/azure/architecture/best-practices/transactional-outbox-cosmos)
- [pgmq — PostgreSQL Message Queue](https://github.com/pgmq/pgmq)
- [Debezium CDC Platform](https://debezium.io/)
- [NATS.io — Cloud-Native Messaging](https://nats.io/)
- [NATS JetStream Documentation](https://docs.nats.io/nats-concepts/jetstream)
- [pgnats — PostgreSQL extension for NATS messaging](https://github.com/luxms/pgnats) (MIT, Rust/pgrx)
- [CloudEvents — CNCF Event Data Specification](https://cloudevents.io/)
- [AsyncAPI — Event-Driven API Documentation](https://www.asyncapi.com/)
- Krzysztof Atłasik, [Microservices 101: Transactional Outbox and Inbox](https://softwaremill.com/microservices-101/)
- pg_trickle [ARCHITECTURE.md](../../docs/ARCHITECTURE.md), [PATTERNS.md](../../docs/PATTERNS.md), [SQL_REFERENCE.md](../../docs/SQL_REFERENCE.md)
