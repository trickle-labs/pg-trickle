# Downstream Consumer Patterns for Stream Tables

**Type:** REPORT · **Status:** Exploration · **Date:** 2026-03-03

## Problem Statement

Analysts query stream tables directly via SQL — that's the simple case. But a
growing class of consumers wants to be **triggered** when a stream table
changes: push data to a cache layer, invalidate a CDN, update a search index,
notify a microservice, or feed a downstream event pipeline.

Today, pg_trickle's value proposition stops at the materialized table. Getting
changes *out* of that table to external systems is left to the user. This
report explores what exists, what's missing, and what options make sense.

---

## What Already Exists

### 1. LISTEN/NOTIFY Alerts

pg_trickle emits PostgreSQL notifications on two channels:

**`pg_trickle_alert`** — operational events (JSON payloads):

| Event | Payload Fields | When |
|-------|---------------|------|
| `refresh_completed` | `stream_table`, `action`, `rows_inserted`, `rows_deleted`, `duration_ms` | After every successful refresh |
| `refresh_failed` | `stream_table`, `action`, `error` | After a failed refresh |
| `stale_data` | `stream_table`, `staleness_seconds` | Staleness exceeds 2× schedule |
| `auto_suspended` | `stream_table`, `consecutive_errors` | ST auto-suspended |
| `resumed` | `stream_table` | ST resumed |
| `reinitialize_needed` | `stream_table` | Upstream DDL detected |

**`pgtrickle_refresh`** — emitted after FULL refresh when user triggers are
present (contains `stream_table`, `schema`, `mode`, `rows`).

**Verdict:** Good for "something changed" signals. No row-level detail — the
consumer knows *that* data changed and aggregate counts, but not *what* changed.

### 2. User Triggers on Stream Tables (Implemented)

When a DIFFERENTIAL-mode ST has user-defined row-level triggers, the refresh
engine switches from `MERGE` to explicit `DELETE` → `UPDATE` → `INSERT`
statements. This gives correct `TG_OP`, `OLD`, and `NEW` values.

- Controlled by GUC: `pg_trickle.user_triggers` = `auto` | `on` | `off`
- Works for DIFFERENTIAL mode only
- FULL refresh **suppresses** user triggers (`DISABLE TRIGGER USER`) and emits
  a `NOTIFY pgtrickle_refresh` instead
- Performance overhead: ~25–60% vs MERGE for triggered STs; zero for
  non-triggered STs

**Verdict:** The most powerful existing mechanism. Users can write an
`AFTER INSERT/UPDATE/DELETE` trigger that does anything: insert into an audit
table, call `pg_notify()` with row data, invoke `pg_net` for HTTP, etc.

### 3. Logical Replication from Stream Tables

Stream tables are standard heap tables. Users can set up logical replication:

```sql
CREATE PUBLICATION my_pub FOR TABLE public.order_totals;
-- Then: CREATE SUBSCRIPTION ... on the subscriber
```

Debezium, pgoutput, wal2json all work — stream tables are just tables.

**Verdict:** Works out of the box for systems that already consume PG logical
replication. No pg_trickle-specific setup needed. But adds operational
complexity (replication slots, WAL retention) and doesn't work for non-PG
consumers without a connector.

---

## Consumer Personas & Needs

| Persona | Needs | Latency | Volume | Best Existing Fit |
|---------|-------|---------|--------|-------------------|
| **Analyst / BI tool** | Query latest data | Minutes–hours | Bulk reads | Direct SQL — solved |
| **Dashboard / cache** | Know when to refresh | Seconds | Signal only | `LISTEN pg_trickle_alert` |
| **Microservice** | Get changed rows | Seconds | Row-level | User triggers → `pg_notify()` |
| **Search indexer** | Get inserts/updates/deletes | Seconds–minutes | Row-level | User triggers → audit table |
| **Event pipeline (Kafka)** | Structured change feed | Sub-second–seconds | Row-level | Logical replication or user trigger |
| **API / webhook** | HTTP push on change | Seconds | Row-level | User trigger → `pg_net` |
| **Audit / compliance** | Complete change history | N/A | Row-level + temporal | No good fit today |

---

## Gaps

### Gap 1: No Row-Level Change Details in NOTIFY

`refresh_completed` tells you "42 rows inserted, 3 deleted in `order_totals`"
but not which rows. A consumer that only cares about orders above $10,000 must
re-query the entire table to find out.

### Gap 2: FULL Refresh Is a Blind Spot

User triggers are suppressed during FULL refresh for correctness (TRUNCATE +
INSERT would fire INSERT triggers for every row, creating a false "all rows are
new" signal). The only notification is a bulk `NOTIFY`. Consumers must treat
FULL refresh as a complete invalidation — "re-read everything."

### Gap 3: No Built-In Change Audit Table for STs

The CDC layer maintains `pgtrickle_changes.changes_{source_oid}` for *source*
tables. There is no equivalent for *stream* tables. A consumer wanting "what
changed in the ST in the last 5 minutes" has no structured feed to query.

### Gap 4: No Transactional Outbox

For event-driven architectures, the transactional outbox pattern guarantees
that events are produced exactly when data changes commit. Today, user triggers
run inside the refresh transaction (good for atomicity), but there's no
pg_trickle-managed outbox table with guaranteed delivery semantics.

### Gap 5: No External Push Integration

No webhooks, no Kafka producer, no NATS publisher. All external push requires
user-written triggers or an external connector (Debezium on logical
replication). This is arguably the right boundary for a PostgreSQL extension —
but it's a gap that users will hit.

---

## Approaches

### Approach A: Enhanced NOTIFY with Change Summary

Extend the existing `refresh_completed` notification to include actionable
metadata: affected primary key values or a change summary.

```json
{
  "event": "refresh_completed",
  "stream_table": "public.order_totals",
  "action": "DIFFERENTIAL",
  "changes": {
    "inserted": 42,
    "updated": 7,
    "deleted": 3,
    "affected_pks": [101, 205, 308, "..."],
    "truncated": true
  },
  "data_timestamp": "2026-03-03T10:15:00Z",
  "duration_ms": 230
}
```

**Pros:** Zero infrastructure — pure PG NOTIFY. Consumers get enough detail to
do targeted re-queries. Simple to implement (collect PKs during refresh).

**Cons:** NOTIFY payload is limited to 8000 bytes. For large changesets,
the PK list must be truncated (with a `truncated: true` flag). Not suitable
for row-level detail.

**Effort:** Low.

---

### Approach B: Output Change Buffer Tables

Mirror the input CDC pattern: maintain a
`pgtrickle_changes.output_{st_oid}` table that captures the delta applied
to each ST during refresh.

```sql
CREATE TABLE pgtrickle_changes.output_12345 (
    change_id   BIGSERIAL,
    refresh_id  BIGINT,     -- FK to pgt_refresh_history
    action      CHAR(1),    -- I/U/D
    pk_hash     BIGINT,
    row_data    JSONB,      -- or typed columns
    created_at  TIMESTAMPTZ DEFAULT now()
);
```

Populated automatically during each DIFFERENTIAL refresh:
- INSERT rows → `action = 'I'`, `row_data = NEW`
- UPDATE rows → `action = 'U'`, `row_data = NEW` (with `old_data` for the
  previous version)
- DELETE rows → `action = 'D'`, `row_data = OLD`
- FULL refresh → special `action = 'F'` (full invalidation marker)

Consumers poll the output buffer:

```sql
SELECT * FROM pgtrickle_changes.output_12345
 WHERE change_id > :last_seen_id
 ORDER BY change_id;
```

**Pros:**
- Structured, queryable change feed — works for any consumer.
- Survives connection drops (unlike NOTIFY, which is ephemeral).
- Supports replay and rewind.
- Can be combined with logical replication on the output buffer itself for
  streaming to Kafka/Debezium.
- Natural fit with the existing `pgtrickle_changes` schema.
- FULL refresh can log a single "full invalidation" row rather than
  re-logging every row.

**Cons:**
- Storage overhead — every ST change is written twice (to the ST and to the
  output buffer).
- Cleanup responsibility — who deletes consumed output rows? Need a consumer
  tracking mechanism (similar to `tracked_by_pgt_ids` for input CDC).
- Adds write amplification to every refresh.
- JSONB serialization cost for `row_data` (or complexity of typed columns).

**Effort:** Medium.

---

### Approach C: Change Feed via AFTER Triggers (Document the Pattern)

Rather than building output CDC into the engine, **document and optimize the
user-trigger pattern** as the official recommendation. Provide helper functions
and examples.

```sql
-- pg_trickle provides a helper to create an output change table
SELECT pgtrickle.create_change_feed('public.order_totals');
-- Creates: pgtrickle_changes.feed_order_totals (change_id, action, row_data, ...)
-- Installs: AFTER INSERT/UPDATE/DELETE trigger on public.order_totals

-- Consumers read:
SELECT * FROM pgtrickle_changes.feed_order_totals
 WHERE change_id > :cursor
 ORDER BY change_id;

-- Cleanup:
SELECT pgtrickle.cleanup_change_feed('public.order_totals', older_than => interval '1 hour');
```

**Pros:**
- Builds on existing, working user-trigger infrastructure.
- Opt-in per ST — no overhead for STs that don't need it.
- Helper functions reduce boilerplate.
- Clear separation: pg_trickle maintains the ST, the feed is a user-space
  concern managed by pg_trickle-provided utilities.
- Works today (user triggers are implemented).

**Cons:**
- Still has the FULL refresh blind spot (triggers suppressed).
- ~25–60% overhead for triggered STs.
- Not truly "built-in" — it's a convenience wrapper around existing PG
  mechanisms.
- Consumer tracking / cleanup is still manual (or semi-automated).

**Effort:** Low–Medium.

---

### Approach D: pg_trickle as Logical Replication Publisher

Use PostgreSQL's logical replication protocol to publish changes from stream
tables. Create a publication automatically for each ST.

```sql
-- Automatic on creation:
CREATE PUBLICATION pgtrickle_order_totals FOR TABLE public.order_totals;

-- Consumers use standard PG subscriptions, Debezium, etc.
```

pg_trickle would manage the publication lifecycle (create on ST creation, drop
on ST drop) and document the pattern.

**Pros:**
- Standard PostgreSQL protocol — works with all PG-compatible consumers
  (Debezium, Kafka Connect, pglogical, etc.).
- No custom storage or triggers needed.
- Leverages PG's built-in WAL infrastructure.
- Familiar to users who already know logical replication.

**Cons:**
- Requires `wal_level = logical` — not all deployments have this.
- FULL refresh does TRUNCATE + INSERT, which generates WAL for every row
  (potentially huge WAL spikes).
- Replication slots consume WAL until all subscribers consume — risk of
  WAL bloat if a subscriber is slow.
- pg_trickle doesn't control the consumer side — debugging delivery issues
  is outside the extension's scope.
- Publication management adds complexity to create/drop lifecycle.

**Effort:** Low (publication management) to Medium (documentation + guardrails).

---

### Approach E: Outbox Table with Delivery Guarantees

Implement a transactional outbox pattern: changes written to an outbox table
in the same transaction as the ST refresh, then a delivery worker processes
the outbox and pushes to configured destinations.

This is the most complete solution but essentially turns pg_trickle into a
small event broker — likely out of scope for a PostgreSQL extension.

**Effort:** High. Likely post-1.0 / external sidecar territory.

---

## Comparison Matrix

| Criterion | A: NOTIFY++ | B: Output Buffer | C: Feed Helper | D: Pub/Sub | E: Outbox |
|-----------|:---:|:---:|:---:|:---:|:---:|
| Row-level detail | ❌ (PKs only) | ✅ | ✅ | ✅ | ✅ |
| Survives disconnect | ❌ | ✅ | ✅ | ✅ | ✅ |
| Works with FULL refresh | ⚠️ Signal only | ✅ Marker row | ❌ Suppressed | ✅ (WAL) | ✅ |
| No extra storage | ✅ | ❌ | ❌ | ✅ (WAL) | ❌ |
| Opt-in per ST | ✅ | ✅ | ✅ | ✅ | ✅ |
| No `wal_level = logical` | ✅ | ✅ | ✅ | ❌ | ✅ |
| Standard PG consumers | ❌ (NOTIFY-only) | ❌ (custom) | ❌ (custom) | ✅ | ❌ (custom) |
| Implementation effort | Low | Medium | Low–Med | Low–Med | High |
| Overhead when unused | Zero | Zero | Zero | Low (pub exists) | Low |

---

## Recommendation

A **layered approach** that meets consumers where they are:

### Layer 1: Enhanced NOTIFY (Approach A) — Do First

Extend `refresh_completed` with affected PK list (truncated at 8000 bytes).
Zero-cost for consumers who don't listen. Gives dashboard/cache consumers
enough to do targeted invalidation. Minimal implementation effort.

### Layer 2: Change Feed Helper (Approach C) — Do Second

Provide `pgtrickle.create_change_feed()` / `cleanup_change_feed()` utility
functions that install an AFTER trigger + output table on a stream table.
Opt-in, per-ST, builds on existing user-trigger infrastructure. Solves the
microservice / search indexer persona.

For the FULL refresh blind spot: when a FULL refresh completes and a change
feed exists, insert a single `action = 'F'` (full-invalidation) marker row.
Consumers treat this as "re-read everything" — same semantics as the
existing NOTIFY but now in the durable feed table.

### Layer 3: Logical Replication Docs (Approach D) — Document

Don't build custom pub/sub machinery. Document how to set up PG logical
replication on stream tables for Kafka/Debezium consumers. Provide a
`pgtrickle.create_publication('order_totals')` convenience function that
creates the publication and handles lifecycle (drop on ST drop).

### Layer 4: Output Change Buffer (Approach B) — Evaluate Later

If the change feed helper (Layer 2) proves insufficient — e.g., users need
the delta to include old/new row values without user-trigger overhead, or
need FULL refresh change tracking — then build output change buffers into
the refresh engine itself. This is the "proper" solution but has higher
write amplification cost and should be justified by real demand.

---

## Integration with Blue-Green Deployment

The blue-green report ([REPORT_BLUE_GREEN_DEPLOYMENT.md](REPORT_BLUE_GREEN_DEPLOYMENT.md))
introduces pipeline hot-swapping. How does that interact with downstream
consumers?

| Scenario | Impact |
|----------|--------|
| **LISTEN consumer** | Receives `refresh_completed` for both blue and green STs during transition. After promote, receives events for the promoted ST only. Consumer must filter by `stream_table` name. |
| **User trigger / change feed** | Trigger is on the storage table. After promote (rename), the trigger moves with the table. The feed table continues to receive changes from the now-promoted green ST. |
| **Logical replication** | Publication is on the table name. After `ALTER TABLE RENAME`, PG updates the publication automatically. Subscribers see the renamed table. |
| **Output change buffer** | If implemented, buffer is per-ST-OID. After promote, the green ST's buffer becomes the active feed. The blue ST's buffer is frozen on cleanup. |

Key design rule: **change feed identity should follow the logical name, not
the physical `pgt_id`.** When green is promoted and takes over the name
`order_totals`, the change feed should seamlessly continue under that name.

---

## Open Questions

1. **NOTIFY payload size.** PG's 8000-byte limit constrains PK lists. Should
   we use a summary format (bloom filter, hash ranges) instead of literal PKs?

2. **Change feed cleanup policy.** Who decides when output rows are deleted?
   Options: time-based (configurable retention), cursor-based (consumer
   reports progress), hybrid.

3. **Change feed during FULL refresh.** A single `F` marker works but loses
   row-level granularity. Should FULL refresh optionally populate the feed
   with all rows (expensive but complete)?

4. **Trigger overhead threshold.** At what point does the 25–60% overhead of
   explicit DML (for user triggers) become a problem? Should the engine
   offer a "parallel output" mode that writes to the feed table MERGE-style
   alongside the main MERGE?

5. **Relationship to the transactional IVM plan.** IMMEDIATE mode (v0.2.0
   roadmap) would update STs within the source transaction. Would change
   feeds fire within that same transaction? If so, the outbox pattern (E)
   becomes more natural.

6. **Multi-consumer feed.** Should a single change feed table support multiple
   independent consumers with separate cursors? Or should each consumer get
   its own feed (more isolation, more storage)?

---

## Related Work

- [REPORT_BLUE_GREEN_DEPLOYMENT.md](REPORT_BLUE_GREEN_DEPLOYMENT.md) — pipeline
  hot-swapping (parallel concern).
- Implemented user-trigger support — existing user trigger support (foundation for Layer 2).
- [PLAN_TRANSACTIONAL_IVM.md](../sql/PLAN_TRANSACTIONAL_IVM.md) — immediate-mode
  IVM changes the latency profile for consumers.
- `ROADMAP.md` — planned integrations and post-1.0 directions
  (Airflow, Prometheus, CLI).
- [REPORT_EXTERNAL_PROCESS.md](REPORT_EXTERNAL_PROCESS.md) — sidecar HTTP API
  could host webhook/SSE endpoints (post-1.0).
- Implemented CDC mode transitions
  (input-side analogue of output CDC lifecycle).
