# Transactional Outbox Helper & Consumer Offset Extension

> **Status:** Implementation Plan (DESIGN REVIEW COMPLETE — 2026-04-19)
> **Created:** 2026-04-18
> **Category:** Integration Pattern — Implementation
> **Related:** [PLAN_TRANSACTIONAL_OUTBOX.md](PLAN_TRANSACTIONAL_OUTBOX.md) ·
> [PLAN_OVERALL_ASSESSMENT.md §9.12](https://github.com/trickle-labs/pg-trickle/blob/5ed05167/plans/PLAN_OVERALL_ASSESSMENT.md#912-transactional-outbox-helper-s-effort--1-week) ·
> [ROADMAP v0.28.0 OUTBOX](../../roadmap/v0.28.0.md-full.md)

---

## Table of Contents

- [Executive Summary](#executive-summary)
- [Goals & Non-Goals](#goals--non-goals)
- [Background](#background)
- [Part A — Outbox Helper (v0.24.0)](#part-a--outbox-helper-v0240)
  - [A.1 SQL API](#a1-sql-api)
  - [A.2 Catalog Schema](#a2-catalog-schema)
  - [A.3 Outbox Table Schema](#a3-outbox-table-schema)
  - [A.4 Refresh-Path Integration](#a4-refresh-path-integration)
  - [A.5 Payload Format](#a5-payload-format)
  - [A.6 Retention Management](#a6-retention-management)
  - [A.7 Lifecycle & Drop Cascade](#a7-lifecycle--drop-cascade)
  - [A.8 GUCs](#a8-gucs)
  - [A.9 Failure Handling](#a9-failure-handling)
  - [A.10 Performance Considerations](#a10-performance-considerations)
  - [A.11 Migration & Upgrade](#a11-migration--upgrade)
- [Part B — Consumer Offset Extension (v0.24.0)](#part-b--consumer-offset-extension-v0240)
  - [B.1 Goals](#b1-goals)
  - [B.2 SQL API](#b2-sql-api)
  - [B.3 Catalog Schema](#b3-catalog-schema)
  - [B.4 Polling Semantics](#b4-polling-semantics)
  - [B.5 Heartbeat & Liveness](#b5-heartbeat--liveness)
  - [B.6 Offset Reset & Replay](#b6-offset-reset--replay)
  - [B.7 Monitoring Stream Tables](#b7-monitoring-stream-tables)
  - [B.8 Concurrency & Isolation](#b8-concurrency--isolation)
  - [B.9 Auto-Cleanup of Dead Consumers](#b9-auto-cleanup-of-dead-consumers)
- [Part C — Reference Relay Implementation](#part-c--reference-relay-implementation)
- [Part D — Testing Strategy](#part-d--testing-strategy)
- [Part E — Documentation Plan](#part-e--documentation-plan)
- [Part F — Implementation Roadmap](#part-f--implementation-roadmap)
- [Part G — Open Questions](#part-g--open-questions)
- [Appendix — End-to-End Worked Example](#appendix--end-to-end-worked-example)

---

## Executive Summary

This plan specifies two complementary features for pg_trickle:

1. **Outbox Helper (v0.24.0):** A built-in, minimal-API mechanism that
   captures the per-refresh delta `{inserted, deleted}` of any DIFFERENTIAL
   stream table into a dedicated audit-style table
   `pgtrickle.outbox_<st>`. This eliminates the dual-write problem for
   downstream event buses **without an additional CDC connector or
   replication slot**.

2. **Consumer Offset Extension (v0.24.0):** An optional layer on top of the
   Outbox Helper that adds Kafka-style consumer groups, per-consumer offset
   tracking, lag visibility, heartbeats, and replay primitives so that
   multiple relay processes can safely share a single outbox table with
   first-class operational tooling.

Both features compose: Part A is shippable on its own, gives users an
immediate productivity win, and provides the substrate Part B builds on.

---

## Goals & Non-Goals

### Goals

- **Eliminate dual-writes** for stream-table-driven event publication.
- **Zero additional cost** in the refresh hot path when outbox is disabled.
- **Stable, durable contract**: outbox rows are written in the same
  transaction as the MERGE; either both succeed or neither does.
- **Operationally safe defaults** (24 h retention, automatic cleanup, no
  unbounded growth).
- **Composable with existing relays**: the outbox table is a plain SQL table
  any relay (Python, Go, NATS, Kafka) can consume.
- **Optional consumer-group semantics** for multi-relay deployments without
  forcing the complexity on simple deployments.

### Non-Goals

- pg_trickle does **not** become a message broker. Publishing to NATS, Kafka,
  RabbitMQ, etc. remains the relay's job.
- pg_trickle does **not** maintain consumer-side state by default — only when
  the optional consumer offset extension is enabled.
- The Outbox Helper is **not** a replacement for logical replication or
  Debezium for high-throughput, multi-table CDC topologies.
- IMMEDIATE-mode stream tables with outbox enabled are out of scope for the
  initial release (see Open Questions).

---

## Background

The Transactional Outbox Pattern (see
[PLAN_TRANSACTIONAL_OUTBOX.md](PLAN_TRANSACTIONAL_OUTBOX.md)) guarantees that
domain events are published if and only if the originating transaction
commits. The traditional implementation requires applications to write
*twice*: once to the business table, once to an outbox table. pg_trickle's
DIFFERENTIAL refresh already computes the delta of every change to a stream
table — making it perfectly positioned to write the outbox automatically as
a byproduct of refresh.

This plan formalises that observation into a concrete implementation.

---

## Part A — Outbox Helper (v0.24.0)

### A.1 SQL API

> **Relay integration:** `pgtrickle.set_relay_outbox()` (relay plan A.14)
> calls `enable_outbox()` automatically when creating a new forward pipeline,
> so most users never need to call `enable_outbox()` directly. Use it when
> you want outbox capture without an associated relay pipeline — e.g. for
> direct polling by application code.

```sql
-- Enable outbox capture for a stream table
SELECT pgtrickle.enable_outbox(
    stream_table_name TEXT,            -- name of the stream table
    retention_hours   INT DEFAULT NULL -- override the global GUC if set
) RETURNS void;

-- Disable outbox capture (drops the outbox table and companion delta rows table)
SELECT pgtrickle.disable_outbox(
    stream_table_name TEXT,
    if_exists         BOOLEAN DEFAULT false
) RETURNS void;

-- Inspect outbox status for one or all stream tables
SELECT * FROM pgtrickle.outbox_status(
    stream_table_name TEXT DEFAULT NULL
);
-- Columns: pgt_name, outbox_table, enabled_at, retention_hours,
--          row_count, oldest_row_at, newest_row_at, total_bytes,
--          claim_check_pending_count

-- Called by relay after cursor consumption of a claim-check batch; idempotent.
-- stream_table_name is the pg_trickle stream table name (e.g. 'pending_order_events'),
-- NOT the outbox table name. The function resolves the outbox table via
-- pgt_outbox_config.outbox_table_name, so callers never need to re-derive it.
SELECT pgtrickle.outbox_rows_consumed(
    stream_table_name TEXT,   -- the stream table name as registered in pgt_stream_tables
    outbox_id         BIGINT  -- the id column value from pgtrickle.outbox_<st>
) RETURNS void;
```

#### Argument Validation

| Function | Validation |
|----------|------------|
| `enable_outbox` | Stream table must exist; refresh mode must be `DIFFERENTIAL` or `AUTO` (when AUTO resolves to DIFFERENTIAL); refresh mode must **not** be `IMMEDIATE` — returns `OutboxRequiresNotImmediateMode`; outbox must not already be enabled (idempotent if same retention). |
| `disable_outbox` | Stream table must exist; outbox must be enabled (or `if_exists := true`). |
| `outbox_status` | Returns empty set if argument provided but no match. |

#### Errors

All errors use existing `PgTrickleError` variants:
- `StreamTableNotFound { name }`
- `OutboxAlreadyEnabled { name }` (new variant)
- `OutboxRequiresNotImmediateMode { name }` (new variant) — rejected with message: _"Outbox capture is not supported for IMMEDIATE-mode stream tables. The outbox integrates with DIFFERENTIAL refresh to write one row per refresh cycle within the same transaction. For zero-lag downstream events from IMMEDIATE tables, use LISTEN/NOTIFY or logical replication directly."_
- `OutboxNotEnabled { name }` (new variant)
- `OutboxRequiresDifferential { name }` (new variant)

### A.2 Catalog Schema

A new metadata table tracks per-stream-table outbox configuration:

```sql
CREATE TABLE pgtrickle.pgt_outbox_config (
    pgt_id            UUID PRIMARY KEY REFERENCES pgtrickle.pgt_stream_tables(pgt_id) ON DELETE CASCADE,
    outbox_table_name TEXT NOT NULL UNIQUE,    -- e.g. "outbox_pending_order_events"
    enabled_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    retention_hours   INT,                     -- NULL = use global GUC
    last_drained_at   TIMESTAMPTZ,             -- last successful retention sweep
    last_drained_count BIGINT NOT NULL DEFAULT 0
);
```

This is the source of truth for "is outbox enabled for ST X?" — checked
once per refresh in the hot path via a small in-memory cache.

### A.3 Outbox Table Schema

For a stream table named `pending_order_events`, `enable_outbox()` creates:

```sql
CREATE TABLE pgtrickle.outbox_pending_order_events (
    id              BIGSERIAL PRIMARY KEY,
    pgt_id          UUID NOT NULL,
    refresh_id      UUID NOT NULL,             -- correlates to pgt_refresh_history
    created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    inserted_count  INT NOT NULL,
    deleted_count   INT NOT NULL,
    is_claim_check  BOOLEAN NOT NULL DEFAULT false, -- true when delta > outbox_inline_threshold_rows
    payload         JSONB NOT NULL             -- inline: {v:1, inserted:[…], deleted:[…]}
                                               -- claim-check header: {v:1, claim_check:true, inserted_count:N, deleted_count:N, refresh_id:"…"}
                                               -- full-refresh: adds "full_refresh":true to either form
);

CREATE INDEX idx_outbox_<st>_created_at
    ON pgtrickle.outbox_<st> (created_at);

-- Companion table for claim-check rows (large deltas only)
-- Populated atomically in the same transaction as the parent outbox row.
-- Cascade-deleted when the parent outbox row is deleted by retention drain.
CREATE TABLE pgtrickle.outbox_delta_rows_<st> (
    outbox_id  BIGINT NOT NULL REFERENCES pgtrickle.outbox_<st>(id) ON DELETE CASCADE,
    row_num    INT NOT NULL,
    op         CHAR(1) NOT NULL CHECK (op IN ('I', 'D')),
    payload    JSONB NOT NULL,
    PRIMARY KEY (outbox_id, row_num)
);
```

#### Why These Columns?

| Column | Purpose |
|--------|---------|
| `id` | Monotonic offset; primary key for relay polling (`WHERE id > $last`). |
| `pgt_id` | Links back to `pgt_stream_tables` for joinable diagnostics. |
| `refresh_id` | Correlates to `pgt_refresh_history` for end-to-end tracing. |
| `created_at` | Enables time-based queries and retention drain. |
| `inserted_count` / `deleted_count` | Cheap statistics without parsing JSONB; useful for dashboards. |
| `is_claim_check` | Quick flag for relays; avoids parsing JSONB to decide fetch strategy. |
| `payload` | Inline delta or claim-check header; JSONB for downstream flexibility. |

#### Naming Rules

- Outbox table name = `outbox_` + `<stream_table_name>`, truncated so the
  combined identifier fits within PostgreSQL's 63-byte limit.
- Truncation algorithm: the `outbox_` prefix is 7 bytes; the stream table
  name is truncated to at most **56 bytes**, yielding a maximum 63-byte
  identifier.
- If the resulting name collides with an existing table in the `pgtrickle`
  schema (possible after migrations or short name reuse), a 7-character hex
  suffix derived from `left(md5(stream_table_name), 7)` is appended:
  `outbox_<name_truncated_to_48>_<hex7>` (7 + 48 + 1 + 7 = 63).
- Final name is stored in `pgt_outbox_config.outbox_table_name` so callers
  never need to re-derive it.
- Always lives in schema `pgtrickle`.

#### Latest-Row View (Automatically Created)

Alongside `outbox_<st>`, `enable_outbox()` also creates a **latest-row
view** for quick operational checks and lag monitoring:

```sql
-- Auto-created alongside the outbox table:
CREATE VIEW pgtrickle.pgt_outbox_latest_<st> AS
SELECT *
FROM pgtrickle.outbox_<st>
ORDER BY id DESC
LIMIT 1;
```

> **Note on state-sync consumers:** The outbox is a per-refresh audit log,
> not a per-source-row log. `pgt_id` is the stream table's UUID — every row
> in `outbox_<st>` shares the same `pgt_id`, so `DISTINCT ON (pgt_id)` would
> return only one row. To query the *current state* of all rows in the stream
> table, consumers should query the stream table directly
> (`SELECT * FROM pgtrickle.<st>`), which always contains the current
> materialised state.

| Consumer | Which view to query |
|----------|-----------------------|
| Full audit trail, event sourcing, replay testing | `outbox_<st>` — all rows, in insertion order |
| Quick lag check / last-delta inspection | `pgt_outbox_latest_<st>` — most recent refresh row only |
| Current materialised state for cache sync | Query the stream table: `SELECT * FROM pgtrickle.<st>` |

No extra storage. Dropped automatically when `disable_outbox()` is called.

### A.4 Refresh-Path Integration

The outbox write hooks into the existing refresh transaction in
[`src/refresh.rs`](../../src/refresh.rs) immediately after the MERGE
completes successfully and before `COMMIT`:

```text
fn run_differential_refresh(...) -> Result<RefreshResult> {
    // ... existing delta computation and MERGE ...

    if outbox_enabled_for(pgt_id) {
        write_outbox_row(
            pgt_id,
            refresh_id,
            inserted_rows,    // already in memory
            deleted_rows,
        )?;
    }

    // Same transaction commits MERGE + outbox row atomically
    Ok(result)
}
```

#### Hot-Path Cost When Disabled

- One in-memory hash lookup per refresh: `outbox_enabled_set.contains(pgt_id)`.
- Cache invalidated on any DDL via the existing event-trigger hook.
- Target: < 50 ns per refresh when disabled. Benchmark with `criterion`.

#### Hot-Path Cost When Enabled

- One additional INSERT into `pgtrickle.outbox_<st>` per refresh cycle,
  **unless** `pg_trickle.outbox_skip_empty_delta = true` (default) and
  `inserted_count = 0 AND deleted_count = 0` — in which case no INSERT or
  NOTIFY is issued, saving ~5–10 µs and one outbox row.
- Payload serialisation reuses the in-memory `Vec<DeltaRow>` already
  computed for the MERGE — no extra round trips.
- A single `pg_notify('pgtrickle_outbox_new', outbox_table_name)` is issued
  inside the same transaction so the relay can `LISTEN` for sub-second
  wake-up in addition to polling (see [§A.10 Gap #3 note](#a10-performance-considerations);
  the relay implementation lands in v0.25.0 but the NOTIFY is cheap enough
  to emit from v0.24.0 onwards).

#### FULL-Refresh Fallback

When a stream table is in AUTO mode and pg_trickle falls back to a FULL
refresh (e.g. when the IVM cost exceeds the threshold), the outbox must
still write a correct payload — but its semantics differ from a differential
row:

- The `inserted` array contains **all current rows** of the stream table.
- The `deleted` array is **empty**.
- The payload sets **`"full_refresh": true`** at the top level as a sentinel.
- A `pg_trickle_alert` event of type **`outbox_full_refresh`** is emitted so
  operators and monitoring pipelines can detect the event independently.

Relays **must** inspect `payload.full_refresh` before publishing to a broker.
A relay that treats a FULL-refresh payload as incremental events will publish
every current row as a spurious new event. The reference relay
(`examples/relay/outbox_relay.py`) demonstrates the correct check:

```python
if row["payload"].get("full_refresh"):
    # Apply snapshot semantics: upsert all rows, do not send as new events
    handle_full_refresh_snapshot(row["payload"]["inserted"])
else:
    publish_delta(row["payload"])
```

The `full_refresh` flag is also set on claim-check rows (see A.5) whenever
a FULL-refresh delta exceeds `outbox_inline_threshold_rows`.

### A.5 Payload Format — Two Paths

The JSONB payload follows a stable, versioned schema with **two routing paths**
controlled by `outbox_inline_threshold_rows` (default 10 000 rows):

#### Inline path (delta ≤ threshold)

```json
{
  "v": 1,
  "inserted": [
    { "<col1>": <val>, "<col2>": <val>, ... },
    ...
  ],
  "deleted": [
    { "<col1>": <val>, "<col2>": <val>, ... },
    ...
  ]
}
```

#### Claim-check path (delta > threshold)

When `delta_row_count > outbox_inline_threshold_rows`, no row data is written
into the outbox row itself. Instead the actual rows are batched into
`pgtrickle.outbox_delta_rows_<st>` (1 000 rows per SPI call), and the outbox
row carries only a lightweight header:

```json
{
  "v": 1,
  "claim_check": true,
  "inserted_count": 12345,
  "deleted_count": 0,
  "refresh_id": "<uuid>"
}
```

The relay reads `outbox_delta_rows_<st>` via a server-side cursor in bounded
batches, then calls `outbox_rows_consumed(stream_table, outbox_id)` to signal
completion. **No data is ever silently dropped — there is no truncation path.**

#### FULL-refresh fallback

For FULL-refresh fallbacks (see A.4), the payload additionally includes
`"full_refresh": true`. The inline/claim-check routing still applies based on
current row count vs. threshold:

```json
{ "v": 1, "full_refresh": true, "inserted": [ { ... all current rows ... } ], "deleted": [] }
// or, when row count exceeds threshold:
{ "v": 1, "claim_check": true, "full_refresh": true, "inserted_count": N, "deleted_count": 0, "refresh_id": "..." }
```

Relays must check both `full_refresh` and `claim_check` flags independently.
The reference relay (`examples/relay/outbox_relay.py`) demonstrates all four
combinations: inline/differential, inline/full-refresh, claim-check/differential,
claim-check/full-refresh.

#### Versioning

- Top-level `v` field allows schema evolution without breaking consumers.
- Initial release ships `v: 1`.
- Documented as a stable contract in `docs/SQL_REFERENCE.md`.

#### Row Encoding

- Each row is an object keyed by column name.
- PostgreSQL types are converted via `to_jsonb()` rules (numeric → number,
  text → string, timestamps → ISO 8601 strings, etc.).
- `NULL` values are emitted as JSON `null`.
- Claim-check delta rows in `outbox_delta_rows_<st>` use the same encoding.

### A.6 Retention Management

#### Per-Stream-Table Retention

`enable_outbox(name, retention_hours)` accepts an optional override.
If `NULL`, the global GUC `pg_trickle.outbox_retention_hours` (default 24)
applies.

#### Drain Mechanism

The existing scheduler cleanup phase (see
[`src/scheduler.rs`](../../src/scheduler.rs)) gains a new step that runs
once per cleanup cycle:

```sql
-- PostgreSQL does not support LIMIT on DELETE directly; use a CTE.
-- For claim-check safety: only delete a row if all consumer groups
-- that have polled past this outbox_id have called outbox_rows_consumed().
-- Check pgt_consumer_claim_check_acks completeness before deletion.
WITH rows_to_delete AS (
    SELECT id FROM pgtrickle.outbox_<st> o
    WHERE created_at < now() - INTERVAL '<retention_hours> hours'
      AND (NOT EXISTS (
            SELECT 1 FROM pgtrickle.pgt_consumer_groups cg
          ) OR NOT EXISTS (
            SELECT 1 FROM pgtrickle.pgt_consumer_claim_check_acks aca
            WHERE aca.outbox_id = o.id
              AND NOT EXISTS (
                SELECT 1 FROM pgtrickle.pgt_consumer_offsets co
                WHERE co.group_id = aca.group_id
                  AND co.last_offset >= o.id
              )
          ))
    ORDER BY id
    LIMIT pg_trickle.outbox_drain_batch_size  -- default 10000
)
DELETE FROM pgtrickle.outbox_<st>
WHERE id IN (SELECT id FROM rows_to_delete);
```

- Batched to avoid long locks.
- Loops until the CTE returns < `outbox_drain_batch_size` rows.
- Updates `pgt_outbox_config.last_drained_at` and
  `last_drained_count`.
- **Claim-check safety:** Before deleting a row, verifies that all consumer
  groups that have polled past this `outbox_id` have called
  `outbox_rows_consumed()` (tracked in `pgt_consumer_claim_check_acks`).
  This prevents data loss when relays are cursor-fetching claim-check rows.

#### Edge Cases

- If the consumer offset extension is loaded, retention drain **skips rows
  with offsets greater than the slowest consumer's `last_offset`** to avoid
  data loss for active consumers (see B.6).
- If retention is set to `0` hours, no drain runs (useful for archive-only
  outboxes).

### A.7 Lifecycle & Drop Cascade

| Event | Effect |
|-------|--------|
| `pgtrickle.create_stream_table(...)` | No outbox created; opt-in via `enable_outbox()`. |
| `pgtrickle.enable_outbox(name)` | Creates `pgtrickle.outbox_<st>` and metadata row. |
| `pgtrickle.alter_stream_table(name, ...)` | Outbox preserved if column set unchanged; else `ERROR` requiring `disable_outbox()` first. |
| `pgtrickle.disable_outbox(name)` | Drops the outbox table and metadata row. |
| `pgtrickle.drop_stream_table(name)` | Cascades: outbox metadata row + outbox table dropped automatically. |
| `DROP EXTENSION pg_trickle` | All `pgtrickle.outbox_*` tables dropped via extension membership. |

### A.8 GUCs

#### Part A — Outbox Helper GUCs

| GUC | Type | Default | Description |
|-----|------|---------|-------------|
| `pg_trickle.outbox_enabled` | bool | `true` | Master switch. When `false`, refresh skips outbox writes even if enabled per ST. |
| `pg_trickle.outbox_retention_hours` | int | `24` | Default retention for outbox rows. |
| `pg_trickle.outbox_drain_batch_size` | int | `10000` | Max rows deleted per drain pass. |
| `pg_trickle.outbox_inline_threshold_rows` | int | `10000` | Delta row count at or below which rows are inlined into the outbox `payload` column. Above this threshold the claim-check path is used: rows land in `outbox_delta_rows_<st>` and the relay fetches via server-side cursor. **Replaces** the old `outbox_max_payload_bytes` GUC (removed). |
| `pg_trickle.outbox_claim_check_batch_size` | int | `1000` | Number of delta rows fetched per SPI call on the write side, and per FETCH on the relay cursor side. Decrease for very wide rows (e.g. 100 KB JSONB payloads) to cap per-batch memory; increase for narrow rows to reduce round-trip count. |
| `pg_trickle.outbox_drain_interval_seconds` | int | `60` | Minimum interval between drain passes. |
| `pg_trickle.outbox_storage_critical_mb` | int | `1024` | When the total size of any `outbox_<st>` table exceeds this threshold (in MiB), a `pg_trickle_alert outbox_storage_critical` event is emitted on every cleanup cycle and `outbox_status()` marks the outbox as `degraded`. Intended as an early warning when a dead or slow consumer blocks the retention drain. Set to `0` to disable. |
| `pg_trickle.outbox_skip_empty_delta` | bool | `true` | When `true`, the refresh path skips the outbox INSERT (and NOTIFY) for cycles where `inserted_count = 0` and `deleted_count = 0` — no observable change, nothing to relay. Set to `false` only when relay pipelines require a heartbeat row on every refresh cycle (e.g. for lag monitoring in audit-trail deployments). |

#### Part B — Consumer Group GUCs

| GUC | Type | Default | Description |
|-----|------|---------|-------------|
| `pg_trickle.consumer_dead_threshold_hours` | int | `24` | Hours since last heartbeat before a consumer is considered dead and its leases released. Reduce to `1` for high-criticality systems. |
| `pg_trickle.consumer_stale_offset_threshold_days` | int | `7` | Days since last commit before a dead consumer's offset row is permanently removed. |
| `pg_trickle.consumer_cleanup_enabled` | bool | `true` | Enable scheduler step that reaps dead consumers and releases their leases. |
| `pg_trickle.outbox_force_retention` | bool | `false` | Override the per-consumer retention guard. When `true`, the drain deletes rows regardless of consumer offsets. Use only when a consumer is permanently abandoned. |

All GUCs are `SUSET` (require superuser to change).

### A.9 Failure Handling

| Failure | Behaviour |
|---------|-----------|
| Outbox table missing (DDL drift) | Refresh **fails**; ST marked `ERROR`; alert emitted. Recovery: `disable_outbox()` then `enable_outbox()`. |
| Outbox INSERT fails (e.g. disk full) | Refresh transaction **rolls back**; both MERGE and outbox row reverted — including any partially-written `outbox_delta_rows_<st>` rows. ST returns `ERROR`. Never let outbox and view drift. |
| Delta exceeds `outbox_inline_threshold_rows` | Claim-check path used: header row plus delta rows in `outbox_delta_rows_<st>`, both in the same transaction. No data lost. Relay detects `payload.claim_check = true`, fetches via server-side cursor, then calls `outbox_rows_consumed()`. |
| Drain loop fails | Logged via `pgrx::warning!`; retried next cleanup cycle. Does not block refresh. |
| `pg_trickle.outbox_enabled = false` mid-refresh | Refresh completes normally; subsequent refreshes skip outbox until re-enabled. |
| `enable_outbox()` on IMMEDIATE-mode stream table | Rejected immediately with `OutboxRequiresNotImmediateMode`; no partial state created. |
| Retention drain races with active cursor consumption | The drain **checks** `outbox_rows_consumed()` completion for claim-check rows before deleting the parent row. `ON DELETE CASCADE` on `outbox_delta_rows_<st>` ensures delta rows cannot outlive their parent, so the drain must not delete a parent row whose cursor has not yet been signalled as complete (see §B.6 retention safety guard). |

### A.10 Performance Considerations

#### Write Amplification

For every refresh that enables outbox:
- 1 additional INSERT (~1 KB row + payload size).
- 1 index entry on `created_at`.
- 1 `pg_notify('pgtrickle_outbox_new', ...)` inside the same transaction (negligible — ~2 µs).

Expected overhead at 1 refresh/sec: ~5–10 µs/refresh. Benchmark with
`benches/refresh_bench.rs` extended to cover outbox-enabled and
outbox-disabled cases.

#### Storage Growth

- 86,400 rows/day per ST at 1 Hz refresh.
- Average row size: 200 bytes (header) + payload (typically 1–10 KB).
- ~1 GB/day per ST at modest payload sizes.
- Bounded by retention drain.

> **Multi-ST storage estimate:** With 50 stream tables all at 1 Hz refresh,
> expect ~4.3 M outbox rows/day across all tables. Budget storage accordingly
> and monitor via `outbox_status()`. The `outbox_storage_critical_mb` GUC
> triggers an alert when any single outbox exceeds the threshold.

#### WAL Overhead

Outbox tables are regular (logged) heap tables — every INSERT generates WAL.
**UNLOGGED outbox tables are intentionally not supported**: a crash could
leave the outbox permanently behind the stream table's materialised state,
silently dropping events that were committed to the source table but never
published. Durability of committed changes is non-negotiable.

Expected WAL impact per stream table at 1 Hz:
- ~1–10 KB WAL/refresh (depending on payload size).
- ~86 MB–860 MB WAL/day.
- With 50 outbox-enabled STs: ~4–43 GB WAL/day (dominated by payload size).

To reduce WAL pressure, prefer DIFFERENTIAL mode over AUTO to avoid
FULL-refresh outbox rows (which contain all current ST rows). If WAL
generation is a concern, increase `schedule` on high-frequency STs or
consider disabling outbox and using logical replication/Debezium instead.

#### Claim-Check Batch Size Tuning

The `outbox_claim_check_batch_size` GUC (default 1 000 rows) controls both
the write-side SPI batch size and should match the relay's FETCH batch:

| Payload width | Recommended batch size | Approximate per-batch memory |
|---------------|------------------------|------------------------------|
| < 1 KB/row    | 5 000                  | ~5 MB                        |
| 1–10 KB/row   | 1 000 (default)        | ~1–10 MB                     |
| 10–100 KB/row | 200                    | ~2–20 MB                     |
| > 100 KB/row  | 50                     | ~5–50 MB                     |

#### Storage Alert and Retention Guard Interaction

When a consumer is dead and `outbox_force_retention = false` (default),
the retention drain cannot delete rows past the dead consumer's offset.
Storage grows unboundedly until either the consumer resumes or an operator
sets `outbox_force_retention = true`. The `outbox_storage_critical_mb` GUC
provides an automated alert at a configurable threshold — reducing the
window between a consumer dying and an operator noticing.

Typical operator response to a `outbox_storage_critical` alert:
1. Investigate consumer health via `consumer_lag()` and `pgt_consumer_status`.
2. Either recover the consumer or run `drop_consumer_group()` to release the
   retention guard.
3. If emergency storage reclaim is needed: `SET pg_trickle.outbox_force_retention = true;`
   (requires superuser; reverts automatically to `false` after reload).

#### Bloat Mitigation

- Outbox tables are append-mostly; `autovacuum` settings should be tuned
  via the existing `pgtrickle_changes` template:
  - `autovacuum_vacuum_scale_factor = 0.05`
  - `autovacuum_vacuum_cost_limit = 1000`

### A.11 Migration & Upgrade

#### v0.23.0 → v0.24.0

- New catalog table `pgtrickle.pgt_outbox_config` created via
  `sql/upgrade--0.23.0--0.24.0.sql`.
- New SQL functions registered.
- Existing stream tables unaffected; outbox is opt-in.

#### Downgrade

Not supported in initial release. Users must:
1. `disable_outbox()` for all stream tables.
2. `DROP EXTENSION pg_trickle CASCADE; CREATE EXTENSION pg_trickle VERSION '0.21.0';`.

---

## Part B — Consumer Offset Extension (v0.24.0)

### B.1 Goals

- Support **multiple relay instances** sharing a single outbox table without
  duplicate publication.
- Provide **lag visibility** ("how far behind is each consumer?").
- Enable **replay** for disaster recovery and onboarding new consumers.
- Detect and reclaim resources from **crashed consumers**.
- Stay **opt-in** — simple deployments using `FOR UPDATE SKIP LOCKED` don't
  need this.

### B.2 SQL API

```sql
-- Group lifecycle
SELECT pgtrickle.create_consumer_group(
    group_name        TEXT,
    outbox_table_name TEXT,                        -- e.g. "outbox_pending_order_events"
    auto_offset_reset TEXT DEFAULT 'latest'        -- 'latest' | 'earliest'
) RETURNS UUID;                                    -- group_id

SELECT pgtrickle.drop_consumer_group(group_name TEXT) RETURNS void;

-- Polling (called by relay)
SELECT * FROM pgtrickle.poll_outbox(
    group_name   TEXT,
    consumer_id  TEXT,
    batch_size   INT DEFAULT 100,
    visibility_seconds INT DEFAULT 30              -- like SQS visibility timeout
);
-- Returns: TABLE(id BIGINT, created_at TIMESTAMPTZ, payload JSONB)

-- Commit progress
SELECT pgtrickle.commit_offset(
    group_name   TEXT,
    consumer_id  TEXT,
    last_offset  BIGINT
) RETURNS void;

-- Liveness
SELECT pgtrickle.consumer_heartbeat(
    group_name   TEXT,
    consumer_id  TEXT
) RETURNS void;
-- Updates last_heartbeat_at only. Does NOT extend active leases.
-- Use extend_lease() to renew a lease for a long-running batch.

-- Lease renewal for long-running batches
SELECT pgtrickle.extend_lease(
    group_name        TEXT,
    consumer_id       TEXT,
    extension_seconds INT DEFAULT 30
) RETURNS TIMESTAMPTZ;              -- new visibility_until timestamp
-- Extends visibility_until of ALL active leases held by this consumer.

-- Replay
SELECT pgtrickle.seek_offset(
    group_name   TEXT,
    consumer_id  TEXT,
    new_offset   BIGINT                            -- 0 = beginning, NULL = end
) RETURNS void;

-- Inspection
SELECT * FROM pgtrickle.consumer_lag(
    group_name TEXT DEFAULT NULL                   -- NULL = all groups
);
-- Columns: group_name, consumer_id, last_offset, lag_rows,
--          last_heartbeat, heartbeat_age_seconds, healthy
```

### B.3 Catalog Schema

```sql
CREATE TABLE pgtrickle.pgt_consumer_groups (
    group_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    group_name          TEXT NOT NULL UNIQUE,
    outbox_table_name   TEXT NOT NULL,
    auto_offset_reset   TEXT NOT NULL DEFAULT 'latest'
        CHECK (auto_offset_reset IN ('latest', 'earliest')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE pgtrickle.pgt_consumer_offsets (
    group_id            UUID NOT NULL REFERENCES pgtrickle.pgt_consumer_groups ON DELETE CASCADE,
    consumer_id         TEXT NOT NULL,
    last_offset         BIGINT NOT NULL DEFAULT 0,
    last_commit_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id, consumer_id)
);

CREATE TABLE pgtrickle.pgt_consumer_leases (
    group_id            UUID NOT NULL REFERENCES pgtrickle.pgt_consumer_groups(group_id) ON DELETE CASCADE,
    consumer_id         TEXT NOT NULL,
    leased_id_min       BIGINT NOT NULL,           -- inclusive
    leased_id_max       BIGINT NOT NULL,           -- inclusive
    leased_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    visibility_until    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (group_id, leased_id_min)
);

-- High-churn table: one INSERT per poll_outbox() call, one DELETE per
-- commit_offset(). Tune autovacuum aggressively at sustained poll rates.
ALTER TABLE pgtrickle.pgt_consumer_leases SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_cost_limit   = 2000
);

-- Per-consumer-group claim-check completion tracking (created in Part B / CG-1).
-- Required for retention drain safety: the drain must not delete an outbox
-- row (and cascade-delete its delta rows) until every consumer group that
-- has polled past that outbox_id has signalled cursor consumption complete
-- via outbox_rows_consumed(). Created by create_consumer_group(); dropped
-- when the last consumer group for the outbox is removed.
CREATE TABLE pgtrickle.pgt_consumer_claim_check_acks (
    group_id            UUID NOT NULL REFERENCES pgtrickle.pgt_consumer_groups(group_id) ON DELETE CASCADE,
    outbox_id           BIGINT NOT NULL,           -- FK to outbox_<st>(id) NOT enforced (cross-table)
    consumer_id         TEXT NOT NULL,
    acked_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id, outbox_id, consumer_id)
);
-- Retention drain: a claim-check outbox row may only be deleted when
-- every group that polled past this id has called outbox_rows_consumed() for it.
-- Rows in this table are removed as part of the drain step itself after the
-- parent outbox row is safely deleted.

-- pgt_consumer_claim_check_acks is created in Part B (create_consumer_group()),
-- not here. It is only meaningful when consumer groups are in use, so creating
-- it in Part A would result in a permanently empty table for Part-A-only deployments.
-- See §CG-1 for the DDL. Referenced here for documentation completeness only.
```

### B.4 Polling Semantics

`poll_outbox()` is implemented as a single SQL function that:

1. Validates the group exists and the consumer is registered (auto-registers
   if first call).
2. Acquires a lease over the next `batch_size` outbox rows whose `id` is
   greater than `last_offset` and not currently leased by another consumer:

   ```sql
   WITH next_batch AS (
       SELECT o.id, o.created_at, o.payload
       FROM pgtrickle.outbox_<st> o
       WHERE o.id > (SELECT last_offset FROM pgt_consumer_offsets
                     WHERE group_id = $g AND consumer_id = $c)
         AND NOT EXISTS (
             SELECT 1 FROM pgt_consumer_leases l
             WHERE l.group_id = $g
               AND o.id BETWEEN l.leased_id_min AND l.leased_id_max
               AND l.visibility_until > now()
         )
       ORDER BY o.id
       LIMIT $batch_size
       FOR UPDATE OF o SKIP LOCKED
   )
   INSERT INTO pgt_consumer_leases (group_id, consumer_id,
                                    leased_id_min, leased_id_max,
                                    visibility_until)
   SELECT $g, $c, MIN(id), MAX(id), now() + interval '$visibility_seconds seconds'
   FROM next_batch
   HAVING COUNT(*) > 0
   RETURNING leased_id_min, leased_id_max;
   ```

3. Returns the batch rows.

#### Visibility Timeout

If the relay doesn't call `commit_offset()` within `visibility_seconds`,
the lease expires and the rows become available to other consumers. This
mimics SQS / pgmq semantics.

#### Auto-Registration

First call to `poll_outbox()` for a new `consumer_id` creates a row in
`pgt_consumer_offsets`:
- If `auto_offset_reset = 'latest'`: `last_offset` set to current `MAX(id)`.
- If `auto_offset_reset = 'earliest'`: `last_offset` set to 0.

#### Lease Renewal

If a relay's batch processing takes longer than `visibility_seconds`
(e.g. slow broker publish, large payloads, business logic), it should call
`extend_lease()` before the lease expires to prevent another relay from
re-polling the same rows:

```python
# In relay loop, after poll_outbox() returns a batch:
new_deadline = execute_scalar(
    "SELECT pgtrickle.extend_lease($1, $2, 30)",
    group, consumer_id
)
# Process batch ... publish to broker ...
execute("SELECT pgtrickle.commit_offset($1, $2, $3)", group, consumer_id, max_id)
```

`extend_lease()` updates `visibility_until` for **all** active leases held
by the named consumer. It does **not** affect `last_heartbeat_at` — heartbeat
and lease lifetime are independent concerns.

If the consumer has no active leases (e.g. called after `commit_offset()`),
`extend_lease()` is a no-op and returns the current timestamp.

### B.5 Heartbeat & Liveness

- Relays call `consumer_heartbeat()` periodically (recommended every 10 s).
- A consumer is **healthy** if `last_heartbeat_at > now() - interval '60 seconds'`.
- `consumer_heartbeat()` updates **only** `last_heartbeat_at`; it does **not**
  extend active lease timeouts. Call `extend_lease()` (see B.4) to renew
  leases for long-running batches.
- `consumer_lag()` is a **live SQL function** (executes against `pgt_consumer_offsets`
  directly, always current) suitable for ad-hoc inspection and application
  code health checks. It exposes per-consumer `lag_rows`, `healthy`, and
  `heartbeat_age_seconds`. Use it when you need the current `heartbeat_age_sec`
  or when checking consumer health in application code.
- `pgt_consumer_group_lag` (see B.7) is a **DIFFERENTIAL stream table**
  materialized every 10 s, suitable for Grafana dashboards and alerting rules.
  It shows per-group aggregate lag (min/max offset, row lag) but **not**
  per-consumer heartbeat freshness — use `consumer_lag()` for that.
  Use `consumer_lag()` for programmatic health checks; use the ST for
  monitoring dashboards and time-series alerting.
- A `pg_trickle_alert` event of type `consumer_unhealthy` is emitted when a
  registered consumer transitions from healthy → unhealthy.

### B.6 Offset Reset & Replay

`seek_offset(group, consumer, n)`:
- Sets `last_offset = n` for the named consumer.
- Releases all active leases held by that consumer.
- Emits `pg_trickle_alert` event `consumer_seeked`.

#### Retention Interaction

When the consumer offset extension is loaded, retention drain refuses to
delete rows whose `id > MIN(last_offset across all consumers)`. This
prevents silent data loss for slow consumers.

A separate GUC `pg_trickle.outbox_force_retention` (default `false`) allows
operators to override this safety check when a consumer is permanently
abandoned.

### B.7 Monitoring Stream Tables

Three pg_trickle-managed stream tables are auto-created (idempotently) on the
first `create_consumer_group()` call **for a given outbox**. Each outbox that
has consumer groups gets its own set of monitoring STs (named
`pgt_consumer_status_<outbox>`, etc.). They are dropped when the **last**
consumer group for that outbox is removed via `drop_consumer_group()` (tracked
by a reference counter in `pgt_consumer_groups`). This prevents multiple
different outboxes' lag data from colliding in a single shared view.

Creation uses `IF NOT EXISTS` semantics in `create_stream_table()` so concurrent
or repeated `create_consumer_group()` calls are safe.

```sql
-- Per-consumer status.
-- Uses FULL mode: pgt_consumer_offsets is updated on every heartbeat and
-- offset commit, so most rows change between refreshes at typical relay
-- poll rates. FULL is simpler for this small table (one row per consumer)
-- and avoids the overhead of delta computation for frequently-changing data.
-- Consumers compute heartbeat_age_sec client-side from last_heartbeat_at;
-- for live health status use consumer_lag() instead.
SELECT pgtrickle.create_stream_table(
    'pgt_consumer_status',
    $$SELECT g.group_name, o.consumer_id, o.last_offset,
             o.last_commit_at,
             o.last_heartbeat_at
      FROM pgtrickle.pgt_consumer_groups g
      JOIN pgtrickle.pgt_consumer_offsets o USING (group_id)$$,
    schedule => '5 seconds',
    refresh_mode => 'FULL'   -- small, frequently-updated table; FULL is simpler than DIFFERENTIAL
);

-- Per-group aggregate lag.
-- Does NOT use now() in computed columns; DIFFERENTIAL is safe here.
SELECT pgtrickle.create_stream_table(
    'pgt_consumer_group_lag',
    $$SELECT g.group_name,
             MAX(o.last_offset)                   AS max_offset,
             MIN(o.last_offset)                   AS min_offset,
             COUNT(o.consumer_id)                 AS consumer_count,
             (SELECT MAX(id) FROM pgtrickle.outbox_<st>) - MIN(o.last_offset)
                                                  AS max_lag_rows
      FROM pgtrickle.pgt_consumer_groups g
      JOIN pgtrickle.pgt_consumer_offsets o USING (group_id)
      GROUP BY g.group_name$$,
    schedule => '10 seconds',
    refresh_mode => 'DIFFERENTIAL'
);

-- Active leases.
-- Uses FULL mode: the WHERE clause filters on visibility_until > now(),
-- which changes on every refresh cycle (expired leases disappear).
-- DIFFERENTIAL would emit spurious deletes for every expired lease row.
SELECT pgtrickle.create_stream_table(
    'pgt_consumer_active_leases',
    $$SELECT * FROM pgtrickle.pgt_consumer_leases
      WHERE visibility_until > now()$$,
    schedule => '5 seconds',
    refresh_mode => 'FULL'   -- WHERE now() changes every cycle; FULL avoids false-change churn
);
```

> **Why FULL for pgt_consumer_status and pgt_consumer_active_leases?**
> `pgt_consumer_active_leases` filters on `visibility_until > now()`, which
> changes every refresh cycle — expired leases disappear, making DIFFERENTIAL
> emit spurious deletes. FULL is the only correct mode here.
> `pgt_consumer_status` does *not* use `now()` in its query, but it reads from
> `pgt_consumer_offsets` which is updated on every heartbeat and offset commit
> — at typical relay poll rates (every 1–5 s), most rows change between
> refreshes. FULL mode is simpler and avoids the overhead of delta computation
> for a table that is both small (one row per consumer) and frequently changing.
> Use `consumer_lag()` (a live SQL function) for programmatic health checks
> that need the current `heartbeat_age_sec`.

### B.8 Concurrency & Isolation

- All catalog updates use `READ COMMITTED`.
- `poll_outbox()` uses `FOR UPDATE SKIP LOCKED` on the outbox table to
  prevent two concurrent polls from leasing overlapping ranges.
  > **Design note:** `FOR UPDATE OF o` locks the outbox rows themselves, which
  > are append-only and never updated by the application. The lock serves purely
  > to prevent two concurrent `poll_outbox()` calls from selecting the same
  > `id` range before either inserts a lease. An alternative is to rely solely
  > on the lease INSERT for mutual exclusion (using `ON CONFLICT DO NOTHING` on
  > the lease PK). The `FOR UPDATE SKIP LOCKED` approach was chosen because it
  > provides atomicity within the CTE without a retry loop and is idiomatic for
  > PostgreSQL queue patterns. The lock is held only for the duration of the
  > `poll_outbox()` function call (milliseconds), so contention with the
  > retention drain is negligible — the drain targets old rows well behind the
  > consumers' offsets.
- `commit_offset()` is idempotent: monotonically advances `last_offset`,
  rejects regression with a warning.
- Per-consumer rows in `pgt_consumer_offsets` are isolated; no cross-consumer
  contention.

#### Delivery Guarantee

The consumer group mechanism provides **at-least-once delivery per consumer
instance** within a group. End-to-end exactly-once requires all three layers:

| Layer | Mechanism |
|-------|-----------|
| pg_trickle outbox | One row per DIFFERENTIAL refresh; committed atomically with MERGE. Never lost once committed. |
| Relay | Publishes with broker idempotency key (`Nats-Msg-Id`, Kafka record key, etc.). Re-poll after crash re-publishes; broker deduplicates. |
| Inbox | `ON CONFLICT (event_id) DO NOTHING` deduplicates at the consumer. |

If a relay crashes after `poll_outbox()` but before `commit_offset()`, its
lease expires after `consumer_dead_threshold_hours`. Another relay then
re-polls the same batch. Any rows the first relay had already published are
dedup'd by the broker; any unpublished rows are published fresh. **The inbox
always provides the final deduplication layer regardless.** This three-layer
design means no single layer needs to be perfectly reliable — correctness is
achieved by composition.

### B.9 Auto-Cleanup of Dead Consumers

A new scheduler step (gated by GUC
`pg_trickle.consumer_cleanup_enabled`, default `true`):

1. Find consumers with `last_heartbeat_at < now() - interval '<N> hours'`
   where `<N>` = `pg_trickle.consumer_dead_threshold_hours` (default `24`).
2. Release all their leases.
3. Optionally remove from `pgt_consumer_offsets` if also
   `last_commit_at < now() - interval '<D> days'`
   where `<D>` = `pg_trickle.consumer_stale_offset_threshold_days` (default `7`).
4. Emit `pg_trickle_alert` event `consumer_reaped`.

> **Tuning for high-criticality systems:** Set
> `pg_trickle.consumer_dead_threshold_hours = 1` to reduce the lease-hold
> window for crashed relays. Relays should call `consumer_heartbeat()` every
> 10 s (at most every 30 s) so the 1-hour dead threshold gives 120× the
> normal heartbeat interval as buffer.

---

## Part C — Reference Relay Implementation

A canonical Python relay shipped under `examples/relay/outbox_relay.py`:

```python
import asyncio, asyncpg, json, os, signal
import nats

GROUP_NAME    = os.environ['RELAY_GROUP']        # e.g. "order-publisher"
CONSUMER_ID   = os.environ['RELAY_CONSUMER_ID']  # e.g. "relay-1"
OUTBOX_NAME   = os.environ['RELAY_OUTBOX']       # e.g. "outbox_pending_order_events"
STREAM_TABLE  = os.environ['RELAY_STREAM_TABLE'] # e.g. "pending_order_events" (the pg_trickle ST name)
NATS_URL      = os.environ['NATS_URL']
PG_DSN        = os.environ['PG_DSN']

# SAFETY: OUTBOX_NAME and STREAM_TABLE are catalog-verified table names read
# from environment variables set by the operator, not from user input. Validate
# their format here to prevent injection if this script is ever adapted to
# accept CLI arguments.
import re
for name, val in [("RELAY_OUTBOX", OUTBOX_NAME), ("RELAY_STREAM_TABLE", STREAM_TABLE)]:
    if not re.match(r'^[a-z_][a-z0-9_]*$', val):
        raise ValueError(f"{name} must be a valid SQL identifier, got: {val!r}")

async def main():
    db = await asyncpg.connect(PG_DSN)
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()

    # Auto-register (idempotent — create_consumer_group() is a no-op if the group exists)
    await db.execute(
        "SELECT pgtrickle.create_consumer_group($1, $2, 'latest')",
        GROUP_NAME, OUTBOX_NAME,
    )

    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)

    while not stop.is_set():
        rows = await db.fetch(
            "SELECT * FROM pgtrickle.poll_outbox($1, $2, 100, 30)",
            GROUP_NAME, CONSUMER_ID,
        )
        if not rows:
            await db.execute(
                "SELECT pgtrickle.consumer_heartbeat($1, $2)",
                GROUP_NAME, CONSUMER_ID,
            )
            await asyncio.wait([stop.wait()], timeout=1.0)
            continue

        last_id = None
        for row in rows:
            payload = row['payload']
            if payload.get('claim_check'):
                # Large delta: fetch rows via server-side cursor from companion table
                # OUTBOX_NAME is read from RELAY_OUTBOX env var and validated
                # against the catalog at startup. Table names cannot be
                # parameterised in PostgreSQL, so string interpolation is used.
                delta_rows = await db.fetch(
                    f"SELECT op, payload FROM pgtrickle.outbox_delta_rows_{OUTBOX_NAME} "
                    "WHERE outbox_id = $1 ORDER BY row_num",
                    row['id'],
                )
                inserted = [r['payload'] for r in delta_rows if r['op'] == 'I']
                deleted  = [r['payload'] for r in delta_rows if r['op'] == 'D']
                # Signal completion so retention drain can safely clean up delta rows
                # NOTE: outbox_rows_consumed() takes the stream table name, not
                # the outbox table name — it resolves the outbox via pgt_outbox_config.
                await db.execute(
                    "SELECT pgtrickle.outbox_rows_consumed($1, $2)",
                    STREAM_TABLE, row['id'],
                )
            else:
                inserted = payload.get('inserted', [])
                deleted  = payload.get('deleted', [])

            is_full_refresh = payload.get('full_refresh', False)
            if is_full_refresh:
                # Apply snapshot semantics: upsert all rows, do not send as new events
                await handle_full_refresh_snapshot(inserted, js)
            else:
                for ev in inserted:
                    await js.publish(
                        f"orders.{ev['event_type'].lower()}",
                        json.dumps(ev).encode(),
                        headers={"Nats-Msg-Id": ev['event_id']},
                    )
            last_id = row['id']

        if last_id is not None:
            await db.execute(
                "SELECT pgtrickle.commit_offset($1, $2, $3)",
                GROUP_NAME, CONSUMER_ID, last_id,
            )

    await nc.close()
    await db.close()

if __name__ == '__main__':
    asyncio.run(main())
```

A Rust equivalent (`examples/relay/outbox_relay.rs`) ships alongside.

---

## Part D — Testing Strategy

### D.1 Unit Tests

- `enable_outbox()` / `disable_outbox()` happy paths and validation errors.
- Outbox table naming with truncation and collision handling.
- Payload serialisation for diverse PostgreSQL types (numeric, jsonb,
  timestamptz, arrays, NULL).
- Truncation marker emitted when payload exceeds limit.
- Drop cascade: `drop_stream_table` removes outbox table and metadata.

### D.2 Integration Tests (Testcontainers)

- End-to-end: create ST → enable outbox → INSERT into source → assert
  outbox row appears with correct payload.
- Retention drain removes rows older than threshold but preserves rows
  needed by active consumers.
- Refresh failure rolls back outbox row.
- Outbox INSERT failure (simulated via tablespace exhaustion) rolls back
  refresh.
- Concurrent refreshes on multiple STs each write their own outbox rows
  atomically.

### D.3 E2E Tests

- Full Docker stack with NATS sidecar; reference relay publishes events;
  consumer verifies exactly-once delivery via `Nats-Msg-Id`.
- Relay crash mid-batch: visibility timeout expires, second relay picks up
  the same batch, broker dedup absorbs the duplicate.
- Replay: `seek_offset(0)` causes relay to re-publish all events; verify
  consumer receives expected count after broker dedup.

### D.4 Property-Based Tests

- `proptest` over arbitrary refresh sequences ensures:
  - Sum of `inserted_count` − sum of `deleted_count` across all outbox rows
    equals current ST row count.
  - Reconstructing ST state by replaying outbox payloads matches the
    materialised view.

### D.5 Benchmark

Extend `benches/refresh_bench.rs`:
- `refresh_no_outbox` (baseline)
- `refresh_outbox_enabled_small_payload` (10 rows)
- `refresh_outbox_enabled_large_payload` (10,000 rows)
- `refresh_outbox_claim_check` (11,000 rows — one above inline threshold)
- `refresh_outbox_empty_delta_skipped` — verify no INSERT/NOTIFY when skip=true and delta=0
- `poll_outbox_latency_10k_rows` — `poll_outbox()` latency with 10K pending outbox rows
- `commit_offset_concurrent_10` — `commit_offset()` latency with 10 concurrent relays
- `consumer_lag_100k_rows` — `consumer_lag()` cost at 100K+ outbox rows

Acceptance criteria:
- < 10 % overhead vs. baseline at small payloads.
- < 25 % at large payloads.
- `poll_outbox()` < 5 ms at 10K outbox rows.
- `commit_offset()` < 10 ms at 10 concurrent relays.
- `consumer_lag()` < 50 ms at 100K outbox rows.

Tracked in CI via `scripts/criterion_regression_check.py`.

### D.6 End-to-End Latency Benchmark

Measures the full path: **source DML → CDC trigger → refresh MERGE → outbox
INSERT + NOTIFY → relay poll/LISTEN wake → broker publish**. This is the
latency number users cite in blog posts and competitive comparisons.

```
Metric: time from `INSERT INTO source_table` (committed) to broker message
        visible in consumer
Scenario: 1 row INSERT → DIFFERENTIAL refresh at 1 s schedule
Targets:
  p50  < 1.5 s  (dominated by refresh schedule)
  p95  < 2.5 s
  p99  < 5.0 s
  With LISTEN/NOTIFY wake (v0.25.0 relay): p50 < 100 ms
Infrastructure: postgres + NATS (Testcontainers), relay running in-process
```

Add as `benches/e2e_outbox_latency.rs` (uses `tokio` + Testcontainers;
gated behind `#[cfg(feature = "e2e-bench")]`). Tracked manually (not in
Criterion regression check — environment-sensitive).

### D.7 Chaos Tests

- Kill the bgworker mid-drain; verify next cycle resumes correctly.
- Force `outbox_inline_threshold_rows` to `1` and verify the claim-check path is used;
  relay cursor-fetch returns all rows; `outbox_rows_consumed()` called correctly;
  retention drain blocks until signal received.
- Partition the outbox table via `pg_partman` and verify drain still works.

### D.8 Concurrent Relay Stress Test

A critical correctness gate for Part B:

- Spawn 10 concurrent relay goroutines/tasks against a single consumer group.
- Insert 100 000 outbox rows via rapid source table mutations.
- Verify: (a) every row published exactly once across all relays (set
  equality); (b) no row published more than once within the broker (idempotency
  key check); (c) `pg_trickle_alert consumer_reaped` fires correctly when a
  relay is killed mid-batch and not replaced.

Acceptance: < 0.1% duplicate rate at the broker before dedup; 0% after
broker dedup. Total throughput > 10 000 rows/sec across 10 relays.

---

## Part E — Documentation Plan

| File | Sections to Add |
|------|-----------------|
| [docs/SQL_REFERENCE.md](../../docs/SQL_REFERENCE.md) | `pgtrickle.enable_outbox()`, `disable_outbox()`, `outbox_status()`, payload schema (with version), `pgt_outbox_latest_<st>` view usage, consumer group API |
| [docs/CONFIGURATION.md](../../docs/CONFIGURATION.md) | All seven outbox+consumer GUCs with defaults and effect descriptions |
| [docs/PATTERNS.md](../../docs/PATTERNS.md) | New section "Transactional Outbox" linking to PLAN_TRANSACTIONAL_OUTBOX.md; "Claim-Check Large Delta Relay" pattern (server-side cursor, `outbox_rows_consumed()`, memory-bounded loop); "Latest-State Consumers" (dedup view); "Monitoring Outbox Health" (custom ST example); "Multi-Relay Consumer Groups" (competing consumers) |
| [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md) | Diagram of refresh path with optional outbox write step |
| [docs/TROUBLESHOOTING.md](../../docs/TROUBLESHOOTING.md) | Common outbox issues: bloat, claim-check delta rows accumulation, failed drains, lease starvation, dead consumer cleanup |
| [CHANGELOG.md](../../CHANGELOG.md) | v0.24.0 entry for OUTBOX-1 through OUTBOX-8 and OUTBOX-B1 through OUTBOX-B9 |
| [dbt-pgtrickle/](../../dbt-pgtrickle/) | `outbox_enabled` property in dbt model config; `pgtrickle_outbox_config` macro; dbt docs update |
| [examples/](../../examples/) | `outbox_relay.py` (inline + claim-check + full-refresh), `outbox_relay.rs`, `outbox_relay_nats.py` |

---

## Part F — Implementation Roadmap

### Part A — v0.24.0 (Essential Patterns)

| Item | Description | Effort |
|------|-------------|--------|
| OUTBOX-1 | Catalog table, `enable_outbox` / `disable_outbox` / `outbox_rows_consumed` SQL functions, validation, error variants | 0.5d |
| OUTBOX-2 | Outbox table DDL incl. `is_claim_check` column + `outbox_delta_rows_<st>` companion table + dedup view | 1d |
| OUTBOX-3 | Refresh-path integration: inline path + claim-check routing at `outbox_inline_threshold_rows`; delta rows batched at 1 000/SPI call; FULL-refresh fallback handling | 1.5d |
| OUTBOX-4 | Versioned payload format (inline + claim-check); `outbox_inline_threshold_rows` GUC | 0.5d |
| OUTBOX-5 | Retention drain: cascades via FK to `outbox_delta_rows_<st>`; respects consumer offsets and `outbox_rows_consumed()` completion | 0.5d |
| OUTBOX-6 | Lifecycle & cascade; `outbox_status()` with `claim_check_pending_count`; `outbox_rows_consumed()` idempotency | 0.5d |
| OUTBOX-7 | Unit + integration + E2E tests, benchmark (`refresh_outbox_inline` vs `refresh_outbox_claim_check`), docs updates | 1.5d |
| OUTBOX-8 | Documentation & reference relay demonstrating all four payload modes | 1d |

**Total: ~7 days** (matches roadmap estimate).

### Part B — v0.24.0 (Production Patterns)

| Item | Description | Effort |
|------|-------------|--------|
| CG-1 | Catalog tables for groups, offsets, leases | 0.5d |
| CG-2 | `create_consumer_group`, `drop_consumer_group`, `poll_outbox`, `commit_offset` | 2d |
| CG-3 | `consumer_heartbeat`, `extend_lease`, `seek_offset`, monitoring stream tables (idempotent creation) | 1d |
| CG-4 | Visibility timeout enforcement, auto-cleanup of dead consumers | 1d |
| CG-5 | Retention safety (refuse to drain rows below slowest consumer; claim-check completion guard) | 0.5d |
| CG-6 | Reference relay (Python + Rust), examples, docs | 1d |
| CG-7 | E2E tests (multi-relay, crash recovery, replay), proptest, benchmarks | 1.5d |

**Total: ~7.5–8.5 days.**

---

## Part G — Open Questions

1. **IMMEDIATE-mode outbox.** ✅ **Resolved (v0.24.0):** `enable_outbox()`
   is disallowed for `IMMEDIATE`-mode stream tables and returns
   `OutboxRequiresNotImmediateMode` with an explanatory message. IMMEDIATE
   mode hooks into the source write transaction; adding an outbox INSERT
   there would impose that cost on every application transaction. Revisit
   post-1.0 with an explicit opt-in parameter if demand is high.

2. **Per-row vs per-refresh granularity.** Current design writes one outbox
   row per refresh. Alternative: one outbox row per inserted/deleted source
   row. **Proposal:** stick with per-refresh for v0.24.0 (matches DIFFERENTIAL
   semantics, lower write amplification). Add per-row mode if user demand
   appears.

3. **Outbox compression.** Should large payloads be compressed (e.g.
   `pg_lz`)? **Proposal:** rely on PostgreSQL's TOAST compression for now;
   revisit if profiling shows JSONB inflation is a real cost.

4. **Cross-database outbox.** Multi-database support is post-1.0 (S3). Outbox
   table lives in the same database as the ST. **Proposal:** no change.

5. **Logical replication of the outbox table itself.** Some users may want
   to publish `pgtrickle.outbox_*` via PostgreSQL logical replication
   instead of an external relay. **Proposal:** document but don't build —
   the table is ordinary SQL and `CREATE PUBLICATION` works out of the box.

6. **Schema evolution.** What happens if the ST's column set changes via
   `alter_stream_table` while outbox is enabled? **Proposal:** require
   `disable_outbox()` first; error otherwise. Document in upgrade guide.

7. **Consumer offset extension naming.** Should it ship as part of
   `pg_trickle` core or a separate extension `pg_trickle_consumer`?
   **Proposal:** core — too tightly coupled to outbox internals to split.

8. **LISTEN/NOTIFY on outbox write.** ✅ **Resolved (v0.24.0):** `pg_notify('pgtrickle_outbox_new', outbox_table_name)` is emitted inside the same transaction as each outbox INSERT (see §A.4). This is already reflected in the OUTBOX-3 implementation item and in the Known Limitations section of the ROADMAP. The `pgtrickle-relay` CLI will subscribe to this channel in v0.25.0 for sub-100 ms wake-up latency. Relay authors can begin using it immediately.

9. **Monitoring STs idempotent creation.** `pgt_consumer_status`,
   `pgt_consumer_group_lag`, and `pgt_consumer_active_leases` are created on
   the first `create_consumer_group()` call. Subsequent calls must not fail.
   **Proposal:** use `IF NOT EXISTS` semantics in `create_stream_table()` when
   called from consumer group setup. A reference count or explicit teardown
   path is needed: the STs should only be dropped when the *last* consumer
   group for a given outbox is dropped. Implementation: store a reference
   count in `pgt_consumer_groups`; decrement on `drop_consumer_group()`;
   drop monitoring STs when count reaches zero.

---

## Appendix — End-to-End Worked Example

### Setup

```sql
-- Source business table
CREATE TABLE orders (
    id          SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    total       NUMERIC(10,2) NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

-- Stream table for unpublished orders
SELECT pgtrickle.create_stream_table(
    'pending_order_events',
    $$SELECT id, customer_id, total, status, created_at
      FROM orders
      WHERE published_at IS NULL$$,
    schedule => '1 second',
    refresh_mode => 'DIFFERENTIAL'
);

-- Enable outbox capture
SELECT pgtrickle.enable_outbox('pending_order_events');

-- Optional: enable consumer groups (Part B)
SELECT pgtrickle.create_consumer_group(
    'order-publisher',
    'outbox_pending_order_events',
    'earliest'
);
```

### Application Write (No Dual-Write!)

```sql
-- Application only writes to the business table:
INSERT INTO orders (customer_id, total) VALUES (42, 99.95);
-- pg_trickle handles the outbox automatically on the next refresh.
```

### Outbox Table After 3 Refreshes

```sql
SELECT id, created_at, inserted_count, deleted_count, payload
FROM pgtrickle.outbox_pending_order_events;
```

| id | created_at | inserted_count | deleted_count | payload |
|----|------------|---------------:|--------------:|---------|
| 1 | 10:00:01 | 1 | 0 | `{"v":1,"inserted":[{"id":1,"customer_id":42,"total":99.95,"status":"pending","created_at":"2026-04-18T10:00:00Z"}],"deleted":[]}` |
| 2 | 10:00:02 | 0 | 0 | `{"v":1,"inserted":[],"deleted":[]}` |
| 3 | 10:00:03 | 0 | 1 | `{"v":1,"inserted":[],"deleted":[{"id":1,"customer_id":42,"total":99.95,"status":"pending","created_at":"2026-04-18T10:00:00Z"}]}` |

Row 3 appears because the relay (running separately) marked order 1 as
`published_at = now()`, which removed it from the stream table.

### Relay Polls

```sql
SELECT * FROM pgtrickle.poll_outbox(
    'order-publisher', 'relay-1', 100, 30
);
-- Returns rows 1, 2, 3 (in order)

-- Relay publishes to NATS, then:
SELECT pgtrickle.commit_offset('order-publisher', 'relay-1', 3);
```

### Inspection

```sql
-- Lag visibility
SELECT * FROM pgtrickle.consumer_lag('order-publisher');
--  group_name      | consumer_id | last_offset | lag_rows | healthy
-- -----------------+-------------+-------------+----------+---------
--  order-publisher | relay-1     |           3 |        0 | t

-- Outbox status
SELECT * FROM pgtrickle.outbox_status('pending_order_events');
--  pgt_name              | outbox_table                       | row_count | total_bytes
-- -----------------------+------------------------------------+-----------+-------------
--  pending_order_events  | outbox_pending_order_events        |         3 |        2048
```

---

## Cross-References

- [PLAN_TRANSACTIONAL_OUTBOX.md](PLAN_TRANSACTIONAL_OUTBOX.md) — research
  background and pattern overview.
- [PLAN_TRANSACTIONAL_INBOX.md](PLAN_TRANSACTIONAL_INBOX.md) — companion
  pattern for inbound events.
- [PLAN_OVERALL_ASSESSMENT.md §9.12](https://github.com/trickle-labs/pg-trickle/blob/5ed05167/plans/PLAN_OVERALL_ASSESSMENT.md#912-transactional-outbox-helper-s-effort--1-week) —
  original proposal.
- [ROADMAP.md v0.28.0 §Transactional Inbox & Outbox](../../roadmap/v0.28.0.md-full.md) —
  scheduled OUTBOX-1 through OUTBOX-8 and OUTBOX-B1 through OUTBOX-B9 items.

---

## Part H — Design Review Checklist

Five critical questions resolved before implementation begins:

| # | Question | Decision | Rationale |
|---|----------|----------|-----------|
| 1 | Should `enable_outbox()` work for IMMEDIATE-mode STs? | **No — hard error** (`OutboxRequiresNotImmediateMode`) | IMMEDIATE refreshes inside the source transaction; an extra INSERT on every write is unacceptable overhead. Exactly-once downstream semantics require a separate commit point. |
| 2 | Outbox deduplication — whose responsibility? | **Relay + inbox** (not outbox) | Outbox is an append-only audit log. Relay uses broker idempotency keys; inbox uses `ON CONFLICT DO NOTHING`. pg_trickle provides the `pgt_outbox_latest_<st>` view (ORDER BY id DESC LIMIT 1) as a zero-cost helper for latest-delta inspection. |
| 3 | Consumer lease hold time after crash — configurable? | **Yes — `pg_trickle.consumer_dead_threshold_hours`** (default 24) | High-criticality systems can set this to 1 hour. Relay heartbeat interval (recommended 10 s) gives 360× buffer even at 1-hour threshold. |
| 4 | Large-delta relay memory safety | **Claim-check path** (`outbox_delta_rows_<st>`) | When `delta_row_count > outbox_inline_threshold_rows`, rows land in `pgtrickle.outbox_delta_rows_<st>` (not in `payload`). The relay fetches via server-side cursor in bounded batches and calls `outbox_rows_consumed()`. There is **no truncation path** — all rows are always available. The old `payload.truncated` concept was removed when the claim-check design was finalised. |
| 5 | Naming collision resolution algorithm | **Exact 56-byte truncation + 7-char hex suffix** | Deterministic, reproducible, and stored in `pgt_outbox_config.outbox_table_name` so no re-derivation is needed at runtime. |
