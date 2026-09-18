# PLAN_CITUS.md — Citus Compatibility for pg_trickle

> **Date:** 2026-04-24
> **Status:** PROPOSED — user-validated (#619, 2026-04-24)
> **Targets:** Citus 13.x on PostgreSQL 18.x
> **Supersedes:** previous draft of this file (pre-WAL-decoder, pre-relay)

## 1. Executive Summary

pg_trickle today targets a single PostgreSQL instance. Citus changes three
load-bearing assumptions:

1. **OIDs are local.** A distributed table has a different OID on every
   worker; the coordinator OID may not even point at any data.
2. **WAL is local.** Each worker has its own WAL stream;
   `pg_current_wal_lsn()` on the coordinator says nothing about what
   happened on workers.
3. **DML lands on workers, not the coordinator.** Coordinator triggers
   never fire for DML against a distributed table — Citus rewrites the
   statement and ships shard-targeted SQL straight to the workers.

**Crucially, pg_trickle has changed since the previous Citus plan was
written.** Two features turn what was a 6‑month rewrite into a
much narrower integration:

- **WAL-based CDC** ([src/wal_decoder.rs](src/wal_decoder.rs)) — pg_trickle
  can already capture changes from a logical replication slot using
  `pg_logical_slot_get_changes()` polling, with a documented
  `TRIGGER → TRANSITIONING → WAL` lifecycle. Slots and publications can
  live on **other PostgreSQL nodes**; the polling SPI just needs a
  connection. This lines up exactly with the user-suggested architecture
  in [discussion #619](https://github.com/trickle-labs/pg-trickle/discussions/619):
  publications + slots on workers, consumed from the coordinator.
- **Outbox + `pgtrickle-relay`** (v0.28.0–0.29.0) give us a battle-tested
  way to **fan deltas out across nodes** (NATS / Kafka / HTTP / pg-inbox)
  with idempotent delivery and consumer-group leases. We can reuse this
  for the inverse problem too: relay deltas **between** PostgreSQL
  nodes inside the same Citus cluster when triggers can't be propagated.

Result: **the Citus path is a per-source CDC backend choice plus a
storage-table placement decision**, not a wholesale rearchitecture.

**Scope target:** correctness on a stable Citus topology
(no automatic shard rebalancing, no failover during refresh — both
explicit constraints from the discussion thread). Rebalance support is a
follow-up release.

**Confirmed use case (discussion #619 follow-up, 2026-04-24):** all sources
are hash-distributed (no reference tables), PKs are deterministic and
never change, lower-layer STs in a SQL-WITH-replacement chain expected to
reach billions of rows. This is one topology among many; design decisions
below are scoped to what is broadly correct, not to this use case alone.
See §11 for per-topology implications.

---

## 2. What Changed Since the Previous Plan

| Old assumption | Reality (v0.29.0) |
|---|---|
| CDC is trigger-only | Two backends: triggers and a WAL decoder, with hot transition |
| `src/api.rs` | Split into [src/api/mod.rs](src/api/mod.rs) + 9 submodules (`cluster`, `publication`, `inbox`, `outbox`, `planner`, `snapshot`, `self_monitoring`, `helpers`, `metrics_ext`, `diagnostics`) |
| `src/refresh.rs` | Split into [src/refresh/mod.rs](src/refresh/mod.rs) + `codegen.rs`, `merge.rs`, `orchestrator.rs`, `phd1.rs`, `tests.rs` |
| Schemas `pg_trickle` / `pg_trickle_changes` | `pgtrickle` / `pgtrickle_changes` |
| Row-level triggers only | Statement-level triggers default (transition tables, 3 triggers per source) |
| MERGE only | MERGE plus alternative apply paths (`refresh/merge.rs`, append-only fast path) |
| No downstream API | `stream_table_to_publication()` exposes ST storage tables for logical replication |
| No relay | `pgtrickle-relay` Rust binary bridges outbox/inbox with NATS, Kafka, HTTP, Redis, SQS, RabbitMQ, pg-inbox |
| LSN-only frontier | Frontier still LSN-based, but now per-source and tracked in `pgtrickle.pgt_change_tracking`; ST chains use `changes_pgt_<pgt_id>` |
| Single-node scheduler only | Multi-tenant scheduler, tiered scheduling, parallel refresh, per-DB metrics ([src/api/cluster.rs](src/api/cluster.rs)) |
| Partitioned sources unsupported | Fully supported with `publish_via_partition_root = true` (CDC-2) |

The naming and frontier work the old plan called for is **still
required**, but it now lands on top of a much more flexible CDC layer.

---

## 3. Citus-Specific Incompatibilities (current code)

### 3.1 OID-keyed object naming (HIGH — pervasive)

`pgtrickle_changes.changes_<oid>` and trigger / function names embed the
**local** OID of the source table:

- [src/cdc.rs](src/cdc.rs#L94) — `create_change_trigger`, names like
  `pg_trickle_cdc_ins_{oid}`, `pg_trickle_cdc_fn_{oid}`
- [src/cdc.rs](src/cdc.rs#L541) — `create_change_buffer_table`,
  `changes_{oid}` and `idx_changes_{oid}_lsn_pk_cid`
- [src/cdc.rs](src/cdc.rs#L860) — compaction queries by OID
- [src/wal_decoder.rs](src/wal_decoder.rs#L48-L54) — `slot_name_for_source`
  and `publication_name_for_source` use `oid.to_u32()`
- [src/dvm/diff.rs](src/dvm/diff.rs) — `__PGS_PREV_LSN_{oid}__`
  placeholder tokens
- Catalog columns in [src/lib.rs](src/lib.rs) — `source_relid OID` joins

A distributed table has a different OID on every worker. Even with
`publish_via_partition_root = true`, the publication name on each worker
must match a coordinator-side identifier. **OID is the wrong key in a
multi-node world.**

### 3.2 Trigger-based CDC on distributed tables (HIGH)

Citus does not propagate `CREATE TRIGGER` to workers. DML on a hash-distributed
table never triggers a coordinator-side `pg_trickle_cdc_*` function. **Reference
tables** are the exception — they live on every node including the coordinator,
so coordinator triggers do fire.

Affected:
- [src/cdc.rs](src/cdc.rs#L94-L280)
- [src/api/mod.rs](src/api/mod.rs) `setup_cdc_for_source`-equivalent paths
- [src/hooks.rs](src/hooks.rs) DDL event triggers — only fire on the
  coordinator

### 3.3 LSN as a global frontier (MEDIUM — narrower than before)

`pg_current_wal_lsn()` is per-node. The good news is that the WAL decoder
already operates on **per-source slot LSNs**, not the global cluster
LSN, so the change buffer is happy to carry whatever LSN-shaped value
the source produced. The bad news is that comparison logic
(`lsn_gt`, frontier merging, placeholder substitution in
[src/refresh/codegen.rs](src/refresh/codegen.rs#L1170)) implicitly
assumes one LSN namespace.

Affected:
- [src/version.rs](src/version.rs) — `SourceVersion`, `Frontier.sources`
- [src/refresh/codegen.rs](src/refresh/codegen.rs#L937-L1453) — LSN
  literals embedded in SQL
- [src/scheduler.rs](src/scheduler.rs#L1027-L1054) — global watermark gating
- [src/api/publication.rs](src/api/publication.rs#L597) — replication-lag
  metrics

### 3.4 MERGE / row-id apply on distributed STs (MEDIUM)

[src/refresh/codegen.rs](src/refresh/codegen.rs#L2088) generates
`MERGE INTO {st} USING (delta) ON st.__pgt_row_id = d.__pgt_row_id`. Citus
support for MERGE is improving but still constrained: cross-shard MERGE
is rejected, the USING side must co-locate, and `__pgt_row_id` is a
synthesized BIGINT that has no natural distribution key.

### 3.5 Coordination is node-local (MEDIUM)

- `pg_try_advisory_lock` in [src/scheduler.rs](src/scheduler.rs)
- Shared memory state in [src/shmem.rs](src/shmem.rs)
- LISTEN/NOTIFY (`pgtrickle_wake`) — only delivered to backends connected
  to the same node

### 3.6 Catalog estimates (LOW)

`pg_class.reltuples` for a distributed table on the coordinator reflects
only the (usually empty) local placeholder. The DAG planner uses this in
[src/dag.rs](src/dag.rs) and the cost model in v0.22.0
predictive-cost code paths.

### 3.7 Things that already work

- **Reference tables** — coordinator triggers fire; current code works
  unchanged for reference-only sources.
- **Local tables in a Citus cluster** — same.
- **Stream table storage on the coordinator** — STs are just regular
  tables; if all sources are local/reference and you keep the ST local
  too, nothing changes.
- **Outbox / inbox / `stream_table_to_publication`** — already speak
  logical replication, so they cross node boundaries today as long as
  the consumer can reach the coordinator.

---

## 4. Design Decisions

| Concern | Decision | Rationale |
|---|---|---|
| **Default CDC backend for distributed sources** | WAL decoder against per-worker slots | Triggers don't propagate; this is also what discussion #619 asked for |
| **Naming** | Stable hash of `(database_oid, schema_name, table_name)` | Identical on every node, survives OID churn and pg_dump/restore |
| **Frontier** | Per-source LSN keyed by stable name; **no global frontier** | LSNs are per-WAL; we already track per-source frontiers, just need cross-node identity |
| **ST storage placement** | User-selectable, auto-suggested: `local` (default), `reference`, `distributed` | Mirrors Citus's own table types; small reference STs avoid join blowups |
| **MERGE replacement (distributed STs)** | `DELETE … WHERE __pgt_row_id IN (...)` + `INSERT … ON CONFLICT (__pgt_row_id) DO UPDATE` | Citus-supported; same semantics |
| **Worker→coordinator transport** | Logical replication (publications + slots) — **no triggers on workers** | Matches WAL-decoder model already in tree; zero new write-path overhead |
| **Reference-table sources** | Keep trigger CDC path | Already works; no reason to change |
| **REPLICA IDENTITY** | `FULL` required when PKs can change; `DEFAULT` (PK-only) sufficient when PKs are immutable — and strongly preferred at large scale | PK-only decoding works only when UPDATE never touches a PK column; if it does, `pgoutput` with DEFAULT cannot reconstruct the old identity, causing silent loss in the ST. `FULL` amplifies WAL proportionally to row width (negligible on narrow tables, severe on wide ones at scale). Pre-flight must verify the PK-immutability assumption or require FULL. |
| **Locking** | Catalog table for cross-node locks; advisory locks remain the local fast path | Advisory locks are node-local |
| **Wake signalling** | `LISTEN/NOTIFY` on coordinator only; workers don't run a scheduler | Single scheduler per database (today's model) is fine for Citus |
| **Detection** | Auto-detect Citus at extension load + per-source at create time | No new GUC required for the common case |
| **Rebalance** | Out of scope for v1 (matches user's constraint in #619) | Slot location follows shard placement; rebalance would invalidate slots |
| **Chained distributed STs** | Attempt to distribute all STs in the same DAG subgraph on the same column; error out when this is structurally impossible | Co-location is required for shard-local delta apply. Auto-co-location cannot be resolved when the chain includes aggregations (GROUP BY changes cardinality), JOINs on a non-source-distribution key, or projections that drop the distribution column — in those cases the user must choose placement explicitly. Note also that `__pgt_row_id`-distributed STs do **not** co-locate with their source tables; JOINs between an ST and its source remain cross-shard. |

---

## 5. Architecture

```
        ┌──────────────────────────  COORDINATOR  ──────────────────────────┐
        │                                                                   │
        │   pg_trickle scheduler ─┐                                         │
        │                         │                                         │
        │   ┌─────────────────────▼──────────────────────┐                  │
        │   │ pgtrickle_changes.changes_<stable_hash>    │  (one per source)│
        │   │   ▲                                         │                 │
        │   └───┼─────────────────────────────────────────┘                 │
        │       │            ▲                       ▲                      │
        │       │ trigger    │ pg_logical_slot_get_changes (over dblink     │
        │       │ (local +   │  / postgres_fdw / libpq) per worker          │
        │       │  reference)│                                              │
        │       │            │                       │                      │
        │   ┌───┴────────┐   │                       │                      │
        │   │ Local /    │   │                       │                      │
        │   │ reference  │   │                       │                      │
        │   │ tables     │   │                       │                      │
        │   └────────────┘   │                       │                      │
        │                                                                   │
        │   STs:  local | citus reference | citus distributed (__pgt_row_id)│
        └───┼───────────────┼───────────────────────┼──────────────────────┘
            │               │                       │
        ┌───▼────┐      ┌───▼────┐              ┌───▼────┐
        │WORKER 1│      │WORKER 2│      …       │WORKER N│
        │        │      │        │              │        │
        │ shard  │      │ shard  │              │ shard  │
        │ tables │      │ tables │              │ tables │
        │  +     │      │  +     │              │  +     │
        │  pub   │      │  pub   │              │  pub   │
        │  +     │      │  +     │              │  +     │
        │  slot  │      │  slot  │              │  slot  │
        └────────┘      └────────┘              └────────┘
```

Two CDC paths coexist:

- **Reference / local sources** — current trigger pipeline, unchanged.
- **Distributed sources** — coordinator creates a publication and a
  logical slot **on every worker** that hosts a shard of the source. The
  scheduler polls each slot via libpq (reusing the WAL-decoder code in
  [src/wal_decoder.rs](src/wal_decoder.rs)) and writes decoded rows into
  the coordinator's `changes_<stable_hash>` buffer.

The decoder already speaks `pgoutput`, already handles INSERT/UPDATE/
DELETE/TRUNCATE, already understands REPLICA IDENTITY FULL vs DEFAULT,
and already advances the slot when the refresh commits. It does **not**
yet know how to talk to a remote node — that is the principal new code
in this plan.

---

## 6. Implementation Phases

### Phase 1 — Citus Detection + Stable Naming (foundation)

**Goal:** Land the additive groundwork that benefits even non-Citus
deployments, with no behaviour change on single-node.

**P1.1 — `src/citus.rs` module.** Detection helpers:

- `is_citus_loaded()` — `SELECT 1 FROM pg_extension WHERE extname='citus'`
- `placement(oid)` → `Local | Reference | Distributed { dist_column }`
  via `pg_dist_partition`
- `worker_nodes()` → `Vec<NodeAddr>` from `pg_dist_node`
- `shard_placements(table_oid)` — which workers actually host shards
- Wraps `run_command_on_workers(...)` and `run_command_on_all_nodes(...)`

**P1.2 — `SourceIdentifier`.** Carries `(oid, stable_name)`.
`stable_name = stable_hash(database_oid || '/' || schema_name ||
'.' || table_name)` (16-char hex). Serialised in catalog and used in
all object names.

**P1.3 — Catalog migration.** Add nullable columns; old rows continue to
function with OID-derived synthetic stable names:

```sql
ALTER TABLE pgtrickle.pgt_change_tracking
  ADD COLUMN source_stable_name TEXT,
  ADD COLUMN source_placement TEXT NOT NULL DEFAULT 'local';
ALTER TABLE pgtrickle.pgt_dependencies
  ADD COLUMN source_stable_name TEXT,
  ADD COLUMN source_placement TEXT NOT NULL DEFAULT 'local';
ALTER TABLE pgtrickle.pgt_stream_tables
  ADD COLUMN st_placement TEXT NOT NULL DEFAULT 'local';
CREATE SCHEMA IF NOT EXISTS pgtrickle_changes;  -- already exists
```

**P1.4 — Rename buffer objects.** Replace `changes_{oid}`,
`pg_trickle_cdc_*_{oid}`, `idx_changes_{oid}_*` and the LSN
placeholder tokens in [src/dvm/diff.rs](src/dvm/diff.rs) with
`{stable_name}`. Provide an upgrade SQL script that renames in place
(stable hash of existing source row).

**Files:** `src/citus.rs` (new), `src/lib.rs`, `src/catalog.rs`,
`src/cdc.rs`, `src/wal_decoder.rs`, `src/dvm/diff.rs`,
`src/refresh/codegen.rs`, `src/dvm/operators/*`, `sql/pg_trickle--<n>--<n+1>.sql`.

**Ships in:** a regular release. Non-breaking.

---

### Phase 2 — Per-Source Frontier Cleanup

**Goal:** Make the frontier per-source-LSN-namespace correct, not
"whatever the coordinator's WAL says".

**P2.1.** `Frontier.sources` is already a `HashMap`; rekey from OID
string to `stable_name`. Serialised JSONB schema gets a side migration.

**P2.2.** Ban global LSN comparisons across sources. Audit
[src/refresh/codegen.rs](src/refresh/codegen.rs) and
[src/scheduler.rs](src/scheduler.rs#L1054) for places that read
`pg_current_wal_lsn()` and compare across sources; replace with
per-source watermark logic that already exists in
[src/wal_decoder.rs](src/wal_decoder.rs).

**P2.3.** When a source is `Distributed`, the LSN stored in
`pgt_change_tracking` is **the per-worker slot LSN that produced the
last consumed change** (a tuple keyed by `(worker_id, lsn)`), not a
coordinator LSN. Add a `frontier_per_node JSONB` column for these
sources.

**Files:** `src/version.rs`, `src/refresh/codegen.rs`,
`src/scheduler.rs`, `src/wal_decoder.rs`, `src/lib.rs`.

---

### Phase 3 — Distributed CDC via Per-Worker Slots

**This is the heart of the plan.** Reuses the WAL decoder; doesn't
introduce a new mechanism.

**P3.1 — Remote slot consumption.** Today
[src/wal_decoder.rs](src/wal_decoder.rs#L495) calls
`pg_logical_slot_get_changes()` via SPI on the local node. Add a
sibling code path that calls it on a **remote** node. Two options,
benched in Phase 6:

- **A: `dblink` / `postgres_fdw`** wrapping the same SQL. Simplest,
  fewest dependencies; relies on plain SPI from the coordinator.
- **B: native libpq + `START_REPLICATION` streaming**. Lower latency
  but a much bigger code surface; we already depend on the
  `pgoutput` parser, so the marginal cost is moderate.

**Default to A for the first release.** It composes with existing
connection pooling and Citus user mapping.

**P3.2 — Setup for distributed sources.** When `setup_cdc_for_source`
sees `placement == Distributed`:

1. `create_publication(stable_name, partitioned=true)` is wrapped in
   `run_command_on_all_nodes(...)` so every worker has the
   publication on its local shard.
2. `pg_create_logical_replication_slot('pgtrickle_<stable_name>',
   'pgoutput')` is also issued on every worker via
   `run_command_on_all_nodes`.
3. The coordinator records `(worker_id, slot_name)` per-source in a
   new `pgtrickle.pgt_remote_slots` catalog.

**P3.3 — REPLICA IDENTITY enforcement on workers.** The pre-flight check
already at [src/wal_decoder.rs](src/wal_decoder.rs#L1509) needs to run
**on each worker** (run_command_on_workers) for distributed sources.

- `NOTHING` — reject with a clear error; change capture is impossible.
- `DEFAULT` (PK-only) — accepted only when the user explicitly opts in
  with `immutable_pk => true` on `create_stream_table()`. This is an
  assertion by the user that no PK column is ever updated. pg_trickle cannot
  verify this statically (application-level UPDATE patterns are invisible
  to `pg_attribute`). **If a PK column is updated, `pgoutput` with DEFAULT
  emits only the new row; the coordinator cannot match it to the old
  `__pgt_row_id`, producing silent data loss in the ST.** This is not a
  recoverable error — it corrupts the materialisation silently. Emit a
  prominent warning at setup time when `immutable_pk => true` is used.
- `FULL` — accepted always; **this is the default**. WAL amplification is
  proportional to row width: negligible on narrow tables, potentially severe
  (10× or more) on very wide tables with high UPDATE rates. Log a sizing
  advisory at setup time.

Default guidance: require `FULL` unless the user passes `immutable_pk =>
  true`, in which case `DEFAULT` is used and a warning is emitted.

**P3.4 — Slot polling.** Scheduler tick iterates over
`pgt_remote_slots` for the source and pulls from each worker, writing
into the coordinator's `changes_<stable_name>` buffer with
`(worker_id, slot_lsn)` recorded in two new columns
(`origin_node SMALLINT`, `origin_lsn PG_LSN`). Compaction uses the
same `(origin_node, origin_lsn)` tuple as a watermark.

**P3.5 — TRUNCATE + DDL.** `pg_logical_slot_get_changes()` already
emits a TRUNCATE message; the existing decoder writes an action='T'
marker. DDL (column add/drop, partition attach) on a distributed
table is propagated by Citus; the coordinator's existing event
triggers fire there. Add a worker-side `IF EXISTS` re-create of the
publication when the column set changes.

**P3.6 — Reference / local sources stay on triggers.** No change.

**Files:** `src/wal_decoder.rs`, `src/cdc.rs`, `src/api/mod.rs`,
`src/api/publication.rs`, `src/scheduler.rs`, `src/lib.rs`.

---

### Phase 4 — Distributed ST Storage & Apply Path

**P4.1 — ST placement at create time.** Extend
`create_stream_table()` in [src/api/mod.rs](src/api/mod.rs) with an
optional `placement => 'local' | 'reference' | 'distributed'`
parameter. Auto-select when omitted using the placement of declared
sources:

| Sources | Default placement |
|---|---|
| All `Local` | `local` |
| Includes `Reference`, no `Distributed` | `local` |
| All sources `Distributed` | `distributed` |
| Mixed `Distributed` + `Reference`/`Local`, projected ST < 1M rows | `reference` |
| Mixed `Distributed` + `Reference`/`Local`, projected ST ≥ 1M rows | `distributed` |

Persisted in `pgtrickle.pgt_stream_tables.st_placement`. Threshold
GUC: `pg_trickle.citus_reference_st_max_rows` (default `1_000_000`); only
applied in the mixed-topology case. When all sources are `Distributed` the
threshold is bypassed and `distributed` is always the default.

> **Rationale:** An all-distributed source topology with billion-row lower-layer
> STs (confirmed in #619) makes `reference` placement untenable regardless of
> projected ST size. The 1M threshold is only meaningful when the user has
> a mix of source placements and genuinely small aggregation outputs.

**P4.2 — Apply for `distributed` STs.** The DELETE + INSERT…ON CONFLICT
pair replaces MERGE. Distribution column is `__pgt_row_id`. Codegen
in [src/refresh/codegen.rs](src/refresh/codegen.rs) gets a second
template selected by `st_placement`.

```sql
WITH delta AS (...)
DELETE FROM {st} st
 USING delta d
 WHERE d.__pgt_action = 'D'
   AND st.__pgt_row_id = d.__pgt_row_id;

WITH delta AS (...)
INSERT INTO {st} (__pgt_row_id, ...)
SELECT __pgt_row_id, ... FROM delta WHERE __pgt_action = 'I'
ON CONFLICT (__pgt_row_id) DO UPDATE SET ...;
```

**P4.3 — Apply for `reference` STs.** MERGE works as today, but the
USING side often pulls data from workers; add a hint to **materialise
the delta** to a `TEMP` table when the planner reports a non-pushable
plan (Phase 6 gate).

**P4.4 — `__pgt_row_id` distribution.** It's a `BIGINT` already; mark
it as the distribution column at `create_distributed_table` time.
Validation: refuse `distributed` placement if the user supplied an
ORDER BY / GROUP BY shape that would produce skew on row id (rare —
row id is a monotonically increasing surrogate).

> **Limitation:** Distributing an ST by `__pgt_row_id` (a synthetic
> surrogate) does **not** align shards with the source table's distribution
> column. Any query that JOINs the ST back to its source produces cross-shard
> joins, which Citus executes correctly but at higher cost (re-partition join
> or broadcast). Document this and recommend denormalising the source
> distribution key into the ST projection if frequent ST-source joins are
> expected.

**P4.5 — `reltuples` fix.** `src/dag.rs` and the predictive cost
model (`src/api/planner.rs`) sum `pg_dist_shard` row counts when the
table is distributed.

**Files:** `src/api/mod.rs`, `src/refresh/codegen.rs`,
`src/refresh/merge.rs`, `src/dag.rs`, `src/api/planner.rs`,
`src/lib.rs`.

---

### Phase 5 — Coordination & Operability

**P5.1 — Catalog locks.** Add
`pgtrickle.pgt_st_locks(pgt_id BIGINT PRIMARY KEY, locked_by INT,
locked_at TIMESTAMPTZ, lease_until TIMESTAMPTZ)`. Replace
advisory-lock acquisition in [src/scheduler.rs](src/scheduler.rs)
with `INSERT … ON CONFLICT … DO NOTHING`. Lease expiry handled by
`now() > lease_until`. Advisory locks remain a fast in-process check.

**P5.2 — Wake.** Stay with `LISTEN/NOTIFY` on the coordinator. In
the rare case the user runs the extension on a worker (e.g. for
debugging), the wake is a no-op — that worker has no scheduler.

**P5.3 — Cluster observability.** Extend
[src/api/cluster.rs](src/api/cluster.rs) `cluster_worker_summary()`
to expose Citus context: which sources are distributed, slot
positions per worker, replication lag per worker. New view
`pgtrickle.citus_status`.

**P5.4 — Failure modes.**
- Worker unreachable: pause refresh of any ST whose source has a
  stuck slot, surface via `monitor`/Prometheus.
- Worker WAL recycled past slot: the existing fallback to FULL
  refresh (already implemented for local WAL gaps in
  [src/wal_decoder.rs](src/wal_decoder.rs)) applies.
- Slot not found on worker after a node addition: re-create on
  next tick, log warning.

**Files:** `src/scheduler.rs`, `src/shmem.rs`, `src/api/cluster.rs`,
`src/monitor.rs`, `src/lib.rs`.

---

### Phase 6 — Validation, Benchmarks, Migration, Docs

**P6.1 — Test harness.** New
`tests/e2e_citus_tests.rs` using a `citusdata/citus:13.0`-based
testcontainer set (1 coord + 2 workers via docker-compose).
`tests/Dockerfile.e2e-citus` builds the extension into the Citus
image. Reuse `cargo pgrx package` artefacts where possible — most of
this should be light-E2E once the image is available.

**P6.2 — Test matrix.**

| Source(s) | ST placement | Exercises |
|---|---|---|
| Local table | local | regression — trigger CDC |
| Reference table | local | trigger CDC, coordinator fires |
| Distributed table | reference | per-worker slots, MERGE, replication of ST |
| Distributed table | distributed | per-worker slots, DELETE + UPSERT |
| Mixed (ref + dist) | local | mixed CDC backends, frontier per source |
| Distributed ⇒ outbox | distributed | downstream pub + relay against distributed ST |
| ALTER TABLE on dist source | distributed | DDL propagation, slot rebuild |
| Worker restart | distributed | slot survives, refresh resumes |
| TRUNCATE on dist source | any | full-refresh fallback |
| Concurrent DML + refresh | distributed | apply correctness under load |

**P6.3 — Benchmarks.** Add to `benches/`:
- `bench_remote_slot_poll` — throughput of `dblink`-wrapped
  `pg_logical_slot_get_changes()` vs local SPI.
- `bench_distributed_apply` — DELETE+UPSERT vs MERGE on a 100M-row
  distributed ST.

Set a non-Citus regression budget of **0%** — when Citus isn't
present, code paths must compile out or short-circuit on the
detection check.

**P6.4 — Migration script.**
`sql/pg_trickle--<prev>--<next>.sql` performs P1.3 column adds, then
backfills `source_stable_name`/`source_placement` from
`pg_class`+`pg_namespace`+`pg_dist_partition`. Renames existing
`changes_<oid>` tables and trigger functions to the stable-hash form
in a single transaction. Provides a downgrade path that renames back
when `pg_dist_partition` is empty.

**P6.5 — Docs.** New page `docs/integrations/citus.md` covering
prerequisites (`wal_level=logical` on every worker, `max_replication_slots`
sized appropriately, `REPLICA IDENTITY DEFAULT` (minimum; `FULL` is also
supported but not recommended at large scale due to WAL amplification) on
distributed sources, matching extension version on coordinator and workers,
stable shard placement). Update [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) with
the multi-node diagram from §5. Update [INSTALL.md](INSTALL.md) with
"installing on a Citus cluster".

**Files:** `tests/e2e_citus_tests.rs`, `tests/Dockerfile.e2e-citus`,
`benches/`, `sql/`, `docs/integrations/citus.md`,
`docs/ARCHITECTURE.md`, `INSTALL.md`.

---

## 7. Sequencing & Release Mapping

| Phase | Ships in | User-visible? |
|---|---|---|
| P1 — detection + stable naming | next minor (e.g. v0.30.0) | No (internal); enables Citus |
| P2 — per-source frontier | same release as P1 | Bug fixes for users hitting WAL recycling |
| P3 — per-worker slot CDC | follow-up minor | New: distributed sources supported |
| P4 — distributed ST apply | same release as P3 | New: `placement` option |
| P5 — coordination | same release as P3 | Operability |
| P6 — validation / docs | rolls in continuously, gated on P3 + P4 | Yes |

P1 and P2 are safe single-node ships and produce immediate quality
benefits (better naming through pg_dump/restore, cleaner per-source
LSN tracking). P3–P5 are the Citus-specific deliverable.

---

## 8. Risk Register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| `dblink`-wrapped slot polling has unacceptable latency | Medium | Med | Bench in P6.3; fallback path is direct libpq streaming (P3.1 option B) |
| Citus rejects MERGE on `__pgt_row_id`-distributed STs in some shapes | Med | High | DELETE + INSERT ON CONFLICT (P4.2) is the primary path anyway; MERGE only used for `reference` STs |
| Slot fills WAL on a worker if the coordinator scheduler stops | High | High | Document monitoring requirement; expose lag in `pgtrickle.citus_status`; auto-drop slot after configurable lease expiry |
| `__pgt_row_id` skew on distributed STs | Low | Med | It's monotonic; warn if user attempts to repurpose the column |
| Shard rebalance invalidates slots | Low (user opted out) | High | Out of scope for v1; emit a hard error in slot-poll path if `pg_dist_node` topology hash changes |
| Schema/role differences on workers | Med | Med | `run_command_on_all_nodes` for `CREATE SCHEMA pgtrickle_changes` and grants; document required roles |
| Mixed extension versions across nodes | Med | High | Pre-flight: `SELECT extversion FROM pg_extension WHERE extname='pg_trickle'` on every worker; refuse to start if mismatched |
| User runs without `wal_level=logical` on workers | High | Low | Detected at slot-create; clear error message |
| Citus columnar STs requested | Low | Low | Refuse `distributed` placement when `USING columnar`; columnar can't be UPDATE/DELETE'd |

---

## 9. Open Questions

1. **`dblink` vs streaming libpq.** Bench result determines P3.1 default.
   Streaming gives push-based wake-ups (no polling latency) but adds a
   long-running connection per worker.
2. **Reference vs distributed ST default (mixed topology only).** The 1M-row
   threshold in P4.1 now only applies when sources are mixed
   (`Distributed` + `Reference`/`Local`). Instrument and tune in P6.3 for
   that case.
3. **Source table count and slot budget (resolved, 2026-04-25).** erikmata
   confirmed "several hundreds, growing over time" of sources on a cluster of
   1 coordinator + 9 worker nodes (144 shards, 16 per worker; shard count may
   be adjusted after benchmarking — see ambiguity note below).

   **Connection model correction:** the coordinator opens **`N_workers`
   long-lived polling connections** (one per worker, batching all slot polls
   for that worker in sequence), **not** `N_sources × N_workers`. Connection
   pressure is a function of worker count only; adding more sources does not
   add connections.

   **Shard count does not affect slot count.** One logical replication slot is
   created per source per worker regardless of how many shards the worker holds
   for that table. A publication covers all local shards under one slot.
   Slot formula: `max_replication_slots ≥ peak_source_count` per worker
   (suggested: `peak_source_count × 1.5 + other_replication_consumers`).

   **Still open — rebalancing ambiguity.** erikmata noted the shard count "will
   be adapted depending on benchmarking". This is either (A) changing
   `citus.shard_count` for newly created tables — fully fine, slots unaffected
   — or (B) rebalancing existing shard placements across workers, which is the
   deferred case: v0.33.0 detects a topology hash change and raises a hard error
   rather than silently producing wrong results. Confirm with erikmata which case
   is intended before v0.33.0 ships (follow up in discussion #619).
4. **Dynamic query construction compatibility (resolved, 2026-04-25).**
   Confirmed compatible. erikmata's system translates a conceptual model into
   `create_stream_table()` calls at **design time** — the SQL is fixed at
   creation; the "dynamic" part is choosing which ST chain to deploy, not
   varying the SQL per execution. No action required.
5. **Multi-database Citus.** Citus 13 supports multiple databases per
   cluster; each pg_trickle scheduler is per-database. No change
   expected, but verify in P6.2.
6. **Interaction with `pgtrickle-relay`.** A distributed ST exposed via
   `stream_table_to_publication()` already streams over logical
   replication from the coordinator's storage table — works today
   regardless of placement. No new code; document the pattern.
7. **CitusData vs Microsoft fork divergence.** Track the upstream
   (microsoft/citus) repo; pin tested versions in CI.

---

## 11. Known Use Cases

### 11.1 — All-distributed ELT chain (discussion #619)

**Source topology:** All sources hash-distributed; no reference tables;
PKs deterministic and never modified.

**Confirmed cluster details (2026-04-25):** 1 coordinator + 9 worker nodes,
144 shards / 16 per worker. Source table count: several hundreds, growing
incrementally over time via design-time `create_stream_table()` calls generated
by a conceptual-model translator (not runtime SQL generation — confirmed
compatible in discussion #619, 2026-04-25). Shard count may be adjusted after
benchmarking — see §9 OQ #3 for the open rebalancing ambiguity.

**Pattern:** Chain of SQL-WITH expressions replaced with a chain of STs;
lower-layer STs expected to reach billions of rows.

**Configuration target:** `REPLICA IDENTITY FULL` (safe default) or
`REPLICA IDENTITY DEFAULT` + `immutable_pk => true` (opt-in, saves WAL
overhead when PKs genuinely never change) + `placement => 'distributed'`
automatically selected by the all-distributed rule in P4.1.

**Known limitations that apply to this topology:**

- **Cross-shard ST↔source JOINs** (see P4.4): `__pgt_row_id`-distributed
  STs don't co-locate with source tables. Denormalise the source
  distribution key into the ST if frequent back-joins are needed.
- **Co-location breaks at aggregation boundaries**: if a mid-chain ST
  aggregates (GROUP BY) or joins on a different key, automatic co-location
  with the next ST in the chain is impossible; that ST boundary requires
  explicit placement choice.
- **PK immutability is a pre-condition, not verified automatically**:
  using `DEFAULT` with mutable PKs causes silent data loss (see P3.3).
  The confirmed use case meets this pre-condition; other users may not.
- **Frontend/background workload ratio**: the apply phase
  (`DELETE + INSERT ON CONFLICT`) runs as background work and fans out
  across worker nodes, competing with frontend queries for CPU and I/O on
  each worker. Tune `pg_trickle.max_concurrent_refreshes`,
  `pg_trickle.max_dynamic_refresh_workers`, and
  `pg_trickle.max_parallel_workers` to reserve headroom. A dedicated
  "tuning for mixed frontend/background workloads" section is planned for
  `docs/integrations/citus.md`.

---

## 12. Cross-References

- [src/wal_decoder.rs](src/wal_decoder.rs) — already-implemented
  WAL-based CDC; foundation of Phase 3.
- [src/api/publication.rs](src/api/publication.rs) — downstream
  publication API for ST storage; reused as-is.
- [pgtrickle-relay/](pgtrickle-relay/) — outbox/inbox bridge
  available for cross-cluster fan-out.
- The completed partitioning compatibility work; some primitives overlap.
- [plans/infra/PLAN_MULTI_DATABASE.md](plans/infra/PLAN_MULTI_DATABASE.md)
  — multi-database scheduler.
- [discussion #619](https://github.com/trickle-labs/pg-trickle/discussions/619)
  — original user request that motivated the rewrite; follow-up comments
  confirmed all-distributed topology, billion-row ST chains, and PK-only
  REPLICA IDENTITY preference (2026-04-24).
