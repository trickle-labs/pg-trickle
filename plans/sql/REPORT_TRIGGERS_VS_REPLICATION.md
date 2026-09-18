# Triggers vs Logical Replication for CDC in pg_trickle

**Status:** Evaluation Report (updated with implementation status)  
**Date:** 2026-02-24  
**Context:** ADR-001/ADR-002 in [PLAN_ADRS.md](../adrs/PLAN_ADRS.md) · the implemented user-trigger support

---

## Executive Summary

pg_trickle uses **row-level AFTER triggers** to capture changes on source tables.
This report evaluates the trigger-based approach against **logical replication**
(WAL-based CDC) across five dimensions: correctness, performance, operations,
and two end-user features — user-defined triggers on stream tables and logical
replication subscriptions from stream tables.

**Conclusion:** Triggers remain the correct choice for the current scope given
operational simplicity and zero-config deployment. The **hybrid approach** —
trigger bootstrap for creation with automatic WAL transition for steady-state —
is now **implemented** (`pg_trickle.cdc_mode` GUC, `src/wal_decoder.rs`). User-
defined triggers on stream tables are also **implemented** (`pg_trickle.user_triggers`
GUC, `DISABLE TRIGGER USER` during refresh). These were previously recommendations
(§6.2, §6.6); both are now shipped.

However, the atomicity constraint — the original reason for choosing triggers —
is primarily a **creation-time inconvenience**, not a steady-state limitation.
Once a stream table exists, logical replication has three significant runtime
advantages:

- **No write-side overhead** — With triggers, every INSERT/UPDATE/DELETE on a
  tracked source table does extra work *before the application's transaction
  can commit*: it runs a PL/pgSQL function, writes a row into a buffer table,
  and updates an index. This slows down the application. With logical
  replication, PostgreSQL already writes every change to its internal
  transaction log (WAL) regardless — the CDC layer simply reads that log
  after the fact, so the application's writes are not slowed down at all.

- **TRUNCATE capture** — When someone runs `TRUNCATE` on a source table, row-level
  triggers do not fire (TRUNCATE replaces the entire file rather than deleting
  rows one-by-one). This leaves stream tables silently stale until a manual
  refresh. Logical replication captures TRUNCATE natively from the WAL,
  so pg_trickle would know immediately that all rows were removed.

- **Change ordering from the transaction log** — With triggers, each trigger
  independently calls `pg_current_wal_lsn()` to timestamp its change. With
  logical replication, the ordering comes directly from the WAL — the
  authoritative, global record of all database changes — which means change
  ordering is guaranteed to match commit order, even across concurrent
  transactions.

The two end-user features (user triggers and logical replication FROM stream
tables) are both achievable without changing the CDC mechanism. A hybrid
approach (triggers for creation, logical replication for steady-state) deserves
serious consideration. See §3 for the full analysis.

---

## 1. Background

### Current Architecture

CDC triggers on each tracked source table write typed per-column rows into
per-table buffer tables (`pgtrickle_changes.changes_<oid>`). Each buffer row
captures:

| Column | Purpose |
|---|---|
| `change_id` | BIGSERIAL ordering within a source |
| `lsn` | `pg_current_wal_lsn()` at trigger time |
| `action` | `'I'` / `'U'` / `'D'` |
| `pk_hash` | Content hash of PK columns (optional) |
| `new_<col>` | Per-column NEW values (INSERT/UPDATE) |
| `old_<col>` | Per-column OLD values (UPDATE/DELETE) |

A covering B-tree index `(lsn, pk_hash, change_id) INCLUDE (action)` supports
the differential refresh's LSN-range scan.

### The Atomicity Constraint

`create_stream_table()` performs DDL (CREATE TABLE) and DML (catalog inserts)
before setting up CDC. `pg_create_logical_replication_slot()` **cannot execute
inside a transaction that has already performed writes**. This makes
single-transaction atomic creation impossible with logical replication — the
decisive factor in the original ADR.

---

## 2. Comparison Matrix

### 2.1 Correctness & Transactional Safety

| Aspect | Triggers | Logical Replication |
|---|---|---|
| Atomic creation | ✅ Same transaction as DDL+catalog | ❌ Slot creation requires separate transaction |
| Change visibility | ✅ Immediate (same transaction) | ⚠️ Asynchronous (after COMMIT + WAL decode) |
| TRUNCATE capture | ❌ Row-level triggers not fired | ✅ WAL emits TRUNCATE since PG 11 |
| Transaction ordering | ✅ Change buffer rows ordered by LSN | ✅ WAL stream preserves commit order |
| Crash recovery | ✅ Buffer tables are WAL-logged; no orphan state | ⚠️ Slot survives crash but may need re-sync |
| Schema change handling | ✅ DDL event hooks rebuild trigger in-place | ⚠️ Requires slot re-creation or output plugin awareness |

**Key insight:** The TRUNCATE gap is the most significant correctness
limitation of the trigger approach. A statement-level `AFTER TRUNCATE` trigger
that marks downstream STs for automatic FULL refresh would close this gap
without changing the CDC architecture (see §6 Recommendation 3).

### 2.2 Performance

| Metric | Triggers | Logical Replication |
|---|---|---|
| Per-row write overhead | ~2–4 μs (narrow INSERT) to ~5–15 μs (wide UPDATE) | ~0 (WAL writes happen regardless) |
| Expected throughput reduction | 1.5–5× on tracked source tables | None on source tables |
| Write amplification | 2× (source WAL + buffer table WAL + index) | 1× (only source WAL) |
| Change buffer storage | Heap table + index per source | WAL segments (shared, recycled) |
| Sequence contention | BIGSERIAL per buffer (lightweight) | N/A |
| Throughput ceiling | ~5,000 writes/sec (estimated) | WAL throughput (much higher) |
| Decoding CPU cost | N/A | Non-trivial; output plugin runs in WAL sender |
| Zero-change refresh | ~3 ms (EXISTS check on empty buffer) | ~3 ms (no pending WAL changes) |

**Key insight:** Trigger overhead is **synchronous** — every committing
transaction pays the cost. For applications with moderate write rates
(<5,000 writes/sec) this is acceptable. For high-throughput OLTP workloads,
logical replication's zero write-side overhead is a significant advantage.

### 2.3 Operational Complexity

| Aspect | Triggers | Logical Replication |
|---|---|---|
| PostgreSQL configuration | None required | `wal_level = logical` + restart |
| Managed PG compatibility | ✅ Works everywhere | ⚠️ Some providers restrict `wal_level` |
| WAL retention risk | None (buffer tables are independent) | Slots prevent WAL cleanup; disk exhaustion risk |
| Slot management | N/A | Create, monitor, drop; orphan detection |
| `max_replication_slots` | N/A | Must be sized for number of tracked sources |
| `REPLICA IDENTITY` config | N/A | Required on all tracked source tables |
| Monitoring | Buffer table row counts | Slot lag, WAL retention, decode rate |
| Extension dependencies | None | Output plugin (`pgoutput`, `wal2json`, or custom) |
| Upgrade path | `CREATE OR REPLACE FUNCTION` | Slot protocol version compatibility |

**Key insight:** Triggers are operationally simpler by a wide margin. Logical
replication introduces a class of failure modes (stuck slots, WAL bloat,
replica identity misconfiguration) that require dedicated monitoring and
operational runbooks.

### 2.4 Feature: User Triggers on Stream Tables

This addresses end-user triggers on the **output** stream tables, not CDC
triggers on source tables.

| Aspect | Current (Trigger CDC) | With Logical Replication CDC |
|---|---|---|
| Feasibility | ✅ Achievable via `session_replication_role` | ✅ Same mechanism applies |
| Refresh suppression | `SET LOCAL session_replication_role = 'replica'` | Same |
| Post-refresh notification | `NOTIFY pg_trickle_refresh` with metadata | Same |
| MERGE firing pattern | DELETE+INSERT (not UPDATE); must be suppressed | Same — refresh mechanism is independent of CDC |

**Key insight:** User trigger support on stream tables is **orthogonal to the
CDC mechanism** and is now **implemented**. The solution uses `ALTER TABLE ...
DISABLE TRIGGER USER` / `ENABLE TRIGGER USER` around FULL refresh (avoiding
the `session_replication_role` conflict with logical replication publishing).
In DIFFERENTIAL mode, explicit per-row DML (INSERT/UPDATE/DELETE) is used
instead of MERGE so that user-defined AFTER triggers fire correctly. The
implementation is controlled by the `pg_trickle.user_triggers` GUC (`auto`/
`on`/`off`). See the implemented user-trigger support
for the full design.

> **Note:** Sections 2.1–2.5 compare creation-time and operational aspects.
> For a focused steady-state comparison (what matters once the ST exists),
> see §3.

### 2.5 Feature: Logical Replication FROM Stream Tables

This addresses end-users **subscribing** to stream table changes via
PostgreSQL's built-in logical replication.

| Aspect | Status | Notes |
|---|---|---|
| Basic publishing | ✅ Works today | STs are regular heap tables; `CREATE PUBLICATION` works |
| `__pgt_row_id` column | ⚠️ Replicated by default | Use column list in PUBLICATION to exclude, or document as usable PK |
| Differential refresh | ✅ DELETE+INSERT via MERGE are replicated | Subscriber sees individual DELETEs and INSERTs, not UPDATEs |
| Full refresh | ✅ TRUNCATE + INSERT replicated | Subscriber needs `replica_identity` set; receives TRUNCATE + mass INSERT |
| `REPLICA IDENTITY` | Needs configuration | `__pgt_row_id` could serve as unique index for identity |

#### The `session_replication_role` Conflict

If the refresh engine sets `session_replication_role = 'replica'` to suppress
user triggers (Phase 1 of the user-trigger plan), this may also suppress
**publication of the DML to logical replication subscribers**. When a session
is in `replica` mode, PostgreSQL treats it as a replication subscriber — DML
performed in that session may not be forwarded to downstream subscribers
(depending on the publication's `publish_via_partition_root` and the
subscriber's `origin` setting).

**This is a potential conflict** between the two features. Options:

| Option | User Triggers Suppressed? | Replication Published? | Drawback |
|---|---|---|---|
| `session_replication_role = 'replica'` | ✅ Yes | ❌ May not be published | Breaks logical replication from STs |
| `ALTER TABLE ... DISABLE TRIGGER USER` | ✅ Yes | ✅ Yes | Requires `ACCESS EXCLUSIVE` lock |
| `pg_trickle.suppress_user_triggers` GUC → `DISABLE TRIGGER USER` only when needed | ✅ Configurable | ✅ Yes | Lock overhead; crash safety concern (ENABLE on recovery) |
| `tgisinternal` flag manipulation | ✅ Yes | ✅ Yes | Non-portable; catalog-level hack |

**Recommended resolution:** Use `ALTER TABLE ... DISABLE TRIGGER USER` within
a SAVEPOINT, restoring on error. The `ACCESS EXCLUSIVE` lock is brief (only
held for the catalog update, not the entire refresh). If the user has enabled
both user triggers AND logical replication on a stream table, this is the only
approach that supports both simultaneously. If neither feature is in use, skip
the overhead entirely.

---

## 3. Separating Creation-Time from Steady-State

The original ADR chose triggers because `pg_create_logical_replication_slot()`
cannot execute inside a transaction that has already performed writes. This
report initially treated that constraint as "decisive." But it deserves
scrutiny: **the atomicity constraint only affects the `create_stream_table()`
call — a one-time event.** Once a stream table exists, CDC runs for hours,
days, or months. The steady-state characteristics are what actually matter for
performance, correctness, and user experience.

### 3.1 The Atomicity Constraint Is a Solvable Engineering Problem

The constraint is real but workable. Three approaches exist, all with
well-understood trade-offs:

| Approach | How It Works | Downside |
|---|---|---|
| **Two-phase creation** | Phase 1: DDL + catalog in one transaction. Phase 2: slot creation in a separate transaction. Rollback Phase 1 artifacts on Phase 2 failure. | Brief window where catalog entry exists without CDC. Cleanup on failure adds ~50 lines of code. |
| **Background worker handoff** | Main transaction creates DDL + catalog + temporary trigger. Background worker creates slot asynchronously, then drops trigger. | Race window: changes between COMMIT and slot creation are captured by the temporary trigger, so no data is lost. Adds complexity (~100 lines). |
| **Trigger bootstrap → slot transition** | Create with triggers (current approach). After first successful refresh, migrate to logical replication in the background. | Trigger overhead during bootstrap period (minutes). Most natural hybrid approach. |

None of these are architecturally difficult. The two-phase approach is
straightforward — if slot creation fails, drop the storage table and catalog
entry. The temporary-trigger approach eliminates even the theoretical data-loss
window. These are **engineering inconveniences**, not fundamental blockers.

### 3.2 Steady-State: Triggers vs Logical Replication (Honest Comparison)

Once the stream table exists and CDC is running, here is how the two approaches
compare on their actual runtime merits.

**In plain terms:** With triggers, every time the application writes a row to a
tracked source table, the database does *extra work right then and there* —
calling a function, writing to a buffer table, updating an index — all before
the application's transaction can finish. This is like a toll booth on a highway:
every car (write) must stop and pay (trigger overhead) before continuing.

With logical replication, the database already writes every change to its
internal transaction log (the WAL) as part of normal operation. CDC simply reads
that log *after the fact*, in a separate background process. The application's
writes pass through without stopping — there is no toll booth. The cost of
reading the log is paid by the database server, but it happens asynchronously
and never slows down the application.

#### Where Logical Replication Wins (Steady-State)

| Dimension | Trigger Impact | Logical Replication Advantage |
|---|---|---|
| **Write-path latency** | Every INSERT/UPDATE/DELETE on a tracked source pays ~2–15 μs synchronous overhead (PL/pgSQL dispatch, buffer INSERT, index update). This is **inside the committing transaction's critical path.** | Zero additional write-path cost. WAL writes happen regardless; decoding is asynchronous. Source table DML performance is completely unaffected. |
| **Write amplification** | Each source row change produces: (1) source table WAL, (2) buffer table heap write, (3) buffer table WAL, (4) buffer index update, (5) index WAL. **~2–3× total write amplification.** | 1× — only the source table's normal WAL. No additional heap writes, no secondary indexes. |
| **TRUNCATE capture** | Cannot capture. Row-level triggers don't fire. Requires a separate statement-level AFTER TRUNCATE workaround (§4) that only marks for reinit — the actual row deletions are invisible to differential mode. | Native. WAL emits TRUNCATE events since PG 11. The decoder receives a clean signal that all rows were removed. |
| **Throughput ceiling** | Estimated ~5,000 writes/sec on tracked sources before trigger overhead dominates. PL/pgSQL function dispatch is the bottleneck. | Bounded by WAL throughput — typically 50,000–200,000+ writes/sec depending on hardware and `wal_buffers`. |
| **Connection-pool pressure** | Trigger executes in the application's connection. Long-running trigger INSERTs can increase connection hold time under load. | Decoding runs in a dedicated WAL sender process. Application connections are unaffected. |
| **Vacuum pressure** | Buffer tables accumulate dead tuples between cleanups. Each refresh cycle creates bloat that autovacuum must reclaim. | No buffer tables to vacuum. WAL segments are recycled by the WAL management subsystem. |
| **Transaction ID consumption** | Each trigger INSERT consumes sub-transaction resources within the outer transaction. High-volume batch operations can cause excessive subtransaction overhead. | No additional transaction work. |

#### Where Triggers Win (Steady-State)

| Dimension | Trigger Advantage | Logical Replication Impact |
|---|---|---|
| **Operational simplicity** | No external state to manage. Buffer tables are regular heap tables — queryable, monitorable, backed up normally. Drop the trigger and it's gone. | Replication slots are persistent server-side state. A stuck or crashed consumer prevents WAL recycling, potentially filling the disk. Requires monitoring, max_slot_wal_keep_size guards, and orphan-slot cleanup. |
| **Zero configuration** | Works with any `wal_level` (`minimal`, `replica`, `logical`). No restart required. No `REPLICA IDENTITY` configuration. | Requires `wal_level = logical` (server restart), `max_replication_slots` sizing, and `REPLICA IDENTITY` on every tracked source table. Many managed PostgreSQL providers default to `wal_level = replica`. |
| **Schema evolution** | DDL event hooks rebuild the trigger function via `CREATE OR REPLACE FUNCTION`. New columns are added to the buffer table with `ADD COLUMN IF NOT EXISTS`. Simple, same-transaction, no coordination. | Schema changes on tracked tables require careful handling. The output plugin must be aware of column additions/removals. Slot may need to be recreated. `ALTER TABLE` during active decoding can cause protocol errors. |
| **Debugging & visibility** | Change buffers are queryable tables: `SELECT * FROM pgtrickle_changes.changes_12345 ORDER BY change_id DESC LIMIT 10`. Immediate visibility into what was captured. | WAL is binary and opaque. Inspecting captured changes requires `pg_logical_slot_peek_changes()` which advances or peeks the slot — disruptive in production. |
| **Crash recovery** | Buffer tables are WAL-logged and survive crashes. No special recovery needed — the refresh engine picks up from the last frontier LSN. | Slots survive crashes, but the decoding position may be ahead of what pg_trickle has consumed. Requires careful bookkeeping to avoid replaying or losing changes. |
| **Multi-source coordination** | Each source has an independent buffer table. The refresh engine reads from multiple buffers with independent LSN ranges. No coordination between sources. | Multiple sources could share a single slot (decoding all tables) or use per-source slots. Shared slots require demultiplexing; per-source slots multiply the slot management burden. |
| **Isolation** | Trigger failure (e.g., buffer table full) raises an error in the application transaction — visible and immediate. | Decoding failure is asynchronous. The application commits successfully, but changes may never reach the buffer. Silent data loss is possible unless monitored. |

#### Neutral (Roughly Equivalent)

| Dimension | Notes |
|---|---|
| **Refresh-path performance** | Both approaches populate the same buffer table schema. The MERGE/DVM pipeline is identical regardless of how buffers were filled. |
| **Zero-change detection** | Triggers: `EXISTS` check on empty buffer (~3 ms). Logical replication: check slot position vs current WAL LSN (~3 ms). Equivalent. |
| **Memory footprint** | Triggers: PL/pgSQL function cache per backend. Logical replication: WAL sender process + decoding context. Both are modest. |

### 3.3 When Does Logical Replication Become the Better Choice?

The crossover point depends on workload characteristics:

| Scenario | Better Choice | Why |
|---|---|---|
| **< 1,000 writes/sec** on tracked sources | Triggers | Overhead is negligible; operational simplicity dominates |
| **1,000–5,000 writes/sec** | Either / Triggers still acceptable | Trigger overhead is measurable but unlikely to be the bottleneck |
| **> 5,000 writes/sec** | Logical Replication | Write-path overhead starts to matter; 2–3× write amplification compounds |
| **ETL patterns** (TRUNCATE + bulk INSERT) | Logical Replication | Native TRUNCATE capture; no stale-data gap |
| **Wide tables** (20+ columns) | Logical Replication | Trigger overhead scales with column count (~5–15 μs); WAL overhead does not |
| **Managed PostgreSQL** with `wal_level` restrictions | Triggers | No choice — logical replication may not be available |
| **Many tracked sources** (50+) | Logical Replication | Fewer moving parts than 50 triggers + 50 buffer tables + 50 indexes |
| **Need logical replication FROM stream tables** | Triggers (with caveats) | see §2.5 — `session_replication_role` conflict with `DISABLE TRIGGER USER` as workaround |

### 3.4 Reassessing the Decision

With the atomicity constraint properly scoped as a creation-time concern, the
decision to use triggers rests on three remaining pillars:

1. **Operational simplicity** — no `wal_level` change, no slot management, no
   `REPLICA IDENTITY` configuration. This is genuinely valuable for an
   early-stage extension that needs frictionless adoption.

2. **Debugging visibility** — queryable buffer tables are a major developer
   experience advantage. Being able to `SELECT * FROM changes_<oid>` during
   debugging is invaluable.

3. **Zero-config deployment** — works on any PostgreSQL 18 instance without
   server restarts or configuration changes. Critical for managed PostgreSQL
   environments.

However, these advantages are primarily about **developer and operator
experience**, not about the fundamental capability of the system. A mature
pg_trickle deployment that needs high write throughput, TRUNCATE support, or
minimal source-table impact would be better served by logical replication in
steady-state.

**The honest assessment:** Triggers are the right choice *today* for pragmatic
reasons (simplicity, early-stage adoption, managed PG compatibility). But the
report should not overstate the atomicity constraint as a fundamental blocker —
it is a solvable problem. If pg_trickle grows to serve high-throughput
production workloads, the migration to logical replication for steady-state CDC
should be treated as a planned evolution, not a theoretical future.

---

## 4. TRUNCATE: The Gap and How to Close It

> This limitation is one of the strongest arguments for logical replication
> in steady-state — see §3.2 for the comparison.

The TRUNCATE limitation is the most commonly cited drawback of trigger-based
CDC. PostgreSQL does not fire row-level triggers for TRUNCATE because TRUNCATE
operates at the file level (O(1)) — there are no individual rows to enumerate.

### Current Behavior

1. User runs `TRUNCATE source_table`
2. CDC trigger does **not** fire — change buffer remains empty
3. Scheduler sees zero changes → `NO_DATA` → stream table is **stale**
4. Stream table shows data from rows that no longer exist

### Proposed Fix: Statement-Level AFTER TRUNCATE Trigger

PostgreSQL supports statement-level `AFTER TRUNCATE` triggers. While they
provide no `OLD` row data, they can mark downstream stream tables for
reinitialization:

```sql
CREATE TRIGGER pg_trickle_truncate_<oid>
  AFTER TRUNCATE ON <source_table>
  FOR EACH STATEMENT
  EXECUTE FUNCTION pgtrickle.on_source_truncated('<source_oid>');
```

The trigger function would:
1. Look up all stream tables that depend on this source
2. Mark them `needs_reinit = true` in the catalog
3. Cascade transitively to downstream STs

This closes the TRUNCATE gap without changing the CDC architecture. The next
scheduler cycle would trigger a FULL refresh automatically.

**Effort estimate:** ~2–4 hours (trigger creation in `cdc.rs`, PL/pgSQL or
Rust function for `on_source_truncated`, cascade logic reuse from `hooks.rs`).

---

## 5. Migration Path: Trigger → Logical Replication (Now Implemented)

> **Status:** Phase A (Hybrid Creation) is now implemented in `src/wal_decoder.rs`.
> The `pg_trickle.cdc_mode` GUC controls the behavior (`trigger`/`auto`/`wal`).

As discussed in §3, the atomicity constraint is a creation-time problem with
known solutions. The buffer table schema and downstream IVM pipeline are
**decoupled** from the capture mechanism, so migration is isolated to the CDC
layer. This should be treated as a **planned evolution** for high-throughput
deployments, not a theoretical future:

### Phase A: Hybrid Creation

1. `create_stream_table()` continues using triggers for atomic creation
2. After first successful full refresh, a background worker creates a
   replication slot and transitions to WAL-based capture
3. Trigger is dropped; buffer table continues to be populated from WAL decode

### Phase B: Steady-State WAL Capture

1. Background worker runs a logical decoding consumer per tracked source
2. WAL changes are decoded and written to the same buffer table schema
3. Downstream pipeline (DVM, MERGE, frontier) is unchanged
4. TRUNCATE events are captured natively from WAL

### Prerequisites

- `wal_level = logical` (must be documented as optional upgrade path)
- `REPLICA IDENTITY` on tracked sources (auto-configured or user-managed)
- Custom output plugin or `pgoutput` + column mapping
- Slot health monitoring (WAL retention alerts, orphan cleanup)

**Effort estimate:** 3–5 weeks for a production-quality implementation.

---

## 6. Recommendations

### Recommendation 1: Keep Trigger-Based CDC (For Now)

Operational simplicity and zero-config deployment are strong advantages for an
early-stage extension. The performance ceiling (~5,000 writes/sec) is adequate
for current target use cases. The atomicity constraint, while solvable (see
§3.1), adds creation-time complexity that is not yet justified.

**However:** This decision should be revisited when any of these triggers are
hit: (a) users report write-path latency from CDC triggers, (b) TRUNCATE-based
ETL patterns become a common pain point, (c) pg_trickle targets environments
where `wal_level = logical` is already the norm. The steady-state advantages of
logical replication (§3.2) are substantial and should not be dismissed.

### Recommendation 2: ✅ IMPLEMENTED — User Trigger Suppression

User-defined triggers on stream tables are now fully supported. The
implementation uses `ALTER TABLE ... DISABLE TRIGGER USER` / `ENABLE TRIGGER
USER` around FULL refresh, and explicit per-row DML (INSERT/UPDATE/DELETE)
instead of MERGE during DIFFERENTIAL refresh so user AFTER triggers fire
correctly. Controlled by `pg_trickle.user_triggers` GUC (`auto`/`on`/`off`).
The `session_replication_role` approach from the original plan was rejected to
avoid conflict with logical replication publishing (see §2.5).

### Recommendation 3: Add TRUNCATE Capture Trigger

Add a statement-level `AFTER TRUNCATE` trigger on each tracked source table
that marks downstream STs for reinitialization. This closes the most
significant usability gap without changing the CDC architecture.

### Recommendation 4: Document Logical Replication FROM Stream Tables

Add documentation and examples for `CREATE PUBLICATION` on stream tables,
including:
- Column filtering to exclude `__pgt_row_id`
- `REPLICA IDENTITY` configuration using `__pgt_row_id` as unique index
- Behavior during FULL vs DIFFERENTIAL refresh
- Interaction with user trigger suppression

### Recommendation 5: Benchmark Trigger Overhead

Execute the benchmark plan in
[PLAN_TRIGGERS_OVERHEAD.md](../performance/PLAN_TRIGGERS_OVERHEAD.md) to establish
data-driven thresholds for the logical replication migration crossover point.
The results should feed directly into the §3.3 crossover analysis.

### Recommendation 6: ✅ IMPLEMENTED — Hybrid CDC Approach

The "trigger bootstrap → slot transition" pattern is now implemented in
`src/wal_decoder.rs` (1152 lines). The implementation includes:

- **Automatic transition**: After stream table creation with triggers, a
  background worker creates a logical replication slot and transitions to
  WAL-based capture.
- **GUC control**: `pg_trickle.cdc_mode` (`trigger`/`auto`/`wal`) and
  `pg_trickle.wal_transition_timeout` control the behavior.
- **Transition orchestration**: Create slot → wait for catch-up → drop trigger.
  Automatic fallback to triggers if slot creation fails.
- **Catalog extension**: `pgt_dependencies` gains `cdc_mode`, `slot_name`,
  `decoder_confirmed_lsn`, `transition_started_at` columns.
- **Health monitoring**: `pgtrickle.check_cdc_health()` function and
  `NOTIFY pg_trickle_cdc_transition` notifications.

---

## 7. Decision Log

| # | Decision | Rationale |
|---|---|---|
| D1 | Keep triggers for CDC on source tables — for now | Zero-config, operational simplicity, adequate for current scale |
| D2 | Atomicity constraint is solvable, not fundamental | Two-phase creation and hybrid bootstrap are proven patterns (§3.1) |
| D3 | Logical replication is superior in steady-state | Zero write overhead, TRUNCATE capture, higher throughput ceiling (§3.2) |
| D4 | User triggers on STs are orthogonal to CDC choice | `session_replication_role` / `DISABLE TRIGGER USER` works with either approach |
| D5 | Logical replication FROM STs works today | Regular heap tables; needs documentation, not code |
| D6 | TRUNCATE gap is closable with statement-level trigger | Low effort, high impact — but logical replication handles it natively |
| D7 | Hybrid approach is the optimal long-term target | Trigger bootstrap for creation + logical replication for steady-state |
| D8 | User trigger suppression uses `DISABLE TRIGGER USER` | Avoids `session_replication_role` conflict with logical replication publishing (§2.5) |
| D9 | Hybrid CDC implemented with auto-transition | `pg_trickle.cdc_mode = 'auto'` triggers → WAL transition after creation |
| D10 | Explicit DML for DIFFERENTIAL refresh with user triggers | INSERT/UPDATE/DELETE instead of MERGE so AFTER triggers fire correctly |
