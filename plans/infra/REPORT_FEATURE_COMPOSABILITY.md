# Feature Composability Analysis — Major Proposed Features

**Date:** 2026-03-03
**Status:** Exploration
**Type:** REPORT

---

## Executive Summary

This analysis examines seven major proposed features through the lens of
composability:

- **Fuse** — anomalous change volume protection
- **Watermark gating** — cross-source temporal alignment
- **Blue-green deployment** — hot-swap pipeline evolution
- **External process (sidecar)** — extension-free deployment
- **Diamond dependency consistency** — multi-path refresh atomicity
- **Cross-source snapshot consistency** — independent-source temporal coherence
- **Transactional IVM (IMMEDIATE mode)** — same-transaction view maintenance

For each, we ask:

1. Could this be a **standalone project** (separate crate or binary)?
2. Could its **internal components** be composed from smaller, reusable pieces?
3. What **shared abstractions** emerge across the features?

The key finding: most features share a common pattern — they are
**scheduling gates and orchestration layers** that wrap the existing refresh
pipeline. This suggests a unified **RefreshGate** trait abstraction that would
make most features composable, testable independently, and stackable.
Transactional IVM is the exception — it introduces a fundamentally different
execution model (in-transaction triggers) that requires a separate
**DeltaSource** abstraction for composability.

---

## 1. Feature-by-Feature Analysis

### 1.1 Fuse — Anomalous Change Volume Protection

**Source:** [PLAN_FUSE.md](../sql/PLAN_FUSE.md)

#### What It Does

A per-stream-table safety mechanism that halts refresh when change volume
exceeds a statistical threshold or hard ceiling. Binary state machine:
INTACT → BLOWN (manual reset required).

#### Standalone Project Potential: **High**

The fuse is fundamentally a **data quality gate** — a pattern used far beyond
IVM. The core logic is:

```
f(change_count, baseline_μ, baseline_σ, ceiling, sensitivity) → blow | pass
```

This is a **pure function** with zero PostgreSQL dependency. The plan already
identifies this (§11 Step 1: `should_blow()` as a pure function).

**Extractable as: `pg-fuse` or `data-quality-gate` crate**

| Component | PG-coupled? | Extractable? |
|-----------|-------------|-------------|
| `should_blow()` — trip decision logic | No | ✅ Immediately |
| `FuseMode` / `FuseState` enums | No | ✅ Immediately |
| Rolling baseline computation (Welford's algorithm / windowed stats) | No | ✅ Immediately |
| EWMA / fixed-window statistics | No | ✅ Immediately |
| Fuse catalog storage (`pgt_fuses` table) | Yes — SPI/SQL | Stays in extension |
| `reset_fuse()` SQL function | Yes — `#[pg_extern]` | Stays in extension |
| `fuse_status()` introspection | Yes — SPI | Stays in extension |
| NOTIFY alert emission | Yes — PG channels | Stays in extension |
| Scheduler integration (pre-check gate) | Yes — scheduler loop | Adapter pattern |

**Standalone value:** Any system with a data pipeline (Kafka consumers,
ETL jobs, dbt models) could use the statistical trip logic to detect
anomalous data volumes. The gate pattern (check → pass/block) is universal.

#### Internal Decomposition Opportunities

1. **Statistics engine** — The rolling baseline (mean, stddev, Welford's
   online algorithm, EWMA) is a general-purpose streaming statistics
   module. Could be shared with future adaptive threshold features
   (e.g., adaptive scheduling intervals based on change rate trends).

2. **Gate interface** — The fuse's scheduler integration follows a pattern:

   ```rust
   trait RefreshGate {
       fn should_proceed(&self, ctx: &RefreshContext) -> GateDecision;
   }

   enum GateDecision {
       Proceed,
       Skip { reason: String },
       Blow { reason: String },  // permanent block until reset
   }
   ```

   This same interface applies to watermark gating, diamond consistency
   checks, the existing adaptive fallback, and even blue-green convergence
   detection.

---

### 1.2 Watermark Gating — Cross-Source Temporal Alignment

**Source:** the implemented watermark-gating behavior

#### What It Does

User-injected watermarks per source table declare "external data is complete
through timestamp T." Watermark groups enforce alignment: downstream STs
skip refresh until all sources in the group report sufficiently aligned
watermarks.

#### Standalone Project Potential: **Medium-High**

The watermark concept has two layers:

1. **Watermark algebra** (pure logic) — monotonic advancement, group
   alignment predicate ($\max(W_i) - \min(W_i) \leq \tau$), effective
   watermark computation, tolerance checking.

2. **Gating orchestration** (PG-coupled) — catalog storage, scheduler
   integration, LSN mapping for hold-back mode, NOTIFY signaling.

Layer 1 is fully extractable. Layer 2 is an adapter.

**Extractable as: `watermark-gate` crate (or part of a broader `pipeline-gate` crate)**

| Component | PG-coupled? | Extractable? |
|-----------|-------------|-------------|
| Watermark monotonicity check | No | ✅ |
| Group alignment predicate | No | ✅ |
| Effective watermark computation | No | ✅ |
| Tolerance evaluation | No | ✅ |
| `WatermarkGroup` / `Watermark` types | No | ✅ |
| `pgt_watermarks` / `pgt_watermark_groups` catalog | Yes | Stays in extension |
| `advance_watermark()` SQL function | Yes | Stays in extension |
| LSN ↔ watermark mapping (hold-back) | Yes — WAL coupling | Stays in extension |
| Scheduler gating pre-check | Yes | Adapter pattern |

**Standalone value:** Any pipeline orchestrator dealing with multi-source
temporal alignment (Airflow DAGs waiting for upstream datasets, Kafka Streams
multi-topic joins, Flink watermark propagation) could use the watermark
algebra. The tolerance-based alignment predicate is directly applicable to
event-time processing systems.

#### Internal Decomposition Opportunities

1. **Watermark algebra module** — The core types and predicates are
   independent of pg_trickle. This module would contain:
   - `Watermark` (monotonic wrapper around `DateTime<Utc>` or generic `Ord`)
   - `WatermarkGroup` (set of source IDs + tolerance)
   - `alignment_check(group, watermarks) → Aligned(effective_wm) | Misaligned(lag)`

2. **Gate interface** — Watermark gating fits the same `RefreshGate` trait
   as the fuse:

   ```rust
   impl RefreshGate for WatermarkGate {
       fn should_proceed(&self, ctx: &RefreshContext) -> GateDecision {
           match self.check_alignment(ctx.source_watermarks()) {
               Aligned(wm) => GateDecision::Proceed,
               Misaligned(lag) => GateDecision::Skip {
                   reason: format!("watermark lag {} exceeds tolerance", lag),
               },
           }
       }
   }
   ```

3. **Hold-back as a separate concern** — The plan's §5.2 describes
   "hold-back" mode where intermediate STs cap their change window to the
   effective watermark. This is a fundamentally different mechanism from
   gating (it changes *what data* is consumed, not *whether* to refresh).
   It should be a separate composition layer, not conflated with the gate.

---

### 1.3 Blue-Green Deployment — Hot-Swap Pipelines

**Source:** [REPORT_BLUE_GREEN_DEPLOYMENT.md](REPORT_BLUE_GREEN_DEPLOYMENT.md)

#### What It Does

Create a "green" copy of a stream table (or pipeline) that catches up
independently. Once converged with the "blue" (active) version, atomically
swap them. Supports query changes, rollback, and zero-downtime evolution.

#### Standalone Project Potential: **Low**

Unlike fuse and watermark, blue-green deployment is deeply tied to
pg_trickle's specific catalog, CDC sharing, frontier tracking, and storage
table management. The "green" ST is a full pg_trickle stream table — it uses
all the same infrastructure (triggers, change buffers, DVM, scheduler).

What *could* be extracted is the **convergence detection** and
**orchestration state machine**, but these are thin layers on top of
pg_trickle-specific concepts (frontier LSN comparison, `pgt_id` isolation,
advisory locks).

**Not a good candidate for a standalone project.** Better served by
internal decomposition.

#### Internal Decomposition Opportunities

1. **Convergence detector** — The report identifies four convergence
   strategies (frontier LSN, data timestamp, lag threshold, content hash).
   These are composable — a user should be able to combine them:

   ```rust
   trait ConvergenceCheck {
       fn is_converged(&self, blue: &StMeta, green: &StMeta) -> ConvergenceResult;
   }

   // Composable: all checks must pass
   struct CompositeConvergence(Vec<Box<dyn ConvergenceCheck>>);
   ```

   This uses the same pattern as the `RefreshGate` trait — a composable
   predicate evaluated before an action.

2. **Pipeline lifecycle state machine** — The blue-green lifecycle
   (`create_green → catching_up → converged → promote | rollback →
   cleanup`) is a generic state machine. This pattern appears in:
   - Blue-green deployment (this feature)
   - CDC mode transitions (trigger → WAL, per the implemented hybrid CDC behavior)
   - Stream table status transitions (ACTIVE → SUSPENDED → ERROR)

   A generic `LifecycleStateMachine<State, Event>` could unify these.

3. **Gate interface** — During promotion, the scheduler must recognize that
   a green ST is "catching up" and not yet the active version. This is
   another scheduling gate: the green ST participates in refresh, but the
   blue ST is the one downstream STs reference. The promote operation is
   an atomic gate swap.

---

### 1.4 External Process (Sidecar) — Extension-Free Deployment

**Source:** [REPORT_EXTERNAL_PROCESS.md](REPORT_EXTERNAL_PROCESS.md)

#### What It Does

Run the entire pg_trickle engine as an external binary connecting to
PostgreSQL over standard connections, removing the requirement to install
a C extension. Enables managed PG services (RDS, Cloud SQL, Neon).

#### Standalone Project Potential: **This IS a standalone project**

The sidecar is not a "feature" to be extracted — it is the **primary driver
for all other extraction work**. It requires every core component to be
decoupled from pgrx:

- DVM engine → `pg-query-diff` crate (uses `pg_query.rs` instead of `pg_sys::raw_parser`)
- DAG → `pg-dag` crate (already nearly pure Rust)
- CDC → SQL generators (trigger DDL, change buffer queries)
- Scheduler → Tokio-based main loop
- Catalog → `tokio-postgres` / `sqlx` client
- Config → TOML file instead of GUCs

The sidecar report (§12.1) demonstrates that even IMMEDIATE mode (previously
assumed to require the extension) can be delivered via pre-compiled PL/pgSQL
triggers, achieving correctness parity.

#### Internal Decomposition Required by Sidecar

The sidecar's crate restructuring (Phase S0 in the report) proposes:

```
crates/
├── pgtrickle-core/       # Pure Rust: DAG, DVM, diff, scheduling logic
├── pgtrickle-parser/     # pg_query.rs-based SQL parsing
├── pgtrickle-client/     # PgClient trait + tokio-postgres impl
├── pgtrickle-extension/  # pgrx shim (#[pg_extern] → core)
└── pgtrickle-sidecar/    # Tokio binary
```

This workspace structure is the **end state** that all other decomposition
efforts converge toward. The trait abstractions proposed in
[REPORT_ENGINE_COMPOSABILITY.md](REPORT_ENGINE_COMPOSABILITY.md) (ParseFrontend,
StorageBackend, CatalogAccess) are exactly what the sidecar needs.

#### Relationship to Other Three Features

The sidecar changes HOW features are deployed but not WHAT they do:

| Feature | Extension Mode | Sidecar Mode | Logic Shared? |
|---------|---------------|--------------|---------------|
| Fuse | SPI catalog + bgworker gate | SQL catalog + Tokio gate | ✅ `should_blow()` is pure Rust |
| Watermark | SPI + GUC + bgworker gate | SQL + TOML + Tokio gate | ✅ Alignment predicate is pure Rust |
| Blue-green | SPI + advisory locks | SQL + advisory locks | ✅ Convergence checks are pure Rust |
| Diamond | SPI SAVEPOINT atomic groups | pgwire SAVEPOINT atomic groups | ✅ Detection is pure Rust; execution is standard SQL |
| Cross-source | REPEATABLE READ via SPI | REPEATABLE READ via pgwire | ✅ Group logic is pure Rust |
| Transactional IVM | Native Rust triggers + ENRs | Compiled PL/pgSQL triggers | ✅ Delta SQL templates shared; execution differs |
| All scheduling gates | BGWorker scheduler loop | Tokio scheduler loop | ✅ `RefreshGate` trait |

**Key insight:** If fuse, watermark, and blue-green are implemented with
the `RefreshGate` trait pattern, the sidecar gets them "for free" —
the trait implementations are in `pgtrickle-core`, and both scheduler
implementations (bgworker + Tokio) compose the same gates.

---

### 1.5 Diamond Dependency Consistency — Multi-Path Refresh Atomicity

**Source:** [PLAN_DIAMOND_DEPENDENCY_CONSISTENCY.md](../sql/PLAN_DIAMOND_DEPENDENCY_CONSISTENCY.md)

#### What It Does

When two intermediate STs (B, C) share a common upstream source (A) and
converge at a downstream ST (D), the current sequential scheduler can produce
inconsistent results if B refreshes but C fails. The diamond plan introduces
**epoch-based atomic refresh groups** using SAVEPOINTs: either all members of
a consistency group succeed or all roll back.

#### Standalone Project Potential: **Medium**

The feature has two distinct layers:

1. **Diamond detection algorithm** (pure graph logic) — find fan-in nodes,
   trace shared ancestors, compute consistency groups. This is already in
   `dag.rs` and is nearly pure Rust.

2. **Atomic group execution** (PG-coupled) — SAVEPOINT-based transaction
   control, epoch tracking, rollback-on-failure semantics.

Layer 1 is fully extractable as part of `pg-dag`. Layer 2 is scheduler
orchestration.

| Component | PG-coupled? | Extractable? |
|-----------|-------------|-------------|
| `detect_consistency_groups()` — graph algorithm | No | ✅ Part of `pg-dag` |
| `ConsistencyGroup` struct + epoch tracking | No | ✅ |
| Frontier alignment check (skip D if B/C diverge) | No (compares LSN values) | ✅ |
| SAVEPOINT-based atomic execution | Yes — SPI/SQL | Adapter pattern |
| `diamond_consistency` GUC / config | Yes — GUC in extension, TOML in sidecar | Per-mode config |

**Standalone value:** The diamond detection algorithm generalizes to any DAG
scheduler that needs atomic group semantics — CI/CD pipelines, build systems,
data pipeline orchestrators. The `ConsistencyGroup` concept with
frontier-alignment checks is reusable.

#### Internal Decomposition Opportunities

1. **Gate interface** — Diamond consistency is a natural `RefreshGate`:

   ```rust
   impl RefreshGate for DiamondConsistencyGate {
       fn evaluate(&self, st: &StreamTableMeta, ctx: &GateContext) -> GateDecision {
           // If this ST is a convergence point and its upstream group
           // members have divergent frontiers, skip.
           match self.check_frontier_alignment(st, ctx) {
               Aligned => GateDecision::Proceed,
               Divergent(reason) => GateDecision::Skip { reason },
           }
       }
   }
   ```

2. **Group execution wrapper** — The SAVEPOINT-based atomic execution is
   separate from gating. It wraps a set of refresh calls in a transaction
   boundary. This is an **execution strategy**, not a gate:

   ```rust
   trait GroupExecutionStrategy {
       fn execute_group(&self, members: &[NodeId], refresh_fn: &dyn Fn(NodeId) -> Result<()>) -> Result<()>;
   }

   struct SavepointStrategy;  // Extension: SPI SAVEPOINT
   struct TokioTxStrategy;    // Sidecar: pgwire BEGIN/SAVEPOINT
   ```

   This cleanly separates the "should we refresh?" question (gate) from
   the "how do we execute the group atomically?" question (strategy).

---

### 1.6 Cross-Source Snapshot Consistency — Independent-Source Coherence

**Source:** [PLAN_CROSS_SOURCE_SNAPSHOT_CONSISTENCY.md](../sql/PLAN_CROSS_SOURCE_SNAPSHOT_CONSISTENCY.md)

#### What It Does

Addresses the case where D joins B2 and C2, but B2 and C2 depend on
**independent** base tables (B1 and C1) with no shared ancestor. The diamond
algorithm cannot detect this structurally. Three approaches:

- **Approach A:** Shared `REPEATABLE READ` transaction for co-refresh groups
- **Approach B:** User-declared co-refresh groups with configurable isolation
- **Approach C:** Global LSN watermark per scheduler tick

#### Standalone Project Potential: **Low-Medium**

This feature is fundamentally about **PostgreSQL transaction isolation** —
it controls the isolation level of the refresh execution context. The logic
is thin (choose isolation level, wrap execution in appropriate transaction),
and the value is entirely PG-specific.

What *is* extractable is the **group management** and **alignment checking**
logic:

| Component | PG-coupled? | Extractable? |
|-----------|-------------|-------------|
| Co-refresh group detection / management | No | ✅ |
| LSN watermark computation (`pg_current_wal_lsn`) | Yes | Stays in extension |
| `REPEATABLE READ` transaction wrapping | Yes | Adapter pattern |
| `create_refresh_group()` / `drop_refresh_group()` SQL API | Yes | Stays in extension |
| Group membership validation | Partially (graph queries are pure, catalog lookups are PG) | Split |

#### Internal Decomposition Opportunities

1. **Extends `GroupExecutionStrategy`** — Cross-source snapshot is the same
   group execution concept as diamond consistency, but with a stronger
   isolation level:

   ```rust
   struct RepeatableReadStrategy;  // REPEATABLE READ + SAVEPOINT
   struct ReadCommittedStrategy;   // READ COMMITTED + SAVEPOINT (diamond)
   ```

   The `ConsistencyGroup` struct from the diamond plan gains an
   `isolation_level` field — the cross-source plan explicitly proposes this.

2. **LSN watermark as a global gate** — Approach C (capping all refreshes
   in a tick to a single WAL LSN) is a global gate applied before any
   per-ST gate. It fits the pipeline:

   ```
   LSN Watermark (global) → Status → Fuse → Watermark → Diamond → Refresh
   ```

3. **User-declared groups compose with auto-detected groups** — The plan
   specifies that declared co-refresh groups merge with auto-detected
   diamond groups during DAG rebuild. A declared group can override the
   isolation level of an auto-detected group. This is clean composition.

#### Relationship to Watermark Gating

These two features are **complementary layers** addressing different
freshness domains:

| Concern | Mechanism | Scope |
|---------|-----------|-------|
| External temporal coherence | Watermark gating | Cross-source (external APIs, ETL) |
| PG-internal snapshot coherence | Cross-source snapshot | Cross-source (independent PG tables) |
| Same-source split-path atomicity | Diamond consistency | Same-source diamond DAGs |

All three can apply simultaneously to the same ST, and they compose
naturally: watermark gates run first (external), then diamond/cross-source
gates (PG-internal).

---

### 1.7 Transactional IVM (IMMEDIATE Mode) — Same-Transaction Maintenance

**Source:** [PLAN_TRANSACTIONAL_IVM.md](../sql/PLAN_TRANSACTIONAL_IVM.md)

#### What It Does

Update stream tables **within the same transaction** as base table DML, using
statement-level AFTER triggers with transition tables. Provides
read-your-writes consistency. Serves as a drop-in replacement for pg_ivm.

#### Standalone Project Potential: **Low** (but high internal decomposition value)

Transactional IVM is deeply PG-specific — it relies on PostgreSQL's trigger
infrastructure, transition tables, Ephemeral Named Relations (ENRs), and
transaction isolation semantics. It cannot meaningfully exist outside PG.

However, the **DVM engine output is pure SQL** — the delta computation
produces SQL strings, not runtime code. This is the critical insight from
REPORT_EXTERNAL_PROCESS.md §12.1: the sidecar can pre-compile delta SQL
into PL/pgSQL trigger functions, achieving IMMEDIATE mode without the
extension.

| Component | PG-coupled? | Extractable? |
|-----------|-------------|-------------|
| `DeltaSource` enum (how Scan operators emit SQL) | No | ✅ Core abstraction |
| Delta SQL template generation | No | ✅ Already in DVM engine |
| `CachedMergeTemplate` (INSERT/DELETE/MERGE SQL) | No | ✅ |
| Trigger function installation (CREATE TRIGGER DDL) | Yes | SQL generation extractable |
| ENR registration / transition table access | Yes — `pg_sys` C API | Extension only |
| Before/after trigger counting + locking | Yes — SPI/pg_sys | Extension only |
| PL/pgSQL compiled trigger bodies (sidecar) | Yes — SQL DDL | Sidecar-specific |
| pg_ivm compatibility layer (`pgivm.*` functions) | Yes | Extension only |

#### Internal Decomposition Opportunities

1. **`DeltaSource` abstraction** — This is the key composability point.
   The DVM engine's Scan operator already needs to know where delta rows
   come from. Making this a first-class enum enables three modes from the
   same operator tree:

   ```rust
   pub enum DeltaSource {
       /// Deferred mode: change buffer tables with LSN range filter.
       ChangeBuffer { table: String, lsn_range: (String, String) },
       /// Immediate mode (extension): ENRs from transition tables.
       TransitionTable { old_name: String, new_name: String },
       /// Immediate mode (sidecar): same SQL, embedded in PL/pgSQL.
       CompiledTrigger { old_name: String, new_name: String },
   }
   ```

   In practice, `TransitionTable` and `CompiledTrigger` produce **identical
   SQL** — only the execution context differs (C-level SPI vs PL/pgSQL
   `EXECUTE`). They could be a single variant.

2. **Template compiler** — A `generate_immediate_trigger_sql()` function
   that takes a delta template and produces a complete PL/pgSQL trigger
   function body. This lives in `pgtrickle-core` and is consumed by:
   - The extension (for installing Rust-native triggers in Phase 1,
     or PL/pgSQL triggers as a fallback)
   - The sidecar (for installing compiled triggers remotely)

3. **NOT a RefreshGate** — Transactional IVM is fundamentally different
   from the deferred features. It doesn't participate in the scheduler
   loop at all — it fires synchronously within user transactions via
   triggers. The `RefreshGate` pattern does not apply. Instead, IMMEDIATE
   mode STs bypass the scheduler entirely.

4. **Mode switching as a lifecycle transition** — Switching between
   DIFFERENTIAL and IMMEDIATE mode (drop CDC triggers, create IVM triggers,
   full refresh) is a lifecycle state machine transition:

   ```
   DIFFERENTIAL ←→ IMMEDIATE ←→ FULL
   ```

   Each transition requires cleanup of the old mode's infrastructure and
   setup of the new mode's. This fits the `Lifecycle` trait from §2.3.

#### Relationship to Other Features

| Feature | Interaction with Transactional IVM |
|---------|-----------------------------------|
| Fuse | N/A — IMMEDIATE mode has no change buffer to count. Could monitor trigger-applied delta sizes instead, but the fuse concept is less relevant (changes are applied immediately, not batched). |
| Watermark | N/A — IMMEDIATE mode refreshes synchronously within user transactions. External watermarks are a deferred-mode concept. |
| Diamond | **Inherently solved** — trigger nesting ensures B and C are both updated within the same transaction as A's modification. No consistency group needed. |
| Cross-source snapshot | **Inherently solved** — all changes visible within the same transaction snapshot. |
| Blue-green | Compatible — a green ST could use IMMEDIATE mode while the blue ST uses DIFFERENTIAL. But the catch-up semantics differ fundamentally. |
| Sidecar | ✅ Via compiled PL/pgSQL triggers (REPORT_EXTERNAL_PROCESS §12.1). Extension retains performance advantage (native Rust dispatch vs PL/pgSQL `EXECUTE`). |

---

## 2. Cross-Cutting Patterns

### 2.1 The RefreshGate Abstraction

All four features (plus existing mechanisms) follow the same pattern: a
**predicate evaluated before refresh** that decides whether to proceed.

```rust
/// A composable gate that decides whether a stream table should refresh.
pub trait RefreshGate: Send + Sync {
    /// Evaluate the gate for a given stream table and refresh context.
    fn evaluate(&self, st: &StreamTableMeta, ctx: &GateContext) -> GateDecision;

    /// Human-readable name for logging and introspection.
    fn name(&self) -> &str;
}

pub enum GateDecision {
    /// Proceed with refresh.
    Proceed,
    /// Skip this refresh cycle; re-evaluate next tick.
    Skip { reason: String },
    /// Permanently block until explicit reset (fuse-blown semantics).
    Block { reason: String },
}

pub struct GateContext {
    pub change_buffer_count: Option<i64>,
    pub source_watermarks: HashMap<Oid, Option<DateTime<Utc>>>,
    pub frontier: Frontier,
    pub green_of: Option<i64>,
    pub consistency_group: Option<ConsistencyGroupId>,
    // ... extensible
}
```

**Every existing and proposed scheduling check maps to this trait:**

| Gate | Current Implementation | RefreshGate equivalent |
|------|----------------------|----------------------|
| Status check (ACTIVE?) | Inline in scheduler loop | `StatusGate` |
| Schedule check (due?) | Inline in scheduler loop | `ScheduleGate` |
| Advisory lock | Inline in scheduler loop | `LockGate` |
| Upstream changes check | Inline in scheduler loop | `UpstreamChangesGate` |
| Adaptive DIFF→FULL fallback | Inline in refresh logic | (Not a gate — mode selection) |
| **Fuse** | Proposed inline check | `FuseGate` |
| **Watermark alignment** | Proposed inline check | `WatermarkGate` |
| **Diamond consistency** | Proposed group check | `DiamondConsistencyGate` |
| **Cross-source snapshot** | Proposed group check | `SnapshotCoherenceGate` |
| **Blue-green convergence** | Proposed convergence check | `ConvergenceGate` |
| **LSN tick watermark** | Not yet implemented | `LsnWatermarkGate` (global) |

The scheduler becomes a **gate pipeline**:

```rust
fn should_refresh(st: &StreamTableMeta, ctx: &GateContext, gates: &[&dyn RefreshGate]) -> GateDecision {
    for gate in gates {
        match gate.evaluate(st, ctx) {
            GateDecision::Proceed => continue,
            decision => return decision,
        }
    }
    GateDecision::Proceed
}
```

**Benefits:**
- Each gate is independently testable (unit tests, no PG).
- Gates compose: a deployment can stack fuse + watermark + diamond checks.
- New gates can be added without modifying the scheduler loop.
- The sidecar and extension share the same gate implementations.
- Gate evaluation order is explicit and configurable.

### 2.2 Shared Statistics Engine

Both fuse and potential future features need streaming statistics:

| Feature | Needs |
|---------|-------|
| Fuse (adaptive mode) | Rolling mean + stddev of delta sizes |
| Adaptive scheduling | Change rate trends for interval tuning |
| Blue-green convergence | Lag rate estimation for ETA |
| Monitoring / alerting | Anomaly detection on any metric |

A small `StreamingStats` module would serve all:

```rust
pub struct RollingStats {
    window: VecDeque<f64>,
    max_window: usize,
}

impl RollingStats {
    pub fn push(&mut self, value: f64);
    pub fn mean(&self) -> Option<f64>;
    pub fn stddev(&self) -> Option<f64>;
    pub fn is_anomalous(&self, value: f64, sensitivity: f64) -> bool;
}

pub struct EwmaStats {
    alpha: f64,
    mean: f64,
    variance: f64,
}
```

Pure Rust, no PG dependency, trivially extractable.

### 2.3 Lifecycle State Machines

Multiple features use state machines with similar patterns:

| Feature | States | Transitions |
|---------|--------|------------|
| Fuse | INTACT → BLOWN | blow (automatic) / reset (manual) |
| Blue-green | NONE → GREEN_CATCHING_UP → CONVERGED → PROMOTED / ROLLED_BACK | create / converge / promote / rollback |
| CDC mode | TRIGGER → TRANSITIONING → WAL | trigger threshold / WAL ready |
| Refresh mode | DIFFERENTIAL ↔ IMMEDIATE ↔ FULL | alter_stream_table (mode switch triggers cleanup + setup) |
| ST status | ACTIVE → SUSPENDED → ERROR → ACTIVE | error / recovery / manual |

A generic state machine with transition validation:

```rust
pub trait Lifecycle: Sized {
    type Event;
    fn transition(self, event: Self::Event) -> Result<Self, InvalidTransition>;
    fn is_terminal(&self) -> bool;
}
```

---

## 3. Composability Matrix

How the four features interact with each other and with the proposed
abstractions:

```
            ┌──────────────────────────────────────────────────────────────────┐
            │                        Scheduler Loop                           │
            │                                                                  │
            │  ┌─────────┐ ┌──────┐ ┌─────────┐ ┌─────────┐ ┌────────────┐   │
            │  │LSN Tick  │→│Fuse  │→│Watermark│→│Snapshot │→│Diamond     │   │
            │  │Watermark │ │Gate  │ │Gate     │ │Coherence│ │Consistency │   │
            │  │(global)  │ │      │ │         │ │Gate     │ │Gate        │   │
            │  └─────────┘ └──────┘ └─────────┘ └─────────┘ └────────────┘   │
            │       │           │         │           │            │           │
            │       ▼           ▼         ▼           ▼            ▼           │
            │  ┌──────────────────────────────────────────────────────────┐    │
            │  │   All gates passed → GroupExecutionStrategy → Refresh    │    │
            │  └──────────────────────────────────────────────────────────┘    │
            │       │                                                          │
            │       ▼                                                          │
            │  ┌──────────────────────────────────────────────────────────┐    │
            │  │     Blue-Green: convergence check (post-refresh)        │    │
            │  └──────────────────────────────────────────────────────────┘    │
            └──────────────────────────────────────────────────────────────────┘

            ┌──────────────────────────────────────────────────────────────────┐
            │               IMMEDIATE Mode (bypasses scheduler)                │
            │                                                                  │
            │  User DML → BEFORE trigger → AFTER trigger (transition tables)   │
            │           → DeltaSource::TransitionTable → delta SQL → MERGE     │
            └──────────────────────────────────────────────────────────────────┘
```

### Feature Interaction Table

| Feature A × Feature B | Interaction | Composable? |
|----------------------|-------------|-------------|
| Fuse × Watermark | Independent gates — both must pass. Fuse checks change volume; watermark checks temporal alignment. Ordered: fuse first (cheaper). | ✅ Trivially composable |
| Fuse × Blue-green | Fuse protects both blue and green STs independently. A blown fuse on a green ST pauses catch-up but doesn't affect blue. | ✅ Independent evaluation |
| Fuse × Diamond | Blown fuse on any diamond group member blocks the entire group (all-or-nothing semantics). | ✅ Via group-aware gate |
| Watermark × Blue-green | Green ST inherits watermark group membership from blue. Watermark gating applies to green during catch-up. | ✅ Inherited configuration |
| Watermark × Diamond | Diamond groups operate on PG-internal consistency; watermarks on external temporal consistency. Orthogonal layers. | ✅ Independent layers |
| Watermark × Cross-source | Watermark = external temporal coherence; cross-source = PG-internal snapshot coherence. Complementary layers; a ST can have both. | ✅ Independent layers |
| Diamond × Cross-source | Diamond detects shared-ancestor splits (auto). Cross-source handles independent-source joins (user-declared). Groups merge during DAG rebuild; declared isolation overrides auto-detected. | ✅ Merged groups |
| Diamond × Transactional IVM | IMMEDIATE mode **inherently solves** diamond inconsistency — trigger nesting ensures all paths update within the same transaction. No group needed. | ✅ Orthogonal (IMMEDIATE bypasses) |
| Cross-source × Transactional IVM | IMMEDIATE mode inherently provides snapshot coherence within a single transaction. Cross-source groups are a deferred-mode concept. | ✅ Orthogonal |
| Fuse × Transactional IVM | Fuse gates deferred refresh; IMMEDIATE STs bypass the scheduler entirely. Fuse does not apply to IMMEDIATE STs. | ✅ Independent (different modes) |
| Blue-green × Diamond | Green STs are new entities with their own `pgt_id` — not part of the blue ST's diamond group. Promoted green ST joins the group. | ✅ Via catalog update |
| Fuse × Watermark × Blue-green | Green ST catching up: watermark gate must pass AND fuse must be intact. All three compose naturally through the gate pipeline. | ✅ Multi-gate pipeline |
| All deferred gates × Transactional IVM | IMMEDIATE STs are excluded from the gate pipeline — they don't participate in the scheduler loop. Clean separation of execution models. | ✅ Mode-based dispatch |

---

## 4. Extraction Strategy by Feature

### 4.1 What to Extract (Separate Crates)

| Crate | Contains | Source Features | PG-Free? |
|-------|----------|----------------|----------|
| `pgtrickle-gates` | `RefreshGate` trait, `GateDecision`, `GateContext`, `FuseGate`, `WatermarkGate`, `DiamondConsistencyGate`, `SnapshotCoherenceGate`, `ConvergenceGate`, `LsnWatermarkGate` | Fuse, Watermark, Blue-green, Diamond, Cross-source, LSN watermark | ✅ Yes |
| `pgtrickle-stats` | `RollingStats`, `EwmaStats`, Welford's algorithm, anomaly detection | Fuse (adaptive), future adaptive scheduling | ✅ Yes |
| `pgtrickle-watermark` | `Watermark`, `WatermarkGroup`, alignment predicate, tolerance evaluation | Watermark gating | ✅ Yes |
| `pgtrickle-groups` | `ConsistencyGroup`, `GroupExecutionStrategy` trait, `IsolationLevel`, group merge logic | Diamond, Cross-source, (Watermark groups) | ✅ Yes (logic only; execution adapters are PG-coupled) |

These could live as modules in `pgtrickle-core` rather than independent
crates — the key point is that they have **zero PG dependency** and are
**independently testable**.

### 4.2 What to Keep Internal (Extension + Sidecar Adapters)

| Component | Why Not Extract | Shared How? |
|-----------|----------------|-------------|
| Blue-green orchestration (create/promote/rollback) | Deeply tied to catalog, CDC sharing, frontier management | `PgClient` trait — same orchestration logic, different DB client |
| Fuse catalog CRUD | SPI in extension, SQL in sidecar | `CatalogAccess` trait |
| Watermark `advance_watermark()` | Transaction semantics, LSN recording | `#[pg_extern]` in extension, SQL function in sidecar |
| Blue-green promote transaction | Advisory locks, table renames, catalog updates | Single SQL transaction in both modes |
| SAVEPOINT / REPEATABLE READ group execution | PG transaction control | `GroupExecutionStrategy` trait — SPI impl vs pgwire impl |
| IMMEDIATE mode trigger installation | PG DDL (CREATE TRIGGER) | SQL generation is extractable; execution is PG-coupled |
| IMMEDIATE mode delta application | ENR access (extension) or PL/pgSQL EXECUTE (sidecar) | Shared delta SQL template; different execution wrappers |
| pg_ivm compatibility layer | PL/pgSQL wrapper functions | Extension only (no sidecar equivalent needed) |

### 4.3 Sequencing

```
Phase 1 — Internal trait boundaries (no crate extraction)
  └─ Define RefreshGate trait
  └─ Define GroupExecutionStrategy trait
  └─ Define DeltaSource enum in DVM engine
  └─ Refactor existing scheduler checks into gate implementations
  └─ All gates unit-testable without PG

Phase 2 — Feature implementation using shared abstractions
  └─ Diamond: DiamondConsistencyGate + SavepointStrategy (IN PROGRESS)
  └─ Fuse: FuseGate + catalog + SQL API + scheduler integration
  └─ Cross-source snapshot: SnapshotCoherenceGate + RepeatableReadStrategy
         + user-declared groups + LSN tick watermark
  └─ Watermark: WatermarkGate + catalog + SQL API + scheduler integration
  └─ Transactional IVM: DeltaSource::TransitionTable + trigger installation
         + pg_ivm compatibility layer
  └─ Blue-green: ConvergenceGate + orchestration layer + SQL API

Phase 3 — Crate extraction (when sidecar work begins)
  └─ Move gate implementations to pgtrickle-core
  └─ Move DeltaSource + template compiler to pgtrickle-core
  └─ Sidecar scheduler composes same gates via Tokio loop
  └─ Sidecar installs compiled PL/pgSQL triggers for IMMEDIATE mode
  └─ Extension scheduler composes same gates via BGWorker loop
```

This ordering means features ship **before** extraction. The trait boundaries
are established in Phase 1 so that Phase 3 is mechanical, not a redesign.

---

## 5. Impact on Sidecar Feasibility

The sidecar report ([REPORT_EXTERNAL_PROCESS.md](REPORT_EXTERNAL_PROCESS.md))
estimated 15–22 weeks (including cross-plan concerns). How do the four features
affect this?

| Feature | Sidecar Impact | Additional Effort |
|---------|---------------|-------------------|
| Fuse | `should_blow()` is pure Rust — zero sidecar-specific work beyond wiring the gate | ~0 (free via shared core) |
| Watermark | Alignment predicate is pure Rust. `advance_watermark()` needs a SQL function in sidecar mode (PL/pgSQL wrapper calling the catalog update). | ~1 day |
| Blue-green | Convergence checks are pure Rust. Promote/rollback are SQL transactions — identical in extension and sidecar. | ~2 days |
| Diamond | `detect_consistency_groups()` is pure Rust in `pg-dag`. SAVEPOINT execution is standard SQL — works identically over pgwire. | ~0 (free via shared core) |
| Cross-source | Group management is pure Rust. `REPEATABLE READ` wrapping is standard SQL. LSN watermark query is a single SQL call. | ~1 day |
| Transactional IVM | Delta SQL templates are shared. Sidecar compiles them into PL/pgSQL trigger functions. Extension uses native Rust triggers. This is the **largest** sidecar-specific effort among all features. | ~1-2 weeks (compiled trigger generator + testing) |
| RefreshGate pipeline | Both schedulers compose the same gates. The gate interface is the **primary** shared abstraction. | ~0 (architectural benefit) |

**Net impact:** If features are built with the `RefreshGate` pattern and
`DeltaSource` abstraction, the sidecar gets most features essentially for
free. Transactional IVM is the exception — compiled PL/pgSQL triggers require
sidecar-specific development, but the delta SQL generation is fully shared.

The `RefreshGate` trait and `DeltaSource` enum are the **two highest-leverage
abstractions** for making all seven features composable across deployment modes.

---

## 6. Recommendations

### R1: Introduce `RefreshGate` trait before implementing any of the four features

This is the architectural foundation. Define it, refactor existing inline
checks into gate implementations, then build fuse/watermark/blue-green as
new gates. Estimated effort: 8–12 hours for the trait + refactoring.

### R2: Diamond consistency validates the group execution pattern

Diamond consistency is already IN PROGRESS. It should be the first feature
to use both `RefreshGate` (frontier alignment check) and
`GroupExecutionStrategy` (SAVEPOINT atomic groups). This validates two
abstractions at once.

### R3: Implement fuse next — smallest scope, highest standalone value

Fuse has the simplest interaction surface (single ST, single predicate,
no group semantics). It validates the `RefreshGate` pattern with a real
feature before watermark (groups, tolerance, LSN mapping) adds complexity.

### R4: Build watermark gating with `'gate'` mode only — defer `'hold_back'`

The plan's §5.4 already suggests this. Gate-only is a pure scheduling
predicate (fits `RefreshGate`). Hold-back changes the refresh data window,
requiring deeper frontier machinery changes — a separate phase.

### R5: Cross-source snapshot extends diamond — implement together or immediately after

Cross-source snapshot reuses `ConsistencyGroup` and `GroupExecutionStrategy`
from the diamond plan, adding `REPEATABLE READ` as an isolation option and
user-declared groups as a configuration mechanism. Implementing them in
sequence minimizes rework.

### R6: Define `DeltaSource` before transactional IVM implementation

The `DeltaSource` enum should be established in the DVM engine before
IMMediaTE mode work begins. This ensures the operator tree's Scan node is
parameterized from day one, and the sidecar's compiled-trigger path is
architecturally supported without retrofitting.

### R7: Treat blue-green as orchestration, not a gate

Blue-green uses `RefreshGate` for convergence detection, but its core
complexity is lifecycle management (create/promote/rollback). Keep the
orchestration in a dedicated module, with the convergence check plugged
in as a composable `RefreshGate`.

### R8: Do NOT extract crates until the sidecar work begins

Premature crate extraction adds build complexity without immediate benefit.
The internal trait boundaries (R1) give all the testability and composability
advantages without the workspace restructuring overhead. Extract when there
is a **consumer** (the sidecar) that needs the crate.

### R9: Treat external process as the integration test for composability

The sidecar is both a product and a forcing function. Every trait boundary
established for the features above is validated when the sidecar composes
the same logic. Plan the sidecar's MVP to include at least one gate (fuse)
and IMMEDIATE mode (compiled triggers) to prove both abstractions end-to-end.

---

## 7. Open Questions

1. **Should `RefreshGate` evaluation order be configurable?** The current
   proposal uses a fixed order (status → fuse → watermark → diamond). But
   different deployments may want different gate priorities. Is this
   over-engineering, or a genuine requirement?

2. **Should gates be async?** For the extension (bgworker), gates run
   synchronously via SPI. For the sidecar (Tokio), gate context retrieval
   (e.g., counting change buffer rows) is naturally async. Should the trait
   be `async fn evaluate()` with a sync wrapper for the extension?

3. **Gate context cost:** The `GateContext` includes fields like
   `change_buffer_count` that are expensive to compute. Should the context
   be lazily populated (each gate requests only what it needs), or eagerly
   computed once per ST per tick?

4. **Fuse × watermark ordering:** If the fuse blows on a watermark-gated
   ST, should the fuse reason mention that the ST was also watermark-gated?
   Or are the two reasons independent? For user comprehension, showing all
   active gates and their states in `pgt_status()` would be ideal.

5. **Blue-green and the `RefreshGate` pattern:** The promote/rollback
   lifecycle doesn't fit the per-tick gate model. Should blue-green have
   its own orchestration interface separate from `RefreshGate`, or should
   `ConvergenceGate` be sufficient to model the "is green ready?" question?

6. **Transactional IVM and fuse:** Should IMMEDIATE mode STs have any
   form of anomaly protection? The fuse concept doesn't directly apply
   (no batched change buffer), but a per-trigger delta size check could
   serve a similar role. Is this worth the complexity, or should users
   rely on application-level safeguards for IMMEDIATE mode?

7. **`GroupExecutionStrategy` vs inline transaction control:** Is the
   trait abstraction for group execution worth the indirection? The
   extension and sidecar both emit the same SQL (`SAVEPOINT`, `BEGIN
   ISOLATION LEVEL REPEATABLE READ`). The trait's value is primarily
   testability (mock execution strategy for unit tests).

8. **Cross-source + watermark group unification:** User-declared
   co-refresh groups (cross-source) and watermark groups both manage
   sets of sources with alignment semantics. Should they share a catalog
   table or remain separate? They serve different purposes (PG snapshot
   coherence vs external temporal alignment), but the management UX
   overlaps.

---

## Related Documents

| Document | Relationship |
|----------|-------------|
| [REPORT_ENGINE_COMPOSABILITY.md](REPORT_ENGINE_COMPOSABILITY.md) | General module-level extraction analysis (complementary) |
| [REPORT_EXTERNAL_PROCESS.md](REPORT_EXTERNAL_PROCESS.md) | Sidecar feasibility — primary consumer of extracted components |
| [REPORT_BLUE_GREEN_DEPLOYMENT.md](REPORT_BLUE_GREEN_DEPLOYMENT.md) | Full blue-green design |
| [PLAN_FUSE.md](../sql/PLAN_FUSE.md) | Full fuse design |
| Historical watermark-gating implementation | Full watermark gating design |
| [PLAN_DIAMOND_DEPENDENCY_CONSISTENCY.md](../sql/PLAN_DIAMOND_DEPENDENCY_CONSISTENCY.md) | Diamond consistency — atomic refresh groups |
| [PLAN_CROSS_SOURCE_SNAPSHOT_CONSISTENCY.md](../sql/PLAN_CROSS_SOURCE_SNAPSHOT_CONSISTENCY.md) | Cross-source snapshot consistency — REPEATABLE READ groups |
| [PLAN_TRANSACTIONAL_IVM.md](../sql/PLAN_TRANSACTIONAL_IVM.md) | Transactional IVM — IMMEDIATE mode with transition tables |
| [PLAN_ECO_SYSTEM.md](../ecosystem/PLAN_ECO_SYSTEM.md) | Ecosystem integration plan |
