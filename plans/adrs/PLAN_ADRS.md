# Plan: Architecture Decision Records

Date: 2026-02-24
Status: ACTIVE
Last Updated: 2026-07-01 (v0.61.0 — ADR-001, ADR-002, ADR-003 promoted to ACTIVE files)

---

## Overview

This plan proposes a comprehensive set of Architecture Decision Records (ADRs)
for pg_trickle — covering both **decisions already made** during development and
**forward-looking decisions** needed to achieve full PostgreSQL and SQL feature
coverage.

The goal is to eventually support all relevant PostgreSQL and SQL features in
both FULL and DIFFERENTIAL refresh modes. Each ADR documents the reasoning
behind a significant technical choice, including alternatives considered and
consequences.

### ADR Format

Each ADR follows a standard template:

```markdown
# ADR-NNN: <Title>

| Field         | Value                   |
|---------------|-------------------------|
| **Status**    | Accepted / Superseded / Proposed / Not Started |
| **Date**      | YYYY-MM-DD              |
| **Deciders**  | pg_trickle core team     |
| **Category**  | <area>                  |

## Context
## Decision
## Options Considered
## Consequences
## References
```

### Numbering Convention

- **ADR-001–009**: Core architecture (CDC, IVM engine, storage)
- **ADR-010–019**: API & schema design
- **ADR-020–029**: Scheduling & runtime
- **ADR-030–039**: Tooling, testing, ecosystem
- **ADR-040–049**: Performance & optimization
- **ADR-050–059**: SQL feature coverage & operator design
- **ADR-060–069**: PostgreSQL integration & compatibility
- **ADR-070–079**: Correctness & safety guarantees

---

## Part 1: Decisions Already Made

These ADRs document technical choices that have been implemented. The decisions
are settled; the ADR documents capture the rationale so future contributors
understand the "why."

### ADR-001: Trigger-Based CDC over Logical Replication

| Field | Value |
|-------|-------|
| **Status** | ✅ Accepted — see [ADR-001.md](ADR-001.md) |
| **Category** | CDC |
| **Implemented** | v0.61.0 (promoted from implied decision) |

**Decision:** Use row/statement-level AFTER triggers as the default CDC
mechanism (WAL-based CDC as opt-in `REFRESH MODE WAL`).  Single-transaction
atomicity and simpler failure modes justify the write-amplification trade-off.

---

### ADR-002: Z-Set / Multiset Formalism

| Field | Value |
|-------|-------|
| **Status** | ✅ Accepted — see [ADR-002.md](ADR-002.md) |
| **Category** | DVM Engine |
| **Implemented** | v0.61.0 (promoted from implied decision) |

**Decision:** The differential engine uses Z-sets (signed integer
multiplicities).  Negative multiplicities represent deletions; all stream-table
rows settle to multiplicity 0 (absent) or 1 (present) after each refresh.

---

### ADR-003: EC-01 Join-Correctness Invariant and the PHD1 Cleanup Pass

| Field | Value |
|-------|-------|
| **Status** | ✅ Accepted — see [ADR-003.md](ADR-003.md) |
| **Category** | Correctness |
| **Implemented** | v0.38.0 (R₀ snapshot-splitting); v0.61.0 (ctid invariant documented) |

**Decision:** Outer-join delta rules use split R₀ snapshots (EC-01); the PHD1
cleanup pass corrects residual phantom rows after each differential refresh of
outer-join stream tables.

---

### ADR-004: Frontier Durability Model — Single-Phase Canonical Path

| Field | Value |
|-------|-------|
| **Status** | ✅ Accepted — see [ADR-004.md](ADR-004.md) |
| **Category** | Correctness / Durability |
| **Implemented** | v0.72.0 |

**Decision:** The DUR-1 two-phase tentative-frontier design (never shipped,
dead code) is removed. The canonical single-phase frontier path
(`store_frontier` / `store_frontier_and_complete_refresh`) is the only
mechanism for advancing the frontier. Frontier-store failures are fatal —
they abort the refresh transaction, preventing partial commits. PostgreSQL WAL
atomicity guarantees that the frontier and the data change are committed
together.

---

### ADR-002: Hybrid CDC — Trigger Bootstrap with WAL Steady-State

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Category** | CDC |
| **Sources** | `plans/sql/PLAN_HYBRID_CDC.md`, `plans/sql/REPORT_TRIGGERS_VS_REPLICATION.md` |

**Decision:** After ADR-001 chose triggers as default, implement a hybrid
approach: use triggers at creation time (zero-config, atomic), then
transparently transition to logical replication for steady-state if
`wal_level = logical`.

**Key points:**
- Three CDC states: TRIGGER → TRANSITIONING → WAL
- No-data-loss transition (trigger stays active until WAL catches up)
- Graceful fallback if slot creation fails or WAL decoder doesn't catch up
  within `pg_trickle.wal_transition_timeout`
- Same buffer table schema regardless of CDC mode
- `pg_trickle.cdc_mode` GUC for user control (auto/trigger/wal)

---

### ADR-003: Query Differentiation via Operator Tree (DVM Engine Design)

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Category** | IVM Engine |
| **Sources** | `plans/PLAN.md` Phase 6, `docs/DVM_OPERATORS.md`, `docs/ARCHITECTURE.md` |

**Decision:** Implement incremental view maintenance by parsing the defining
query into an operator tree (`OpTree`) and applying per-operator differentiation
rules (analogous to automatic differentiation in calculus) to generate delta SQL.

**Alternatives considered:**
- Full recomputation only (simple but O(n) always)
- Log-based delta replay (simpler operators, less SQL coverage)
- DBSP-style Z-sets with explicit multiplicity tracking
- pg_ivm's approach (limited to single-table aggregates at the time)

**Key points:**
- 21 OpTree variants: Scan, Filter, Project, InnerJoin, LeftJoin, FullJoin,
  Aggregate, Distinct, UnionAll, Intersect, Except, Subquery, CteScan,
  RecursiveCte, Window, LateralFunction, LateralSubquery, SemiJoin, AntiJoin,
  ScalarSubquery (+more planned)
- Delta SQL is generated as CTEs, not materialized intermediates
- Row identity via `__pgt_row_id` (xxHash) for diff-based delta application
- Theoretical basis: DBSP (Budiu et al. 2023), Gupta & Mumick (1995)

---

### ADR-004: xxHash Row IDs Instead of UUIDs

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Category** | Storage / IVM Engine |
| **Sources** | `plans/PLAN.md` Key Design Decisions, `src/hash.rs`, `src/dvm/row_id.rs` |

**Decision:** Use 64-bit xxHash of the primary key as the `__pgt_row_id`
column (stored as `BIGINT`) rather than UUIDs or composite-key matching.

**Alternatives considered:**
- UUID v4 (128-bit, zero collision, 16 bytes per row)
- Composite primary key matching (no extra column, but complex MERGE logic)
- MD5/SHA hash (cryptographically stronger but slower)

**Key points:**
- 8 bytes vs 16 bytes per row (significant at scale)
- Collision probability: ~1 in 2^64 per unique key — acceptable for practical
  datasets
- `pg_trickle_hash()` for single-column PKs, `pg_trickle_hash_multi()` for
  composites
- Visible to users via `SELECT *` — a known tradeoff

---

### ADR-005: Per-Table Change Buffer Tables Instead of In-Memory Queues

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Category** | CDC / Storage |
| **Sources** | `plans/PLAN.md` Key Design Decisions, `src/cdc.rs` |

**Decision:** Store CDC changes in dedicated PostgreSQL tables
(`pgtrickle_changes.changes_<oid>`) rather than in shared memory, message
queues, or a single global changes table.

**Alternatives considered:**
- Shared memory ring buffer (fast, but limited size, not crash-safe)
- Single global changes table (simpler, but contention on high-write workloads)
- External message queue (Kafka, NATS — unnecessary dependency)

**Key points:**
- Crash-safe: survives backend/worker crashes
- Queryable for debugging and monitoring
- Per-table isolation avoids contention across independent source tables
- Aggressive cleanup after each refresh cycle
- Trade-off: extra I/O vs. durability and simplicity

---

### ADR-006: Explicit DML for User Triggers Instead of Always-MERGE

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Category** | Refresh Engine |
| **Sources** | `plans/sql/PLAN_USER_TRIGGERS_EXPLICIT_DML.md` |

**Decision:** When a stream table has user-defined triggers, decompose the
MERGE into three explicit DML statements (DELETE, UPDATE, INSERT) so triggers
fire with correct `TG_OP`, `OLD`, and `NEW`. When no user triggers exist,
keep the fast single-MERGE path.

**Alternatives considered:**
- Always use explicit DML (simpler code, but ~10-30% slower for the common case)
- Always use MERGE + replay triggers after (complex, wrong `TG_OP` context)
- Disallow user triggers on stream tables entirely

**Key points:**
- `has_user_triggers()` detection at refresh time
- `CachedMergeTemplate` extended with explicit DML templates
- `pg_trickle.user_triggers` GUC (canonical `auto` / `off`, deprecated `on` alias)
- FULL refresh: triggers suppressed via `DISABLE TRIGGER USER` + `NOTIFY`

---

### ADR-007: Semi-Naive Evaluation for Recursive CTEs

| Field | Value |
|-------|-------|
| **Status** | Accepted (Updated) |
| **Category** | IVM Engine |
| **Sources** | `docs/DVM_OPERATORS.md`, `src/dvm/operators/recursive_cte.rs` |

**Decision:** Handle `WITH RECURSIVE` CTEs using three strategies in
DIFFERENTIAL mode, selected automatically based on column compatibility and
change type. FULL mode continues to execute the query as-is.

**Key points:**
- FULL mode: query executes as-is (PostgreSQL handles recursion natively)
- DIFFERENTIAL mode uses three strategies:
  1. **Semi-naive evaluation** — INSERT-only changes: differentiate the base
     case, then propagate new rows through the recursive term via a nested
     `WITH RECURSIVE`
  2. **Delete-and-Rederive (DRed)** — mixed INSERT/DELETE/UPDATE changes:
     insert propagation → over-deletion cascade → rederivation → combine
  3. **Recomputation fallback** — when CTE columns ⊃ ST storage columns
     (column mismatch), re-execute the full query and diff against storage
- Strategy selection is automatic: column match + INSERT-only → semi-naive;
  column match + mixed → DRed; column mismatch → recomputation
- Non-linear recursion (multiple self-references in the recursive term) is
  rejected — PostgreSQL restricts the recursive term to reference the CTE
  at most once

---

### ADR-008: Group-Rescan Strategy for Non-Algebraic Aggregates

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Category** | IVM Engine |
| **Sources** | `docs/DVM_OPERATORS.md`, `src/dvm/operators/aggregate.rs` |

**Decision:** For aggregates that cannot be maintained algebraically
(STRING_AGG, ARRAY_AGG, JSON_AGG, BOOL_AND/OR, statistical functions, etc.),
use a NULL-sentinel approach: when any row in a group changes, return NULL for
the aggregate value, triggering re-aggregation from source data.

**Key points:**
- Algebraic: COUNT, SUM, AVG (maintained via auxiliary counters — O(1) per change)
- Semi-algebraic: MIN, MAX (O(1) for non-extremum changes, rescan on extremum
  deletion)
- Group-rescan: 17+ aggregates (STRING_AGG, ARRAY_AGG, JSON_AGG, BOOL_AND/OR,
  BIT_AND/OR/XOR, STDDEV/VAR, MODE, PERCENTILE_CONT/DISC, etc.)
- Group-rescan is correct and handles arbitrary aggregates; trade-off is O(group)
  per affected group
- Unified pattern: adding new group-rescan aggregates is a copy-paste exercise

---

### ADR-010: SQL Functions Instead of DDL Syntax

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Category** | API Design |
| **Sources** | `plans/PLAN.md` Key Design Decisions |

**Decision:** Expose the API as SQL functions (`pgtrickle.create_stream_table()`,
etc.) rather than custom DDL syntax (`CREATE STREAM TABLE ...`).

**Alternatives considered:**
- Custom DDL via PostgreSQL parser hooks or grammar extension
- Foreign Data Wrapper interface
- Hook-based interception of `CREATE MATERIALIZED VIEW`

**Key points:**
- Works without PostgreSQL parser modifications
- Clean extension boundary — standard `CREATE EXTENSION` installation
- Idiomatic PostgreSQL extension pattern
- Trade-off: less "native" feel, no `\d`-style psql integration

---

### ADR-011: `pgtrickle` Schema with `pgt_` Prefix Convention

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Category** | API Design / Naming |
| **Sources** | Code history (dt_ → st_ → pgt_ rename across 72 files) |

**Decision:** All internal catalog objects use the `pgtrickle` schema and `pgt_`
column/table prefix. Change buffers live in a separate `pgtrickle_changes` schema.

**Key points:**
- Original naming used `dt_` (derived table), renamed to `st_` (stream table),
  then to `pgt_` (pg_trickle) for global uniqueness and consistency
- Two schemas: `pgtrickle` (API + catalog) and `pgtrickle_changes` (buffer tables)
- `pgt_` prefix avoids collisions with user objects

---

### ADR-012: PostgreSQL 18 as Sole Target

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Category** | API Design / Platform |
| **Sources** | `plans/PLAN.md` Key Design Decisions |

**Decision:** Target PostgreSQL 18 exclusively. No backward compatibility with
PG 16 or PG 17.

**Alternatives considered:**
- Multi-version support via conditional compilation (broader adoption, higher
  maintenance)
- Target PG 17 as minimum (more users, but miss PG 18 features)

**Key points:**
- PG 18 features used: custom cumulative statistics, improved logical
  replication, DSM improvements
- Narrows user base but simplifies development and testing
- pgrx 0.17.x provides PG 18 support

---

### ADR-020: Canonical Scheduling Periods (48·2ⁿ Seconds)

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Category** | Scheduling |
| **Sources** | `plans/PLAN.md` Key Design Decisions, `src/scheduler.rs` |

**Decision:** Use a discrete set of canonical refresh periods (48, 96, 192, ...
seconds) rather than arbitrary user-specified intervals.

**Key points:**
- Guarantees `data_timestamp` alignment across stream tables with different
  schedules in the same DAG
- User-specified schedule is snapped to the nearest (smaller) canonical period
- NULL schedule = DOWNSTREAM (refresh only when triggered by a dependent)
- Advisory locks prevent concurrent refreshes of the same ST

---

### ADR-021: Single Background Worker Scheduler

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Category** | Scheduling / Runtime |
| **Sources** | `src/scheduler.rs`, `src/shmem.rs`, `docs/ARCHITECTURE.md` |

**Decision:** Use a single background worker for scheduling, with shared memory
for inter-process communication (`PgLwLock<PgTrickleSharedState>` and
`PgAtomic<AtomicU64>` DAG rebuild signal).

**Key points:**
- Wakes at `pg_trickle.scheduler_interval_ms` intervals
- Detects DAG changes via atomic counter comparison (lock-free)
- Topological refresh ordering within each wake cycle
- `SIGTERM` graceful shutdown
- `pg_trickle.enabled` GUC to disable without unloading

---

### ADR-022: Replication Origin for Feedback Loop Prevention

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Category** | Refresh Engine |
| **Sources** | `plans/PLAN.md` Key Design Decisions, `src/refresh.rs` |

**Decision:** Use PostgreSQL's replication origin mechanism
(`pg_trickle_refresh`) to tag refresh-generated writes, preventing CDC triggers
from re-capturing changes made by the refresh itself (feedback loops).

**Key points:**
- Standard PostgreSQL mechanism (`pg_replication_origin_session_setup`)
- Reliable filtering in the trigger function
- No user-visible side effects

---

### ADR-023: Adaptive Full-Refresh Fallback

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Category** | Refresh Engine / Performance |
| **Sources** | `docs/ARCHITECTURE.md`, `src/refresh.rs` |

**Decision:** When the change ratio exceeds
`pg_trickle.differential_max_change_ratio`, automatically downgrade a
DIFFERENTIAL refresh to FULL, since delta processing becomes more expensive
than full recomputation at high change rates.

**Key points:**
- Benchmarks show DIFFERENTIAL is slower than FULL at ~50% change rate
- Automatic switching keeps the default experience fast
- Per-stream-table `auto_threshold` in catalog allows tuning
- `last_full_ms` tracks full-refresh cost for adaptive comparison

---

### ADR-024: Dynamic Workers as Sole Scheduling Path (Pool Removal)

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Category** | Scheduling & Runtime |
| **Sources** | `src/scheduler/mod.rs`, `plans/PLAN_OVERALL_ASSESSMENT_13.md` |
| **Decided** | v0.68.0 (SCAL-001) |

**Decision:** Remove the persistent worker pool (`pool.rs`) and adopt
on-demand dynamic background worker spawning as the sole scheduling path.

**Context:**  A persistent worker pool was introduced in SCAL-5 (v0.25.0)
as an optimisation to avoid per-tick BGW registration overhead.  Assessment 13
found the pool path is never activated in practice: `pg_trickle.worker_pool_size`
defaults to 0 and no production deployment has enabled it.  The pool code
diverged from the dynamic-worker path, carrying duplicate job execution logic
that received no maintenance.  Activating it would break on the current job
model (fused chains, SCC cycles, immediate closures) because the pool executor
pre-dates all of those execution strategies.

**Decision points:**
- **Keep pool:** would require re-implementing all execution strategies in the
  pool loop; estimated ~3 weeks effort with high correctness risk.
- **Delete pool:** one-time 297-line deletion; dynamic workers handle the same
  use-cases with lower latency variance (no idle spin in the pool).

**Key points:**
- Dynamic BGWs are spawned per-tick and de-registered on completion.
- PostgreSQL's internal BGW management handles restart and cleanup.
- No operator-visible behaviour change — `worker_pool_size = 0` was the
  effective setting in all deployments.
- The GUC `pg_trickle.worker_pool_size` is retained as a no-op for backward
  compatibility of any operator scripts that reference it.

---

### ADR-030: dbt Integration via Macro Package (Not Custom Adapter)

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Category** | Ecosystem / Tooling |
| **Sources** | `plans/dbt/PLAN_DBT_MACRO.md`, `plans/dbt/PLAN_DBT_ADAPTER.md` |

**Decision:** Integrate with dbt via a Jinja macro package with a custom
`stream_table` materialization, using the standard `dbt-postgres` adapter.
Defer the full custom Python adapter as an upgrade path.

**Key points:**
- ~15 hours effort (vs ~54 for adapter)
- No Python code — pure Jinja SQL macros
- Works with dbt Core ≥ 1.6 (for `subdirectory` in `packages.yml`)
- Adapter plan exists as documented upgrade path in `plans/dbt/PLAN_DBT_ADAPTER.md`

---

### ADR-031: dbt Package In-Repo (Subdirectory) Instead of Separate Repository

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Category** | Ecosystem / Tooling |
| **Sources** | `plans/dbt/PLAN_DBT_MACRO.md`, `plans/ecosystem/PLAN_ECO_SYSTEM.md` |

**Decision:** Ship the dbt macro package as `dbt-pgtrickle/` inside the main
pg_trickle repository, not in a separate repo.

**Key points:**
- SQL API changes validated against macros in the same PR (via CI)
- Simpler contributor workflow — one repo, one PR
- Users install via `git:` + `subdirectory:` in `packages.yml`
- Extractable to separate repo later if needed

---

### ADR-032: Testcontainers-Based Integration Testing

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Category** | Testing |
| **Sources** | `AGENTS.md`, `plans/testing/STATUS_TESTING.md`, `tests/common/mod.rs` |

**Decision:** All integration and E2E tests use Docker containers via
testcontainers-rs and a custom E2E Docker image. Tests never assume a local
PostgreSQL installation.

**Key points:**
- Custom `Dockerfile.e2e` builds PG 18 + pg_trickle from source
- Deterministic, reproducible test environments
- Three-tier test pyramid: unit (no DB) → integration (testcontainers) → E2E
  (full extension Docker image)

---

### ADR-040: Aggregate Maintenance via Auxiliary Counter Columns

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Category** | IVM Engine / Performance |
| **Sources** | `docs/DVM_OPERATORS.md`, `src/dvm/operators/aggregate.rs` |

**Decision:** Maintain algebraic aggregates incrementally by storing auxiliary
counter columns alongside each aggregate result.

**Key points:**
- `COUNT(*)` maintained via `__pgt_count` counter
- `SUM(x)` maintained via `__pgt_sum_x` + `__pgt_count` for correctness when
  group shrinks to zero
- `AVG(x)` derived from SUM/COUNT at read time
- MIN/MAX uses semi-algebraic approach (CASE/LEAST/GREATEST with NULL sentinel
  for extremum deletion)
- Hidden auxiliary columns increase storage but enable O(1) aggregate updates

---

### ADR-041: LATERAL Diff via Row-Scoped Recomputation

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Category** | IVM Engine |
| **Sources** | `plans/sql/PLAN_LATERAL_JOINS.md`, `src/dvm/operators/lateral_function.rs` |

**Decision:** Differentiate LATERAL subqueries (and SRFs in FROM) by
**row-scoped recomputation**: when an outer row changes, re-execute the
correlated subquery for that specific row only.

**Key points:**
- Handles both implicit LATERAL (comma-syntax) and explicit `LEFT JOIN LATERAL`
- Supports top-N per group, correlated aggregation, multi-column derived values
- Correctness relies on re-executing the subquery in the context of the changed
  outer row — not on incremental maintenance of the inner query

---

## Part 2: Forward-Looking ADRs — SQL Feature Coverage

These ADRs address decisions that **have not yet been made** but are needed to
achieve comprehensive PostgreSQL and SQL support. They cover features currently
rejected, partially supported, or not yet considered.

### Current State Summary

- **49+ of 52 original SQL gaps resolved** (see `plans/sql/GAP_SQL_PHASE_4.md`)
- **Zero P0 (silent corruption) or P1 (incorrect semantics) issues remain**
- **25 aggregate functions** in DIFFERENTIAL mode; 17 recognized-but-rejected
- **All rejected constructs** have clear error messages with rewrite suggestions

### ADR-050: Non-Deterministic Function Handling Strategy

| Field | Value |
|-------|-------|
| **Status** | Not Started |
| **Category** | SQL Feature / Correctness |
| **Sources** | `plans/sql/PLAN_NON_DETERMINISM.md` |
| **Effort** | Medium (3-5 sessions) |

**Context:** Volatile functions (`random()`, `gen_random_uuid()`,
`clock_timestamp()`, `now()`) break delta computation in DIFFERENTIAL mode
because the DVM engine assumes expressions are deterministic. The same
expression can produce different values across refreshes, causing phantom
changes, missed changes, and broken row identity hashes.

**Decision needed:** How to handle volatile, stable, and immutable functions.

**Options:**
1. **Reject volatile functions in DIFFERENTIAL mode** (safest; clear error with
   suggestion to use FULL mode) — simplest, zero correctness risk
2. **Warn but allow** — user accepts phantom-change risk
3. **Snapshot volatile values at change-capture time** — store the computed value
   in the change buffer so it's stable across refreshes. Complex but correct.
4. **Auto-downgrade to FULL mode** when volatile functions detected
5. **Classify as stable-safe / volatile-unsafe** — allow `now()` (same within
   statement) but reject `random()`

**Recommendation:** Option 1 as default with Option 4 as a GUC-controlled
override. Adds `lookup_function_volatility()` using `pg_catalog.pg_proc` and
a recursive `Expr` tree scanner.

**Scope:**
- Volatility lookup infrastructure (SPI query to `pg_proc.provolatile`)
- Recursive expression scanner for `worst_volatility()` computation
- Integration into parser validation at `create_stream_table()` time
- GUC: `pg_trickle.volatile_function_policy` (reject/warn/allow)
- Handle overloaded functions (multiple `proname` entries with different
  volatility)

---

### ADR-051: GROUPING SETS / CUBE / ROLLUP Full Implementation

| Field | Value |
|-------|-------|
| **Status** | Not Started |
| **Category** | SQL Feature / Aggregation |
| **Sources** | `plans/sql/GAP_SQL_PHASE_4.md` (item S1) |
| **Effort** | High (10-15 hours) |

**Context:** Currently rejected with a clear error suggesting separate stream
tables + UNION ALL. GROUPING SETS produce multiple aggregation levels in a
single query — each grouping set is essentially a separate GROUP BY.

**Decision needed:** Whether and how to implement in DIFFERENTIAL mode.

**Options:**
1. **Keep rejection** — the UNION ALL rewrite is a viable workaround and avoids
   significant complexity
2. **Expand to multiple Aggregate operators** — one per grouping set, combined
   with UNION ALL internally. Each grouping set maps to a separate auxiliary
   counter set in storage.
3. **Rewrite to UNION ALL at parse time** — transparently decompose the query
   into multiple GROUP BY queries combined with UNION ALL before building the
   OpTree

**Recommendation:** Option 3 — query rewrite at parse time is cleanest and
reuses existing infrastructure. Option 1 is acceptable if demand is low.

---

### ADR-052: DISTINCT ON Full Implementation

| Field | Value |
|-------|-------|
| **Status** | Not Started |
| **Category** | SQL Feature / Deduplication |
| **Sources** | `plans/sql/GAP_SQL_PHASE_4.md` (item S2) |
| **Effort** | Medium (6-8 hours) |

**Context:** `DISTINCT ON (expr)` is a PostgreSQL-specific extension that
selects the first row per group (based on ORDER BY within the group). Currently
rejected with suggestion to use `ROW_NUMBER() OVER (...) = 1`.

**Decision needed:** Whether to implement natively or via automatic rewrite.

**Options:**
1. **Keep rejection** — the ROW_NUMBER() rewrite works and is portable SQL
2. **Auto-rewrite to window function** — at parse time, transparently convert
   `DISTINCT ON (expr) ORDER BY expr, col` to a subquery with
   `ROW_NUMBER() OVER (PARTITION BY expr ORDER BY col) = 1`
3. **Native DISTINCT ON operator** — new OpTree variant tracking per-group
   "first row" across refreshes

**Recommendation:** Option 2 — automatic rewrite to window function is cleanest,
reuses the existing Window operator, and requires minimal new code.

---

### ADR-053: Circular References in the Stream Table DAG

| Field | Value |
|-------|-------|
| **Status** | Not Started |
| **Category** | SQL Feature / DAG Architecture |
| **Sources** | `plans/sql/PLAN_CIRCULAR_REFERENCES.md` |
| **Effort** | Very High (~20-30 hours) |

**Context:** The dependency graph currently enforces a strict DAG. Creating a
stream table that would form a cycle is rejected. Some use cases naturally
involve mutual dependencies (e.g., ST A references ST B and vice versa).

**Decision needed:** Whether and how to support cycles in the ST dependency
graph.

**Options:**
1. **Keep DAG enforcement** — no cycles, users restructure their queries
2. **SCC-based fixed-point iteration** — decompose the graph into Strongly
   Connected Components (Tarjan's algorithm), create a condensation DAG, and
   iterate SCCs to fixed point
3. **Stratified evaluation** — partition cycles into monotone strata (safe to
   iterate) and non-monotone strata (rejected or user-opted-in with iteration
   limit)

**Recommendation:** Option 3 (stratified evaluation) — aligns with Datalog
theory and DBSP. Only monotone cycles (JOINs, UNIONs, filters) are
automatically iterable. Non-monotone cycles (aggregates, EXCEPT) warn and
require explicit user opt-in.

**Key design points from existing plan:**
- Replace `check_for_cycles()` with SCC decomposition
- Replace `topological_order()` with condensation-DAG ordering
- Add `max_iterations` GUC per SCC (default 100)
- Static monotonicity analysis at `create_stream_table()` time
- Convergence guarantee for monotone-only SCCs

---

### ADR-054: NATURAL JOIN Support

| Field | Value |
|-------|-------|
| **Status** | Not Started |
| **Category** | SQL Feature / Joins |
| **Sources** | `plans/sql/GAP_SQL_OVERVIEW.md` (Gap 2.3) |
| **Effort** | Medium (6-8 hours) |

**Context:** NATURAL JOIN is currently rejected with a clear error suggesting
explicit `JOIN ... ON`. PostgreSQL's raw parser does not resolve NATURAL JOIN
column lists — the `quals` field is NULL, and resolution happens during
analysis. The DVM parser would need catalog access to resolve common columns.

**Decision needed:** Whether to implement or continue rejecting.

**Options:**
1. **Keep rejection** — NATURAL JOIN is generally considered poor practice;
   explicit JOINs are clearer and less fragile to schema changes
2. **Catalog-resolved rewrite** — at parse time, query `pg_attribute` for both
   tables, find common column names, and synthesize an equi-join condition
3. **Query analysis pass** — use `pg_analyze_and_rewrite()` to get the resolved
   join quals, then extract the condition

**Recommendation:** Option 1 — rejection is appropriate. NATURAL JOIN is fragile
(adding a column to either table silently changes the join condition). The error
message already suggests the correct alternative.

---

### ADR-055: Remaining Aggregate Functions (Regression, Hypothetical-Set, XMLAGG)

| Field | Value |
|-------|-------|
| **Status** | Not Started |
| **Category** | SQL Feature / Aggregation |
| **Sources** | `plans/sql/GAP_SQL_PHASE_4.md` (items A3, E5, E6) |
| **Effort** | Low-Medium (6-10 hours total) |

**Context:** 17 aggregate functions are recognized but rejected in DIFFERENTIAL
mode. All follow the proven group-rescan pattern — implementation is
mechanical.

**Decision needed:** Priority and scope of remaining aggregate support.

**Aggregates to consider:**
- **Regression (11 functions):** CORR, COVAR_POP, COVAR_SAMP, REGR_AVGX,
  REGR_AVGY, REGR_COUNT, REGR_INTERCEPT, REGR_R2, REGR_SLOPE, REGR_SXX,
  REGR_SXY — all use group-rescan (~4-6 hours)
- **Hypothetical-set (4 functions):** RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST
  as aggregates — almost always used as window functions; rare as aggregates
  (~4-6 hours)
- **XML:** XMLAGG — very niche (~1-2 hours)

**Recommendation:** Implement regression aggregates on demand. Keep rejection
for hypothetical-set and XMLAGG — extremely rare use cases.

---

### ADR-056: Mixed UNION / UNION ALL Support

| Field | Value |
|-------|-------|
| **Status** | Not Started |
| **Category** | SQL Feature / Set Operations |
| **Sources** | `plans/sql/GAP_SQL_PHASE_4.md` (item S3) |
| **Effort** | Medium (4-6 hours) |

**Context:** Queries mixing `UNION` and `UNION ALL` in the same query are
currently rejected. The DVM parser handles sequences of the same set operation
but not mixed sequences.

**Decision needed:** How to handle mixed set operations.

**Options:**
1. **Keep rejection** — users rewrite to uniform set operations
2. **Per-arm dedup flag** — extend the OpTree set operation nodes with per-branch
   metadata indicating whether deduplication applies to each arm
3. **Parse-time rewrite** — decompose `A UNION B UNION ALL C` into
   `(A UNION B) UNION ALL C` by nesting set operation nodes

**Recommendation:** Option 3 — PostgreSQL's parser already produces a nested
tree structure for mixed set ops; the DVM parser should respect this nesting
rather than flattening.

---

### ADR-057: Multiple PARTITION BY Clauses in Window Functions

| Field | Value |
|-------|-------|
| **Status** | Not Started |
| **Category** | SQL Feature / Window Functions |
| **Sources** | `plans/sql/GAP_SQL_PHASE_4.md` (item S4) |
| **Effort** | High (8-10 hours) |

**Context:** Queries with window functions using different `PARTITION BY` clauses
are currently rejected in DIFFERENTIAL mode. The Window operator recomputes
entire partitions when any row in the partition changes. Multiple partitioning
schemes would require multiple recomputation passes.

**Decision needed:** How to handle queries with heterogeneous window partitions.

**Options:**
1. **Keep rejection** — users split into multiple stream tables
2. **Multi-pass recomputation** — for each distinct PARTITION BY, run a separate
   recomputation pass. The superset of affected partitions across all passes
   determines the final delta.
3. **Finest-grain partition** — find the coarsest common partition (intersection
   of all PARTITION BY keys) and recompute at that granularity
4. **Auto-rewrite to subqueries** — split each window function into a separate
   subquery with its own partitioning, then join the results

**Recommendation:** Option 2 — multi-pass is correct and bounded. Option 1 is
acceptable until demand is demonstrated.

---

### ADR-058: Subquery Expressions in Complex Positions

| Field | Value |
|-------|-------|
| **Status** | Not Started |
| **Category** | SQL Feature / Subqueries |
| **Sources** | `plans/sql/GAP_SQL_PHASE_4.md` (items E1, E2, E3) |
| **Effort** | High (18-24 hours total for all 3) |

**Context:** Three subquery patterns are currently rejected in DIFFERENTIAL mode:

1. **Scalar subquery in WHERE** — `WHERE col > (SELECT avg(x) FROM t)` — requires
   value-change tracking per row
2. **SubLinks inside OR** — `WHERE EXISTS(...) OR col = 1` — requires
   OR-to-UNION rewrite for delta correctness
3. **ALL (subquery)** — `WHERE col > ALL(SELECT x FROM t)` — dual of ANY;
   anti-join with universal quantification

**Decision needed:** Priority and approach for each.

**Options:**
1. **Keep rejection with rewrite suggestions** — all three have documented
   workarounds (JOINs, CTEs, NOT EXISTS)
2. **Implement incrementally** — E3 (ALL subquery) is simplest (anti-join
   pattern); E1 (scalar in WHERE) is hardest (needs value-change tracking);
   E2 (OR + SubLinks) is architecturally complex (OR-to-UNION rewrite)
3. **Auto-rewrite at parse time** — transform these patterns into supported
   equivalents before building the OpTree

**Recommendation:** Implement E3 (ALL subquery) as it follows the existing
AntiJoin pattern; defer E1 and E2 due to high complexity relative to benefit.

---

### ADR-059: ROWS FROM() with Multiple Set-Returning Functions

| Field | Value |
|-------|-------|
| **Status** | Not Started |
| **Category** | SQL Feature / LATERAL |
| **Sources** | `plans/sql/GAP_SQL_PHASE_4.md` (item S5) |
| **Effort** | Low (3-4 hours) |

**Context:** `ROWS FROM(func1(...), func2(...))` zips the output of multiple
set-returning functions into a single result set. Currently rejected.

**Decision needed:** Whether to implement.

**Recommendation:** Keep rejection — extremely rare construct. Single SRF in
FROM + LATERAL covers all practical use cases.

---

## Part 3: Forward-Looking ADRs — PostgreSQL Integration & Compatibility

### ADR-060: Citus Distributed Table Compatibility

| Field | Value |
|-------|-------|
| **Status** | Not Started |
| **Category** | PostgreSQL Integration |
| **Sources** | `plans/ecosystem/PLAN_CITUS.md` |
| **Effort** | Very High (~6 months) |

**Context:** pg_trickle has zero multi-node awareness. Every core module assumes
a single PostgreSQL instance with local OIDs, local WAL, local triggers, and a
single background worker. Citus compatibility requires addressing 6 major
incompatibilities.

**Decision needed:** Architecture for Citus support.

**Key incompatibilities:**
1. OID-based change buffer naming (OIDs not globally unique across nodes)
2. `pg_current_wal_lsn()` as change frontier (independent WAL per worker)
3. Triggers on distributed tables (DML goes to workers, bypassing coordinator
   triggers)
4. MERGE statement compatibility (limited Citus MERGE support)
5. Shared memory & background worker (coordinator-local)
6. System catalog & row estimates (coordinator shard is empty for distributed
   tables)

**Options:**
1. **Single-node only** — document incompatibility, no Citus support
2. **Reference tables only** — support Citus reference tables (triggers fire on
   coordinator) but not distributed tables
3. **Full Citus support** — 7-phase plan: stable naming, distributed sequence
   frontiers, worker-propagated triggers, INSERT ON CONFLICT instead of MERGE,
   coordinator-only scheduler, catalog-based locks, LISTEN/NOTIFY signaling

**Recommendation:** Option 2 as near-term (reference tables); Option 3 as a
long-term roadmap item. Proceed with runtime auto-detection of Citus
availability.

---

### ADR-061: Multi-Version PostgreSQL Support

| Field | Value |
|-------|-------|
| **Status** | Not Started |
| **Category** | PostgreSQL Integration |
| **Effort** | High (ongoing) |

**Context:** ADR-012 chose PostgreSQL 18 as the sole target. As PG 19 and future
versions release, a strategy for multi-version support is needed.

**Decision needed:** How to support new PG versions while maintaining backward
compatibility.

**Options:**
1. **Track latest only** — always target the newest PG version exclusively
2. **N-1 support** — support current and previous major version via conditional
   compilation (`#[cfg(feature = "pg18")]`)
3. **N-2 support** — broader compatibility at higher maintenance cost

**Recommendation:** Option 2 — support N and N-1 via pgrx's built-in
conditional compilation. Drop the oldest when a new PG version releases.

---

### ADR-062: Schema Evolution and DDL Propagation

| Field | Value |
|-------|-------|
| **Status** | Not Started |
| **Category** | PostgreSQL Integration |
| **Sources** | `src/hooks.rs`, `docs/ARCHITECTURE.md` |
| **Effort** | High (10-15 hours) |

**Context:** The current DDL tracking (`_on_ddl_end`, `_on_sql_drop`) detects
source table schema changes and marks affected stream tables for
reinitialization. This is a coarse approach — any column change triggers a full
reinitialization even if the changed column isn't used by the stream table.

**Decision needed:** How to handle schema evolution more gracefully.

**Options:**
1. **Keep full reinitialization** — correct but heavy-handed; any ALTER TABLE on
   a source table forces a full rebuild
2. **Column-level tracking** — only reinitialize if claimed columns are affected.
   The `columns_used` field in `pgt_dependencies` already tracks this; use it
   to filter DDL events.
3. **Transparent ALTER propagation** — when a source table gets a new column,
   automatically add it to the stream table if the defining query uses `SELECT *`
4. **Online schema migration** — apply schema changes to the storage table
   without full reinitialization using `ALTER TABLE ... ADD/DROP COLUMN`

**Recommendation:** Option 2 as near-term improvement; Option 3 for `SELECT *`
queries. Option 4 is complex and deferred.

---

### ADR-063: Extension Upgrade / Migration Strategy

| Field | Value |
|-------|-------|
| **Status** | Not Started |
| **Category** | PostgreSQL Integration |
| **Effort** | Medium (5-8 hours) |

**Context:** As the extension evolves, catalog schema changes, new operators,
and behavioral changes need a migration strategy. PostgreSQL supports
`ALTER EXTENSION ... UPDATE` with versioned migration SQL scripts.

**Decision needed:** Versioning and migration approach.

**Options:**
1. **pgrx-managed migrations** — rely on pgrx's SQL generation for each version
2. **Manual migration scripts** — hand-written `pg_trickle--1.0--1.1.sql` files
   with explicit `ALTER TABLE`, data migrations, etc.
3. **Hybrid** — pgrx for function signatures + manual scripts for catalog
   schema changes

**Recommendation:** Option 3 — pgrx handles function registration; manual
scripts handle catalog table changes, index additions, and data migrations.

---

## Part 4: Forward-Looking ADRs — Correctness & Safety

### ADR-070: TRUNCATE Capture in CDC

| Field | Value |
|-------|-------|
| **Status** | Not Started |
| **Category** | Correctness / CDC |
| **Effort** | Medium (4-6 hours) |

**Context:** `TRUNCATE` on a source table is not currently captured by the
row-level AFTER trigger (PostgreSQL does not fire row-level triggers on
TRUNCATE). If a source table is truncated, the stream table becomes stale
with no automatic mechanism to detect or recover.

**Decision needed:** How to detect and handle TRUNCATE on tracked tables.

**Options:**
1. **Event trigger on TRUNCATE** — use a DDL event trigger or statement-level
   trigger on TRUNCATE to detect the operation and mark affected stream tables
   for reinitialization
2. **TRUNCATE trigger** — PostgreSQL supports `BEFORE/AFTER TRUNCATE` triggers
   (statement-level only); fire a function that marks affected STs
3. **Row-count verification** — before each refresh, verify that source table
   row count hasn't unexpectedly dropped to 0
4. **Replication-based detection** — WAL-mode CDC naturally captures TRUNCATE
   via logical decoding messages

**Recommendation:** Option 2 — `AFTER TRUNCATE` trigger is the most direct and
reliable solution for trigger-mode CDC. Option 4 handles WAL mode automatically.

---

### ADR-071: Type Coercion and Implicit Cast Handling

| Field | Value |
|-------|-------|
| **Status** | Not Started |
| **Category** | Correctness / Expressions |
| **Effort** | Medium (4-6 hours) |

**Context:** PostgreSQL performs implicit type coercions in many contexts
(comparisons, function arguments, INSERT targets). The DVM parser handles
explicit `CAST(x AS type)` and `x::type` but may not preserve implicit coercions
that PostgreSQL's analyzer adds. This could lead to type mismatches in generated
delta SQL.

**Decision needed:** Whether to re-analyze delta SQL or preserve coercions
from the parse tree.

**Options:**
1. **Rely on PostgreSQL's implicit coercion** in generated SQL — trust that
   the database engine will apply the same coercions when executing delta SQL
2. **Explicit coercion insertion** — when generating delta SQL, add explicit
   casts where the source query has implicit coercions
3. **Use analyzed (post-rewrite) parse tree** — parse with `pg_analyze_and_rewrite()`
   instead of `raw_parser()` to get a fully resolved tree

**Recommendation:** Option 1 for now — PostgreSQL's implicit coercion in
generated delta SQL matches the defining query's behavior. Monitor for edge
cases and switch to Option 3 if type mismatches surface.

---

### ADR-072: Row Identity for Keyless Tables

| Field | Value |
|-------|-------|
| **Status** | Not Started |
| **Category** | Correctness / Storage |
| **Effort** | Medium (6-8 hours) |

**Context:** The current `__pgt_row_id` is computed from the primary key of
source tables. For defining queries that involve aggregations, expressions, or
joins, the row ID is derived from GROUP BY keys, join keys, or synthetic
identifiers. But what if a source table has no primary key?

**Decision needed:** How to identify rows when source tables lack primary keys.

**Options:**
1. **Require primary keys** — reject `create_stream_table()` if any source
   table lacks a PK. Simple but restrictive.
2. **Use `ctid`** — PostgreSQL's physical row ID. Not stable across VACUUM, but
   usable within a single refresh window.
3. **Use all columns** — hash all column values to generate a row ID. Works but
   may not be unique for duplicate rows.
4. **Require REPLICA IDENTITY FULL** — for WAL mode, this provides all column
   values in the change record. For trigger mode, the trigger already captures
   `to_jsonb(NEW)`.

**Recommendation:** Option 1 as default (require PK). Support Option 3 via
opt-in for tables where duplicates are acceptable or impossible.

---

### ADR-073: Consistent Snapshot Isolation for Multi-Source Refreshes

| Field | Value |
|-------|-------|
| **Status** | Not Started |
| **Category** | Correctness / Refresh Engine |
| **Effort** | High (8-12 hours) |

**Context:** When a stream table references multiple source tables, the
delta query reads changes from each source's buffer. These changes may
represent different transaction visibility windows. The current frontier
system uses LSN ranges per source, but concurrent transactions may cause
subtle inconsistencies if changes from one source are captured at a
different snapshot boundary than another.

**Decision needed:** Whether and how to enforce cross-source snapshot
consistency during refresh.

**Options:**
1. **Accept eventual consistency** — each source is independently tracked by
   LSN; minor transient inconsistencies self-correct on next refresh
2. **Transaction-ID-based windows** — use `xid` ranges instead of LSN ranges
   to ensure only committed transactions within the same window are processed
3. **Serializable refresh transactions** — run the delta query in a
   SERIALIZABLE transaction to enforce a consistent view

**Recommendation:** Option 1 — the current frontier approach provides
"eventual consistency within one refresh cycle" which is acceptable for the
DVS (Delayed View Semantics) guarantee.

---

## Priority Order

### Tier 1 — High Priority (write first)

| Priority | ADR | Rationale |
|----------|-----|-----------|
| 1 | ADR-003 | Core IVM engine — the heart of the extension |
| 2 | ADR-001 | Foundational CDC decision |
| 3 | ADR-002 | Hybrid CDC — major architectural evolution |
| 4 | ADR-010 | SQL functions vs DDL — shapes user experience |
| 5 | ADR-004 | xxHash row IDs — storage and correctness |
| 6 | ADR-005 | Change buffer design — CDC pipeline foundation |
| 7 | ADR-050 | Non-deterministic functions — open correctness gap |
| 8 | ADR-070 | TRUNCATE capture — open correctness gap |

### Tier 2 — Medium Priority

| Priority | ADR | Rationale |
|----------|-----|-----------|
| 9 | ADR-020 | Canonical scheduling — non-obvious design choice |
| 10 | ADR-023 | Adaptive fallback — performance characteristics |
| 11 | ADR-006 | User triggers — real-world usability |
| 12 | ADR-007 | Recursive CTE strategy — non-trivial IVM decision |
| 13 | ADR-008 | Group-rescan strategy — foundational aggregate pattern |
| 14 | ADR-040 | Aggregate counters — performance detail |
| 15 | ADR-053 | Circular references — major DAG architecture decision |
| 16 | ADR-062 | Schema evolution — operational concern |

### Tier 3 — Lower Priority

| Priority | ADR | Rationale |
|----------|-----|-----------|
| 17 | ADR-012 | PG 18 only — scoping decision |
| 18 | ADR-021 | Single scheduler — straightforward |
| 19 | ADR-022 | Replication origin — safety mechanism |
| 20 | ADR-030 | dbt macro — ecosystem decision |
| 21 | ADR-041 | LATERAL diff — specialized IVM detail |
| 22 | ADR-051 | GROUPING SETS — structural enhancement |
| 23 | ADR-052 | DISTINCT ON — auto-rewrite |
| 24 | ADR-060 | Citus — long-term infrastructure |

### Tier 4 — Document When Relevant

| Priority | ADR | Rationale |
|----------|-----|-----------|
| 25 | ADR-011 | Naming — historical |
| 26 | ADR-031 | In-repo dbt — minor |
| 27 | ADR-032 | Testcontainers — testing infra |
| 28 | ADR-055 | Remaining aggregates — incremental |
| 29 | ADR-056 | Mixed UNION — edge case |
| 30 | ADR-057 | Multiple PARTITION BY — edge case |
| 31 | ADR-058 | Complex subquery positions — edge case |
| 32 | ADR-059 | ROWS FROM — very niche |
| 33 | ADR-061 | Multi-PG-version — ongoing |
| 34 | ADR-063 | Extension upgrades — operational |
| 35 | ADR-071 | Type coercion — monitor for issues |
| 36 | ADR-072 | Keyless tables — restrictive edge |
| 37 | ADR-073 | Snapshot isolation — theoretical |
| 38 | ADR-054 | NATURAL JOIN — keep rejection |

---

## Effort Estimate

| Batch | ADRs | Scope | Estimated Effort |
|-------|------|-------|------------------|
| Batch 1 — Core (Accepted) | ADR-001 through 008, 010 | Document past decisions | ~5 hours |
| Batch 2 — Runtime (Accepted) | ADR-011, 012, 020-023 | Document past decisions | ~3 hours |
| Batch 3 — Ecosystem (Accepted) | ADR-030, 031, 032, 040, 041 | Document past decisions | ~2.5 hours |
| Batch 4 — SQL Features (New) | ADR-050 through 059 | Propose new decisions | ~4 hours |
| Batch 5 — PG Integration (New) | ADR-060 through 063 | Propose new decisions | ~2 hours |
| Batch 6 — Correctness (New) | ADR-070 through 073 | Propose new decisions | ~2 hours |
| **Total** | **38 ADRs** | 22 accepted + 16 forward-looking | **~18.5 hours** |

---

## File Naming Convention

```
plans/adrs/
├── PLAN_ADRS.md                                              ← this file
│
│ ── Core Architecture (001-009) ──
├── adr-001-trigger-based-cdc.md
├── adr-002-hybrid-cdc.md
├── adr-003-dvm-operator-tree.md
├── adr-004-xxhash-row-ids.md
├── adr-005-per-table-change-buffers.md
├── adr-006-explicit-dml-user-triggers.md
├── adr-007-semi-naive-recursive-cte.md
├── adr-008-group-rescan-aggregates.md
│
│ ── API & Schema Design (010-019) ──
├── adr-010-sql-functions-not-ddl.md
├── adr-011-pgtrickle-schema-naming.md
├── adr-012-postgresql-18-only.md
│
│ ── Scheduling & Runtime (020-029) ──
├── adr-020-canonical-scheduling-periods.md
├── adr-021-single-background-worker.md
├── adr-022-replication-origin-feedback-prevention.md
├── adr-023-adaptive-full-refresh-fallback.md
├── adr-024-dynamic-workers-pool-removal.md
│
│ ── Tooling & Ecosystem (030-039) ──
├── adr-030-dbt-macro-package.md
├── adr-031-dbt-in-repo-subdirectory.md
├── adr-032-testcontainers-testing.md
│
│ ── Performance & Optimization (040-049) ──
├── adr-040-aggregate-auxiliary-counters.md
├── adr-041-lateral-row-scoped-recomputation.md
│
│ ── SQL Feature Coverage (050-059) ──
├── adr-050-non-deterministic-function-handling.md
├── adr-051-grouping-sets-implementation.md
├── adr-052-distinct-on-rewrite.md
├── adr-053-circular-references-scc.md
├── adr-054-natural-join-rejection.md
├── adr-055-remaining-aggregates.md
├── adr-056-mixed-union-support.md
├── adr-057-multiple-partition-by.md
├── adr-058-complex-subquery-positions.md
├── adr-059-rows-from-multi-srf.md
│
│ ── PostgreSQL Integration (060-069) ──
├── adr-060-citus-compatibility.md
├── adr-061-multi-version-pg-support.md
├── adr-062-schema-evolution-ddl.md
├── adr-063-extension-upgrade-migration.md
│
│ ── Correctness & Safety (070-079) ──
├── adr-070-truncate-capture.md
├── adr-071-type-coercion-handling.md
├── adr-072-keyless-table-row-identity.md
└── adr-073-snapshot-isolation-multi-source.md
```
