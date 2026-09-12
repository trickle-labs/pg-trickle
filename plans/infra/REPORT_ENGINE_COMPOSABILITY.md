# Engine Composability Analysis — Extractable Components & Internal Decomposition

**Date:** 2026-03-03
**Status:** Analysis / Proposal
**Type:** REPORT

---

## Executive Summary

pg_trickle is a ~48K-line Rust codebase compiled as a single PostgreSQL extension
(`cdylib`). Analysis of the architecture, source coupling, and planned features
reveals **five components that could be extracted as standalone projects** and
**three internal decomposition opportunities** that would make the extension itself
more composable. The existing [PLAN_ECO_SYSTEM.md](ecosystem/PLAN_ECO_SYSTEM.md)
already covers *integrations* (dbt, Airflow, Grafana, CLI); this analysis focuses
on **core engine decomposition** — breaking the monolith into reusable building blocks.

### Why This Matters

1. **Managed PG services** (RDS, Cloud SQL, Neon, Supabase) cannot install C
   extensions. An external sidecar needs the DVM engine *without* pgrx.
2. **Broader adoption** — a standalone SQL-differencing library is useful far
   beyond pg_trickle (migration tools, query planners, testing frameworks).
3. **Independent release cadences** — the DAG engine and DVM engine change at
   different rates than the CDC layer.
4. **Testing & correctness** — pure-Rust crates can be fuzzed and
   property-tested without a PostgreSQL backend.

---

## Part 1: Extractable Components (Separate Crates / Projects)

### 1.1. `pg-query-diff` — SQL Delta Query Generator (DVM Engine)

**What:** The DVM engine (`src/dvm/`) minus the PostgreSQL parser coupling.
A library crate that takes an operator tree (OpTree) and produces delta SQL.

**Current size:** ~25K lines across `dvm/mod.rs`, `dvm/diff.rs`, `dvm/row_id.rs`,
`dvm/parser.rs` (15K lines), and `dvm/operators/` (21 operator files).

**Coupling analysis:**

| Submodule | pgrx/pg_sys refs | Status |
|-----------|-----------------|--------|
| `dvm/operators/*.rs` (20 files) | 2 (only in `recursive_cte.rs`: 1 `pgrx::info!`, 1 `Spi`) | **Nearly pure Rust** |
| `dvm/diff.rs` | 0 | **Pure Rust** |
| `dvm/row_id.rs` | 0 | **Pure Rust** |
| `dvm/mod.rs` | 4 (cache management, SPI for volatility check) | Low coupling |
| `dvm/parser.rs` | ~15 `Spi::connect` + extensive `pg_sys::raw_parser` usage | **Deeply coupled** |

**Extraction strategy:**

The operators + diff engine are already nearly decoupled. The blocker is
`parser.rs` (15K lines), which uses `pg_sys::raw_parser()` to walk PG's
internal C parse tree. Two-phase approach:

1. **Phase 1 — Define a `ParseFrontend` trait** that abstracts the parsing
   interface. The current `pg_sys::raw_parser` implementation becomes one
   backend; a `pg_query.rs` (libpg_query) implementation becomes another.
   This is already identified in [REPORT_EXTERNAL_PROCESS.md](infra/REPORT_EXTERNAL_PROCESS.md)
   §3.1 Strategy A.

2. **Phase 2 — Extract `pg-query-diff` crate** containing:
   - `OpTree` and all operator types
   - `DiffContext` and delta SQL generation
   - row ID schema inference and generation
   - `CteRegistry` and CTE handling
   - The `ParseFrontend` trait (but not the pgrx implementation)
   - All auto-rewrite passes (they operate on SQL strings, not parse trees)

**Value as a standalone project:**
- **Migration tools** could use it to generate incremental SQL from view definitions
- **Testing frameworks** could diff query outputs
- **Other IVM systems** (non-PG) could reuse the operator algebra
- **The sidecar architecture** (REPORT_EXTERNAL_PROCESS.md) becomes viable
  without forking the entire extension

**Effort:** ~40–60 hours (trait abstraction + crate extraction + CI)

---

### 1.2. `pg-dag` — Dependency Graph Engine

**What:** The DAG module (`src/dag.rs`, ~1960 lines) as a standalone crate for
managing dependency graphs with topological ordering, cycle detection, diamond
detection, consistency groups, and schedule propagation.

**Coupling analysis:**

| Concern | pgrx dependency | Extractable? |
|---------|----------------|--------------|
| Graph algorithms (topological sort, cycle detection, diamond detection) | **None** — pure `HashMap`/`HashSet` | ✅ Immediately |
| Consistency group computation | **None** | ✅ Immediately |
| Schedule propagation + canonical periods | **None** | ✅ Immediately |
| `StDag::build_from_catalog()` | `Spi::connect()` to read `pgt_stream_tables` + `pgt_dependencies` | Stays in extension |

The `build_from_catalog()` function (~80 lines) is the only SPI-coupled code.
Everything else is pure Rust graph logic that takes a `HashMap<NodeId, StNode>`
as input.

**Extraction strategy:**
1. Split `dag.rs` into:
   - `pg-dag` crate: `StDag`, `StNode`, `NodeId`, `DiamondConsistency`,
     `DiamondSchedulePolicy`, `ConsistencyGroup`, topological sort, cycle
     detection, diamond detection, schedule propagation
   - In the extension: `build_from_catalog()` adapter that reads SPI and
     feeds data into the crate's constructors

**Value as a standalone project:**
- **Any system with a DAG of dependent tasks** (CI pipelines, build systems,
  data pipeline orchestrators) could reuse the diamond detection + consistency
  group logic
- The canonical-period scheduling algorithm is novel and useful independently
- Fuzzable and property-testable without PG

**Effort:** ~8–12 hours

---

### 1.3. `pg-cdc-triggers` — Trigger-Based CDC Library

**What:** The CDC trigger management code (`src/cdc.rs`, ~1004 lines) as a
reusable library for creating, managing, and consuming row-level change
capture triggers on PostgreSQL tables.

**Coupling analysis:**

The CDC module generates and executes SQL DDL (CREATE TRIGGER, CREATE FUNCTION,
DROP TRIGGER) via SPI. The *logic* of what SQL to generate is decoupled from
SPI — it builds SQL strings and then executes them.

**Extraction strategy:**
1. Extract a `pg-cdc-triggers` crate that provides:
   - Trigger function SQL generation (given a table OID/name, generate the
     PL/pgSQL trigger function and CREATE TRIGGER DDL)
   - Change buffer table DDL generation
   - Change consumption query generation (given a frontier range)
   - Change buffer cleanup query generation
2. The extension calls the library for SQL generation, then executes via SPI
3. An external sidecar could call the library, then execute via `tokio-postgres`

**Value as a standalone project:**
- **Any system needing PG trigger-based CDC** (audit logging, event sourcing,
  cache invalidation) could use the library
- Pairs naturally with `pg-dag` for building custom incremental pipelines
- The hybrid trigger→WAL transition logic is particularly novel

**Effort:** ~16–24 hours

---

### 1.4. `pg-trickle-sidecar` — External Process / Sidecar

**What:** The orchestration layer (scheduler + refresh + CDC + DVM) running as
an external process connecting to PostgreSQL over standard libpq/tokio-postgres
connections. This is the main deliverable of the external process architecture
described in [REPORT_EXTERNAL_PROCESS.md](infra/REPORT_EXTERNAL_PROCESS.md).

**Dependency on other extractions:**
- Requires `pg-query-diff` (1.1) for delta SQL generation
- Requires `pg-dag` (1.2) for scheduling
- Requires `pg-cdc-triggers` (1.3) for CDC management
- Uses `pg_query.rs` (libpg_query) instead of `pg_sys::raw_parser`
- Replaces SPI with `tokio-postgres` or `sqlx`
- Replaces GUCs with a config file (TOML/YAML)
- Replaces `BackgroundWorker` with a Tokio runtime
- Replaces shared memory with internal state (single process)

**Value as a standalone project:**
- **Unlocks managed PG services** (RDS, Cloud SQL, Neon, Supabase)
- Can run as a Kubernetes sidecar, Docker compose service, or systemd unit
- Independent scaling (run multiple scheduler instances for different DB
  subsets)

**Effort:** ~120–160 hours (the REPORT_EXTERNAL_PROCESS.md estimates 200–300h
for a full port, but with pre-extracted crates this is significantly reduced)

**Sequencing:** This is the *end goal* — extracting 1.1–1.3 first makes this
achievable incrementally rather than as a big-bang rewrite.

---

### 1.5. `pg-trickle-cli` — Command-Line Management Tool

Already planned in [PLAN_ECO_SYSTEM.md](ecosystem/PLAN_ECO_SYSTEM.md) Project 7.
Including here for completeness — it benefits from `pg-dag` (1.2) for local
DAG visualization and from the config crate for shared configuration schemas.

**Effort:** ~20 hours (already estimated)

---

## Part 2: Internal Decomposition (Same Crate, Better Boundaries)

These changes keep everything in a single `cdylib` but introduce clearer
internal boundaries via traits and module reorganization. They reduce coupling,
improve testability, and prepare the ground for future crate extraction.

### 2.1. Storage Backend Trait

**Current state:** `refresh.rs` (~2355 lines) directly generates SQL for
`TRUNCATE`, `INSERT`, `DELETE`, `MERGE` and executes via SPI. The refresh
logic (determine action, compute delta, apply, update frontier) is tightly
woven with the SQL execution.

**Proposal:** Introduce a `StorageBackend` trait:

```rust
pub trait StorageBackend {
    fn execute_full_refresh(&self, st: &StreamTableMeta, query: &str) -> Result<RefreshStats>;
    fn execute_delta(&self, st: &StreamTableMeta, delta_sql: &str) -> Result<RefreshStats>;
    fn advance_frontier(&self, st: &StreamTableMeta, new_frontier: &Frontier) -> Result<()>;
    fn record_history(&self, record: &RefreshRecord) -> Result<()>;
}
```

- `SpiStorageBackend` — current implementation (in-extension)
- `SqlxStorageBackend` — future sidecar implementation (tokio-postgres)
- `MockStorageBackend` — for unit-testing refresh orchestration logic

**Benefit:** The refresh orchestration (which action to take, adaptive
fallback, retry logic) can be unit-tested without a PG backend.

**Effort:** ~8–12 hours

---

### 2.2. Parse Frontend Trait (Internal Step Toward 1.1)

**Current state:** `parser.rs` calls `pg_sys::raw_parser()` directly ~150
times throughout 15K lines. Pure logic (AST walking, OpTree construction,
rewrite passes) is interleaved with FFI calls.

**Proposal:** Introduce a `ParseFrontend` trait:

```rust
pub trait ParseFrontend {
    /// Parse SQL and return an abstract parse tree.
    fn parse(&self, sql: &str) -> Result<ParseTree>;
    /// Look up function volatility.
    fn function_volatility(&self, schema: &str, name: &str) -> Result<Volatility>;
    /// Resolve a table's OID and column list.
    fn resolve_table(&self, schema: &str, name: &str) -> Result<TableInfo>;
    /// Get view definition text.
    fn get_view_definition(&self, schema: &str, name: &str) -> Result<Option<String>>;
}
```

With a `PgrxParseFrontend` (using `pg_sys::raw_parser` + SPI) and a
future `PgQueryParseFrontend` (using `pg_query.rs` + client connection).

This is the **highest-leverage internal change** — it decouples the entire
DVM engine from the PostgreSQL backend in a single abstraction boundary.

**Effort:** ~24–40 hours (large due to 15K lines of parser code)

---

### 2.3. Catalog Access Trait

**Current state:** `catalog.rs` (~1150 lines) does all CRUD via `Spi::connect()`.
Other modules (`dag.rs`, `cdc.rs`, `monitor.rs`, `hooks.rs`, `scheduler.rs`)
also call SPI directly for catalog reads.

**Proposal:** Introduce a `CatalogAccess` trait:

```rust
pub trait CatalogAccess {
    fn get_stream_table(&self, name: &str) -> Result<Option<StreamTableMeta>>;
    fn list_stream_tables(&self) -> Result<Vec<StreamTableMeta>>;
    fn get_dependencies(&self, pgt_id: i64) -> Result<Vec<Dependency>>;
    fn update_status(&self, pgt_id: i64, status: StStatus) -> Result<()>;
    fn record_refresh(&self, record: RefreshRecord) -> Result<()>;
    // ... etc
}
```

- `SpiCatalogAccess` — current SPI-based implementation
- `SqlxCatalogAccess` — future sidecar implementation
- `InMemoryCatalogAccess` — for testing

**Benefit:** Tests can inject mock catalogs. All SPI access is centralized
behind a single trait rather than scattered across 6+ modules.

**Effort:** ~12–16 hours

---

## Part 3: Composability Matrix

How the extracted components and internal traits relate:

```
                    ┌──────────────────────────────────────────────┐
                    │           pg_trickle extension               │
                    │                                              │
                    │  ┌──────────┐  ┌────────────┐  ┌─────────┐  │
                    │  │SpiParse  │  │SpiCatalog  │  │SpiStore │  │
                    │  │Frontend  │  │Access      │  │Backend  │  │
                    │  └────┬─────┘  └─────┬──────┘  └────┬────┘  │
                    │       │              │              │        │
                    └───────┼──────────────┼──────────────┼────────┘
                            │              │              │
              Trait boundaries (internal)  │              │
                            │              │              │
    ┌───────────────────────┼──────────────┼──────────────┼───────┐
    │                       ▼              ▼              ▼       │
    │  ┌─────────────────────────┐  ┌────────────┐  ┌──────────┐ │
    │  │  pg-query-diff (1.1)    │  │ pg-dag     │  │pg-cdc-   │ │
    │  │  OpTree, Operators,     │  │ (1.2)      │  │triggers  │ │
    │  │  DiffContext, RowId,    │  │ Topo sort, │  │(1.3)     │ │
    │  │  Rewrite passes         │  │ Diamonds,  │  │Trigger   │ │
    │  │                         │  │ Scheduling │  │SQL gen   │ │
    │  │  ParseFrontend trait    │  │            │  │          │ │
    │  └─────────────────────────┘  └────────────┘  └──────────┘ │
    │                                                             │
    │              Extractable crates (pure Rust)                 │
    └─────────────────────────────────────────────────────────────┘
                            │              │              │
                            ▼              ▼              ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                  pg-trickle-sidecar (1.4)                   │
    │                                                             │
    │  PgQueryParseFrontend + SqlxCatalogAccess + SqlxStorage     │
    │  + Tokio scheduler + TOML config                            │
    └─────────────────────────────────────────────────────────────┘
```

---

## Part 4: Priority Ranking

Ranked by **impact / effort ratio** and **unblocking effect**:

| Priority | Component | Type | Effort | Impact | Unblocks |
|----------|-----------|------|--------|--------|----------|
| **P1** | 1.2 `pg-dag` | Extract | 8–12h | Medium | Sidecar, CLI, testing |
| **P2** | 2.1 StorageBackend trait | Internal | 8–12h | High | Unit-testing refresh logic |
| **P3** | 2.3 CatalogAccess trait | Internal | 12–16h | High | Centralized SPI, testing |
| **P4** | 2.2 ParseFrontend trait | Internal | 24–40h | Very High | DVM extraction, sidecar |
| **P5** | 1.1 `pg-query-diff` | Extract | 40–60h | Very High | Sidecar, broader ecosystem |
| **P6** | 1.3 `pg-cdc-triggers` | Extract | 16–24h | Medium | Standalone CDC users |
| **P7** | 1.4 `pg-trickle-sidecar` | New project | 120–160h | Very High | Managed PG services |

**Recommended sequencing:**

```
Phase A (internal traits):     P1 → P2 → P3    (~28–40h)
Phase B (parse abstraction):   P4              (~24–40h)
Phase C (crate extraction):    P5 → P6         (~56–84h)
Phase D (sidecar):             P7              (~120–160h)
```

Phase A can happen alongside v0.2.0 work. Phase B should target v0.3.0.
Phases C and D are post-1.0 but the internal trait work (A+B) makes them
achievable without a big-bang rewrite.

---

## Part 5: What Should NOT Be Extracted

Some components are deeply tied to the PostgreSQL extension runtime and
extracting them would be counterproductive:

| Component | Why Keep In-Extension |
|-----------|----------------------|
| `hooks.rs` (DDL event triggers) | Requires PG event trigger infrastructure; no equivalent externally |
| `shmem.rs` (shared memory) | PG `PgLwLock`/`PgAtomic` — replaced entirely in sidecar mode |
| `config.rs` (GUCs) | PG GUC registry — replaced by config file in sidecar |
| `hash.rs` (SQL hash functions) | `#[pg_extern]` wrapper around xxHash; the Rust xxHash call is one line |
| `scheduler.rs` (bgworker) | PG `BackgroundWorkerBuilder` — replaced by Tokio in sidecar; but the *scheduling algorithm* (canonical periods, retry) should be extracted |
| `wal_decoder.rs` | Deeply tied to PG logical replication slot API; sidecar would use `tokio-postgres` replication protocol directly |

---

## Part 6: Comparison With Existing Ecosystem Plan

[PLAN_ECO_SYSTEM.md](ecosystem/PLAN_ECO_SYSTEM.md) defines 11 ecosystem projects focused on
**integrations** (dbt, Airflow, Prometheus, CLI, Docker, ORM). This analysis
focuses on **engine decomposition** — they are complementary:

| Ecosystem Plan | This Analysis |
|----------------|---------------|
| Integration wrappers around SQL API | Core engine as composable crates |
| All projects consume pg_trickle as a black box | Projects *are* pg_trickle's internals |
| Separate repos, same SQL interface | Workspace crates, trait-based interfaces |
| Useful today (0.x) | Phase A today, Phases B–D post-1.0 |

The sidecar (1.4) is the bridge: it is both an ecosystem project (separate
binary) and a consumer of the extracted core crates.

---

## Part 7: Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| **Premature extraction** — extracting crates before the API stabilizes causes churn | Start with *internal* traits (Phase A+B) that keep everything in one repo but establish boundaries. Extract to crates only when the trait interfaces are stable. |
| **Build complexity** — workspace with multiple crates is harder to build | Use Cargo workspace; pgrx already supports workspace members. Keep the crate graph shallow (max 2 levels). |
| **Performance regression from trait dynamism** — `dyn Trait` dispatch overhead | Use generics (`impl Trait`) not `dyn Trait` — monomorphized at compile time, zero runtime cost. |
| **Testing burden** — more crates = more CI matrix entries | Extracted crates are pure Rust — `cargo test` without Docker. Actually *reduces* CI cost. |
| **Sidecar feature parity** — sidecar lags extension | Share the same crate for core logic; only the "glue" differs. Feature parity is structural, not manual. |

---

## References

| Document | Relevance |
|----------|-----------|
| [REPORT_EXTERNAL_PROCESS.md](infra/REPORT_EXTERNAL_PROCESS.md) | Full sidecar feasibility study; coupling inventory |
| [REPORT_PGWIRE_PROXY.md](infra/REPORT_PGWIRE_PROXY.md) | Proxy architecture (alternative to sidecar) |
| [PLAN_ECO_SYSTEM.md](ecosystem/PLAN_ECO_SYSTEM.md) | Integration ecosystem plan (complementary) |
| [PLAN_ADRS.md](adrs/PLAN_ADRS.md) | Architecture decisions (constraints) |
| [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) | Current architecture |
| [ROADMAP.md](../ROADMAP.md) | Release timeline for sequencing |
| [REPORT_DOWNSTREAM_CONSUMERS.md](infra/REPORT_DOWNSTREAM_CONSUMERS.md) | Consumer patterns that benefit from composability |
