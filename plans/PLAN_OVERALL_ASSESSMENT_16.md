# pg_trickle Overall Assessment 16 — Vision Audit

> **Date:** 2026-05-30  
> **Scope:** Full architectural and code audit against the "Best IVM System Ever Built" vision  
> **Baseline:** v0.80.0 (final pre-v1.0 hardening arc complete)  
> **Auditor perspective:** Principal Database Engineer / Distributed Systems Architect

---

## 1. Executive Summary

pg_trickle at v0.80.0 is an **exceptional single-node IVM engine**. The
differential dataflow implementation is mathematically grounded (DBSP Z-set
semantics), the operator coverage is comprehensive (22 operator types, TPC-H
22/22 at scale), and the engineering quality is production-grade (660+ tests
across 6 tiers, fuzz targets, property tests, correctness proofs for join
phantom rows). The system is the most capable open-source PostgreSQL IVM
extension available today.

**However, the system has three structural ceilings that prevent it from
achieving the "best IVM system ever built" vision:**

1. **Single-process architecture ceiling:** All computation happens inside a
   single PostgreSQL backend process. The background worker model (launcher +
   per-DB scheduler + 4 refresh workers) cannot scale beyond the resources of
   one PostgreSQL server. There is no mechanism for distributing delta
   computation across multiple nodes, no external state store, and no
   decoupled compute layer.

2. **Synchronous write-path coupling:** Trigger-based CDC adds 2–15μs per row
   to the write path of every source table. While WAL-based CDC is available
   as an alternative, the transition is manual and the WAL decoder still runs
   inside the same PostgreSQL server. There is no true out-of-process CDC
   consumer that would eliminate write-path impact entirely.

3. **Operational complexity at scale:** The 130+ GUCs, while individually
   well-documented, create a combinatorial configuration space that is hostile
   to operators. The system lacks adaptive self-tuning (e.g., automatic worker
   pool sizing based on CPU saturation, automatic memory limit adjustment
   based on available RAM, automatic schedule frequency based on observed
   latency).

**Current state:** World-class single-node IVM. **Gap to vision:** Distributed
compute, zero-impact CDC, and autonomous self-tuning.

### Severity Summary

| Severity | Count | Category |
|----------|-------|----------|
| **CRITICAL** | 0 | — |
| **HIGH** | 7 | Architecture (3), Performance (2), Scalability (2) |
| **MEDIUM** | 18 | Performance (5), Ergonomics (4), Observability (3), Code Quality (4), Testing (2) |
| **LOW** | 12 | Documentation (3), API (4), DevEx (3), Dependencies (2) |

---

## 2. Deep-Dive Pillar Analysis

### Pillar 1: Architecture, State Management & Scale

#### Strengths

- **Clean module boundaries:** The separation of CDC → DVM → Refresh → Scheduler
  is architecturally sound. Each module has a clear contract (change buffers as
  the integration surface between CDC and DVM).
- **Copy-on-write DAG rebuild (SCAL-4):** The scheduler never holds locks during
  catalog loads, enabling concurrent DDL without blocking refresh cycles.
- **Split shared memory (SCAL-3):** DAG state, scheduler metadata, and tick
  watermarks use separate `PgLwLock` instances to minimize contention.
- **Invalidation ring buffer:** DDL events are communicated via a lock-free-ish
  ring buffer with overflow detection (falls back to full DAG rebuild).
- **Fused CTE refresh (v0.63.0):** Multi-node DAG chains execute as a single SQL
  statement, reducing per-tick overhead dramatically for deep DAGs.

#### Gaps / Critical Flaws

| ID | Severity | Finding |
|----|----------|---------|
| ARCH-001 | HIGH | **No external compute layer.** All delta computation runs inside PostgreSQL SPI. A complex 10-join delta query consumes PostgreSQL backend memory and CPU, competing with user OLTP queries. There is no mechanism to offload DVM computation to an external process, sidecar, or distributed cluster. |
| ARCH-002 | HIGH | **Change buffers as PostgreSQL tables create I/O amplification.** Every source write produces a trigger INSERT into `pgtrickle_changes.changes_<oid>`. At high throughput (>100K writes/s), this doubles the WAL volume and requires the cleanup loop to DELETE consumed rows — creating further WAL and vacuum pressure. |
| ARCH-003 | HIGH | **No state externalization.** The entire system state (catalog, change buffers, template cache, refresh history) lives in PostgreSQL tables. This prevents: (a) separating compute from storage, (b) running DVM workers on read replicas, (c) checkpointing state to object storage for disaster recovery independent of pg_basebackup. |
| ARCH-004 | MEDIUM | **Background worker pool is statically sized.** `max_dynamic_refresh_workers=4` is a compile-time-ish constant (GUC, but requires restart-level change to PostgreSQL's `max_worker_processes`). The system cannot dynamically scale workers based on load. |
| ARCH-005 | MEDIUM | **No multi-database coordinator.** Each database has an independent scheduler with no cross-database awareness. In a multi-tenant PostgreSQL cluster, there is no global priority queue or resource governor. |

#### Opportunities

- **ARCH-O1: External DVM Worker Process.** Extract the DVM engine into a
  standalone Rust binary that connects to PostgreSQL via `tokio-postgres`,
  reads change buffers, computes deltas externally, and writes results back.
  This decouples compute from the PostgreSQL process and enables horizontal
  scaling via Kubernetes pod autoscaling.

- **ARCH-O2: WAL-native CDC with pgoutput consumer.** Implement a logical
  replication subscriber as an external process (similar to Debezium) that
  writes to an append-only log (e.g., local WAL files, or Kafka/NATS for
  distributed mode). This eliminates trigger overhead entirely and enables
  CDC consumption from read replicas.

- **ARCH-O3: Tiered state storage.** Hot state (current frontier, active
  template cache) in shared memory. Warm state (recent change buffers) in
  UNLOGGED tables or memory-mapped files. Cold state (refresh history,
  completed deltas) in object storage (S3/GCS) for cost-efficient retention.

- **ARCH-O4: Kubernetes-native operator.** A CRD (`StreamTable`) that the
  pg_trickle operator translates into PostgreSQL DDL + external worker
  deployment. Pod autoscaling based on CDC lag metrics.

---

### Pillar 2: High-Throughput & Low-Latency Mechanics

#### Strengths

- **O(1) placeholder resolution (P-1):** Aho-Corasick automaton replaces LSN
  tokens in a single pass regardless of source count. The automaton is cached
  per session with collision detection.
- **Thread-local template cache (L0/L1):** Delta SQL generation is ~0ns on
  cache hit. The L2 catalog cache (pgt_template_cache) survives session
  restarts.
- **Cost model with precomputed summary (P-3):** Batch update per scheduler
  tick eliminates N per-ST subquery scans on pgt_refresh_history.
- **Predicate pushdown (P2-7):** WHERE predicates pushed into change-buffer
  scans reduce pipeline input volume.
- **Algebraic aggregates:** SUM/COUNT/AVG/STDDEV use invertible O(1) update
  formulas instead of recomputing affected groups.
- **Statement-level CDC triggers (v0.4.0):** 50–80% overhead reduction vs
  row-level triggers by using transition tables.

#### Gaps / Critical Flaws

| ID | Severity | Finding |
|----|----------|---------|
| PERF-001 | HIGH | **No vectorized batch processing.** Delta queries return individual rows processed one-at-a-time through SPI. The MERGE executor processes rows sequentially. There is no SIMD-friendly columnar batch path for aggregation or join probe. At >1M deltas/refresh, the per-row SPI overhead dominates. |
| PERF-002 | HIGH | **SPI overhead per refresh cycle.** Each differential refresh involves: (1) frontier read, (2) change-count probe, (3) delta SQL execution, (4) MERGE execution, (5) frontier advance, (6) history INSERT, (7) cleanup DELETE. That's 7+ SPI round-trips minimum per stream table per tick. At 1000 STs, this is 7000 SPI calls per scheduler tick. |
| PERF-003 | MEDIUM | **Full refresh TRUNCATE+INSERT is not crash-safe without outer transaction.** If the backend crashes between TRUNCATE and INSERT completion, the stream table is empty. While PostgreSQL's transaction semantics protect against this at the SQL level, the background worker crash recovery path (recover_from_crash) must handle this edge case. |
| PERF-004 | MEDIUM | **No connection pooling for refresh workers.** Each dynamic refresh worker opens a fresh SPI connection. For PgBouncer deployments, this means each worker consumes a real backend slot. There is no internal connection multiplexing. |
| PERF-005 | MEDIUM | **Change buffer cleanup is synchronous with refresh.** The DELETE of consumed rows happens in the same transaction as the refresh. At high throughput, this DELETE competes with incoming INSERTs for the same table's lock. |
| PERF-006 | MEDIUM | **No incremental MERGE for large deltas.** The MERGE statement processes the entire delta set in one execution. For deltas >100K rows, this creates memory pressure and long-running transactions that delay other refreshes. Chunked/batched MERGE would reduce peak memory and lock hold time. |
| PERF-007 | MEDIUM | **HashMap-based template cache lacks size bounding on L0/L1.** While L2 has a byte-size cap (PERF-006 v0.73.0), the thread-local L0/L1 caches grow unbounded in long-lived sessions with many STs. In a 10K-ST deployment, this could consume significant per-backend memory. |

#### Opportunities

- **PERF-O1: Columnar delta batching.** Process deltas in Arrow-compatible
  columnar batches (e.g., 1024-row pages). This enables SIMD aggregation,
  vectorized hash joins, and bulk COPY-based MERGE instead of row-at-a-time
  SPI.

- **PERF-O2: Pipelined SPI.** Use PostgreSQL's extended query protocol
  (prepared statements with portal-based fetching) to overlap delta SQL
  execution with MERGE application. The delta results stream directly into
  the MERGE without materializing the entire result set in memory.

- **PERF-O3: Lock-free change buffer ring.** Replace the `changes_<oid>`
  table with a shared-memory ring buffer for hot sources. Consumers read
  without locks; producers write without waiting for cleanup. Overflow spills
  to the table-based path.

- **PERF-O4: Parallel delta computation within a single ST.** For joins with
  multiple sources, compute each source's delta contribution in parallel
  (fan-out) and merge results (fan-in). This leverages PostgreSQL's parallel
  query infrastructure.

- **PERF-O5: Zero-copy frontier advancement.** Instead of SPI UPDATE per
  frontier advance, use a shared-memory frontier array indexed by pgt_id.
  Persist to disk only on checkpoint boundaries.

---

### Pillar 3: Ergonomics & Operator Experience

#### Strengths

- **Single SQL command to create:** `SELECT pgtrickle.create_stream_table(...)` 
  with sensible defaults (AUTO mode, 60s schedule, differential when possible).
- **Progressive disclosure:** Basic usage requires only `name` + `query`. Advanced
  users can specify `schedule`, `refresh_mode`, `storage_parameters`.
- **explain_stream_table():** Returns full OpTree, strategy details, and DVM
  support assessment — excellent for debugging.
- **health_check():** Single function returns cluster-wide health with actionable
  alerts.
- **DVM_SUPPORT_MATRIX.md:** Documents every query pattern and its support level.

#### Gaps / Critical Flaws

| ID | Severity | Finding |
|----|----------|---------|
| ERG-001 | MEDIUM | **No declarative DDL syntax.** Users must call `pgtrickle.create_stream_table(name, query, ...)` function. There is no `CREATE STREAM TABLE name AS SELECT ...` syntax that feels native to PostgreSQL. This makes the extension feel like an API library rather than a first-class database feature. |
| ERG-002 | MEDIUM | **Schema evolution requires manual intervention.** When a source table adds a column, the stream table's defining query still references the old schema. The user must `ALTER STREAM TABLE ... SET QUERY ...` manually. There is no automatic propagation of additive schema changes. |
| ERG-003 | MEDIUM | **No dry-run / preview mode.** Users cannot see what a `create_stream_table` would do (which sources detected, what CDC mode, what refresh strategy) without actually creating it. A `preview_stream_table()` function would reduce trial-and-error. |
| ERG-004 | MEDIUM | **130+ GUCs with no auto-tuning.** The configuration surface is expert-level. There is no `pg_trickle.tune()` function that analyzes the workload and suggests/applies optimal settings. New users are overwhelmed by the number of knobs. |

#### Opportunities

- **ERG-O1: Native SQL syntax via event trigger.** Intercept `CREATE MATERIALIZED VIEW ... WITH (stream_table=true)` or introduce a custom utility command via the ProcessUtility_hook. This gives users familiar SQL syntax while the extension handles the mechanics.

- **ERG-O2: Auto-schema-evolution mode.** When `pg_trickle.auto_evolve=true`,
  DDL event triggers detect `ALTER TABLE ... ADD COLUMN` on source tables and
  automatically propagate additive changes to downstream stream tables (rewrite
  query to include new column, ALTER the stream table, reinitialize).

- **ERG-O3: Configuration advisor.** `SELECT * FROM pgtrickle.tune_recommendations()`
  returns a table of (guc_name, current_value, recommended_value, reason) based
  on observed workload patterns (refresh latency percentiles, memory usage,
  worker utilization).

- **ERG-O4: Stream table templates / presets.** Named configuration profiles
  (`'real-time'`, `'batch'`, `'cost-optimized'`) that set multiple GUCs at once:
  ```sql
  SELECT pgtrickle.create_stream_table('my_view', 'SELECT ...', preset => 'real-time');
  ```

---

### Pillar 4: Observability & Self-Healing

#### Strengths

- **30+ Prometheus metrics** covering refresh modes, CDC lag percentiles,
  worker queue depth, template cache utilization, and invalidation ring overflow.
- **NOTIFY-based alerting** on `pg_trickle_alert` channel with JSON payloads
  categorized by severity.
- **Refresh history audit trail** with reason codes for every mode decision.
- **health_check()** function with threshold-based alerts.
- **per-module diagnostics** via explain_stream_table() and cache_stats().

#### Gaps / Critical Flaws

| ID | Severity | Finding |
|----|----------|---------|
| OBS-001 | MEDIUM | **No OpenTelemetry trace spans.** While OTel was mentioned in v0.37.0, there are no actual trace spans in the refresh hot path. An SRE cannot correlate a slow refresh with specific operator execution times, SPI latencies, or lock wait times without manual EXPLAIN ANALYZE. |
| OBS-002 | MEDIUM | **No end-to-end latency metric (commit-to-visible).** The system tracks CDC lag and refresh duration separately, but there is no single metric showing "time from source commit to stream table visibility." This is the #1 metric operators need for SLA compliance. |
| OBS-003 | MEDIUM | **Metrics not exposed via pg_stat_statements integration.** Delta queries are executed as dynamic SQL, making them invisible to pg_stat_statements. Operators cannot use standard PostgreSQL monitoring tools to identify slow delta queries. |

#### Opportunities

- **OBS-O1: OpenTelemetry instrumentation.** Add trace spans to:
  `scheduler_tick`, `refresh_cycle`, `delta_sql_execute`, `merge_apply`,
  `frontier_advance`, `cleanup`. Export via OTLP to any collector.

- **OBS-O2: Commit-to-visible latency metric.** Track the wall-clock time
  between the source transaction's commit timestamp (via `pg_xact_commit_timestamp`)
  and the stream table's `data_timestamp` update. Expose as
  `pg_trickle_commit_to_visible_ms` histogram.

- **OBS-O3: pg_stat_pgtrickle virtual view.** A system view (similar to
  pg_stat_user_tables) that exposes per-ST cumulative statistics: total
  refreshes, total delta rows, avg/p95/p99 refresh duration, last refresh
  time, current lag estimate.

- **OBS-O4: Self-healing circuit breaker with auto-remediation.** Beyond the
  current `max_consecutive_errors` suspension, implement auto-remediation:
  detect OOM → reduce `merge_work_mem_mb`; detect lock timeout → increase
  `scheduler_interval_ms`; detect sustained lag → add refresh workers.

---

## 3. Code & Structural Inconsistencies

| # | File/Area | Issue | Severity |
|---|-----------|-------|----------|
| 1 | `src/config.rs` | 130+ GUC declarations in a single file (~2000+ lines). Should be split by category (scheduler, cdc, dvm, monitoring, performance). | LOW |
| 2 | `src/dvm/mod.rs` | Thread-local caches (`DELTA_TEMPLATE_CACHE`, `PLACEHOLDER_RESOLVER_CACHE`) use `RefCell<HashMap>` which panics on re-entrant borrow. While unlikely in the current call graph, a future refactor adding recursive delta generation could trigger this. | MEDIUM |
| 3 | `src/refresh/merge/mod.rs` | `execute_full_refresh()` is 200+ lines with deep nesting. The auxiliary column injection (COUNT, AVG, STDDEV, NONNULL) should be extracted into a composable pipeline. | LOW |
| 4 | `src/scheduler/scheduler_loop.rs` | Launcher uses `HashMap<String, Instant>` for per-DB state without size bounds. In a cluster with thousands of databases (multi-tenant SaaS), this grows unbounded. | LOW |
| 5 | `src/dvm/operators/` | 20+ operator files with similar structure but no shared trait for delta generation. A `DeltaOperator` trait with `fn generate_delta(&self, ctx: &DiffContext, children: &[DiffResult]) -> Result<DiffResult>` would enable plugin-style operator extension. | MEDIUM |
| 6 | `src/shmem.rs` | `INVALIDATION_RING_MAX_CAPACITY=4096` is a compile-time constant. The GUC controls effective capacity but cannot exceed this. Deployments needing >4096 require recompilation. | LOW |
| 7 | `Cargo.toml` | `pgrx = "=0.18.0"` exact pin prevents security patch uptake. Should use `"~0.18"` or document the pin rationale. | LOW |
| 8 | `src/dvm/diff.rs` | `DiffContext` carries 15+ fields including `scan_pushed_predicate`, `st_bypass_tables`, `source_cdc_columns`, `source_key_columns`. This is a God Object pattern — consider splitting into `CdcContext`, `CacheContext`, `OptimizationContext`. | MEDIUM |
| 9 | `src/refresh/orchestrator.rs` | `query_refresh_history_stats()` falls back to a live subquery on `pgt_refresh_history` when the summary table has no entry. This fallback path is O(history_size) and can be triggered during cold start or after table recreation. | MEDIUM |
| 10 | Multiple | `nosemgrep` comments suppress security linting on dynamic SQL. While individually justified (OID-only interpolation), the volume (28+ suppressions per earlier assessment) suggests the SQL builder abstraction is incomplete. | LOW |

---

## 4. The Blueprint for Infinite Scaling

### Phase 1: Decoupled Compute (v0.81–v0.83)

**Goal:** Enable DVM computation outside the PostgreSQL backend process while
maintaining backward compatibility with the current in-process mode.

```
┌────────────────────────────────────┐
│  PostgreSQL Primary                │
│  ┌──────────────────────────────┐  │
│  │ pg_trickle extension         │  │
│  │ • CDC triggers/WAL decoder   │  │
│  │ • Catalog & frontier mgmt    │  │
│  │ • In-process scheduler       │  │
│  │ • MERGE executor             │  │
│  └──────────────────────────────┘  │
│         │ change buffers            │
│         ▼                           │
│  ┌──────────────────────────────┐  │
│  │ pgtrickle_changes.*          │  │
│  └──────────────────────────────┘  │
└────────────────────────┬───────────┘
                         │ logical replication / COPY
                         ▼
┌────────────────────────────────────┐
│  External DVM Workers (optional)   │
│  ┌──────────────────────────────┐  │
│  │ pg_trickle_worker binary     │  │
│  │ • Connects via tokio-postgres│  │
│  │ • Reads change buffers       │  │
│  │ • Computes delta SQL         │  │
│  │ • Writes back via MERGE      │  │
│  └──────────────────────────────┘  │
│  ┌──────────────────────────────┐  │
│  │ pg_trickle_worker binary     │  │
│  │ (N replicas, stateless)      │  │
│  └──────────────────────────────┘  │
└────────────────────────────────────┘
```

**Key design decisions:**
- Workers are **stateless** — all state lives in PostgreSQL catalog tables.
- Workers **claim** stream tables via advisory locks (`pg_advisory_xact_lock`).
- The in-process scheduler acts as a **coordinator** that assigns work to
  external workers when available, falling back to in-process execution.
- Change buffers are read via `COPY ... TO STDOUT (FORMAT binary)` for
  zero-deserialization transfer.

### Phase 2: Distributed CDC (v0.84–v0.86)

**Goal:** Eliminate write-path impact by moving CDC consumption entirely
out-of-process.

```
┌──────────────────────┐       ┌───────────────────────┐
│ PostgreSQL Primary   │       │ CDC Consumer Process  │
│                      │◄──────│ (logical replication) │
│ • No triggers        │ slot  │ • pgoutput protocol   │
│ • Zero write impact  │       │ • Batched writes to   │
│ • wal_level=logical  │       │   change buffers OR   │
│                      │       │   external log (Kafka)│
└──────────────────────┘       └───────────────────────┘
                                         │
                                         ▼
                               ┌───────────────────────┐
                               │ DVM Workers           │
                               │ • Read from log/buf   │
                               │ • Compute deltas      │
                               │ • Apply to ST         │
                               └───────────────────────┘
```

**Key design decisions:**
- CDC consumer is a **separate binary** (`pg_trickle_cdc`) that subscribes to
  PostgreSQL logical replication.
- In distributed mode, change events are written to a durable log (Kafka, NATS
  JetStream, or local WAL files) instead of PostgreSQL tables.
- The `pg_trickle.cdc_mode='external'` GUC switches between in-process triggers
  and external CDC.
- Backward compatible: single-node deployments continue using triggers.

### Phase 3: Kubernetes-Native Scaling (v0.87–v0.90)

**Goal:** Auto-scaling DVM workers on Kubernetes with operator-managed lifecycle.

```yaml
apiVersion: trickle-labs.io/v1alpha1
kind: StreamTableCluster
metadata:
  name: production
spec:
  postgresql:
    host: pg-primary.default.svc
    port: 5432
  workers:
    min: 2
    max: 32
    targetCdcLagMs: 100    # Scale up when lag exceeds this
    targetCpuPercent: 70   # Scale up when CPU exceeds this
  cdc:
    mode: external         # Use logical replication consumer
    logBackend: nats       # NATS JetStream for change events
  monitoring:
    prometheus: true
    otlp:
      endpoint: otel-collector.monitoring.svc:4317
```

**Components:**
1. **pg_trickle Kubernetes Operator** — watches `StreamTableCluster` CRDs,
   manages worker Deployments, HPA policies, and CDC consumer StatefulSets.
2. **Worker Pods** — stateless, auto-scaling, pull work from a coordination
   queue (Redis or PostgreSQL advisory locks).
3. **CDC Consumer StatefulSet** — one pod per replication slot, with exactly-once
   delivery to the change log.
4. **Metrics Adapter** — exposes CDC lag as a Kubernetes custom metric for HPA.

### The "Zero-Config" Adaptive Mode

The system detects its deployment context and adapts:

| Context | Detection | Behavior |
|---------|-----------|----------|
| **Developer laptop** | `max_connections < 20`, no k8s env vars | Single in-process worker, trigger CDC, 5s scheduler interval |
| **Small server** | `max_connections < 100`, no external workers registered | 4 in-process workers, trigger CDC, 1s scheduler interval |
| **Production server** | External workers registered in `pgt_worker_registry` | Coordinator mode: assigns work to external workers, keeps in-process as fallback |
| **Kubernetes cluster** | `PG_TRICKLE_K8S_MODE=true` env var | Full distributed mode: external CDC, external workers, operator-managed scaling |

---

## 5. Prioritized Action Plan

### Immediate — Quick Wins (v0.81.0)

These items require minimal architectural change and deliver immediate value:

| ID | Item | Impact | Effort |
|----|------|--------|--------|
| QW-1 | **Commit-to-visible latency metric.** Track and expose `pg_trickle_commit_to_visible_ms` using `pg_xact_commit_timestamp()`. | High (SLA compliance) | Small |
| QW-2 | **Configuration advisor function.** `pgtrickle.tune_recommendations()` analyzes workload and suggests optimal GUC values. | High (onboarding) | Medium |
| QW-3 | **Preview/dry-run mode.** `pgtrickle.preview_stream_table(query)` returns detected sources, planned CDC mode, estimated complexity, and refresh strategy without creating anything. | Medium (DX) | Small |
| QW-4 | **OpenTelemetry trace spans.** Instrument `scheduler_tick`, `refresh_cycle`, `delta_execute`, `merge_apply` with OTel spans. Export via OTLP. | High (observability) | Medium |
| QW-5 | **Bounded L0/L1 template cache.** Add LRU eviction to thread-local caches with a configurable max-entries GUC. | Medium (memory safety) | Small |
| QW-6 | **DeltaOperator trait.** Define a trait for operator delta generation enabling extensibility and cleaner dispatch. | Medium (code quality) | Medium |
| QW-7 | **Split config.rs by category.** Move GUC declarations into `config/scheduler.rs`, `config/cdc.rs`, `config/dvm.rs`, `config/monitoring.rs`. | Low (maintainability) | Small |
| QW-8 | **Self-healing circuit breaker.** Auto-reduce `merge_work_mem_mb` on OOM, auto-increase `scheduler_interval_ms` on lock timeout, auto-add workers on sustained lag. | High (reliability) | Medium |
| QW-9 | **Chunked MERGE for large deltas.** When delta row count exceeds `merge_batch_size` (default 50K), split into batched MERGE statements to reduce peak memory and lock hold time. | High (performance) | Medium |
| QW-10 | **Stream table presets.** Named configuration profiles ('real-time', 'batch', 'cost-optimized') that set multiple parameters at once. | Medium (DX) | Small |

### Medium-Term — Core Engine Refactoring (v0.82.0–v0.84.0)

| ID | Item | Impact | Effort |
|----|------|--------|--------|
| MT-1 | **External worker binary.** Extract DVM + MERGE into a standalone `pg_trickle_worker` binary connecting via `tokio-postgres`. In-process mode remains default. | Critical (scaling) | Large |
| MT-2 | **Worker coordination protocol.** Advisory-lock-based work claiming, heartbeat monitoring, and automatic failover for external workers. | Critical (scaling) | Large |
| MT-3 | **Pipelined refresh execution.** Overlap delta SQL execution with MERGE application using portal-based streaming. Reduce peak memory by not materializing full delta set. | High (performance) | Large |
| MT-4 | **External CDC consumer binary.** `pg_trickle_cdc` binary that subscribes to logical replication and writes to change buffers or external log, eliminating trigger overhead. | High (write-path) | Large |
| MT-5 | **Shared-memory change buffer ring.** For hot sources (>10K writes/s), replace table-based change buffers with a fixed-size ring buffer in shared memory. Overflow spills to table. | High (performance) | Large |
| MT-6 | **Auto-schema-evolution.** DDL event trigger detects source table changes and automatically propagates additive schema changes to stream tables. | Medium (DX) | Medium |
| MT-7 | **DiffContext decomposition.** Split the 15-field God Object into focused sub-contexts (CdcContext, CacheContext, OptimizationContext). | Medium (code quality) | Medium |
| MT-8 | **Vectorized aggregate path.** For pure-aggregate STs (no joins), compute delta aggregates using Arrow-compatible columnar batches with SIMD operations. | High (performance) | Large |
| MT-9 | **pg_stat_pgtrickle system view.** Virtual view exposing per-ST cumulative statistics compatible with standard PostgreSQL monitoring tools. | Medium (observability) | Medium |
| MT-10 | **Adaptive worker pool sizing.** Automatically adjust `max_dynamic_refresh_workers` based on CPU utilization, refresh queue depth, and lag metrics. No GUC required. | High (self-tuning) | Medium |

### Long-Term — Distributed Scale (v0.85.0–v0.90.0)

| ID | Item | Impact | Effort |
|----|------|--------|--------|
| LT-1 | **Kubernetes operator.** CRD-based management of worker deployments, HPA policies, and CDC consumer StatefulSets. | Critical (K8s) | Very Large |
| LT-2 | **External change log backend.** Support Kafka, NATS JetStream, or local WAL files as the intermediate storage between CDC and DVM workers. | Critical (scaling) | Very Large |
| LT-3 | **Partitioned delta computation.** For large stream tables, partition the delta computation by source key ranges across multiple workers. | High (throughput) | Very Large |
| LT-4 | **Read-replica DVM execution.** Run delta computation on read replicas (streaming replication), apply results to primary. Eliminates compute load on primary entirely. | High (isolation) | Very Large |
| LT-5 | **Multi-cluster federation.** Coordinate stream tables across multiple PostgreSQL clusters with a global DAG and cross-cluster frontier synchronization. | Medium (enterprise) | Very Large |
| LT-6 | **Declarative SQL syntax.** Implement `CREATE STREAM TABLE name AS SELECT ...` via ProcessUtility_hook or event trigger interception. | Medium (DX) | Large |
| LT-7 | **Incremental window function computation.** Replace partition-based recomputation with proper incremental algorithms for ROW_NUMBER, RANK, DENSE_RANK, LAG/LEAD. | High (performance) | Very Large |
| LT-8 | **State checkpointing to object storage.** Periodically checkpoint DVM state (frontier, template cache, operator state) to S3/GCS for disaster recovery independent of pg_basebackup. | Medium (durability) | Large |
| LT-9 | **Cost-based operator scheduling.** Reorder operators within a delta query based on estimated cardinality to minimize intermediate result sizes. | Medium (performance) | Large |
| LT-10 | **Zero-copy change buffer protocol.** Use PostgreSQL's shared memory and UNIX domain sockets for zero-copy transfer of change events between CDC and DVM workers on the same host. | Medium (latency) | Large |

---

## 6. Version Mapping

> **Superseded.** The v0.82.x–v0.87.x mapping below records this assessment's
> original recommendation. The roadmap was subsequently reshaped around a
> product thesis rather than a scaling thesis, and then resequenced: the four
> implementation-audit gates were renumbered from v0.81.1–v0.81.4 to
> v0.82.0–v0.85.0 (they are minor releases, not patches), and the product arc
> now runs v0.86.0–v0.93.0. The distributed items (MT-1, MT-2, MT-4, LT-1
> through LT-5, LT-8, LT-10) moved to v1.7.0–v1.9.0. **Every version number in
> the table below is historical.** See [ROADMAP.md](../ROADMAP.md) for the
> current plan.

The action plan maps to the following release schedule:

| Version | Theme | Items |
|---------|-------|-------|
| **v0.81.0** | Observability, Self-Tuning & Quick Wins | QW-1 through QW-10 |
| **v0.81.1** | Frontier and CDC Durability Gate | Pre-scaling implementation audit; now [roadmap/v0.82.0.md](../roadmap/v0.82.0.md) |
| **v0.81.2** | DVM Semantic Fidelity Gate | Pre-scaling implementation audit; now [roadmap/v0.83.0.md](../roadmap/v0.83.0.md) |
| **v0.81.3** | Catalog, Privilege, and Upgrade Integrity | Pre-scaling implementation audit; now [roadmap/v0.84.0.md](../roadmap/v0.84.0.md) |
| **v0.81.4** | Scheduler and Resource Resilience Gate | Pre-scaling implementation audit; now [roadmap/v0.85.0.md](../roadmap/v0.85.0.md) |
| **v0.82.0** | External Worker Foundation | MT-1, MT-2, MT-7, MT-9, MT-10 |
| **v0.83.0** | Performance Pipeline & CDC Extraction | MT-3, MT-4, MT-5, MT-6 |
| **v0.84.0** | Vectorized Compute & Adaptive Engine | MT-8, LT-7, LT-9 |
| **v0.85.0** | Kubernetes-Native Deployment | LT-1, LT-2, LT-6 |
| **v0.86.0** | Distributed Delta Computation | LT-3, LT-4, LT-10 |
| **v0.87.0** | Enterprise Federation & State Management | LT-5, LT-8 |

---

## 7. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| External worker adds latency vs in-process | Medium | Medium | Benchmark gate: external mode must be within 2× of in-process latency for identical workloads |
| Kafka/NATS dependency increases operational complexity | High | Medium | Make external log optional; local WAL files as zero-dependency default |
| Schema evolution auto-propagation breaks queries | Medium | High | Conservative: only additive changes (ADD COLUMN), require explicit opt-in |
| Kubernetes operator maintenance burden | Medium | Medium | Use controller-runtime patterns; generate CRDs from Rust types |
| Thread-local cache migration to external workers | Low | Medium | Design worker protocol to include template cache warming on assignment |

---

## 8. Conclusion

pg_trickle v0.80.0 is the **strongest single-node IVM implementation in the
PostgreSQL ecosystem** — superior to pg_ivm in SQL coverage, to Materialize in
deployment simplicity, and to Feldera in PostgreSQL integration depth. The
codebase quality is exceptional: rigorous error handling, comprehensive tests,
and clean module boundaries.

The path from "excellent single-node extension" to "best IVM system ever built"
requires three structural investments:

1. **Decoupled compute** (v0.82–v0.83): External workers that can scale
   independently of the PostgreSQL process.
2. **Zero-impact CDC** (v0.83–v0.84): External logical replication consumer
   that eliminates trigger overhead.
3. **Kubernetes-native scaling** (v0.85–v0.87): Operator-managed auto-scaling
   based on lag metrics.

Each phase is backward-compatible. A developer's laptop continues running
pg_trickle as a single in-process extension with zero additional infrastructure.
A production Kubernetes cluster deploys the full distributed topology with
auto-scaling workers. The "Zero-Config Paradox" is solved through runtime
detection and progressive opt-in.

The immediate v0.81.0 release delivers quick wins (OTel tracing, commit-to-visible
metric, configuration advisor, chunked MERGE, self-healing) that make the current
single-node engine significantly more observable and self-tuning — valuable
regardless of whether the distributed scaling path is pursued. The subsequent
implementation-audit gates (now v0.82.0–v0.85.0) strengthen existing durability,
DVM, catalog, privilege, upgrade, scheduler, and resource contracts without
adding major features. MT-1 and MT-2 begin only after those gates pass.
