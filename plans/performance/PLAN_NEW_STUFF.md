# Performance & Scalability — New Feature Research Report

> Date: 2026-03-12
> Status: Research / Proposal
> Scope: High-level features for performance and scalability beyond current plans

---

## Executive Summary

This report identifies **12 high-level performance and scalability features**
not yet covered by existing plans (Parts 8–9, parallelization report, TPC-H
plan). Features are grouped into four themes:

1. **Reduce the MERGE bottleneck** — The 70–97% hotspot
2. **Smarter delta computation** — Do less work per refresh cycle
3. **Scale to larger deployments** — 100+ STs, multi-TB databases, multi-tenant
4. **Reduce CDC overhead** — Lighter change capture on the write path

Each feature includes a description, rationale, expected impact, effort
estimate, and references to prior art from Feldera, Noria/ReadySet, DBSP,
Flink, and academic literature.

---

## Theme A — Reduce the MERGE Bottleneck

Per Part 8 profiling, PostgreSQL `MERGE` execution dominates 70–97% of refresh
time. The MERGE statement joins the delta CTE against the stream table, then
performs INSERT/UPDATE/DELETE. Every feature in this theme attacks that cost
from a different angle.

### A-1: Partitioned Stream Tables (Partition-Scoped MERGE)

**Problem.** When a stream table has 10M+ rows, the MERGE scans the entire
target table even when the delta touches only a handful of partitions.
PostgreSQL's MERGE does support partition pruning, but only if the target is
declaratively partitioned and the join predicate aligns with the partition key.

**Proposal.** When the user defines a stream table with `PARTITION BY` on the
source or explicitly on the stream table:

1. Auto-create the stream table as a partitioned table (RANGE or LIST on the
   declared partition key).
2. At refresh time, inspect the delta to determine which partitions are
   affected (scan the delta CTE for `min/max` of the partition key).
3. Rewrite the MERGE to target only the affected partitions using a
   `WHERE partition_key BETWEEN ? AND ?` predicate that enables partition
   pruning.
4. For cold partitions (no changes), skip entirely — zero-cost.

**Expected Impact.** For a 10M-row stream table partitioned into 100
partitions with a 0.1% change rate concentrated in 2–3 partitions, the MERGE
scans ~100K rows instead of 10M — **100× reduction** in MERGE I/O.

**Effort.** 3–5 weeks (DDL changes, partition detection, MERGE rewrite).

**Prior Art.** Flink's partitioned state backends; TimescaleDB hypertable
chunk-scoped operations; Materialize's partitioned arrangements.

**⚠️ Risk Analysis — Partition pruning does not flow through `__pgt_row_id`.**

PostgreSQL partition pruning during MERGE activates only when the MERGE join
predicate or a WHERE clause aligns with the partition key. In pg_trickle, the
MERGE always joins on `__pgt_row_id` (a content hash unrelated to any user
column). The planner has no basis to prune partitions from this join alone.

Step 3 of the proposal (`WHERE partition_key BETWEEN ? AND ?`) would force
pruning, but this predicate must be separately computed from the delta (via
a `min/max` scan), and it must provably contain every delta row's partition
key — otherwise correct rows are silently excluded from the MERGE. This means
the implementation cannot simply target the partitioned parent table; it must
either:
- Issue one MERGE per affected child partition (partition-by-partition loop
  in Rust), or
- Inject a verified partition-key range predicate into the MERGE WHERE clause.

Both approaches require the stream table's partition key to be a user-visible
column derived from the defining query (not `__pgt_row_id`), which the current
catalog does not track. The `P3` priority and `High` risk rating in the matrix
are appropriate; this is not a quick win.

---

### A-2: Columnar Change Tracking (Column-Scoped Delta)

**Problem.** When a source table UPDATE changes only 1 of 50 columns, the
current CDC captures the entire row (old + new) and the delta query processes
all columns. If the changed column is not referenced by the stream table's
defining query, the entire refresh is wasted work.

**Proposal.** Track per-column change metadata:

1. In the CDC trigger, compute a bitmask of actually-changed columns
   (`old.col IS DISTINCT FROM new.col` for each column) and store it as an
   `int8` or `bit(n)` alongside the change row.
2. At delta-query build time, consult the bitmask:
   - If no referenced column changed → **skip the row entirely** (filter in
     the `delta_scan` CTE).
   - If only projected columns changed (no join keys, no filter predicates,
     no aggregate keys) → use a lightweight UPDATE-only path instead of
     full DELETE+INSERT.
3. For aggregates, if only the aggregated value column changed (not the GROUP
   BY key), emit a single correction row instead of two (delete old group +
   insert new group).

**Expected Impact.** In OLTP workloads where UPDATEs dominate and typically
touch 1–3 columns of wide tables: **50–90% reduction in delta row volume**
for wide-table scenarios.

**Effort.** 3–4 weeks (trigger changes, bitmask propagation, delta CTE
generation changes).

**Prior Art.** Oracle's column-change tracking in materialized view logs;
Feldera's per-field Z-set weights; DBToaster's multi-level deltas.

---

### A-3: MERGE Bypass via Direct INSERT OVERWRITE

**Problem.** PostgreSQL's MERGE planner can choose suboptimal join strategies,
especially for small deltas against large target tables. The MERGE also
imposes overhead from its three-way MATCHED/NOT MATCHED/NOT MATCHED BY SOURCE
logic.

**Proposal.** For specific operator tree shapes, bypass MERGE entirely:

1. **Append-Only Stream Tables.** ✅ **Recommended — do this.**
   When the defining query references only INSERT-only sources (e.g., event
   logs, append-only tables where CDC never sees DELETE/UPDATE), skip the
   MERGE entirely and use a simple `INSERT INTO st SELECT ... FROM delta`.
   This is the clear winner: removes MERGE entirely for append-only sources,
   takes only `RowExclusiveLock` (same as a plain INSERT), and `IvmLockMode::
   RowExclusive` already exists in the codebase. Detection strategy: expose
   an explicit user declaration (`CREATE STREAM TABLE … APPEND ONLY`) as the
   primary signal, with a CDC-observed heuristic fallback (no DELETE/UPDATE
   seen yet → use fast path, fall back to MERGE on first non-insert). Low
   risk, high payoff for event-sourced architectures.

2. **Full-Replacement Window.** ⚠️ **Not recommended — keep MERGE.**
   When the stream table has TopK semantics (ORDER BY + LIMIT) with a small
   limit (< 10K), use
   `TRUNCATE st; INSERT INTO st SELECT ... FROM (full_query) LIMIT N`.
   This is stated to be faster than MERGE when the target is small and the
   delta can recompute cheaply.

   **Assessment:** `execute_topk_refresh` already uses MERGE correctly and
   is not a measured bottleneck for TopK. The actual bottleneck for TopK
   tables is the `full_query` re-scan of the base table, not the MERGE of a
   handful of rows. The performance crossover where TRUNCATE+INSERT beats
   MERGE only occurs at limits in the hundreds-of-thousands range — at which
   point the query cost dwarfs everything. Meanwhile, the operational risks
   are substantial (see Safety Analysis below). **Move to "Won't Do" unless
   profiling demonstrates MERGE itself is the bottleneck for TopK.**

3. **Bulk COPY Path.** ❌ **Not feasible as described — drop or rescope.**
   When the delta produces > 10K rows, switch to a `COPY`-based bulk load.
   COPY is 2–5× faster than INSERT for bulk loads due to bypassing
   per-row executor overhead.

   **Assessment:** The premise is flawed. `COPY FROM` inside pgrx via SPI
   only reads from a file or stdin — `COPY st FROM (SELECT …)` is not valid
   SQL. The quoted throughput advantage applies to the client-side `psql
   \copy` or binary COPY protocol, neither of which is accessible from inside
   the backend via SPI. The actual bulk path available via SPI is
   `INSERT INTO st SELECT …`, which `execute_full_refresh` already does on
   the adaptive-FULL threshold. There is no speedup to unlock here without a
   fundamentally different data path outside SPI — a much larger investment.
   This sub-path should be **dropped** unless re-scoped as a separate
   low-level infrastructure feature.

**Expected Impact.** Append-only path eliminates MERGE entirely for
event-sourced architectures; significant throughput gain for those workloads.
The other two sub-paths have negligible or negative net impact.

**Effort.** 1–2 weeks (append-only declaration, CDC heuristic, path
selection logic, benchmarks). Scoping down from the original 2–3 weeks.

**Prior Art.** Snowflake's MERGE vs INSERT OVERWRITE cost-based selection;
DuckDB's bulk INSERT path; Redshift's MERGE alternatives.

**Safety Analysis — Full-Replacement Window (TRUNCATE + INSERT).**

The `TRUNCATE st; INSERT INTO st ...` pattern for TopK tables is
**correct under PostgreSQL's MVCC** — external readers see the old
snapshot until the transaction commits, so there is no "empty table"
visibility window for concurrent sessions. The existing
`execute_full_refresh` path in `refresh.rs` already uses this pattern
and handles the required guard work:

- Sets `SET LOCAL pg_trickle.internal_refresh = 'true'` before TRUNCATE
  so DML-guard triggers allow the modification.
- Disables user triggers via `DISABLE TRIGGER USER` / `ENABLE TRIGGER USER`
  around the TRUNCATE + INSERT window.

However, the following concerns must be addressed before implementing
this path:

1. **`ACCESS EXCLUSIVE` lock starvation.** `TRUNCATE` acquires
   `ACCESS EXCLUSIVE` on the stream table, which blocks *all* concurrent
   readers (`SELECT`) for the entire duration including the INSERT phase.
   By contrast, MERGE acquires only row-level locks and allows reads to
   proceed against the pre-MERGE snapshot. For TopK tables refreshed
   sub-second this is a significant regression. **Mitigation:** gate
   the TRUNCATE path on a minimum `refresh_interval` (e.g., ≥ 5 s), or
   expose a GUC `pg_trickle.topk_bypass_mode = 'merge' | 'truncate'`
   so operators can opt in explicitly.

2. **Change-buffer LSN reset required.** After a TRUNCATE+INSERT full
   replacement, buffered change-buffer entries for the source table must
   not be applied in the next differential refresh — that would
   double-count them. `execute_full_refresh` avoids this because the
   scheduler resets the change-frontier LSN after a full refresh.
   The TRUNCATE path for TopK must trigger the same LSN reset; omitting
   it would silently corrupt the stream table.

3. **No-op guard missing.** TRUNCATE+INSERT always rewrites the table and
   holds `ACCESS EXCLUSIVE` even when the TopK result is unchanged.
   MERGE's `WHEN MATCHED AND (IS DISTINCT FROM ...)` clause naturally
   skips unchanged rows. The implementation should check whether the
   source delta contains any changes before issuing the TRUNCATE, or
   keep MERGE as the default and only fall back to TRUNCATE+INSERT when
   the delta exceeds a size threshold where MERGE wins.

4. **`__pgt_row_id` stability.** TRUNCATE+INSERT regenerates all row IDs.
   Since `row_id_expr_for_query` produces a deterministic content hash,
   IDs are stable across refreshes for unchanged rows. No correctness
   issue, but this should be verified by tests that assert row-ID
   continuity across a TRUNCATE-path refresh cycle.

5. **No RETURNING from TRUNCATE.** TRUNCATE has no `RETURNING` clause,
   so per-row counts for `pgtrickle_refresh` NOTIFY payloads and
   monitoring metrics must be derived from the INSERT's row count only.

---

### A-4: Index-Aware MERGE Planning

**Problem.** The MERGE joins delta against the stream table on `__pgt_row_id`.
If the stream table lacks an index on this column (it currently has one, but
the planner may not use it for small deltas), PostgreSQL may choose a
sequential scan.

**Proposal.**

1. **MERGE planner hints injection.** Extend `pg_trickle.merge_planner_hints`
   to automatically include:
   - `SET enable_seqscan = off` when `delta_row_count < 0.01 * target_count`
   - Index hints (PG 18 planner hints extension) pointing to the
     `__pgt_row_id` index.
2. **Covering index auto-creation.** When a stream table is created, if the
   defining query's output columns are small enough (< 8 columns), create a
   covering index `INCLUDE (col1, col2, ...)` on `__pgt_row_id` to enable
   index-only scans during MERGE. This eliminates the heap-fetch for matched
   rows.
3. **Partial index for hot partitions.** For time-series stream tables, create
   a partial index on `__pgt_row_id WHERE updated_at > now() - interval '1 day'`
   to keep the index small and hot.

**Expected Impact.** 20–50% MERGE time reduction for small-delta/large-target
scenarios where the planner currently chooses seq-scan.

**Effort.** 1–2 weeks (planner hint logic, auto-index DDL).

**Prior Art.** pg_hint_plan; Oracle's materialized view log indexes;
SQL Server's indexed views with clustered index selection.

---

## Theme B — Smarter Delta Computation

These features reduce the amount of work the DVM engine and PostgreSQL do
per refresh cycle, independent of the MERGE step.

### B-1: Incremental Aggregate Maintenance (Algebraic Shortcuts)

**Problem.** The current aggregate delta rule recomputes entire groups where
the GROUP BY key appears in the delta. For a group with 100K rows where 1 row
changed, the aggregate re-scans all 100K rows in that group.

**Proposal.** Implement algebraic aggregate maintenance for decomposable
aggregates:

| Aggregate | Algebraic Maintenance | Aux State |
|-----------|-----------------------|-----------|
| COUNT     | `old_count + Δcount` | Single counter |
| SUM       | `old_sum + Δsum` | Single accumulator |
| AVG       | `(old_sum + Δsum) / (old_count + Δcount)` | Sum + count |
| MIN/MAX   | If deleted value = current min/max → rescan; otherwise → O(1) update | Current min/max + count-at-min |
| STDDEV    | Online Welford update | Mean + M2 + count |

Implementation:
1. Add auxiliary columns to the stream table: `__pgt_aux_count`,
   `__pgt_aux_sum`, etc. (hidden from user queries via view wrapper).
2. Delta query emits correction values instead of full group recomputation:
   ```sql
   UPDATE st SET
     total_amount = total_amount + delta.sum_amount,
     row_count = row_count + delta.count_change,
     avg_amount = (total_amount + delta.sum_amount) /
                  NULLIF(row_count + delta.count_change, 0)
   WHERE st.group_key = delta.group_key
   ```
3. Fall back to full-group recomputation for non-decomposable aggregates
   (mode, percentile, string_agg with ordering).

**Expected Impact.** For high-cardinality GROUP BY with large groups:
**10–1000× speedup** per aggregate group (from O(group_size) to O(1)).
Benchmarks show aggregate scenarios at 100K/1% go from 2.5ms to sub-1ms.

**Effort.** 4–6 weeks (auxiliary column management, algebraic rules per
aggregate, fallback logic, correctness tests).

**Prior Art.** DBSP's linear operator lifting; DBToaster's higher-order
maintenance; Feldera's in-memory aggregate state; Noria's partial state.

**Risk.** Auxiliary columns increase storage. Need migration story for
existing stream tables. Floating-point aggregates may accumulate rounding
errors over many cycles (periodic recomputation to reset).

**⚠️ Risk Analysis — MIN/MAX rule stated backwards; correctness bug.**

The table above originally stated: *"If deleted value ≠ current min/max → no
rescan"*. This is the inverse of the correct condition and has been corrected
above. The correct rule is:
- Deleted value **equals** current min/max → **must rescan** the group
  (the aggregate value may change).
- Deleted value **does not equal** current min/max → safe to skip rescan
  (the aggregate is unaffected).

For `INSERT` changes the same logic applies in reverse for MAX. Getting
this wrong would silently produce stale aggregate values as source rows
are deleted — the most common OLTP pattern. Any implementation must have
explicit property-based tests covering the boundary case (deleting the
exact current min or max value).

---

### B-2: Delta Predicate Pushdown

**Problem.** The delta CTE chain processes all changes, then filters. For a
query like `SELECT ... FROM orders WHERE status = 'shipped'`, if a change row
has `status = 'pending'`, the delta processes it through scan → filter → discard.
The scan and any join work is wasted.

**Proposal.** Push WHERE predicates from the defining query down into the
change buffer scan:

1. During OpTree construction, identify `Filter` nodes whose predicates
   reference only columns from a single source table.
2. Inject these predicates into the `delta_scan` CTE as additional WHERE
   clauses:
   ```sql
   -- Before: scans all changes, filters later
   delta_scan_orders AS (
     SELECT * FROM pgtrickle_changes.changes_12345
     WHERE lsn BETWEEN ? AND ?
   )

   -- After: filters at scan time
   delta_scan_orders AS (
     SELECT * FROM pgtrickle_changes.changes_12345
     WHERE lsn BETWEEN ? AND ?
       AND (new_status = 'shipped' OR old_status = 'shipped')
   )
   ```
3. The `OR old_status = 'shipped'` handles deletions from the filter's
   qualifying set (a row that was 'shipped' and is now 'pending' must be
   removed from the stream table).

**Expected Impact.** For selective queries (< 10% selectivity),
**5–10× reduction in delta row volume** before any join processing.

**Effort.** 2–3 weeks (predicate extraction, pushdown rules, correctness
for UPDATE scenarios where old/new may differ).

**Prior Art.** Standard RDBMS predicate pushdown; Flink's filter pushdown
into source connectors; Materialize's predicate pushdown through arrangements.

---

### B-3: Multi-Table Delta Batching

**Problem.** When a stream table joins tables A, B, and C, and all three have
changes in the same cycle, the current delta query generates:
```
ΔQ = (ΔA ⋈ B₁ ⋈ C₁) ∪ (A₀ ⋈ ΔB ⋈ C₁) ∪ (A₀ ⋈ B₀ ⋈ ΔC)
```
This requires 3 separate passes through the source tables with different
snapshots.

**Proposal.** Optimize multi-source delta by:

1. **Simultaneous delta merging.** When Δ is small relative to base tables,
   precompute a merged delta:
   ```sql
   merged_delta AS (
     SELECT 'A' AS src, * FROM delta_A
     UNION ALL
     SELECT 'B' AS src, * FROM delta_B
     UNION ALL
     SELECT 'C' AS src, * FROM delta_C
   )
   ```
   Then run a single join pass that handles all source changes together.

2. **Change-ordering optimization.** Within a single cycle, if table A has 0
   changes but B has 100, skip the A-delta branch entirely (already
   implemented via no-change short-circuit), but extend this to skip
   individual UNION ALL branches within a multi-source delta.

3. **Cross-delta deduplication.** When ΔA and ΔB produce the same output row
   (e.g., both sides of a join changed), the current approach may produce
   duplicate corrections. Add a final `DISTINCT ON (__pgt_row_id)` only when
   the optimizer detects diamond-shaped delta flow.

**Expected Impact.** For multi-join queries with changes in multiple sources:
**30–50% reduction in total I/O** by eliminating redundant base-table scans.

**Effort.** 4–6 weeks (delta branch pruning, merged delta generation,
correctness proofs for simultaneous changes).

**Prior Art.** DBSP's simultaneous delta processing; DBToaster's factorized
evaluation; Materialize's shared arrangements.

**⚠️ Risk Analysis — Cross-delta deduplication via `DISTINCT ON` is a correctness bug.**

Step 3 proposes: *"Add a final `DISTINCT ON (__pgt_row_id)` … when the
optimizer detects diamond-shaped delta flow."*

This is incorrect. When both ΔA and ΔB produce corrections to the same
output row (e.g., both sides of a join changed in the same cycle), those
corrections carry independent multiplicities (weights) in the DBSP Z-set
algebra: the correct result requires summing them, not deduplicating. A
`DISTINCT ON (__pgt_row_id)` arbitrarily keeps one correction and discards
the other, silently producing wrong data for any query with a diamond delta
flow — precisely the scenario this feature targets.

The correct approach is algebraic combination: group by `__pgt_row_id` and
aggregate the weight column (`SUM(weight)`), removing rows where the net
weight is zero. This is how DBSP handles simultaneous deltas.

Given that the merged-delta approach requires a correct implementation of
weight aggregation to avoid silent data corruption, the risk rating for B-3
should be **Very High** and it should not move out of Wave 4 until formal
correctness proofs or property-based tests exist for simultaneous multi-source
change scenarios.

---

### B-4: Cost-Based Refresh Strategy Selection

**Problem.** The adaptive FULL/DIFFERENTIAL threshold is currently a fixed
ratio (`pg_trickle.differential_max_change_ratio` default 0.5). This is a
blunt instrument — a join-heavy query may be better off with FULL at 5% change
rate, while a scan-only query benefits from DIFFERENTIAL up to 80%.

**Proposal.** Replace the fixed threshold with a cost-based optimizer:

1. **Collect statistics per ST.** After each refresh, record:
   - `delta_row_count` (already tracked)
   - `merge_duration_ms` (already tracked)
   - `full_refresh_duration_ms` (tracked from FULL refreshes)
   - `target_row_count` (already tracked)
   - `query_complexity_class` (scan/filter/aggregate/join/join_agg)

2. **Build a cost model.** Using historical data from `pgt_refresh_history`:
   ```
   estimated_diff_cost(Δ) = α × Δ_rows + β × target_rows + γ × complexity
   estimated_full_cost    = δ × target_rows + ε × complexity
   ```
   Fit α, β, γ, δ, ε via linear regression on the last N refreshes.

3. **Select strategy per cycle.** Before each refresh:
   ```
   IF estimated_diff_cost(current_Δ) < estimated_full_cost × safety_margin:
     USE DIFFERENTIAL
   ELSE:
     USE FULL
   ```
   Safety margin (default 0.8) ensures we prefer DIFFERENTIAL even when costs
   are close, since it has lower lock contention.

4. **Cold-start heuristic.** Before enough data is collected (< 10 refreshes),
   use the current fixed threshold as fallback.

**Expected Impact.** Eliminates the join_agg 100K/10% regression (0.3×) by
correctly falling back to FULL. Improves scan/filter scenarios by allowing
DIFFERENTIAL at higher change rates. **5–20% overall throughput improvement**
across mixed workloads.

**Effort.** 2–3 weeks (statistics collection, cost model, strategy selector).

**Prior Art.** PostgreSQL's query planner cost model; Oracle's cost-based
refresh decision for materialized views; SQL Server's indexed-view maintenance
cost estimation.

---

## Theme C — Scale to Larger Deployments

These features address operational scalability: supporting 100+ stream tables,
multi-TB databases, and multi-tenant environments.

### C-1: Tiered Refresh Scheduling (Hot/Warm/Cold)

**Problem.** Current scheduling treats all STs equally — each runs on its
configured schedule regardless of how stale the data actually is or how
frequently it's queried. In a deployment with 500 STs, refreshing all of them
every few seconds wastes CPU on STs that nobody is reading.

**Proposal.** Implement demand-driven tiered scheduling:

1. **Query tracking.** Use `pg_stat_user_tables` to track `seq_scan` and
   `idx_scan` counts on each stream table. Periodically sample these counters
   to determine read frequency.

2. **Tier classification:**
   | Tier | Read Frequency | Refresh Strategy |
   |------|---------------|-----------------|
   | **Hot** | > 10 reads/min | Refresh at configured schedule |
   | **Warm** | 1–10 reads/min | Refresh at 2× configured schedule |
   | **Cold** | < 1 read/min | Refresh only on-demand (lazy) |
   | **Frozen** | 0 reads in last hour | Skip entirely; refresh on first read |

3. **Lazy refresh trigger.** For Cold/Frozen STs, install a lightweight
   `pg_stat_user_tables` polling check. When a read is detected, queue an
   immediate refresh. Optionally, use PostgreSQL's `track_io_timing` or a
   custom hook to detect the first sequential scan.

4. **Configurable per-ST.** Allow `ALTER STREAM TABLE ... SET (tier = 'hot')`
   to override automatic classification.

**Expected Impact.** In a 500-ST deployment where 80% are Cold/Frozen:
**80% reduction in scheduler CPU** and background worker utilization.
Hot STs see no degradation.

**Effort.** 3–4 weeks (tier classification, lazy refresh trigger, scheduler
integration).

**Prior Art.** Materialize's on-demand clusters; Oracle's query-driven refresh;
Flink's idle source detection.

**Risk.** Lazy refresh introduces latency on first read after changes. Need a
`pg_trickle.lazy_refresh_timeout` GUC to bound maximum read latency.

**⚠️ Risk Analysis — `pg_stat_user_tables` signal is polluted by internal refreshes.**

The proposal tracks `seq_scan` / `idx_scan` from `pg_stat_user_tables` to
determine how frequently a stream table is queried. However, pg_trickle's own
refresh path reads the stream table during MERGE (the MERGE engine scans the
target to match on `__pgt_row_id`) and during TopK refresh. These internal
scans increment the same `seq_scan` / `idx_scan` counters. A stream table
that has zero user reads but is refreshed every 10 seconds would accumulate
6 seq_scans per minute — classifying it as **Warm** even though no application
is reading it.

Consequences: Cold/Frozen tier assignment may never trigger for actively
refreshed STs, defeating the primary goal of reducing scheduler CPU.

Mitigation options:
- Track user reads via a separate mechanism (e.g., a custom `ExecutorStart`
  / `ExecutorEnd` hook that filters for queries originating outside the
  pg_trickle background worker).
- Alternatively, compare `pg_stat_user_tables` deltas before and after each
  refresh cycle: increment not caused by the refresh = user read.
- Or expose explicit user-controlled tier setting only (no auto-classification),
  which is simpler, safer, and already included in the proposal as a fallback.

---

### C-2: Incremental DAG Rebuild

**Problem.** When any DDL change occurs (detected via `DAG_REBUILD_SIGNAL`),
the entire dependency graph is rebuilt from scratch by querying
`pgt_dependencies`. For 1000+ STs this becomes expensive (O(V+E) SPI queries).

**Proposal.** Implement incremental DAG maintenance:

1. **Delta-based rebuild.** When `DAG_REBUILD_SIGNAL` fires, record which
   specific ST was affected (store the `pgt_id` in shared memory alongside
   the signal counter).
2. Add/remove only the affected edges and vertices, then re-run topological
   sort only on the affected subgraph.
3. Cache the sorted schedule in shared memory (currently recomputed each
   cycle).

**Expected Impact.** DAG rebuild from O(V+E) to O(Δ_V + Δ_E) — typically
O(1) for single-ST DDL changes. Reduces scheduler latency spike from
~50ms to ~1ms at 1000 STs.

**Effort.** 2–3 weeks (incremental topo-sort, shared memory changes).

**Prior Art.** Flink's incremental job graph updates; Spark's adaptive query
re-optimization.

**⚠️ Risk Analysis — Race condition when two DDL changes arrive before the scheduler ticks.**

The proposal stores the affected `pgt_id` alongside the signal counter in
shared memory. If two DDL changes occur in the gap between scheduler ticks,
the second write of `pgt_id` overwrites the first. The scheduler then
rebuilds only the second ST's subgraph, leaving the first ST with stale DAG
state — silently, with no error.

The fix requires storing a **set** of affected pgt_ids (e.g., a bounded
ring buffer in shared memory), not a single scalar. If the ring overflows
(more DDL changes than buffer capacity between any two scheduler ticks), fall
back to a full rebuild. This is the safe default and should be the starting
implementation; incremental rebuild is only an optimization on top.

---

### C-3: Multi-Database Scheduler Isolation

**Problem.** The current launcher spawns one scheduler per database, but all
schedulers share a single cluster-wide worker budget
(`pg_trickle.max_dynamic_refresh_workers`). In multi-tenant deployments, one
busy database can starve others.

**Proposal.**

1. **Per-database worker quotas.** Add
   `pg_trickle.per_database_worker_quota` (default: equal share of
   `max_dynamic_refresh_workers`). Allow DBA to configure per-database:
   ```sql
   ALTER DATABASE analytics SET pg_trickle.worker_quota = 8;
   ALTER DATABASE reporting SET pg_trickle.worker_quota = 2;
   ```

2. **Priority-based scheduling.** When worker demand exceeds budget, use
   priority ordering:
   - Priority 1: STs with IMMEDIATE mode (transactional consistency)
   - Priority 2: Hot-tier STs (high read frequency)
   - Priority 3: Warm-tier STs
   - Priority 4: Cold-tier STs

3. **Burst capacity.** Allow databases to temporarily exceed their quota
   (up to 150%) if other databases are under-utilizing theirs. Reclaim
   burst capacity within 1 scheduler cycle.

**Expected Impact.** Prevents noisy-neighbor problems in multi-tenant
deployments. Ensures SLA compliance for high-priority databases.

**Effort.** 2–3 weeks (quota tracking in shared memory, priority queue,
burst logic).

**Prior Art.** Kubernetes resource quotas; YARN capacity scheduler;
PostgreSQL's per-database connection limits.

---

### C-4: Change Buffer Compaction

**Problem.** The change buffer tables (`pgtrickle_changes.changes_<oid>`) grow
unboundedly between refresh cycles. For high-write tables with long refresh
intervals, buffers can reach millions of rows, making the delta scan expensive.
Additionally, UPDATE operations produce two rows (DELETE old + INSERT new),
doubling the buffer size.

**Proposal.** Implement periodic compaction of change buffers:

1. **Row-level compaction.** Between refresh cycles, merge multiple changes to
   the same row ID into a single net change:
   - INSERT + DELETE = no-op (remove both)
   - INSERT + UPDATE = INSERT (with final values)
   - UPDATE + UPDATE = single UPDATE (with final values)
   - UPDATE + DELETE = DELETE (with original values)

2. **Trigger.** Run compaction when buffer size exceeds
   `pg_trickle.compact_threshold` (default: 100K rows per buffer) or at a
   configurable interval.

3. **Implementation.** Use a single SQL statement:
   ```sql
   WITH ranked AS (
     SELECT *, ROW_NUMBER() OVER (
       PARTITION BY __pgt_row_id ORDER BY lsn DESC
     ) AS rn
   FROM changes_<oid>
   WHERE lsn >= frontier_lsn
   )
   DELETE FROM changes_<oid> WHERE ctid NOT IN (
     SELECT ctid FROM ranked WHERE rn = 1
   )
   ```

4. **Net-zero detection.** When compaction discovers that a row was inserted
   and then deleted within the same cycle, eliminate both rows — the refresh
   need not process them at all.

**Expected Impact.** For high-churn tables with long refresh intervals:
**50–90% reduction in change buffer size**, proportional reduction in delta
scan time. Major benefit for `join` scenarios where delta size dominates.

**Effort.** 2–3 weeks (compaction logic, trigger integration, concurrency
safety).

**Prior Art.** LSM-tree compaction (RocksDB, LevelDB); Kafka log compaction;
Materialize's compaction of arrangements; HBase major/minor compaction.

**Risk.** Compaction itself has a cost. Must ensure it doesn't run during an
active refresh cycle (use advisory locks). Compaction on UNLOGGED tables is
simpler but loses crash safety for intermediate states.

**⚠️ Risk Analysis — `ctid` is not a stable row identifier; the proposed SQL will corrupt data.**

The DELETE in step 3 uses:
```sql
DELETE FROM changes_<oid> WHERE ctid NOT IN (
  SELECT ctid FROM ranked WHERE rn = 1
)
```
`ctid` is a physical tuple pointer that changes whenever a row is
HOT-updated. If autovacuum or a concurrent session causes any row in the
buffer to be reorganized between the CTE evaluation and the DELETE, the
`ctid` values in `ranked` may no longer point to the same rows. The DELETE
would then silently remove the **wrong** rows — either keeping stale
entries or deleting the freshest entries.

The stable identifier in the change buffer is the `seq` column (the
sequence-generated primary key). The DELETE must be rewritten as:
```sql
DELETE FROM changes_<oid> WHERE seq NOT IN (
  SELECT seq FROM ranked WHERE rn = 1
)
```
or equivalently using a CTE with `RETURNING` to atomically delete and
re-insert, or using `DELETE … USING` with a joined subquery on `seq`.

This is a **data-correctness bug** in the proposed SQL that must be
fixed before implementation begins.

---

## Theme D — Reduce CDC Overhead

These features target the write-side cost of change data capture, reducing the
impact of pg_trickle on OLTP write throughput.

### D-1: UNLOGGED Change Buffers

**Problem.** Change buffer tables are currently logged (WAL-written). Every
INSERT into a change buffer generates WAL records, doubling the WAL volume
for the source table's writes. This is especially painful for high-write
workloads.

**Proposal.** Make change buffer tables UNLOGGED:

1. Create change buffers as `CREATE UNLOGGED TABLE pgtrickle_changes.changes_<oid>`.
2. After a crash, change buffers are empty (PostgreSQL truncates UNLOGGED
   tables on recovery). This is acceptable because:
   - The next refresh cycle will fall back to FULL mode (no changes = no
     frontier advancement, triggering reinit).
   - Stream table data is still intact (it's a regular logged table).
3. Add a `pg_trickle.unlogged_buffers` GUC (default: `false`) to control this — opt-in by operators who have measured WAL pressure and accept the crash/standby tradeoff.

**Expected Impact.** **~30% reduction in WAL volume** from CDC triggers.
For write-heavy workloads (> 10K writes/sec), this translates to measurably
lower replication lag and checkpoint pressure.

**Effort.** 1–2 weeks (DDL changes, crash recovery handling, GUC).

**Prior Art.** PostgreSQL's UNLOGGED tables; Oracle's NOLOGGING mode for
materialized view logs.

**Risk.** After crash, one FULL refresh per ST is required. For large or expensive
stream tables this can be a significant availability event. The GUC defaults to
`false` (logged buffers, crash-safe) so only operators who have explicitly
accepted the tradeoff enable it with `pg_trickle.unlogged_buffers = true`.

**⚠️ Risk Analysis — Standby promotion creates a stale-data window; UNLOGGED migration requires care.**

**Standby promotion.** UNLOGGED tables are never streamed to physical
standbys (PostgreSQL zeroes them on standby startup and crash recovery).
On failover/promotion, the new primary has empty change buffers and will
correctly trigger FULL refresh on the next scheduler tick — this is the
same behaviour as after a crash on the primary, and is acceptable.

However, operators using streaming replication for read scaling (queries
against the standby) will see the stream tables on the standby as
potentially stale if the primary's change buffers haven't been flushed
recently. This is an existing limitation but becomes more visible once the
buffers are UNLOGGED (previously the **in-flight CDC buffer contents** — not
the stream table output — were truncated on crash only; with UNLOGGED buffers
this also happens on any standby restart). In both cases the stream table
output is recovered correctly via FULL refresh on the next scheduler tick:
no committed user data is ever permanently lost. The documentation should
call out the temporary staleness window explicitly.

**Migration for existing installations.** `ALTER TABLE ... SET UNLOGGED`
transparently rewrites the table and acquires `ACCESS EXCLUSIVE`. For
existing change buffer tables, the upgrade migration script must issue
this ALTER per buffer table. During the ALTER window, CDC triggers on
the source table will block — the migration must be scripted to minimize
this window (e.g., run during low-traffic periods or per-table in batches).

---

### D-2: Async CDC via Logical Decoding Output Plugin

**Problem.** The current WAL CDC mode uses `pgoutput` (standard logical
replication protocol) which requires a replication slot and a background
worker polling for changes. The polling interval introduces latency and the
replication slot retains WAL segments.

**Proposal.** Develop a custom logical decoding output plugin
(`pg_trickle_decoder`) that:

1. **Direct buffer writes.** Instead of decoding WAL → logical messages →
   polling → buffer insert, decode WAL directly into the change buffer table
   in a single step within the output plugin callback.
2. **Filtered decoding.** Only decode tables that are sources of active stream
   tables (skip all other tables in the WAL stream). This reduces CPU from
   decoding irrelevant tables.
3. **Batched output.** Accumulate decoded rows and flush in batches of 1000+
   rows using `SPI_exec` within the plugin, amortizing per-row overhead.

**Expected Impact.** **50–80% reduction in WAL decoding CPU** compared to
pgoutput polling. Eliminates the polling latency (changes appear in buffer
within ~10ms of WAL write instead of at the next poll interval).

**Effort.** 6–8 weeks (C-based output plugin, pgrx integration, testing).

**Prior Art.** wal2json, pgoutput, test_decoding (PostgreSQL built-in);
Debezium's custom output plugins; pg_logical's direct decoding.

**Risk.** High complexity. C code in PostgreSQL output plugin requires careful
memory management. Must handle transactions that span multiple WAL segments.
Consider as a v1.0+ feature.

**⚠️ Risk Analysis — SPI writes inside logical decoding `change` callbacks are not supported.**

The proposal states: *"flush in batches … using `SPI_exec` within the plugin"*.
Logical decoding output plugins run in a special decoding context where SPI
is only accessible during the `commit` callback, and even then requires
explicit setup (`SPI_connect` in the right memory context). Calling
`SPI_exec` inside the `change` callback (where individual row changes are
delivered) is not supported and will either crash the backend or produce
corrupted data due to memory context violations.

The correct architecture requires buffering decoded rows in-memory within
the output plugin (using the plugin's `context` memory context), then
flushing to the change buffer table via SPI only in the `commit` callback
after the full transaction is decoded. This means:
- In-memory buffer must handle arbitrarily large transactions without OOM.
- The commit callback must tolerate SPI failures (e.g., if the target
  table was concurrently dropped) without crashing the WAL sender process.

This substantially increases complexity beyond the stated 6–8 week estimate.
The `P4` priority is appropriate; treat as a research spike before committing.

---

### D-3: Write-Batched CDC Triggers

**Problem.** Even with statement-level triggers (v0.4.0), each DML statement
invokes the trigger function once. For COPY operations that insert 100K rows,
the trigger processes all 100K transition-table rows in a single function call,
which is good — but the trigger overhead is still per-statement.

**Proposal.** Optimize the CDC trigger function for batch scenarios:

1. **Bulk COPY path.** Detect when the trigger is processing > 1000 rows
   (via `SELECT count(*) FROM new_table`) and switch to a bulk-optimized
   path:
   ```sql
   INSERT INTO pgtrickle_changes.changes_<oid>
   SELECT nextval('...'), pg_current_wal_lsn(), 'I',
          <pk_cols>, <tracked_cols>
   FROM new_table;
   ```
   Skip per-row hashing and row-ID computation; use a set-returning insert.

2. **Deferred hashing.** For bulk operations, defer content hashing to
   refresh time instead of trigger time. Store raw rows in the change buffer;
   compute hashes lazily when the delta query runs.

3. **Adaptive trigger complexity.** For narrow tables (< 5 columns) with
   simple PKs, use a minimal trigger function that skips JSON serialization
   and stores typed columns directly.

**Expected Impact.** **20–40% reduction in trigger overhead** for bulk DML
operations. Larger gains for wide tables.

**Effort.** 2–3 weeks (trigger function variants, adaptive selection).

**Prior Art.** pgaudit's minimal-overhead logging; Debezium's batched
snapshot mode.

**⚠️ Risk Analysis — Deferred hashing breaks the change buffer schema and differential refresh path.**

Point 1's "bulk COPY path" suffix has the same SPI infeasibility as A-3c
(`COPY FROM` cannot read from a subquery via SPI — see A-3 analysis).
The set-returning INSERT (`INSERT INTO changes … SELECT … FROM new_table`)
is already how current statement-level triggers work, so this is not a
new optimization.

More significantly, point 2 (deferred hashing) would be a **breaking schema
change**. The change buffer currently stores `__pgt_row_id` (a pre-computed
content hash) as the primary join key used by MERGE in differential refresh.
Removing it from the buffer requires:
- Altering every existing `changes_<oid>` table to drop the column.
- Rewriting the differential refresh delta-scan CTE to recompute the hash
  from raw column values at query time.
- Invalidating the C-4 compaction proposal (which partitions by `__pgt_row_id`).
- The 2–3 week estimate does not account for any of this.

The safe sub-path here is point 3 (adaptive trigger complexity for narrow
tables) only — it changes the trigger function body without altering the
buffer schema.

---

### D-4: Shared Change Buffers (Multi-ST Deduplication)

**Problem.** When multiple stream tables reference the same source table,
each has its own change buffer. A single INSERT into the source table triggers
N independent change buffer writes (one per dependent ST). For 10 STs
referencing the same source, write amplification is 10×.

**Proposal.** Share change buffers across STs that reference the same source:

1. **Single buffer per source.** Instead of `changes_<st_oid>`, use
   `changes_<source_oid>` (already partially implemented in CDC mode).
2. **Reference counting.** Track which STs have consumed changes via the
   frontier (already tracked per-ST in `pgt_dependencies.decoder_confirmed_lsn`).
3. **Cleanup coordination.** Only delete change buffer rows when ALL
   dependent STs have advanced their frontier past them.
4. **Column superset.** The shared buffer stores the union of columns needed
   by all dependent STs. Each ST's delta scan projects only the columns it
   needs.

**Expected Impact.** For N STs referencing the same source:
**N× reduction in change buffer write volume**, proportional reduction in
trigger overhead and WAL volume.

**Effort.** 3–4 weeks (buffer sharing logic, multi-frontier cleanup,
column superset management).

**Prior Art.** Kafka's shared topics with consumer groups;
Materialize's shared arrangements; Flink's shared source state.

**⚠️ Risk Analysis — Column superset schema requires ALTER TABLE when STs are added or removed.**

The shared buffer stores the union of columns needed by all dependent STs.
When a new ST is created that needs a column not yet in the superset, the
buffer table must be altered (`ALTER TABLE … ADD COLUMN`), which requires
`ACCESS EXCLUSIVE` on the change buffer table and will briefly block CDC
trigger insertions into it.

Furthermore, a reference-counting mechanism per column is needed: a column
can only be dropped from the superset when no remaining ST depends on it.
Adding/dropping STs must atomically update both the column reference counts
and the trigger function body. This is not mentioned in the proposal but is
a necessary part of the implementation.

For deployments that frequently CREATE/DROP stream tables referencing the
same source, this causes repeated ALTER TABLE cycles on a hot, actively-
written table. Recommend gating this feature on a static-superset mode
(columns fixed at first ST creation; never dropped) for the initial
implementation, with dynamic column management as a follow-on.

---

## Prioritization Matrix

| ID | Feature | Impact | Effort | Risk | Priority |
|----|---------|--------|--------|------|----------|
| D-1 | UNLOGGED Change Buffers | Medium | 1–2 wk | Low | **P0** |
| A-3a | MERGE Bypass — Append-Only INSERT | High | 1–2 wk | Low | **P0** |
| A-3b | MERGE Bypass — TopK TRUNCATE | Low | 2–3 wk | High | **Won't Do** |
| A-3c | MERGE Bypass — Bulk COPY | Low | 4–6 wk | High | **Won't Do** |
| B-2 | Delta Predicate Pushdown | High | 2–3 wk | Low | **P1** |
| B-4 | Cost-Based Refresh Strategy | Medium | 2–3 wk | Low | **P1** |
| A-2 | Columnar Change Tracking | High | 3–4 wk | Medium | **P1** |
| C-4 | Change Buffer Compaction | High | 2–3 wk | Medium | **P1** |
| D-4 | Shared Change Buffers | High | 3–4 wk | Medium | **P2** |
| A-4 | Index-Aware MERGE Planning | Medium | 1–2 wk | Low | **P2** |
| C-1 | Tiered Refresh Scheduling | High | 3–4 wk | Medium | **P2** |
| B-1 | Incremental Aggregate Maintenance | Very High | 4–6 wk | High | **P2** |
| A-1 | Partitioned Stream Tables | Very High | 3–5 wk | **Very High** | **P3** |
| C-2 | Incremental DAG Rebuild | Medium | 2–3 wk | **Medium** | **P3** |
| C-3 | Multi-DB Scheduler Isolation | Medium | 2–3 wk | Low | **P3** |
| B-3 | Multi-Table Delta Batching | High | 4–6 wk | **Very High** | **P3** |
| D-2 | Async CDC Output Plugin | High | 6–8 wk | **Very High** | **P4** |
| D-3 | Write-Batched CDC Triggers | **Low** | 2–3 wk | **Medium** | **P3** |

### Recommended Implementation Order

**Wave 1 (Quick Wins — v0.5.0):**
- A-3a: MERGE Bypass — Append-Only INSERT path only (1–2 wk, low risk)
  - Expose `APPEND ONLY` declaration on `CREATE STREAM TABLE`
  - CDC heuristic fallback: use fast path until first DELETE/UPDATE seen
  - **Not** the TopK TRUNCATE sub-path (not worth doing — see A-3 analysis)
  - **Not** the Bulk COPY sub-path (infeasible via SPI — see A-3 analysis)

**Wave 2 (Core Optimizations — v0.6.0):**
- A-4: Index-Aware MERGE Planning (deferred from v0.5.0 — blunt `enable_seqscan` hint; better after PG18 hint infra is stable)
- B-2: Delta Predicate Pushdown (deferred from v0.5.0 — DVM engine correctness risk; requires dedicated wave)
- C-4: Change Buffer Compaction (deferred from v0.5.0 — advisory lock races + `ctid` bug; fix `ctid` → `seq` before implementing)
- B-4: Cost-Based Refresh Strategy Selection

**Wave 3 (Scalability — v0.7.0):**
- A-2: Columnar Change Tracking
- C-1: Tiered Refresh Scheduling
- D-4: Shared Change Buffers

**Wave 4 (Advanced — v0.8.0+):**
- B-1: Incremental Aggregate Maintenance
- A-1: Partitioned Stream Tables
- B-3: Multi-Table Delta Batching
- C-2: Incremental DAG Rebuild
- C-3: Multi-DB Scheduler Isolation
- D-2: Custom Logical Decoding Output Plugin

---

## Competitive Positioning

These features, if implemented, would give pg_trickle significant advantages
over every competing system:

| Feature | pg_trickle (proposed) | Feldera | Epsio | pg_ivm |
|---------|----------------------|---------|-------|--------|
| Algebraic aggregate maint. | ✅ (B-1) | ✅ (native) | Partial | ❌ |
| Predicate pushdown in delta | ✅ (B-2) | ✅ (native) | Unknown | ❌ |
| Cost-based strategy | ✅ (B-4) | N/A (streaming) | N/A | ❌ |
| Partitioned targets | ✅ (A-1) | N/A | ❌ | ❌ |
| Column-scoped delta | ✅ (A-2) | Partial | ❌ | ❌ |
| Change buffer compaction | ✅ (C-4) | ✅ (native) | Unknown | N/A |
| Tiered scheduling | ✅ (C-1) | N/A | N/A | N/A |
| UNLOGGED buffers | ✅ (D-1) | N/A | N/A | N/A |
| Shared change buffers | ✅ (D-4) | ✅ (native) | Unknown | N/A |
| Custom WAL decoder | ✅ (D-2) | N/A | ✅ | ❌ |

The combination of **embedded PostgreSQL architecture** (zero external
dependencies) + **comprehensive SQL coverage** (39+ aggregates, full window
functions) + **these optimizations** would make pg_trickle the most capable
IVM system available for PostgreSQL.

---

## References

1. Budiu et al., "DBSP: Automatic Incremental View Maintenance," VLDB 2023
2. Koch et al., "DBToaster: Higher-Order Delta Processing for Dynamic,
   Frequently Fresh Views," VLDB Journal 2014
3. Gupta & Mumick, "Maintenance of Materialized Views: Problems, Techniques,
   and Applications," IEEE Data Engineering Bulletin 1995
4. Gjengset et al., "Noria: Dynamic, Partially-Stateful Data-Flow for
   High-Performance Web Applications," OSDI 2018
5. Abadi et al., "Materialize: A Streaming Database," CIDR 2023
6. McSherry et al., "Differential Dataflow," CIDR 2013
7. Nikolic et al., "LINVIEW: Incremental View Maintenance for Complex
   Analytical Queries," SIGMOD 2014
8. Chirkova & Yang, "Materialized Views," Foundations and Trends in
   Databases 2012
9. Oracle, "Materialized View Refresh: Fast, Complete, Force, and
   On-Demand," Oracle Database Data Warehousing Guide
10. PostgreSQL 18, "CREATE MATERIALIZED VIEW / REFRESH MATERIALIZED VIEW"
