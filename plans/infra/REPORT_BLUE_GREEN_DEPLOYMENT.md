# Blue-Green Deployment for pg_trickle

**Type:** REPORT · **Status:** Exploration · **Date:** 2026-03-03

## Problem Statement

Propagating changes through a large DAG of stream tables can take significant
time — especially initial `FULL` refreshes on wide or high-cardinality tables.
When users need to modify a pipeline (new query logic, schema changes, operator
upgrades, schedule tuning), the existing pipeline must keep serving
stale-but-consistent data while the replacement pipeline catches up in the
background. Once the new pipeline is current, a hot swap redirects consumers
transparently.

This is analogous to blue-green deployment in application servers, applied to
incremental view maintenance pipelines.

### Use Cases

1. **Query logic change** — rewrite a defining query (e.g., add a new join or
   aggregation) without downtime.
2. **Schema evolution** — upstream `ALTER TABLE` requires rebuilding downstream
   STs; keep the old pipeline serving while the new one reinitializes.
3. **Refresh mode migration** — switch from `FULL` to `DIFFERENTIAL` (or vice
   versa) with a safety net.
4. **DAG restructuring** — split, merge, or reorder stream tables in the
   dependency graph.
5. **Extension upgrade** — new pg_trickle version with different DVM operators
   needs pipeline rebuild.

### Design Goals

| Goal | Priority |
|------|----------|
| Zero (or near-zero) read downtime during swap | Must-have |
| Works for individual STs and whole pipelines | Must-have |
| Automatic convergence detection + manual override | Must-have |
| Rollback capability | Should-have |
| No breaking changes to existing catalog schema | Should-have |
| Minimal scheduler overhead during dual-pipeline phase | Nice-to-have |

---

## Current Architecture — What Already Supports Blue-Green

A thorough analysis of the existing internals reveals that pg_trickle is
**already largely blue-green friendly** at the infrastructure level.

### CDC Sharing (Ready)

Triggers and change buffer tables are keyed by **source OID**, not by stream
table ID. The `pgtrickle.pgt_change_tracking.tracked_by_pgt_ids` array
supports multiple consumers. Both the blue (existing) and green (new) STs can
consume from the same change buffers simultaneously with independent frontiers.

- Trigger naming: `pg_trickle_cdc_{source_oid}` — one trigger per source
  regardless of how many STs depend on it.
- Buffer table: `pgtrickle_changes.changes_{source_oid}` — shared.
- Deferred cleanup uses `MIN(frontier)` across all consumers, so green's
  presence prevents premature deletion of rows it hasn't consumed yet.

### Independent Frontiers (Ready)

Each ST has its own `frontier` JSONB column in `pgt_stream_tables` tracking
per-source LSN watermarks. A green ST with a different `pgt_id` maintains
completely independent frontier state.

### Internal Key Isolation (Ready)

- **MERGE template cache** — keyed by `pgt_id` (thread-local `HashMap`).
- **Prepared statements** — per-session, keyed by `pgt_id`.
- **Advisory locks** — keyed by `pgt_id`.
- **Retry/backoff state** — keyed by `pgt_id`.

Two versions of the same logical ST with different `pgt_id` values will not
collide in any internal data structure.

### DAG Rebuild (Ready)

`signal_dag_rebuild()` is a cheap atomic increment. The scheduler rebuilds the
full DAG from catalog tables on the next cycle, so adding/removing STs is a
live operation with no restart required.

### Status Lifecycle (Ready)

The `INITIALIZING → ACTIVE → SUSPENDED → ERROR` state machine exists. A green
ST can be created in `INITIALIZING` state, manually refreshed to catch up, then
flipped to `ACTIVE` for the scheduler to pick up.

---

## Core Challenge: Identity & Naming

The central tension is the **UNIQUE constraint** on
`(pgt_schema, pgt_name)` in `pgt_stream_tables` and the **UNIQUE on
`pgt_relid`** (storage table OID). Two STs cannot share the same qualified name
or point to the same physical table. The swap mechanism must navigate this.

---

## Approaches

### Approach A: Shadow Tables with Atomic Rename

**Concept:** Create the green ST with a shadow name (e.g.,
`order_totals__pgt_green`), let it catch up, then do an atomic three-way rename
in a single transaction.

**Lifecycle:**

```
1. User calls:  pgtrickle.create_green('order_totals', query => '...', ...)
2. System creates ST:  order_totals__pgt_green  (status: INITIALIZING)
3. System runs FULL refresh on green ST
4. Green ST flips to ACTIVE — scheduler runs differentials alongside blue
5. Convergence detected (or user calls promote)
6. In one transaction:
     ALTER TABLE order_totals         RENAME TO order_totals__pgt_retiring;
     ALTER TABLE order_totals__pgt_green RENAME TO order_totals;
     UPDATE pgtrickle.pgt_stream_tables ...  -- fix names, relids
     DROP the retired ST (or keep for rollback)
7. Signal DAG rebuild
```

**Pros:**
- Simple mental model — two STs, one rename.
- Consumers querying `SELECT * FROM order_totals` see no change.
- Minimal catalog schema changes (only need a `green_of BIGINT` FK column).
- Works for both per-ST and whole-pipeline swaps.

**Cons:**
- `ALTER TABLE ... RENAME` takes `AccessExclusiveLock` — brief (sub-ms
  catalog-only operation) but blocks concurrent queries momentarily.
- For whole-pipeline swap of N STs, N renames in one transaction — lock
  ordering must be deterministic to avoid deadlocks.
- Downstream STs referencing the blue ST by OID in their `defining_query` need
  query text rewriting at promote time.
- Not truly zero-downtime: there is a lock window (typically < 1ms per table).

**Complexity:** Low–Medium.

---

### Approach B: Views as Public Interface

**Concept:** The public-facing name is always a **view**. The physical storage
table has a versioned name. Swap = `CREATE OR REPLACE VIEW` pointing to the new
storage table.

**Lifecycle:**

```
1. On initial ST creation:
     Storage table:  order_totals__pgt_v1
     Public view:    CREATE VIEW order_totals AS SELECT * FROM order_totals__pgt_v1;
2. Green version:    order_totals__pgt_v2  (new ST, separate pgt_id)
3. Once caught up:   CREATE OR REPLACE VIEW order_totals AS SELECT * FROM order_totals__pgt_v2;
4. Drop v1 ST (or retain for rollback)
```

**Pros:**
- `CREATE OR REPLACE VIEW` is lightweight, faster than rename.
- Clean separation: logical name (view) vs physical storage (versioned table).
- Rollback = point the view back.
- View replacement doesn't invalidate open cursors on the old version.

**Cons:**
- **Breaking change for existing deployments** — all STs would now live behind
  views. This changes behavior for `INSERT` (views need `INSTEAD OF` triggers
  or are read-only), `\d` output, `pg_class` catalogue lookups, index
  visibility from `pg_indexes`, etc.
- Adds an indirection layer to every read query (minor but measurable on
  micro-benchmarks).
- Downstream ST `defining_query` that references the view gets rewritten via
  the existing view-inlining code in `api.rs` — the inliner resolves views to
  their base query, so it would see the underlying versioned table. This
  creates a coupling: green STs depending on another green ST would need their
  view pointer updated first (ordering constraint).
- More complex catalog model: need to track both view OID and storage table
  OID per entry.

**Complexity:** Medium–High. The breaking-change aspect makes this unsuitable
as a retrofit unless pg_trickle adopts views-as-public-interface from the start
(e.g., as a major version change).

---

### Approach C: Pipeline Generations (First-Class Concept)

**Concept:** Introduce a "pipeline" entity with a generation counter. STs
belong to a pipeline + generation. Only one generation is "active" at a time.

**New catalog table:**

```sql
CREATE TABLE pgtrickle.pgt_pipelines (
    pipeline_id    BIGSERIAL PRIMARY KEY,
    pipeline_name  TEXT UNIQUE NOT NULL,
    active_gen     INT NOT NULL DEFAULT 1,
    green_gen      INT,        -- NULL when no green is being prepared
    created_at     TIMESTAMPTZ DEFAULT now()
);
```

**Extended `pgt_stream_tables`:**

```sql
ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN pipeline_id  BIGINT REFERENCES pgtrickle.pgt_pipelines,
    ADD COLUMN generation   INT;
```

The UNIQUE constraint on `(pgt_schema, pgt_name)` would be relaxed to
`UNIQUE (pgt_schema, pgt_name, generation)`.

**Lifecycle:**

```
1. pgtrickle.begin_green('my_pipeline')
   → clones all STs in the pipeline with generation + 1
   → new storage tables, new pgt_ids, inherits queries/config
   → green STs start catching up (FULL → DIFFERENTIAL)

2. pgtrickle.promote_green('my_pipeline')
   → atomically swap active_gen, rename tables, update references
   → mark old generation as retired

3. pgtrickle.rollback_green('my_pipeline')
   → drop green-generation STs, reset green_gen to NULL

4. pgtrickle.cleanup_generation('my_pipeline', gen)
   → drop retired storage tables and catalog entries
```

**Pros:**
- First-class concept with clear semantics and good observability.
- `pgt_status()` can show generation info: "green is 95% caught up."
- Supports both whole-pipeline and per-ST swaps (single-ST "pipeline").
- Enables rollback without keeping shadow tables.
- Generation info is queryable — useful for monitoring and alerting.

**Cons:**
- Largest schema change: new table, altered `pgt_stream_tables`, migration
  path for existing deployments, extension upgrade SQL.
- "Pipeline" grouping may not map cleanly to arbitrary DAG subsets — what if
  a user wants to green-deploy 3 of 10 STs that share sources?
- The rename/swap mechanics under the hood are the same as Approach A — this
  is essentially Approach A with richer metadata.
- More concepts for users to learn.

**Complexity:** High, but most comprehensive.

---

### Approach D: Schema-Based Isolation

**Concept:** Blue pipeline lives in the user's schema. Green pipeline lives in
a staging schema (e.g., `pgtrickle_staging`). Swap = rename schemas or adjust
`search_path`.

**Lifecycle:**

```
1. pgtrickle.prepare_green(staging_schema => 'pgtrickle_staging')
   → recreate all STs in the staging schema
2. Green STs catch up in isolation
3. ALTER SCHEMA public RENAME TO public_old;
   ALTER SCHEMA pgtrickle_staging RENAME TO public;
4. Drop old schema
```

**Pros:**
- Complete isolation during build-up.
- Schema rename is a single catalog operation (fast).

**Cons:**
- Very coarse-grained — swaps the **entire schema**, not just STs.
- Source tables in the original schema → cross-schema query references.
- `ALTER SCHEMA ... RENAME` is `AccessExclusiveLock` on all contained objects.
- Doesn't support per-ST granularity at all.
- Application `search_path` changes required.

**Complexity:** Medium, but too inflexible for most scenarios.

---

## Comparison Matrix

| Criterion | A: Shadow Rename | B: Views | C: Generations | D: Schema |
|-----------|:---:|:---:|:---:|:---:|
| Per-ST granularity | ✅ | ✅ | ✅ | ❌ |
| Whole-pipeline swap | ✅ | ✅ | ✅ | ✅ |
| No breaking changes | ✅ | ❌ | ⚠️ Schema migration | ✅ |
| Rollback support | ⚠️ Manual | ✅ Easy | ✅ Built-in | ⚠️ Manual |
| Lock duration | ~ms | ~0 | ~ms | ~ms × N |
| Implementation effort | Low | Medium-High | High | Medium |
| Observability | Basic | Basic | Rich | Basic |
| Downstream query rewrite | Required | Automatic (via view) | Required | Not needed |
| Catalog schema changes | Minimal | Moderate | Significant | None |

---

## Convergence Detection

For auto-swap, the system needs to determine when the green pipeline has
"caught up" to the blue pipeline. Options:

### 1. Frontier LSN Comparison (Recommended)

Compare `green.frontier.sources[oid].lsn` ≥ `blue.frontier.sources[oid].lsn`
for all shared sources. When all sources pass, green is at least as fresh as
blue.

- **Pro:** Already tracked, no extra queries, precise.
- **Con:** Doesn't guarantee data equivalence (different query logic produces
  different results even at the same LSN).

### 2. Data Timestamp Comparison

`green.data_timestamp ≥ blue.data_timestamp` — simpler but coarser.

- **Pro:** Single scalar comparison.
- **Con:** Timestamp resolution may not match LSN precision.

### 3. Lag Threshold

User-defined maximum acceptable lag: "swap when green is within 5 seconds of
blue." Useful for time-sensitive workloads where exact convergence isn't
required.

### 4. Row Count / Content Hash

Compare `COUNT(*)` or `pg_trickle.content_hash(table)` between blue and green
storage tables.

- **Pro:** Deterministic proof of equivalence.
- **Con:** Expensive on large tables; only meaningful when queries are identical.

**Recommendation:** Use **frontier LSN comparison** (#1) as the primary
mechanism, with an optional **lag threshold** (#3) for user control. Content
hash (#4) can be offered as a `verify` flag for cautious deployments.

---

## Recommendation

**Approach A (Shadow Tables with Atomic Rename)** as the implementation
strategy, augmented with lightweight metadata from **Approach C** for
observability.

### Rationale

- Approach A is the **lowest-risk, lowest-effort** path that solves all
  must-have requirements.
- The internal machinery (CDC sharing, independent frontiers, `pgt_id`
  isolation) already supports dual pipelines — what's missing is just the
  orchestration layer.
- Adding a `green_of BIGINT` FK and `pipeline_group TEXT` column to
  `pgt_stream_tables` gives the observability benefits of Approach C without
  the full pipeline entity overhead.
- Approach B (views) would be a better long-term architecture but is a
  breaking change that warrants its own migration plan.
- Approach D is too coarse for real-world use.

### Proposed API Surface

```sql
-- Create a green version of an existing stream table.
-- Inherits query/schedule/mode from blue unless overridden.
SELECT pgtrickle.create_green(
    'public.order_totals',
    query    => 'SELECT ... FROM orders JOIN ...',  -- optional override
    schedule => '10s',                               -- optional override
    mode     => 'DIFFERENTIAL'                       -- optional override
);

-- Check green progress: frontier lag, row count, status.
SELECT * FROM pgtrickle.green_status('public.order_totals');
-- Returns: green_name, status, frontier_lag_bytes, data_timestamp_lag,
--          blue_row_count, green_row_count, converged (bool)

-- Promote: atomic swap green → active, retire blue.
-- Fails if green hasn't converged (override with force => true).
SELECT pgtrickle.promote_green('public.order_totals');
SELECT pgtrickle.promote_green('public.order_totals', force => true);

-- Promote all green STs that have converged.
SELECT * FROM pgtrickle.promote_all_green();

-- Abort: drop green ST, clean up.
SELECT pgtrickle.cancel_green('public.order_totals');

-- Rollback: swap back to the retired blue (if not yet cleaned up).
SELECT pgtrickle.rollback_green('public.order_totals');
```

### Catalog Changes (Minimal)

```sql
ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN green_of       BIGINT REFERENCES pgtrickle.pgt_stream_tables(pgt_id),
    ADD COLUMN pipeline_group TEXT;
```

- `green_of` — points to the blue ST's `pgt_id`. `NULL` for normal STs.
- `pipeline_group` — optional label for grouping multi-ST swaps.

### Promote Transaction (Pseudocode)

```sql
BEGIN;

-- 1. Lock both tables deterministically (by OID, ascending) to avoid deadlocks
LOCK TABLE blue_table IN ACCESS EXCLUSIVE MODE;
LOCK TABLE green_table IN ACCESS EXCLUSIVE MODE;

-- 2. Rename
ALTER TABLE blue_table  RENAME TO order_totals__pgt_retired;
ALTER TABLE green_table RENAME TO order_totals;

-- 3. Update catalog
UPDATE pgtrickle.pgt_stream_tables
   SET pgt_name = 'order_totals__pgt_retired',
       status   = 'SUSPENDED'
 WHERE pgt_id   = :blue_id;

UPDATE pgtrickle.pgt_stream_tables
   SET pgt_name  = 'order_totals',
       green_of  = NULL
 WHERE pgt_id    = :green_id;

-- 4. Rewrite defining_query of any downstream STs that reference
--    the old blue OID → new green OID
UPDATE pgtrickle.pgt_dependencies
   SET source_relid = :green_oid
 WHERE source_relid = :blue_oid;

-- 5. Signal DAG rebuild
SELECT pgtrickle.signal_dag_rebuild();

COMMIT;
```

The blue ST remains in the catalog as `SUSPENDED` until explicitly cleaned up,
enabling rollback.

---

## Open Questions

1. **Downstream OID rewrites.** When a green ST replaces a blue ST that other
   STs depend on, the downstream `defining_query` text contains the old OID (or
   table name). How deep does the rewrite need to go? Just the dependency
   edges, or the SQL text too? (The SQL text uses names, not OIDs, so renaming
   the table should suffice — but needs verification.)

2. **Consistency groups during transition.** If blue and green STs participate
   in the same diamond consistency group, could the scheduler produce
   inconsistent results? Likely no — they have independent `pgt_id`s and
   different names, so the consistency group logic treats them as separate
   entities. But edge cases need analysis.

3. **Whole-pipeline ordering during promote.** For a pipeline with 5 STs in a
   DAG, the promote must happen in topological order (upstream first) so that
   downstream STs immediately see the promoted upstream. Or should the promote
   be all-at-once in a single transaction?

4. **CDC trigger sharing with modified queries.** If the green ST has a
   different `defining_query` that references *additional* source tables not in
   the blue ST's dependency set, new CDC triggers must be created for those
   sources. The existing `setup_cdc_for_source()` handles this, but the green
   ST creation flow needs to call it.

5. **Storage table column mismatch.** If the green ST has a different column
   set (due to query changes), the rename alone isn't sufficient — downstream
   queries expecting the blue schema will break. Should `promote_green` verify
   schema compatibility, or is this the user's responsibility?

6. **Advisor lock contention.** Both blue and green STs use independent
   advisory locks (keyed by `pgt_id`), so no contention. But during the promote
   transaction, we need to ensure neither ST is mid-refresh. The promote should
   acquire both advisory locks before proceeding.

7. **Auto-promote timing.** Should the scheduler perform auto-promote, or
   should it be a separate background check? Embedding it in the scheduler
   loop adds complexity; a separate polling function called by cron may be
   simpler.

---

## Related Work

- [PLAN_UPGRADE_MIGRATIONS.md](../sql/PLAN_UPGRADE_MIGRATIONS.md) — extension
  upgrade migrations (overlapping concern for version-to-version transitions).
- Implemented CDC mode transitions
  already implement a form of "hot swap" from trigger to WAL.
- [PLAN_FUSE.md](../sql/PLAN_FUSE.md) — anomalous change volume detection
  could trigger auto-rollback of a green pipeline.
- [REPORT_EXTERNAL_PROCESS.md](REPORT_EXTERNAL_PROCESS.md) — external sidecar
  could orchestrate blue-green at a higher level.
- [REPORT_DOWNSTREAM_CONSUMERS.md](REPORT_DOWNSTREAM_CONSUMERS.md) — downstream
  consumer patterns (change feeds, NOTIFY, logical replication) and how they
  interact with blue-green swap.

---

## Next Steps

1. **Validate Approach A** with a manual proof-of-concept: create two STs on
   the same source, let both catch up, then rename in a transaction. Measure
   lock duration and verify downstream queries.
2. **Prototype `create_green` / `promote_green`** as SQL functions — minimal
   viable implementation.
3. **Design the convergence check** — implement `green_status()` using frontier
   comparison.
4. **Evaluate Approach B (views)** as a longer-term migration path, potentially
   gated behind a GUC (`pg_trickle.use_view_aliases = on`).
5. **Write an ADR** once we commit to an approach.
