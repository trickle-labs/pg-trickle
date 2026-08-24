# Upgrading pg_trickle

This guide covers upgrading pg_trickle from one version to another.

## 0.87.4 → 0.87.5

v0.87.5 makes no catalog or SQL API changes. Install the new extension files
and run ALTER EXTENSION pg_trickle UPDATE; the migration is intentionally empty
because this release adds DVM schema contracts, structured snapshot planning,
decision tracing, and semantic coverage checks.

## 0.87.3 → 0.87.4

v0.87.4 makes no catalog or SQL API changes. Install the new extension files
and run ALTER EXTENSION pg_trickle UPDATE; the migration is intentionally empty
because this release adds state-directed and metamorphic DVM correctness tests.

## 0.87.2 → 0.87.3

v0.87.3 makes no catalog or SQL API changes. Install the new extension files
and run ALTER EXTENSION pg_trickle UPDATE; the migration is intentionally empty
because this release adds composition-aware differential correctness coverage.

## 0.87.1 → 0.87.2

v0.87.2 makes no catalog or SQL API changes. Install the new extension files
and run ALTER EXTENSION pg_trickle UPDATE; the migration is intentionally empty
because this release adds deterministic DVM replay and regression testing
infrastructure.

## 0.86.0 → 0.87.0

v0.87.0 keeps the existing stream-table and change-buffer schema. Large
ordinary differential MERGE deltas use the bounded portal pipeline; small
proven non-amplifying deltas remain direct. `pg_trickle.pipeline_batch_size`
defaults to 4096. `pg_trickle.merge_batch_size` is a deprecated alias for the
same setting, now meaning apply-batch size rather than a materialization
threshold, and is scheduled for removal in v0.88.

Pipeline batches use internal savepoints inside one outer refresh transaction.
They do not become visible to other sessions until the outer transaction
commits, and they do not release row locks early. Logged change-buffer
durability remains the default. `pg_trickle.memory_budget_mb` defaults to 256
MiB and bounds pg_trickle-owned accumulations; it is not a PostgreSQL-wide RSS
limit, and the lossless change-buffer guard never drops committed rows.

---

## Quick Upgrade (Recommended)

```sql
-- 1. Check current version
SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle';

-- 2. Replace the binary files (.so/.dylib, .control, .sql)
--    See the installation method below for your platform.

-- 3. Restart PostgreSQL (required for shared library changes)
--    sudo systemctl restart postgresql

-- 4. Run the upgrade in each database that has pg_trickle installed
ALTER EXTENSION pg_trickle UPDATE;

-- 5. Verify the upgrade
SELECT pgtrickle.version();
SELECT * FROM pgtrickle.health_check();
```

---

## Step-by-Step Instructions

### 1. Check Current Version

```sql
SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle';
-- Returns your current installed version, e.g. '0.9.0'
```

### 2. Install New Binary Files

Replace the extension files in your PostgreSQL installation directory.
The method depends on how you originally installed pg_trickle.

**From release tarball:**

```bash
# Replace <new-version> with the target release, for example 0.2.3
curl -LO https://github.com/getretake/pg_trickle/releases/download/v<new-version>/pg_trickle-<new-version>-pg18-linux-amd64.tar.gz
tar xzf pg_trickle-<new-version>-pg18-linux-amd64.tar.gz

# Copy files to PostgreSQL directories
sudo cp pg_trickle-<new-version>-pg18-linux-amd64/lib/* $(pg_config --pkglibdir)/
sudo cp pg_trickle-<new-version>-pg18-linux-amd64/extension/* $(pg_config --sharedir)/extension/
```

**From source (cargo-pgrx):**

```bash
cargo pgrx install --release
```

### 3. Restart PostgreSQL

The shared library (`.so` / `.dylib`) is loaded at server start via
`shared_preload_libraries`. A restart is required for the new binary to
take effect.

```bash
sudo systemctl restart postgresql
# or on macOS with Homebrew:
brew services restart postgresql@18
```

### 4. Run ALTER EXTENSION UPDATE

Connect to **each database** where pg_trickle is installed and run:

```sql
ALTER EXTENSION pg_trickle UPDATE;
```

This executes the upgrade migration scripts in order (for example,
`pg_trickle--0.5.0--0.6.0.sql` → `pg_trickle--0.6.0--0.7.0.sql`).
PostgreSQL automatically determines the full upgrade chain from your current
version to the new `default_version`.

### 0.82.0 → 0.83.0 row-identity rebuild

The migration adds `row_identity_version` to `pgt_stream_tables` and
`pgt_change_buffers`, creates the durable private set-operation registry, and
locks tracked source/storage relations in OID order. Pending CDC rows are
discarded except for each buffer's sentinel, then the buffers are marked
version `2`; the migration rolls back if a registered buffer is missing.
Existing stream tables are marked `needs_reinit` with legacy version `1`.

The next protected FULL refresh rebuilds each table from source data and marks
its row identity version `2`. Incremental refresh remains blocked until that
reinitialization succeeds. `INTERSECT` and `EXCEPT` continue to use FULL/AUTO
until their private multiplicity state is admitted by the semantic gate.

### 0.83.0 → 0.84.0 catalog and privilege integrity

This hop repairs migration-era catalogs and installs a deny-first function ACL
policy. Before upgrading, inspect roles that rely on PostgreSQL's default
`PUBLIC EXECUTE` privilege and generate exact grants from
`scripts/sql_api_policy.json`; do not replace them with
`GRANT EXECUTE ON ALL FUNCTIONS`.

Run the preflight checks in every database:

```sql
SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle';
SELECT pgtrickle.migrate(); -- read-only diagnostic
SELECT snapshot_id, snapshot_schema, snapshot_table
FROM pgtrickle.pgt_snapshots
WHERE snapshot_relid IS NULL OR provenance_token IS NULL;
```

Unresolved legacy snapshots remain cataloged but cannot be restored or dropped
until their relation identity and provenance are repaired. Invalid numeric
configuration (including NaN/infinity) and ambiguous ownership are rejected;
the migration never clamps or guesses values.

Install the released 0.83.0 shared library and SQL together, restart
PostgreSQL, then run `ALTER EXTENSION pg_trickle UPDATE` before switching to
the 0.84.0 package. Verify that `pgtrickle.version()`,
`pg_extension.extversion`, and the latest migration audit row all report
`0.84.0`. After a logical restore, keep scheduling disabled until
`pgtrickle.restore_stream_tables()` completes protected FULL baselines and
post-restore DML converges with each defining query.

### 0.84.0 → 0.85.0 scheduler and resource resilience

Install the 0.85.0 shared library and SQL together, restart PostgreSQL to
initialize the new shared-memory layout, then run:

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.85.0';
```

The migration adds typed refresh outcomes, worker-generation tracking, and
self-healing state to the scheduler catalogs. Drain requests now remain active
after completion or timeout; resume dispatch explicitly with:

```sql
SELECT pgtrickle.resume_after_drain();
```

Before upgrading, review parser, bulk-control, and metrics settings against the
new finite limits. `is_drained()` returns `NULL` before the first drain
request, and metrics remain loopback-bound unless
`pg_trickle.metrics_bind_address` is configured explicitly.

### 5. Verify the Upgrade

```sql
-- Check version
SELECT pgtrickle.version();

-- Run health check
SELECT * FROM pgtrickle.health_check();

-- Verify stream tables are intact
SELECT * FROM pgtrickle.stream_tables_info;

-- Test a refresh
SELECT pgtrickle.refresh_stream_table('your_stream_table');
```

---

## Version-Specific Notes

### 0.1.3 → 0.2.0

**New functions added:**
- `pgtrickle.list_sources(name)` — list source tables for a stream table
- `pgtrickle.change_buffer_sizes()` — inspect CDC change buffer sizes
- `pgtrickle.health_check()` — diagnostic health checks
- `pgtrickle.dependency_tree()` — visualize the dependency DAG
- `pgtrickle.trigger_inventory()` — audit CDC triggers
- `pgtrickle.refresh_timeline(max_rows)` — refresh history
- `pgtrickle.diamond_groups()` — diamond dependency group info
- `pgtrickle.version()` — extension version string
- `pgtrickle.pgt_ivm_apply_delta(...)` — internal IVM delta application
- `pgtrickle.pgt_ivm_handle_truncate(...)` — internal TRUNCATE handler
- `pgtrickle._signal_launcher_rescan()` — internal launcher signal

**No schema changes** to `pgtrickle.pgt_stream_tables` or
`pgtrickle.pgt_dependencies` catalog tables.

**No breaking changes.** All v0.1.3 functions and views continue to work
as before.

### 0.2.0 → 0.2.1

**Three new catalog columns** added to `pgtrickle.pgt_stream_tables`:

| Column | Type | Default | Purpose |
|--------|------|---------|--------|
| `topk_offset` | `INT` | `NULL` | Pre-provisioned for paged TopK OFFSET (activated in v0.2.2) |
| `has_keyless_source` | `BOOLEAN NOT NULL` | `FALSE` | EC-06: keyless source flag; switches apply strategy from MERGE to counted DELETE |
| `function_hashes` | `TEXT` | `NULL` | EC-16: stores MD5 hashes of referenced function bodies for change detection |

The migration script (`pg_trickle--0.2.0--0.2.1.sql`) adds these columns
via `ALTER TABLE … ADD COLUMN IF NOT EXISTS`.

**No breaking changes.** All v0.2.0 functions, views, and event triggers
continue to work as before.

**What's also new:**
- Upgrade migration safety infrastructure (scripts, CI, E2E tests)
- GitHub Pages book expansion (6 new documentation pages)
- User-facing upgrade guide (this document)

### 0.2.1 → 0.2.2

**No catalog table DDL changes.** The `topk_offset` column needed for paged
TopK was already added in v0.2.1.

**Two SQL function updates** are applied by
`pg_trickle--0.2.1--0.2.2.sql`:

- `pgtrickle.create_stream_table(...)`
  - default `schedule` changes from `'1m'` to `'calculated'`
  - default `refresh_mode` changes from `'DIFFERENTIAL'` to `'AUTO'`
- `pgtrickle.alter_stream_table(...)`
  - adds the optional `query` parameter used by ALTER QUERY support

Because PostgreSQL stores argument defaults and function signatures in
`pg_proc`, the migration script must `DROP FUNCTION` and recreate both
signatures during `ALTER EXTENSION ... UPDATE`.

**Behavioral notes:**

- Existing stream tables keep their current catalog values. The migration only
  changes the defaults used by future `create_stream_table(...)` calls.
- Existing applications can opt a table into the new defaults explicitly via
  `pgtrickle.alter_stream_table(...)` after the upgrade.
- After installing the new binary and restarting PostgreSQL, the scheduler now
  warns if the shared library version and SQL-installed extension version do
  not match. This helps detect stale `.so`/`.dylib` files after partial
  upgrades.

### 0.2.2 → 0.2.3

**One new catalog column** is added to `pgtrickle.pgt_stream_tables`:

| Column | Type | Default | Purpose |
|--------|------|---------|--------|
| `requested_cdc_mode` | `TEXT` | `NULL` | Optional per-stream-table CDC override (`'auto'`, `'trigger'`, `'wal'`) |

**The upgrade script also recreates two SQL functions**:

- `pgtrickle.create_stream_table(...)`
  - adds the optional `cdc_mode` parameter
- `pgtrickle.alter_stream_table(...)`
  - adds the optional `cdc_mode` parameter

**Monitoring view updates:**

- `pgtrickle.pg_stat_stream_tables` gains the `cdc_modes` column
- `pgtrickle.pgt_cdc_status` is added for per-source CDC visibility

Because PostgreSQL stores function signatures and defaults in `pg_proc`, the
upgrade script drops and recreates both lifecycle functions during
`ALTER EXTENSION ... UPDATE`.

### 0.6.0 → 0.7.0

**One new catalog column** is added to `pgtrickle.pgt_stream_tables`:

| Column | Type | Default | Purpose |
|--------|------|---------|--------|
| `last_fixpoint_iterations` | `INT` | `NULL` | Records how many rounds the last circular-dependency fixpoint run required |

**Two new catalog tables** are added:

| Table | Purpose |
|------|---------|
| `pgtrickle.pgt_watermarks` | Stores per-source watermark progress reported by external loaders |
| `pgtrickle.pgt_watermark_groups` | Stores groups of sources that must stay temporally aligned before refresh |

**The upgrade script also updates and adds SQL functions**:

- Recreates `pgtrickle.pgt_status()` so the result includes `scc_id`
- Adds `pgtrickle.pgt_scc_status()` for circular-dependency monitoring
- Adds `pgtrickle.advance_watermark(source, watermark)`
- Adds `pgtrickle.create_watermark_group(name, sources[], tolerance_secs)`
- Adds `pgtrickle.drop_watermark_group(name)`
- Adds `pgtrickle.watermarks()`
- Adds `pgtrickle.watermark_groups()`
- Adds `pgtrickle.watermark_status()`

**Behavioral notes:**

- Circular stream table dependencies can now run to convergence when
  `pg_trickle.allow_circular = true` and every member of the cycle is safe for
  monotone DIFFERENTIAL refresh.
- The scheduler can now hold back refreshes until related source tables are
  aligned within a configured watermark tolerance.
- Existing non-circular stream tables continue to work as before. The new
  catalog objects are additive.

### 0.7.0 → 0.8.0

**No catalog schema changes.** The upgrade migration script contains no DDL.

**New operational features:**

- `pg_dump` / `pg_restore` support: stream tables are now safely exported and
  re-connected after restore without manual intervention.
- Connection pooler opt-in was introduced at the per-stream level (superseded
  by the more comprehensive `pooler_compatibility_mode` added in v0.10.0).

**No breaking changes.** All v0.7.0 functions, views, and event triggers
continue to work as before.

### 0.8.0 → 0.9.0

**No catalog schema DDL changes** to `pgtrickle.pgt_stream_tables` or the
dependency catalog.

**New API function added:**

- `pgtrickle.restore_stream_tables()` — re-installs CDC triggers and
  re-registers stream tables after a `pg_restore` from a `pg_dump`.

**Hidden auxiliary columns for AVG / STDDEV / VAR aggregates.** Stream tables
using these aggregates will automatically receive hidden `__pgt_aux_*`
columns on the next refresh after upgrading. No manual action is needed —
pg_trickle detects missing auxiliary columns and performs a single full
reinitialise to add them.

**Behavioral notes:**

- COUNT, SUM, and AVG now update in constant time (O(changed rows)) instead
  of rescanning the whole group.
- STDDEV and VAR variants likewise update in O(changed rows) via hidden
  sum-of-squares auxiliary columns.
- MIN/MAX still requires a group rescan only when the deleted value is the
  current extreme.
- Refresh groups (`create_refresh_group`, `drop_refresh_group`,
  `refresh_groups()`) are available starting from this version.

### 0.9.0 → 0.10.0

**Two new catalog columns** added to `pgtrickle.pgt_stream_tables`:

| Column | Type | Default | Purpose |
|--------|------|---------|--------|
| `pooler_compatibility_mode` | `BOOLEAN NOT NULL` | `FALSE` | Disables prepared statements and NOTIFY for this stream table — required when accessed through PgBouncer in transaction-pool mode |
| `refresh_tier` | `TEXT NOT NULL` | `'hot'` | Tiered scheduling tier: `hot`, `warm`, `cold`, or `frozen` |

**One new catalog table** is added:

| Table | Purpose |
|------|---------|
| `pgtrickle.pgt_refresh_groups` | Stores refresh groups for snapshot-consistent multi-table refresh |

**The upgrade script also updates and adds SQL functions**:

- `pgtrickle.create_stream_table(...)` gains the `pooler_compatibility_mode` parameter
- `pgtrickle.create_stream_table_if_not_exists(...)` likewise
- `pgtrickle.create_or_replace_stream_table(...)` likewise
- `pgtrickle.alter_stream_table(...)` likewise
- Adds `pgtrickle.create_refresh_group(name, members, isolation)`
- Adds `pgtrickle.drop_refresh_group(name)`
- Adds `pgtrickle.refresh_groups()` — lists all declared groups

**Behavioral notes:**

- `pooler_compatibility_mode` defaults to `false`. Existing stream tables are
  unaffected. Enable it only for stream tables accessed through PgBouncer
  transaction-mode pooling.
- `pg_trickle.auto_backoff` now defaults to `on` (was `off`). The backoff
  threshold is raised from 80 % → 95 % and the maximum slowdown is capped at
  8× (was 64×). If you relied on the old opt-in behaviour, set
  `pg_trickle.auto_backoff = off` explicitly.
- `diamond_consistency` now defaults to `'atomic'` for new stream tables
  (was `'none'`). Existing stream tables keep their current setting.
- The scheduler now uses row-level locking for concurrency control instead of
  session-level advisory locks, making pg_trickle compatible with PgBouncer
  transaction-pool and similar connection poolers.
- Statistical aggregates (`CORR`, `COVAR_*`, `REGR_*`) now update
  incrementally using Welford-style accumulation, no longer requiring a
  group rescan.
- Materialized view sources can now be used in DIFFERENTIAL mode when
  `pg_trickle.matview_polling = on` is set.
- Recursive CTE stream tables with DELETE/UPDATE now use the Delete-and-Rederive
  algorithm (O(delta) instead of O(n)).

### 0.10.0 → 0.11.0

**New catalog columns** added to `pgtrickle.pgt_stream_tables`:

| Column | Type | Default | Purpose |
|--------|------|---------|--------|
| `effective_refresh_mode` | `TEXT` | `NULL` | Actual refresh mode used in the last cycle (FULL / DIFFERENTIAL / APPEND_ONLY / TOP_K / NO_DATA); populated by the scheduler after each completed refresh |
| `fuse_mode` | `TEXT NOT NULL` | `'off'` | Circuit-breaker mode: `off`, `on`, or `auto` |
| `fuse_state` | `TEXT NOT NULL` | `'armed'` | Circuit-breaker state: `armed`, `blown`, or `disabled` |
| `fuse_ceiling` | `BIGINT` | `NULL` | Maximum change-row count that can pass through in one refresh before the fuse blows; `NULL` = unlimited |
| `fuse_sensitivity` | `INT` | `NULL` | Sensitivity multiplier for auto-fuse detection |
| `blown_at` | `TIMESTAMPTZ` | `NULL` | Timestamp when the fuse last triggered |
| `blow_reason` | `TEXT` | `NULL` | Human-readable reason the fuse blew |
| `st_partition_key` | `TEXT` | `NULL` | Partition key column for declaratively partitioned stream tables; `NULL` = not partitioned |

**Updated function signatures** — existing calls continue to work because new parameters all have defaults:

- `pgtrickle.create_stream_table(...)` gains `partition_by TEXT DEFAULT NULL`
- `pgtrickle.create_stream_table_if_not_exists(...)` likewise
- `pgtrickle.create_or_replace_stream_table(...)` likewise
- `pgtrickle.alter_stream_table(...)` gains `fuse TEXT DEFAULT NULL`, `fuse_ceiling BIGINT DEFAULT NULL`, `fuse_sensitivity INT DEFAULT NULL`

**New functions**:

- `pgtrickle.reset_fuse(name TEXT, action TEXT DEFAULT 'apply')` — clear a blown fuse and resume scheduling
- `pgtrickle.fuse_status()` — returns circuit-breaker state for every stream table
- `pgtrickle.explain_refresh_mode(name TEXT)` — shows configured mode, effective mode, and the reason for any downgrade

**Behavioral notes:**

- Stream-table-to-stream-table chains now refresh **incrementally** — downstream tables receive a small insert/delete delta rather than cascading full refreshes.
- `pg_trickle.tiered_scheduling` now defaults to `on`.
- Declaratively partitioned stream tables are supported via `partition_by` — the refresh MERGE is automatically restricted to only the changed partitions.

### 0.11.0 → 0.12.0

**No schema changes.** This release adds four new diagnostic SQL functions only:

| Function | Returns | Purpose |
|----------|---------|--------|
| `pgtrickle.explain_query_rewrite(query TEXT)` | `TABLE(pass_name TEXT, changed BOOL, sql_after TEXT)` | Walk a query through every DVM rewrite pass to see how pg_trickle transforms it |
| `pgtrickle.diagnose_errors(name TEXT)` | `TABLE(event_time TIMESTAMPTZ, error_type TEXT, error_message TEXT, remediation TEXT)` | Last 5 FAILED refresh events with error classification and suggested fixes |
| `pgtrickle.list_auxiliary_columns(name TEXT)` | `TABLE(column_name TEXT, data_type TEXT, purpose TEXT)` | List all hidden `__pgt_*` auxiliary columns on a stream table's storage relation |
| `pgtrickle.validate_query(query TEXT)` | `TABLE(valid BOOL, mode TEXT, reason TEXT)` | Parse and validate a query for stream-table compatibility without creating one |

**Behavioral notes:**

- The incremental engine now handles multi-table join deletes correctly — phantom rows after simultaneous deletes from multiple join sides no longer occur.
- Stream-table-to-stream-table row identity is now computed consistently between the change buffer and the downstream table, eliminating stale duplicate rows after upstream UPDATEs.
- `pg_trickle.tiered_scheduling` defaults to `on` (same as 0.11.0 runtime behaviour; this release makes it the explicit default).

### 0.12.0 → 0.13.0

**Ten new catalog columns** added to `pgtrickle.pgt_stream_tables`:

| Column | Type | Default | Purpose |
|--------|------|---------|--------|
| `effective_refresh_mode` | `TEXT` | `NULL` | Computed refresh mode after AUTO resolution |
| `fuse_mode` | `TEXT NOT NULL` | `'off'` | Fuse configuration: off, auto, or manual |
| `fuse_state` | `TEXT NOT NULL` | `'armed'` | Current fuse state: armed or blown |
| `fuse_ceiling` | `BIGINT` | `NULL` | Maximum change count before fuse blows |
| `fuse_sensitivity` | `INT` | `NULL` | Consecutive cycles above ceiling before triggering |
| `blown_at` | `TIMESTAMPTZ` | `NULL` | Timestamp when the fuse last blew |
| `blow_reason` | `TEXT` | `NULL` | Reason the fuse blew |
| `st_partition_key` | `TEXT` | `NULL` | Partition key specification (RANGE, LIST, or HASH) |
| `max_differential_joins` | `INT` | `NULL` | Maximum join count for differential mode (auto-fallback to FULL when exceeded) |
| `max_delta_fraction` | `DOUBLE PRECISION` | `NULL` | Maximum delta-to-table ratio for differential mode (auto-fallback to FULL when exceeded) |

All columns use `ADD COLUMN IF NOT EXISTS` for idempotent upgrades.

**Nine new SQL functions** (plus one replacement with new signature):

| Function | Purpose |
|----------|---------|
| `pgtrickle.explain_delta(name, format)` | Delta SQL query plan inspection |
| `pgtrickle.dedup_stats()` | MERGE deduplication frequency counters |
| `pgtrickle.shared_buffer_stats()` | Per-source-buffer observability |
| `pgtrickle.explain_refresh_mode(name)` | Refresh mode decision explanation |
| `pgtrickle.reset_fuse(name)` | Reset a blown fuse |
| `pgtrickle.fuse_status()` | Fuse state across all stream tables |
| `pgtrickle.explain_query_rewrite(query)` | DVM rewrite pass inspection |
| `pgtrickle.diagnose_errors(name)` | Error classification and remediation |
| `pgtrickle.list_auxiliary_columns(name)` | Hidden `__pgt_*` column listing |
| `pgtrickle.validate_query(query)` | Query compatibility validation |
| `pgtrickle.alter_stream_table(...)` | *(replaced)* — new `partition_by` parameter |

**New GUC variables:**

| GUC | Default | Purpose |
|-----|---------|---------|
| `pg_trickle.per_database_worker_quota` | `0` (auto) | Per-database parallel worker limit |

**Behavioral notes:**

- **Shared change buffers:** Multiple stream tables reading from the same source now automatically share a single change buffer. No migration action required — existing per-source buffers continue to work.
- **Columnar change tracking:** Wide-table UPDATEs that touch only value columns (not GROUP BY / JOIN / WHERE columns) now generate significantly less delta volume. This is fully automatic.
- **Auto buffer partitioning:** Set `pg_trickle.buffer_partitioning = 'auto'` to let high-throughput buffers self-promote to partitioned mode for O(1) cleanup.
- **dbt macros:** If you use dbt-pgtrickle, update your macros to the matching v0.13.0 version. New config options: `partition_by`, `fuse`, `fuse_ceiling`, `fuse_sensitivity`.

**No breaking changes.** All v0.12.0 functions, views, and event triggers continue to work as before.

### 0.13.0 → 0.14.0

**Two new catalog columns** added to `pgtrickle.pgt_stream_tables`:

| Column | Type | Default | Purpose |
|--------|------|---------|--------|
| `last_error_message` | `TEXT` | `NULL` | Error message from the last permanent refresh failure |
| `last_error_at` | `TIMESTAMPTZ` | `NULL` | Timestamp of the last permanent refresh failure |

**Updated function signature** (return type gained new columns):

- `pgtrickle.st_refresh_stats()` — gains `consecutive_errors`, `schedule`,
  `refresh_tier`, and `last_error_message` columns. The upgrade script
  drops and recreates the function. No behavior change for existing callers
  that ignore unknown columns.

**New SQL functions** (available immediately after `ALTER EXTENSION ... UPDATE`):

| Function | Purpose |
|----------|---------|
| `pgtrickle.recommend_refresh_mode(name)` | Workload-based refresh mode recommendation with confidence level |
| `pgtrickle.refresh_efficiency(name)` | Per-table FULL vs. DIFFERENTIAL performance metrics |
| `pgtrickle.export_definition(name)` | Export stream table as reproducible DROP+CREATE+ALTER DDL |
| `pgtrickle.convert_buffers_to_unlogged()` | Convert logged change buffers to UNLOGGED |

**New GUC variables:**

| GUC | Default | Purpose |
|-----|---------|---------|
| `pg_trickle.planner_aggressive` | `true` | Consolidated switch replacing `merge_planner_hints` + `merge_work_mem_mb` |
| `pg_trickle.unlogged_buffers` | `false` | Create new change buffers as UNLOGGED (reduces WAL by ~30%) |
| `pg_trickle.agg_diff_cardinality_threshold` | `1000` | Warn at creation time when GROUP BY cardinality is below this |

**Deprecated GUCs** (still accepted but ignored at runtime):

- `pg_trickle.merge_planner_hints` → use `pg_trickle.planner_aggressive`
- `pg_trickle.merge_work_mem_mb` → use `pg_trickle.planner_aggressive`

**Behavioral notes:**

- **Error-state circuit breaker:** A single permanent refresh failure (e.g.
  a function that doesn't exist for the column type) now immediately sets the
  stream table status to `ERROR` with a message stored in `last_error_message`.
  The scheduler skips `ERROR` tables. Use `pgtrickle.resume_stream_table(name)`
  followed by `pgtrickle.alter_stream_table(name, query => ...)` to recover.
- **Tiered scheduling NOTICE:** Demoting a stream table from `hot` to `cold`
  or `frozen` now emits a NOTICE so operators are aware the effective refresh
  interval has changed (10× for cold, suspended for frozen).
- **SECURITY DEFINER triggers:** All CDC trigger functions now run with
  `SECURITY DEFINER` and an explicit `SET search_path`, hardening against
  privilege-escalation attacks. This is applied automatically on upgrade —
  no manual action needed.

**No breaking changes.** All v0.13.0 functions, views, and event triggers
continue to work as before.

---

### 0.14.0 → 0.15.0

**No schema changes.** New features: interactive dashboard,
bulk `create_stream_tables_from_schema()`, and per-table runaway-refresh
protection (`max_refresh_duration_ms`).

**New GUC variables:**

| GUC | Default | Purpose |
|-----|---------|---------|
| `pg_trickle.ivm_cache_max_entries` | `0` (unbounded) | Bound per-backend IVM delta cache |

---

### 0.15.0 → 0.16.0

**No schema changes.** Performance improvements to the delta pipeline and
refresh path. L2 catalog-backed template cache (`pgtrickle.pgt_template_cache`)
introduced.

---

### 0.16.0 → 0.17.0

**No schema changes.** Query intelligence improvements: window function
differentiation, correlated-sublink rewriting.

---

### 0.17.0 → 0.18.0

**No schema changes.** Hardening pass: tightened unsafe blocks, improved error
propagation, delta performance improvements (prepared-statement MERGE path).

---

### 0.18.0 → 0.19.0

**No schema changes.** Security enhancements: SECURITY DEFINER on all
public-facing functions, improved RLS awareness in delta generation.

---

### 0.19.0 → 0.20.0

**New catalog table:** `pgtrickle.pgt_self_monitoring` for extension health
metrics. New function: `pgtrickle.metrics_summary()`.

---

### 0.20.0 → 0.21.0

**No schema changes.** Reliability improvements: advisory-lock hardening,
WAL-receiver retry, graceful SIGTERM in background workers.

---

### 0.21.0 → 0.22.0

**No schema changes.** New features: downstream CDC pipeline, parallel refresh
scheduling, predictive cost model for FULL vs DIFFERENTIAL selection.

---

### 0.22.0 → 0.23.0

**No schema changes.** Performance tuning and diagnostics: delta amplification
detection, EXPLAIN capture (`PGS_PROFILE_DELTA`), adaptive threshold
auto-tuning.

**New GUC variables:**

| GUC | Default | Purpose |
|-----|---------|---------|
| `pg_trickle.delta_amplification_threshold` | `10.0` | Warn when output/input delta ratio exceeds this |
| `pg_trickle.log_delta_sql` | `false` | Log resolved delta SQL at `DEBUG1` |

---

### 0.23.0 → 0.24.0

**No schema changes.** Join correctness hardening: phantom-row detection
infrastructure, durability improvements for committed change buffers.

---

### 0.24.0 → 0.25.0

**No schema changes.** Scheduler scalability: worker pool, L1 template cache
with LRU eviction.

**New GUC variables:**

| GUC | Default | Purpose |
|-----|---------|---------|
| `pg_trickle.worker_pool_size` | `0` | Persistent worker pool size |
| `pg_trickle.template_cache_max_entries` | `0` | L1 delta SQL template cache cap |

---

### 0.25.0 → 0.26.0

**No schema changes.** Concurrency hardening: improved lock ordering, stress
test suite, fixed MERGE race under high concurrency.

---

### 0.26.0 → 0.27.0

**New catalog columns** added to `pgtrickle.pgt_stream_tables`:

| Column | Type | Default | Purpose |
|--------|------|---------|---------|
| `last_full_ms` | `FLOAT8` | `NULL` | Duration of last FULL refresh (ms) |
| `auto_threshold` | `FLOAT8` | `NULL` | Adaptive FULL/DIFF cost-ratio threshold |

**New catalog table:** `pgtrickle.pgt_template_cache` for L2 cross-backend
delta SQL storage.

**New SQL functions:**

| Function | Purpose |
|----------|---------|
| `pgtrickle.snapshot_stream_table(name, dest)` | Consistent snapshot copy |
| `pgtrickle.restore_from_snapshot(name, source)` | Restore from snapshot |
| `pgtrickle.list_snapshots(name)` | List available snapshots |
| `pgtrickle.recommend_schedule(name)` | SLA-based scheduling recommendation |
| `pgtrickle.schedule_recommendations()` | Multi-table scheduling report |
| `pgtrickle.cluster_worker_summary()` | Cross-database scheduler health |
| `pgtrickle.metrics_summary()` | Prometheus-compatible extension metrics |

**New GUC variables:**

| GUC | Default | Purpose |
|-----|---------|---------|
| `pg_trickle.metrics_port` | `9187` | Prometheus metrics port |
| `pg_trickle.metrics_request_timeout_ms` | `5000` | Metrics endpoint timeout |
| `pg_trickle.frontier_holdback_mode` | `warn` | Holdback action on stale frontier |
| `pg_trickle.frontier_holdback_warn_seconds` | `300` | Frontier holdback warning threshold |
| `pg_trickle.publication_lag_warn_bytes` | `104857600` | WAL lag warning threshold |
| `pg_trickle.schedule_recommendation_min_samples` | `20` | Min samples for schedule recommendation |
| `pg_trickle.schedule_alert_cooldown_seconds` | `300` | Min interval between schedule alerts |
| `pg_trickle.change_buffer_durability` | `logged` | Change buffer WAL level (`logged`, `sync`, or `unlogged`) |

No breaking changes.

---

### 0.27.0 → 0.28.0

**New catalog tables:** `pgtrickle.outbox_events`, `pgtrickle.inbox_messages`,
`pgtrickle.inbox_dead_letters` for transactional outbox and inbox patterns.

**Historical SQL functions:** this release introduced pg_trickle-managed
outbox/inbox functions such as `enable_outbox()` and `enable_inbox()`. Those
APIs were later extracted to [`pg_tide`](https://github.com/trickle-labs/pg-tide)
in v0.46.0; current pg_trickle keeps only the integration hooks
`attach_outbox()` and `detach_outbox()`.

No breaking changes.

---

### 0.28.0 → 0.29.0

Added relay catalog tables and SQL functions (`set_relay_outbox`, `set_relay_inbox`,
`enable_relay`, `disable_relay`, `delete_relay`, `get_relay_config`,
`list_relay_configs`) and the standalone `pgtrickle-relay` binary. These were
later extracted to [`pg_tide`](https://github.com/trickle-labs/pg-tide) in v0.46.0.

No breaking changes.

---

### 0.29.0 → 0.30.0

**No schema changes.** All improvements are confined to the Rust extension
binary. The migration file (`sql/pg_trickle--0.29.0--0.30.0.sql`) is empty
other than documentation comments.

**New GUC variables:**

| GUC | Default | Purpose |
|-----|---------|---------|
| `pg_trickle.use_sqlstate_classification` | `false` | Locale-safe SQLSTATE-based retry classification |
| `pg_trickle.template_cache_max_age_hours` | `168` | Max age for L2 template-cache entries (hours) |
| `pg_trickle.max_parse_nodes` | `100000` | Parser node-count guard (always enabled; hard maximum 1000000) |

**Behavioral changes:**

- `restore_from_snapshot()` now returns a typed error
  (`SnapshotSchemaVersionMismatch`) when the snapshot has no
  `__pgt_snapshot_version` column (pre-v0.27 snapshots).  Previously it
  silently treated the missing column as compatible.
- `snapshot_stream_table()` and `restore_from_snapshot()` now wrap critical
  operations in PostgreSQL subtransactions.  A failed catalog INSERT rolls
  back the snapshot table creation, preventing orphan tables.
- Cross-cycle phantom rows are now cleaned up unconditionally after every
  differential refresh of a join query.

No breaking changes.

---

### 0.30.0 → 0.31.0

**No schema changes.** All improvements are confined to the Rust extension binary and scheduler logic.

**New GUC variables:**

| GUC | Default | Purpose |
|-----|---------|---------|
| `pg_trickle.cost_model_miss_penalty` | `2.0` | Weight applied to the estimated cost when the planner's row count estimate is inaccurate |
| `pg_trickle.scheduler_hot_tier_interval_ms` | `500` | Effective polling interval (ms) for Hot-tier stream tables |

**Behavioral changes:**

- Scheduler now uses a predictive cost model to decide DIFFERENTIAL vs. FULL refresh per cycle; the model activates after `pg_trickle.prediction_min_samples` samples.
- Event-driven wake now debounces duplicate NOTIFY payloads within a single tick to avoid redundant wakeups on bulk writes.

No breaking changes.

---

### 0.31.0 → 0.32.0

**No schema changes.** Citus stable naming infrastructure is added without altering the public catalog schema.

**Behavioral changes:**

- `pgtrickle.source_stable_name(rel_oid)` introduced as a deterministic, version-stable WAL slot name for Citus distributed sources.
- Per-source `last_frontier` column added to `pgtrickle.pgt_stream_tables` via `ADD COLUMN IF NOT EXISTS` — existing rows receive `NULL`.

No breaking changes.

---

### 0.32.0 → 0.33.0

**Schema additions** — new catalog tables for Citus distributed CDC:

| Object | Type | Purpose |
|--------|------|---------|
| `pgtrickle.pgt_worker_slots` | Table | Tracks per-worker WAL slot name and last-consumed frontier for each Citus worker / source combination |
| `pgtrickle.pgt_st_locks` | Table | Lightweight distributed mutex for cross-coordinator refresh serialisation |
| `pgtrickle.citus_status` | View | Per-(stream table, source, worker) CDC health view |

**New SQL functions:**

- `pgtrickle.ensure_worker_slot(st_name, worker_host, worker_port)` — creates the WAL slot on a Citus worker if it does not exist.
- `pgtrickle.poll_worker_slot_changes(st_name, worker_host, worker_port)` — drains pending WAL changes from a worker slot into the coordinator change buffer.
- `pgtrickle.handle_vp_promoted(payload TEXT)` — processes a `pg_ripple.vp_promoted` NOTIFY payload and signals the scheduler.
- `pgtrickle.check_citus_version_compat()` — verifies that all worker nodes run the same pg_trickle version.
- `pgtrickle.check_worker_wal_level()` — verifies that `wal_level = logical` on every worker.

**New `create_stream_table()` parameter:**

- `output_distribution_column TEXT` — when provided (and Citus is installed), converts the output storage table to a Citus distributed table on that column immediately after creation.

**New GUC:**

| GUC | Default | Purpose |
|-----|---------|---------|
| `pg_trickle.citus_st_lock_lease_ms` | `60000` | Duration (ms) of the `pgt_st_locks` lease for cross-node coordination |

No application-level breaking changes.  Existing stream tables on non-Citus deployments are completely unaffected.

---

### 0.33.0 → 0.34.0

**Schema additions** — the `pgtrickle.citus_status` view gains five new columns:

| Column | Type | Description |
|--------|------|-------------|
| `last_polled_at` | `timestamptz` | Timestamp of the last successful per-worker poll |
| `lease_holder` | `text` | Session holding the `pgt_st_locks` lease (NULL when unlocked) |
| `lease_acquired_at` | `timestamptz` | When the current lease was acquired |
| `lease_expires_at` | `timestamptz` | When the current lease expires |
| `lease_health` | `text` | `'unlocked'` / `'locked'` / `'expired'` |

**Behavioral changes:**

- The scheduler now drives the full per-worker slot lifecycle automatically for stream tables with `source_placement = 'distributed'`: `ensure_worker_slot()` on first tick (and after topology changes), `poll_worker_slot_changes()` on every tick, and `pgt_st_locks` lease acquire/extend/release.  Manual wiring via `LISTEN "pg_ripple.vp_promoted" + handle_vp_promoted()` is no longer required (though harmless if left in place).
- Shard rebalance auto-recovery: the scheduler detects `pg_dist_node` topology changes, prunes stale `pgt_worker_slots` rows, inserts new ones, and marks the stream table for a full refresh — no operator intervention required.
- Worker failure isolation: per-worker `poll_worker_slot_changes()` failures are caught, logged, and skipped for that tick; healthy workers continue uninterrupted.

**New GUC:**

| GUC | Default | Purpose |
|-----|---------|---------|
| `pg_trickle.citus_worker_retry_ticks` | `5` | Consecutive per-worker poll failures before emitting a WARNING and flagging in `citus_status`. Set to `0` to disable. |

**Migration note:**

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.34.0';
```

The migration script adds the five new columns to `citus_status` via `CREATE OR REPLACE VIEW`.  No data loss.

No breaking changes.  Non-Citus deployments are completely unaffected.

---

### 0.34.0 → 0.35.0

**New catalog columns** added to `pgtrickle.pgt_stream_tables`:

| Column | Type | Default | Purpose |
|--------|------|---------|---------|
| `in_shadow_build` | `BOOLEAN NOT NULL` | `FALSE` | Whether this stream table is currently undergoing zero-downtime schema evolution |
| `shadow_table_name` | `TEXT` | `NULL` | Name of the shadow table being built during schema evolution |

**New catalog table:**

| Table | Purpose |
|-------|---------|
| `pgtrickle.pgt_subscriptions` | Stores reactive subscription registrations (NOTIFY channel → stream table mappings) |

**New SQL functions:**

| Function | Purpose |
|----------|---------|
| `pgtrickle.subscribe(stream_table TEXT, channel TEXT)` | Register a NOTIFY channel to fire after each refresh cycle |
| `pgtrickle.unsubscribe(stream_table TEXT, channel TEXT)` | Remove a subscription |
| `pgtrickle.list_subscriptions()` | List all active subscriptions |
| `pgtrickle.sla_summary()` | Return p50/p99 latency, freshness lag, error rate, and budget over the SLA window |
| `pgtrickle.explain_stream_table(name TEXT)` | Human-readable DVM configuration and refresh mode explanation |
| `pgtrickle.view_evolution_status()` | Status of in-progress shadow table builds |

**Behavioral notes:**

- Zero-downtime schema evolution (`ALTER STREAM TABLE`) now builds a shadow table
  in the background and cuts over atomically. The `in_shadow_build` column tracks
  progress; check `pgtrickle.view_evolution_status()` to monitor.
- `pgtrickle.sla_summary()` queries `pgt_refresh_history` using the
  `pg_trickle.sla_window_hours` GUC (default 24 h).
- Reactive subscriptions emit `pg_notify(channel, '')` after each non-empty
  refresh cycle. Debounce interval is controlled by `pg_trickle.notify_coalesce_ms`.

**New GUCs:**

| GUC | Default | Description |
|-----|---------|-------------|
| `pg_trickle.cdc_paused` | `false` | Pause CDC trigger writes (discard mode — see CONFIGURATION.md) |
| `pg_trickle.notify_coalesce_ms` | `250` | Debounce window (ms) for reactive subscription NOTIFY calls |
| `pg_trickle.sla_window_hours` | `24` | Reporting window (h) for `sla_summary()` |
| `pg_trickle.history_prune_interval_seconds` | `60` | Interval between `pgt_refresh_history` cleanup sweeps |

**Migration note:**

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.35.0';
```

No breaking changes. All v0.34.0 functions continue to work.

---

### 0.35.0 → 0.36.0

**New catalog columns** added to `pgtrickle.pgt_stream_tables`:

| Column | Type | Default | Purpose |
|--------|------|---------|---------|
| `temporal_mode` | `BOOLEAN NOT NULL` | `FALSE` | Enable temporal IVM (SCD Type 2) tracking |
| `storage_backend` | `TEXT NOT NULL` | `'heap'` | Storage backend: `'heap'` or `'citus'` |
| `column_lineage` | `JSONB` | `NULL` | Column-level lineage mapping output columns to source tables/columns |

**New SQL functions:**

| Function | Purpose |
|----------|---------|
| `pgtrickle.drain(timeout_s INT)` | Gracefully quiesce all in-flight refreshes |
| `pgtrickle.is_drained()` | Check whether the scheduler is fully drained |
| `pgtrickle.bulk_alter_stream_tables(names TEXT[], params JSONB)` | Alter multiple stream tables in one call |
| `pgtrickle.bulk_drop_stream_tables(names TEXT[])` | Drop multiple stream tables in one call |
| `pgtrickle.stream_table_lineage(name TEXT)` | Return column-level lineage for a stream table |
| `pgtrickle.exec_stream_ddl(cmd TEXT)` | Execute DDL in the stream-table DDL sandbox |

**Updated function signatures:**

- `pgtrickle.create_stream_table(...)` gains `temporal BOOLEAN DEFAULT FALSE` and `storage_backend TEXT DEFAULT 'heap'` parameters.

**Behavioral notes:**

- **Temporal IVM (CORR-1):** Stream tables created with `temporal := true` maintain SCD Type 2 history. Each row carries `__pgt_valid_from TIMESTAMPTZ` and `__pgt_valid_to TIMESTAMPTZ`. Existing tables are unaffected.
- **Alternative storage backends:** `storage_backend = 'citus'` creates the stream table storage as a Citus distributed table. Requires the Citus extension to be installed.
- **Drain mode (A35):** `pgtrickle.drain()` is a safety mechanism for maintenance windows. The scheduler completes all in-flight refreshes, then stops dispatching new ones until the drain is cancelled or the server restarts.
- **WAL slot backpressure (A12):** The `pg_trickle.enforce_backpressure` GUC is now wired — when slot lag exceeds `slot_lag_critical_threshold_mb`, CDC writes are suppressed. See `CONFIGURATION.md` for details and the **discard semantics** of `cdc_paused`.

**New GUCs:**

| GUC | Default | Description |
|-----|---------|-------------|
| `pg_trickle.enforce_backpressure` | `false` | Suppress CDC writes when WAL slot lag exceeds critical threshold |
| `pg_trickle.log_format` | `'text'` | Structured log format: `'text'` or `'json'` |
| `pg_trickle.temporal_stream_tables` | `false` | Master switch for temporal IVM support |

**TRUNCATE and CDC semantics:** When `pg_trickle.cdc_paused = on`, CDC trigger
bodies return `NULL` — changes are **discarded**. This is the discard mode.
After un-pausing, stream tables must be reinitialized (FULL refresh) to
recover from the gap. A future `cdc_capture_mode = 'hold'` option is planned.

**Migration note:**

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.36.0';
```

No breaking changes. The `column_lineage` column is additive. The
`temporal_mode` and `storage_backend` columns have safe defaults.

---

### 0.36.0 → 0.37.0

**Schema change:** All existing change buffer tables in
`pgtrickle_changes.*` gain a `__pgt_trace_context TEXT` column via dynamic
`ALTER TABLE`. This is applied automatically by the upgrade script.

**Behavioral notes:**

- **W3C Trace Context (F10):** When `pg_trickle.enable_trace_propagation = true`,
  CDC triggers capture the session `pg_trickle.trace_id` GUC into the
  `__pgt_trace_context` column. At refresh time, the stored trace context is
  propagated to any OTLP span exported to `pg_trickle.otel_endpoint`.
- **pgVectorMV (F4):** `avg(vector_col)` and `sum(vector_col)` in defining
  queries are now handled incrementally when `pg_trickle.enable_vector_agg =
  true`. Requires pgvector ≥ 0.7.0.

**New GUCs:**

| GUC | Default | Description |
|-----|---------|-------------|
| `pg_trickle.enable_vector_agg` | `false` | Enable incremental vector aggregate operators |
| `pg_trickle.enable_trace_propagation` | `false` | Enable W3C Trace Context propagation |
| `pg_trickle.otel_endpoint` | `''` | OTLP/gRPC endpoint for span export (empty = off) |
| `pg_trickle.trace_id` | `''` | Session-level W3C traceparent header |

**Migration note:**

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.37.0';
```

The upgrade script applies `ALTER TABLE ... ADD COLUMN IF NOT EXISTS
__pgt_trace_context TEXT` to each existing change buffer table. This is
idempotent and safe for large installations. Expect a brief metadata lock
on each buffer table during the upgrade.

No breaking changes. The `__pgt_trace_context` column is `NULL` unless
`enable_trace_propagation = true` and a session trace_id is set.

---

### 0.37.0 → 0.38.0

**No schema changes.** This is a correctness and diagnostic release.

**Behavioral notes:**

- **EC-01 correctness closeout:** Join phantom row elimination is complete.
  Property tests now prove convergence across join patterns including three-way
  joins with simultaneous multi-side deletes.
- **Fuzz regression fixes:** All known fuzz corpus failures are resolved.

**Migration note:**

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.38.0';
```

No breaking changes.

---

### 0.38.0 → 0.39.0

**New SQL function:**

| Function | Purpose |
|----------|---------|
| `pgtrickle.cdc_pause_status()` | Return the active CDC pause state: paused flag, capture mode, and operator guidance |

**Extended SQL function:**

- `pgtrickle.explain_stream_table(name TEXT)` — now includes CDC status, backpressure state, and explicit DIFF/FULL reasoning.

**New GUC:**

| GUC | Default | Description |
|-----|---------|-------------|
| `pg_trickle.cdc_capture_mode` | `'discard'` | CDC semantics when `cdc_paused=on`: `'discard'` (default, drops changes) or `'hold'` (reserved) |

**Behavioral notes:**

- **O39-6 SQLSTATE-first retry:** When `use_sqlstate_classification = true`
  (default), scheduler retry decisions use the bracketed SQLSTATE code from
  `PgTrickleError::SpiErrorCode` instead of English error message text,
  making retry behavior locale-independent.
- **O39-8 CDC capture mode:** The `cdc_paused` discard semantics are now
  explicitly documented and operator-visible via `pgtrickle.cdc_pause_status()`.
  The `cdc_capture_mode` GUC is reserved for a future hold mode.

**TRUNCATE/CDC semantics (explicit):**

When `pg_trickle.cdc_paused = on`:

- `cdc_capture_mode = 'discard'` (default): All DML against source tables
  passes through the CDC trigger, but the trigger returns `NULL` immediately
  without writing to the change buffer. **Changes are permanently lost.**
  After un-pausing, run a FULL refresh on any stream table that received DML
  during the pause:
  ```sql
  SELECT pgtrickle.refresh_stream_table('my_stream', 'FULL');
  ```
- `cdc_capture_mode = 'hold'`: Not yet implemented. Emits a `WARNING` and
  falls back to `'discard'`.

When `TRUNCATE` occurs on a source table:

- In trigger-based CDC mode, the TRUNCATE trigger calls `pgtrickle.pgt_ivm_handle_truncate()`
  which schedules a FULL refresh. If `cdc_paused = on`, the trigger returns
  `NULL` and the TRUNCATE is **not recorded**. After un-pausing, the stream
  table will not be aware that a TRUNCATE occurred — reinitialize explicitly.
- In WAL-based CDC mode, TRUNCATEs are detected during the next logical
  decoding poll and scheduled for FULL refresh.

**Migration note:**

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.39.0';
```

No catalog schema changes. The upgrade script is a no-op DDL-wise.

---

### 0.39.0 → 0.40.0

**No SQL schema changes.** This release contains internal improvements only.

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.40.0';
```

---

### 0.40.0 → 0.50.0

**No SQL schema changes.** This release contains internal improvements and new documentation only.

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.50.0';
```

---

### 0.50.0 → 0.51.0

**Breaking changes:**

- `pg_trickle.event_driven_wake` GUC has been **removed** (CQ-10-02). Remove
  any `ALTER SYSTEM SET pg_trickle.event_driven_wake ...` or
  `postgresql.conf` entries for this GUC before upgrading.
- `pg_trickle.wake_debounce_ms` GUC has been **removed** (CQ-10-02). Remove
  any references to this GUC from your configuration.

**No SQL schema changes.** Only code and documentation changes.

**Migration:**

```sql
-- Remove obsolete GUC settings before upgrading (run as superuser):
ALTER SYSTEM RESET pg_trickle.event_driven_wake;
ALTER SYSTEM RESET pg_trickle.wake_debounce_ms;
SELECT pg_reload_conf();

-- Then upgrade:
ALTER EXTENSION pg_trickle UPDATE TO '0.51.0';
```

---

## Supported Upgrade Paths

The following migration hops are available. PostgreSQL chains them
automatically when you run `ALTER EXTENSION pg_trickle UPDATE`.

| From | To | Script |
|------|----|--------|
| 0.1.3 | 0.2.0 | `pg_trickle--0.1.3--0.2.0.sql` |
| 0.2.0 | 0.2.1 | `pg_trickle--0.2.0--0.2.1.sql` |
| 0.2.1 | 0.2.2 | `pg_trickle--0.2.1--0.2.2.sql` |
| 0.2.2 | 0.2.3 | `pg_trickle--0.2.2--0.2.3.sql` |
| 0.2.3 | 0.3.0 | `pg_trickle--0.2.3--0.3.0.sql` |
| 0.3.0 | 0.4.0 | `pg_trickle--0.3.0--0.4.0.sql` |
| 0.4.0 | 0.5.0 | `pg_trickle--0.4.0--0.5.0.sql` |
| 0.5.0 | 0.6.0 | `pg_trickle--0.5.0--0.6.0.sql` |
| 0.6.0 | 0.7.0 | `pg_trickle--0.6.0--0.7.0.sql` |
| 0.7.0 | 0.8.0 | `pg_trickle--0.7.0--0.8.0.sql` |
| 0.8.0 | 0.9.0 | `pg_trickle--0.8.0--0.9.0.sql` |
| 0.9.0 | 0.10.0 | `pg_trickle--0.9.0--0.10.0.sql` |
| 0.10.0 | 0.11.0 | `pg_trickle--0.10.0--0.11.0.sql` |
| 0.11.0 | 0.12.0 | `pg_trickle--0.11.0--0.12.0.sql` |
| 0.12.0 | 0.13.0 | `pg_trickle--0.12.0--0.13.0.sql` |
| 0.13.0 | 0.14.0 | `pg_trickle--0.13.0--0.14.0.sql` |
| 0.14.0 | 0.15.0 | `pg_trickle--0.14.0--0.15.0.sql` |
| 0.15.0 | 0.16.0 | `pg_trickle--0.15.0--0.16.0.sql` |
| 0.16.0 | 0.17.0 | `pg_trickle--0.16.0--0.17.0.sql` |
| 0.17.0 | 0.18.0 | `pg_trickle--0.17.0--0.18.0.sql` |
| 0.18.0 | 0.19.0 | `pg_trickle--0.18.0--0.19.0.sql` |
| 0.19.0 | 0.20.0 | `pg_trickle--0.19.0--0.20.0.sql` |
| 0.20.0 | 0.21.0 | `pg_trickle--0.20.0--0.21.0.sql` |
| 0.21.0 | 0.22.0 | `pg_trickle--0.21.0--0.22.0.sql` |
| 0.22.0 | 0.23.0 | `pg_trickle--0.22.0--0.23.0.sql` |
| 0.23.0 | 0.24.0 | `pg_trickle--0.23.0--0.24.0.sql` |
| 0.24.0 | 0.25.0 | `pg_trickle--0.24.0--0.25.0.sql` |
| 0.25.0 | 0.26.0 | `pg_trickle--0.25.0--0.26.0.sql` |
| 0.26.0 | 0.27.0 | `pg_trickle--0.26.0--0.27.0.sql` |
| 0.27.0 | 0.28.0 | `pg_trickle--0.27.0--0.28.0.sql` |
| 0.28.0 | 0.29.0 | `pg_trickle--0.28.0--0.29.0.sql` |
| 0.29.0 | 0.30.0 | `pg_trickle--0.29.0--0.30.0.sql` |
| 0.30.0 | 0.31.0 | `pg_trickle--0.30.0--0.31.0.sql` |
| 0.31.0 | 0.32.0 | `pg_trickle--0.31.0--0.32.0.sql` |
| 0.32.0 | 0.33.0 | `pg_trickle--0.32.0--0.33.0.sql` |
| 0.33.0 | 0.34.0 | `pg_trickle--0.33.0--0.34.0.sql` |
| 0.34.0 | 0.35.0 | `pg_trickle--0.34.0--0.35.0.sql` |
| 0.35.0 | 0.36.0 | `pg_trickle--0.35.0--0.36.0.sql` |
| 0.36.0 | 0.37.0 | `pg_trickle--0.36.0--0.37.0.sql` |
| 0.37.0 | 0.38.0 | `pg_trickle--0.37.0--0.38.0.sql` |
| 0.38.0 | 0.39.0 | `pg_trickle--0.38.0--0.39.0.sql` |
| 0.39.0 | 0.40.0 | `pg_trickle--0.39.0--0.40.0.sql` |
| 0.40.0 | 0.50.0 | `pg_trickle--0.40.0--0.50.0.sql` |
| 0.50.0 | 0.51.0 | `pg_trickle--0.50.0--0.51.0.sql` |

Any installation from 0.1.3 onward can be upgraded to 0.51.0 in a single
`ALTER EXTENSION pg_trickle UPDATE` — PostgreSQL chains the hops automatically
after the new binaries are installed and the server has been restarted.

---

## Rollback / Downgrade

PostgreSQL does not support automatic extension downgrades. To roll back:

1. **Export stream table definitions** (if you want to recreate them later):
  ```bash
  cargo run --bin pg_trickle_dump -- --output backup.sql
  ```
  Or, if the binary is already installed in your PATH:
  ```bash
  pg_trickle_dump --output backup.sql
  ```
  Use `--dsn '<connection string>'` or standard `PG*` / `DATABASE_URL`
  environment variables when the default local connection parameters are not
  sufficient.

2. **Drop the extension** (destroys all stream tables):
   ```sql
   DROP EXTENSION pg_trickle CASCADE;
   ```

3. **Install the old version** and restart PostgreSQL.

4. **Recreate the extension** at the old version:
   ```sql
   CREATE EXTENSION pg_trickle VERSION '0.1.3';
   ```

5. **Recreate stream tables** from your backup.

---

## Troubleshooting

### "function pgtrickle.xxx does not exist" after upgrade

This means the upgrade script is missing a function. Workaround:

```sql
-- Check what version PostgreSQL thinks is installed
SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle';

-- If the version looks correct but functions are missing,
-- the upgrade script may be incomplete. Try a clean reinstall:
DROP EXTENSION pg_trickle CASCADE;
CREATE EXTENSION pg_trickle CASCADE;
-- Warning: this destroys all stream tables!
```

Report this as a bug — upgrade scripts should never silently drop functions.

### "could not access file pg_trickle" after restart

The new shared library file was not installed correctly. Verify:

```bash
ls -la $(pg_config --pkglibdir)/pg_trickle*
```

### ALTER EXTENSION UPDATE says "already at version X"

The binary files are already the new version but the SQL catalog wasn't
upgraded. This usually means the `.control` file's `default_version`
matches your current version. Check:

```bash
cat $(pg_config --sharedir)/extension/pg_trickle.control
```

---

## Multi-Database Environments

`ALTER EXTENSION UPDATE` must be run in **each database** where pg_trickle
is installed. A common pattern:

```bash
for db in $(psql -t -c "SELECT datname FROM pg_database WHERE datname NOT IN ('template0', 'template1')"); do
  psql -d "$db" -c "ALTER EXTENSION pg_trickle UPDATE;" 2>/dev/null || true
done
```

---

## CloudNativePG (CNPG)

For CNPG deployments, see [cnpg/README.md](https://github.com/trickle-labs/pg-trickle/blob/main/cnpg/) for upgrade
instructions specific to the Kubernetes operator.

---

## Upgrading to v0.23.0

### New GUCs

| GUC | Default | Description |
|-----|---------|-------------|
| `pg_trickle.log_delta_sql` | `off` | Log generated delta SQL at DEBUG1 level for diagnosis |
| `pg_trickle.delta_work_mem` | `0` (inherit) | work_mem override (MB) for delta SQL execution |
| `pg_trickle.delta_enable_nestloop` | `on` | Allow nested-loop joins during delta execution |
| `pg_trickle.analyze_before_delta` | `on` | Run ANALYZE on change buffers before delta SQL |
| `pg_trickle.max_change_buffer_alert_rows` | `0` (disabled) | Alert threshold for change buffer overflow |
| `pg_trickle.diff_output_format` | `split` | DIFF output format: `split` or `merged` |

### Behavioral Changes

**DI-2 aggregate UPDATE-split:** The DIFF output row format for aggregate
stream tables changes from UPDATE rows to DELETE+INSERT pairs. This is the
algebraically correct form that enables O(Δ) performance for multi-join
queries.

**Impact:** Application code that reads the change buffer or outbox and
checks `op = 'UPDATE'` will silently produce incorrect results.

**Migration path:**
1. Set `pg_trickle.diff_output_format = 'merged'` before upgrading
2. Migrate application code to handle DELETE+INSERT pairs
3. Switch to `pg_trickle.diff_output_format = 'split'` (default)

### Rollback Strategy

The DI-2/DI-6 code paths are gated by detecting UPDATE rows in the change
buffer. Downgrading to v0.22.0 is safe if no writes have occurred to
upgraded stream tables.

### Pre-Upgrade Validation

```bash
# Verify version files are in sync
just check-version-sync
```

### New SQL Functions

- `pgtrickle.explain_diff_sql(name TEXT)` — Returns the delta SQL template
  for a stream table (for inspection/EXPLAIN)
- `pgtrickle.pgtrickle_refresh_stats()` — Per-stream-table timing stats
  with avg/p95/p99 percentiles
