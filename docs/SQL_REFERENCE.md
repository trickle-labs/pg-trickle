# SQL Reference

Complete reference for all SQL functions, views, and catalog tables provided by pgtrickle.

---

## Table of Contents

- [Functions](#functions)
  - [Core Lifecycle](#core-lifecycle)
    - [pgtrickle.create\_stream\_table](#pgtricklecreate_stream_table)
    - [pgtrickle.create\_stream\_table\_fast\_append\_only](#pgtricklecreate_stream_table_fast_append_only)
    - [pgtrickle.create\_stream\_table\_realtime](#pgtricklecreate_stream_table_realtime)
    - [pgtrickle.create\_stream\_table\_batch](#pgtricklecreate_stream_table_batch)
    - [pgtrickle.create\_stream\_table\_cost\_optimized](#pgtricklecreate_stream_table_cost_optimized)
    - [pgtrickle.create\_stream\_table\_if\_not\_exists](#pgtricklecreate_stream_table_if_not_exists)
    - [pgtrickle.create\_or\_replace\_stream\_table](#pgtricklecreate_or_replace_stream_table)
    - [pgtrickle.bulk\_create](#pgtricklebulk_create)
    - [pgtrickle.alter\_stream\_table](#pgtricklealter_stream_table)
    - [pgtrickle.drop\_stream\_table](#pgtrickledrop_stream_table)
    - [pgtrickle.resume\_stream\_table](#pgtrickleresume_stream_table)
    - [pgtrickle.pause\_stream\_table](#pgtricklepause_stream_table)
    - [pgtrickle.set\_stream\_table\_refresh\_policy](#pgtrickleset_stream_table_refresh_policy)
    - [pgtrickle.set\_stream\_table\_storage\_policy](#pgtrickleset_stream_table_storage_policy)
    - [pgtrickle.refresh\_stream\_table](#pgtricklerefresh_stream_table)
    - [pgtrickle.repair\_stream\_table](#pgtricklerepair_stream_table)
  - [Status & Monitoring](#status--monitoring)
    - [pgtrickle.pgt\_status](#pgtricklepgt_status)
    - [pgtrickle.health\_check](#pgtricklehealth_check)
    - [pgtrickle.health\_summary](#pgtricklehealth_summary)
    - [pgtrickle.refresh\_timeline](#pgtricklerefresh_timeline)
    - [pgtrickle.st\_refresh\_stats](#pgtricklest_refresh_stats)
    - [pgtrickle.get\_refresh\_history](#pgtrickleget_refresh_history)
    - [pgtrickle.get\_staleness](#pgtrickleget_staleness)
    - [pgtrickle.explain\_refresh\_mode](#pgtrickleexplain_refresh_mode)
    - [pgtrickle.cache\_stats](#pgtricklecache_stats)
    - [pgtrickle.commit\_latency\_stats](#pgtricklecommit_latency_stats)
    - [pgtrickle.tune\_recommendations](#pgtrickletune_recommendations)
    - [pgtrickle.preview\_stream\_table](#pgtricklepreview_stream_table)
    - [pgtrickle.explain](#pgtrickleexplain)
    - [pgtrickle.explain\_json](#pgtrickleexplain_json)
    - [pgtrickle.stat\_reset](#pgtricklestat_reset)
    - [pgtrickle.stat\_reset\_all](#pgtricklestat_reset_all)
    - [pgtrickle.history\_prune\_status](#pgtricklehistory_prune_status)
    - [pgtrickle.metrics\_summary](#pgtricklemetrics_summary)
  - [CDC Diagnostics](#cdc-diagnostics)
    - [pgtrickle.slot\_health](#pgtrickleslot_health)
    - [pgtrickle.check\_cdc\_health](#pgtricklecheck_cdc_health)
    - [pgtrickle.change\_buffer\_sizes](#pgtricklechange_buffer_sizes)
    - [pgtrickle.trigger\_inventory](#pgtrickletrigger_inventory)
    - [pgtrickle.worker\_pool\_status](#pgtrickleworker_pool_status)
    - [pgtrickle.parallel\_job\_status](#pgtrickleparallel_job_status)
    - [pgtrickle.fuse\_status](#pgtricklefuse_status)
    - [pgtrickle.reset\_fuse](#pgtricklereset_fuse)
    - [pgtrickle.lifecycle\_preflight](#pgtricklelifecycle_preflight)
  - [Dependency & Inspection](#dependency--inspection)
    - [pgtrickle.dependency\_tree](#pgtrickledependency_tree)
    - [pgtrickle.diamond\_groups](#pgtricklediamond_groups)
    - [pgtrickle.pgt\_scc\_status](#pgtricklepgt_scc_status)
    - [pgtrickle.explain\_st](#pgtrickleexplain_st)
    - [pgtrickle.list\_sources](#pgtricklelist_sources)
  - [Utilities](#utilities)
    - [pgtrickle.rebuild\_cdc\_triggers](#pgtricklerebuild_cdc_triggers)
    - [pgtrickle.convert\_buffers\_to\_unlogged](#pgtrickleconvert_buffers_to_unlogged)
    - [pgtrickle.pg\_trickle\_hash](#pgtricklepg_trickle_hash)
    - [pgtrickle.pg\_trickle\_hash\_multi](#pgtricklepg_trickle_hash_multi)
    - [pgtrickle.encode\_row\_id\_v2](#pgtrickleencode_row_id_v2)
    - [pgtrickle.row\_probe\_v1](#pgtricklerow_probe_v1)
  - [Diagnostics](#diagnostics)
    - [pgtrickle.recommend\_refresh\_mode](#pgtricklerecommend_refresh_mode)
    - [pgtrickle.refresh\_efficiency](#pgtricklerefresh_efficiency)
    - [pgtrickle.export\_definition](#pgtrickleexport_definition)
    - [pgtrickle.row\_identity\_v2\_recreation\_preflight](#pgtricklerow_identity_v2_recreation_preflight)
    - [pgtrickle.row\_identity\_v2\_consumer\_inventory](#pgtricklerow_identity_v2_consumer_inventory)
- [Expression Support](#expression-support)
  - [Conditional Expressions](#conditional-expressions)
  - [Comparison Operators](#comparison-operators)
  - [Boolean Tests](#boolean-tests)
  - [SQL Value Functions](#sql-value-functions)
  - [Array and Row Expressions](#array-and-row-expressions)
  - [Subquery Expressions](#subquery-expressions)
    - [ALL (subquery) — Worked Example](#all-subquery--worked-example)
  - [Auto-Rewrite Pipeline](#auto-rewrite-pipeline)
    - [Window Functions in Expressions (Auto-Rewrite)](#window-functions-in-expressions-auto-rewrite)
  - [HAVING Clause](#having-clause)
  - [Tables Without Primary Keys (Keyless Tables)](#tables-without-primary-keys-keyless-tables)
  - [Volatile Function Detection](#volatile-function-detection)
  - [COLLATE Expressions](#collate-expressions)
  - [IS JSON Predicate (PostgreSQL 16+)](#is-json-predicate-postgresql-16)
  - [SQL/JSON Constructors (PostgreSQL 16+)](#sqljson-constructors-postgresql-16)
  - [JSON\_TABLE (PostgreSQL 17+)](#json_table-postgresql-17)
  - [Unsupported Expression Types](#unsupported-expression-types)
- [Restrictions & Interoperability](#restrictions--interoperability)
  - [Referencing Other Stream Tables](#referencing-other-stream-tables)
  - [Views as Sources in Defining Queries](#views-as-sources-in-defining-queries)
  - [Partitioned Tables as Sources](#partitioned-tables-as-sources)
  - [Foreign Tables as Sources](#foreign-tables-as-sources)
  - [IMMEDIATE Mode Query Restrictions](#immediate-mode-query-restrictions)
  - [Logical Replication Targets](#logical-replication-targets)
  - [Views on Stream Tables](#views-on-stream-tables)
  - [Materialized Views on Stream Tables](#materialized-views-on-stream-tables)
  - [Logical Replication of Stream Tables](#logical-replication-of-stream-tables)
  - [Known Delta Computation Limitations](#known-delta-computation-limitations)
  - [What Is NOT Allowed](#what-is-not-allowed)
  - [Row-Level Security (RLS)](#row-level-security-rls)
- [Views](#views)
  - [pgtrickle.stream\_tables\_info](#pgtricklestream_tables_info)
  - [pgtrickle.pg\_stat\_stream\_tables](#pgtricklepg_stat_stream_tables)
  - [pgtrickle.pg\_stat\_pgtrickle](#pgtricklepg_stat_pgtrickle)
  - [pgtrickle.quick\_health](#pgtricklequick_health)
  - [pgtrickle.pgt\_cdc\_status](#pgtricklepgt_cdc_status)
- [Stream Table Lifecycle](#stream-table-lifecycle)
  - [Status values](#status-values)
  - [State transitions](#state-transitions)
  - [Auto-suspend behaviour](#auto-suspend-behaviour)
  - [Reinitialisation](#reinitialisation)
- [Catalog Tables](#catalog-tables)
  - [pgtrickle.pgt\_stream\_tables](#pgtricklepgt_stream_tables)
  - [pgtrickle.pgt\_dependencies](#pgtricklepgt_dependencies)
  - [pgtrickle.pgt\_refresh\_history](#pgtricklepgt_refresh_history)
  - [pgtrickle.pgt\_change\_tracking](#pgtricklepgt_change_tracking)
  - [pgtrickle.pgt\_source\_gates](#pgtricklepgt_source_gates)
  - [pgtrickle.pgt\_refresh\_groups](#pgtricklepgt_refresh_groups)
- [Advanced and Diagnostic Functions](#advanced-and-diagnostic-functions)
- [Delta SQL Profiling](#delta-sql-profiling-v0130)
  - [pgtrickle.explain\_delta](#pgtrickleexplain_delta)
  - [pgtrickle.explain\_delta\_plan](#pgtrickleexplain_delta_plan)
  - [pgtrickle.dedup\_stats](#pgtricklededup_stats)
  - [pgtrickle.shared\_buffer\_stats](#pgtrickleshared_buffer_stats)
- [dbt Integration](#dbt-integration-v0130)
  - [partition\_by config](#partition_by-config)
  - [fuse config](#fuse-config)
- [Stream Table Snapshots](#stream-table-snapshots-v0270)
  - [snapshot\_stream\_table](#pgtricklesnapshotst)
  - [restore\_from\_snapshot](#pgtricklerestorefromsnapshot)
  - [list\_snapshots](#pgtricklelistsnapshots)
  - [drop\_snapshot](#pgtrickledropsnapshot)
- [Transactional Outbox & Consumer Groups](#transactional-outbox--consumer-groups-v0280)
  - [enable\_outbox](#pgtrickleenableoutbox)
  - [disable\_outbox](#pgtrickledisableoutbox)
  - [outbox\_status](#pgtrickleoutboxstatus)
  - [create\_consumer\_group](#pgtricklecreateconsumergroup)
  - [poll\_outbox](#pgtricklepolloutbox)
  - [commit\_offset](#pgtricklecommitoffset)
  - [consumer\_lag](#pgtrickleconsumerlag)
- [Transactional Inbox](#transactional-inbox-v0280)
  - [create\_inbox](#pgtricklecreateinbox)
  - [drop\_inbox](#pgtrickledropinbox)
  - [inbox\_status](#pgtrickleinboxstatus)
  - [enable\_inbox\_ordering](#pgtrickleenableinboxordering)
  - [inbox\_is\_my\_partition](#pgtrickleinboxismypartition)

---

## Functions

> **Parameter naming convention:** Core lifecycle functions use bare,
> user-facing argument names (`name`, `query`, `schedule`, `refresh_mode`).
> Internal wrappers and compatibility overloads use a `p_` prefix
> (`p_name`, `p_retention_hours`). When calling functions with named
> arguments in SQL, prefer bare names: `pgtrickle.create_stream_table(name => 'x', query => 'SELECT 1')`.

<!-- v0.84.0-contract:start -->
## v0.84.0 API contract

The packaged SQL and `scripts/sql_api_policy.json` are the source of truth for
overloads. CI compares each schema/name/identity-argument signature with this
contract; function names alone are not sufficient.

| API class | Default execute policy | Runtime rule |
|---|---|---|
| `public_read` | Explicit grant to `PUBLIC` | Side-effect-free read-only operation |
| `owner_lifecycle` | Revoked from `PUBLIC` | Original caller owns the resolved stream-table relation |
| `admin_global` | Revoked from `PUBLIC` | Original caller is superuser or extension owner |
| `arbitrary_sql` | Revoked from `PUBLIC` | Always invoker; normal SQL privileges apply |
| `trigger_entry` | Revoked from `PUBLIC` | PostgreSQL trigger/event-trigger invocation only |
| `internal` | Revoked from `PUBLIC` | Extension-owned callers only |

Every overload also has an exact `execution_policy` in
[`scripts/sql_api_policy.json`](../scripts/sql_api_policy.json):
`definer_owner_checked` requires the original caller/owner boundary,
`invoker_delegating` preserves the caller's normal SQL privileges for a pure
delegate or arbitrary-SQL wrapper, and `capability_specific` uses the rule in
the API class. CI rejects an exported overload without both fields.

Bulk create/alter/drop inputs are closed and bounded to 1,000 items. Unknown
keys, wrong JSON types, NULL entries, duplicate canonical targets, invalid
numeric ranges, and integer overflow fail before the first mutation. Bulk
operations are atomic.

`pgtrickle.migrate()` is a read-only diagnostic. Only
`ALTER EXTENSION pg_trickle UPDATE` and packaged migration SQL may change the
extension schema or schema-version audit.
<!-- v0.84.0-contract:end -->

### Core Lifecycle

Create, modify, and manage the lifecycle of stream tables.

---

### pgtrickle.create_stream_table

Create a new stream table.

```sql
pgtrickle.create_stream_table(
    name                      text,
    query                     text,
    schedule                  text      DEFAULT 'calculated',
    refresh_mode              text      DEFAULT 'AUTO',
    initialize                bool      DEFAULT true,
    diamond_consistency       text      DEFAULT NULL,
    diamond_schedule_policy   text      DEFAULT NULL,
    cdc_mode                  text      DEFAULT NULL,
    append_only               bool      DEFAULT false,
    pooler_compatibility_mode bool      DEFAULT false,
    partition_by              text      DEFAULT NULL,
    max_differential_joins    integer   DEFAULT NULL,
    max_delta_fraction        float8    DEFAULT NULL,
    output_distribution_column text     DEFAULT NULL,
    temporal                  bool      DEFAULT false,
    storage_backend           text      DEFAULT NULL,
    sink                      text      DEFAULT NULL,
) → void
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `text` | — | Name of the stream table. May be schema-qualified (`myschema.my_st`). Defaults to `public` schema. |
| `query` | `text` | — | The defining SQL query. Must be a valid SELECT statement using supported operators. |
| `schedule` | `text` | `'calculated'` | Refresh schedule as a Prometheus/GNU-style duration string (e.g., `'30s'`, `'5m'`, `'1h'`, `'1h30m'`, `'1d'`) **or** a cron expression (e.g., `'*/5 * * * *'`, `'@hourly'`). Use `'calculated'` for CALCULATED mode (inherits schedule from downstream dependents). |
| `refresh_mode` | `text` | `'AUTO'` | `'AUTO'` (adaptive — uses DIFFERENTIAL when possible, falls back to FULL if the query is not differentiable), `'FULL'` (truncate and reload), `'DIFFERENTIAL'` (apply delta only — errors if the query is not differentiable), or `'IMMEDIATE'` (synchronous in-transaction maintenance via statement-level triggers). |
| `initialize` | `bool` | `true` | If `true`, populates the table immediately via a full refresh. If `false`, creates the table empty. |
| `diamond_consistency` | `text` | `NULL` (defaults to `'atomic'`) | Diamond dependency consistency mode: `'atomic'` (SAVEPOINT-based atomic group refresh) or `'none'` (independent refresh). |
| `diamond_schedule_policy` | `text` | `NULL` (defaults to `'fastest'`) | Schedule policy for atomic diamond groups: `'fastest'` (fire when any member is due) or `'slowest'` (fire when all are due). Set on the convergence node. |
| `cdc_mode` | `text` | `NULL` (use `pg_trickle.cdc_mode`) | Optional per-stream-table CDC override: `'auto'`, `'trigger'`, or `'wal'`. This affects all deferred TABLE sources of the stream table. |
| `append_only` | `bool` | `false` | When `true`, differential refreshes use a fast INSERT path instead of MERGE. Skips DELETE/UPDATE/IS DISTINCT FROM checks. If a DELETE or Update is later detected in the change buffer, the flag is automatically reverted to `false`. Not compatible with `FULL`, `IMMEDIATE`, or keyless sources. |
| `pooler_compatibility_mode` | `bool` | `false` | When `true`, the refresh engine uses inline SQL instead of `PREPARE`/`EXECUTE` and suppresses all `NOTIFY` emissions for this stream table. Enable this when the stream table is accessed through a transaction-mode connection pooler (e.g. PgBouncer). |
| `partition_by` | `text` | `NULL` | Partition key column. When set, the storage table is created with `PARTITION BY RANGE (column)`. Only effective at creation time. |
| `max_differential_joins` | `integer` | `NULL` (use global default) | Per-stream-table cap on the number of join combinations examined during differential refresh. Overrides `pg_trickle.max_differential_joins`. |
| `max_delta_fraction` | `float8` | `NULL` (use global default) | Per-stream-table threshold (0.0–1.0) for the AUTO cost model. If the estimated delta-to-table-size ratio exceeds this fraction, AUTO falls back to FULL. Overrides `pg_trickle.differential_max_change_ratio`. |
| `output_distribution_column` | `text` | `NULL` | Citus only: distribution column for the stream table storage table. Must be a column present in the query output. |
| `temporal` | `bool` | `false` | When `true`, enables temporal IVM mode — the stream table tracks effective-time ranges and can answer as-of queries. |
| `storage_backend` | `text` | `NULL` | Columnar storage backend to use for the stream table storage table (e.g. `'hydra'`, `'citus_columnar'`). When set, differential refreshes use the `delete_insert` strategy (columnar backends are append-only). |

When `refresh_mode => 'IMMEDIATE'`, the cluster-wide `pg_trickle.cdc_mode`
setting is ignored. IMMEDIATE mode always uses statement-level IVM triggers
instead of CDC triggers or WAL replication slots. If you explicitly pass
`cdc_mode => 'wal'` together with `refresh_mode => 'IMMEDIATE'`, pg_trickle
rejects the call because WAL CDC is asynchronous and incompatible with
in-transaction maintenance.

**Duration format:**

| Unit | Suffix | Example |
|---|---|---|
| Seconds | `s` | `'30s'` |
| Minutes | `m` | `'5m'` |
| Hours | `h` | `'2h'` |
| Days | `d` | `'1d'` |
| Weeks | `w` | `'1w'` |
| Compound | — | `'1h30m'`, `'2m30s'` |

**Cron expression format:**

`schedule` also accepts standard cron expressions for time-based scheduling. The scheduler refreshes the stream table when the cron schedule fires, rather than checking staleness.

| Format | Fields | Example | Description |
|---|---|---|---|
| 5-field | min hour dom mon dow | `'*/5 * * * *'` | Every 5 minutes |
| 6-field | sec min hour dom mon dow | `'0 */5 * * * *'` | Every 5 minutes at :00 seconds |
| Alias | — | `'@hourly'` | Every hour |
| Alias | — | `'@daily'` | Every day at midnight |
| Alias | — | `'@weekly'` | Every Sunday at midnight |
| Alias | — | `'@monthly'` | First of every month |
| Weekday range | — | `'0 6 * * 1-5'` | 6 AM on weekdays |

> **Note:** Cron-scheduled stream tables do not participate in CALCULATED schedule resolution. The `stale` column in monitoring views returns `NULL` for cron-scheduled tables.

**Schedule mode quick-reference:**

| Mode | `schedule` value | `refresh_mode` | Refresh trigger | Use when |
|------|-----------------|----------------|-----------------|----------|
| **Duration** | `'30s'`, `'5m'`, `'1h'` | Any | Scheduler ticks when staleness ≥ duration | You want data freshened on a fixed cadence |
| **Cron** | `'*/5 * * * *'`, `'@hourly'` | Any | Scheduler fires at each cron tick | You need time-based scheduling (off-peak ETL, daily reports) |
| **CALCULATED** | `'calculated'` | `AUTO`/`DIFFERENTIAL` | Fired when a downstream dependent is due | Intermediate nodes in a DAG; let dependents drive timing |
| **IMMEDIATE** | *(ignored; set to `NULL`)* | `'IMMEDIATE'` | Every source-table write, within the same transaction | Zero-lag dashboards, read-your-writes; query must be IVM-eligible |

> **Note:** IMMEDIATE mode does not use the scheduler at all — `schedule` is
> cleared and the `active_workers` counter is unaffected. See
> [IMMEDIATE Mode Query Restrictions](#immediate-mode-query-restrictions) for
> supported query patterns.

**Example:**

```sql
-- Duration-based: refresh when data is staler than 2 minutes (refresh_mode defaults to 'AUTO')
SELECT pgtrickle.create_stream_table(
    name     => 'order_totals',
    query    => 'SELECT region, SUM(amount) AS total FROM orders GROUP BY region',
    schedule => '2m'
);

-- Cron-based: refresh every hour
SELECT pgtrickle.create_stream_table(
    name         => 'hourly_summary',
    query        => 'SELECT date_trunc(''hour'', ts), COUNT(*) FROM events GROUP BY 1',
    schedule     => '@hourly',
    refresh_mode => 'FULL'
);

-- Cron-based: refresh at 6 AM on weekdays
SELECT pgtrickle.create_stream_table(
    name         => 'daily_report',
    query        => 'SELECT region, SUM(revenue) AS total FROM sales GROUP BY region',
    schedule     => '0 6 * * 1-5',
    refresh_mode => 'FULL'
);

-- Immediate mode: maintained synchronously within the same transaction
-- No schedule needed — updates happen automatically when base table changes
SELECT pgtrickle.create_stream_table(
    name         => 'live_totals',
    query        => 'SELECT region, SUM(amount) AS total FROM orders GROUP BY region',
    refresh_mode => 'IMMEDIATE'
);

-- Force WAL CDC for this stream table even if the global GUC is 'trigger'
SELECT pgtrickle.create_stream_table(
    name         => 'wal_orders',
    query        => 'SELECT id, amount FROM orders',
    schedule     => '1s',
    refresh_mode => 'DIFFERENTIAL',
    cdc_mode     => 'wal'
);
```

**Aggregate Examples:**

All supported aggregate functions work in AUTO mode (and all other modes).
Examples below omit `refresh_mode` — the default `'AUTO'` selects DIFFERENTIAL automatically.
Explicit modes are shown only when the mode itself is being demonstrated.

```sql
-- Algebraic aggregates (fully differential — no rescan needed)
SELECT pgtrickle.create_stream_table(
    name     => 'sales_summary',
    query    => 'SELECT region, COUNT(*) AS cnt, SUM(amount) AS total, AVG(amount) AS avg_amount
     FROM orders GROUP BY region',
    schedule => '1m'
);

-- Semi-algebraic aggregates (MIN/MAX)
SELECT pgtrickle.create_stream_table(
    name     => 'salary_ranges',
    query    => 'SELECT department, MIN(salary) AS min_sal, MAX(salary) AS max_sal
     FROM employees GROUP BY department',
    schedule => '2m'
);

-- Group-rescan aggregates (BOOL_AND/OR, STRING_AGG, ARRAY_AGG, JSON_AGG, JSONB_AGG,
--                          BIT_AND, BIT_OR, BIT_XOR, JSON_OBJECT_AGG, JSONB_OBJECT_AGG,
--                          STDDEV, STDDEV_POP, STDDEV_SAMP, VARIANCE, VAR_POP, VAR_SAMP,
--                          MODE, PERCENTILE_CONT, PERCENTILE_DISC,
--                          CORR, COVAR_POP, COVAR_SAMP, REGR_AVGX, REGR_AVGY,
--                          REGR_COUNT, REGR_INTERCEPT, REGR_R2, REGR_SLOPE,
--                          REGR_SXX, REGR_SXY, REGR_SYY, ANY_VALUE)
SELECT pgtrickle.create_stream_table(
    name     => 'team_members',
    query    => 'SELECT department,
            STRING_AGG(name, '', '' ORDER BY name) AS members,
            ARRAY_AGG(employee_id) AS member_ids,
            BOOL_AND(active) AS all_active,
            JSON_AGG(name) AS members_json
     FROM employees
     GROUP BY department',
    schedule => '1m'
);

-- Bitwise aggregates
SELECT pgtrickle.create_stream_table(
    name     => 'permission_summary',
    query    => 'SELECT department,
            BIT_OR(permissions) AS combined_perms,
            BIT_AND(permissions) AS common_perms,
            BIT_XOR(flags) AS xor_flags
     FROM employees
     GROUP BY department',
    schedule => '1m'
);

-- JSON object aggregates
SELECT pgtrickle.create_stream_table(
    name     => 'config_map',
    query    => 'SELECT department,
            JSON_OBJECT_AGG(setting_name, setting_value) AS settings,
            JSONB_OBJECT_AGG(key, value) AS metadata
     FROM config
     GROUP BY department',
    schedule => '1m'
);

-- Statistical aggregates
SELECT pgtrickle.create_stream_table(
    name     => 'salary_stats',
    query    => 'SELECT department,
            STDDEV_POP(salary) AS sd_pop,
            STDDEV_SAMP(salary) AS sd_samp,
            VAR_POP(salary) AS var_pop,
            VAR_SAMP(salary) AS var_samp
     FROM employees
     GROUP BY department',
    schedule => '1m'
);

-- Ordered-set aggregates (MODE, PERCENTILE_CONT, PERCENTILE_DISC)
SELECT pgtrickle.create_stream_table(
    name     => 'salary_percentiles',
    query    => 'SELECT department,
            MODE() WITHIN GROUP (ORDER BY grade) AS most_common_grade,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary,
            PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY salary) AS p90_salary
     FROM employees
     GROUP BY department',
    schedule => '1m'
);

-- Regression / correlation aggregates (CORR, COVAR_*, REGR_*)
SELECT pgtrickle.create_stream_table(
    name     => 'regression_stats',
    query    => 'SELECT department,
            CORR(salary, experience) AS sal_exp_corr,
            COVAR_POP(salary, experience) AS covar_pop,
            COVAR_SAMP(salary, experience) AS covar_samp,
            REGR_SLOPE(salary, experience) AS slope,
            REGR_INTERCEPT(salary, experience) AS intercept,
            REGR_R2(salary, experience) AS r_squared,
            REGR_COUNT(salary, experience) AS regr_n
     FROM employees
     GROUP BY department',
    schedule => '1m'
);

-- ANY_VALUE aggregate (PostgreSQL 16+)
SELECT pgtrickle.create_stream_table(
    name     => 'dept_sample',
    query    => 'SELECT department, ANY_VALUE(office_location) AS sample_office
     FROM employees GROUP BY department',
    schedule => '1m'
);

-- FILTER clause on aggregates
SELECT pgtrickle.create_stream_table(
    name     => 'order_metrics',
    query    => 'SELECT region,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE status = ''active'') AS active_count,
            SUM(amount) FILTER (WHERE status = ''shipped'') AS shipped_total
     FROM orders
     GROUP BY region',
    schedule => '1m'
);

-- PgBouncer compatibility (transaction-mode pooler)
SELECT pgtrickle.create_stream_table(
    name                      => 'pooled_orders',
    query                     => 'SELECT id, amount FROM orders',
    schedule                  => '5m',
    pooler_compatibility_mode => true
);
```

**CTE Examples:**

Non-recursive CTEs are fully supported in both FULL and DIFFERENTIAL modes:

```sql
-- Simple CTE
SELECT pgtrickle.create_stream_table(
    name     => 'active_order_totals',
    query    => 'WITH active_users AS (
        SELECT id, name FROM users WHERE active = true
    )
    SELECT a.id, a.name, SUM(o.amount) AS total
    FROM active_users a
    JOIN orders o ON o.user_id = a.id
    GROUP BY a.id, a.name',
    schedule => '1m'
);

-- Chained CTEs (CTE referencing another CTE)
SELECT pgtrickle.create_stream_table(
    name     => 'top_regions',
    query    => 'WITH regional AS (
        SELECT region, SUM(amount) AS total FROM orders GROUP BY region
    ),
    ranked AS (
        SELECT region, total FROM regional WHERE total > 1000
    )
    SELECT * FROM ranked',
    schedule => '2m'
);

-- Multi-reference CTE (referenced twice in FROM — shared delta optimization)
SELECT pgtrickle.create_stream_table(
    name     => 'self_compare',
    query    => 'WITH totals AS (
        SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id
    )
    SELECT t1.user_id, t1.total, t2.total AS next_total
    FROM totals t1
    JOIN totals t2 ON t1.user_id = t2.user_id + 1',
    schedule => '1m'
);

-- Append-only stream table (INSERT-only fast path)
SELECT pgtrickle.create_stream_table(
    name        => 'event_log_st',
    query       => 'SELECT id, event_type, payload, created_at FROM events',
    schedule    => '30s',
    append_only => true
);
```

Recursive CTEs work with FULL, DIFFERENTIAL, and IMMEDIATE modes:

```sql
-- Recursive CTE (hierarchy traversal)
SELECT pgtrickle.create_stream_table(
    name         => 'category_tree',
    query        => 'WITH RECURSIVE cat_tree AS (
        SELECT id, name, parent_id, 0 AS depth
        FROM categories WHERE parent_id IS NULL
        UNION ALL
        SELECT c.id, c.name, c.parent_id, ct.depth + 1
        FROM categories c
        JOIN cat_tree ct ON c.parent_id = ct.id
    )
    SELECT * FROM cat_tree',
    schedule     => '5m',
    refresh_mode => 'FULL'  -- FULL mode: standard re-execution
);

-- Recursive CTE with DIFFERENTIAL mode (incremental semi-naive / DRed)
SELECT pgtrickle.create_stream_table(
    name         => 'org_chart',
    query        => 'WITH RECURSIVE reports AS (
        SELECT id, name, manager_id FROM employees WHERE manager_id IS NULL
        UNION ALL
        SELECT e.id, e.name, e.manager_id
        FROM employees e JOIN reports r ON e.manager_id = r.id
    )
    SELECT * FROM reports',
    schedule     => '2m',
    refresh_mode => 'DIFFERENTIAL'  -- Uses semi-naive, DRed, or recomputation (auto-selected)
);

-- Recursive CTE with IMMEDIATE mode (same-transaction maintenance)
SELECT pgtrickle.create_stream_table(
    name         => 'org_chart_live',
    query        => 'WITH RECURSIVE reports AS (
        SELECT id, name, manager_id FROM employees WHERE manager_id IS NULL
        UNION ALL
        SELECT e.id, e.name, e.manager_id
        FROM employees e JOIN reports r ON e.manager_id = r.id
    )
    SELECT * FROM reports',
    refresh_mode => 'IMMEDIATE'  -- Uses transition tables with semi-naive / DRed maintenance
);
```

> **Non-monotone recursive terms:** If the recursive term contains operators
> like `EXCEPT`, aggregate functions, window functions, `DISTINCT`, `INTERSECT`
> (set), or anti-joins, the system automatically falls back to recomputation
> to guarantee correctness. Semi-naive and DRed strategies require monotone
> recursive terms (JOIN, UNION ALL, filter/project only).

**Set Operation Examples:**

`INTERSECT`, `INTERSECT ALL`, `EXCEPT`, `EXCEPT ALL`, `UNION`, and `UNION ALL`
are supported in FULL mode.  INTERSECT/EXCEPT forms are currently guarded
from DIFFERENTIAL and IMMEDIATE admission; AUTO falls back to FULL.

```sql
-- INTERSECT: customers who placed orders in BOTH regions
SELECT pgtrickle.create_stream_table(
    name     => 'bi_region_customers',
    query    => 'SELECT customer_id FROM orders_east
     INTERSECT
     SELECT customer_id FROM orders_west',
    schedule => '2m'
);

-- INTERSECT ALL: preserves duplicates (bag semantics)
SELECT pgtrickle.create_stream_table(
    name     => 'common_items',
    query    => 'SELECT item_name FROM warehouse_a
     INTERSECT ALL
     SELECT item_name FROM warehouse_b',
    schedule => '1m'
);

-- EXCEPT: orders not yet shipped
SELECT pgtrickle.create_stream_table(
    name     => 'unshipped_orders',
    query    => 'SELECT order_id FROM orders
     EXCEPT
     SELECT order_id FROM shipments',
    schedule => '1m'
);

-- EXCEPT ALL: preserves duplicate counts (bag subtraction)
SELECT pgtrickle.create_stream_table(
    name     => 'excess_inventory',
    query    => 'SELECT sku FROM stock_received
     EXCEPT ALL
     SELECT sku FROM stock_shipped',
    schedule => '5m'
);

-- UNION: deduplicated merge of two sources
SELECT pgtrickle.create_stream_table(
    name     => 'all_contacts',
    query    => 'SELECT email FROM customers
     UNION
     SELECT email FROM newsletter_subscribers',
    schedule => '5m'
);
```

**LATERAL Set-Returning Function Examples:**

Set-returning functions (SRFs) in the FROM clause are supported in both FULL and DIFFERENTIAL modes. Common SRFs include `jsonb_array_elements`, `jsonb_each`, `jsonb_each_text`, and `unnest`:

```sql
-- Flatten JSONB arrays into rows
SELECT pgtrickle.create_stream_table(
    name     => 'flat_children',
    query    => 'SELECT p.id, child.value AS val
     FROM parent_data p,
     jsonb_array_elements(p.data->''children'') AS child',
    schedule => '1m'
);

-- Expand JSONB key-value pairs (multi-column SRF)
SELECT pgtrickle.create_stream_table(
    name     => 'flat_properties',
    query    => 'SELECT d.id, kv.key, kv.value
     FROM documents d,
     jsonb_each(d.metadata) AS kv',
    schedule => '2m'
);

-- Unnest arrays
SELECT pgtrickle.create_stream_table(
    name     => 'flat_tags',
    query    => 'SELECT t.id, tag.tag
     FROM tagged_items t,
     unnest(t.tags) AS tag(tag)',
    schedule => '1m'
);

-- SRF with WHERE filter
SELECT pgtrickle.create_stream_table(
    name     => 'high_value_items',
    query    => 'SELECT p.id, (e.value)::int AS amount
     FROM products p,
     jsonb_array_elements(p.prices) AS e
     WHERE (e.value)::int > 100',
    schedule => '5m'
);

-- SRF combined with aggregation
SELECT pgtrickle.create_stream_table(
    name         => 'element_counts',
    query        => 'SELECT a.id, count(*) AS cnt
     FROM arrays a,
     jsonb_array_elements(a.data) AS e
     GROUP BY a.id',
    schedule     => '1m',
    refresh_mode => 'FULL'
);
```

**LATERAL Subquery Examples:**

LATERAL subqueries in the FROM clause are supported in both FULL and DIFFERENTIAL modes. Use them for top-N per group, correlated aggregation, and conditional expansion:

```sql
-- Top-N per group: latest item per order
SELECT pgtrickle.create_stream_table(
    name     => 'latest_items',
    query    => 'SELECT o.id, o.customer, latest.amount
     FROM orders o,
     LATERAL (
         SELECT li.amount
         FROM line_items li
         WHERE li.order_id = o.id
         ORDER BY li.created_at DESC
         LIMIT 1
     ) AS latest',
    schedule => '1m'
);

-- Correlated aggregate
SELECT pgtrickle.create_stream_table(
    name     => 'dept_summaries',
    query    => 'SELECT d.id, d.name, stats.total, stats.cnt
     FROM departments d,
     LATERAL (
         SELECT SUM(e.salary) AS total, COUNT(*) AS cnt
         FROM employees e
         WHERE e.dept_id = d.id
     ) AS stats',
    schedule => '1m'
);

-- LEFT JOIN LATERAL: preserve outer rows with NULLs when subquery returns no rows
SELECT pgtrickle.create_stream_table(
    name     => 'dept_stats_all',
    query    => 'SELECT d.id, d.name, stats.total
     FROM departments d
     LEFT JOIN LATERAL (
         SELECT SUM(e.salary) AS total
         FROM employees e
         WHERE e.dept_id = d.id
     ) AS stats ON true',
    schedule => '1m'
);
```

**WHERE Subquery Examples:**

Subqueries in the `WHERE` clause are automatically transformed into semi-join, anti-join, or scalar subquery operators in the DVM operator tree:

```sql
-- EXISTS subquery: customers who have placed orders
SELECT pgtrickle.create_stream_table(
    name     => 'active_customers',
    query    => 'SELECT c.id, c.name
     FROM customers c
     WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)',
    schedule => '1m'
);

-- NOT EXISTS: customers with no orders
SELECT pgtrickle.create_stream_table(
    name     => 'inactive_customers',
    query    => 'SELECT c.id, c.name
     FROM customers c
     WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)',
    schedule => '1m'
);

-- IN subquery: products that have been ordered
SELECT pgtrickle.create_stream_table(
    name     => 'ordered_products',
    query    => 'SELECT p.id, p.name
     FROM products p
     WHERE p.id IN (SELECT product_id FROM order_items)',
    schedule => '1m'
);

-- NOT IN subquery: products never ordered
SELECT pgtrickle.create_stream_table(
    name     => 'unordered_products',
    query    => 'SELECT p.id, p.name
     FROM products p
     WHERE p.id NOT IN (SELECT product_id FROM order_items)',
    schedule => '1m'
);

-- Scalar subquery in SELECT list
SELECT pgtrickle.create_stream_table(
    name     => 'products_with_max_price',
    query    => 'SELECT p.id, p.name, (SELECT max(price) FROM products) AS max_price
     FROM products p',
    schedule => '1m'
);
```

**Notes:**
- The defining query is parsed into an operator tree and validated for DVM support.
- **Views as sources** — views referenced in the defining query are automatically inlined as subqueries (auto-rewrite pass #0). CDC triggers are created on the underlying base tables. Nested views (view → view → table) are fully expanded. The user's original query is preserved in `original_query` for reinit and introspection. Materialized views are rejected in DIFFERENTIAL mode (use FULL mode or the underlying query directly). Foreign tables are also rejected in DIFFERENTIAL mode.
- CDC triggers and change buffer tables are created automatically for each source table.
- **TRUNCATE on source tables** — when a source table is TRUNCATEd, a CDC trigger writes a marker row (`action='T'`) into the change buffer. On the next refresh cycle, pg_trickle detects the marker and automatically falls back to a FULL refresh. For single-source stream tables where no subsequent DML occurred after the TRUNCATE, an optimized fast path deletes all ST rows directly without re-running the full defining query.
- The ST is registered in the dependency DAG; cycles are rejected.
- Non-recursive CTEs are inlined as subqueries during parsing (Tier 1). Multi-reference CTEs share delta computation (Tier 2).
- Recursive CTEs in DIFFERENTIAL mode use three strategies, auto-selected per refresh: **semi-naive evaluation** for INSERT-only changes, **DRed (Delete-and-Rederive)** for mixed DELETE/UPDATE changes, and **recomputation fallback** when CTE columns do not match ST storage columns. **Non-monotone recursive terms** (containing EXCEPT, Aggregate, Window, DISTINCT, AntiJoin, or INTERSECT SET) automatically fall back to recomputation to ensure correctness.

> **Recursive CTE DIFFERENTIAL mode -- DRed algorithm (P2-1)**
> In DIFFERENTIAL mode, mixed DELETE/UPDATE changes now use the DRed (Delete-and-Rederive)
> algorithm: (1) semi-naive INSERT propagation; (2) over-deletion cascade from ST storage;
> (3) rederivation from current source tables; (4) combine net deletions. DRed correctly
> handles derived-column changes such as path rebuilds under a renamed ancestor node.
> When CTE output columns differ from ST storage columns (mismatch), recomputation is used.
> Implemented in P2-1.
- LATERAL SRFs in DIFFERENTIAL mode use row-scoped recomputation: when a source row changes, only the SRF expansions for that row are re-evaluated.
- LATERAL subqueries in DIFFERENTIAL mode also use row-scoped recomputation: when an outer row changes, the correlated subquery is re-executed only for that row.
- WHERE subqueries (`EXISTS`, `IN`, scalar) are parsed into dedicated semi-join, anti-join, and scalar subquery operators with specialized delta computation.
- `ALL (subquery)` is the only subquery form that is currently rejected.
- **ORDER BY** is accepted but silently discarded — row order in the storage table is undefined (consistent with PostgreSQL's `CREATE MATERIALIZED VIEW` behavior). Apply ORDER BY when *querying* the stream table.
- **TopK (ORDER BY + LIMIT)** — When a top-level `ORDER BY … LIMIT N` is present (with a constant integer limit, optionally with `OFFSET M`), the query is recognized as a "TopK" pattern and accepted. TopK stream tables store exactly N rows (starting from position M+1 if OFFSET is specified) and are refreshed via a scoped-recomputation MERGE strategy. The DVM delta pipeline is bypassed; instead, each refresh re-evaluates the full ORDER BY + LIMIT [+ OFFSET] query and merges the result into the storage table. The catalog records `topk_limit`, `topk_order_by`, and optionally `topk_offset` for the stream table. TopK is not supported with set operations (UNION/INTERSECT/EXCEPT) or GROUP BY ROLLUP/CUBE/GROUPING SETS.
- **LIMIT / OFFSET** without ORDER BY are rejected — stream tables materialize the full result set. Apply LIMIT when querying the stream table.

---

---

### pgtrickle.create_stream_table_fast_append_only

Convenience wrapper for creating an append-only DIFFERENTIAL stream table
in a single call. Equivalent to calling `pgtrickle.create_stream_table` with
`append_only => true` and `refresh_mode => 'DIFFERENTIAL'`.

```sql
pgtrickle.create_stream_table_fast_append_only(
    name               text,
    query              text,
    schedule           text    DEFAULT 'calculated',
    cdc_mode           text    DEFAULT NULL,
    partition_by       text    DEFAULT NULL,
    max_differential_joins  integer          DEFAULT NULL,
    max_delta_fraction       double precision DEFAULT NULL
) → void
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `text` | — | Name of the stream table to create. |
| `query` | `text` | — | SQL SELECT query to materialize. |
| `schedule` | `text` | `'calculated'` | Refresh schedule (cron, interval, or `'calculated'`). |
| `cdc_mode` | `text` | `NULL` | CDC mode override (`'TRIGGER'`, `'WAL'`, or `NULL` for auto). |
| `partition_by` | `text` | `NULL` | Partition expression (passed to `CREATE TABLE … PARTITION BY`). |
| `max_differential_joins` | `integer` | `NULL` | Cap on the number of delta joins per refresh cycle. |
| `max_delta_fraction` | `double precision` | `NULL` | Max fraction of source rows to process as a delta before falling back to full refresh. |

**Example:**

```sql
-- Append-only event log of completed orders
SELECT pgtrickle.create_stream_table_fast_append_only(
    'completed_orders',
    'SELECT id, total, completed_at FROM orders WHERE status = ''completed'''
);
```

**Notes:**
- Implicitly sets `append_only = true` and `refresh_mode = 'DIFFERENTIAL'`.
- Append-only tables never issue DELETE during refresh — safe for audit logs and
  event streams.
- The stream table is initialized immediately; use `schedule` to control ongoing
  cadence.

---

### pgtrickle.create_stream_table_realtime

Convenience preset for creating a low-latency DIFFERENTIAL stream table with a
1-second refresh schedule.  Equivalent to calling `pgtrickle.create_stream_table`
with `schedule => '1s'` and `refresh_mode => 'DIFFERENTIAL'`.

```sql
pgtrickle.create_stream_table_realtime(
    name   text,
    query  text
) → void
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `text` | — | Name of the stream table to create. |
| `query` | `text` | — | SQL SELECT query to materialize. |

**Example:**

```sql
SELECT pgtrickle.create_stream_table_realtime(
    'live_order_counts',
    'SELECT status, count(*) FROM orders GROUP BY status'
);
```

---

### pgtrickle.create_stream_table_batch

Convenience preset for creating an AUTO-mode stream table with a 5-minute
refresh schedule.  Equivalent to calling `pgtrickle.create_stream_table` with
`schedule => '5m'` and `refresh_mode => 'AUTO'`.

```sql
pgtrickle.create_stream_table_batch(
    name   text,
    query  text
) → void
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `text` | — | Name of the stream table to create. |
| `query` | `text` | — | SQL SELECT query to materialize. |

**Example:**

```sql
SELECT pgtrickle.create_stream_table_batch(
    'hourly_revenue',
    'SELECT date_trunc(''hour'', created_at) AS hour, sum(amount) FROM orders GROUP BY 1'
);
```

---

### pgtrickle.create_stream_table_cost_optimized

Convenience preset for creating a cost-optimized AUTO-mode stream table with a
15-minute refresh schedule.  Equivalent to calling `pgtrickle.create_stream_table`
with `schedule => '15m'` and `refresh_mode => 'AUTO'`.

```sql
pgtrickle.create_stream_table_cost_optimized(
    name   text,
    query  text
) → void
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `text` | — | Name of the stream table to create. |
| `query` | `text` | — | SQL SELECT query to materialize. |

**Example:**

```sql
SELECT pgtrickle.create_stream_table_cost_optimized(
    'daily_kpi_cache',
    'SELECT date_trunc(''day'', ts) AS day, count(*) AS events FROM activity GROUP BY 1'
);
```

---

### pgtrickle.create_stream_table_if_not_exists

Create a stream table if it does not already exist. If a stream table with the
given name already exists, this is a silent no-op (an `INFO` message is logged).
The existing definition is never modified.

```sql
pgtrickle.create_stream_table_if_not_exists(
    name                      text,
    query                     text,
    schedule                  text      DEFAULT 'calculated',
    refresh_mode              text      DEFAULT 'AUTO',
    initialize                bool      DEFAULT true,
    diamond_consistency       text      DEFAULT NULL,
    diamond_schedule_policy   text      DEFAULT NULL,
    cdc_mode                  text      DEFAULT NULL,
    append_only               bool      DEFAULT false,
    pooler_compatibility_mode bool      DEFAULT false,
    partition_by              text      DEFAULT NULL,
    max_differential_joins    integer   DEFAULT NULL,
    max_delta_fraction        float8    DEFAULT NULL,
    output_distribution_column text     DEFAULT NULL,
    temporal                  bool      DEFAULT false,
    storage_backend           text      DEFAULT NULL,
    sink                      text      DEFAULT NULL,
) → void
```

**Parameters:** Same as [`create_stream_table`](#pgtricklecreate_stream_table).

**Example:**

```sql
-- Safe to re-run in migrations:
SELECT pgtrickle.create_stream_table_if_not_exists(
    'order_totals',
    'SELECT customer_id, sum(amount) AS total FROM orders GROUP BY customer_id',
    '1m',
    'DIFFERENTIAL'
);
```

**Notes:**
- Useful for deployment / migration scripts that should be safe to re-run.
- If the stream table already exists, the provided `query`, `schedule`, and other parameters are ignored — the existing definition is preserved.

---

### pgtrickle.create_or_replace_stream_table

Create a stream table if it does not exist, or replace the existing one if the definition changed. This is the **declarative, idempotent** API for deployment workflows (dbt, SQL migrations, GitOps).

```sql
pgtrickle.create_or_replace_stream_table(
    name                      text,
    query                     text,
    schedule                  text      DEFAULT 'calculated',
    refresh_mode              text      DEFAULT 'AUTO',
    initialize                bool      DEFAULT true,
    diamond_consistency       text      DEFAULT NULL,
    diamond_schedule_policy   text      DEFAULT NULL,
    cdc_mode                  text      DEFAULT NULL,
    append_only               bool      DEFAULT false,
    pooler_compatibility_mode bool      DEFAULT false,
    partition_by              text      DEFAULT NULL,
    max_differential_joins    integer   DEFAULT NULL,
    max_delta_fraction        float8    DEFAULT NULL,
    output_distribution_column text     DEFAULT NULL,
    temporal                  bool      DEFAULT false,
    storage_backend           text      DEFAULT NULL
) → void
```


**Behavior:**

| Current state | Action taken |
|---|---|
| Stream table does **not** exist | **Create** — identical to `create_stream_table(...)` |
| Stream table exists, query **and** all config identical | **No-op** — logs INFO, returns immediately |
| Stream table exists, query identical but config differs | **Alter config** — delegates to `alter_stream_table(...)` for schedule, refresh_mode, diamond settings, cdc_mode, append_only, pooler_compatibility_mode, partition_by, max_differential_joins, max_delta_fraction, output_distribution_column, temporal, storage_backend |
| Stream table exists, query differs | **Replace query** — in-place ALTER QUERY migration plus any config changes; a full refresh is applied |

The `initialize` parameter is honoured on **create** only. On replace, the stream table is always repopulated via a full refresh.

Query comparison uses the post-rewrite (normalized) form of the SQL. Cosmetic differences such as whitespace, casing, and extra parentheses are ignored.

**Example:**

```sql
-- Idempotent deployment — safe to run on every deploy:
SELECT pgtrickle.create_or_replace_stream_table(
    name         => 'order_totals',
    query        => 'SELECT region, SUM(amount) AS total FROM orders GROUP BY region',
    schedule     => '2m',
    refresh_mode => 'DIFFERENTIAL'
);

-- If the query changed since last deploy, the stream table is
-- migrated in place (no data gap). If nothing changed, it's a no-op.
```

**Notes:**
- Mirrors PostgreSQL's `CREATE OR REPLACE` convention (`CREATE OR REPLACE VIEW`, `CREATE OR REPLACE FUNCTION`).
- Never drops the stream table — even for incompatible schema changes, the ALTER QUERY path rebuilds storage in place while preserving the catalog entry (`pgt_id`).
- For migration scripts that should **not** modify an existing definition, use [`create_stream_table_if_not_exists`](#pgtricklecreate_stream_table_if_not_exists) instead.

---

### pgtrickle.bulk_create

Create multiple stream tables in a single transaction.

```sql
pgtrickle.bulk_create(
    definitions  jsonb     -- Array of stream table definitions
) → jsonb                  -- Array of result objects
```

Each element in the `definitions` array must be a JSON object with at least `name` and `query` keys. All other keys match the parameters of `create_stream_table` (snake_case):

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `name` | `string` | (required) | Stream table name (optionally schema-qualified). |
| `query` | `string` | (required) | Defining SQL query. |
| `schedule` | `string` | `'calculated'` | Refresh schedule. |
| `refresh_mode` | `string` | `'AUTO'` | `'AUTO'`, `'FULL'`, `'DIFFERENTIAL'`, or `'IMMEDIATE'`. |
| `initialize` | `boolean` | `true` | Whether to populate immediately. |
| `diamond_consistency` | `string` | `NULL` | `'atomic'` or `'none'`. |
| `diamond_schedule_policy` | `string` | `NULL` | `'fastest'` or `'slowest'`. |
| `cdc_mode` | `string` | `NULL` | `'auto'`, `'trigger'`, or `'wal'`. |
| `append_only` | `boolean` | `false` | Enable append-only fast path. |
| `pooler_compatibility_mode` | `boolean` | `false` | PgBouncer compatibility. |
| `partition_by` | `string` | `NULL` | Partition key. |
| `max_differential_joins` | `integer` | `NULL` | Max join scan limit. |
| `max_delta_fraction` | `number` | `NULL` | Max delta fraction (0.0–1.0). |

**Returns** a JSONB array of result objects:

```json
[
  {"name": "st1", "status": "created", "pgt_id": 42},
  {"name": "st2", "status": "created", "pgt_id": 43}
]
```

On any error, the entire transaction is rolled back (standard PostgreSQL transactional semantics). The error message includes the index and name of the failing definition.

**Example:**

```sql
SELECT pgtrickle.bulk_create('[
  {"name": "order_totals", "query": "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id", "schedule": "30s"},
  {"name": "product_stats", "query": "SELECT product_id, COUNT(*) AS cnt FROM order_items GROUP BY product_id", "schedule": "1m"}
]'::jsonb);
```

---

### pgtrickle.alter_stream_table

Alter properties of an existing stream table.

```sql
pgtrickle.alter_stream_table(
    name                       text,
    query                      text      DEFAULT NULL,
    schedule                   text      DEFAULT NULL,
    refresh_mode               text      DEFAULT NULL,
    status                     text      DEFAULT NULL,
    diamond_consistency        text      DEFAULT NULL,
    diamond_schedule_policy    text      DEFAULT NULL,
    cdc_mode                   text      DEFAULT NULL,
    append_only                bool      DEFAULT NULL,
    pooler_compatibility_mode  bool      DEFAULT NULL,
    tier                       text      DEFAULT NULL,
    fuse                       text      DEFAULT NULL,
    fuse_ceiling               bigint    DEFAULT NULL,
    fuse_sensitivity           integer   DEFAULT NULL,
    partition_by               text      DEFAULT NULL,
    max_differential_joins     integer   DEFAULT NULL,
    max_delta_fraction         float8    DEFAULT NULL,
    post_refresh_action        text      DEFAULT NULL,
    reindex_drift_threshold    float8    DEFAULT NULL,
    sink                       text      DEFAULT NULL,
) → void
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `text` | — | Name of the stream table (schema-qualified or unqualified). |
| `query` | `text` | `NULL` | New defining query. Pass `NULL` to leave unchanged. When set, the function validates the new query, migrates the storage table schema if needed, updates catalog entries and dependencies, and runs a full refresh. Schema changes are classified as **same** (no DDL), **compatible** (ALTER TABLE ADD/DROP COLUMN), or **incompatible** (full storage rebuild with OID change). |
| `schedule` | `text` | `NULL` | New schedule as a duration string (e.g., `'5m'`). Pass `NULL` to leave unchanged. Pass `'calculated'` to switch to CALCULATED mode. |
| `refresh_mode` | `text` | `NULL` | New refresh mode (`'AUTO'`, `'FULL'`, `'DIFFERENTIAL'`, or `'IMMEDIATE'`). Pass `NULL` to leave unchanged. Switching to/from `'IMMEDIATE'` migrates trigger infrastructure (IVM triggers ↔ CDC triggers), clears or restores the schedule, and runs a full refresh. |
| `status` | `text` | `NULL` | New status (`'ACTIVE'`, `'SUSPENDED'`). Pass `NULL` to leave unchanged. Resuming resets consecutive errors to 0. |
| `diamond_consistency` | `text` | `NULL` | New diamond consistency mode (`'none'` or `'atomic'`). Pass `NULL` to leave unchanged. |
| `diamond_schedule_policy` | `text` | `NULL` | New schedule policy for atomic diamond groups (`'fastest'` or `'slowest'`). Pass `NULL` to leave unchanged. |
| `cdc_mode` | `text` | `NULL` | New requested CDC mode override (`'auto'`, `'trigger'`, or `'wal'`). Pass `NULL` to leave unchanged. |
| `append_only` | `bool` | `NULL` | Enable or disable the append-only INSERT fast path. Pass `NULL` to leave unchanged. When `true`, rejected for FULL, IMMEDIATE, or keyless source stream tables. |
| `pooler_compatibility_mode` | `bool` | `NULL` | Enable or disable pooler-safe mode. When `true`, prepared statements are bypassed and NOTIFY emissions are suppressed. Pass `NULL` to leave unchanged. |
| `tier` | `text` | `NULL` | Refresh tier for tiered scheduling (`'hot'`, `'warm'`, `'cold'`, or `'frozen'`). Only effective when `pg_trickle.tiered_scheduling` GUC is enabled. Hot (1×), Warm (2×), Cold (10×), Frozen (skip). Pass `NULL` to leave unchanged. |
| `fuse` | `text` | `NULL` | Fuse (circuit breaker) mode: `'off'`, `'on'`, or `'auto'`. `'auto'` blows the fuse when FULL would be cheaper than DIFFERENTIAL. Pass `NULL` to leave unchanged. |
| `fuse_ceiling` | `bigint` | `NULL` | Change-count threshold that triggers the fuse. `NULL` uses the global `pg_trickle.fuse_default_ceiling`. |
| `fuse_sensitivity` | `integer` | `NULL` | Number of consecutive over-ceiling cycles before the fuse blows. `NULL` means 1 (blow immediately). |
| `partition_by` | `text` | `NULL` | Partition key column for the storage table. Pass `NULL` to leave unchanged. |
| `max_differential_joins` | `integer` | `NULL` | Per-stream-table cap on join combinations during differential refresh. Pass `NULL` to leave unchanged. |
| `max_delta_fraction` | `float8` | `NULL` | Per-stream-table AUTO cost model threshold (0.0–1.0). Pass `NULL` to leave unchanged. |
| `post_refresh_action` | `text` | `NULL` | Action to take after each successful refresh: `'reindex'` (rebuild indexes when drift exceeds threshold) or `NULL` (no action). |
| `reindex_drift_threshold` | `float8` | `NULL` | Index drift ratio threshold (0.0–1.0) for `post_refresh_action = 'reindex'`. Pass `NULL` to leave unchanged. |

If you switch a stream table to `refresh_mode => 'IMMEDIATE'` while the
cluster-wide `pg_trickle.cdc_mode` GUC is set to `'wal'`, pg_trickle logs an
INFO and proceeds with IVM triggers. WAL CDC does not apply to IMMEDIATE mode.
If the stream table has an explicit `cdc_mode => 'wal'` override, switching to
`IMMEDIATE` is rejected until you change the requested CDC mode back to
`'auto'` or `'trigger'`.

**Examples:**

```sql
-- Change the defining query (same output schema — fast path)
SELECT pgtrickle.alter_stream_table('order_totals',
    query => 'SELECT customer_id, SUM(amount) AS total FROM orders WHERE status = ''active'' GROUP BY customer_id');

-- Change query and add a column (compatible schema migration)
SELECT pgtrickle.alter_stream_table('order_totals',
    query => 'SELECT customer_id, SUM(amount) AS total, COUNT(*) AS cnt FROM orders GROUP BY customer_id');

-- Change query and mode simultaneously
SELECT pgtrickle.alter_stream_table('order_totals',
    query => 'SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id',
    refresh_mode => 'FULL');

-- Change schedule
SELECT pgtrickle.alter_stream_table('order_totals', schedule => '5m');

-- Switch to full refresh mode
SELECT pgtrickle.alter_stream_table('order_totals', refresh_mode => 'FULL');

-- Switch to immediate (transactional) mode — installs IVM triggers, clears schedule
SELECT pgtrickle.alter_stream_table('order_totals', refresh_mode => 'IMMEDIATE');

-- Switch from immediate back to differential — re-creates CDC triggers, restores schedule
SELECT pgtrickle.alter_stream_table('order_totals',
    refresh_mode => 'DIFFERENTIAL', schedule => '5m');

-- Pin a deferred stream table to trigger CDC even when the global GUC is 'auto'
SELECT pgtrickle.alter_stream_table('order_totals', cdc_mode => 'trigger');

-- Enable append-only INSERT fast path
SELECT pgtrickle.alter_stream_table('event_log_st', append_only => true);

-- Enable pooler compatibility mode (for PgBouncer transaction mode)
SELECT pgtrickle.alter_stream_table('order_totals', pooler_compatibility_mode => true);

-- Set refresh tier (requires pg_trickle.tiered_scheduling = on)
SELECT pgtrickle.alter_stream_table('order_totals', tier => 'warm');
SELECT pgtrickle.alter_stream_table('archive_stats', tier => 'frozen');

-- Suspend a stream table
SELECT pgtrickle.alter_stream_table('order_totals', status => 'SUSPENDED');

-- Resume a suspended stream table
SELECT pgtrickle.resume_stream_table('order_totals');
-- Or via alter_stream_table
SELECT pgtrickle.alter_stream_table('order_totals', status => 'ACTIVE');
```

**Notes:**
- When `query` is provided, the function runs the full query rewrite pipeline (view inlining, DISTINCT ON, GROUPING SETS, etc.) and validates the new query before applying changes.
- The entire ALTER QUERY operation runs within a single transaction. If any step fails, the stream table is left unchanged.
- For same-schema and compatible-schema changes, the storage table OID is preserved — views, policies, and publications referencing the stream table remain valid.
- For incompatible schema changes (e.g., changing a column from `integer` to `text`), the storage table is rebuilt and the OID changes. A `WARNING` is emitted.
- The stream table is temporarily suspended during query migration to prevent concurrent scheduler refreshes.

---

### pgtrickle.drop_stream_table

Drop a stream table, removing the storage table and all catalog entries.

```sql
pgtrickle.drop_stream_table(name text) → void
```

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `name` | `text` | Name of the stream table to drop. |

**Example:**

```sql
SELECT pgtrickle.drop_stream_table('order_totals');
```

**Notes:**
- Drops the underlying storage table with `CASCADE`.
- Removes all catalog entries (metadata, dependencies, refresh history).
- Cleans up CDC triggers and change buffer tables for source tables that are no longer tracked by any ST.
- Automatically drops a downstream publication only when its recorded binding
  still matches; stale bindings reject the stream-table drop for safety.

---

### pgtrickle.stream\_table\_to\_publication

Create a PostgreSQL logical replication publication for a stream table,
enabling downstream consumers (Debezium, Kafka Connect, standby replicas)
to subscribe to changes.

```sql
pgtrickle.stream_table_to_publication(name text) → void
```

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `name` | `text` | Name of the stream table (schema-qualified or unqualified). |

**Example:**

```sql
SELECT pgtrickle.stream_table_to_publication('order_totals');
-- Creates publication 'pgt_pub_order_totals'
```

**Notes:**
- The publication is named `pgt_pub_<table_name>`.
- Only one publication per stream table is allowed.
- The publication is automatically dropped when the stream table is dropped.
- The caller must own the stream table and have `CREATE` on the current database.
- The publication owner is the caller. The function does not lend the extension
  owner's privileges to the caller.
- The private binding records the live publication identity and is committed
  with the public DDL.

---

### pgtrickle.drop\_stream\_table\_publication

Drop the logical replication publication for a stream table.

```sql
pgtrickle.drop_stream_table_publication(name text) → void
```

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `name` | `text` | Name of the stream table (schema-qualified or unqualified). |

**Example:**

```sql
SELECT pgtrickle.drop_stream_table_publication('order_totals');
```

**Notes:**
- The caller must own the stream table.
- The function validates the bound publication before dropping it. A renamed,
  replaced, transferred, or relation-set-mismatched publication is rejected.
- A failed drop leaves both the publication and the private binding unchanged.

---

### pgtrickle.set\_stream\_table\_sla

Assign a freshness deadline SLA to a stream table. The extension automatically
assigns the appropriate refresh tier based on the SLA.

```sql
pgtrickle.set_stream_table_sla(name text, sla interval) → void
```

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `name` | `text` | Name of the stream table (schema-qualified or unqualified). |
| `sla` | `interval` | Maximum acceptable data staleness. |

**Tier assignment:**
- SLA ≤ 5 seconds → **Hot** tier
- SLA ≤ 30 seconds → **Warm** tier
- SLA > 30 seconds → **Cold** tier

**Example:**

```sql
SELECT pgtrickle.set_stream_table_sla('order_totals', interval '10 seconds');
-- Assigns Warm tier
```

**Notes:**
- The scheduler periodically checks actual refresh performance and dynamically
  re-assigns tiers if the SLA is consistently breached or over-served.

---

### pgtrickle.resume_stream_table

Resume a suspended stream table, clearing its consecutive error count and
re-enabling automated and manual refreshes.

```sql
pgtrickle.resume_stream_table(name text) → void
```

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `name` | `text` | Name of the stream table to resume (schema-qualified or unqualified). |

**Example:**

```sql
-- Resume a stream table that was auto-suspended due to repeated errors
SELECT pgtrickle.resume_stream_table('order_totals');
```

**Notes:**
- Errors if the ST is not in `SUSPENDED` state.
- Resets `consecutive_errors` to `0` and sets `status = 'ACTIVE'`.
- Emits a `resumed` event on the `pg_trickle_alert` NOTIFY channel.
- After resuming, the scheduler will include the ST in its next cycle.

---

---

### pgtrickle.pause_stream_table

Pause an active stream table, preventing the scheduler from including it in
future refresh cycles. Unlike `SUSPEND` (which is set automatically after
repeated errors), `PAUSE` is an explicit operator action.

```sql
pgtrickle.pause_stream_table(name text) → void
```

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `name` | `text` | Name of the stream table to pause (schema-qualified or unqualified). |

**Example:**

```sql
-- Pause a stream table for maintenance
SELECT pgtrickle.pause_stream_table('order_totals');
-- ... do maintenance work ...
SELECT pgtrickle.resume_stream_table('order_totals');
```

**Notes:**
- Only active (`ACTIVE`) stream tables can be paused; errors if the ST is already
  `SUSPENDED` or in `ERROR` state.
- Use `pgtrickle.resume_stream_table(name)` to re-enable automated refreshes.
- Emits a `paused` event on the `pg_trickle_alert` NOTIFY channel.

---

### pgtrickle.set_stream_table_refresh_policy

Update the refresh mode of an existing stream table without recreating it.

```sql
pgtrickle.set_stream_table_refresh_policy(
    name         text,
    refresh_mode text
) → void
```

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `name` | `text` | Name of the stream table to update. |
| `refresh_mode` | `text` | New refresh mode: `'DIFFERENTIAL'`, `'FULL'`, or `'AUTO'`. |

**Example:**

```sql
-- Switch from FULL to DIFFERENTIAL refresh
SELECT pgtrickle.set_stream_table_refresh_policy('order_totals', 'DIFFERENTIAL');
```

**Notes:**
- Valid values are `'DIFFERENTIAL'`, `'FULL'`, and `'AUTO'`.
- The change takes effect on the next scheduled or manual refresh.
- Equivalent to `pgtrickle.alter_stream_table(name, refresh_mode => '...')` but
  with a cleaner, intent-revealing name.

---

### pgtrickle.set_stream_table_storage_policy

Update the storage policy (append-only flag and/or storage tier) of an existing
stream table.

```sql
pgtrickle.set_stream_table_storage_policy(
    name        text,
    append_only boolean          DEFAULT NULL,
    tier        text             DEFAULT NULL
) → void
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `text` | — | Name of the stream table to update. |
| `append_only` | `boolean` | `NULL` | Set or clear the append-only flag. `NULL` leaves the current value unchanged. |
| `tier` | `text` | `NULL` | Storage tier override. `NULL` leaves unchanged. |

**Example:**

```sql
-- Enable append-only mode on an existing stream table
SELECT pgtrickle.set_stream_table_storage_policy('event_log', append_only => true);
```

**Notes:**
- Pass `NULL` for parameters you do not want to change.
- `append_only = true` prevents DELETE operations during refresh; safe for
  insert-only event streams and audit logs.

---

### pgtrickle.refresh_stream_table

Manually trigger a synchronous refresh of a stream table.

```sql
pgtrickle.refresh_stream_table(name text) → void
```

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `name` | `text` | Name of the stream table to refresh. |

**Example:**

```sql
SELECT pgtrickle.refresh_stream_table('order_totals');
```

**Notes:**
- Blocked if the ST is `SUSPENDED` — use `pgtrickle.resume_stream_table(name)` first.
- Uses an advisory lock to prevent concurrent refreshes of the same ST.
- For `DIFFERENTIAL` mode, generates and applies a delta query. For `FULL` mode, truncates and reloads.
- Records the refresh in `pgtrickle.pgt_refresh_history` with `initiated_by = 'MANUAL'`.

---

### pgtrickle.repair_stream_table

Repair a stream table by reinstalling any missing CDC triggers, validating
catalog entries, and reconciling change buffer state.

```sql
pgtrickle.repair_stream_table(name text) → text
```

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `name` | `text` | Name of the stream table to repair. |

**Example:**

```sql
-- Reinstall missing CDC triggers after a point-in-time recovery
SELECT pgtrickle.repair_stream_table('order_totals');
```

**Notes:**
- Inspects all source tables in the stream table's dependency graph and reinstalls any missing or disabled CDC triggers.
- Validates that the stream table's catalog entry, storage table, and change buffer tables are consistent.
- Useful after `pg_basebackup` or PITR restores where triggers may not have been captured in the backup.
- Use `pgtrickle.trigger_inventory()` first to identify which triggers are missing.
- Safe to call on a healthy stream table — it is a no-op if everything is intact.

---

### Status & Monitoring

Query the state of stream tables, view refresh statistics, and diagnose problems.

---

### pgtrickle.pgt_status

Get the status of all stream tables.

```sql
pgtrickle.pgt_status() → SETOF record(
    name                text,
    status              text,
    refresh_mode        text,
    is_populated        bool,
    consecutive_errors  int,
    schedule            text,
    data_timestamp      timestamptz,
    staleness           interval
)
```

**Example:**

```sql
SELECT * FROM pgtrickle.pgt_status();
```

| name | status | refresh_mode | is_populated | consecutive_errors | schedule | data_timestamp | staleness |
|---|---|---|---|---|---|---|---|
| public.order_totals | ACTIVE | DIFFERENTIAL | true | 0 | 5m | 2026-02-21 12:00:00+00 | 00:02:30 |

---

### pgtrickle.health_check

Run a set of health checks against the pg_trickle installation and return one row per check.

```sql
pgtrickle.health_check() → SETOF record(
    check_name  text,   -- identifier for the check
    severity    text,   -- 'OK', 'WARN', or 'ERROR'
    detail      text    -- human-readable explanation
)
```

Filter to problems only:

```sql
SELECT check_name, severity, detail
FROM pgtrickle.health_check()
WHERE severity != 'OK';
```

Checks: `scheduler_running`, `error_tables`, `stale_tables`, `needs_reinit`,
`consecutive_errors`, `buffer_growth` (> 10 000 pending rows), `slot_lag`
(retained WAL above `pg_trickle.slot_lag_warning_threshold_mb`, default 100 MB),
`worker_pool` (all worker tokens in use — parallel mode only), `job_queue`
(> 10 jobs queued — parallel mode only),
`dvm_fallbacks` (v0.80.0+, WARN if DVM fallback refreshes were recorded in the last hour),
`ring_overflow_trend` (v0.80.0+, WARN if the DDL invalidation ring has overflowed since last restart).

---

### pgtrickle.health_summary

Single-row summary of the entire pg_trickle deployment's health. Designed for
monitoring dashboards that want one endpoint to poll instead of joining
multiple views.

```sql
pgtrickle.health_summary() → SETOF record(
    total_stream_tables   int,
    active_count          int,
    error_count           int,
    suspended_count       int,
    stale_count           int,
    reinit_pending        int,
    max_staleness_seconds float8,    -- NULL if no stream tables
    scheduler_status      text,      -- 'ACTIVE', 'STOPPED', or 'NOT_LOADED'
    cache_hit_rate        float8     -- NULL if no cache lookups yet
)
```

**Example:**

```sql
SELECT * FROM pgtrickle.health_summary();
```

| total_stream_tables | active_count | error_count | suspended_count | stale_count | reinit_pending | max_staleness_seconds | scheduler_status | cache_hit_rate |
|---|---|---|---|---|---|---|---|---|
| 12 | 11 | 0 | 1 | 0 | 0 | 45.2 | ACTIVE | 0.94 |

> **Tip:** Use this in a Grafana single-stat panel or a Prometheus exporter
> to surface fleet-level health at a glance.

---

### pgtrickle.refresh_timeline

Return recent refresh records across **all** stream tables in a single chronological view.

```sql
pgtrickle.refresh_timeline(
    max_rows int  DEFAULT 50
) → SETOF record(
    start_time      timestamptz,
    stream_table    text,
    action          text,
    status          text,
    rows_inserted   bigint,
    rows_updated    bigint,
    rows_deleted    bigint,
    duration_ms     float8,
    error_message   text
)
```

**Example:**

```sql
-- Most recent 20 events across all stream tables:
SELECT start_time, stream_table, action, status, round(duration_ms::numeric,1) AS ms
FROM pgtrickle.refresh_timeline(20);

-- Just failures in the last 100 events:
SELECT * FROM pgtrickle.refresh_timeline(100) WHERE status = 'ERROR';
```

---

### pgtrickle.st_refresh_stats

Return per-ST refresh statistics aggregated from the refresh history.

```sql
pgtrickle.st_refresh_stats() → SETOF record(
    pgt_name                text,
    pgt_schema              text,
    status                 text,
    refresh_mode           text,
    is_populated           bool,
    total_refreshes        bigint,
    successful_refreshes   bigint,
    failed_refreshes       bigint,
    total_rows_inserted    bigint,
    total_rows_updated     bigint,
    total_rows_deleted     bigint,
    avg_duration_ms        float8,
    last_refresh_action    text,
    last_refresh_status    text,
    last_refresh_at        timestamptz,
    staleness_secs       float8,
    stale           bool
)
```

**Example:**

```sql
SELECT pgt_name, status, total_refreshes, avg_duration_ms, stale
FROM pgtrickle.st_refresh_stats();
```

---

### pgtrickle.get_refresh_history

Return refresh history for a specific stream table.

```sql
pgtrickle.get_refresh_history(
    name      text,
    max_rows  int  DEFAULT 20
) → SETOF record(
    refresh_id       bigint,
    data_timestamp   timestamptz,
    start_time       timestamptz,
    end_time         timestamptz,
    action           text,
    status           text,
    rows_inserted    bigint,
    rows_updated     bigint,
    rows_deleted     bigint,
    duration_ms      float8,
    error_message    text
)
```

**Example:**

```sql
SELECT action, status, rows_inserted, duration_ms
FROM pgtrickle.get_refresh_history('order_totals', 5);
```

---

### pgtrickle.get_staleness

Get the current staleness in seconds for a specific stream table.

```sql
pgtrickle.get_staleness(name text) → float8
```

Returns `NULL` if the ST has never been refreshed.

**Example:**

```sql
SELECT pgtrickle.get_staleness('order_totals');
-- Returns: 12.345  (seconds since last refresh)
```

---

### pgtrickle.explain_refresh_mode

Explain the configured vs. effective refresh mode for a stream table, including the reason for any downgrade (e.g., AUTO choosing FULL).

```sql
pgtrickle.explain_refresh_mode(name text) → TABLE(
    configured_mode  text,
    effective_mode   text,
    downgrade_reason text
)
```

**Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `configured_mode` | `text` | The refresh mode set on the stream table (e.g., `DIFFERENTIAL`, `AUTO`, `FULL`, `IMMEDIATE`) |
| `effective_mode` | `text` | The mode actually used on the most recent refresh. NULL for IMMEDIATE mode (handled by triggers) |
| `downgrade_reason` | `text` | Human-readable explanation when `effective_mode` differs from `configured_mode`, or informational note for IMMEDIATE / APPEND_ONLY |

**Example:**

```sql
SELECT * FROM pgtrickle.explain_refresh_mode('public.orders_summary');
```

| configured_mode | effective_mode | downgrade_reason |
|---|---|---|
| AUTO | FULL | The most recent refresh used FULL mode. Possible causes: defining query contains a CTE or unsupported operator, adaptive change-ratio threshold was exceeded, or aggregate saturation occurred. Check pgtrickle.pgt_refresh_history for details. |

---

### pgtrickle.cache_stats

Return template cache statistics from shared memory.

Reports L1 (thread-local) hits, L2 (catalog table) hits, full misses
(DVM re-parse), evictions (generation flushes), and the current L1 cache
size for this backend.

```sql
pgtrickle.cache_stats() → SETOF record(
    l1_hits    bigint,
    l2_hits    bigint,
    misses     bigint,
    evictions  bigint,
    l1_size    integer
)
```

| Column | Description |
|--------|-------------|
| `l1_hits` | Number of delta template cache hits in the thread-local (L1) cache. ~0 ns lookup. |
| `l2_hits` | Number of delta template cache hits in the catalog table (L2) cache. ~1 ms SPI lookup. |
| `misses` | Number of full cache misses requiring DVM re-parse (~45 ms). |
| `evictions` | Number of entries evicted from L1 due to DDL-triggered generation flushes. |
| `l1_size` | Current number of entries in this backend's L1 cache. |

**Example:**

```sql
SELECT * FROM pgtrickle.cache_stats();
```

| l1_hits | l2_hits | misses | evictions | l1_size |
|---------|---------|--------|-----------|---------|
| 142 | 3 | 5 | 10 | 8 |

> **Note:** Counters are cluster-wide (shared memory) except `l1_size` which
> is per-backend. Requires `shared_preload_libraries = 'pg_trickle'`; returns
> zeros when loaded dynamically.

---

### pgtrickle.commit_latency_stats

Return per-stream-table refresh latency statistics (min, p50, p95, max) in
milliseconds, computed from `pgtrickle.pgt_refresh_history`.

When `pg_trickle.commit_timestamp_tracking = off` (default) the latency
measures total refresh duration (`end_time - start_time`).  Enabling
`commit_timestamp_tracking` with `track_commit_timestamp = on` in
`postgresql.conf` switches to true commit-to-visible wall-clock latency.

```sql
pgtrickle.commit_latency_stats() → SETOF record(
    pgt_schema    text,
    pgt_name      text,
    samples       bigint,
    min_ms        double precision,
    p50_ms        double precision,
    p95_ms        double precision,
    max_ms        double precision,
    tracking_mode text
)
```

**Example:**

```sql
SELECT * FROM pgtrickle.commit_latency_stats();
```

---

### pgtrickle.tune_recommendations

Return a set of GUC tuning recommendations derived from observed query history
and current configuration.  Each row describes one recommendation.

```sql
pgtrickle.tune_recommendations() → SETOF record(
    guc_name          text,
    current_value     text,
    recommended_value text,
    reason            text
)
```

**Example:**

```sql
SELECT * FROM pgtrickle.tune_recommendations();
```

---

### pgtrickle.preview_stream_table

Perform a dry-run parse and analysis of a defining query without creating any
stream table.  Returns a set of key/value property rows describing the detected
refresh mode, source tables, query complexity, and IVM support status.

```sql
pgtrickle.preview_stream_table(query text) → SETOF record(
    property text,
    value    text
)
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `query` | `text` | — | SQL SELECT query to analyse. |

**Example:**

```sql
SELECT * FROM pgtrickle.preview_stream_table(
    'SELECT user_id, count(*) FROM events GROUP BY user_id'
);
```

### pgtrickle.explain

Return a bounded explanation of one stream table's refresh mode, recent
refresh evidence, cost-model summary, and target freshness state as text.

```sql
SELECT pgtrickle.explain('public.orders_summary');
```

### pgtrickle.explain_json

Return the same bounded explanation as evidence-aware JSON.

```sql
SELECT pgtrickle.explain_json('public.orders_summary');
```

### pgtrickle.stat_reset

Reset cumulative refresh and cost-model statistics without deleting refresh
history. `stat_reset` requires ownership of the selected stream table;
```sql
SELECT pgtrickle.stat_reset(42);
```

### pgtrickle.stat_reset_all

Reset cumulative refresh and cost-model statistics for all stream tables.
This requires superuser or extension-owner privilege.

```sql
SELECT pgtrickle.stat_reset_all();
```

---

### pgtrickle.history_prune_status

Return history prune statistics from shared memory.

Reports the cumulative count of SPI failures in the background prune loop,
the timestamp of the last successful prune run, and the number of rows deleted
in the most recent prune. A non-zero `prune_error_count` indicates the
background cleanup loop is failing and `pgt_refresh_history` may grow unbounded.

```sql
pgtrickle.history_prune_status() → SETOF record(
    prune_error_count  bigint,
    last_prune_at      timestamptz,
    last_rows_deleted  bigint
)
```

| Column | Description |
|--------|-------------|
| `prune_error_count` | Cumulative number of SPI failures in the background history prune loop. |
| `last_prune_at` | Timestamp of the last successful prune run, or `NULL` if no prune has run yet. |
| `last_rows_deleted` | Number of rows deleted in the most recent prune run, or `NULL` if no prune has run yet. |

**Example:**

```sql
SELECT * FROM pgtrickle.history_prune_status();
```

| prune_error_count | last_prune_at | last_rows_deleted |
|-------------------|---------------|-------------------|
| 0 | 2024-01-15 10:30:00+00 | 1250 |

> **Note:** Counters are stored in shared memory. Requires
> `shared_preload_libraries = 'pg_trickle'`. The prune interval is controlled
> by `pg_trickle.history_prune_interval_seconds` (default 60 s; 0 disables).

---

### pgtrickle.metrics_summary

Return a cluster-wide aggregation of key pg_trickle counters for the current
database. Designed as the primary data source for Grafana cluster-overview
dashboards. Aggregates refresh counts, error counts, and worker utilisation
from `pgtrickle.pgt_refresh_summary` (the incremental summary table) and
the shared-memory hold-back probe metrics.

```sql
pgtrickle.metrics_summary() → SETOF record(
    db_name                      text,
    total_stream_tables          bigint,
    active_stream_tables         bigint,
    suspended_stream_tables      bigint,
    total_refreshes              bigint,
    successful_refreshes         bigint,
    failed_refreshes             bigint,
    total_rows_processed         bigint,
    active_workers               integer,
    ivm_lock_parse_error_count   bigint,
    holdback_probe_calls         bigint,
    holdback_probe_cache_hits    bigint,
    holdback_probe_avg_ms        double precision,
    cleanup_backlog_count        bigint,      -- v0.80.0+
    cleanup_blocked_count        bigint       -- v0.80.0+
)
```

| Column | Description |
|--------|-------------|
| `db_name` | Name of the current database (`current_database()`). Useful for multi-database Grafana panels that union results from several pg_trickle instances. |
| `total_stream_tables` | Total number of stream tables registered in `pgtrickle.pgt_stream_tables`. |
| `active_stream_tables` | Number of stream tables with `status = 'ACTIVE'`. |
| `suspended_stream_tables` | Number of stream tables with `status = 'SUSPENDED'` (fuse blown or manually paused). |
| `total_refreshes` | Cumulative refresh attempts (successful + failed) aggregated from `pgt_refresh_summary`. |
| `successful_refreshes` | Cumulative successful refreshes. |
| `failed_refreshes` | Cumulative failed refreshes. A non-zero value indicates stream tables that need attention. |
| `total_rows_processed` | Cumulative rows inserted + deleted across all differential refreshes. |
| `active_workers` | Current number of active background worker processes (from shared memory). |
| `ivm_lock_parse_error_count` | Cumulative count of IMMEDIATE-mode lock-mode downgrades due to query parse failures (v0.31.0+). |
| `holdback_probe_calls` | Total hold-back probes executed since last extension restart. |
| `holdback_probe_cache_hits` | Probes satisfied from the hold-back cache without a full snapshot scan. |
| `holdback_probe_avg_ms` | Average hold-back probe latency in milliseconds. High values (> 50 ms) indicate contention on the watermark table. |
| `cleanup_backlog_count` | Total entries currently in `pgt_cleanup_status` awaiting deferred cleanup. Non-zero values indicate pending tombstone or orphan removal work. (v0.80.0+) |
| `cleanup_blocked_count` | Entries in `pgt_cleanup_status` that are currently blocked (e.g., held by an active transaction). Persistent non-zero values may indicate lock contention. (v0.80.0+) |

**Example:**

```sql
SELECT * FROM pgtrickle.metrics_summary();
```

| db_name | total_stream_tables | active_stream_tables | suspended_stream_tables | total_refreshes | successful_refreshes | failed_refreshes | total_rows_processed | active_workers | ivm_lock_parse_error_count | holdback_probe_calls | holdback_probe_cache_hits | holdback_probe_avg_ms | cleanup_backlog_count | cleanup_blocked_count |
|---------|---------------------|----------------------|------------------------|-----------------|---------------------|------------------|---------------------|----------------|---------------------------|---------------------|--------------------------|----------------------|-----------------------|-----------------------|
| mydb | 12 | 11 | 1 | 4820 | 4818 | 2 | 193400 | 3 | 0 | 142 | 138 | 0.8 | 0 | 0 |

**Grafana usage:**

```sql
-- Panel query for cluster overview dashboard
SELECT
    db_name,
    total_stream_tables,
    active_stream_tables,
    failed_refreshes,
    active_workers,
    holdback_probe_avg_ms,
    cleanup_backlog_count,
    cleanup_blocked_count
FROM pgtrickle.metrics_summary();
```

> **Cost caveat:** `metrics_summary()` joins `pgt_stream_tables` against
> `pgt_refresh_summary` (one row per stream table). Cost is O(N stream tables)
> — typically a few milliseconds for hundreds of stream tables. For very large
> installations (thousands of stream tables), scrape at a longer interval
> (e.g., every 30 s) rather than on every Prometheus scrape tick.
>
> **Note:** `active_workers` and holdback-probe metrics come from shared
> memory. They require `shared_preload_libraries = 'pg_trickle'` and return
> 0 / 0.0 when the extension is loaded dynamically.

---

### CDC Diagnostics

Inspect CDC pipeline health, replication slots, change buffers, and trigger coverage.

---

### pgtrickle.slot_health

Check replication slot health for all tracked CDC slots.

```sql
pgtrickle.slot_health() → SETOF record(
    slot_name          text,
    source_relid       bigint,
    active             bool,
    retained_wal_bytes bigint,
    wal_status         text
)
```

**Example:**

```sql
SELECT * FROM pgtrickle.slot_health();
```

| slot_name | source_relid | active | retained_wal_bytes | wal_status |
|---|---|---|---|---|
| pg_trickle_slot_16384 | 16384 | false | 1048576 | reserved |

---

### pgtrickle.check_cdc_health

Check CDC health for all tracked source tables. Returns per-source health status including the current CDC mode, replication slot details, estimated lag, and any alerts.

The `alert` column uses the critical threshold configured by
`pg_trickle.slot_lag_critical_threshold_mb` (default 1024 MB).

```sql
pgtrickle.check_cdc_health() → SETOF record(
    source_relid   bigint,
    source_table   text,
    cdc_mode       text,
    slot_name      text,
    lag_bytes      bigint,
    confirmed_lsn  text,
    alert          text
)
```

**Columns:**

| Column | Type | Description |
|---|---|---|
| `source_relid` | `bigint` | OID of the tracked source table |
| `source_table` | `text` | Resolved name of the source table (e.g., `public.orders`) |
| `cdc_mode` | `text` | Current CDC mode: `TRIGGER`, `TRANSITIONING`, or `WAL` |
| `slot_name` | `text` | Replication slot name (NULL for TRIGGER mode) |
| `lag_bytes` | `bigint` | Replication slot lag in bytes (NULL for TRIGGER mode) |
| `confirmed_lsn` | `text` | Last confirmed WAL position (NULL for TRIGGER mode) |
| `alert` | `text` | Alert message if unhealthy (e.g., `slot_lag_exceeds_threshold`, `replication_slot_missing`) |

**Example:**

```sql
SELECT * FROM pgtrickle.check_cdc_health();
```

| source_relid | source_table | cdc_mode | slot_name | lag_bytes | confirmed_lsn | alert |
|---|---|---|---|---|---|---|
| 16384 | public.orders | TRIGGER | | | | |
| 16390 | public.events | WAL | pg_trickle_slot_16390 | 524288 | 0/1A8B000 | |

---

### pgtrickle.change_buffer_sizes

Show pending change counts and estimated on-disk sizes for all CDC-tracked
source tables.

Returns one row per `(stream_table, source_table)` pair.

```sql
pgtrickle.change_buffer_sizes() → SETOF record(
    stream_table  text,     -- qualified stream table name
    source_table  text,     -- qualified source table name
    source_oid    bigint,
    cdc_mode      text,     -- 'trigger', 'wal', or 'transitioning'
    pending_rows  bigint,   -- rows in buffer not yet consumed
    buffer_bytes  bigint    -- estimated buffer table size in bytes
)
```

**Example:**

```sql
SELECT * FROM pgtrickle.change_buffer_sizes()
ORDER BY pending_rows DESC;
```

Useful for spotting a source table whose CDC buffer is growing unexpectedly
(which may indicate a stalled differential refresh or a high-write source that
has outpaced the schedule).

---

### pgtrickle.worker_pool_status

Snapshot of the parallel refresh worker pool. Returns a single row.

```sql
pgtrickle.worker_pool_status() → SETOF record(
    active_workers  int,   -- workers currently executing refresh jobs
    max_workers     int,   -- cluster-wide worker budget (GUC)
    per_db_cap      int,   -- per-database dispatch cap (GUC)
    parallel_mode   text   -- current parallel_refresh_mode value
)
```

**Example:**

```sql
SELECT * FROM pgtrickle.worker_pool_status();
```

Returns `0` active workers when `parallel_refresh_mode = 'off'`.

---

### pgtrickle.parallel_job_status

Active and recently completed scheduler jobs from the `pgt_scheduler_jobs`
table. Shows jobs that are currently queued or running, plus jobs that
finished within the last `max_age_seconds` (default 300).

```sql
pgtrickle.parallel_job_status(
    max_age_seconds int  DEFAULT 300
) → SETOF record(
    job_id         bigint,
    unit_key       text,        -- stable unit identifier (s:42, a:1,2, etc.)
    unit_kind      text,        -- 'singleton', 'atomic_group', 'immediate_closure'
    status         text,        -- 'QUEUED', 'RUNNING', 'SUCCEEDED', etc.
    member_count   int,
    attempt_no     int,
    scheduler_pid  int,
    worker_pid     int,         -- NULL if not yet claimed
    enqueued_at    timestamptz,
    started_at     timestamptz, -- NULL if still queued
    finished_at    timestamptz, -- NULL if not finished
    duration_ms    float8       -- NULL if not finished
)
```

**Example — show running and recently failed jobs:**

```sql
SELECT job_id, unit_key, status, duration_ms
FROM pgtrickle.parallel_job_status(60)
WHERE status NOT IN ('SUCCEEDED');
```

---

### pgtrickle.trigger_inventory

List all CDC triggers that pg_trickle should have installed, and verify each one exists and is enabled in `pg_catalog`.

```sql
pgtrickle.trigger_inventory() → SETOF record(
    source_table  text,    -- qualified source table name
    source_oid    bigint,
    trigger_name  text,    -- expected trigger name
    trigger_type  text,    -- 'DML' or 'TRUNCATE'
    present       bool,    -- trigger exists in pg_catalog
    enabled       bool     -- trigger is not disabled
)
```

A `present = false` row means change capture is broken for that source.

**Example:**

```sql
-- Show only missing or disabled triggers:
SELECT source_table, trigger_type, trigger_name
FROM pgtrickle.trigger_inventory()
WHERE NOT present OR NOT enabled;
```

---

### pgtrickle.fuse_status

Return the circuit-breaker (fuse) state for every stream table that has a
fuse configured.

```sql
pgtrickle.fuse_status() → SETOF record(
    name           text,         -- stream table name
    fuse_mode      text,         -- 'off', 'on', or 'auto'
    fuse_state     text,         -- 'armed' or 'blown'
    fuse_ceiling   bigint,       -- change-count threshold
    fuse_sensitivity int,        -- consecutive over-ceiling cycles before blow
    blown_at       timestamptz,  -- when the fuse last blew (NULL if armed)
    blow_reason    text          -- reason the fuse blew (NULL if armed)
)
```

**Example:**

```sql
-- Check all fuse-enabled stream tables
SELECT name, fuse_mode, fuse_state, fuse_ceiling, blown_at
FROM pgtrickle.fuse_status();

-- Find blown fuses
SELECT name, blow_reason, blown_at
FROM pgtrickle.fuse_status()
WHERE fuse_state = 'blown';
```

**Notes:**
- Returns one row per stream table where `fuse_mode != 'off'`.
- A blown fuse suspends differential refreshes until cleared with `pgtrickle.reset_fuse()`.
- A `pgtrickle_alert` NOTIFY with event `fuse_blown` is emitted when the fuse trips.
- See [Configuration — fuse_default_ceiling](CONFIGURATION.md#pg_tricklefuse_default_ceiling) for global defaults.

---

### pgtrickle.reset_fuse

Clear a blown circuit-breaker fuse and resume scheduling for the stream table.

```sql
pgtrickle.reset_fuse(name text, action text DEFAULT 'apply') → void
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `text` | — | Name of the stream table whose fuse to reset. |
| `action` | `text` | `'apply'` | How to handle the pending changes that caused the fuse to blow. |

**Actions:**

| Action | Behavior |
|--------|----------|
| `'apply'` | Process all pending changes normally and resume scheduling. |
| `'reinitialize'` | Drop and repopulate the stream table from scratch (full refresh from defining query). |
| `'skip_changes'` | Discard the pending changes that triggered the fuse and resume from the current frontier. |

**Example:**

```sql
-- After investigating a bulk load, apply the changes:
SELECT pgtrickle.reset_fuse('category_summary', action => 'apply');

-- Or skip the oversized batch entirely:
SELECT pgtrickle.reset_fuse('category_summary', action => 'skip_changes');

-- Or rebuild from scratch:
SELECT pgtrickle.reset_fuse('category_summary', action => 'reinitialize');
```

**Notes:**
- Errors if the stream table's fuse is not in `'blown'` state.
- After reset, the fuse returns to `'armed'` state and the scheduler resumes normal operation.
- Use `pgtrickle.fuse_status()` to inspect the fuse state before resetting.
- The `'skip_changes'` action advances the frontier past the pending changes without applying them — use only when you are certain the changes should be discarded.

---

### Dependency & Inspection

Visualize dependencies, understand query plans, and audit source table relationships.

---

### pgtrickle.dependency_tree

Render all stream table dependencies as an indented ASCII tree.

```sql
pgtrickle.dependency_tree() → SETOF record(
    tree_line    text,    -- indented visual line (├──, └──, │ characters)
    node         text,    -- qualified name (schema.table)
    node_type    text,    -- 'stream_table' or 'source_table'
    depth        int,
    status       text,    -- NULL for source_table nodes
    refresh_mode text     -- NULL for source_table nodes
)
```

Roots (stream tables with no stream-table parents) appear at depth 0. Each
dependent is indented beneath its parent. Plain source tables are rendered as
leaf nodes tagged `[src]`.

**Example:**

```sql
SELECT tree_line, status, refresh_mode
FROM pgtrickle.dependency_tree();
```

```
tree_line                               status   refresh_mode
----------------------------------------+---------+--------------
report_summary                          ACTIVE   DIFFERENTIAL
├── orders_by_region                    ACTIVE   DIFFERENTIAL
│   ├── public.orders [src]
│   └── public.customers [src]
└── revenue_totals                      ACTIVE   DIFFERENTIAL
    └── public.orders [src]
```

---

### pgtrickle.diamond_groups

List all detected diamond dependency groups and their members.

When stream tables form diamond-shaped dependency graphs (multiple paths converge at a single fan-in node), the scheduler groups them for coordinated refresh. This function exposes those groups for monitoring and debugging.

```sql
pgtrickle.diamond_groups() → SETOF record(
    group_id        int4,
    member_name     text,
    member_schema   text,
    is_convergence  bool,
    epoch           int8,
    schedule_policy text
)
```

**Return columns:**

| Column | Type | Description |
|---|---|---|
| `group_id` | `int4` | Numeric identifier for the consistency group (1-based). |
| `member_name` | `text` | Name of the stream table in this group. |
| `member_schema` | `text` | Schema of the stream table. |
| `is_convergence` | `bool` | `true` if this member is a convergence (fan-in) node where multiple paths meet. |
| `epoch` | `int8` | Group epoch counter — advances on each successful atomic refresh of the group. |
| `schedule_policy` | `text` | Effective schedule policy for this group (`'fastest'` or `'slowest'`). Computed from convergence node settings with strictest-wins. |

**Example:**

```sql
SELECT * FROM pgtrickle.diamond_groups();
```

| group_id | member_name | member_schema | is_convergence | epoch | schedule_policy |
|---|---|---|---|---|---|
| 1 | st_b | public | false | 0 | fastest |
| 1 | st_c | public | false | 0 | fastest |
| 1 | st_d | public | true | 0 | fastest |

**Notes:**
- Singleton stream tables (not part of any diamond) are omitted.
- The DAG is rebuilt on each call from the catalog — results reflect the current dependency graph.
- Groups are only relevant when `diamond_consistency = 'atomic'` is set on the convergence node or globally via the `pg_trickle.diamond_consistency` GUC.

---

### pgtrickle.pgt\_scc\_status

List all cyclic strongly connected components (SCCs) and their convergence status.

When stream tables form circular dependencies (with `pg_trickle.allow_circular = true`), they are grouped into SCCs and iterated to a fixed point. This function exposes those groups for monitoring and debugging.

```sql
pgtrickle.pgt_scc_status() → SETOF record(
    scc_id              int4,
    member_count        int4,
    members             text[],
    last_iterations     int4,
    last_converged_at   timestamptz
)
```

**Return columns:**

| Column | Type | Description |
|---|---|---|
| `scc_id` | `int4` | SCC group identifier (1-based). |
| `member_count` | `int4` | Number of stream tables in this SCC. |
| `members` | `text[]` | Array of `schema.name` for each member. |
| `last_iterations` | `int4` | Number of fixpoint iterations in the last convergence (NULL if never iterated). |
| `last_converged_at` | `timestamptz` | Timestamp of the most recent refresh among SCC members (NULL if never refreshed). |

**Example:**

```sql
SELECT * FROM pgtrickle.pgt_scc_status();
```

| scc_id | member_count | members | last_iterations | last_converged_at |
|---|---|---|---|---|
| 1 | 2 | {public.reach_a,public.reach_b} | 3 | 2026-03-15 12:00:00+00 |

**Notes:**
- Only cyclic SCCs (with `scc_id IS NOT NULL`) are returned. Acyclic stream tables are omitted.
- `last_iterations` reflects the maximum `last_fixpoint_iterations` across SCC members.
- Results are queried from the catalog on each call.

---

### pgtrickle.explain_st

Explain the DVM plan for a stream table's defining query.

```sql
pgtrickle.explain_st(name text) → SETOF record(
    property  text,
    value     text
)
```

**Example:**

```sql
SELECT * FROM pgtrickle.explain_st('order_totals');
```

| property | value |
|---|---|
| pgt_name | public.order_totals |
| defining_query | SELECT region, SUM(amount) ... |
| refresh_mode | DIFFERENTIAL |
| status | active |
| is_populated | true |
| dvm_supported | true |
| operator_tree | Aggregate → Scan(orders) |
| output_columns | region, total |
| source_oids | 16384 |
| delta_query | WITH ... SELECT ... |
| frontier | {"orders": "0/15A3B80"} |
| amplification_stats | {"samples":10,"min":1.0,...} |
| refresh_timing_stats | {"samples":10,"min_ms":12.3,...} |
| source_partitions | [{"source":"public.orders",...}] |
| dependency_graph_dot | digraph dependency_subgraph { ... } |
| spill_info | {"temp_blks_read":0,"temp_blks_written":1234,...} |

#### Output Fields

| Property | Description |
|---|---|
| `pgt_name` | Fully-qualified stream table name |
| `defining_query` | The SQL query that defines the stream table |
| `refresh_mode` | `DIFFERENTIAL`, `FULL`, or `IMMEDIATE` |
| `status` | Current status (`active`, `suspended`, etc.) |
| `is_populated` | Whether the stream table has been initially populated |
| `dvm_supported` | Whether the defining query supports differential view maintenance |
| `operator_tree` | Debug representation of the DVM operator tree |
| `output_columns` | Comma-separated list of output column names |
| `source_oids` | Comma-separated list of source table OIDs |
| `aggregate_strategies` | Per-aggregate maintenance strategies (JSON, if aggregates present) |
| `delta_query` | The generated delta SQL used for DIFFERENTIAL refresh |
| `frontier` | Current LSN/watermark frontier (JSON) |
| `amplification_stats` | Delta amplification ratio statistics over the last 20 refreshes (JSON) |
| `refresh_timing_stats` | Refresh duration statistics over the last 20 completed refreshes (JSON). Fields: `samples`, `min_ms`, `max_ms`, `avg_ms`, `latest_ms`, `latest_action` |
| `source_partitions` | Partition info for partitioned source tables (JSON array). Fields per entry: `source`, `partition_key`, `partitions` |
| `dependency_graph_dot` | Dependency sub-graph in [DOT format](https://graphviz.org/doc/info/lang.html). Shows immediate upstream sources (ellipses for base tables, boxes for stream tables) and downstream dependents. Paste into a Graphviz renderer to visualize. |
| `spill_info` | Temp file spill metrics from `pg_stat_statements` (JSON). Fields: `temp_blks_read`, `temp_blks_written`, `threshold`, `exceeds_threshold`. Only present when `pg_trickle.spill_threshold_blocks > 0`. |

> **Note:** Properties are only included when data is available. For example,
> `source_partitions` only appears when at least one source table is
> partitioned, and `refresh_timing_stats` only appears after at least one
> completed refresh.

---

### pgtrickle.list_sources

List the source tables that a stream table depends on.

```sql
pgtrickle.list_sources(name text) → SETOF record(
    source_table   text,         -- qualified source table name
    source_oid     bigint,
    source_type    text,         -- 'table', 'stream_table', etc.
    cdc_mode       text,         -- 'trigger', 'wal', or 'transitioning'
    columns_used   text          -- column-level dependency info (if available)
)
```

**Example:**

```sql
SELECT * FROM pgtrickle.list_sources('order_totals');
```

Returns the tables tracked by CDC for the given stream table, along with
how they are being tracked. Useful when diagnosing why a stream table is
not refreshing or to audit which source tables are being trigger-tracked.

---

### Utilities

Utility functions for CDC management and row identity hashing.

---

### pgtrickle.rebuild_cdc_triggers

Rebuild all CDC triggers (function body + trigger DDL) for every source
table tracked by pg_trickle. This recreates trigger functions and
re-attaches the trigger to each source table.

```sql
pgtrickle.rebuild_cdc_triggers() → text
```

Returns `'done'` on success. Emits a `WARNING` per table on error and
continues processing remaining sources.

**When to use:**

- After changing [`pg_trickle.cdc_trigger_mode`](CONFIGURATION.md#pg_tricklecdc_trigger_mode) from `row` to `statement` (or vice versa).
- After `ALTER EXTENSION pg_trickle UPDATE` when the CDC trigger function body has changed.
- After restoring from a backup where triggers may have been lost.

**Example:**

```sql
-- Switch to statement-level triggers and rebuild
SET pg_trickle.cdc_trigger_mode = 'statement';
SELECT pgtrickle.rebuild_cdc_triggers();
```

**Notes:**
- Called automatically during `ALTER EXTENSION pg_trickle UPDATE` (0.3.0 → 0.4.0) migration.
- Safe to call at any time — existing triggers are dropped and recreated.
- On error for a specific table, a `WARNING` is logged and processing continues with remaining sources.

---

### pgtrickle.pg_trickle_hash

Compute a 64-bit xxHash row ID from a text value.

```sql
pgtrickle.pg_trickle_hash(input text) → bigint
```

Marked `IMMUTABLE, PARALLEL SAFE`.

**Example:**

```sql
SELECT pgtrickle.pg_trickle_hash('some_key');
-- Returns: 1234567890123456789
```

---

### pgtrickle.pg_trickle_hash_multi

Compute a row ID by hashing multiple text values (composite keys).

```sql
pgtrickle.pg_trickle_hash_multi(inputs text[]) → bigint
```

Marked `IMMUTABLE, PARALLEL SAFE`. Composite inputs use version-2 framing:
big-endian version and component-count fields, explicit NULL/value tags, and
byte lengths before each non-NULL UTF-8 value. This makes component boundaries,
NULLs, empty values, and separator-like values unambiguous. The single-value
`pg_trickle_hash` encoding remains unchanged.

**Example:**

```sql
SELECT pgtrickle.pg_trickle_hash_multi(ARRAY['key1', 'key2']);
```

### pgtrickle.encode_row_id_v2

Encode a PostgreSQL record using the exact, typed Version 2 row-identity
contract. The `domain` must be one of `SCAN_KEY`, `KEYLESS_ROW`, `GROUP_KEY`,
`JOIN_KEY`, `SET_KEY`, `WINDOW_KEY`, or `SYNTHETIC`.

```sql
pgtrickle.encode_row_id_v2(domain text, record anyelement) → bytea
```

The result is opaque canonical bytes. Unsupported types, non-deterministic
collations, malformed metadata, and values above the published resource limits
raise an error; the function never falls back to text formatting or hashing.
See [ROW_IDENTITY_V2.md](ROW_IDENTITY_V2.md) for the complete wire contract.

### pgtrickle.row_probe_v1

Produce the bounded, non-unique index probe for a complete V2 row identity.
Inputs up to 128 bytes are returned unchanged. Longer inputs retain the first
128 bytes and append the fixed-seed XXH3-128 digest. The full identity remains
the authoritative equality value.

```sql
pgtrickle.row_probe_v1(input bytea) → bytea
```

Marked `IMMUTABLE, PARALLEL SAFE`; the probe is an accelerator, not a row
identity and must not be used as a unique key.

---

## Operator Support Matrix — Summary

pg_trickle supports 60+ SQL constructs across three refresh modes. The table below summarises broad categories. For the **complete per-operator matrix** (including notes on caveats, auxiliary columns and strategies), see **[DVM_OPERATORS.md](DVM_OPERATORS.md)**.

| Category | FULL | DIFFERENTIAL | IMMEDIATE | Notes |
|---|:---:|:---:|:---:|---|
| Basic SELECT / WHERE / DISTINCT | ✅ | ✅ | ✅ | |
| Joins (INNER, LEFT, RIGHT, FULL, CROSS, LATERAL) | ✅ | ✅ | ✅ | Hybrid delta strategy |
| Subqueries (EXISTS, IN, NOT EXISTS, NOT IN, scalar) | ✅ | ✅ | ✅ | |
| Set operations (UNION ALL, INTERSECT, EXCEPT) | ✅ | ⚠️ | ⚠️ | INTERSECT/EXCEPT are FULL-only; AUTO falls back to FULL |
| Algebraic aggregates (COUNT, SUM, AVG, STDDEV, …) | ✅ | ✅ | ✅ | Fully invertible delta |
| Semi-algebraic aggregates (MIN, MAX) | ✅ | ✅ | ✅ | Group rescan on ambiguous delete |
| Group-rescan aggregates (STRING_AGG, ARRAY_AGG, …) | ✅ | ⚠️ | ⚠️ | Warning emitted at creation time |
| Window functions (ROW_NUMBER, RANK, LAG, LEAD, …) | ✅ | ✅ | ✅ | Partition-scoped recompute |
| CTEs (non-recursive and WITH RECURSIVE) | ✅ | ✅ | ✅ | Semi-naive / DRed strategies |
| TopK (ORDER BY … LIMIT) | ✅ | ✅ | ✅ | Scoped recomputation |
| LATERAL / set-returning functions / JSON_TABLE | ✅ | ✅ | ✅ | Row-scoped re-execution |
| ST-to-ST dependencies | ✅ | ✅ | ✅ | Differential via change buffers |
| VOLATILE functions | ✅ | ❌ | ❌ | Rejected at creation time |

**Legend:** ✅ fully supported — ⚠️ supported with caveats — ❌ not supported

> For details on each operator's delta strategy, auxiliary columns, and known limitations, see the full [Operator Support Matrix](DVM_OPERATORS.md#operator-support-matrix).

---

## Expression Support

pgtrickle's DVM parser supports a wide range of SQL expressions in defining queries. All expressions work in both `FULL` and `DIFFERENTIAL` modes.

### Conditional Expressions

| Expression | Example | Notes |
|---|---|---|
| `CASE WHEN … THEN … ELSE … END` | `CASE WHEN amount > 100 THEN 'high' ELSE 'low' END` | Searched CASE |
| `CASE <expr> WHEN … THEN … END` | `CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'inactive' END` | Simple CASE |
| `COALESCE(a, b, …)` | `COALESCE(phone, email, 'unknown')` | Returns first non-NULL argument |
| `NULLIF(a, b)` | `NULLIF(divisor, 0)` | Returns NULL if `a = b` |
| `GREATEST(a, b, …)` | `GREATEST(score1, score2, score3)` | Returns the largest value |
| `LEAST(a, b, …)` | `LEAST(price, max_price)` | Returns the smallest value |

### Comparison Operators

| Expression | Example | Notes |
|---|---|---|
| `IN (list)` | `category IN ('A', 'B', 'C')` | Also supports `NOT IN` |
| `BETWEEN a AND b` | `price BETWEEN 10 AND 100` | Also supports `NOT BETWEEN` |
| `IS DISTINCT FROM` | `a IS DISTINCT FROM b` | NULL-safe inequality |
| `IS NOT DISTINCT FROM` | `a IS NOT DISTINCT FROM b` | NULL-safe equality |
| `SIMILAR TO` | `name SIMILAR TO '%pattern%'` | SQL regex matching |
| `op ANY(array)` | `id = ANY(ARRAY[1,2,3])` | Array comparison |
| `op ALL(array)` | `score > ALL(ARRAY[50,60])` | Array comparison |

### Boolean Tests

| Expression | Example |
|---|---|
| `IS TRUE` | `active IS TRUE` |
| `IS NOT TRUE` | `flag IS NOT TRUE` |
| `IS FALSE` | `completed IS FALSE` |
| `IS NOT FALSE` | `valid IS NOT FALSE` |
| `IS UNKNOWN` | `result IS UNKNOWN` |
| `IS NOT UNKNOWN` | `flag IS NOT UNKNOWN` |

### SQL Value Functions

| Function | Description |
|---|---|
| `CURRENT_DATE` | Current date |
| `CURRENT_TIME` | Current time with time zone |
| `CURRENT_TIMESTAMP` | Current date and time with time zone |
| `LOCALTIME` | Current time without time zone |
| `LOCALTIMESTAMP` | Current date and time without time zone |
| `CURRENT_ROLE` | Current role name |
| `CURRENT_USER` | Current user name |
| `SESSION_USER` | Session user name |
| `CURRENT_CATALOG` | Current database name |
| `CURRENT_SCHEMA` | Current schema name |

### Array and Row Expressions

| Expression | Example | Notes |
|---|---|---|
| `ARRAY[…]` | `ARRAY[1, 2, 3]` | Array constructor |
| `ROW(…)` | `ROW(a, b, c)` | Row constructor |
| Array subscript | `arr[1]` | Array element access |
| Field access | `(rec).field` | Composite type field access |
| Star indirection | `(data).*` | Expand all fields |

### Subquery Expressions

Subqueries are supported in the `WHERE` clause and `SELECT` list. They are parsed into dedicated DVM operators with specialized delta computation for incremental maintenance.

| Expression | Example | DVM Operator |
|---|---|---|
| `EXISTS (subquery)` | `WHERE EXISTS (SELECT 1 FROM orders WHERE orders.cid = c.id)` | Semi-Join |
| `NOT EXISTS (subquery)` | `WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.cid = c.id)` | Anti-Join |
| `IN (subquery)` | `WHERE id IN (SELECT product_id FROM order_items)` | Semi-Join (rewritten as equality) |
| `NOT IN (subquery)` | `WHERE id NOT IN (SELECT product_id FROM order_items)` | Anti-Join |
| `ALL (subquery)` | `WHERE price > ALL (SELECT price FROM competitors)` | Anti-Join (NULL-safe) |
| Scalar subquery (SELECT) | `SELECT (SELECT max(price) FROM products) AS max_p` | Scalar Subquery |

**Notes:**
- `EXISTS` and `IN (subquery)` in the `WHERE` clause are transformed into semi-join operators. `NOT EXISTS` and `NOT IN (subquery)` become anti-join operators.
- **Multi-column `IN (subquery)` is not supported** (e.g., `WHERE (a, b) IN (SELECT x, y FROM ...)`). Rewrite as `WHERE EXISTS (SELECT 1 FROM ... WHERE a = x AND b = y)` for equivalent semantics.
- Multiple subqueries in the same `WHERE` clause are supported when combined with `AND`. Subqueries combined with `OR` are also supported — they are automatically rewritten into `UNION` of separate filtered queries.
- Scalar subqueries in the `SELECT` list are supported as long as they return exactly one row and one column.
- `ALL (subquery)` is supported — see the worked example below.

#### ALL (subquery) — Worked Example

`ALL (subquery)` tests whether a comparison holds against **every** row returned
by the subquery. pg_trickle rewrites it to a NULL-safe anti-join so it can be
maintained incrementally.

**Comparison operators supported:** `>`, `>=`, `<`, `<=`, `=`, `<>`

**Example — products cheaper than all competitors:**

```sql
-- Source tables
CREATE TABLE products (
    id    INT PRIMARY KEY,
    name  TEXT,
    price NUMERIC
);
CREATE TABLE competitor_prices (
    id          INT PRIMARY KEY,
    product_id  INT,
    price       NUMERIC
);

-- Sample data
INSERT INTO products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 24.99), (3, 'Gizmo', 14.99);
INSERT INTO competitor_prices VALUES (1, 1, 12.99), (2, 1, 11.50), (3, 2, 19.99), (4, 3, 14.99);

-- Stream table: find products priced below ALL competitor prices
SELECT pgtrickle.create_stream_table(
    name  => 'cheapest_products',
    query => $$
        SELECT p.id, p.name, p.price
        FROM products p
        WHERE p.price < ALL (
            SELECT cp.price
            FROM competitor_prices cp
            WHERE cp.product_id = p.id
        )
    $$,
    schedule => '1m'
);
```

**Result:** Widget (9.99 < all of [12.99, 11.50]) is included. Gadget (24.99 ≮ 19.99) is excluded. Gizmo (14.99 ≮ 14.99) is excluded.

**How pg_trickle handles it internally:**

1. `WHERE price < ALL (SELECT ...)` is parsed into an anti-join with a NULL-safe condition.
2. The condition `NOT (x op col)` is wrapped as `(col IS NULL OR NOT (x op col))` to correctly handle NULL values in the subquery — if any subquery row is NULL, the ALL comparison fails (standard SQL semantics).
3. The anti-join uses the same incremental delta computation as `NOT EXISTS`, so changes to either `products` or `competitor_prices` are propagated efficiently.

**Other common patterns:**

```sql
-- Employees whose salary meets or exceeds all department maximums
WHERE salary >= ALL (SELECT max_salary FROM department_caps)

-- Orders with ratings better than all thresholds
WHERE rating > ALL (SELECT min_rating FROM quality_thresholds)
```

### Auto-Rewrite Pipeline

pg_trickle transparently rewrites certain SQL constructs before parsing. These rewrites are applied automatically and require no user action:

| Order | Trigger | Rewrite |
|-------|---------|--------|
| #0 | View references in FROM | Inline view body as subquery |
| #1 | `DISTINCT ON (expr)` | Convert to `ROW_NUMBER() OVER (PARTITION BY expr ORDER BY ...) = 1` subquery |
| #2 | `GROUPING SETS` / `CUBE` / `ROLLUP` | Decompose into `UNION ALL` of separate `GROUP BY` queries |
| #3 | Scalar subquery in `WHERE` | Convert to `CROSS JOIN` with inline view |
| #4 | Correlated scalar subquery in `SELECT` | Convert to `LEFT JOIN` with grouped inline view |
| #5 | `EXISTS`/`IN` inside `OR` | Split into `UNION` of separate filtered queries |
| #6 | Multiple `PARTITION BY` clauses | Split into joined subqueries, one per distinct partitioning |
| #7 | Window functions inside expressions | Lift to inner subquery with synthetic `__pgt_wf_N` columns (see below) |

#### Window Functions in Expressions (Auto-Rewrite)

Window functions nested inside expressions (e.g., `CASE WHEN ROW_NUMBER() ...`,
`ABS(RANK() OVER (...) - 5)`) are automatically rewritten. pg_trickle lifts
each window function call into a synthetic column in an inner subquery, then
applies the original expression in the outer SELECT.

This rewrite is transparent — you write your query naturally and pg_trickle
handles it:

**Your query:**

```sql
SELECT
    id,
    name,
    CASE WHEN ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) = 1
         THEN 'top earner'
         ELSE 'other'
    END AS rank_label
FROM employees
```

**What pg_trickle generates internally:**

```sql
SELECT
    "__pgt_wf_inner".id,
    "__pgt_wf_inner".name,
    CASE WHEN "__pgt_wf_inner"."__pgt_wf_1" = 1
         THEN 'top earner'
         ELSE 'other'
    END AS "rank_label"
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS "__pgt_wf_1"
    FROM employees
) "__pgt_wf_inner"
```

The inner subquery produces the window function result as a plain column
(`__pgt_wf_1`), which the DVM engine can maintain incrementally using its
existing window function support. The outer expression is then a simple
column reference.

**More examples:**

```sql
-- Arithmetic with window functions
SELECT id, ABS(RANK() OVER (ORDER BY score) - 5) AS adjusted_rank
FROM players

-- COALESCE with window function
SELECT id, COALESCE(LAG(value) OVER (ORDER BY ts), 0) AS prev_value
FROM sensor_readings

-- Multiple window functions in expressions
SELECT id,
       ROW_NUMBER() OVER (ORDER BY created_at) * 100 AS seq,
       SUM(amount) OVER (ORDER BY created_at) / COUNT(*) OVER (ORDER BY created_at) AS running_avg
FROM transactions
```

All of these are handled automatically — each distinct window function call
is extracted to its own `__pgt_wf_N` synthetic column.

### HAVING Clause

`HAVING` is fully supported. The filter predicate is applied on top of the aggregate delta computation — groups that pass the HAVING condition are included in the stream table.

```sql
SELECT pgtrickle.create_stream_table(
    name     => 'big_departments',
    query    => 'SELECT department, COUNT(*) AS cnt FROM employees GROUP BY department HAVING COUNT(*) > 10',
    schedule => '1m'
);
```

### Tables Without Primary Keys (Keyless Tables)

Tables without a primary key can be used as sources. pg_trickle generates a content-based row identity
by hashing all column values using `pg_trickle_hash_multi()`. This allows DIFFERENTIAL mode to work,
though at the cost of being unable to distinguish truly duplicate rows (rows with identical values in all columns).

```sql
-- No primary key — pg_trickle uses content hashing for row identity
CREATE TABLE events (ts TIMESTAMPTZ, payload JSONB);
SELECT pgtrickle.create_stream_table(
    name     => 'event_summary',
    query    => 'SELECT payload->>''type'' AS event_type, COUNT(*) FROM events GROUP BY 1',
    schedule => '1m'
);
```

> **Known Limitation — Duplicate Rows in Keyless Tables (G7.1)**
>
> When a keyless table contains **exact duplicate rows** (identical values in every column),
> content-based hashing produces the same `__pgt_row_id` for each copy. Consequences:
>
> - **INSERT** of a duplicate row may appear as a no-op (the hash already exists in the stream table).
> - **DELETE** of one copy may delete all copies (the MERGE matches on `__pgt_row_id`, hitting every duplicate).
> - **Aggregate counts** over keyless tables with duplicates may drift from the true query result.
>
> **Recommendation:** Add a `PRIMARY KEY` or at least a `UNIQUE` constraint to source tables used
> in DIFFERENTIAL mode. This eliminates the ambiguity entirely. If duplicates are expected and
> correctness matters, use `FULL` refresh mode, which always recomputes from scratch.

### Volatile Function Detection

PostgreSQL analyzes the defining query before pg_trickle checks volatility. The
check uses the exact function, operator, and cast overload selected from the
schema and argument types.

- **VOLATILE** and **STABLE** functions (e.g., `random()`, `clock_timestamp()`, `now()`) are FULL-only in incremental admission. AUTO falls back to FULL; explicit DIFFERENTIAL and IMMEDIATE modes are rejected.
- **VOLATILE operators** backed by volatile functions are also detected.
- **IMMUTABLE** functions are always safe and produce no warnings.

FULL mode accepts all volatility classes since it re-evaluates the entire query each time.

#### Volatile Function Policy (VOL-1)

The `pg_trickle.volatile_function_policy` GUC controls how volatile functions are handled:

| Value | Behavior |
|-------|----------|
| `reject` (default) | ERROR — volatile functions are rejected at creation time. |
| `warn` | Retained for compatibility; incremental admission still falls back or rejects. |
| `allow` | Retained for compatibility; incremental admission still falls back or rejects. |

```sql
-- Allow volatile functions with a warning
SET pg_trickle.volatile_function_policy = 'warn';

-- Allow volatile functions silently
SET pg_trickle.volatile_function_policy = 'allow';

-- Restore default (reject volatile functions)
SET pg_trickle.volatile_function_policy = 'reject';
```

### COLLATE Expressions

`COLLATE` clauses on expressions are supported:

```sql
SELECT pgtrickle.create_stream_table(
    name     => 'sorted_names',
    query    => 'SELECT name COLLATE "C" AS c_name FROM users',
    schedule => '1m'
);
```

### IS JSON Predicate (PostgreSQL 16+)

The `IS JSON` predicate validates whether a value is valid JSON. All variants are supported:

```sql
-- Filter rows with valid JSON
SELECT pgtrickle.create_stream_table(
    name     => 'valid_json_events',
    query    => 'SELECT id, payload FROM events WHERE payload::text IS JSON',
    schedule => '1m'
);

-- Type-specific checks
SELECT pgtrickle.create_stream_table(
    name         => 'json_objects_only',
    query        => 'SELECT id, data IS JSON OBJECT AS is_obj,
          data IS JSON ARRAY AS is_arr,
          data IS JSON SCALAR AS is_scalar
   FROM json_data',
    schedule     => '1m',
    refresh_mode => 'FULL'
);
```

Supported variants: `IS JSON`, `IS JSON OBJECT`, `IS JSON ARRAY`, `IS JSON SCALAR`, `IS NOT JSON` (all forms), `WITH UNIQUE KEYS`.

### SQL/JSON Constructors (PostgreSQL 16+)

SQL-standard JSON constructor functions are supported in both FULL and DIFFERENTIAL modes:

```sql
-- JSON_OBJECT: construct a JSON object from key-value pairs
SELECT pgtrickle.create_stream_table(
    name     => 'user_json',
    query    => 'SELECT id, JSON_OBJECT(''name'' : name, ''age'' : age) AS data FROM users',
    schedule => '1m'
);

-- JSON_ARRAY: construct a JSON array from values
SELECT pgtrickle.create_stream_table(
    name         => 'value_arrays',
    query        => 'SELECT id, JSON_ARRAY(a, b, c) AS arr FROM measurements',
    schedule     => '1m',
    refresh_mode => 'FULL'
);

-- JSON(): parse a text value as JSON
-- JSON_SCALAR(): wrap a scalar value as JSON
-- JSON_SERIALIZE(): serialize a JSON value to text
```

> **Note:** `JSON_ARRAYAGG()` and `JSON_OBJECTAGG()` are SQL-standard aggregate functions fully recognized by the DVM engine. In DIFFERENTIAL mode, they use the group-rescan strategy (affected groups are re-aggregated from source data). The full deparsed SQL is preserved to handle the special `key: value`, `ABSENT ON NULL`, `ORDER BY`, and `RETURNING` clause syntax.

### JSON_TABLE (PostgreSQL 17+)

`JSON_TABLE()` generates a relational table from JSON data. It is supported in the FROM clause in both FULL and DIFFERENTIAL modes. Internally, it is modeled as a `LateralFunction`.

```sql
-- Extract structured data from a JSON column
SELECT pgtrickle.create_stream_table(
    name     => 'user_phones',
    query    => $$SELECT u.id, j.phone_type, j.phone_number
    FROM users u,
         JSON_TABLE(u.contact_info, '$.phones[*]'
           COLUMNS (
             phone_type TEXT PATH '$.type',
             phone_number TEXT PATH '$.number'
           )
         ) AS j$$,
    schedule => '1m'
);
```

Supported column types:
- **Regular columns** — `name TYPE PATH '$.path'` (with optional `ON ERROR`/`ON EMPTY` behaviors)
- **EXISTS columns** — `name TYPE EXISTS PATH '$.path'`
- **Formatted columns** — `name TYPE FORMAT JSON PATH '$.path'`
- **Nested columns** — `NESTED PATH '$.path' COLUMNS (...)`

The `PASSING` clause is also supported for passing named variables to path expressions.

### Unsupported Expression Types

The following are **rejected with clear error messages** rather than producing broken SQL:

| Expression | Error Behavior | Suggested Rewrite |
|---|---|---|
| `TABLESAMPLE` | Rejected — stream tables materialize the complete result set | Use `WHERE random() < 0.1` if sampling is needed |
| `FOR UPDATE` / `FOR SHARE` | Rejected — stream tables do not support row-level locking | Remove the locking clause |
| Unknown node types | Rejected with type information | — |

> **Note:** Window functions inside expressions (e.g., `CASE WHEN ROW_NUMBER() OVER (...) ...`) were unsupported in earlier versions but are now **automatically rewritten** — see [Auto-Rewrite Pipeline § Window Functions in Expressions](#window-functions-in-expressions-auto-rewrite).

---

## Restrictions & Interoperability

Stream tables are standard PostgreSQL heap tables stored in the `pgtrickle` schema with an additional `__pgt_row_id BYTEA NOT NULL` column managed by the refresh engine. Depending on the identity schema, pg_trickle uses either a direct full-ID index or a non-unique `row_probe_v1(__pgt_row_id)` accelerator; full `BYTEA` equality remains authoritative. This section describes what you can and cannot do with them.

### Referencing Other Stream Tables

Stream tables **can** reference other stream tables in their defining query. This creates a dependency edge in the internal DAG, and the scheduler refreshes upstream tables before downstream ones. By default, cycles are detected and rejected at creation time.

When `pg_trickle.allow_circular = true`, circular dependencies are allowed for stream tables that use DIFFERENTIAL refresh mode and have **monotone** defining queries (no aggregates, EXCEPT, window functions, or NOT EXISTS/NOT IN). Cycle members are assigned an `scc_id` and the scheduler iterates them to a fixed point. Non-monotone operators are rejected because they prevent convergence.

```sql
-- ST1 reads from a base table
SELECT pgtrickle.create_stream_table(
    name     => 'order_totals',
    query    => 'SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id',
    schedule => '1m'
);

-- ST2 reads from ST1
SELECT pgtrickle.create_stream_table(
    name     => 'big_customers',
    query    => 'SELECT customer_id, total FROM pgtrickle.order_totals WHERE total > 1000',
    schedule => '1m'
);
```

### Views as Sources in Defining Queries

PostgreSQL views **can** be used as source tables in a stream table's defining query. Views are automatically **inlined** — replaced with their underlying SELECT definition as subqueries — so CDC triggers land on the actual base tables.

```sql
CREATE VIEW active_orders AS
  SELECT * FROM orders WHERE status = 'active';

-- This works (views are auto-inlined):
SELECT pgtrickle.create_stream_table(
    name     => 'order_summary',
    query    => 'SELECT customer_id, COUNT(*) FROM active_orders GROUP BY customer_id',
    schedule => '1m'
);
-- Internally, 'active_orders' is replaced with:
--   (SELECT ... FROM orders WHERE status = 'active') AS active_orders
```

**Nested views** (view → view → table) are fully expanded via a fixpoint loop. **Column-renaming views** (`CREATE VIEW v(a, b) AS ...`) work correctly — `pg_get_viewdef()` produces the proper column aliases.

When a view is inlined, the user's original SQL is stored in the `original_query` catalog column for reinit and introspection. The `defining_query` column contains the expanded (post-inlining) form.

**DDL hooks:** `CREATE OR REPLACE VIEW` on a view that was inlined into a stream table marks that ST for reinit. `DROP VIEW` sets affected STs to ERROR status.

**Materialized views** are **rejected** in DIFFERENTIAL mode — their stale-snapshot semantics prevent CDC triggers from tracking changes.  Use the underlying query directly, or switch to FULL mode. In FULL mode, materialized views are allowed (no CDC needed).

**Foreign tables** are **rejected** in DIFFERENTIAL mode — row-level triggers cannot be created on foreign tables. Use FULL mode instead.

### Partitioned Tables as Sources

**Partitioned tables are fully supported** as source tables in both FULL and DIFFERENTIAL modes. CDC triggers are installed on the partitioned parent table, and PostgreSQL 13+ ensures the trigger fires for all DML routed to child partitions. The change buffer uses the parent table's OID (`pgtrickle_changes.changes_<parent_oid>`).

```sql
CREATE TABLE orders (
    id INT, region TEXT, amount NUMERIC
) PARTITION BY LIST (region);
CREATE TABLE orders_us PARTITION OF orders FOR VALUES IN ('US');
CREATE TABLE orders_eu PARTITION OF orders FOR VALUES IN ('EU');

-- Works — inserts into any partition are captured:
SELECT pgtrickle.create_stream_table(
    name     => 'order_summary',
    query    => 'SELECT region, SUM(amount) FROM orders GROUP BY region',
    schedule => '1m'
);
```

**ATTACH PARTITION detection:** When a new partition is attached to a tracked
source table via `ALTER TABLE parent ATTACH PARTITION child ...`, pg_trickle's
DDL event trigger detects the change in partition structure and automatically
marks affected stream tables for reinitialize. This ensures pre-existing rows
in the newly attached partition are included on the next refresh. DETACH
PARTITION is also detected and triggers reinitialization.

**WAL mode:** When using WAL-based CDC (`cdc_mode = 'wal'`), publications for
partitioned source tables are created with `publish_via_partition_root = true`.
This ensures changes from child partitions are published under the parent
table's identity, matching trigger-mode CDC behavior.

> **Note:** pg_trickle targets PostgreSQL 18. On PostgreSQL 12 or earlier (not supported), parent triggers do **not** fire for partition-routed rows, which would cause silent data loss.

### Foreign Tables as Sources

Foreign tables (via `postgres_fdw` or other FDWs) can be used as stream table
sources with these constraints:

| CDC Method | Supported? | Why |
|------------|-----------|-----|
| Trigger-based | ❌ No | Foreign tables don't support row-level triggers |
| WAL-based | ❌ No | Foreign tables don't generate local WAL entries |
| FULL refresh | ✅ Yes | Re-executes the remote query each cycle |
| Polling-based | ✅ Yes | When `pg_trickle.foreign_table_polling = on` |

```sql
-- Foreign table source — FULL refresh only
SELECT pgtrickle.create_stream_table(
    name         => 'remote_summary',
    query        => 'SELECT region, SUM(amount) FROM remote_orders GROUP BY region',
    schedule     => '5m',
    refresh_mode => 'FULL'
);
```

When pg_trickle detects a foreign table source, it emits an INFO message
explaining the constraints. If you attempt to use DIFFERENTIAL mode without
polling enabled, the creation will succeed but the refresh falls back to FULL.

**Polling-based CDC** creates a local snapshot table and computes `EXCEPT ALL`
differences on each refresh. Enable with:

```sql
SET pg_trickle.foreign_table_polling = on;
```

> For a complete step-by-step setup guide, see the
> [Foreign Table Sources tutorial](tutorials/FOREIGN_TABLE_SOURCES.md).

### IMMEDIATE Mode Query Restrictions

The `'IMMEDIATE'` refresh mode supports nearly all SQL constructs supported by `'DIFFERENTIAL'` and `'FULL'` modes. Queries are validated at stream table creation and when switching to IMMEDIATE mode via `alter_stream_table`.

**Supported in IMMEDIATE mode:**

- Simple `SELECT ... FROM table` scans, filters, projections
- `JOIN` (INNER, LEFT, FULL OUTER)
- `GROUP BY` with standard aggregates (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, etc.)
- `DISTINCT`
- Non-recursive `WITH` (CTEs)
- `UNION ALL`, `INTERSECT`, `EXCEPT`
- `EXISTS` / `IN` subqueries (`SemiJoin`, `AntiJoin`)
- Subqueries in `FROM`
- Window functions (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, etc.)
- `LATERAL` subqueries
- `LATERAL` set-returning functions (`unnest()`, `jsonb_array_elements()`, etc.)
- Scalar subqueries in `SELECT`
- Cascading IMMEDIATE stream tables (ST depending on another IMMEDIATE ST)
- Recursive CTEs (`WITH RECURSIVE`) — uses semi-naive evaluation (INSERT-only) or
  Delete-and-Rederive (DELETE/UPDATE); bounded by `pg_trickle.ivm_recursive_max_depth`
  (default 100) to guard against infinite loops from cyclic data

**Not yet supported in IMMEDIATE mode:**

None — all constructs that work in `'DIFFERENTIAL'` mode are now also available in
`'IMMEDIATE'` mode.

**Notes on `WITH RECURSIVE` in IMMEDIATE mode:**

- A `__pgt_depth` counter is injected into the generated semi-naive SQL.  Propagation
  stops when the counter reaches `ivm_recursive_max_depth` (default 100).  Raise this
  GUC for deeper hierarchies or set it to 0 to disable the guard.
- A WARNING is emitted at stream table creation time reminding operators to monitor
  for `stack depth limit exceeded` errors on very deep hierarchies.
- Non-linear recursion (multiple self-references) is rejected — PostgreSQL itself
  enforces this restriction.

Attempting to create a stream table with an unsupported construct produces a clear
error message.

### Logical Replication Targets

Tables that receive data via **logical replication** require special consideration. Changes arriving via replication do **not** fire normal row-level triggers, which means CDC triggers will miss those changes.

pg_trickle emits a **WARNING** at stream table creation time if any source table is detected as a logical replication target (via `pg_subscription_rel`).

**Workarounds:**
- Use `cdc_mode = 'wal'` for WAL-based CDC that captures all changes regardless of origin.
- Use `FULL` refresh mode, which recomputes entirely from the current table state.
- Set a frequent refresh schedule with FULL mode to limit staleness.

### Views on Stream Tables

PostgreSQL views **can** reference stream tables. The view reflects the data as of the most recent refresh.

```sql
CREATE VIEW top_customers AS
SELECT customer_id, total
FROM pgtrickle.order_totals
WHERE total > 500
ORDER BY total DESC;
```

### Materialized Views on Stream Tables

Materialized views **can** reference stream tables, though this is typically redundant (both are physical snapshots of a query). The materialized view requires its own `REFRESH MATERIALIZED VIEW` — it does **not** auto-refresh when the stream table refreshes.

### Logical Replication of Stream Tables

Stream tables **can** be published for logical replication like any ordinary table:

```sql
-- On publisher
CREATE PUBLICATION my_pub FOR TABLE pgtrickle.order_totals;

-- On subscriber
CREATE SUBSCRIPTION my_sub
  CONNECTION 'host=... dbname=...'
  PUBLICATION my_pub;
```

**Caveats:**
- The `__pgt_row_id` column is replicated (it is the primary key), which is an internal implementation detail.
- The subscriber receives materialized data, not the defining query. Refreshes on the publisher propagate as normal DML via logical replication.
- Do **not** install pg_trickle on the subscriber and attempt to refresh the replicated table — it will have no CDC triggers or catalog entries.
- The internal change buffer tables (`pgtrickle_changes.changes_<oid>`) and catalog tables are **not** published by default; subscribers only receive the final output.

### Known Delta Computation Limitations

The following edge cases produce incorrect delta results in DIFFERENTIAL mode under specific
data mutation patterns. They have no effect on FULL mode.

#### JOIN Key Column Change + Simultaneous Right-Side Delete — Fixed (EC-01)

> **Resolved.** This limitation no longer exists — the delta query
> now uses a pre-change right snapshot (R₀) for DELETE deltas, ensuring stale
> rows are correctly removed even when the join partner is simultaneously deleted.

The fix splits Part 1 of the JOIN delta into two arms:
- **Part 1a** (inserts): `ΔL_inserts ⋈ R₁` — uses current right state
- **Part 1b** (deletes): `ΔL_deletes ⋈ R₀` — uses pre-change right state

R₀ is reconstructed as `R_current EXCEPT ALL ΔR_inserts UNION ALL ΔR_deletes` (or via
NOT EXISTS anti-join for simple Scan nodes). This ensures the DELETE half always
finds the old join partner, even if that partner was deleted in the same cycle.

The fix applies to INNER JOIN, LEFT JOIN, and FULL OUTER JOIN delta operators.
See [DVM_OPERATORS.md](DVM_OPERATORS.md) for implementation details.

#### CUBE/ROLLUP Expansion Limit

`CUBE(a, b, c...n)` on **N** columns generates $2^N$ grouping set branches (a UNION ALL of N queries).
pg_trickle rejects CUBE/ROLLUP that would produce more than **64 branches** to prevent runaway
memory usage during query generation. Use explicit `GROUPING SETS(...)` instead:

```sql
-- Rejected: CUBE(a, b, c, d, e, f, g) would generate 128 branches
-- Use instead:
SELECT pgtrickle.create_stream_table(
    name     => 'multi_dim',
    query    => 'SELECT a, b, c, SUM(v) FROM t
   GROUP BY GROUPING SETS ((a, b, c), (a, b), (a), ())',
    schedule => '5m'
);
```

### What Is NOT Allowed

| Operation | Restriction | Reason |
|---|---|---|
| Direct DML (`INSERT`, `UPDATE`, `DELETE`) | ❌ Not supported | Stream table contents are managed exclusively by the refresh engine. |
| Direct DDL (`ALTER TABLE`) | ❌ Not supported | Use `pgtrickle.alter_stream_table()` to change the defining query or schedule. |
| Foreign keys referencing or from a stream table | ❌ Not supported | The refresh engine performs bulk `MERGE` operations that do not respect FK ordering. |
| User-defined triggers on stream tables | ✅ Supported (DIFFERENTIAL) | In DIFFERENTIAL mode, the refresh engine decomposes changes into explicit DELETE + UPDATE + INSERT statements so triggers fire with correct `TG_OP`, `OLD`, and `NEW`. Row-level triggers are suppressed during FULL refresh. Controlled by `pg_trickle.user_triggers` GUC (default: `auto`). |
| `TRUNCATE` on a stream table | ❌ Not supported | Use `pgtrickle.refresh_stream_table()` to reset data. |

> **Tip:** The `__pgt_row_id` column is visible but should be ignored by consuming queries — it is an implementation detail used for delta `MERGE` operations.

### Internal `__pgt_*` Auxiliary Columns

Stream tables may contain additional hidden columns whose names begin with `__pgt_`. These are managed exclusively by the refresh engine — they are **not** part of the user-visible schema and should never be read or written by application queries.

#### `__pgt_row_id` — Row identity (always present)

Every stream table has a `BYTEA NOT NULL` column named `__pgt_row_id`. It stores the complete canonical V2 identity, updated by the refresh engine on every refresh. A bounded identity uses a direct B-tree; an unbounded identity uses a `row_probe_v1(__pgt_row_id)` accelerator plus exact full-identity equality for inserts, updates, and deletes.

#### `__pgt_count` — Group multiplicity (aggregates & DISTINCT)

Added when the defining query contains `GROUP BY`, `DISTINCT`, `UNION ALL ... GROUP BY`, or any aggregate expression that requires tracking how many source rows contribute to each output row.

| Type | Triggers |
|------|---------|
| `BIGINT NOT NULL DEFAULT 0` | `GROUP BY`, `DISTINCT`, `COUNT(*)`, `SUM(...)`, `AVG(...)`, `STDDEV(...)`, `VAR(...)`, `UNION` deduplication |

#### `__pgt_count_l` / `__pgt_count_r` — Legacy dual multiplicity

Older INTERSECT/EXCEPT stream tables may contain these columns from the
pre-WP2 boundary-update implementation. New FULL set-operation tables do not
create them, and a successful FULL refresh removes them. INTERSECT/EXCEPT
remain guarded from DIFFERENTIAL and IMMEDIATE admission until their durable
private state is complete.

| Type | Triggers |
|------|---------|
| `BIGINT NOT NULL DEFAULT 0` each | `INTERSECT`, `INTERSECT ALL`, `EXCEPT`, `EXCEPT ALL` |

#### `__pgt_aux_sum_<alias>` / `__pgt_aux_count_<alias>` — Running totals for AVG

Pairs of auxiliary columns added for each `AVG(expr)` in the query. Instead of recomputing the average from scratch on each delta, the refresh engine maintains a running sum and count and derives the average algebraically.

| Type | Triggers |
|------|---------|
| `NUMERIC NOT NULL DEFAULT 0` (sum), `BIGINT NOT NULL DEFAULT 0` (count) | Any `AVG(expr)` in `GROUP BY` query |

Named `__pgt_aux_sum_<output_alias>` and `__pgt_aux_count_<output_alias>`, where `<output_alias>` is the column alias for the `AVG` expression in the SELECT list.

#### `__pgt_aux_sum2_<alias>` — Sum-of-squares for STDDEV / VARIANCE

Added alongside the sum/count pair when the query contains `STDDEV`, `STDDEV_POP`, `STDDEV_SAMP`, `VARIANCE`, `VAR_POP`, or `VAR_SAMP`. Enables O(1) algebraic computation of variance from the Welford identity.

| Type | Triggers |
|------|---------|
| `NUMERIC NOT NULL DEFAULT 0` | `STDDEV(...)`, `STDDEV_POP(...)`, `STDDEV_SAMP(...)`, `VARIANCE(...)`, `VAR_POP(...)`, `VAR_SAMP(...)` |

#### `__pgt_aux_sumx_*` / `__pgt_aux_sumy_*` / `__pgt_aux_sumxy_*` / `__pgt_aux_sumx2_*` / `__pgt_aux_sumy2_*` — Cross-product accumulators for regression aggregates

Five auxiliary columns per aggregate, used for O(1) algebraic maintenance of the twelve PostgreSQL regression and correlation aggregates.

| Type | Triggers |
|------|---------|
| `NUMERIC NOT NULL DEFAULT 0` (five columns per aggregate) | `CORR(Y,X)`, `COVAR_POP(Y,X)`, `COVAR_SAMP(Y,X)`, `REGR_AVGX(Y,X)`, `REGR_AVGY(Y,X)`, `REGR_COUNT(Y,X)`, `REGR_INTERCEPT(Y,X)`, `REGR_R2(Y,X)`, `REGR_SLOPE(Y,X)`, `REGR_SXX(Y,X)`, `REGR_SXY(Y,X)`, `REGR_SYY(Y,X)` |

The five columns are named with base prefix `__pgt_aux_<kind>_<output_alias>` where `<kind>` is `sumx`, `sumy`, `sumxy`, `sumx2`, or `sumy2`. The shared group count is stored in the companion `__pgt_aux_count_<output_alias>` column.

#### `__pgt_aux_nonnull_<alias>` — Non-NULL count for SUM + FULL OUTER JOIN

Added when the query contains `SUM(expr)` inside a `FULL OUTER JOIN` aggregate. When matched rows transition to unmatched (null-padded), standard algebraic SUM would produce `0` instead of `NULL`. This counter tracks how many non-NULL argument values exist in each group; when it reaches zero the SUM is definitively `NULL` without a full rescan.

| Type | Triggers |
|------|---------|
| `BIGINT NOT NULL DEFAULT 0` | `SUM(expr)` in a query with `FULL OUTER JOIN` at the top level |

#### `__pgt_wf_<N>` — Window function lift-out (query rewrite)

Added at query-rewrite time (before storage table creation) when the defining query contains window functions embedded inside larger expressions (e.g. `CASE WHEN ROW_NUMBER() OVER (...) = 1 THEN ...`). The engine lifts the window function to a synthetic inner-subquery column so the outer SELECT can reference it by alias.

| Type | Triggers |
|------|---------|
| Inherits the window-function return type | Window function inside expression — e.g. `RANK()`, `ROW_NUMBER()`, `DENSE_RANK()`, `LAG()`, `LEAD()`, etc. |

#### `__pgt_depth` — Recursion depth counter (recursive CTE)

Present only inside the DVM-generated SQL for recursive CTE queries. Used to limit unbounded recursion in semi-naive evaluation. Not added as a permanent column to the storage table.

---

> **Rule of thumb:** Unless you see an `ALTER TABLE` query mentioning one of these columns, they are transparent to consuming queries. Never `SELECT __pgt_*` columns in application code — their names, types, and presence may change across minor versions.

### Row-Level Security (RLS)

Every refresh evaluates defining SQL as the stream-table owner with
`row_security = on`. Source-table policies therefore determine the rows that
are materialized, while policies on the stream table determine who may read
that materialized result.

#### How It Works

| Area | Behavior |
|------|----------|
| **RLS on source tables** | Applied as the stream-table owner during initial, manual, scheduled, full, differential, and IMMEDIATE maintenance. |
| **RLS on the stream table** | Works naturally. Enable RLS and create policies on the stream table to filter reads per role — exactly as you would on any regular table. |
| **RLS policy changes on source tables** | `CREATE POLICY`, `ALTER POLICY`, and `DROP POLICY` on a source table are detected by pg_trickle's DDL event trigger and mark the stream table for reinitialisation. |
| **ENABLE/DISABLE RLS on source tables** | `ALTER TABLE … ENABLE ROW LEVEL SECURITY` and `DISABLE ROW LEVEL SECURITY` on a source table mark the stream table for reinitialisation. |
| **Change buffer tables** | Private change buffers remain extension-only. Differential refresh copies the bounded CDC window to a temporary stage, grants the stream owner read access only to that stage, and evaluates the delta without granting access to `pgtrickle_changes`. |
| **IMMEDIATE mode** | Trigger bookkeeping remains `SECURITY DEFINER` with a locked `search_path`; definition-derived delta SQL switches to the stream owner and stored defining path before evaluation. |

#### Recommended Pattern: RLS on the Stream Table

```sql
-- 1. Create a stream table (materializes rows visible to its owner)
SELECT pgtrickle.create_stream_table(
    name  => 'order_totals',
    query => 'SELECT tenant_id, SUM(amount) AS total FROM orders GROUP BY tenant_id'
);

-- 2. Enable RLS on the stream table
ALTER TABLE pgtrickle.order_totals ENABLE ROW LEVEL SECURITY;

-- 3. Create per-tenant policies
CREATE POLICY tenant_isolation ON pgtrickle.order_totals
    USING (tenant_id = current_setting('app.tenant_id')::INT);

-- 4. Each role sees only its own rows
SET app.tenant_id = '42';
SELECT * FROM pgtrickle.order_totals;  -- only tenant 42's rows
```

> **Note:** Source and target RLS have different jobs: source policies define
> the materialized data set; target policies filter reads of that data set.

---

## Views

### pgtrickle.stream_tables_info

Status overview with computed staleness information.

```sql
SELECT * FROM pgtrickle.stream_tables_info;
```

Columns include all `pgtrickle.pgt_stream_tables` columns plus:

| Column | Type | Description |
|---|---|---|
| `staleness` | `interval` | `now() - last_refresh_at` |
| `stale` | `bool` | `true` when the scheduler itself is behind (last_refresh_at age exceeds schedule); `false` when the scheduler is healthy even if source tables have had no writes |

---

### pgtrickle.pg_stat_stream_tables

Comprehensive monitoring view combining catalog metadata with aggregate refresh statistics.

```sql
SELECT * FROM pgtrickle.pg_stat_stream_tables;
```

Key columns:

| Column | Type | Description |
|---|---|---|
| `pgt_id` | `bigint` | Stream table ID |
| `pgt_schema` / `pgt_name` | `text` | Schema and name |
| `status` | `text` | INITIALIZING, ACTIVE, SUSPENDED, ERROR |
| `refresh_mode` | `text` | FULL or DIFFERENTIAL |
| `data_timestamp` | `timestamptz` | Timestamp of last refresh |
| `staleness` | `interval` | `now() - last_refresh_at` |
| `stale` | `bool` | `true` when the scheduler is behind its schedule; `false` when the scheduler is healthy (quiet source tables do not count as stale) |
| `total_refreshes` | `bigint` | Total refresh count |
| `successful_refreshes` | `bigint` | Successful refresh count |
| `failed_refreshes` | `bigint` | Failed refresh count |
| `avg_duration_ms` | `float8` | Average refresh duration |
| `consecutive_errors` | `int` | Current error streak |
| `cdc_modes` | `text[]` | Distinct CDC modes across TABLE-type sources (e.g. `{wal}`, `{trigger,wal}`, `{transitioning,wal}`) |
| `scc_id` | `int` | SCC group identifier for circular dependencies (`NULL` if not in a cycle) |
| `last_fixpoint_iterations` | `int` | Number of fixpoint iterations in the last SCC convergence (`NULL` if not cyclic) |

---

### pgtrickle.pg_stat_pgtrickle

Bounded PostgreSQL-style statistics view. Its counters and percentiles come
from maintained summary tables, so scraping it does not scan refresh history.

```sql
SELECT * FROM pgtrickle.pg_stat_pgtrickle;
```

The view reports refresh totals, average/p95/p99 duration, current lag,
requested and target freshness modes, the last machine-readable FULL reason
and detail, the last operational error, and `stats_reset_at`.

---

### pgtrickle.quick_health

Single-row health summary for dashboards and alerting. Returns the overall
health status of the pg_trickle extension at a glance.

```sql
SELECT * FROM pgtrickle.quick_health;
```

| Column | Type | Description |
|---|---|---|
| `total_stream_tables` | `bigint` | Total number of stream tables |
| `error_tables` | `bigint` | Stream tables with `status = 'ERROR'` or `consecutive_errors > 0` |
| `stale_tables` | `bigint` | Stream tables whose data is older than their schedule interval |
| `scheduler_running` | `boolean` | Whether a pg_trickle scheduler backend is detected in `pg_stat_activity` |
| `status` | `text` | Overall status: `EMPTY`, `OK`, `WARNING`, or `CRITICAL` |

**Status values:**
- `EMPTY` — No stream tables exist.
- `OK` — All stream tables are healthy and up-to-date.
- `WARNING` — Some tables have errors or are stale.
- `CRITICAL` — At least one stream table is `SUSPENDED`.

---

### pgtrickle.pgt_cdc_status

Convenience view for inspecting the CDC mode and WAL slot state of every
TABLE-type source for all stream tables. Useful for monitoring in-progress
TRIGGER→WAL transitions.

```sql
SELECT * FROM pgtrickle.pgt_cdc_status;
```

| Column | Type | Description |
|---|---|---|
| `pgt_schema` | `text` | Schema of the stream table |
| `pgt_name` | `text` | Name of the stream table |
| `source_relid` | `oid` | OID of the source table |
| `source_name` | `text` | Name of the source table |
| `source_schema` | `text` | Schema of the source table |
| `cdc_mode` | `text` | Current CDC mode: `trigger`, `transitioning`, or `wal` |
| `slot_name` | `text` | Replication slot name (`NULL` for trigger mode) |
| `decoder_confirmed_lsn` | `pg_lsn` | Last WAL position decoded (`NULL` for trigger mode) |
| `transition_started_at` | `timestamptz` | When the trigger→WAL transition began (`NULL` if not transitioning) |

Subscribe to the `pgtrickle_cdc_transition` NOTIFY channel to receive real-time
events when a source moves between CDC modes (payload is a JSON object with
`source_oid`, `from`, and `to` fields).

---

## Stream Table Lifecycle

Each stream table progresses through well-defined states stored in
`pgtrickle.pgt_stream_tables.status`.  Understanding the lifecycle helps with
troubleshooting, automation, and capacity planning.

### Status values

| Status | Meaning | Refreshes? | User operations allowed |
|--------|---------|-----------|------------------------|
| `INITIALIZING` | The stream table has been registered but has not yet completed its first full refresh. `is_populated` is `false`. | First refresh is in progress or queued | `ALTER STREAM TABLE`, `DROP STREAM TABLE`, `refresh_stream_table()` |
| `ACTIVE` | Normal operating state. The scheduler picks it up on each cycle. `is_populated` is `true`. | Yes — DIFFERENTIAL or FULL per schedule | All operations |
| `SUSPENDED` | Paused — either by a user call (`ALTER STREAM TABLE … SUSPEND`) or automatically after too many consecutive errors. | No | `ALTER STREAM TABLE … RESUME`, `DROP STREAM TABLE` |
| `ERROR` | An unrecoverable error was detected during refresh. The stream table is suspended and cannot auto-resume. | No | Inspect `pgt_refresh_history`, fix the root cause, then `ALTER STREAM TABLE … RESUME` |

### State transitions

```
                      ┌─────────────┐
        create_stream_table()       │ INITIALIZING │
                      └──────┬──────┘
                             │  first successful refresh
                             ▼
                        ┌────────┐
            resume ───► │ ACTIVE │ ◄─── resume
                        └───┬────┘
                            │ consecutive_errors ≥ max_consecutive_errors
                            │   OR user suspension
                            ▼
                       ┌───────────┐
                       │ SUSPENDED │
                       └───────────┘
                            │ unrecoverable internal error
                            ▼
                         ┌───────┐
                         │ ERROR │
                         └───────┘
```

### Key catalog fields

| Field | Type | Description |
|-------|------|-------------|
| `is_populated` | `bool` | `false` until the first successful refresh completes. Queries against an unpopulated stream table return 0 rows. |
| `consecutive_errors` | `int` | Number of consecutive refresh failures. Reset to 0 on success. When this reaches `pg_trickle.max_consecutive_errors` (default `3`), the stream table is automatically suspended. |
| `needs_reinit` | `bool` | Set to `true` when a DDL change to a source table is detected (column added/removed/renamed). The next refresh will run as a full reinitialisation rather than an incremental update. |
| `data_timestamp` | `timestamptz` | Timestamp bound to which the stream table's data is current. Advances after each successful refresh. |
| `last_refresh_at` | `timestamptz` | Wall-clock time of the last completed refresh attempt (success or failure). |

### Auto-suspend behaviour

When `consecutive_errors` reaches `pg_trickle.max_consecutive_errors` (default
`3`), pg_trickle suspends the stream table automatically and logs a `WARNING`.
To resume:

```sql
-- Inspect the error
SELECT pgt_name, status, consecutive_errors, last_refresh_error
FROM pgtrickle.pgt_stream_tables
WHERE status = 'SUSPENDED';

-- Fix the root cause, then resume
ALTER STREAM TABLE my_summary RESUME;
```

Use `pg_trickle.max_consecutive_errors = 0` to disable auto-suspension (not
recommended for production).

### Reinitialisation

When `needs_reinit = true`, the next scheduled or manual refresh performs a full
reinit: the storage table is truncated, `is_populated` is reset to `false`, and
a full refresh runs. After completion `needs_reinit` is reset to `false` and
`is_populated` returns to `true`.

Trigger `needs_reinit` manually with:

```sql
UPDATE pgtrickle.pgt_stream_tables
SET needs_reinit = true
WHERE pgt_name = 'my_view';
-- Then trigger a refresh:
SELECT pgtrickle.refresh_stream_table('my_view');
```

See also: [TROUBLESHOOTING.md](TROUBLESHOOTING.md), [FAQ.md](FAQ.md).

---

## Catalog Tables

### pgtrickle.pgt_stream_tables

Core metadata for each stream table.

| Column | Type | Description |
|---|---|---|
| `pgt_id` | `bigserial` | Primary key |
| `pgt_relid` | `oid` | OID of the storage table |
| `pgt_name` | `text` | Table name |
| `pgt_schema` | `text` | Schema name |
| `defining_query` | `text` | The SQL query that defines the ST |
| `original_query` | `text` | The user-supplied query before normalization |
| `schedule` | `text` | Refresh schedule (duration or cron expression) |
| `refresh_mode` | `text` | FULL, DIFFERENTIAL, or IMMEDIATE |
| `status` | `text` | INITIALIZING, ACTIVE, SUSPENDED, ERROR |
| `is_populated` | `bool` | Whether the table has been populated |
| `data_timestamp` | `timestamptz` | Timestamp of the data in the ST |
| `frontier` | `jsonb` | Per-source LSN positions (version tracking) |
| `last_refresh_at` | `timestamptz` | When last refreshed |
| `consecutive_errors` | `int` | Current error streak count |
| `needs_reinit` | `bool` | Whether upstream DDL requires reinitialization |
| `row_identity_version` | `smallint` | Composite row-identity framing version; legacy or `NULL` values block incremental maintenance |
| `auto_threshold` | `double precision` | Per-ST adaptive fallback threshold (overrides GUC) |
| `last_full_ms` | `double precision` | Last FULL refresh duration in milliseconds |
| `functions_used` | `text[]` | Function names used in the defining query (for DDL tracking) |
| `topk_limit` | `int` | LIMIT value for TopK stream tables (`NULL` if not TopK) |
| `topk_order_by` | `text` | ORDER BY clause SQL for TopK stream tables |
| `topk_offset` | `int` | OFFSET value for paged TopK queries (`NULL` if not paged) |
| `diamond_consistency` | `text` | Diamond consistency mode: `none` or `atomic` |
| `diamond_schedule_policy` | `text` | Diamond schedule policy: `fastest` or `slowest` |
| `has_keyless_source` | `bool` | Whether any source table lacks a PRIMARY KEY (EC-06) |
| `function_hashes` | `text` | MD5 hashes of referenced function bodies for change detection (EC-16) |
| `scc_id` | `int` | SCC group identifier for circular dependencies (`NULL` if not in a cycle) |
| `last_fixpoint_iterations` | `int` | Number of iterations in the last SCC fixpoint convergence (`NULL` if never iterated) |
| `created_at` | `timestamptz` | Creation timestamp |
| `updated_at` | `timestamptz` | Last modification timestamp |

### pgtrickle.pgt_dependencies

DAG edges — records which source tables each ST depends on, including CDC mode metadata.

| Column | Type | Description |
|---|---|---|
| `pgt_id` | `bigint` | FK to pgt_stream_tables |
| `source_relid` | `oid` | OID of the source table |
| `source_type` | `text` | TABLE, STREAM_TABLE, VIEW, MATVIEW, or FOREIGN_TABLE |
| `columns_used` | `text[]` | Which columns are referenced |
| `column_snapshot` | `jsonb` | Snapshot of source column metadata at creation time |
| `schema_fingerprint` | `text` | SHA-256 fingerprint of column snapshot for fast equality checks |
| `cdc_mode` | `text` | Current CDC mode: TRIGGER, TRANSITIONING, or WAL |
| `slot_name` | `text` | Replication slot name (WAL/TRANSITIONING modes) |
| `decoder_confirmed_lsn` | `pg_lsn` | WAL decoder's last confirmed position |
| `transition_started_at` | `timestamptz` | When the trigger→WAL transition started |

### pgtrickle.pgt_refresh_history

Audit log of all refresh operations.

| Column | Type | Description |
|---|---|---|
| `refresh_id` | `bigserial` | Primary key |
| `pgt_id` | `bigint` | FK to pgt_stream_tables |
| `data_timestamp` | `timestamptz` | Data timestamp of the refresh |
| `start_time` | `timestamptz` | When the refresh started |
| `end_time` | `timestamptz` | When it completed |
| `action` | `text` | NO_DATA, FULL, DIFFERENTIAL, REINITIALIZE, SKIP |
| `rows_inserted` | `bigint` | Rows inserted |
| `rows_updated` | `bigint` | Rows updated; TopK and fused MERGE paths use `merge_action()` accounting |
| `rows_deleted` | `bigint` | Rows deleted |
| `delta_row_count` | `bigint` | Number of delta rows processed from change buffers |
| `merge_strategy_used` | `text` | Which merge strategy was used (e.g. MERGE, DELETE+INSERT) |
| `was_full_fallback` | `bool` | Whether the refresh fell back to FULL from DIFFERENTIAL |
| `error_message` | `text` | Error message if failed |
| `status` | `text` | RUNNING, COMPLETED, FAILED, SKIPPED |
| `initiated_by` | `text` | What triggered: SCHEDULER, MANUAL, or INITIAL |
| `freshness_deadline` | `timestamptz` | SLA deadline (duration schedules only; NULL for cron) |
| `fixpoint_iteration` | `int` | Iteration of the fixed-point loop (`NULL` for non-cyclic refreshes) |

### pgtrickle.pgt_change_tracking

CDC slot tracking per source table.

| Column | Type | Description |
|---|---|---|
| `source_relid` | `oid` | OID of the tracked source table |
| `slot_name` | `text` | Logical replication slot name |
| `last_consumed_lsn` | `pg_lsn` | Last consumed WAL position |
| `tracked_by_pgt_ids` | `bigint[]` | Array of ST IDs depending on this source |

### pgtrickle.pgt_source_gates

Bootstrap source gate registry. One row per source table that has ever been
gated. Only sources with `gated = true` are actively blocking scheduler
refreshes.

| Column | Type | Description |
|---|---|---|
| `source_relid` | `oid` | OID of the gated source table (PK) |
| `gated` | `boolean` | `true` while the source is gated; `false` after `ungate_source()` |
| `gated_at` | `timestamptz` | When the gate was most recently set |
| `ungated_at` | `timestamptz` | When the gate was cleared (`NULL` if still active) |
| `gated_by` | `text` | Actor that set the gate (e.g. `'gate_source'`) |

### pgtrickle.pgt_refresh_groups

User-declared Cross-Source Snapshot Consistency groups. A refresh
group guarantees that all member stream tables are refreshed against a snapshot
taken at the same point in time, preventing partial-update visibility (e.g.
`orders` and `order_lines` both reflecting the same transaction boundary).

| Column | Type | Description |
|---|---|---|
| `group_id` | `serial` | Primary key |
| `group_name` | `text` | Unique human-readable group name |
| `member_oids` | `oid[]` | OIDs of the stream table storage relations that participate in this group |
| `isolation` | `text` | Snapshot isolation level for the group: `'read_committed'` (default) or `'repeatable_read'` |
| `created_at` | `timestamptz` | When the group was created |

#### Management API

```sql
-- Create a refresh group
SELECT pgtrickle.create_refresh_group(
    'orders_snapshot',
    ARRAY['public.orders_summary', 'public.order_lines_summary'],
    'repeatable_read'   -- or 'read_committed' (default)
);

-- List all groups:
SELECT * FROM pgtrickle.refresh_groups();

-- Remove a group:
SELECT pgtrickle.drop_refresh_group('orders_snapshot');
```

**Validation rules:**
- At least 2 member stream tables are required.
- All members must exist in `pgt_stream_tables`.
- No member can appear in more than one refresh group.
- Valid isolation levels: `'read_committed'` (default), `'repeatable_read'`.

---

## Bootstrap Source Gating

These functions let operators pause and resume scheduler-driven refreshes for
individual source tables — useful during large bulk loads or ETL windows.

### pgtrickle.gate_source(source TEXT)

Mark a source table as gated. The scheduler will skip any stream table that
reads from this source until `ungate_source()` is called.

```sql
SELECT pgtrickle.gate_source('my_schema.big_source');
```

Manual `refresh_stream_table()` calls are **not** affected by gates.

### pgtrickle.ungate_source(source TEXT)

Clear a gate set by `gate_source()`. After this call the scheduler resumes
normal refresh scheduling for dependent stream tables.

```sql
SELECT pgtrickle.ungate_source('my_schema.big_source');
```

### pgtrickle.source_gates()

Table function returning the current gate status for all registered sources.

```sql
SELECT * FROM pgtrickle.source_gates();
-- source_table | schema_name | gated | gated_at | ungated_at | gated_by
```

| Column | Type | Description |
|---|---|---|
| `source_table` | `text` | Relation name |
| `schema_name` | `text` | Schema name |
| `gated` | `boolean` | Whether the source is currently gated |
| `gated_at` | `timestamptz` | When the gate was set |
| `ungated_at` | `timestamptz` | When the gate was cleared (`NULL` if active) |
| `gated_by` | `text` | Which function set the gate |

**Typical workflow**

```sql
-- 1. Gate the source before a bulk load.
SELECT pgtrickle.gate_source('orders');

-- 2. Load historical data (scheduler sits idle for orders-based STs).
COPY orders FROM '/data/historical_orders.csv';

-- 3. Ungate — the next scheduler tick refreshes everything cleanly.
SELECT pgtrickle.ungate_source('orders');
```

### pgtrickle.bootstrap_gate_status()

Rich introspection of bootstrap gate lifecycle.  Returns the same columns as
`source_gates()` plus computed fields for debugging.

```sql
SELECT * FROM pgtrickle.bootstrap_gate_status();
-- source_table | schema_name | gated | gated_at | ungated_at | gated_by | gate_duration | affected_stream_tables
```

| Column | Type | Description |
|---|---|---|
| `source_table` | `text` | Relation name |
| `schema_name` | `text` | Schema name |
| `gated` | `boolean` | Whether the source is currently gated |
| `gated_at` | `timestamptz` | When the gate was set (updated on re-gate) |
| `ungated_at` | `timestamptz` | When the gate was cleared (`NULL` if active) |
| `gated_by` | `text` | Which function set the gate |
| `gate_duration` | `interval` | How long the gate has been active (gated: `now() - gated_at`; ungated: `ungated_at - gated_at`) |
| `affected_stream_tables` | `text` | Comma-separated list of stream tables whose scheduler refreshes are blocked by this gate |

Rows are sorted with currently-gated sources first, then alphabetically.

## ETL Coordination Cookbook

Step-by-step recipes for common bulk-load patterns using source gating.

### Recipe 1 — Single Source Bulk Load

Gate one source table during a large data import.  The scheduler pauses
refreshes for all stream tables that depend on this source.

```sql
-- 1. Gate the source before loading.
SELECT pgtrickle.gate_source('orders');

-- 2. Load the data.  The scheduler sits idle for orders-dependent STs.
COPY orders FROM '/data/orders_2026.csv' WITH (FORMAT csv, HEADER);

-- 3. Ungate.  On the next tick the scheduler refreshes everything cleanly.
SELECT pgtrickle.ungate_source('orders');
```

### Recipe 2 — Coordinated Multi-Source Load

When multiple sources feed into a shared downstream stream table,
gate them all before loading so no intermediate refreshes occur.

```sql
-- 1. Gate all sources that will be loaded.
SELECT pgtrickle.gate_source('orders');
SELECT pgtrickle.gate_source('order_lines');

-- 2. Load each source (can be parallel, any order).
COPY orders FROM '/data/orders.csv' WITH (FORMAT csv, HEADER);
COPY order_lines FROM '/data/lines.csv' WITH (FORMAT csv, HEADER);

-- 3. Ungate all sources.  The scheduler refreshes downstream STs once.
SELECT pgtrickle.ungate_source('orders');
SELECT pgtrickle.ungate_source('order_lines');
```

### Recipe 3 — Gate + Deferred Initialization

Combine gating with `initialize => false` to prevent incomplete initial
population when sources are loaded asynchronously.

```sql
-- 1. Gate sources before creating any stream tables.
SELECT pgtrickle.gate_source('orders');
SELECT pgtrickle.gate_source('order_lines');

-- 2. Create stream tables without initial population.
SELECT pgtrickle.create_stream_table(
    'order_summary',
    'SELECT region, SUM(amount) FROM orders GROUP BY region',
    '1m', initialize => false
);
SELECT pgtrickle.create_stream_table(
    'order_report',
    'SELECT s.region, s.total, l.line_count
     FROM order_summary s
     JOIN (SELECT region, COUNT(*) AS line_count FROM order_lines GROUP BY region) l
       USING (region)',
    '1m', initialize => false
);

-- 3. Run ETL processes (can be in separate transactions).
BEGIN;
  COPY orders FROM 's3://warehouse/orders.parquet';
  SELECT pgtrickle.ungate_source('orders');
COMMIT;

BEGIN;
  COPY order_lines FROM 's3://warehouse/lines.parquet';
  SELECT pgtrickle.ungate_source('order_lines');
COMMIT;

-- 4. Once all sources are ungated, the scheduler initializes and refreshes
--    all stream tables in dependency order.
```

### Recipe 4 — Nightly Batch Pattern

For scheduled ETL that runs overnight, gate sources before the batch starts
and ungate after the batch completes.

```sql
-- Nightly ETL script:

-- Gate all sources that will be refreshed.
SELECT pgtrickle.gate_source('sales');
SELECT pgtrickle.gate_source('inventory');

-- Truncate and reload (or use COPY, INSERT...SELECT, etc.).
TRUNCATE sales;
COPY sales FROM '/data/nightly/sales.csv' WITH (FORMAT csv, HEADER);

TRUNCATE inventory;
COPY inventory FROM '/data/nightly/inventory.csv' WITH (FORMAT csv, HEADER);

-- All data loaded — ungate and let the scheduler handle the rest.
SELECT pgtrickle.ungate_source('sales');
SELECT pgtrickle.ungate_source('inventory');

-- Verify: check the gate status to confirm everything is ungated.
SELECT * FROM pgtrickle.bootstrap_gate_status();
```

### Recipe 5 — Monitoring During a Gated Load

Use `bootstrap_gate_status()` to monitor progress when streams appear stalled.

```sql
-- Check which sources are currently gated and how long they've been paused.
SELECT source_table, gate_duration, affected_stream_tables
FROM pgtrickle.bootstrap_gate_status()
WHERE gated = true;

-- If a gate has been active too long (e.g. ETL failed), ungate manually.
SELECT pgtrickle.ungate_source('stale_source');
```

---

## Watermark Gating

Watermark gating is a scheduling control for ETL pipelines where multiple
source tables are populated by separate jobs that finish at different times.
Each ETL job declares "I'm done up to timestamp X", and the scheduler waits
until all sources in a group are caught up within a configurable tolerance
before refreshing downstream stream tables.

### Catalog Tables

#### pgtrickle.pgt_watermarks

Per-source watermark state. One row per source table that has had a watermark
advanced.

| Column | Type | Description |
|--------|------|-------------|
| `source_relid` | `oid` | Source table OID (primary key) |
| `watermark` | `timestamptz` | Current watermark value |
| `updated_at` | `timestamptz` | When the watermark was last advanced |
| `advanced_by` | `text` | User/role that advanced the watermark |
| `wal_lsn_at_advance` | `text` | WAL LSN at the time of advancement |

#### pgtrickle.pgt_watermark_groups

Watermark group definitions. Each group declares that a set of sources must
be temporally aligned.

| Column | Type | Description |
|--------|------|-------------|
| `group_id` | `serial` | Auto-generated group ID (primary key) |
| `group_name` | `text` | Unique group name |
| `source_relids` | `oid[]` | Array of source table OIDs in the group |
| `tolerance_secs` | `float8` | Maximum allowed lag in seconds (default 0) |
| `created_at` | `timestamptz` | When the group was created |

#### pgtrickle.pgt_template_cache

Cross-backend delta SQL template cache (UNLOGGED). Stores
compiled delta query templates so new backends skip the ~45 ms DVM
parse+differentiate step. Managed automatically — no user interaction required.

| Column | Type | Description |
|--------|------|-------------|
| `pgt_id` | `bigint` | Stream table ID (PK, FK → pgt_stream_tables) |
| `query_hash` | `bigint` | Hash of the defining query (staleness detection) |
| `delta_sql` | `text` | Delta SQL template with LSN placeholder tokens |
| `columns` | `text[]` | Output column names |
| `source_oids` | `integer[]` | Source table OIDs |
| `is_dedup` | `boolean` | Whether the delta is deduplicated per row ID |
| `key_changed` | `boolean` | Whether `__pgt_key_changed` column is present |
| `all_algebraic` | `boolean` | Whether all aggregates are algebraically invertible |
| `cached_at` | `timestamptz` | When the entry was last populated |

### Functions

#### pgtrickle.advance_watermark(source TEXT, watermark TIMESTAMPTZ)

Signal that a source table's data is complete through the given timestamp.

- **Monotonic:** rejects watermarks that go backward (raises error).
- **Idempotent:** advancing to the same value is a silent no-op.
- **Transactional:** the watermark is part of the caller's transaction.

```sql
SELECT pgtrickle.advance_watermark('orders', '2026-03-01 12:05:00+00');
```

#### pgtrickle.create_watermark_group(group_name TEXT, sources TEXT[], tolerance_secs FLOAT8 DEFAULT 0)

Create a watermark group. Requires at least 2 sources.

- `tolerance_secs`: maximum allowed lag between the most-advanced and
  least-advanced watermarks. Default `0` means strict alignment.

```sql
SELECT pgtrickle.create_watermark_group(
    'order_pipeline',
    ARRAY['orders', 'order_lines'],
    0    -- strict alignment (default)
);
```

#### pgtrickle.drop_watermark_group(group_name TEXT)

Remove a watermark group by name.

```sql
SELECT pgtrickle.drop_watermark_group('order_pipeline');
```

#### pgtrickle.watermarks()

Return the current watermark state for all registered sources.

```sql
SELECT * FROM pgtrickle.watermarks();
```

| Column | Type | Description |
|--------|------|-------------|
| `source_table` | `text` | Source table name |
| `schema_name` | `text` | Schema name |
| `watermark` | `timestamptz` | Current watermark value |
| `updated_at` | `timestamptz` | Last advancement time |
| `advanced_by` | `text` | User that advanced it |
| `wal_lsn` | `text` | WAL LSN at advancement |

#### pgtrickle.watermark_groups()

Return all watermark group definitions.

```sql
SELECT * FROM pgtrickle.watermark_groups();
```

#### pgtrickle.watermark_status()

Return live alignment status for each watermark group.

```sql
SELECT * FROM pgtrickle.watermark_status();
```

| Column | Type | Description |
|--------|------|-------------|
| `group_name` | `text` | Group name |
| `min_watermark` | `timestamptz` | Least-advanced watermark |
| `max_watermark` | `timestamptz` | Most-advanced watermark |
| `lag_secs` | `float8` | Lag in seconds between max and min |
| `aligned` | `boolean` | Whether lag is within tolerance |
| `sources_with_watermark` | `int4` | Number of sources that have a watermark |
| `sources_total` | `int4` | Total sources in the group |

### Recipes

#### Recipe 6 — Nightly ETL with Watermarks

```sql
-- Create a watermark group for the order pipeline.
SELECT pgtrickle.create_watermark_group(
    'order_pipeline',
    ARRAY['orders', 'order_lines']
);

-- Nightly ETL job 1: Load orders
BEGIN;
  COPY orders FROM '/data/orders_20260301.csv';
  SELECT pgtrickle.advance_watermark('orders', '2026-03-01');
COMMIT;

-- Nightly ETL job 2: Load order lines (may run later)
BEGIN;
  COPY order_lines FROM '/data/lines_20260301.csv';
  SELECT pgtrickle.advance_watermark('order_lines', '2026-03-01');
COMMIT;

-- order_report refreshes on the next tick after both watermarks align.
```

#### Recipe 7 — Micro-Batch Tolerance

```sql
-- Allow up to 30 seconds of skew between trades and quotes.
SELECT pgtrickle.create_watermark_group(
    'realtime_pipeline',
    ARRAY['trades', 'quotes'],
    30   -- 30-second tolerance
);

-- External process advances watermarks every few seconds.
SELECT pgtrickle.advance_watermark('trades', '2026-03-01 12:00:05+00');
SELECT pgtrickle.advance_watermark('quotes', '2026-03-01 12:00:02+00');
-- Lag is 3s, within 30s tolerance → stream tables refresh normally.
```

#### Recipe 8 — Monitoring Watermark Alignment

```sql
-- Check which groups are currently misaligned.
SELECT group_name, lag_secs, aligned
FROM pgtrickle.watermark_status()
WHERE NOT aligned;

-- Check individual source watermarks.
SELECT source_table, watermark, updated_at
FROM pgtrickle.watermarks()
ORDER BY watermark;
```

### Stuck Watermark Detection (WM-7)

When `pg_trickle.watermark_holdback_timeout` is set to a positive value
(seconds), the scheduler periodically checks all watermark sources. If any
source in a watermark group has not been advanced within the timeout,
downstream stream tables in that group are **paused** (refresh is skipped)
and a `pgtrickle_alert` NOTIFY is emitted.

This protects against silent data staleness when an ETL pipeline breaks and
stops advancing watermarks -- without this guard, stream tables would
continue refreshing with stale external data.

**Behavior:**

- **Stuck detection**: Every ~60 seconds, the scheduler checks
  `updated_at` for all watermark sources. If `now() - updated_at >
  watermark_holdback_timeout`, the source is stuck.
- **Pause**: Any stream table whose source set overlaps a group containing
  a stuck source is skipped. A SKIP record with `"stuck"` in the reason
  is logged to `pgt_refresh_history`.
- **Alert**: A `pgtrickle_alert` NOTIFY with event `watermark_stuck` is
  emitted (once per newly-stuck source, not repeated every check cycle).
- **Auto-resume**: When the stuck watermark is advanced via
  `advance_watermark()`, the next scheduler check detects the advancement,
  lifts the pause, and emits a `watermark_resumed` event.

#### Recipe 9 — Stuck Watermark Protection

```sql
-- Enable stuck-watermark detection with a 10-minute timeout.
ALTER SYSTEM SET pg_trickle.watermark_holdback_timeout = 600;
SELECT pg_reload_conf();

-- Listen for alerts in a monitoring process.
LISTEN pgtrickle_alert;

-- When the ETL pipeline breaks and stops calling advance_watermark(),
-- the scheduler will start skipping downstream STs after 10 minutes.
-- You'll receive a NOTIFY payload like:
--   {"event":"watermark_stuck","group":"order_pipeline","source_oid":16385,"age_secs":620}

-- When the ETL pipeline recovers and advances the watermark:
SELECT pgtrickle.advance_watermark('orders', '2026-03-02 00:00:00+00');
-- The scheduler automatically resumes, and you'll receive:
--   {"event":"watermark_resumed","source_oid":16385}
```

---

## Developer Diagnostics

Four SQL-callable introspection functions that surface internal DVM state
without side-effects. All functions are read-only — they never modify catalog
tables or trigger refreshes.

### `pgtrickle.explain_query_rewrite(query TEXT)`

Walk a query through the full DVM rewrite pipeline and report each pass.

Returns one row per rewrite pass. When a pass changes the query, `changed = true`
and `sql_after` contains the SQL after the transformation. Two synthetic rows
are appended: `topk_detection` (detects `ORDER BY … LIMIT`) and `dvm_patterns`
(lists detected DVM constructs such as aggregation strategy, join types, and
volatility).

```sql
SELECT pass_name, changed, sql_after
FROM pgtrickle.explain_query_rewrite(
  'SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id'
);
```

**Return columns:**

| Column | Type | Description |
|--------|------|-------------|
| `pass_name` | `text` | Rewrite pass name (e.g. `view_inlining`, `distinct_on`, `grouping_sets`) |
| `changed` | `bool` | Whether this pass modified the query |
| `sql_after` | `text` | SQL text after this pass (NULL if unchanged) |

**Rewrite passes (in order):**

| Pass | Description |
|------|-------------|
| `view_inlining` | Expand view references to their defining SQL |
| `nested_window_lift` | Lift window functions out of expressions (e.g. `CASE WHEN ROW_NUMBER() OVER (...) ...`) |
| `distinct_on` | Rewrite `DISTINCT ON` to a `ROW_NUMBER()` window |
| `grouping_sets` | Expand `GROUPING SETS / CUBE / ROLLUP` to `UNION ALL` of `GROUP BY` |
| `scalar_subquery_in_where` | Rewrite scalar subqueries in `WHERE` to `CROSS JOIN` |
| `correlated_scalar_in_select` | Rewrite correlated scalar subqueries in `SELECT` to `LEFT JOIN` |
| `sublinks_in_or_demorgan` | Apply De Morgan normalization and expand `SubLinks` inside `OR` |
| `rows_from` | Rewrite `ROWS FROM()` multi-function expressions |
| `topk_detection` | Detect `ORDER BY … LIMIT n` TopK pattern |
| `dvm_patterns` | Detected DVM constructs: join types, aggregate strategies, volatility |

---

### `pgtrickle.diagnose_errors(name TEXT)`

Return the last 5 FAILED refresh events for a stream table, with each error
classified by type and supplied with a remediation hint.

```sql
SELECT event_time, error_type, error_message, remediation
FROM pgtrickle.diagnose_errors('my_stream_table');
```

**Return columns:**

| Column | Type | Description |
|--------|------|-------------|
| `event_time` | `timestamptz` | When the failed refresh started |
| `error_type` | `text` | Classification: `user`, `schema`, `correctness`, `performance`, `infrastructure` |
| `error_message` | `text` | Raw error text from `pgt_refresh_history` |
| `remediation` | `text` | Suggested next step |

**Error types:**

| Type | Trigger patterns | Typical action |
|------|-----------------|----------------|
| `user` | `query parse error`, `unsupported operator`, `type mismatch` | Check query; run `validate_query()` |
| `schema` | `upstream table schema changed`, `upstream table dropped` | Reinitialize; check `pgt_dependencies` |
| `correctness` | `phantom`, `EXCEPT ALL`, `row count mismatch` | Switch to `refresh_mode='FULL'`; report bug |
| `performance` | `lock timeout`, `deadlock`, `serialization failure`, `spill` | Tune `lock_timeout`; enable `buffer_partitioning` |
| `infrastructure` | `permission denied`, `SPI error`, `replication slot` | Check role grants; verify slot config |

---

### `pgtrickle.list_auxiliary_columns(name TEXT)`

List all `__pgt_*` internal columns on a stream table's storage relation,
with an explanation of each column's role.

These columns are normally hidden from `SELECT *` output. This function
surfaces them for debugging and operator visibility.

```sql
SELECT column_name, data_type, purpose
FROM pgtrickle.list_auxiliary_columns('my_stream_table');
```

**Return columns:**

| Column | Type | Description |
|--------|------|-------------|
| `column_name` | `text` | Internal column name (e.g. `__pgt_row_id`) |
| `data_type` | `text` | PostgreSQL type (e.g. `bigint`, `text`) |
| `purpose` | `text` | Human-readable description of the column's role |

**Common auxiliary columns:**

| Column | Purpose |
|--------|---------|
| `__pgt_row_id` | Row identity hash — MERGE join key for delta application |
| `__pgt_count` | Multiplicity counter for DISTINCT / aggregation / UNION dedup |
| `__pgt_count_l` | Left-side multiplicity for INTERSECT / EXCEPT |
| `__pgt_count_r` | Right-side multiplicity for INTERSECT / EXCEPT |
| `__pgt_aux_sum_<col>` | Running SUM for algebraic AVG maintenance |
| `__pgt_aux_count_<col>` | Running COUNT for algebraic AVG maintenance |
| `__pgt_aux_sum2_<col>` | Sum-of-squares for STDDEV / VAR maintenance |
| `__pgt_aux_sum{x,y,xy,x2,y2}_<col>` | Five-column set for CORR / COVAR / REGR_* |
| `__pgt_aux_nonnull_<col>` | Non-null count for SUM-above-FULL-JOIN maintenance |

---

### `pgtrickle.validate_query(query TEXT)`

Parse and validate a query through the DVM pipeline without creating a stream
table. Returns detected SQL constructs, warnings, and the resolved refresh mode.

```sql
SELECT check_name, result, severity
FROM pgtrickle.validate_query(
  'SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id'
);
```

**Return columns:**

| Column | Type | Description |
|--------|------|-------------|
| `check_name` | `text` | Name of the check or detected construct |
| `result` | `text` | Resolved value or construct description |
| `severity` | `text` | `INFO`, `WARNING`, or `ERROR` |

The first row always has `check_name = 'resolved_refresh_mode'` with the mode
that would be assigned under `refresh_mode = 'AUTO'`: `DIFFERENTIAL`, `FULL`,
or `TOPK`.

**Common check names:**

| Check | Description |
|-------|-------------|
| `resolved_refresh_mode` | `DIFFERENTIAL`, `FULL`, or `TOPK` |
| `topk_pattern` | Detected LIMIT + ORDER BY values |
| `unsupported_construct` | Feature not supported for DIFFERENTIAL mode (→ WARNING) |
| `matview_or_foreign_table` | Query references matview/foreign table (→ WARNING, FULL) |
| `ivm_support_check` | DVM parse result (→ WARNING if DIFFERENTIAL not possible) |
| `aggregate` | Aggregate with strategy: `ALGEBRAIC_INVERTIBLE`, `ALGEBRAIC_VIA_AUX`, `SEMI_ALGEBRAIC`, or `GROUP_RESCAN` |
| `join` | Detected join type: `INNER`, `LEFT_OUTER`, `FULL_OUTER`, `SEMI`, `ANTI` |
| `set_op` | Set operation: `DISTINCT`, `UNION_ALL`, `INTERSECT`, `EXCEPT`, `EXCEPT_ALL` |
| `window_function` | Query contains window functions |
| `scalar_subquery` | Query contains scalar subqueries |
| `lateral` | Query contains LATERAL functions or subqueries |
| `recursive_cte` | Query uses `WITH RECURSIVE` |
| `volatility` | Worst-case volatility of functions used: `immutable`, `stable`, `volatile` |
| `needs_pgt_count` | Multiplicity counter column will be added |
| `needs_dual_count` | Left/right multiplicity counters will be added |
| `parse_warning` | Advisory warning from the DVM parse phase |

**Example output for a GROUP_RESCAN query:**

```sql
SELECT check_name, result, severity
FROM pgtrickle.validate_query(
  'SELECT grp, STRING_AGG(tag, '','') FROM events GROUP BY grp'
);
```

| check_name | result | severity |
|---|---|---|
| `resolved_refresh_mode` | `DIFFERENTIAL` | `INFO` |
| `aggregate` | `STRING_AGG(GROUP_RESCAN)` | `WARNING` |
| `needs_pgt_count` | `true — multiplicity counter column required` | `INFO` |
| `volatility` | `immutable` | `INFO` |

> **Note on GROUP_RESCAN:** `STRING_AGG`, `ARRAY_AGG`, `BOOL_AND`, and other
> non-algebraic aggregates use a group-rescan strategy — any change in a group
> triggers full re-aggregation from the source data for that group. This is
> still DIFFERENTIAL (only changed groups are rescanned), but has higher
> per-group cost than algebraic strategies. If this is performance-sensitive,
> consider pre-aggregating with a simpler aggregate and post-processing.

---

## Advanced and Diagnostic Functions

The following functions are useful for operational troubleshooting, query
validation, and introspection. They are not needed for day-to-day streaming
table management.

| Function | Returns | Purpose |
|----------|---------|---------|
| `pgtrickle.preflight()` | `text` (JSON) | Pre-deployment health check — returns one JSON object per check with `pass`, `check`, `detail`. |
| `pgtrickle.lifecycle_preflight()` | `text` (JSON) | Superuser-only, read-only owner-privilege preflight for upgrades; lists every missing source `SELECT` or source-schema `USAGE` grant with exact remediation SQL. |
| `pgtrickle.row_identity_v2_recreation_preflight()` | `jsonb` | Owner/superuser-only, read-only V2 recreation gate. Checks storage, markers, source contracts, indexes, PostgreSQL major, scheduler quiescence, and consumer acknowledgments without returning identity bytes. |
| `pgtrickle.row_identity_v2_consumer_inventory()` | `SETOF record` | Owner/superuser-only inventory of external consumers and their `BYTEA` schema/resnapshot plans. |
| `pgtrickle.validate_query(sql text)` | `SetOf row` | Validates whether a SQL query is IVM-compatible. Returns analysis results including unsupported patterns. |
| `pgtrickle.explain_stream_table(st_name text)` | `text` | Shows the effective refresh mode, delta plan, fallback reasons, and current state flags for a stream table. |
| `pgtrickle.explain_dag()` | `text` (DOT) | Returns a Graphviz DOT representation of the full dependency graph. |
| `pgtrickle.stream_table_lineage(st_name text)` | `SetOf row` | Returns the lineage graph for a single stream table — direct and transitive source tables with metadata. |
| `pgtrickle.cdc_pause_status()` | `SetOf row` | Shows whether CDC is paused, the capture mode (`discard` / `hold`), and a human-readable explanation. |
| `pgtrickle.cluster_worker_summary()` | `SetOf row` | Shows active background workers across all pg_trickle-enabled databases (requires `pg_monitor`). |
| `pgtrickle.drain()` | — | Initiates a graceful drain: the scheduler finishes in-flight refreshes then stops. Useful before `pg_upgrade` or a rolling restart. |
| `pgtrickle.resume_after_drain()` | `bool` | Explicitly re-enables scheduler dispatch after a persistent drain request. |
| `pgtrickle.is_drained()` | `bool` | Returns `true` when all scheduler workers have completed their current cycle and are waiting. |
| `pgtrickle.st_refresh_stats()` | `SetOf row` | Per-stream-table refresh metrics: counts, durations, error rates. Primary monitoring function. |
| `pgtrickle.pgtrickle_refresh_stats()` | `SetOf row` | Cluster-wide aggregate refresh statistics. |
| `pgtrickle.recommend_refresh_mode(st_name text)` | `jsonb` | Returns a recommendation (`DIFFERENTIAL` / `FULL`) for a specific stream table based on current change rate. |
| `pgtrickle.recommend_schedule(st_name text)` | `jsonb` | Returns a schedule recommendation with confidence score and reasoning. |
| `pgtrickle.schedule_recommendations()` | `SetOf row` | Bulk version of `recommend_schedule()` — one row per registered stream table. |
| `pgtrickle.setup_self_monitoring()` | — | Creates the five self-monitoring stream tables that track pg_trickle's own performance. |

`pgtrickle.preflight()` includes a hard `row_identity` check. A missing or
legacy `row_identity_version` on a stream table or required CDC buffer means
incremental maintenance is not considered safe until a protected rebuild has
completed.

### pgtrickle.lifecycle_preflight

Run the v0.87.10 lifecycle-upgrade privilege check without changing catalog
state. The function must be called by a superuser and returns one JSON object.
`owner_privileges.missing` is empty when every stream-table owner can read each
declared source and use its schema.

```sql
SELECT pgtrickle.lifecycle_preflight();
```

The `remediation` value in each reported gap contains the exact quoted
`GRANT SELECT ON TABLE ...` and/or `GRANT USAGE ON SCHEMA ...` statements.
Apply those grants and rerun the preflight before `ALTER EXTENSION pg_trickle
UPDATE`.

### pgtrickle.row_identity_v2_recreation_preflight

Run the v0.87.17 recreation gate as the extension owner or a superuser. It is
read-only and returns `jsonb` with `ok`, named checks, and the ordered
recreation instructions. It fails until the scheduler is quiesced and every
recorded external consumer has acknowledged its `BYTEA` schema change and
resnapshot plan.

```sql
ALTER SYSTEM SET pg_trickle.enabled = 'off';
SELECT pg_reload_conf();
SELECT pgtrickle.row_identity_v2_recreation_preflight();
```

The report contains no complete row identities. It reports only operational
metadata such as counts and the largest managed row-ID index size.

### pgtrickle.row_identity_v2_consumer_inventory

Record the inventory before acknowledging the preflight. Consumer names,
owners, affected stream tables, required schema changes, and resnapshot status
are private to the extension owner and superusers.

```sql
SELECT pgtrickle.row_identity_v2_record_inventory('change ticket and window');
SELECT pgtrickle.row_identity_v2_register_consumer(
    'warehouse', 'analytics_owner', ARRAY['public.orders_st'],
    true, true, 'Change the identity column to BYTEA and resnapshot'
);
SELECT * FROM pgtrickle.row_identity_v2_consumer_inventory();
SELECT pgtrickle.row_identity_v2_acknowledge_consumer(1, 'PENDING');
SELECT pgtrickle.row_identity_v2_acknowledge_inventory();
```

Use `COMPLETE` after the downstream resnapshot or `SKIPPED` when the consumer
was removed. `PENDING` and `IN_PROGRESS` acknowledge ownership of the plan
before teardown; they do not claim that resnapshot is complete.

### pgtrickle.resume_after_drain

Explicitly re-enables scheduler dispatch after a persistent drain request.

```sql
pgtrickle.resume_after_drain() → bool
```

| `pgtrickle.self_monitoring_status()` | `SetOf row` | Shows whether each self-monitoring stream table exists, its status, and last refresh time. |
| `pgtrickle.teardown_self_monitoring()` | — | Drops all self-monitoring stream tables. Safe to call even if some are missing. |
| `pgtrickle.reliability_counters()` | `SetOf row` | Returns shared-memory reliability counters (scheduler errors, worker crashes, CDC pause events). Useful for alert dashboards. |
| `pgtrickle.wal_source_status()` | `SetOf row` | Per-source WAL CDC slot status: LSN lag, slot name, active/inactive state. Only populated when WAL-based CDC is active. |
| `pgtrickle.worker_allocation_status()` | `SetOf row` | Per-database dynamic background worker allocation: quota, active count, and DB name. |
| `pgtrickle.vector_status()` | `SetOf row` | One row per stream table that has a `post_refresh_action` configured or any ANN-relevant index. Shows index type, dimensions, and last rebuild time. |
| `pgtrickle.st_auto_threshold(st_name text)` | `float8` | Returns the effective `differential_max_change_ratio` for a specific stream table — the per-ST override if set, otherwise the global `pg_trickle.differential_max_change_ratio` GUC. |

See also:
- [Delta SQL Profiling](#delta-sql-profiling-v0130) — `explain_delta`, `dedup_stats`, `shared_buffer_stats`
- [Stream Table Lifecycle](#stream-table-lifecycle) — status values and transitions
- [GUC_CATALOG.md](GUC_CATALOG.md) — complete parameter reference

---

## Delta SQL Profiling

### `pgtrickle.explain_delta(st_name text, format text DEFAULT 'text')`

Generate the delta SQL query plan for a stream table without executing a refresh.

`explain_delta` produces the differential delta SQL that would be used on the
next DIFFERENTIAL refresh, then runs `EXPLAIN (ANALYZE false, FORMAT <format>)`
on it and returns the plan lines. This function is useful for:

- Identifying slow joins or missing indexes in auto-generated delta SQL.
- Comparing plan complexity between different query forms.
- Monitoring how the size of change buffers affects plan shape.

The delta SQL is generated against a hypothetical "scan all changes" window
(LSN `0/0 → FF/FFFFFFFF`) so the plan shows the full join/filter structure
even when the change buffer is currently empty.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `st_name` | `text` | Qualified stream table name (e.g. `'public.orders_summary'`). |
| `format` | `text` | Plan format: `'text'` (default), `'json'`, `'xml'`, or `'yaml'`. |

**Returns:** `SETOF text` — one row per plan line (text format) or one row containing the full JSON/XML/YAML plan.

**Example:**

```sql
-- Show the text plan for the delta query
SELECT line FROM pgtrickle.explain_delta('public.orders_summary');

-- Get the JSON plan for programmatic analysis
SELECT line FROM pgtrickle.explain_delta('public.orders_summary', 'json');
```

**Environment variable (`PGS_PROFILE_DELTA=1`):** When the environment variable
`PGS_PROFILE_DELTA=1` is set in the PostgreSQL server process, every
DIFFERENTIAL refresh automatically captures `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`
for the resolved delta SQL and writes the plan to
`/tmp/delta_plans/<schema>_<table>.json`. This is intended for E2E test
diagnostics and local profiling sessions.

---

### pgtrickle.explain_delta_plan

Return pg_trickle's engine-planning evidence as `jsonb`. The function parses
and plans the defining query but does not execute it, consume change rows,
advance a frontier, or populate execution caches.

The document has `format_version = 1`. It reports the statistics epoch,
statistics completeness, available observations, the current operator order,
the shadow candidate, the chosen order, rule decisions, skipped rules, and
planning time. Observed intermediate rows are the final delta cardinality from
the latest completed differential refresh; per-node and candidate runtime stay
absent until a benchmark executes that plan. A shadow rule has
`selected = false`; PostgreSQL still plans the generated relational SQL.

Only the stream-table owner or a superuser can inspect the plan.

```sql
SELECT jsonb_pretty(pgtrickle.explain_delta_plan(42));
```

Use `explain_delta_plan` to inspect pg_trickle's decision. Use
`pgtrickle.explain_delta` next when you need PostgreSQL's plan for the generated
delta SQL.

---

### `pgtrickle.dedup_stats()`

Show MERGE deduplication profiling counters accumulated since server start.

When the delta cannot be guaranteed to contain at most one row per
`__pgt_row_id` (e.g. for aggregate queries or keyless sources), the MERGE
must group and aggregate the delta before merging. This is tracked as
*dedup needed*. A consistently high ratio indicates that pre-MERGE compaction
in the change buffer would reduce refresh latency.

**Returns:** one row with:

| Column | Type | Description |
|--------|------|-------------|
| `total_diff_refreshes` | `bigint` | Total DIFFERENTIAL refreshes executed since server start that processed at least one change. Resets on server restart. |
| `dedup_needed` | `bigint` | Number of those refreshes where the delta required weight aggregation / deduplication in the MERGE USING clause. |
| `dedup_ratio_pct` | `float8` | `dedup_needed / total_diff_refreshes × 100`. 0 when `total_diff_refreshes = 0`. |

**Example:**

```sql
SELECT * FROM pgtrickle.dedup_stats();
-- total_diff_refreshes | dedup_needed | dedup_ratio_pct
-- ----------------------+--------------+-----------------
--                  1234 |           87 |            7.05
```

A `dedup_ratio_pct` ≥ 10 is the threshold recommended for investigating a
two-pass MERGE strategy. See `plans/performance/REPORT_OVERALL_STATUS.md §14`
for background.

### `pgtrickle.shared_buffer_stats()`

D-4 observability function. Returns one row per shared change buffer (one per
tracked source table), showing how many stream tables share the buffer, which
columns are tracked, the safe cleanup frontier, and the current buffer size.

**Return columns:**

| Column | Type | Description |
|--------|------|-------------|
| `source_oid` | `bigint` | PostgreSQL OID of the source table |
| `source_table` | `text` | Fully qualified source table name |
| `consumer_count` | `integer` | Number of stream tables sharing this buffer |
| `consumers` | `text` | Comma-separated list of consumer stream table names |
| `columns_tracked` | `integer` | Number of `new_*` columns in the buffer (column superset) |
| `safe_frontier_lsn` | `text` | MIN(frontier LSN) across all consumers — rows at or below this are safe to clean up |
| `buffer_rows` | `bigint` | Current number of rows in the change buffer |
| `is_partitioned` | `boolean` | Whether the buffer uses LSN-range partitioning |

**Example:**

```sql
SELECT * FROM pgtrickle.shared_buffer_stats();
-- source_oid | source_table       | consumer_count | consumers                          | columns_tracked | safe_frontier_lsn | buffer_rows | is_partitioned
-- -----------+--------------------+----------------+------------------------------------+-----------------+-------------------+-------------+----------------
--      16456 | public.orders      |              3 | public.orders_by_region, public... |               5 | 0/1A2B3C4D        |         142 | f
```

---

### `pgtrickle.explain_diff_sql(st_name text)`

Returns the raw differential (delta) SQL string that pg_trickle would execute
on the next DIFFERENTIAL refresh of `st_name`, without running it. Useful for
auditing generated SQL, spotting missing indexes, or understanding why a
particular query pattern is or is not differentiable.

Unlike `pgtrickle.explain_delta()` which returns a query *plan*, this function
returns the SQL text itself. The two complement each other: use
`explain_diff_sql` to read the generated query, then pass it to `EXPLAIN
ANALYZE` or `explain_delta` to profile it.

**Returns:** `text` — the differential SQL string (may be `NULL` if the stream
table is in FULL mode or has never been successfully differentiated).

**Example:**

```sql
SELECT pgtrickle.explain_diff_sql('public.revenue_by_region');
```

**See also:** [`pgtrickle.explain_delta()`](#pgtrickleexplain_deltast_name-text-format-text-default-text)

---

## UNLOGGED Change Buffers

### `pgtrickle.convert_buffers_to_unlogged()`

Converts all existing logged change buffer tables to `UNLOGGED`. This
eliminates WAL writes for trigger-inserted CDC rows, reducing WAL
amplification by ~30%.

**Returns:** `bigint` — the number of buffer tables converted.

```sql
SELECT pgtrickle.convert_buffers_to_unlogged();
-- convert_buffers_to_unlogged
-- ----------------------------
--                            5
```

> **Warning:** Each conversion acquires `ACCESS EXCLUSIVE` lock on the buffer
> table. Run this function during a low-traffic maintenance window to minimize
> lock contention.

> **After conversion:** Buffer contents will be lost on crash recovery. The
> scheduler automatically detects this and enqueues a FULL refresh for
> affected stream tables. See [`pg_trickle.unlogged_buffers`](CONFIGURATION.md#pg_trickleunlogged_buffers)
> for the full trade-off discussion.

---

## Refresh Mode Diagnostics

### `pgtrickle.recommend_refresh_mode(st_name TEXT DEFAULT NULL)`

Analyze stream table workload characteristics and recommend the optimal
refresh mode (FULL vs DIFFERENTIAL). When `st_name` is NULL, returns one
row per stream table. When provided, returns a single row for the named
stream table.

The function evaluates seven weighted signals — change ratio, empirical
timing, query complexity, target size, index coverage, and latency variance
— and computes a composite score. Scores above +0.15 recommend DIFFERENTIAL;
below −0.15 recommend FULL; in between, the function recommends KEEP
(current mode is near-optimal).

**Parameters:**

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `st_name` | `text` | `NULL` | Optional stream table name. NULL = all stream tables. |

**Return columns:**

| Column | Type | Description |
|--------|------|-------------|
| `pgt_schema` | `text` | Stream table schema |
| `pgt_name` | `text` | Stream table name |
| `current_mode` | `text` | Currently configured refresh mode |
| `effective_mode` | `text` | Mode actually used in the last refresh |
| `recommended_mode` | `text` | `DIFFERENTIAL`, `FULL`, or `KEEP` |
| `confidence` | `text` | `high`, `medium`, or `low` |
| `reason` | `text` | Human-readable explanation of the recommendation |
| `signals` | `jsonb` | Detailed signal breakdown with scores and weights |

**Example:**

```sql
-- Check all stream tables
SELECT pgt_name, current_mode, recommended_mode, confidence, reason
FROM pgtrickle.recommend_refresh_mode();

-- Check a specific stream table
SELECT recommended_mode, confidence, reason, signals
FROM pgtrickle.recommend_refresh_mode('public.orders_summary');
```

**Signal weights:**

| Signal | Base Weight | Description |
|--------|-------------|-------------|
| `change_ratio_current` | 0.25 | Current pending changes / source rows |
| `change_ratio_avg` | 0.30 | Historical average change ratio |
| `empirical_timing` | 0.35 | Observed DIFF vs FULL speed ratio |
| `query_complexity` | 0.10 | JOIN/aggregate/window count |
| `target_size` | 0.10 | Target relation + index size |
| `index_coverage` | 0.05 | Whether `__pgt_row_id` index exists |
| `latency_variance` | 0.05 | DIFF latency p95/p50 ratio |

---

### `pgtrickle.refresh_efficiency()`

Per-table refresh efficiency metrics. Returns operational statistics for
every stream table — useful for monitoring dashboards and Grafana alerts.

**Return columns:**

| Column | Type | Description |
|--------|------|-------------|
| `pgt_schema` | `text` | Stream table schema |
| `pgt_name` | `text` | Stream table name |
| `refresh_mode` | `text` | Current refresh mode |
| `total_refreshes` | `bigint` | Total completed refresh count |
| `diff_count` | `bigint` | DIFFERENTIAL refresh count |
| `full_count` | `bigint` | FULL refresh count |
| `avg_diff_ms` | `float8` | Average DIFFERENTIAL duration (ms) |
| `avg_full_ms` | `float8` | Average FULL duration (ms) |
| `avg_change_ratio` | `float8` | Average change ratio from history |
| `diff_speedup` | `text` | Speedup factor (e.g. `12.3x`) of FULL / DIFF timing |
| `last_refresh_at` | `text` | Timestamp of last data refresh |

**Example:**

```sql
SELECT pgt_name, refresh_mode, diff_count, full_count,
       avg_diff_ms, avg_full_ms, diff_speedup
FROM pgtrickle.refresh_efficiency()
ORDER BY total_refreshes DESC;
```

---

## Export API

### `pgtrickle.export_definition(st_name TEXT)`

Export a stream table's configuration as reproducible DDL. Returns a SQL
script containing `DROP STREAM TABLE IF EXISTS` followed by
`SELECT pgtrickle.create_stream_table(...)` with all configured options,
plus any `ALTER STREAM TABLE` calls for post-creation settings (tier,
fuse mode, etc.).

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `st_name` | `text` | Fully qualified or search-path-resolved stream table name. |

**Returns:** `text` — SQL script that recreates the stream table.

**Example:**

```sql
-- Export a single definition
SELECT pgtrickle.export_definition('public.orders_summary');

-- Export all definitions
SELECT pgtrickle.export_definition(pgt_schema || '.' || pgt_name)
FROM pgtrickle.pgt_stream_tables;
```

---

## dbt Integration

The `dbt-pgtrickle` package exposes two new `config(...)` keys added in
The `partition_by` and fuse circuit-breaker options are available. Use them directly
in any `stream_table` materialization model.

For full dbt documentation see `dbt-pgtrickle/README.md`.

---

### `partition_by` config

Partition the stream table's underlying storage table using PostgreSQL
`PARTITION BY RANGE`. Only applied at **creation time** — changing it after the
stream table exists has no effect (use `--full-refresh` to recreate).

```sql
-- models/marts/events_by_day.sql
{{ config(
    materialized='stream_table',
    schedule='1m',
    refresh_mode='DIFFERENTIAL',
    partition_by='event_day'
) }}

SELECT
    event_day,
    user_id,
    COUNT(*) AS event_count
FROM {{ source('raw', 'events') }}
GROUP BY event_day, user_id
```

pg_trickle creates a `PARTITION BY RANGE (event_day)` storage table with an
automatic default catch-all partition. Add named partitions via standard DDL:

```sql
CREATE TABLE analytics.events_by_day_2026
  PARTITION OF analytics.events_by_day
  FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
```

The `partition_by` value is stored in `pgtrickle.pgt_stream_tables.st_partition_key`
and visible via `pgtrickle.stream_tables_info`.

---

### `fuse` config

The fuse circuit breaker suspends differential refreshes when the incoming
change volume exceeds a threshold, preventing runaway refresh cycles during
bulk ingestion. Fuse parameters are applied via `alter_stream_table()` on
every `dbt run`; they are a **no-op if the values have not changed**.

```sql
-- models/marts/order_totals.sql
{{ config(
    materialized='stream_table',
    schedule='5m',
    refresh_mode='DIFFERENTIAL',
    fuse='auto',
    fuse_ceiling=50000,
    fuse_sensitivity=3
) }}

SELECT customer_id, SUM(amount) AS total
FROM {{ source('raw', 'orders') }}
GROUP BY customer_id
```

| Config key | Type | Default | Description |
|-----------|------|---------|-------------|
| `fuse` | `'off'`\|`'on'`\|`'auto'` | `null` (no-op) | Fuse mode. `'auto'` activates only when FULL refresh would be cheaper than DIFFERENTIAL. |
| `fuse_ceiling` | integer | `null` | Change-count threshold (number of changed rows) that triggers the fuse. `null` uses the global `pg_trickle.fuse_default_ceiling` GUC. |
| `fuse_sensitivity` | integer | `null` | Number of consecutive over-ceiling observations required before the fuse blows. `null` means 1 (blow immediately). |

Monitor fuse state via `pgtrickle.dedup_stats()` or check
`pgtrickle.pgt_stream_tables.fuse_state` directly:

```sql
SELECT pgt_name, fuse_mode, fuse_state, fuse_ceiling, fuse_sensitivity
FROM pgtrickle.pgt_stream_tables
WHERE fuse_mode != 'off';
```

---

## Self Monitoring — Self-Monitoring

pg_trickle can monitor itself using its own stream tables. Five *self-monitoring*
stream tables maintain reactive analytics over the internal catalog, replacing
repeated full-scan diagnostic queries with continuously-maintained incremental
views.

### Quick Start

```sql
-- Create all five self-monitoring stream tables (idempotent).
SELECT pgtrickle.setup_self_monitoring();

-- Check status.
SELECT * FROM pgtrickle.self_monitoring_status();

-- View threshold recommendations (after 10+ refresh cycles).
SELECT * FROM pgtrickle.df_threshold_advice
WHERE confidence IN ('HIGH', 'MEDIUM');

-- View anomalies.
SELECT * FROM pgtrickle.df_anomaly_signals
WHERE duration_anomaly IS NOT NULL;

-- Enable auto-apply (optional).
SET pg_trickle.self_monitoring_auto_apply = 'threshold_only';

-- Clean up.
SELECT pgtrickle.teardown_self_monitoring();
```

### `pgtrickle.setup_self_monitoring()`

Creates all five self-monitoring stream tables. Idempotent — safe to call multiple
times. Emits a warm-up warning if `pgt_refresh_history` has fewer than 50 rows.

**Stream tables created:**

| Name | Schedule | Mode | Purpose |
|------|----------|------|---------|
| `pgtrickle.df_efficiency_rolling` | 48s | AUTO | Rolling-window refresh statistics |
| `pgtrickle.df_anomaly_signals` | 48s | AUTO | Duration spikes, error bursts, mode oscillation |
| `pgtrickle.df_threshold_advice` | 96s | AUTO | Multi-cycle threshold recommendations |
| `pgtrickle.df_cdc_buffer_trends` | 48s | AUTO | CDC buffer growth rates per source |
| `pgtrickle.df_scheduling_interference` | 96s | FULL | Concurrent refresh overlap detection |

### `pgtrickle.teardown_self_monitoring()`

Drops all self-monitoring stream tables. Safe with partial setups — missing tables
are silently skipped. User stream tables are never affected.

### `pgtrickle.self_monitoring_status()`

Returns the status of all five expected self-monitoring stream tables:

| Column | Type | Description |
|--------|------|-------------|
| `st_name` | text | Stream table name |
| `exists` | bool | Whether the ST exists |
| `status` | text | Current status (ACTIVE, SUSPENDED, etc.) |
| `refresh_mode` | text | Effective refresh mode |
| `last_refresh_at` | text | Last successful refresh timestamp |
| `total_refreshes` | bigint | Total completed refreshes |

### `pgtrickle.scheduler_overhead()`

Returns scheduler efficiency metrics for the last hour:

| Column | Type | Description |
|--------|------|-------------|
| `total_refreshes_1h` | bigint | Total refreshes in the last hour |
| `df_refreshes_1h` | bigint | Dog-feeding refreshes in the last hour |
| `df_refresh_fraction` | float | Fraction of refreshes that are self-monitoring |
| `avg_refresh_ms` | float | Average refresh duration (ms) |
| `avg_df_refresh_ms` | float | Average DF refresh duration (ms) |
| `total_refresh_time_s` | float | Total time spent refreshing (seconds) |
| `df_refresh_time_s` | float | Time spent on DF refreshes (seconds) |

### `pgtrickle.explain_dag(format)`

Returns the full refresh DAG as a Mermaid markdown (default) or Graphviz DOT
string. Node colours: user STs = blue, self-monitoring STs = green,
suspended = red, fused = orange.

```sql
-- Mermaid format (default).
SELECT pgtrickle.explain_dag();

-- Graphviz DOT format.
SELECT pgtrickle.explain_dag('dot');
```

### Auto-Apply Policy

The `pg_trickle.self_monitoring_auto_apply` GUC controls whether analytics can
automatically adjust stream table configuration:

| Value | Behaviour |
|-------|-----------|
| `off` (default) | Advisory only — no automatic changes |
| `threshold_only` | Apply threshold recommendations when confidence is HIGH and delta > 5% |
| `full` | Also apply scheduling hints from interference analysis |

Auto-apply is rate-limited to at most one threshold change per stream table
per 10 minutes. Changes are logged to `pgt_refresh_history` with
`initiated_by = 'SELF_MONITOR'`.

### Confidence Levels and Sparse History

`df_threshold_advice` assigns a confidence level to each recommendation:

| Confidence | Criteria | What to expect |
|------------|----------|----------------|
| **HIGH** | ≥ 20 total refreshes, ≥ 5 DIFFERENTIAL, ≥ 2 FULL | Reliable recommendation — auto-apply will act on this |
| **MEDIUM** | ≥ 10 total refreshes | Directionally useful, but may lack enough FULL/DIFF mix |
| **LOW** | < 10 total refreshes | Insufficient data — recommendation equals the current threshold |

**When you see LOW confidence:** This is normal during the first minutes after
`setup_self_monitoring()`. The stream tables need time to accumulate refresh
history. In typical deployments with a 1-minute schedule, expect:
- **LOW** for the first ~10 minutes
- **MEDIUM** after ~10 minutes
- **HIGH** after ~20 minutes (requires at least 2 FULL refreshes — these
  happen naturally when the auto-threshold triggers a mode switch)

If a stream table uses `FULL` mode exclusively, the advice will remain
at MEDIUM because no DIFFERENTIAL observations exist for comparison.

The `sla_headroom_pct` column shows how much faster DIFFERENTIAL is compared
to FULL as a percentage. A value of 70% means "DIFF is 70% faster than FULL".
This column is `NULL` when either FULL or DIFF observations are missing.

---

## Stream Table Snapshots

Snapshots let you export the current state of a stream table into an archival
table, then restore from that snapshot on another node or after a PITR
operation. The main use cases are:

- **Replica bootstrap** — populate a new standby without a full re-scan
- **PITR alignment** — re-align stream table frontiers after point-in-time
  recovery so the first refresh is DIFFERENTIAL, not a full re-scan
- **Archiving** — preserve a historical snapshot for audit or rollback

### `pgtrickle.snapshot_stream_table(name, target)`

Export the current content of a stream table into a new archival table.

```sql
pgtrickle.snapshot_stream_table(
    name   TEXT,              -- stream table (schema.name or plain name)
    target TEXT DEFAULT NULL  -- destination table name; auto-generated if NULL
) → TEXT                      -- returns the fully-qualified snapshot table name
```

The snapshot table is created in the `pgtrickle` schema with the naming
convention `snapshot_<name>_<epoch_ms>` unless you supply `target`. The table
includes three metadata columns added by pg_trickle: `__pgt_snapshot_version`,
`__pgt_frontier`, and `__pgt_snapshotted_at`.

Default snapshots are created through private extension infrastructure and then
owned by the caller. A caller-supplied target schema requires caller `USAGE`
and `CREATE` privileges.

```sql
-- Auto-named snapshot
SELECT pgtrickle.snapshot_stream_table('public.orders_agg');
-- → 'pgtrickle.snapshot_orders_agg_1745452800000'

-- Named snapshot (useful when targeting a replica)
SELECT pgtrickle.snapshot_stream_table(
    'public.orders_agg',
    'pgtrickle.orders_agg_replica_init'
);
```

### `pgtrickle.restore_from_snapshot(name, source)`

Restore a stream table from an archival snapshot and realign its frontier.

```sql
pgtrickle.restore_from_snapshot(
    name   TEXT,  -- stream table to restore into
    source TEXT   -- fully-qualified snapshot table created by snapshot_stream_table()
) → void
```

After `restore_from_snapshot()` completes:

1. The stream table's rows are replaced with the snapshot contents.
2. The frontier is set to the snapshot's frontier, so the **next refresh cycle
   is DIFFERENTIAL** — only changes made after the snapshot are fetched.

The caller must own the destination stream table and have `SELECT` privilege on
the snapshot. The snapshot relation must still match its cataloged OID,
provenance, owner, stream binding, and user-column layout.

```sql
SELECT pgtrickle.restore_from_snapshot(
    'public.orders_agg',
    'pgtrickle.orders_agg_replica_init'
);
```

### `pgtrickle.list_snapshots(name)`

List all archival snapshots for a stream table.

```sql
pgtrickle.list_snapshots(
    name TEXT  -- stream table name
) → SETOF record(
    snapshot_table TEXT,
    created_at     TIMESTAMPTZ,
    row_count      BIGINT,
    frontier       JSONB,
    size_bytes     BIGINT
)
```

### `pgtrickle.drop_snapshot(snapshot_table)`

Drop an archival snapshot table and remove it from the catalog.

```sql
pgtrickle.drop_snapshot(
    snapshot_table TEXT  -- fully-qualified snapshot table
) → void
```

Only the snapshot owner or a superuser can drop it. Transferring a stream table
does not transfer historical snapshot ownership.

```sql
SELECT pgtrickle.drop_snapshot('pgtrickle.orders_agg_replica_init');
```

### Catalog Table

| Table | Description |
|-------|-------------|
| `pgtrickle.pgt_snapshots` | One row per snapshot: `pgt_id`, `snapshot_schema`, `snapshot_table`, `snapshot_version`, `frontier`, `created_at` |

---

## pg_tide Outbox Integration

The full transactional outbox, inbox, consumer-group, and relay APIs live in
the standalone [`pg_tide`](https://github.com/trickle-labs/pg-tide) extension.
Current pg_trickle exposes only the stream-table integration hooks that connect
a refreshed stream table to a pg_tide outbox.

### Quickstart

```sql
-- 1. Install pg_tide in the same database.
CREATE EXTENSION pg_tide;

-- 2. Create a stream table.
SELECT pgtrickle.create_stream_table(
    name     => 'public.orders_agg',
    query    => 'SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id',
    schedule => '5s'
);

-- 3. Attach a pg_tide outbox to the stream table.
SELECT pgtrickle.attach_outbox('public.orders_agg');
```

After attachment, each non-empty refresh publishes a compact delta-summary
event to pg_tide inside the same transaction as the stream-table update.
Payloads have the current pg_trickle envelope:

```json
{
  "v": 1,
  "refresh_id": "...",
  "inserted": 12,
  "updated": 4,
  "deleted": 3,
  "source": "public.orders_agg"
}
```

Use pg_tide's own SQL API to poll, relay, retain, and manage consumers. The old
pg_trickle functions `enable_outbox`, `disable_outbox`, `poll_outbox`,
`commit_offset`, `create_consumer_group`, and `create_inbox` are not part of the
current pg_trickle SQL API.

### `pgtrickle.attach_outbox(name, retention_hours, inline_threshold_rows)`

Attach a pg_tide outbox to a stream table.

```sql
pgtrickle.attach_outbox(
    p_name                  TEXT,
    p_retention_hours       INT DEFAULT 24,
    p_inline_threshold_rows INT DEFAULT 10000
) → void
```

The function resolves and authorizes the stream table under the original
caller, verifies a supported pg_tide installation, and calls
`tide.outbox_create(text, integer, integer)` under that same caller. pg_tide
remains responsible for its own permissions. pg_trickle then records the
mapping and immutable pg_tide provenance in `pgtrickle.pgt_outbox_config`.

The supported pg_tide version range is 0.47.0 through 0.53.0. Missing,
unsupported, incomplete-upgrade, and denied-operation states are reported
separately. A stale or recreated outbox never receives events through an old
mapping.

```sql
SELECT pgtrickle.attach_outbox(
    p_name                  => 'public.orders_agg',
    p_retention_hours       => 48,
    p_inline_threshold_rows => 10000
);
```

### `pgtrickle.detach_outbox(name, if_exists)`

Remove the pg_trickle-to-pg_tide mapping for a stream table.

```sql
pgtrickle.detach_outbox(
    p_name      TEXT,
    p_if_exists BOOLEAN DEFAULT false
) → void
```

`detach_outbox()` does not drop the pg_tide outbox table or delete delivered
messages. Use pg_tide's drop/retention APIs for outbox storage cleanup.

```sql
SELECT pgtrickle.detach_outbox('public.orders_agg', p_if_exists => true);
```

### `pgtrickle.attach_embedding_outbox(name, vector_column, retention_hours, inline_threshold_rows)`

Attach a pg_tide outbox and mark its events as embedding-change events.

```sql
pgtrickle.attach_embedding_outbox(
    p_name                  TEXT,
    p_vector_column         TEXT,
    p_retention_hours       INT DEFAULT 24,
    p_inline_threshold_rows INT DEFAULT 10000
) → void
```

Embedding outbox events include `event_type = 'embedding_change'` and the
configured vector-column name in the pg_tide headers and payload.

```sql
SELECT pgtrickle.attach_embedding_outbox(
    p_name          => 'public.product_embeddings',
    p_vector_column => 'embedding'
);
```

### Outbox Catalog Table

#### `pgtrickle.pgt_outbox_config`

Maps stream tables to their pg_tide outbox names. Populated by
`attach_outbox()` and `attach_embedding_outbox()`; one row per stream table
with an attached outbox.

| Column | Type | Description |
|--------|------|-------------|
| `stream_table_oid` | `OID` | PostgreSQL OID of the stream table (PRIMARY KEY) |
| `stream_table_name` | `TEXT` | Qualified name (`schema.table`) of the stream table |
| `tide_outbox_name` | `TEXT` | Name of the corresponding pg_tide outbox |
| `pg_tide_extension_oid` | `OID` | pg_tide extension OID captured at attach time |
| `pg_tide_version` | `TEXT` | pg_tide extension version captured at attach time |
| `tide_outbox_created_at` | `TIMESTAMPTZ` | pg_tide outbox creation time captured at attach time |
| `embedding_vector_column` | `TEXT` | Optional vector column used by embedding outbox events |
| `created_at` | `TIMESTAMPTZ` | When the outbox was attached |

---

## Public API Stability Contract

### Stable (will not break without a major version bump)

| Surface | Guarantee |
|---------|-----------|
| All functions in the `pgtrickle` schema documented in this reference | Signature and return type preserved across minor releases. New optional parameters may be added with defaults that preserve existing behaviour. |
| Catalog tables `pgtrickle.pgt_stream_tables`, `pgtrickle.pgt_dependencies`, `pgtrickle.pgt_refresh_history` | Existing columns are not renamed or removed. New columns may be added. |
| NOTIFY channels `pg_trickle_refresh`, `pgtrickle_alert`, `pgtrickle_wake` | Channel names and JSON payload structure preserved. New keys may be added to JSON payloads. |
| GUC names listed in `docs/CONFIGURATION.md` | Names preserved; default values may change between minor releases (documented in CHANGELOG). |

### Unstable (may change in any release)

| Surface | Notes |
|---------|-------|
| Functions prefixed with `_` (e.g. `_signal_launcher_rescan`) | Internal use only. |
| Catalog tables not listed above (e.g. `pgt_scheduler_jobs`, `pgt_source_gates`, `pgt_watermarks`) | Schema may change. |
| The `pgtrickle_changes` schema and its `changes_*` tables | CDC implementation detail; format may change. |
| SQL generated by the DVM engine (MERGE, delta CTEs) | Internal query structure is not an API. |
| The `pgtrickle.pgt_schema_version` table | Migration infrastructure; rows and schema may change. |

### Versioning Policy

- **Patch releases** (0.x.Y): Bug fixes only. No breaking changes.
- **Minor releases** (0.X.0): New features. Stable API preserved; unstable surfaces may change. Breaking changes to stable API only with a deprecation cycle (WARNING for one release, removal in the next).
- **Major release** (1.0.0): Stable API locked. Breaking changes require a major version bump.

---

**See also:**
[Configuration](CONFIGURATION.md) ·
[Patterns](PATTERNS.md) ·
[Performance Cookbook](PERFORMANCE_COOKBOOK.md) ·
[Error Reference](ERRORS.md) ·
[Glossary](GLOSSARY.md) ·
[FAQ](FAQ.md)

---

## Reserved Column-Name Prefixes

pg_trickle uses several internal column-name prefixes for synthetic columns it
creates during query analysis and delta SQL generation.  **User-defined columns
whose names begin with these prefixes will conflict with internal template tokens
and produce incorrect query results.**

### `__pgt_*` — DVM engine columns

| Prefix | Purpose | Example |
|--------|---------|---------|
| `__pgt_count` | Weight column for aggregate deduplication (DIFF mode) | `__pgt_count` |
| `__pgt_row_id` | Complete typed-V2 row identity (`BYTEA NOT NULL`) | `__pgt_row_id` |
| `__pgt_wf_N` | Synthetic window-function lifting columns (rewrite pass #7) | `__pgt_wf_1`, `__pgt_wf_2` |
| `__pgt_in_sub_*` | Derived-table alias for multi-column IN → SemiJoin rewrite (M-5) | `__pgt_in_sub_t` |
| `__pgt_src_N` | Source partition aliases in generated delta CTEs | `__pgt_src_0` |

### `__pgs_*` — Scheduler / shared-memory columns

| Prefix | Purpose | Example |
|--------|---------|---------|
| `__pgs_*` | Reserved for future scheduler-side synthetic columns | (none exposed today) |

### Consequences of prefix collision

If your defining query produces a column whose name starts with `__pgt_` or
`__pgs_`, the DVM engine may:

- Fail to generate correct delta SQL (silently wrong results in DIFFERENTIAL mode).
- Produce a parse error if the synthetic name is also used as a template token.
- Cause the MERGE statement to reference the wrong column in the ON clause.

**Mitigation:** rename the conflicting column using an alias:

```sql
-- Bad: __pgt_count collides with the weight column
SELECT id, sum(amount) AS __pgt_count FROM orders GROUP BY id;

-- Good: use any name that does not start with __pgt_ or __pgs_
SELECT id, sum(amount) AS order_total FROM orders GROUP BY id;
```

---

### pgtrickle.pause_scheduler

Pauses the differential refresh scheduler for the listed stream tables.
Prevents the scheduler from starting new refreshes for those nodes until
`pgtrickle.resume_scheduler()` is called. In-flight refreshes are drained
before the function returns, up to `pg_trickle.scheduler_drain_timeout`
(default 30 s).

```sql
pgtrickle.pause_scheduler(nodes text[]) → text
```

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `nodes` | `text[]` | Array of schema-qualified stream table names to pause. |

**Example:**

```sql
SELECT pgtrickle.pause_scheduler(ARRAY['public.order_totals', 'public.customer_summary']);
```

**Notes:**

- Raises `ERROR` if any name in `nodes` does not exist in `pgtrickle.pgt_stream_tables`.
- Safe to call on already-paused nodes (idempotent).
- Returns `'OK'` on success.

---

### pgtrickle.resume_scheduler

Resumes the differential refresh scheduler for the listed stream tables.
Restores normal scheduling for nodes previously paused with `pause_scheduler`.

```sql
pgtrickle.resume_scheduler(nodes text[]) → text
```

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `nodes` | `text[]` | Array of schema-qualified stream table names to resume. |

**Example:**

```sql
SELECT pgtrickle.resume_scheduler(ARRAY['public.order_totals', 'public.customer_summary']);
```

**Notes:**

- Safe to call on nodes that are not paused (idempotent).
- Returns `'OK'` on success.

---

### pgtrickle.stream_table_spec

Returns a stable JSON projection of a single stream table's specification.
Useful for drift detection, import, and configuration auditing.

```sql
pgtrickle.stream_table_spec(relid oid)           → jsonb
pgtrickle.stream_table_spec(qualified_name text) → jsonb
```

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `relid` | `oid` | OID of the stream table relation. |
| `qualified_name` | `text` | Schema-qualified name (e.g. `'public.order_totals'`). |

**Returns:**

```json
{
  "name": "order_totals",
  "schema": "public",
  "query": "SELECT customer_id, sum(amount) FROM orders GROUP BY 1",
  "refresh_mode": "DIFFERENTIAL",
  "schedule": "30s",
  "cdc_mode": "trigger",
  "oid": 12345,
  "diamond_group": null,
  "attach_outbox": null,
  "cdc_slot_name": null
}
```

Returns `NULL` if `relid` / `qualified_name` is not a managed stream table.
All fields in the response are stable across future minor releases.

**Example:**

```sql
SELECT pgtrickle.stream_table_spec('public.order_totals');
SELECT pgtrickle.stream_table_spec('public.order_totals'::regclass::oid);
```

---

## DuckLake Sink Observability

### pgtrickle.ducklake_sink_status

**v0.69.0** — Returns one row per stream table that has a DuckLake sink
configured, showing the most recent delivery outcome.

**Signature:**

```sql
pgtrickle.ducklake_sink_status()
RETURNS TABLE (
    stream_table_name    text,
    last_delivery_status text,
    last_delivery_at     timestamptz,
    last_bytes_written   bigint,
    last_rows_written    bigint,
    failed_attempts      bigint,
    last_error           text
)
```

**Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `stream_table_name` | `text` | Name of the stream table. |
| `last_delivery_status` | `text` | Most recent delivery status: `PENDING`, `WRITING`, `DELIVERED`, `FAILED_RETRYABLE`, or `FAILED_PERMANENT`. `NULL` if no delivery has been attempted. |
| `last_delivery_at` | `timestamptz` | Timestamp when the most recent delivery finished. |
| `last_bytes_written` | `bigint` | Bytes written in the most recent successful delivery. |
| `last_rows_written` | `bigint` | Rows written in the most recent successful delivery. |
| `failed_attempts` | `bigint` | Total number of `FAILED_RETRYABLE` and `FAILED_PERMANENT` rows for this stream table. |
| `last_error` | `text` | Error message from the most recent failed delivery. `NULL` on success. |

**Example:**

```sql
SELECT *
FROM pgtrickle.ducklake_sink_status();
-- stream_table_name | last_delivery_status | last_delivery_at | last_bytes_written | last_rows_written | failed_attempts | last_error
-- revenue_by_region | DELIVERED            | 2026-05-21 ...   |            124208  |               150 |               0 | NULL
```

Requires the `pgtrickle.pgt_ducklake_sink_delivery` catalog table introduced
in v0.69.0. Returns an empty result set if no stream tables have a DuckLake
sink configured.
