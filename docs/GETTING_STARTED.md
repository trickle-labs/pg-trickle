# Getting Started with pg_trickle

## What is pg_trickle?

pg_trickle adds **stream tables** to PostgreSQL — tables that are defined by a SQL query and kept automatically up to date as the underlying data changes. Think of them as materialized views that refresh themselves, but smarter: instead of re-running the entire query on every refresh, pg_trickle uses **Incremental View Maintenance (IVM)** to process only the rows that changed.

Traditional materialized views force a choice: either re-run the full query (expensive) or accept stale data. pg_trickle eliminates this trade-off. When you insert a single row into a million-row table, pg_trickle computes the effect of that one row on the query result — it doesn't touch the other 999,999.

### How data flows

The key concept is that **data flows downstream automatically** — from your base tables through any chain of stream tables, without you writing a single line of orchestration code:

```
  You write to base tables
         │
         ▼
  ┌─────────────┐   triggers (or WAL)   ┌─────────────────────┐
  │ Base Tables │ ─────────────────────▶ │   Change Buffers    │
  │ (you write) │                        │ (pgtrickle_changes.*) │
  └─────────────┘                        └──────────┬──────────┘
                                                     │
                                           delta query (ΔQ) on refresh
                                                     │
                                                     ▼
  ┌──────────────────────────────────────────────────────────────┐
  │  Stream Table A  ◀── depends on base tables                  │
  └──────────────────────────┬───────────────────────────────────┘
                             │  change captured, buffer written
                             ▼
  ┌──────────────────────────────────────────────────────────────┐
  │  Stream Table B  ◀── depends on Stream Table A               │
  └──────────────────────────────────────────────────────────────┘
```

One write to a base table can ripple through an entire DAG of stream tables —
each layer refreshed in the correct topological order, each doing only the work
proportional to what actually changed.

1. You write to your base tables normally — `INSERT`, `UPDATE`, `DELETE`
2. Lightweight `AFTER` row-level triggers capture each change into a buffer, atomically in the same transaction. No polling, no logical replication slots required by default.
3. On each refresh cycle, pg_trickle derives a **delta query (ΔQ)** that reads only the buffered changes since the last refresh frontier
4. The delta is merged into the stream table — only the affected rows are written
5. If other stream tables depend on this one, they are scheduled next (topological order)
6. Optionally: once `wal_level = logical` is available and the first refresh succeeds, pg_trickle automatically transitions from triggers to **WAL-based CDC** (near-zero write-path overhead compared to ~2–15 μs for triggers). The transition is seamless and transparent.

This tutorial walks through a concrete org-chart example so you can see this flow end to end, including a chain of stream tables that propagates changes automatically.

---

## Prerequisites

- PostgreSQL 18.x with pg_trickle installed (see [Installation](installation.md))
- `shared_preload_libraries = 'pg_trickle'` in `postgresql.conf`
- `max_worker_processes` raised to at least 32 (see [Installation](installation.md#postgresql-configuration)); the PostgreSQL default of 8 is often exhausted if you have several databases, causing stream tables to silently stop refreshing
- `psql` or any SQL client

> **Deploying to production?** See the [Pre-Deployment Checklist](PRE_DEPLOYMENT.md)
> for a complete list of requirements, pooler compatibility, and recommended GUC values.

> **Playground:** The fastest way to experiment is the [playground](PLAYGROUND.md) — a Docker Compose environment with sample tables and stream tables pre-loaded. `cd playground && docker compose up -d` and you're running.

> **Quick start with Docker:** Pull the pre-built GHCR image — PostgreSQL 18.3 + pg_trickle ready to run, no configuration needed:
> ```bash
> docker run --rm -e POSTGRES_PASSWORD=secret -p 5432:5432 ghcr.io/trickle-labs/pg_trickle:latest
> ```
> All GUC defaults (`wal_level`, `shared_preload_libraries`, scheduler settings) are pre-configured. See [Installation](installation.md#4-ghcr-docker-image-recommended-for-local-dev) for tag details and volume mounting.

> **Security — RLS bypass:** The pg_trickle background worker executes refresh
> queries with `SET LOCAL row_security = off`. This mirrors the behaviour of
> PostgreSQL's own `REFRESH MATERIALIZED VIEW`, which also bypasses Row-Level
> Security. As a result, **stream table output is always the full, unfiltered
> result set** regardless of any RLS policies defined on source tables.
> Applications must not rely on RLS to restrict what data a stream table
> exposes — use view filters, column masking, or a separate per-role view on
> top of the stream table instead. See [PRE_DEPLOYMENT.md](PRE_DEPLOYMENT.md)
> for the full security checklist.

Connect to the database you want to use and enable the extension:

```sql
CREATE EXTENSION pg_trickle;
```

No additional configuration is needed. pg_trickle automatically discovers all databases on the server and starts a scheduler for each one where the extension is installed.

---

## Chapter 1: Hello World — Your First Stream Table

Before diving into multi-table joins and recursive CTEs, start with the simplest
possible stream table: a **single-source aggregate** with no joins.

### 1.1 Setup

Create one table and enable the extension:

```sql
CREATE EXTENSION IF NOT EXISTS pg_trickle;

CREATE TABLE products (
    id       SERIAL PRIMARY KEY,
    category TEXT           NOT NULL,
    price    NUMERIC(10,2)  NOT NULL,
    in_stock BOOLEAN        NOT NULL DEFAULT true
);

INSERT INTO products (category, price) VALUES
    ('Electronics', 299.99),
    ('Electronics', 49.99),
    ('Books',       14.99),
    ('Books',       24.99),
    ('Books',        9.99);
```

### 1.2 Create the stream table

```sql
SELECT pgtrickle.create_stream_table(
    name     => 'category_summary',
    query    => $$
        SELECT
            category,
            COUNT(*)                    AS product_count,
            ROUND(AVG(price), 2)        AS avg_price,
            MIN(price)                  AS min_price,
            MAX(price)                  AS max_price,
            COUNT(*) FILTER (WHERE in_stock) AS in_stock_count
        FROM products
        GROUP BY category
    $$,
    schedule => '1s'
);
```

Query it immediately — it was populated by the initial full refresh:

```sql
SELECT category, product_count, avg_price, min_price, max_price, in_stock_count
FROM category_summary ORDER BY category;
```

```
  category   | product_count | avg_price | min_price | max_price | in_stock_count
-------------+---------------+-----------+-----------+-----------+----------------
 Books       |             3 |     16.66 |      9.99 |     24.99 |              3
 Electronics |             2 |    174.99 |     49.99 |    299.99 |              2
(2 rows)
```

### 1.3 Watch an INSERT update one group

```sql
INSERT INTO products (category, price) VALUES ('Books', 39.99);
```

Within ~1 second (or call `SELECT pgtrickle.refresh_stream_table('category_summary')` to force it):

```sql
SELECT category, product_count, avg_price, min_price, max_price, in_stock_count
FROM category_summary WHERE category = 'Books';
```

```
 category | product_count | avg_price | min_price | max_price | in_stock_count
----------+---------------+-----------+-----------+-----------+----------------
 Books    |             4 |     22.49 |      9.99 |     39.99 |              4
(1 row)
```

The `Electronics` row was **not touched at all** — pg_trickle read exactly 1
row from the change buffer, adjusted only the Books group.

### 1.4 Watch an UPDATE propagate

```sql
UPDATE products SET price = 19.99 WHERE price = 299.99;
```

After the next refresh:

```sql
SELECT category, product_count, avg_price, min_price, max_price, in_stock_count
FROM category_summary WHERE category = 'Electronics';
```

```
  category   | product_count | avg_price | min_price | max_price | in_stock_count
-------------+---------------+-----------+-----------+-----------+----------------
 Electronics |             2 |     34.99 |     19.99 |     49.99 |              2
(1 row)
```

For `AVG`, pg_trickle maintains running sum and count columns internally, so
re-aggregating a group is O(1) regardless of group size.

### 1.5 What you just saw

- A single function call created the storage table, installed CDC triggers,
  ran the initial full refresh, and registered a 1-second schedule.
- Every subsequent DML on `products` was captured in an `AFTER` trigger —
  no polling, no logical replication.
- Each refresh touched only the rows and groups that changed.
- The stream table is a real PostgreSQL table — you can `SELECT`, index, and
  join against `category_summary` like any other table.

> **Clean up:** `SELECT pgtrickle.drop_stream_table('category_summary'); DROP TABLE products;`

---

## Chapter 2: Joins, Aggregates & Chains

### What you'll build

An **employee org-chart** system with two stream tables:

- **`department_tree`** — a recursive CTE that flattens a department hierarchy into paths like `Company > Engineering > Backend`
- **`department_stats`** — a join + aggregation over `department_tree` (a stream table!) that computes headcount and salary budget, with the full path included
- **`department_report`** — a further aggregation that rolls up stats to top-level departments

The chain `departments` → `department_tree` → `department_stats` → `department_report` demonstrates **automatic downstream propagation**: modify a department name in the base table and all three stream tables update automatically, in the right order, without any manual orchestration.

By the end you will have:

- Seen how stream tables are created, queried, and refreshed
- Watched a single `UPDATE` in a base table cascade through three layers of stream tables automatically
- Understood the four refresh modes and IVM strategies

---

> **Prefer dbt?** A runnable dbt companion project mirrors every step below.
> Clone the repo and run:
> ```bash
> ./examples/dbt_getting_started/scripts/run_example.sh
> ```
> See [examples/dbt_getting_started/](https://github.com/trickle-labs/pg-trickle/tree/main/examples/dbt_getting_started/) for full details.

---

### 2.1 Create the Base Tables

These are ordinary PostgreSQL tables — pg_trickle doesn't require any special column types, annotations, or schema conventions.

Tables without a primary key work, but pg_trickle will emit a `WARNING` at stream table creation time: change detection falls back to a content-based hash across all columns, which is slower for wide tables and cannot distinguish between identical duplicate rows. Adding a primary key gives the best performance and most reliable change detection. A primary key is also required for automatic transition to WAL-based CDC (`cdc_mode = 'auto'`); without one the source table stays on trigger-based CDC.

```sql
-- Department hierarchy (self-referencing tree)
CREATE TABLE departments (
    id         SERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    parent_id  INT REFERENCES departments(id)
);

-- Employees belong to a department
CREATE TABLE employees (
    id            SERIAL PRIMARY KEY,
    name          TEXT NOT NULL,
    department_id INT NOT NULL REFERENCES departments(id),
    salary        NUMERIC(10,2) NOT NULL
);
```

Now insert some data — a three-level department tree and a handful of employees:

```sql
-- Top-level
INSERT INTO departments (id, name, parent_id) VALUES
    (1, 'Company',     NULL);

-- Second level
INSERT INTO departments (id, name, parent_id) VALUES
    (2, 'Engineering', 1),
    (3, 'Sales',       1),
    (4, 'Operations',  1);

-- Third level (under Engineering)
INSERT INTO departments (id, name, parent_id) VALUES
    (5, 'Backend',     2),
    (6, 'Frontend',    2),
    (7, 'Platform',    2);

-- Employees
INSERT INTO employees (name, department_id, salary) VALUES
    ('Alice',   5, 120000),   -- Backend
    ('Bob',     5, 115000),   -- Backend
    ('Charlie', 6, 110000),   -- Frontend
    ('Diana',   7, 130000),   -- Platform
    ('Eve',     3, 95000),    -- Sales
    ('Frank',   3, 90000),    -- Sales
    ('Grace',   4, 100000);   -- Operations
```

At this point these are plain tables with no triggers, no change tracking, nothing special. The department tree looks like this:

```
Company (1)
├── Engineering (2)
│   ├── Backend (5)     — Alice, Bob
│   ├── Frontend (6)    — Charlie
│   └── Platform (7)    — Diana
├── Sales (3)           — Eve, Frank
└── Operations (4)      — Grace
```

---

### 2.2 Create the First Stream Table — Recursive Hierarchy

Our first stream table flattens the department tree. For every department, it computes the full path from the root and the depth level. This uses `WITH RECURSIVE` — a SQL construct that can't be differentiated with simple algebraic rules (the recursion depends on itself), but pg_trickle handles it using **incremental strategies** (semi-naive evaluation for inserts, Delete-and-Rederive for mixed changes) that we'll explain later.

```sql
SELECT pgtrickle.create_stream_table(
    name         => 'department_tree',
    query        => $$
    WITH RECURSIVE tree AS (
        -- Base case: root departments (no parent)
        SELECT id, name, parent_id, name AS path, 0 AS depth
        FROM departments
        WHERE parent_id IS NULL

        UNION ALL

        -- Recursive step: children join back to the tree
        SELECT d.id, d.name, d.parent_id,
               tree.path || ' > ' || d.name AS path,
               tree.depth + 1
        FROM departments d
        JOIN tree ON d.parent_id = tree.id
    )
    SELECT id, name, parent_id, path, depth FROM tree
    $$,
    schedule     => '1s'
);
```

> **Note on short schedules:** A 1-second schedule is safe for development and production thanks to `auto_backoff` (on by default). If a refresh takes more than 95% of the schedule window, the scheduler automatically stretches the effective interval (up to 8× the configured schedule) to prevent CPU runaway, then resets to 1× as soon as a refresh completes on time. You will see a `WARNING` message when backoff activates.
>
> **Additional parameters:** `create_stream_table` also accepts `diamond_consistency` (`'none'` or `'atomic'`) and `diamond_schedule_policy` (`'fastest'` or `'slowest'`) for diamond-shaped dependency graphs. Schedules can be cron expressions (e.g., `'*/5 * * * *'`, `'@hourly'`). Set `pooler_compatibility_mode => true` if you're connecting through PgBouncer or another transaction-mode connection pooler. See [SQL_REFERENCE.md](SQL_REFERENCE.md) for the full parameter list.

### What just happened?

That single function call did a lot of work atomically (all in one transaction):

1. **Parsed** the defining query into an operator tree — identifying the recursive CTE, the scan on `departments`, the join, the union
2. **Created a storage table** called `department_tree` in the `public` schema — a real PostgreSQL heap table with columns matching the SELECT output, plus internal columns `__pgt_row_id` (a hash used to track individual rows)
3. **Installed CDC triggers** on the `departments` table — lightweight `AFTER INSERT OR UPDATE OR DELETE` row-level triggers that will capture every future change
4. **Created a change buffer table** in the `pgtrickle_changes` schema — this is where the triggers write captured changes
5. **Ran an initial full refresh** — executed the recursive query against the current data and populated the storage table
6. **Registered the stream table** in pg_trickle's catalog with a 1-second refresh schedule
> **TRUNCATE caveat:** Row-level triggers do not fire on `TRUNCATE`. If you `TRUNCATE` a base table, the change is not captured incrementally — the stream table will become stale. Use `DELETE FROM table` instead, or call `pgtrickle.refresh_stream_table('department_tree')` after a TRUNCATE. If the stream table uses DIFFERENTIAL mode, temporarily switch to FULL for a full recompute: `pgtrickle.alter_stream_table('department_tree', refresh_mode => 'FULL')`, refresh, then switch back.
Query it immediately — it's already populated:

```sql
SELECT id, name, parent_id, path, depth FROM department_tree ORDER BY path;
```

Expected output:

```
 id |    name     | parent_id |               path               | depth
----+-------------+-----------+----------------------------------+-------
  1 | Company     |           | Company                          |     0
  2 | Engineering |         1 | Company > Engineering            |     1
  5 | Backend     |         2 | Company > Engineering > Backend  |     2
  6 | Frontend    |         2 | Company > Engineering > Frontend |     2
  7 | Platform    |         2 | Company > Engineering > Platform |     2
  4 | Operations  |         1 | Company > Operations             |     1
  3 | Sales       |         1 | Company > Sales                  |     1
(7 rows)
```

This is a **real PostgreSQL table** — you can create indexes on it, join it in other queries, reference it in views, or even use it as a source for other stream tables. pg_trickle keeps it in sync automatically.

> **Key insight:** The recursive query that computes paths and depths would normally need to be re-run manually (or via `REFRESH MATERIALIZED VIEW`). With pg_trickle, it stays fresh — any change to the `departments` table is automatically reflected within the schedule bound (1 second here).

---

### 2.3 Chain Stream Tables — Build the Downstream Layers

Now create `department_stats`. The twist: instead of joining directly against `departments`, it joins against `department_tree` — the stream table we just created. This creates a **chain**: changes to `departments` update `department_tree`, whose changes then trigger `department_stats` to update.

This demonstrates how pg_trickle builds a **DAG** — a directed acyclic graph of stream tables — and automatically schedules refreshes in topological order.

```sql
SELECT pgtrickle.create_stream_table(
    name         => 'department_stats',
    query        => $$
    SELECT
        t.id          AS department_id,
        t.name        AS department_name,
        t.path        AS full_path,
        t.depth,
        COUNT(e.id)                    AS headcount,
        COALESCE(SUM(e.salary), 0)     AS total_salary,
        COALESCE(AVG(e.salary), 0)     AS avg_salary
    FROM department_tree t
    LEFT JOIN employees e ON e.department_id = t.id
    GROUP BY t.id, t.name, t.path, t.depth
    $$,
    schedule     => 'calculated'      -- CALCULATED: inherit schedule from downstream; see explanation below
);
```

### What just happened — and why this one is different?

Like before, pg_trickle parsed the query, created a storage table, and set up CDC. But `department_stats` depends on `department_tree`, not a base table — so *no new triggers were installed*. Instead, pg_trickle registered `department_tree` as an upstream dependency in the DAG.

The schedule is `'calculated'` (CALCULATED mode), which means: "don't give this table its own schedule — inherit the tightest schedule of any downstream table that queries it". Internally this stores `NULL` in the catalog, but you must pass the string `'calculated'` — passing SQL `NULL` is an error. Since no other stream table has been created yet, it will be refreshed on demand or when a downstream dependent triggers it.

The query has no recursive CTE, so pg_trickle uses **algebraic differentiation**:

1. Decomposed into operators: `Scan(department_tree)` → `LEFT JOIN` → `Scan(employees)` → `Aggregate(GROUP BY + COUNT/SUM/AVG)` → `Project`
2. Derived a differentiation rule for each:
   - `Δ(Scan)` = read only change buffer rows (not the full table)
   - `Δ(LEFT JOIN)` = join change rows from one side against the full other side
   - `Δ(Aggregate)` = for COUNT/SUM/AVG, add or subtract per group — no rescan needed
3. Composed these into a single **delta query (ΔQ)** that never touches unchanged rows

When one employee is inserted, the refresh reads one change buffer row, joins to find the department, and adjusts only that group's count and sum.

Query it:

```sql
SELECT department_name, full_path, headcount, total_salary
FROM department_stats
ORDER BY full_path;
```

Expected output:

```
 department_name |            full_path             | headcount | total_salary
-----------------+----------------------------------+-----------+--------------
 Company         | Company                          |         0 |            0
 Engineering     | Company > Engineering            |         0 |            0
 Backend         | Company > Engineering > Backend  |         2 |    235000.00
 Frontend        | Company > Engineering > Frontend |         1 |    110000.00
 Platform        | Company > Engineering > Platform |         1 |    130000.00
 Operations      | Company > Operations             |         1 |    100000.00
 Sales           | Company > Sales                  |         2 |    185000.00
(7 rows)
```

Notice that the `full_path` column comes from `department_tree` — this data already went through one layer of incremental maintenance before landing here.

### Add a third layer: `department_report`

Now add a rollup that aggregates `department_stats` by top-level group (depth = 1):

```sql
SELECT pgtrickle.create_stream_table(
    name         => 'department_report',
    query        => $$
    SELECT
        split_part(full_path, ' > ', 2) AS division,
        SUM(headcount)                  AS total_headcount,
        SUM(total_salary)               AS total_payroll
    FROM department_stats
    WHERE depth >= 1
    GROUP BY 1
    $$,
    schedule     => '1s'              -- this is the only explicit schedule; CALCULATED tables above inherit it
);
```

The DAG is now:

```
departments (base)  employees (base)
      │                   │
      ▼                   │
department_tree ──────────┤
   (DIFF, CALCULATED)     │
      │                   ▼
      └──────▶ department_stats
                 (DIFF, CALCULATED)
                      │
                      ▼
               department_report
                  (DIFF, 1s)   ◀── only explicit schedule
```

`department_report` drives the whole pipeline. Because it has a 1-second schedule, pg_trickle automatically propagates that cadence upstream: `department_stats` and `department_tree` will also be refreshed within 1 second of a base table change, in topological order, with no manual configuration.

Query the report:

```sql
SELECT division, total_headcount, total_payroll FROM department_report ORDER BY division;
```

```
  division   | total_headcount | total_payroll
-------------+-----------------+---------------
 Engineering |               4 |    475000.00
 Operations  |               1 |    100000.00
 Sales       |               2 |    185000.00
(3 rows)
```

---

### 2.4 Watch a Change Cascade Through All Three Layers

This is the heart of pg_trickle. We'll make four changes to the base tables and watch changes propagate automatically through the three-layer DAG — each layer doing only the minimum work.

### The data flow pipeline (three layers)

```
  Your SQL statement
       │
       ▼
  CDC trigger fires (same transaction)
  Change buffer receives one row
       │
       ▼
  Background scheduler fires (within ~1 second)
       │
       ├──▶ [Layer 1] Refresh department_tree
       │         delta query reads change buffer
       │         MERGE touches only affected rows in department_tree
       │         department_tree's own change buffer is updated
       │
       ├──▶ [Layer 2] Refresh department_stats
       │         delta query reads department_tree's change buffer
       │         MERGE touches only affected department groups
       │
       └──▶ [Layer 3] Refresh department_report
                 delta query reads department_stats' change buffer
                 MERGE touches only affected division rows
                 All change buffers cleaned up ✓
```

All three layers run in a single scheduled pass, in topological order.

#### 2.4a: INSERT ripples through all three layers

```sql
INSERT INTO employees (name, department_id, salary) VALUES
    ('Heidi', 6, 105000);  -- New Frontend engineer
```

**What happened immediately (in your transaction):** The `AFTER INSERT` trigger on `employees` fired and wrote one row to `pgtrickle_changes.changes_<employees_oid>`. The row contains the new values, action type `I`, and the LSN at the time of insert. Your transaction committed normally — no blocking.

The stream tables don't know about Heidi yet. The change is in the buffer, waiting for the next refresh.

> **The background scheduler handles this automatically.** With a 1-second schedule, `department_stats` and `department_report` refresh within about a second.
>
> To confirm a refresh has happened, check `data_timestamp` in the monitoring view:
> ```sql
> SELECT name, data_timestamp, staleness FROM pgtrickle.pgt_status();
> ```
> To force an immediate synchronous refresh, wait a moment first (so the scheduler can finish its current tick), then call in topological order. Note that `refresh_stream_table` only refreshes the named table — it does not cascade upstream:
> ```sql
> SELECT pg_sleep(2);  -- let the scheduler finish any in-progress tick
> SELECT pgtrickle.refresh_stream_table('department_stats');
> SELECT pgtrickle.refresh_stream_table('department_report');
> ```

**What happened across the three layers:**

| Layer | What ran | Rows touched |
|-------|----------|--------------|
| `department_tree` | No change — `employees` is not a source for this ST | 0 |
| `department_stats` | Delta query: read 1 buffer row, join to Frontend, COUNT+1, SUM+105000 | 1 (Frontend group only) |
| `department_report` | Delta query: read 1 change from dept_stats, SUM += 1 headcount, += 105000 | 1 (Engineering row only) |

Check the result:

```sql
SELECT department_name, headcount, total_salary FROM department_stats
WHERE department_name = 'Frontend';
```

```
 department_name | headcount | total_salary
-----------------+-----------+--------------
 Frontend        |         2 |    215000.00
```

The 6 other groups in `department_stats` were **not touched at all**.

> **Contrast with a standard materialized view:** `REFRESH MATERIALIZED VIEW` would re-scan all 8 employees, re-join with all 7 departments, re-aggregate, and update all 7 rows. With pg_trickle, the work was proportional to the 1 changed row — across all three layers.

#### 2.4b: A department change cascades through the whole DAG

Now change the `departments` table — the root of the entire chain:

```sql
INSERT INTO departments (id, name, parent_id) VALUES
    (8, 'DevOps', 2);  -- New team under Engineering
```

**What happened:** The CDC trigger on `departments` fired. The change buffer for `departments` has one new row. None of the stream tables know about it yet.

> **The scheduler handles this automatically** — all three tables will refresh within a second in the correct dependency order (upstream first). To force it synchronously, wait a moment first then refresh each table in topological order (`refresh_stream_table` does not cascade upstream):
> ```sql
> SELECT pg_sleep(2);
> SELECT pgtrickle.refresh_stream_table('department_tree');
> SELECT pgtrickle.refresh_stream_table('department_stats');
> SELECT pgtrickle.refresh_stream_table('department_report');
> ```

**What happened across all three layers:**

| Layer | What ran | Rows touched |
|-------|----------|--------------|
| `department_tree` | Semi-naive evaluation: base case finds new dept, recursive term computes its path. Result: 1 new row | 1 inserted |
| `department_stats` | Delta query reads new row from dept_tree's change buffer; DevOps has 0 employees so delta is minimal | 1 inserted (headcount=0) |
| `department_report` | Delta on Engineering row: headcount stays the same (DevOps has 0 employees) | 0 effective changes |

**How the recursive CTE refresh works** — unlike `department_stats`, recursive CTEs can't be algebraically differentiated (the recursion references itself). pg_trickle uses **incremental fixpoint strategies**:

- **INSERT** → semi-naive evaluation: differentiate the base case, propagate the delta through the recursive term, stopping when no new rows are produced. Only new rows inserted.
- **DELETE or UPDATE** → Delete-and-Rederive (DRed): remove rows derived from deleted facts, re-derive rows that may have alternative derivation paths, handle cascades cleanly.

```sql
SELECT id, name, depth, path FROM department_tree WHERE name = 'DevOps';
```

```
 id |  name  | depth |              path
----+--------+-------+--------------------------------
  8 | DevOps |     2 | Company > Engineering > DevOps
(1 row)
```

The recursive CTE automatically expanded to include the new department at the correct depth and path. One inserted row in `departments` produced one new row in the stream table.

#### 2.4c: UPDATE — A single rename that cascades everywhere

Rename "Engineering" to "R&D":

```sql
UPDATE departments SET name = 'R&D' WHERE id = 2;
```

**What happened in the change buffer:** The CDC trigger captured the **old** row (`name='Engineering'`) and the **new** row (`name='R&D'`). Both old and new values are stored so the delta can compute what to remove and what to add.

Wait a moment for the scheduler to propagate the rename through all layers. To force it synchronously, wait then refresh each table in topological order (`refresh_stream_table` does not cascade upstream):

```sql
SELECT pg_sleep(2);
SELECT pgtrickle.refresh_stream_table('department_tree');
SELECT pgtrickle.refresh_stream_table('department_stats');
SELECT pgtrickle.refresh_stream_table('department_report');
```

**What happened across all three layers:**

| Layer | Work done | Result |
|-------|-----------|--------|
| `department_tree` | DRed strategy: delete rows derived with old name, re-derive with new name. 5 rows updated (Engineering + 4 sub-teams) | Paths now say `Company > R&D > …` |
| `department_stats` | Delta reads 5 changed rows from dept_tree's buffer; updates `full_path` column for those 5 departments | 5 rows updated |
| `department_report` | Division name changed: "Engineering" row replaced by "R&D" row | 1 DELETE + 1 INSERT |

Query to verify the cascade:

```sql
SELECT name, path FROM department_tree WHERE path LIKE '%R&D%' ORDER BY depth, name;
```

```
   name   |           path           
----------+--------------------------
 R&D      | Company > R&D
 Backend  | Company > R&D > Backend
 DevOps   | Company > R&D > DevOps
 Frontend | Company > R&D > Frontend
 Platform | Company > R&D > Platform
(5 rows)
```

One `UPDATE` to a department name flowed through all three layers automatically — updating 5 + 5 + 2 rows across the chain.

#### 2.4d: DELETE — Remove an employee

```sql
DELETE FROM employees WHERE name = 'Bob';
```

**What happened:** The `AFTER DELETE` trigger on `employees` fired, writing a change buffer row with action type `D` and Bob's old values (`department_id=5, salary=115000`). The delta query will use these old values to compute the correct aggregate adjustment — it knows to subtract 115000 from Backend's salary sum and decrement the count.

> **Important — refresh before querying:** The background scheduler refreshes all three tables within ~1 second, *in topological order*. To see the result immediately, wait a moment then explicitly refresh in upstream-first order:

```sql
SELECT pg_sleep(2);
SELECT pgtrickle.refresh_stream_table('department_stats');
SELECT pgtrickle.refresh_stream_table('department_report');
```

> **Why call `department_stats` first?** `department_stats` sources from both `employees` and `department_tree`. Refreshing in topological order ensures each layer processes its upstream changes before computing its own deltas. Even when `department_tree` has unprocessed changes from step 4c and a new employee change arrives simultaneously, pg_trickle's differential engine handles both correctly — using the pre-change left snapshot (L₀) to avoid double-counting.

Then verify the result:

```sql
SELECT department_name, headcount, total_salary, avg_salary
FROM department_stats WHERE department_name = 'Backend';
```

```
 department_name | headcount | total_salary |     avg_salary
-----------------+-----------+--------------+---------------------
 Backend         |         1 |    120000.00 | 120000.000000000000
(1 row)
```

Headcount dropped from 2 → 1 and the salary aggregates updated. Again, only the Backend group was touched — the other 6 department rows were untouched.

---

## Chapter 3: Scheduling & Backpressure

### Automatic Scheduling — Let the DAG Drive Itself

pg_trickle runs a **background scheduler** that automatically refreshes stale tables in topological order. In the Step 4 examples above, the scheduler handled every change within about a second. You can also call `refresh_stream_table()` directly when needed (e.g. in scripts or tests), but in normal operation the scheduler takes care of everything.

### How schedules propagate

We gave `department_report` a `'1s'` schedule and the two upstream tables a `NULL` schedule (CALCULATED mode). This is the recommended pattern:

```
 department_tree    (CALCULATED → inherits 1s from downstream)
       │
 department_stats   (CALCULATED → inherits 1s from downstream)
       │
 department_report  (1s — the only explicit schedule)
```

CALCULATED mode (pass `schedule => 'calculated'`) means: compute the tightest schedule across all downstream dependents. You declare freshness requirements at the tables your application queries — the system figures out how often each upstream table needs to refresh.

### What the scheduler does every second

1. Queries the catalog for stream tables past their freshness bound
2. Sorts them topologically (upstream first) — `department_tree` refreshes before `department_stats`, which refreshes before `department_report`
3. Runs each refresh (respecting `pg_trickle.max_concurrent_refreshes`)
4. Updates the last-refresh frontier

### Monitoring

```sql
-- Current status of all stream tables
SELECT name, status, refresh_mode, schedule, data_timestamp, staleness
FROM pgtrickle.pgt_status();
```

```
        name                 | status |  refresh_mode | schedule |       data_timestamp        |    staleness
-----------------------------+--------+---------------+----------+-----------------------------+-----------------
 public.department_tree      | ACTIVE | DIFFERENTIAL  |          | 2026-02-26 10:30:00.123+01 | 00:00:00.877
 public.department_stats     | ACTIVE | DIFFERENTIAL  |          | 2026-02-26 10:30:00.456+01 | 00:00:00.544
 public.department_report    | ACTIVE | DIFFERENTIAL  | 1s       | 2026-02-26 10:30:00.789+01 | 00:00:00.211
```

```sql
-- Detailed performance stats
SELECT pgt_name, total_refreshes, avg_duration_ms, successful_refreshes
FROM pgtrickle.pg_stat_stream_tables;
```

```sql
-- Health check: quick triage of common issues
SELECT check_name, severity, detail FROM pgtrickle.health_check();
```

```sql
-- Visualize the dependency DAG
SELECT * FROM pgtrickle.dependency_tree();
```

```sql
-- Recent refresh timeline across all stream tables
SELECT * FROM pgtrickle.refresh_timeline(10);
```

```sql
-- Check CDC change buffer sizes (spotting buffer build-up)
SELECT * FROM pgtrickle.change_buffer_sizes();
```

See [SQL_REFERENCE.md](SQL_REFERENCE.md) for the full list of monitoring functions including `list_sources()`, `trigger_inventory()`, and `diamond_groups()`.

---

## Chapter 4: Monitoring In Depth

All the monitoring capabilities from the monitoring quick reference above, expanded.
For the five most important day-to-day introspection queries see the
[Monitoring Quick Reference](#monitoring-quick-reference) at the end of this guide.

### Optional: WAL-based CDC

By default pg_trickle uses triggers. If `wal_level = logical` is configured, set:

```sql
ALTER SYSTEM SET pg_trickle.cdc_mode = 'auto';
SELECT pg_reload_conf();
```

pg_trickle will automatically transition each stream table from trigger-based to WAL-based capture after the first successful refresh — reducing per-write overhead from ~2–15 μs (triggers) to near-zero (WAL-based capture adds no synchronous overhead to your DML). The transition is transparent; your queries and the refresh schedule are unaffected.

### Optional: Parallel Refresh

By default the scheduler refreshes stream tables **sequentially** in topological order within a single background worker. This is correct and efficient for most workloads.

For deployments with many independent stream tables, enable parallel refresh:

```sql
ALTER SYSTEM SET pg_trickle.parallel_refresh_mode = 'on';
ALTER SYSTEM SET pg_trickle.max_dynamic_refresh_workers = 4;  -- cluster-wide cap
SELECT pg_reload_conf();
```

Independent stream tables at the same DAG level will then refresh concurrently in separate dynamic background workers. Refresh pairs with IMMEDIATE-trigger connections and atomic consistency groups still run in a single worker for correctness.

**Before enabling,** ensure `max_worker_processes` has enough room:
```
max_worker_processes >= 1 (launcher)
                      + number of databases with stream tables
                      + max_dynamic_refresh_workers (default 4)
                      + autovacuum and other extension workers
```

Monitor parallel refresh:

```sql
SELECT * FROM pgtrickle.worker_pool_status();        -- live worker budget
SELECT * FROM pgtrickle.parallel_job_status(60);     -- recent jobs
```

See [CONFIGURATION.md — Parallel Refresh](CONFIGURATION.md#parallel-refresh) for the complete tuning reference.

### Optional: PgBouncer / Connection Pooler Compatibility

If you're connecting through PgBouncer or another connection pooler in **transaction mode** (the default on Supabase, Railway, Neon, and most managed PostgreSQL platforms), set `pooler_compatibility_mode` when creating or altering a stream table:

```sql
SELECT pgtrickle.create_stream_table(
    name                    => 'live_headcount',
    query                   => 'SELECT department_id, COUNT(*) FROM employees GROUP BY 1',
    schedule                => '1s',
    pooler_compatibility_mode => true
);
```

This disables prepared statements and NOTIFY emissions for that table — the two features that break in transaction-pool mode. Leave it off (the default) if you connect directly to PostgreSQL.

### Optional: Change Buffer Compaction

For high-churn tables, pg_trickle automatically compacts the pending change buffer before each refresh cycle when it exceeds `pg_trickle.compact_threshold` (default 100,000 rows). INSERT→DELETE pairs that cancel each other out are eliminated, and multiple changes to the same row are collapsed to a single net change, reducing delta scan overhead by 50–90%.

---

## Chapter 5: Advanced Topics

### Refresh Modes and IVM Strategies

You've now seen the IVM strategies pg_trickle uses for incremental view maintenance. Understanding the four refresh modes and when each strategy applies helps you write efficient stream table queries.

### The Four Refresh Modes

| Mode | When it refreshes | Use case |
|------|------------------|----------|
| **AUTO** (default) | On a schedule (background) | Most use cases — uses DIFFERENTIAL when possible, falls back to FULL automatically |
| **DIFFERENTIAL** | On a schedule (background) | Like AUTO but errors if the query can't be differentiated |
| **FULL** | On a schedule (background) | Forces full recompute every cycle |
| **IMMEDIATE** | Synchronously, in the same transaction as the DML | Real-time dashboards, audit tables — the stream table is always up-to-date |

When you omit `refresh_mode`, the default is `'AUTO'` — it uses differential (delta-only) maintenance when the query supports it, and automatically falls back to full recomputation when it doesn't. You only need to specify a mode explicitly for advanced cases.

**IMMEDIATE mode** maintains stream tables synchronously within the same transaction as the base table DML. It uses statement-level AFTER triggers with transition tables — no change buffers, no scheduler. The stream table is always consistent with the current transaction.

```sql
-- Create a stream table that updates in real-time
SELECT pgtrickle.create_stream_table(
    name         => 'live_headcount',
    query        => $$
    SELECT department_id, COUNT(*) AS headcount
    FROM employees
    GROUP BY department_id
    $$,
    refresh_mode => 'IMMEDIATE'
);

-- After any INSERT/UPDATE/DELETE on employees,
-- live_headcount is already up-to-date — no refresh needed!
```

IMMEDIATE mode supports joins, aggregates, window functions, LATERAL subqueries, and cascading IMMEDIATE stream tables. Recursive CTEs are not supported in IMMEDIATE mode (use DIFFERENTIAL instead).

You can switch between modes at any time:

```sql
-- Switch from DIFFERENTIAL to IMMEDIATE
SELECT pgtrickle.alter_stream_table('department_stats', refresh_mode => 'IMMEDIATE');

-- Switch back to DIFFERENTIAL with a schedule
SELECT pgtrickle.alter_stream_table('department_stats', refresh_mode => 'DIFFERENTIAL', schedule => '1s');
```

### Algebraic Differentiation (used by `department_stats`)

For queries composed of scans, filters, joins, and algebraic aggregates (COUNT, SUM, AVG), pg_trickle can derive the IVM delta **mathematically**. The rules come from the theory of [DBSP (Database Stream Processing)](https://arxiv.org/abs/2203.16684):

| Operator | Delta Rule | Cost |
|----------|-----------|------|
| **Scan** | Read only change buffer rows (not the full table) | O(changes) |
| **Filter (WHERE)** | Apply predicate to change rows | O(changes) |
| **Join** | Join change rows from one side against the full other side | O(changes × lookup) |
| **Aggregate (COUNT/SUM/AVG)** | Add or subtract deltas per group — no rescan | O(affected groups) |
| **Project** | Pass through | O(changes) |

The total cost is proportional to the number of **changes**, not the table size. For a million-row table with 10 changes, the delta query touches ~10 rows.

### Incremental Strategies for Recursive CTEs (used by `department_tree`)

For recursive CTEs, pg_trickle can't derive an algebraic delta because the recursion references itself. Instead it uses two complementary strategies, chosen automatically based on what changed:

**Semi-naive evaluation** (for INSERT-only changes):
1. Differentiate the base case — find the new seed rows
2. Propagate the delta through the recursive term, iterating until no new rows are produced
3. The result is only the *new* rows created by the change — not the whole tree

**Delete-and-Rederive (DRed)** (for DELETE or UPDATE):
1. Remove all rows derived from the old fact
2. Re-derive rows that had the old fact as *one of their derivation paths* (they may still be reachable via other paths)
3. Insert the newly derived rows under the new fact

Both strategies are more efficient than full recomputation — they work on the *affected portion of the result set*, not the entire recursive query. The MERGE only modifies rows that actually changed.

### When to use which strategy?

You don't choose — pg_trickle detects the strategy automatically based on the query structure:

| Query Pattern | Strategy | Performance |
|---------------|----------|-------------|
| Scan + Filter + Join + algebraic Aggregate (COUNT/SUM/AVG) | Algebraic | Excellent — O(changes) |
| `CORR`, `COVAR_POP/SAMP`, `REGR_*` (12 functions) | Algebraic (Welford running totals) | O(changes) — running totals updated per changed row, no group rescan |
| Non-recursive CTEs | Algebraic (inlined) | CTE body is differentiated inline |
| `MIN` / `MAX` aggregates | Semi-algebraic | Uses LEAST/GREATEST merge; per-group rescan only when an extremum is deleted |
| `STRING_AGG`, `ARRAY_AGG`, ordered-set aggregates | Group-rescan | Affected groups fully re-aggregated from source |
| `GROUPING SETS` / `CUBE` / `ROLLUP` | Algebraic (rewritten) | Auto-expanded to `UNION ALL` of `GROUP BY` queries; CUBE capped at 64 branches |
| Recursive CTEs (`WITH RECURSIVE`) INSERT | Semi-naive evaluation | O(new rows derived from the change) |
| Recursive CTEs (`WITH RECURSIVE`) DELETE/UPDATE | Delete-and-Rederive | Re-derives rows with alternative paths; O(affected subgraph) |
| LATERAL subqueries | Correlated re-evaluation | Only outer rows correlated with changed inner data re-evaluated — O(correlated rows) |
| Window functions | Partition recompute | Only affected partitions recomputed |
| `ORDER BY … LIMIT N` (TopK) | Scoped recomputation | Re-evaluates top-N via MERGE; stores exactly N rows |
| IMMEDIATE mode queries | In-transaction delta | Same algebraic strategies, applied synchronously via transition tables |

### FUSE Circuit Breaker

The fuse is a **circuit breaker** that stops a stream table from processing an
unexpectedly large batch of changes — for example from a runaway script or
mass-delete migration — without operator review.

```sql
-- Arm a fuse: blow when pending changes exceed 50 000 rows
SELECT pgtrickle.alter_stream_table(
    'category_summary',
    fuse           => 'on',
    fuse_ceiling   => 50000
);

-- Check fuse status across all stream tables
SELECT name, fuse_mode, fuse_state, fuse_ceiling, blown_at
FROM pgtrickle.fuse_status();

-- After investigating and deciding to apply the batch:
SELECT pgtrickle.reset_fuse('category_summary', action => 'apply');

-- Or skip the oversized batch entirely and resume from current state:
SELECT pgtrickle.reset_fuse('category_summary', action => 'skip_changes');
```

`reset_fuse` supports three actions:
- `'apply'` — process all pending changes and resume normal scheduling.
- `'reinitialize'` — drop and repopulate the stream table from scratch.
- `'skip_changes'` — discard pending changes and resume from the current frontier.

A `pgtrickle_alert` NOTIFY is emitted when the fuse blows, making it easy to
hook into alerting pipelines or `LISTEN` from application code.

### Partitioned Stream Tables

For large stream tables, declare a **partition key** at creation time so
MERGE operations are scoped to only the relevant partitions:

```sql
SELECT pgtrickle.create_stream_table(
    name         => 'sales_by_month',
    query        => $$
        SELECT
            DATE_TRUNC('month', sale_date) AS month,
            product_id,
            SUM(amount) AS total_sales
        FROM sales
        GROUP BY 1, 2
    $$,
    schedule     => '1m',
    partition_by => 'month'    -- partition key must be in the SELECT output
);
```

pg_trickle creates the storage table as `PARTITION BY RANGE (month)` with a
catch-all partition, then on each refresh:
1. Inspects the delta to find the `MIN` and `MAX` of the partition key.
2. Injects `AND st.month BETWEEN min AND max` into the MERGE ON clause.
3. PostgreSQL prunes all partitions outside the range — giving ~100× I/O
   reduction for a 0.1% change rate on a 10M-row table.

See [SQL_REFERENCE.md](SQL_REFERENCE.md#create_stream_table) for full partitioning options.

### IMMEDIATE Mode — Real-Time In-Transaction IVM

```sql
-- Create a stream table that updates in the same transaction as its source
SELECT pgtrickle.create_stream_table(
    name         => 'live_headcount',
    query        => $$
    SELECT department_id, COUNT(*) AS headcount
    FROM employees
    GROUP BY department_id
    $$,
    refresh_mode => 'IMMEDIATE'
);

-- After any INSERT/UPDATE/DELETE on employees, live_headcount is already up-to-date:
INSERT INTO employees (name, department_id, salary) VALUES ('Zara', 2, 95000);
SELECT * FROM live_headcount WHERE department_id = 2;  -- 4 rows, immediately
```

IMMEDIATE mode uses statement-level `AFTER` triggers with transition tables —
no change buffers, no scheduler, no background workers. The stream table is
always consistent with the current transaction. Ideal for audit tables,
real-time dashboards, and applications that need zero-latency reads.

### Multi-Tenant Worker Quotas

In deployments with multiple databases, one busy database can starve others
if all dynamic refresh workers are claimed. The `per_database_worker_quota`
GUC prevents this:

```sql
-- Limit one performance-critical database to 4 workers (with burst to 6)
ALTER DATABASE analytics  SET pg_trickle.per_database_worker_quota = 4;
-- Allow a reporting database only 2 base workers
ALTER DATABASE reporting  SET pg_trickle.per_database_worker_quota = 2;
-- Apply changes
SELECT pg_reload_conf();
```

When the cluster has spare capacity (active workers < 80% of
`max_dynamic_refresh_workers`), a database may temporarily burst to 150% of
its quota. Burst is reclaimed within 1 scheduler cycle once load rises.
Within each dispatch tick, IMMEDIATE-trigger closures are always dispatched
first, followed by atomic groups, singletons, and cyclic SCCs.

See [CONFIGURATION.md](CONFIGURATION.md#c3-1-per-database-worker-quotas) for
full quota tuning options.

---

## Clean Up

When you're done experimenting, drop the stream tables. Drop dependents before their sources:

```sql
SELECT pgtrickle.drop_stream_table('department_report');
SELECT pgtrickle.drop_stream_table('department_stats');
SELECT pgtrickle.drop_stream_table('department_tree');

DROP TABLE employees;
DROP TABLE departments;
```

`drop_stream_table` atomically removes in a single transaction:
- The storage table (e.g., `public.department_stats`)
- CDC triggers on source tables (removed only if no other stream table references the same source)
- Change buffer tables in `pgtrickle_changes`
- Catalog entries in `pgtrickle.pgt_stream_tables`

---

---

## Monitoring Quick Reference

pg_trickle ships several built-in monitoring functions and a ready-made
Prometheus/Grafana stack. Here are the five most useful functions for
day-to-day operations.

### Stream Table Status

```sql
-- Overview of all stream tables: status, staleness, last refresh time, errors
SELECT name, status, staleness, last_refresh_at, last_error
FROM pgtrickle.pgt_status();
```

### Health Check

```sql
-- Run all built-in health checks; returns severity (OK/WARNING/CRITICAL) per check
SELECT check_name, severity, detail FROM pgtrickle.health_check();
```

### Change Buffer Sizes

```sql
-- Show CDC buffer row counts per source table — useful for spotting backlogs
SELECT * FROM pgtrickle.change_buffer_sizes();
```

### Dependency Tree

```sql
-- Visualize the DAG: which stream tables depend on what
SELECT * FROM pgtrickle.dependency_tree();
```

### Fuse Status

```sql
-- Check circuit breaker state for all stream tables
SELECT * FROM pgtrickle.fuse_status();
```

### Prometheus & Grafana

For production monitoring, pg_trickle ships a ready-made observability stack
in the `monitoring/` directory:

```bash
cd monitoring && docker compose up
```

This starts PostgreSQL + postgres_exporter + Prometheus + Grafana with
pre-configured dashboards and alerting rules. Grafana is available at
`http://localhost:3000` (admin/admin). See the [monitoring README](https://github.com/trickle-labs/pg-trickle/blob/main/monitoring/README.md)
for the full list of exported metrics and alert conditions.

**Key Prometheus metrics:**

| Metric | Description |
|--------|-------------|
| `pgtrickle_refresh_total` | Cumulative refresh count per table |
| `pgtrickle_refresh_duration_seconds` | Last refresh duration per table |
| `pgtrickle_staleness_seconds` | Seconds since last successful refresh |
| `pgtrickle_consecutive_errors` | Current error streak per table |
| `pgtrickle_cdc_buffer_rows` | Pending change buffer rows per source table |

**Pre-configured alerts:** staleness > 5 min, ≥3 consecutive failures, table
SUSPENDED, CDC buffer > 1 GB, scheduler down, high refresh duration.

---

## Summary: What You Learned

| Concept | What you saw |
|---------|-------------|
| **Stream tables** | Tables defined by a SQL query that stay automatically up to date |
| **CDC triggers** | Lightweight change capture in the same transaction — no logical replication or polling required |
| **DAG scheduling** | Stream tables can depend on other stream tables; refreshes run in topological order, schedules propagate upstream via `CALCULATED` mode |
| **Algebraic IVM** | Delta queries that process only changed rows — O(changes) regardless of table size |
| **Semi-naive / DRed** | Incremental strategies for `WITH RECURSIVE` — INSERT uses semi-naive, DELETE/UPDATE uses Delete-and-Rederive |
| **IMMEDIATE mode** | Synchronous in-transaction IVM — stream tables updated within the same transaction as your DML, always consistent |
| **TopK** | `ORDER BY … LIMIT N` queries store exactly N rows, refreshed via scoped recomputation |
| **Diamond consistency** | Atomic refresh groups for diamond-shaped dependency graphs via `diamond_consistency = 'atomic'` |
| **Downstream propagation** | A single base table write cascades through an entire chain of stream tables, automatically, in the right order |
| **Trigger-based CDC** | Lightweight row-level triggers by default (no WAL configuration needed); optional transition to WAL-based capture via `pg_trickle.cdc_mode = 'auto'` |
| **Parallel refresh** | Independent stream tables refresh concurrently in dynamic background workers via `pg_trickle.parallel_refresh_mode = 'on'` (default off) |
| **auto_backoff** | Scheduler automatically stretches effective interval when refresh cost exceeds 95% of the schedule window, capped at 8× (on by default) |
| **PgBouncer compatibility** | Set `pooler_compatibility_mode => true` per stream table to work behind transaction-mode connection poolers |
| **Monitoring** | `pgt_status()`, `health_check()`, `dependency_tree()`, `pg_stat_stream_tables`, and more for freshness, timing, and error history |

The key takeaway: you write to base tables — **pg_trickle does the rest**. Data flows downstream automatically, each layer doing the minimum work proportional to what changed, in dependency order.

---

## Troubleshooting

### Stream table is stale / not refreshing

Check the status view first:

```sql
SELECT name, status, last_error, last_refresh_at, staleness FROM pgtrickle.pgt_status();
```

A `status` of `ERROR` means the last refresh failed. `last_error` contains the message. Fix the underlying issue (e.g., a dropped column referenced in the query) then call:

```sql
SELECT pgtrickle.refresh_stream_table('your_table');
```

For a broader health check:

```sql
SELECT check_name, severity, detail FROM pgtrickle.health_check();
```

### Change buffer growing large

If a stream table has `status = 'PAUSED'` or refreshes are falling behind:

```sql
SELECT * FROM pgtrickle.change_buffer_sizes();  -- find large buffers
```

Large buffers are normal under heavy load — `auto_backoff` slows the schedule to avoid CPU runaway and will self-correct once throughput stabilises. If a buffer stays large indefinitely, check `last_error` in `pgt_status()` for a blocked refresh.

### CDC triggers missing after restore / point-in-time recovery

PITR restores the heap table but not the triggers if the extension was installed after the base backup. Verify:

```sql
SELECT * FROM pgtrickle.trigger_inventory();  -- expected vs installed triggers
```

Any missing trigger can be reinstalled with:

```sql
SELECT pgtrickle.repair_stream_table('your_table');
```

---

## Deployment Best Practices

Once you've built your stream tables interactively, you'll want to deploy them
reliably — via SQL migration scripts, dbt, or GitOps pipelines.

### Kubernetes Deployment (CloudNativePG)

pg_trickle integrates natively with [CloudNativePG](https://cloudnative-pg.io/)
using **Image Volume Extensions** (Kubernetes 1.33+). The extension is packaged
as a scratch-based OCI image containing only the `.so`, `.control`, and `.sql`
files — no custom PostgreSQL image required.

#### Prerequisites

- Kubernetes 1.33+ with the `ImageVolume` feature gate enabled
- CloudNativePG operator 1.28+
- pg_trickle extension image pushed to your cluster registry

#### Quick Start

1. **Deploy the Cluster** with the extension mounted as an Image Volume:

```yaml
# cnpg/cluster-example.yaml (abridged)
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: pg-trickle-demo
spec:
  instances: 3
  imageName: ghcr.io/cloudnative-pg/postgresql:18
  postgresql:
    shared_preload_libraries:
      - pg_trickle
    extensions:
      - name: pg-trickle
        image:
          reference: ghcr.io/<owner>/pg_trickle-ext:<version>
    parameters:
      max_worker_processes: "8"
```

2. **Create the extension** declaratively with a CNPG Database resource:

```yaml
# cnpg/database-example.yaml
apiVersion: postgresql.cnpg.io/v1
kind: Database
metadata:
  name: pg-trickle-app
spec:
  name: app
  owner: app
  cluster:
    name: pg-trickle-demo
  extensions:
    - name: pg_trickle
```

3. **Apply both resources:**

```bash
kubectl apply -f cnpg/cluster-example.yaml
kubectl apply -f cnpg/database-example.yaml
```

Full example manifests are in the [`cnpg/`](https://github.com/trickle-labs/pg-trickle/tree/main/cnpg) directory.

#### Health Monitoring

CNPG manages PostgreSQL liveness/readiness probes via its instance manager.
For pg_trickle-specific health, use the built-in health check function:

```sql
-- Run against the primary or any replica:
SELECT * FROM pgtrickle.health_check();
```

This returns rows for scheduler status, error/suspended tables, stale tables,
CDC buffer growth, WAL slot lag, and worker pool utilization. Integrate it into
your monitoring stack:

- **Prometheus:** Use the CNPG monitoring integration to expose
  `pgtrickle.health_check()` results as custom metrics
- **Kubernetes CronJob:** Schedule periodic health checks and alert via your
  existing alerting pipeline

#### Probe Configuration

The example manifests include probe settings tuned for pg_trickle workloads:

```yaml
probes:
  startup:
    periodSeconds: 10
    failureThreshold: 60     # 10 min for shared_preload_libraries init
  liveness:
    periodSeconds: 10
    failureThreshold: 6      # 60s before restart
  readiness:
    type: streaming
    maximumLag: 64Mi         # replicas must be streaming before serving reads
```

**Why `readiness: streaming`?** Stream tables are readable on replicas, but
a lagging replica serves stale stream table data. The `maximumLag` setting
ensures replicas are caught up before receiving traffic.

#### Failover Behavior

When the primary pod fails and CNPG promotes a replica:

- **Scheduler:** The new primary starts the pg_trickle scheduler background
  worker automatically (registered via `shared_preload_libraries`)
- **Stream tables:** All stream table definitions are stored in the
  `pgtrickle.pgt_stream_tables` catalog table, which is replicated to all
  replicas. The promoted replica has the complete catalog.
- **CDC triggers:** Trigger definitions are replicated as part of the WAL
  stream. The new primary's triggers fire normally on new writes.
- **Change buffers:** Uncommitted change buffer rows from in-flight
  transactions on the old primary are lost (standard PostgreSQL behavior).
  The next refresh cycle detects the gap and performs a FULL refresh to
  resynchronize.
- **Refresh frontiers:** Each stream table's last-refresh frontier is stored
  in the catalog. If the frontier is ahead of the available change buffer data
  (due to WAL replay lag), the scheduler falls back to FULL refresh once and
  then resumes DIFFERENTIAL.

No manual intervention is required after failover.

### Idempotent SQL Migrations

Use `create_or_replace_stream_table()` in your migration scripts. It's safe to
run on every deploy:

```sql
-- migrations/V003__stream_tables.sql
-- Creates if absent, updates if definition changed, no-op if identical.

SELECT pgtrickle.create_or_replace_stream_table(
    name         => 'employee_salaries',
    query        => 'SELECT e.id, e.name, d.name AS department, e.salary
                     FROM employees e JOIN departments d ON e.department_id = d.id',
    schedule     => '30s',
    refresh_mode => 'DIFFERENTIAL'
);

SELECT pgtrickle.create_or_replace_stream_table(
    name         => 'department_stats',
    query        => 'SELECT department, COUNT(*) AS headcount, AVG(salary) AS avg_salary
                     FROM employee_salaries GROUP BY department',
    schedule     => '30s',
    refresh_mode => 'DIFFERENTIAL'
);
```

If someone changes the query in a later migration, `create_or_replace` detects
the difference and migrates the storage table in place — no need to drop and
recreate.

### dbt Integration

With the [dbt-pgtrickle](https://github.com/trickle-labs/pg-trickle/tree/main/dbt-pgtrickle)
package, stream tables are just dbt models with `materialized='stream_table'`:

```sql
-- models/department_stats.sql
{{ config(
    materialized='stream_table',
    schedule='30s',
    refresh_mode='DIFFERENTIAL'
) }}

SELECT department, COUNT(*) AS headcount, AVG(salary) AS avg_salary
FROM {{ ref('employee_salaries') }}
GROUP BY department
```

Every `dbt run` calls `create_or_replace_stream_table()` under the hood,
so deployments are always idempotent.

---

## Day 2 Operations

Once your stream tables are running in production, pg_trickle can monitor
itself using its own stream tables — a technique called *self-monitoring*.

### Enabling Self-Monitoring

```sql
-- Create all five monitoring stream tables (idempotent, safe to repeat).
SELECT pgtrickle.setup_self_monitoring();

-- Check what was created.
SELECT * FROM pgtrickle.self_monitoring_status();
```

This creates five stream tables in the `pgtrickle` schema:

| Stream Table | Purpose |
|-------------|---------|
| `df_efficiency_rolling` | Rolling-window refresh statistics (replaces manual `refresh_efficiency()` calls) |
| `df_anomaly_signals` | Detects duration spikes, error bursts, mode oscillation |
| `df_threshold_advice` | Recommends threshold adjustments based on multi-cycle analysis |
| `df_cdc_buffer_trends` | Tracks CDC buffer growth rates per source table |
| `df_scheduling_interference` | Detects concurrent refresh overlap patterns |

### Checking Recommendations

After at least 10–20 refresh cycles have accumulated:

```sql
-- Which stream tables have poorly calibrated thresholds?
SELECT pgt_name, current_threshold, recommended_threshold, confidence, reason
FROM pgtrickle.df_threshold_advice
WHERE confidence IN ('HIGH', 'MEDIUM')
  AND abs(recommended_threshold - current_threshold) > 0.05;

-- Are any stream tables experiencing anomalies?
SELECT pgt_name, duration_anomaly, recent_failures
FROM pgtrickle.df_anomaly_signals
WHERE duration_anomaly IS NOT NULL OR recent_failures >= 2;
```

### Automatic Threshold Tuning

To let pg_trickle automatically apply threshold recommendations:

```sql
SET pg_trickle.self_monitoring_auto_apply = 'threshold_only';
```

This applies changes only when confidence is HIGH and the recommended threshold
differs by more than 5%. Changes are rate-limited to once per 10 minutes per
stream table and logged with `initiated_by = 'SELF_MONITOR'`.

### Visualizing the DAG

```sql
-- See the full refresh graph (Mermaid format, paste into any Mermaid renderer).
SELECT pgtrickle.explain_dag();
```

Dog-feeding STs appear in green, user STs in blue, suspended in red.

### Disabling Self-Monitoring

```sql
SELECT pgtrickle.teardown_self_monitoring();
```

This drops all monitoring stream tables. User stream tables are never affected.
The control plane continues operating identically without self-monitoring.

---

## What's Next?

- **[SQL_REFERENCE.md](SQL_REFERENCE.md)** — Full API reference for all functions, views, and configuration
- **[ARCHITECTURE.md](ARCHITECTURE.md)** — Deep dive into the system architecture and data flow
- **[DVM_OPERATORS.md](DVM_OPERATORS.md)** — How each SQL operator is differentiated for incremental maintenance
- **[CONFIGURATION.md](CONFIGURATION.md)** — GUC variables for tuning schedule, concurrency, and cleanup behavior
- **[Flyway & Liquibase Integration](integrations/flyway-liquibase.md)** — Migration patterns for Flyway and Liquibase
- **[ORM Integration](integrations/orm.md)** — SQLAlchemy and Django ORM patterns for stream tables
- **[What Happens on INSERT](tutorials/WHAT_HAPPENS_ON_INSERT.md)** — Detailed trace of a single INSERT through the entire pipeline
- **[What Happens on UPDATE](tutorials/WHAT_HAPPENS_ON_UPDATE.md)** — How UPDATEs are split into D+I, group key changes, and net-effect computation
- **[What Happens on DELETE](tutorials/WHAT_HAPPENS_ON_DELETE.md)** — Reference counting, group deletion, and INSERT+DELETE cancellation
- **[What Happens on TRUNCATE](tutorials/WHAT_HAPPENS_ON_TRUNCATE.md)** — Why TRUNCATE bypasses triggers and how to recover
- **[dbt Getting Started example](https://github.com/trickle-labs/pg-trickle/tree/main/examples/dbt_getting_started/)** — Everything above, expressed as dbt models and seeds with a one-command Docker runner
