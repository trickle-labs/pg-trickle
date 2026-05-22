# 5-Minute Quickstart

The shortest possible introduction to pg_trickle. By the end of this
page you will have created a self-maintaining table, watched it update
in real time, and dropped it again — without leaving `psql`.

## Prerequisites

| Requirement | Notes |
|---|---|
| PostgreSQL 18.x | Required — pg_trickle targets PG 18 |
| `shared_preload_libraries = 'pg_trickle'` | Must **restart** PostgreSQL after changing (reload is not sufficient) |
| `max_worker_processes ≥ 32` | The default of 8 is exhausted quickly with multiple databases; silent scheduler failures result |
| `psql` (or any SQL client) | |

> Already using the Docker image or playground? All of these are pre-configured — skip straight to **Step 2**.
> For a full install guide see [Installation](installation.md).

> Prefer to **see it first**? Run the
> [playground](PLAYGROUND.md) (`cd playground && docker compose up -d`)
> for a pre-loaded environment, or pull the prebuilt image:
> ```bash
> docker run --rm -e POSTGRES_PASSWORD=secret -p 5432:5432 \
>   ghcr.io/trickle-labs/pg_trickle:latest
> ```
> Then connect with `psql postgres://postgres:secret@localhost:5432/postgres`
> and skip to **Step 2** below.

---

## Step 1 — Install the extension

If you already have a PostgreSQL 18 server with pg_trickle installed
(via the playground, the Docker image, or a manual install — see
[Installation](installation.md) for full options), skip this step.

Otherwise, the shortest path on a developer machine is the prebuilt
Docker image — one command, no configuration:

```bash
docker run --rm -e POSTGRES_PASSWORD=secret -p 5432:5432 \
  ghcr.io/trickle-labs/pg_trickle:latest
```

Connect with `psql`:

```bash
psql postgres://postgres:secret@localhost:5432/postgres
```

---

## Step 2 — Enable the extension

```sql
CREATE EXTENSION IF NOT EXISTS pg_trickle;
```

That's all the configuration you need. The extension auto-discovers
every database where it's installed and starts a per-database
scheduler.

---

## Step 3 — Create a source table

```sql
CREATE TABLE orders (
    id      SERIAL PRIMARY KEY,
    region  TEXT     NOT NULL,
    amount  NUMERIC  NOT NULL
);

INSERT INTO orders (region, amount) VALUES
    ('US',   100),
    ('EU',   200),
    ('US',   300),
    ('APAC',  50);
```

This is a perfectly ordinary table. You will write to it the normal way.

---

## Step 4 — Create a stream table

```sql
SELECT pgtrickle.create_stream_table(
    name     => 'revenue_by_region',
    query    => $$
        SELECT region,
               SUM(amount) AS total,
               COUNT(*)    AS order_count
        FROM orders
        GROUP BY region
    $$,
    schedule => '1s'
);
```

What just happened:

1. pg_trickle parsed your query and built an internal operator tree.
2. It created a new table `revenue_by_region` with the right columns.
3. It installed lightweight `AFTER` triggers on `orders` to capture
   changes.
4. It ran an initial full refresh, populating the new table.
5. It registered a 1-second refresh schedule.

Query the stream table — it's already populated:

```sql
SELECT * FROM revenue_by_region ORDER BY region;
```

```
 region | total | order_count
--------+-------+-------------
 APAC   |    50 |           1
 EU     |   200 |           1
 US     |   400 |           2
```

---

## Step 5 — Watch it update

Insert a new order:

```sql
INSERT INTO orders (region, amount) VALUES ('US', 999);
```

Wait one second (or call `SELECT pgtrickle.refresh_stream_table('revenue_by_region')`
to refresh immediately):

```sql
SELECT * FROM revenue_by_region WHERE region = 'US';
```

```
 region | total | order_count
--------+-------+-------------
 US     |  1399 |           3
```

Only the `US` group was recomputed — the other regions were not
touched at all. That is differential refresh in action.

---

## Step 6 — Look around

A few useful built-ins worth knowing about right away:

```sql
-- Status of all stream tables in this database
SELECT * FROM pgtrickle.pgt_status();

-- A one-shot health triage (returns rows only when something is wrong)
SELECT * FROM pgtrickle.health_check() WHERE severity != 'OK';

-- See what delta SQL pg_trickle would run on the next refresh
SELECT pgtrickle.explain_st('revenue_by_region');
```

---

## Step 7 — Clean up

```sql
SELECT pgtrickle.drop_stream_table('revenue_by_region');
DROP TABLE orders;
```

This removes the stream table, its catalog entries, and the CDC
triggers on `orders`.

---

## Where to go next

| If you want to… | Read |
|---|---|
| See a multi-table tutorial with chains and aggregates | [15-Minute Tutorial](GETTING_STARTED.md#chapter-2-joins-aggregates--chains) |
| Walk through every feature in depth | [In-Depth Tour](GETTING_STARTED.md) |
| Browse common patterns and example apps | [Use Cases](USE_CASES.md) · [Patterns](PATTERNS.md) |
| Understand how it works underneath | [Architecture](ARCHITECTURE.md) |
| Look up a function, GUC, or operator | [SQL Reference](SQL_REFERENCE.md) · [Configuration](CONFIGURATION.md) |
| Deploy it to production | [Pre-Deployment Checklist](PRE_DEPLOYMENT.md) |
| Decode a piece of jargon | [Glossary](GLOSSARY.md) |
