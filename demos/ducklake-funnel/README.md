# Demo: The Five-Second Funnel

*A live conversion funnel dashboard powered by pg_trickle — no Kafka, no Flink, no separate stream processor*

---

## What you'll build

You'll run a complete real-time analytics pipeline that tracks how users move
through an e-commerce funnel (visit → add-to-cart → purchase) and displays live
conversion rates in a Grafana dashboard that refreshes every 5 seconds.

A Python load generator streams 50 synthetic events per second into PostgreSQL.
Two pg_trickle stream tables aggregate those events differentially — processing
only the rows that changed, not the entire history — and Grafana reads the
results directly from PostgreSQL.

**Time to first dashboard: under 5 minutes.**

---

## Background: What is a conversion funnel?

A **conversion funnel** tracks how many users complete each stage of a
multi-step process. In e-commerce:

1. **Visit** — user lands on a product page
2. **Add to cart** — user adds the product to their cart
3. **Purchase** — user completes the checkout

The *conversion rate* at each stage (e.g. 30% of visitors add to cart, 12% of
visitors purchase) tells you where users are dropping off and which products
perform best.

Traditionally, computing this in near-real-time requires a Kafka + Flink
pipeline or a heavyweight OLAP database. This demo shows you can get the same
result with a single `docker compose up` and two SQL statements.

---

## Prerequisites

- **Docker Engine 24+** and **Docker Compose v2**
  Verify: `docker compose version`
- **4 GB free RAM** for the containers
- No pg_trickle installation needed — it runs inside the Docker container

---

## Architecture

```
Python load generator
  │  50 events/second (configurable)
  │  event_type: 'visit' | 'add_to_cart' | 'purchase'
  ▼
PostgreSQL 18 + pg_trickle  (port 5432, db: lake_demo)
  │  events_bridge table  ← trigger-based CDC
  │
  ├─ stream table: revenue_by_minute
  │    SELECT date_trunc('minute', ...), SUM(amount)
  │    5-second DIFFERENTIAL refresh
  │
  └─ stream table: funnel_by_product
       SELECT product_id,
              COUNT(*) FILTER (WHERE event_type = 'visit') AS visits,
              COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS carts,
              COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases
       5-second DIFFERENTIAL refresh
       ▼
  MinIO (port 9000)       ← optional Parquet sink for historical analysis
       ▼
  Grafana (port 3000)
       Live panels: funnel bars, revenue time series, top products
       Auto-refresh: 5 seconds
```

---

## Step 1: Start the demo

```bash
cd demos/ducklake-funnel
docker compose up
```

Four containers start:

| Container | Port | Purpose |
|-----------|------|---------|
| `ducklake_funnel_postgres` | 5432 | PostgreSQL 18 + pg_trickle |
| `ducklake_funnel_minio` | 9000 / 9001 | S3-compatible object storage |
| `ducklake_funnel_generator` | — | Python script producing 50 events/s |
| `ducklake_funnel_grafana` | 3000 | Live dashboard |

Wait until you see:
```
ducklake_funnel_postgres  | LOG: database system is ready to accept connections
```

---

## Step 2: Open the Grafana dashboard

Go to **http://localhost:3000** in your browser.

- Login: `admin` / `admin`
- Navigate to **Dashboards → Five-Second Funnel**

You should see four panels updating every 5 seconds:

| Panel | What it shows |
|-------|--------------|
| **Conversion funnel** | Bar chart: visits → carts → purchases for all products |
| **Revenue per minute** | Time series of total purchase revenue |
| **Top 10 products by purchases** | Which products convert best |
| **Conversion rate by product** | `purchases / visits` ratio per product |

If the panels show "No data", wait 10 seconds — the first refresh cycle takes a
moment to populate the stream tables.

---

## Step 3: Connect to PostgreSQL and explore the stream tables

In a second terminal, connect to the database:

```bash
psql postgresql://postgres:postgres@localhost:5432/lake_demo
```

See the stream tables pg_trickle created:

```sql
SELECT table_name, refresh_mode, schedule, status
FROM pgtrickle.pgt_stream_tables
ORDER BY table_name;
```

Query the current funnel numbers directly:

```sql
SELECT
    product_id,
    visits,
    carts,
    purchases,
    ROUND(100.0 * purchases / NULLIF(visits, 0), 1) AS purchase_rate_pct
FROM funnel_by_product
ORDER BY purchases DESC
LIMIT 5;
```

Watch the refresh history to see exactly how many rows each cycle processed:

```sql
SELECT
    started_at,
    refresh_mode,
    delta_rows_in,
    delta_rows_out,
    duration_ms
FROM pgtrickle.pgt_refresh_history
WHERE pgt_id = (
    SELECT pgt_id FROM pgtrickle.pgt_stream_tables
    WHERE table_name = 'funnel_by_product'
)
ORDER BY started_at DESC
LIMIT 5;
```

Notice that `delta_rows_in` is always small — only the events from the last 5
seconds, not the entire history. This is DIFFERENTIAL refresh: O(Δ), not O(N).

---

## Step 4: See the stream table SQL

The funnel stream table is defined by this SQL (pre-loaded by
`postgres/init.sql`):

```sql
SELECT pgtrickle.create_stream_table(
    'funnel_by_product',
    query => $$
        SELECT
            product_id,
            COUNT(*) FILTER (WHERE event_type = 'visit')        AS visits,
            COUNT(*) FILTER (WHERE event_type = 'add_to_cart')  AS carts,
            COUNT(*) FILTER (WHERE event_type = 'purchase')     AS purchases
        FROM events_bridge
        GROUP BY product_id
    $$,
    schedule     => '5s',
    refresh_mode => 'DIFFERENTIAL'
);
```

The DVM engine translates this `GROUP BY` + `COUNT FILTER` into a differential
delta query. When new events arrive, it updates only the affected product rows
instead of recomputing all 20 products from scratch.

---

## Step 5: Change the load rate (optional)

Stop the compose stack (`Ctrl+C`), edit the load rate, and restart:

```bash
# .env file (create if it doesn't exist)
echo "EVENTS_PER_SECOND=200" > .env
docker compose up
```

Or set it inline:

```bash
EVENTS_PER_SECOND=200 docker compose up
```

Available variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `EVENTS_PER_SECOND` | `50` | Events inserted per second |
| `PRODUCT_COUNT` | `20` | Number of products in the catalogue |
| `USER_COUNT` | `500` | Number of simulated users |
| `GRAFANA_PORT` | `3000` | Grafana HTTP port |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| Grafana shows "No data" after 30 s | Stream tables not yet populated | Check `docker compose logs postgres` for errors |
| Can't reach http://localhost:3000 | Port 3000 already in use | Set `GRAFANA_PORT=3001` in `.env` |
| Postgres healthcheck fails | Slow machine | Increase the `retries` value in `docker-compose.yml` or wait longer |
| `funnel_by_product` is empty | Generator not running | Check `docker compose logs generator` |
| Dashboard not appearing in Grafana | Grafana provisioning failed | Restart with `docker compose restart grafana` |

---

## What you've built

- A **live conversion funnel** that updates every 5 seconds with zero
  infrastructure beyond PostgreSQL.
- **DIFFERENTIAL refresh in action**: each 5-second cycle processes only the
  events that arrived since the previous cycle — not the full event history.
- The same architecture scales: swap the synthetic generator for your real
  event stream, and the funnel dashboard works the same way.

---

## Stop and clean up

```bash
docker compose down -v
```

The `-v` flag removes the PostgreSQL and MinIO data volumes so the next
`docker compose up` starts completely fresh.

---

## Related resources

- [Tutorial 3: The Modern Data Stack in One Box](../../docs/tutorial-modern-data-stack-one-box.md)
- [Tutorial 4: Streaming PostgreSQL to a Data Lake](../../docs/tutorial-streaming-postgres-to-data-lake.md)
- [Blog: Real-Time Dashboards on Your Data Lake](../../blog/ducklake-real-time-dashboards.md)


A self-contained `docker compose up` demo that streams synthetic e-commerce
events into DuckLake, computes a live visit→add-to-cart→purchase funnel using
pg_trickle stream tables, and displays it in a Grafana dashboard that refreshes
every five seconds.

Designed to run on any developer laptop in under five minutes.

---

## What It Does

```
DuckDB load generator
  │  (writes 50 events/second to events_bridge via psql)
  ▼
PostgreSQL 18 + pg_trickle
  │  events_bridge table — trigger-based CDC, sub-millisecond
  ├─ stream table: revenue_by_minute   (5s schedule, DIFFERENTIAL)
  └─ stream table: funnel_by_product   (5s schedule, DIFFERENTIAL)
  ▼
Grafana
  │  live panels: funnel bars, revenue time series, top products
  │  auto-refresh: 5s
```

No Kafka. No Flink. No separate streaming infrastructure.

---

## Quick Start

```bash
cd demos/ducklake-funnel
docker compose up
```

Then open http://localhost:3000 in your browser.
- Username: `admin`
- Password: `admin`

The dashboard appears under **Dashboards → Five-Second Funnel**.

---

## Architecture

The demo uses four Docker containers:

| Container | Image | Purpose |
|-----------|-------|---------|
| `postgres` | `ghcr.io/trickle-labs/pg-trickle:latest` | PostgreSQL 18 + pg_trickle |
| `generator` | `python:3.12-alpine` (built in compose) | Load generator (50 events/s) |
| `grafana` | `grafana/grafana:latest` | Dashboard visualisation |
| `minio` | `minio/minio` | S3-compatible object storage (simulates DuckLake data path) |

The `postgres` container runs PostgreSQL with pg_trickle installed and initialised
with the stream tables. The `generator` container runs a Python script that
inserts synthetic events at configurable rates. Grafana reads from PostgreSQL
directly via the built-in PostgreSQL data source.

---

## Configuration

Edit `.env` to change default settings:

```bash
# Event generation rate (events per second, default 50)
EVENTS_PER_SECOND=50

# Number of products in the catalogue (default 20)
PRODUCT_COUNT=20

# Number of simulated users (default 500)
USER_COUNT=500

# Grafana port (default 3000)
GRAFANA_PORT=3000

# PostgreSQL port (default 5432)
POSTGRES_PORT=5432
```

---

## Included Grafana Panels

| Panel | Stream Table | Refresh |
|-------|-------------|---------|
| Revenue per minute | `revenue_by_minute` | 5s |
| Conversion funnel (all products) | `funnel_by_product` | 5s |
| Top 10 products by purchases | `funnel_by_product` | 5s |
| Conversion rate by product | `funnel_by_product` | 5s |
| Recent purchases (live feed) | `events_bridge` | 5s |

---

## Stopping the Demo

```bash
docker compose down -v
```

The `-v` flag removes the PostgreSQL data volume so the next `docker compose up`
starts fresh.

---

## What This Demonstrates

- **pg_trickle DIFFERENTIAL refresh:** The funnel numbers update within 5 seconds
  of each new event batch. Only the changed rows are processed — not the entire
  event history.
- **DuckLake integration pattern:** The `events_bridge` table mirrors the write
  pattern that DuckLake uses for inlined data. The same stream tables work
  without modification once the real DuckLake connector is in place.
- **Zero-infrastructure overhead:** Five containers, five minutes, a complete
  analytics pipeline. No message broker, no stream processor, no JVM.

---

## Related Resources

- [Tutorial: Real-Time Dashboards on Your Data Lake](../../blog/ducklake-real-time-dashboards.md)
- [Tutorial: The Modern Data Stack in One Box](../../blog/ducklake-modern-data-stack.md)
- [Blog: Why pg_trickle + DuckLake Is the Missing Piece for Lakehouse IVM](../../blog/ducklake-ivm-missing-piece.md)
