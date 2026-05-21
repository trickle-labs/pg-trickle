# Demo: Multi-Engine Leaderboard

*A live game leaderboard maintained once by pg_trickle, queryable from both PostgreSQL and DuckLake — operational and analytical, no duplication*

---

## What you'll build

You'll run a real-time game leaderboard that does two things simultaneously:

1. **Live leaderboard in Grafana** — player rankings refresh every 5 seconds,
   reading directly from PostgreSQL.
2. **Historical analytics in DuckDB** — every delta is written to MinIO as
   Parquet via the DuckLake sink. A JupyterLab notebook queries the ranking
   history over time.

Both views come from the **same two stream tables** maintained by pg_trickle.
There's no separate ETL job, no data duplication, and no consistency lag between
the operational and analytical views.

**Time to first leaderboard: under 5 minutes.**

---

## Background: Why is this interesting?

The classic problem in data engineering is keeping an OLTP system (fast writes,
low latency) and an analytics system (historical queries, large scans) in sync.
The usual solution is a CDC pipeline (Debezium) feeding a Kafka topic, consumed
by a stream processor, writing to a data lake, eventually queryable from a BI
tool. That's 4–6 systems.

This demo shows you can collapse all of that into PostgreSQL + pg_trickle:

- **PostgreSQL** is the OLTP source and the operational query layer.
- **pg_trickle stream tables** maintain the leaderboard incrementally using only
  the score events that arrived in the last 5 seconds.
- **DuckLake sink** appends each delta as a Parquet file to MinIO.
- **DuckDB** queries the Parquet files for time-travel analytics.

The `RANK()` window function inside the stream table query is particularly
noteworthy: pg_trickle's DVM engine handles differential refresh of ranked
results correctly, so the rank column stays accurate without a full recompute.

---

## Prerequisites

- **Docker Engine 24+** and **Docker Compose v2**
  Verify: `docker compose version`
- **4 GB free RAM**
- No local installation of pg_trickle or DuckDB needed — everything runs in
  containers

---

## Architecture

```
Python score generator
  │  50 score events/second
  │  columns: player_id, player_name, game_id, score
  ▼
PostgreSQL 18 + pg_trickle  (port 5432, db: postgres)
  │  game_scores table  ← trigger-based CDC
  │
  ├─ stream table: top_players
  │    SELECT player_id, player_name,
  │           SUM(score) AS total_score,
  │           RANK() OVER (ORDER BY SUM(score) DESC) AS rank
  │    5-second DIFFERENTIAL refresh
  │    DuckLake sink → MinIO: s3://pg-trickle-demo/top_players/
  │
  └─ stream table: scores_by_game
       SELECT game_id, SUM(score), COUNT(DISTINCT player_id)
       5-second DIFFERENTIAL refresh
       DuckLake sink → MinIO: s3://pg-trickle-demo/scores_by_game/
       ▼
  MinIO (port 9000/9001)    ← Parquet delta files, one per refresh cycle
       ▼
  DuckLake catalog          ← ducklake_snapshot, ducklake_view
       ▼
  ┌─────────────────────────────┬─────────────────────────────────┐
  │  Grafana (port 3000)        │  JupyterLab (port 8888)         │
  │  Live leaderboard           │  Historical ranking analysis     │
  │  Reads from PostgreSQL      │  Reads from MinIO via DuckDB     │
  └─────────────────────────────┴─────────────────────────────────┘
```

---

## Step 1: Start the demo

```bash
cd demos/ducklake-leaderboard
docker compose up
```

Five containers start:

| Container | Port | Purpose |
|-----------|------|---------|
| `postgres` | 5432 | PostgreSQL 18 + pg_trickle |
| `minio` | 9000 / 9001 | S3-compatible object storage |
| `minio-setup` | — | One-shot container that creates the `pg-trickle-demo` bucket |
| `generator` | — | Python script producing 50 score events/s |
| `grafana` | 3000 | Live leaderboard dashboard |

> **Note:** this demo does not include a JupyterLab container in the compose
> file. To run the analytics notebook, install DuckDB locally:
> `pip install duckdb jupyterlab` and open `notebooks/leaderboard_analysis.ipynb`.

Wait until all containers are healthy. You'll see:
```
postgres  | LOG: database system is ready to accept connections
```

---

## Step 2: Watch the live leaderboard in Grafana

Open **http://localhost:3000** in your browser.

- Login: `admin` / `admin`
- Navigate to **Dashboards → Multi-Engine Leaderboard**

The dashboard refreshes every 5 seconds. Watch player rankings change in
real time as the score generator inserts events.

---

## Step 3: Connect to PostgreSQL and explore

In a second terminal:

```bash
psql postgresql://postgres:postgres@localhost:5432/postgres
```

See the stream tables:

```sql
SELECT table_name, refresh_mode, schedule, sink, ducklake_sink_path
FROM pgtrickle.pgt_stream_tables
ORDER BY table_name;
```

Query the current leaderboard:

```sql
SELECT rank, player_name, total_score, games_played
FROM top_players
ORDER BY rank
LIMIT 10;
```

Check that ranks are updating correctly after each refresh cycle:

```sql
-- Watch this query — rank 1 may change as scores accumulate
SELECT rank, player_name, total_score
FROM top_players
WHERE rank <= 3
ORDER BY rank;
```

See the provenance trail — every Parquet delta that was written to MinIO:

```sql
SELECT
    stream_table_name,
    ducklake_snapshot_id,
    delta_row_count,
    written_at
FROM pgtrickle.pgt_ducklake_provenance
ORDER BY written_at DESC
LIMIT 10;
```

---

## Step 4: Browse Parquet files in MinIO

Open the MinIO console at **http://localhost:9001**.

- Login: `minioadmin` / `minioadmin`
- Navigate to `pg-trickle-demo` → `top_players/`

A new `.parquet` file appears every 5 seconds (whenever there are score changes).
Each file is a delta — only the player rows whose total score changed in that
cycle.

---

## Step 5: Understand the stream table SQL

The leaderboard is defined by a single `create_stream_table` call
(pre-loaded by `postgres/init.sql`):

```sql
SELECT pgtrickle.create_stream_table(
    'top_players',
    query => $$
        SELECT
            player_id,
            player_name,
            SUM(score)                             AS total_score,
            COUNT(*)                               AS games_played,
            RANK() OVER (ORDER BY SUM(score) DESC) AS rank
        FROM game_scores
        GROUP BY player_id, player_name
    $$,
    schedule           => '5s',
    refresh_mode       => 'DIFFERENTIAL',
    sink               => 'ducklake',
    ducklake_sink_path => 's3://pg-trickle-demo/top_players/'
);
```

**Why `RANK()` works in a stream table:**
`RANK()` is a window function applied over the aggregated result. pg_trickle's
DVM engine computes the `SUM(score) GROUP BY player_id` differentially (only
over new score events), then applies the `RANK()` over the full refreshed
result set. This means ranks are always globally consistent — no partial
recompute artifacts.

---

## Step 6: Run the analytics notebook (optional)

If you have DuckDB and JupyterLab installed locally:

```bash
pip install duckdb jupyterlab
jupyter lab notebooks/leaderboard_analysis.ipynb
```

In the notebook, attach to the DuckLake catalog and query the ranking history:

```python
import duckdb

con = duckdb.connect()
con.execute("""
    ATTACH 'ducklake:postgresql://postgres:postgres@localhost:5432/postgres'
        AS lake (TYPE DUCKLAKE)
""")

# Who was rank 1 at each snapshot?
result = con.execute("""
    SELECT
        s.snapshot_time,
        p.player_name,
        p.total_score,
        p.rank
    FROM lake.top_players p
    JOIN ducklake_snapshot s USING (snapshot_id)
    WHERE p.rank = 1
    ORDER BY s.snapshot_time
""").df()
print(result)
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| Grafana shows "No data" | Stream tables not yet populated | Wait 15 s; check `docker compose logs postgres` |
| MinIO console is empty | `minio-setup` container failed | Run `docker compose up minio-setup` to retry bucket creation |
| `pgt_ducklake_provenance` has no rows | S3 write failed | Check `docker compose logs postgres` for S3 errors |
| Rankings not changing | Score generator not running | Check `docker compose logs generator` |
| Port 3000 conflict | Another service using port 3000 | The leaderboard compose doesn't expose a `GRAFANA_PORT` env var; edit `docker-compose.yml` to change `3000:3000` |

---

## What you've built

- A **live ranked leaderboard** that updates in under 5 seconds using only
  PostgreSQL — no Kafka, no stream processor, no separate OLAP database.
- **Bidirectional access**: the same data is queryable operationally from
  PostgreSQL (sub-millisecond reads) and analytically from DuckDB via the
  Parquet history on MinIO.
- **RANK() inside a stream table**: window functions work correctly under
  differential refresh — each cycle produces a fully consistent ranked result.

---

## Stop and clean up

```bash
docker compose down -v
```

---

## Related resources

- [Tutorial 3: The Modern Data Stack in One Box](../../docs/tutorial-modern-data-stack-one-box.md)
- [Tutorial 4: Streaming PostgreSQL to a Data Lake](../../docs/tutorial-streaming-postgres-to-data-lake.md)
- [Blog: Why pg_trickle + DuckLake Is the Missing Piece for Lakehouse IVM](../../blog/ducklake-ivm-missing-piece.md)


A self-contained `docker compose up` demo that runs a real-time game leaderboard
simultaneously maintained as a pg_trickle stream table in PostgreSQL **and**
published as a DuckLake table on MinIO.

The same data — maintained once by pg_trickle — is available to both operational
and analytical workloads without duplication.

---

## What It Does

```
Score generator (Python)
  │  50 score events/second
  ▼
PostgreSQL 18 + pg_trickle
  ├─ stream table: top_players   (5s DIFFERENTIAL refresh)
  │    └─ DuckLake sink → MinIO Parquet deltas
  ├─ stream table: scores_by_game (5s DIFFERENTIAL refresh)
  │    └─ DuckLake sink → MinIO Parquet deltas
  └─ DuckLake view registration: top_players, scores_by_game
  ▼
┌─────────────────────┬───────────────────────────────┐
│  Grafana (port 3000) │  DuckDB notebook (port 8888)  │
│  Live leaderboard    │  Historical analytics          │
│  (reads from PG)     │  (reads from DuckLake/MinIO)   │
└─────────────────────┴───────────────────────────────┘
```

---

## Quick Start

```bash
cd demos/ducklake-leaderboard
docker compose up
```

- **Live leaderboard**: http://localhost:3000 (Grafana, admin/admin)
- **Analytics notebook**: http://localhost:8888 (JupyterLab, token: `demo`)
- **MinIO console**: http://localhost:9001 (minioadmin/minioadmin)

---

## What to Watch

1. **Grafana**: The top-players panel refreshes every 5 seconds. Watch scores
   change in real time as the generator inserts events.

2. **MinIO console**: Navigate to the `pg-trickle-demo` bucket. New Parquet
   files appear under `top_players/` and `scores_by_game/` every 5 seconds.

3. **JupyterLab**: Open `leaderboard_analysis.ipynb` and run all cells. DuckDB
   queries the historical Parquet files and plots ranking trends over time.

---

## Services

| Service | Port | Purpose |
|---------|------|---------|
| PostgreSQL 18 + pg_trickle | 5432 | Stream tables, DuckLake catalog |
| MinIO | 9000/9001 | Object storage (S3-compatible) |
| Grafana | 3000 | Live leaderboard dashboard |
| JupyterLab | 8888 | Historical analytics (DuckDB) |
| Score generator | — | Python script producing 50 events/s |

---

## Key SQL

The leaderboard stream table:

```sql
SELECT pgtrickle.create_stream_table(
    'top_players',
    query => $$
        SELECT
            player_id,
            player_name,
            SUM(score) AS total_score,
            COUNT(*) AS games_played,
            RANK() OVER (ORDER BY SUM(score) DESC) AS rank
        FROM game_scores
        GROUP BY player_id, player_name
    $$,
    schedule           => '5s',
    refresh_mode       => 'DIFFERENTIAL',
    sink               => 'ducklake',
    ducklake_sink_path => 's3://pg-trickle-demo/top_players/'
);
```

After creation, pg_trickle automatically registers `top_players` as a native
view in the DuckLake catalog — so the JupyterLab notebook can query it from
DuckDB with a simple `SELECT * FROM lake.top_players`.

---

## Provenance trail

Query the lineage from PostgreSQL:

```sql
SELECT
    stream_table_name,
    ducklake_snapshot_id,
    delta_row_count,
    written_at
FROM pgtrickle.pgt_ducklake_provenance
ORDER BY written_at DESC
LIMIT 10;
```
