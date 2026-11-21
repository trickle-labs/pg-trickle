# Tutorial 3: The Modern Data Stack in One Box

Run PostgreSQL, pg_trickle, DuckLake, MinIO, and DuckDB on one machine, without deploying a separate message broker or stream processor.

This tutorial uses the maintained [OLTP-to-Lake Loop demo](../demos/ducklake-oltp-lake/README.md) as its runnable implementation. That README is the source of truth for the Compose services, ports, credentials, and troubleshooting instructions.

## Prerequisites

- Docker Engine with Docker Compose v2
- At least 4 GB of available RAM
- An internet connection for the initial container build

## 1. Start the stack

From the repository root:

```sh
cd demos/ducklake-oltp-lake
docker compose up
```

The demo starts PostgreSQL 18 with pg_trickle, MinIO for object storage, a synthetic order generator, and JupyterLab with DuckDB. The first startup builds the extension image.

## 2. Explore the operational data

In another terminal, connect to PostgreSQL:

```sh
psql postgresql://postgres:postgres@localhost:5432/postgres
```

```sql
SELECT region, minute, total_revenue, order_count
FROM revenue_by_region
ORDER BY minute DESC, region
LIMIT 20;
```

The generator inserts orders; pg_trickle maintains the aggregate in a stream table with differential refresh.

## 3. Trace the lake updates

```sql
SELECT stream_table_name, ducklake_snapshot_id, delta_row_count, written_at
FROM pgtrickle.pgt_ducklake_provenance
ORDER BY written_at DESC
LIMIT 10;
```

Open the MinIO console at http://localhost:9001 to inspect the Parquet delta files. See the [demo README](../demos/ducklake-oltp-lake/README.md#step-4-explore-the-parquet-files-in-minio) for the demo credentials and bucket layout.

## 4. Query with DuckDB

Open JupyterLab at http://localhost:8888/lab?token=demo and run `oltp_lake_analysis.ipynb`. The notebook connects to PostgreSQL and explores the Parquet history with DuckDB.

The operational stream table and the lake's historical snapshots serve different query needs, but both originate from the same PostgreSQL writes.

## 5. Clean up

From `demos/ducklake-oltp-lake`:

```sh
docker compose down -v
```

**Caution:** `-v` deletes the demo's database and MinIO volumes.

For a second example with Grafana and a ranked leaderboard, see the [Multi-Engine Leaderboard demo](../demos/ducklake-leaderboard/README.md).
