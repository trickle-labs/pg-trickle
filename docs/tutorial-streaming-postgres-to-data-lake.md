# Tutorial 4: Streaming PostgreSQL to a Data Lake without Kafka

This tutorial shows the DuckLake **sink** workflow: PostgreSQL writes are captured by pg_trickle, an incrementally maintained stream table is refreshed, and the changed rows are published to object storage as Parquet deltas.

The complete, maintained runnable setup is in the [OLTP-to-Lake Loop demo](../demos/ducklake-oltp-lake/README.md). Follow that guide for deployment and service configuration.

## 1. Launch the runnable pipeline

From the repository root:

```sh
cd demos/ducklake-oltp-lake
docker compose up
```

The demo configures the PostgreSQL extension, DuckLake catalog, MinIO bucket, and an order generator. It does not require Kafka.

## 2. Inspect the sink configuration

The demo initializes `revenue_by_region` with a stream-table definition equivalent to:

```sql
SELECT pgtrickle.create_stream_table(
    'revenue_by_region',
    query => $$
        SELECT region,
               date_trunc('minute', created_at) AS minute,
               SUM(amount) AS total_revenue,
               COUNT(*) AS order_count
        FROM orders
        GROUP BY region, date_trunc('minute', created_at)
    $$,
    schedule           => '5s',
    refresh_mode       => 'DIFFERENTIAL',
    sink               => 'ducklake',
    ducklake_sink_path => 's3://pg-trickle-demo/revenue_by_region/'
);
```

This is **illustrative**: the demo already creates the table. Do not run it a second time against the initialized database.

The sink publishes changes as refreshes complete; each Parquet file represents a delta, not an independent full snapshot.

## 3. Verify source and sink activity

Connect to the demo PostgreSQL instance:

```sh
psql postgresql://postgres:postgres@localhost:5432/postgres
```

```sql
SELECT region, minute, total_revenue, order_count
FROM revenue_by_region
ORDER BY minute DESC
LIMIT 10;

SELECT stream_table_name, delta_row_count, ducklake_snapshot_id, written_at
FROM pgtrickle.pgt_ducklake_provenance
ORDER BY written_at DESC
LIMIT 10;
```

If provenance is empty, check the PostgreSQL service logs and the sink configuration using the [demo troubleshooting table](../demos/ducklake-oltp-lake/README.md#troubleshooting).

## 4. Read the lake

Browse the output files in MinIO at http://localhost:9001, then open http://localhost:8888/lab?token=demo and execute the `oltp_lake_analysis.ipynb` notebook. It demonstrates querying the historical data with DuckDB.

## 5. Shut down

```sh
docker compose down -v
```

This removes demo volumes as well as containers. For the broader single-box walkthrough, see [Tutorial 3](tutorial-modern-data-stack-one-box.md).
