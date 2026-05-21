-- Demo E: OLTP-to-Lake Loop — PostgreSQL initialization

CREATE EXTENSION IF NOT EXISTS pg_trickle;

CREATE TABLE IF NOT EXISTS orders (
    order_id   BIGSERIAL PRIMARY KEY,
    region     TEXT NOT NULL,
    amount     NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── DuckLake catalog tables ────────────────────────────────────────────────
-- pg_trickle's DuckLake sink writes Parquet deltas to MinIO and registers
-- each file in this catalog so DuckDB, Trino, and Spark can query the lake.

CREATE TABLE IF NOT EXISTS ducklake_table (
    table_id    BIGSERIAL PRIMARY KEY,
    schema_name TEXT NOT NULL DEFAULT 'public',
    table_name  TEXT NOT NULL,
    data_path   TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS ducklake_data_file (
    data_file_id      BIGSERIAL PRIMARY KEY,
    table_id          BIGINT NOT NULL,
    begin_snapshot    BIGINT,
    path              TEXT NOT NULL,
    row_count         BIGINT,
    file_size_bytes   BIGINT,
    encryption_key_id TEXT
);

CREATE TABLE IF NOT EXISTS ducklake_table_stats (
    table_id   BIGINT PRIMARY KEY,
    row_count  BIGINT DEFAULT 0,
    file_count BIGINT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ducklake_snapshot (
    table_id      BIGINT NOT NULL,
    snapshot_id   BIGINT NOT NULL,
    snapshot_time TIMESTAMPTZ DEFAULT now(),
    created_by    TEXT,
    PRIMARY KEY (table_id, snapshot_id)
);

CREATE TABLE IF NOT EXISTS ducklake_view (
    view_name       TEXT PRIMARY KEY,
    view_definition TEXT
);

-- Register the revenue_by_region table in the DuckLake catalog.
INSERT INTO ducklake_table (schema_name, table_name, data_path)
VALUES ('public', 'revenue_by_region', 's3://pg-trickle-demo/revenue_by_region/');

-- Create the revenue_by_region stream table with DuckLake sink
SELECT pgtrickle.create_stream_table(
    name                   => 'revenue_by_region',
    query                  => $$
        SELECT
            region,
            date_trunc('minute', created_at) AS minute,
            SUM(amount)                       AS total_revenue,
            COUNT(*)                          AS order_count
        FROM orders
        GROUP BY region, date_trunc('minute', created_at)
    $$,
    schedule               => '5s',
    refresh_mode           => 'DIFFERENTIAL',
    sink                   => 'ducklake',
    ducklake_sink_path     => 's3://pg-trickle-demo/revenue_by_region/',
    ducklake_sink_table_id => (SELECT table_id FROM ducklake_table WHERE table_name = 'revenue_by_region')
);
