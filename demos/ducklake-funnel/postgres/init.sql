-- pg_trickle DuckLake Funnel Demo — PostgreSQL initialisation
-- Runs once on container first start.

-- Enable pg_trickle
CREATE EXTENSION IF NOT EXISTS pg_trickle;

-- ─────────────────────────────────────────────────────────────────────────────
-- Source table: events bridge
-- In a real DuckLake deployment this would be the DuckLake inlined-data table
-- (ducklake_inlined_data_table_<id>_<version>) or a pg_trickle-managed bridge
-- populated from DuckLake's table_changes() feed.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS events_bridge (
    event_id    BIGSERIAL PRIMARY KEY,
    user_id     INT          NOT NULL,
    product_id  INT          NOT NULL,
    event_type  TEXT         NOT NULL,  -- 'view', 'add_to_cart', 'purchase'
    revenue_usd NUMERIC(10,2),
    occurred_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- ── DuckLake catalog tables ────────────────────────────────────────────────
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

-- Register stream tables in the DuckLake catalog.
INSERT INTO ducklake_table (schema_name, table_name, data_path)
VALUES
    ('public', 'revenue_by_minute',  's3://pg-trickle-demo/revenue_by_minute/'),
    ('public', 'funnel_by_product',  's3://pg-trickle-demo/funnel_by_product/');

-- ─────────────────────────────────────────────────────────────────────────────
-- Stream table T-1: revenue aggregated by minute and product
-- ─────────────────────────────────────────────────────────────────────────────
SELECT pgtrickle.create_stream_table(
    name         => 'revenue_by_minute',
    query        => $$
        SELECT
            date_trunc('minute', occurred_at) AS minute,
            product_id,
            SUM(revenue_usd)                  AS total_revenue,
            COUNT(*)                          AS purchase_count
        FROM events_bridge
        WHERE event_type = 'purchase'
        GROUP BY date_trunc('minute', occurred_at), product_id
    $$,
    schedule               => '5s',
    refresh_mode           => 'DIFFERENTIAL',
    sink                   => 'ducklake',
    ducklake_sink_path     => 's3://pg-trickle-demo/revenue_by_minute/',
    ducklake_sink_table_id => (SELECT table_id FROM ducklake_table WHERE table_name = 'revenue_by_minute')
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Stream table T-2: conversion funnel by product
-- ─────────────────────────────────────────────────────────────────────────────
SELECT pgtrickle.create_stream_table(
    name         => 'funnel_by_product',
    query        => $$
        SELECT
            product_id,
            COUNT(*) FILTER (WHERE event_type = 'view')        AS views,
            COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS add_to_cart,
            COUNT(*) FILTER (WHERE event_type = 'purchase')    AS purchases,
            ROUND(
                100.0 * COUNT(*) FILTER (WHERE event_type = 'purchase')
                       / NULLIF(COUNT(*) FILTER (WHERE event_type = 'view'), 0),
                2
            ) AS conversion_pct
        FROM events_bridge
        GROUP BY product_id
    $$,
    schedule               => '5s',
    refresh_mode           => 'DIFFERENTIAL',
    sink                   => 'ducklake',
    ducklake_sink_path     => 's3://pg-trickle-demo/funnel_by_product/',
    ducklake_sink_table_id => (SELECT table_id FROM ducklake_table WHERE table_name = 'funnel_by_product')
);
