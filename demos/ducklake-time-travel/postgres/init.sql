-- Demo B: DuckLake Time-Travel Debugging — PostgreSQL initialization
--
-- Scenario:
--   1. Generator inserts clean events → stream table aggregates them →
--      DuckLake sink writes Parquet deltas to MinIO every 5 s.
--   2. Take a checkpoint:
--        SELECT pgtrickle.snapshot_stream_table('events_summary');
--   3. Inject a data-quality bug (set INJECT_BUG_AFTER_BATCH in generator,
--      or run: INSERT INTO events(event_type,user_id) VALUES('CORRUPT_BILLING',0)).
--   4. Stream table picks up the corrupt rows; new event_type rows appear.
--   5. Time-travel rewind:
--        SELECT pgtrickle.pause_scheduler(ARRAY['public.events_summary']);
--        SELECT pgtrickle.restore_from_snapshot('public.events_summary',
--               '<snapshot table from step 2>');
--        SELECT pgtrickle.resume_scheduler(ARRAY['public.events_summary']);
--   6. Stream table is back to the clean state; next refresh replays forward
--      from the checkpointed frontier.

CREATE EXTENSION IF NOT EXISTS pg_trickle;

CREATE TABLE IF NOT EXISTS events (
    event_id    BIGSERIAL   PRIMARY KEY,
    event_type  TEXT        NOT NULL,
    user_id     INT         NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
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

-- Register in the DuckLake catalog.
INSERT INTO ducklake_table (schema_name, table_name, data_path)
VALUES ('public', 'events_summary', 's3://pg-trickle-demo/events_summary/');

-- ── Stream table: event counts per type ───────────────────────────────────
SELECT pgtrickle.create_stream_table(
    name                   => 'events_summary',
    query                  => $$
        SELECT
            event_type,
            COUNT(*)                AS event_count,
            COUNT(DISTINCT user_id) AS unique_users,
            MAX(event_id)           AS latest_event_id
        FROM events
        GROUP BY event_type
    $$,
    schedule               => '5s',
    refresh_mode           => 'DIFFERENTIAL',
    sink                   => 'ducklake',
    ducklake_sink_path     => 's3://pg-trickle-demo/events_summary/',
    ducklake_sink_table_id => (SELECT table_id FROM ducklake_table WHERE table_name = 'events_summary')
);
