-- pg_trickle 0.72.0 -> 0.73.0 upgrade migration
-- v0.73.0: Monitoring Scalability & Operational Resilience

-- PERF-001: Incremental refresh-history summary table.
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_refresh_summary (
    pgt_id BIGINT PRIMARY KEY
          REFERENCES pgtrickle.pgt_stream_tables(pgt_id) ON DELETE CASCADE,
    total_refreshes BIGINT NOT NULL DEFAULT 0,
    successful_refreshes BIGINT NOT NULL DEFAULT 0,
    failed_refreshes BIGINT NOT NULL DEFAULT 0,
    total_rows_inserted BIGINT NOT NULL DEFAULT 0,
    total_rows_deleted BIGINT NOT NULL DEFAULT 0,
    total_duration_ms BIGINT NOT NULL DEFAULT 0,
    last_refresh_action TEXT,
    last_refresh_status TEXT,
    last_refresh_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Backfill summary from historical refresh records.
INSERT INTO pgtrickle.pgt_refresh_summary (
    pgt_id,
    total_refreshes,
    successful_refreshes,
    failed_refreshes,
    total_rows_inserted,
    total_rows_deleted,
    total_duration_ms,
    last_refresh_action,
    last_refresh_status,
    last_refresh_at,
    updated_at
)
SELECT
    h.pgt_id,
    count(*)::bigint AS total_refreshes,
    count(*) FILTER (WHERE h.status = 'COMPLETED')::bigint AS successful_refreshes,
    count(*) FILTER (WHERE h.status = 'FAILED')::bigint AS failed_refreshes,
    COALESCE(sum(h.rows_inserted), 0)::bigint AS total_rows_inserted,
    COALESCE(sum(h.rows_deleted), 0)::bigint AS total_rows_deleted,
    COALESCE(sum((EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000)::bigint), 0)::bigint AS total_duration_ms,
    (
        SELECT h2.action
        FROM pgtrickle.pgt_refresh_history h2
        WHERE h2.pgt_id = h.pgt_id
          AND h2.status <> 'RUNNING'
        ORDER BY h2.refresh_id DESC
        LIMIT 1
    ) AS last_refresh_action,
    (
        SELECT h2.status
        FROM pgtrickle.pgt_refresh_history h2
        WHERE h2.pgt_id = h.pgt_id
          AND h2.status <> 'RUNNING'
        ORDER BY h2.refresh_id DESC
        LIMIT 1
    ) AS last_refresh_status,
    (
        SELECT COALESCE(h2.end_time, h2.start_time)
        FROM pgtrickle.pgt_refresh_history h2
        WHERE h2.pgt_id = h.pgt_id
          AND h2.status <> 'RUNNING'
        ORDER BY h2.refresh_id DESC
        LIMIT 1
    ) AS last_refresh_at,
    now()
FROM pgtrickle.pgt_refresh_history h
WHERE h.status <> 'RUNNING'
GROUP BY h.pgt_id
ON CONFLICT (pgt_id) DO UPDATE SET
    total_refreshes = EXCLUDED.total_refreshes,
    successful_refreshes = EXCLUDED.successful_refreshes,
    failed_refreshes = EXCLUDED.failed_refreshes,
    total_rows_inserted = EXCLUDED.total_rows_inserted,
    total_rows_deleted = EXCLUDED.total_rows_deleted,
    total_duration_ms = EXCLUDED.total_duration_ms,
    last_refresh_action = EXCLUDED.last_refresh_action,
    last_refresh_status = EXCLUDED.last_refresh_status,
    last_refresh_at = EXCLUDED.last_refresh_at,
    updated_at = now();

-- ARCH-002 / REL-002: Persistent cleanup retry/backpressure status table.
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_cleanup_status (
    source_relid OID PRIMARY KEY,
    buffer_table TEXT NOT NULL,
    attempt_count INT NOT NULL DEFAULT 0,
    blocked BOOLEAN NOT NULL DEFAULT false,
    last_error TEXT,
    last_operation TEXT,
    last_attempt_at TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,
    backlog_rows BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cleanup_status_next_retry
    ON pgtrickle.pgt_cleanup_status (blocked, next_retry_at);
-- HOT-1: Add `storage_fillfactor` column to pgtrickle.pgt_stream_tables.
-- NULL = PostgreSQL default (100). Accepted range: 10-100.
ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS storage_fillfactor INT;
