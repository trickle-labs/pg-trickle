r#"
CREATE SCHEMA IF NOT EXISTS pgtrickle;
CREATE SCHEMA IF NOT EXISTS pgtrickle_changes;

CREATE TABLE IF NOT EXISTS pgtrickle.pgt_stream_tables (
    pgt_id           BIGSERIAL PRIMARY KEY,
    pgt_relid        OID NOT NULL UNIQUE,
    pgt_name         TEXT NOT NULL,
    pgt_schema       TEXT NOT NULL,
    defining_query  TEXT NOT NULL,
    original_query  TEXT,
    schedule      TEXT,
    refresh_mode    TEXT NOT NULL DEFAULT 'DIFFERENTIAL'
                     CHECK (refresh_mode IN ('FULL', 'DIFFERENTIAL', 'IMMEDIATE')),
    status          TEXT NOT NULL DEFAULT 'INITIALIZING'
                     CHECK (status IN ('INITIALIZING', 'ACTIVE', 'SUSPENDED', 'ERROR')),
    is_populated    BOOLEAN NOT NULL DEFAULT FALSE,
    data_timestamp  TIMESTAMPTZ,
    frontier        JSONB,
    last_refresh_at TIMESTAMPTZ,
    consecutive_errors INT NOT NULL DEFAULT 0,
    needs_reinit    BOOLEAN NOT NULL DEFAULT FALSE,
    auto_threshold  DOUBLE PRECISION,
    last_full_ms    DOUBLE PRECISION,
    functions_used  TEXT[],
    topk_limit      INT,
    topk_order_by   TEXT,
    topk_offset     INT,
    diamond_consistency TEXT NOT NULL DEFAULT 'atomic'
                     CHECK (diamond_consistency IN ('none', 'atomic')),
    diamond_schedule_policy TEXT NOT NULL DEFAULT 'fastest'
                     CHECK (diamond_schedule_policy IN ('fastest', 'slowest')),
    has_keyless_source BOOLEAN NOT NULL DEFAULT FALSE,
    function_hashes TEXT,
    requested_cdc_mode TEXT
                     CHECK (requested_cdc_mode IN ('auto', 'trigger', 'wal')),
    is_append_only  BOOLEAN NOT NULL DEFAULT FALSE,
    scc_id          INT,
    last_fixpoint_iterations INT,
    max_differential_joins   INT,
    max_delta_fraction       DOUBLE PRECISION,
    pooler_compatibility_mode BOOLEAN NOT NULL DEFAULT FALSE,
    refresh_tier    TEXT NOT NULL DEFAULT 'hot'
                     CHECK (refresh_tier IN ('hot', 'warm', 'cold', 'frozen')),
    effective_refresh_mode TEXT,
    fuse_mode       TEXT NOT NULL DEFAULT 'off'
                     CHECK (fuse_mode IN ('off', 'on', 'auto')),
    fuse_state      TEXT NOT NULL DEFAULT 'armed'
                     CHECK (fuse_state IN ('armed', 'blown', 'disabled')),
    fuse_ceiling    BIGINT,
    fuse_sensitivity INT,
    blown_at        TIMESTAMPTZ,
    blow_reason     TEXT,
    last_error_message TEXT,
    last_error_at   TIMESTAMPTZ,
    downstream_publication_name TEXT,
    freshness_deadline_ms BIGINT,
    st_partition_key TEXT,
    
    st_placement    TEXT NOT NULL DEFAULT 'local',
    
    temporal_mode   BOOLEAN NOT NULL DEFAULT FALSE,
    
    storage_backend TEXT NOT NULL DEFAULT 'heap',
    
    post_refresh_action TEXT NOT NULL DEFAULT 'none'
                     CHECK (post_refresh_action IN ('none', 'analyze', 'reindex', 'reindex_if_drift')),
    reindex_drift_threshold DOUBLE PRECISION
                     CHECK (reindex_drift_threshold IS NULL OR (reindex_drift_threshold > 0 AND reindex_drift_threshold <= 1.0)),
    rows_changed_since_last_reindex BIGINT NOT NULL DEFAULT 0,
    last_reindex_at TIMESTAMPTZ,
    
    column_lineage  JSONB,
    
    defining_query_hash BIGINT NOT NULL DEFAULT 0,
    
    storage_fillfactor INT DEFAULT NULL CHECK (storage_fillfactor IS NULL OR (storage_fillfactor >= 10 AND storage_fillfactor <= 100)),
    
    query_complexity_class TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pgt_status ON pgtrickle.pgt_stream_tables (status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pgt_name ON pgtrickle.pgt_stream_tables (pgt_schema, pgt_name);
CREATE INDEX IF NOT EXISTS idx_pgt_relid ON pgtrickle.pgt_stream_tables (pgt_relid);

CREATE TABLE IF NOT EXISTS pgtrickle.pgt_dependencies (
    pgt_id        BIGINT NOT NULL REFERENCES pgtrickle.pgt_stream_tables(pgt_id) ON DELETE CASCADE,
    source_relid OID NOT NULL,
    source_type  TEXT NOT NULL CHECK (source_type IN ('TABLE', 'STREAM_TABLE', 'VIEW', 'MATVIEW', 'FOREIGN_TABLE')),
    columns_used TEXT[],
    column_snapshot JSONB,
    schema_fingerprint TEXT,
    cdc_mode     TEXT NOT NULL DEFAULT 'TRIGGER'
                  CHECK (cdc_mode IN ('TRIGGER', 'TRANSITIONING', 'WAL')),
    slot_name    TEXT,
    decoder_confirmed_lsn PG_LSN,
    transition_started_at TIMESTAMPTZ,
    
    source_stable_name   TEXT,
    
    source_placement     TEXT NOT NULL DEFAULT 'local',
    PRIMARY KEY (pgt_id, source_relid)
);

CREATE INDEX IF NOT EXISTS idx_deps_source ON pgtrickle.pgt_dependencies (source_relid);
CREATE INDEX IF NOT EXISTS idx_deps_pgt_id ON pgtrickle.pgt_dependencies (pgt_id);

CREATE TABLE IF NOT EXISTS pgtrickle.pgt_refresh_history (
    refresh_id      BIGSERIAL PRIMARY KEY,
    pgt_id           BIGINT NOT NULL
                     REFERENCES pgtrickle.pgt_stream_tables(pgt_id) ON DELETE CASCADE,
    data_timestamp  TIMESTAMPTZ NOT NULL,
    start_time      TIMESTAMPTZ NOT NULL,
    end_time        TIMESTAMPTZ,
    action          TEXT NOT NULL
                     CHECK (action IN ('NO_DATA', 'FULL', 'DIFFERENTIAL', 'REINITIALIZE', 'SKIP')),
    rows_inserted   BIGINT DEFAULT 0,
    rows_deleted    BIGINT DEFAULT 0,
    delta_row_count BIGINT DEFAULT 0,
    merge_strategy_used TEXT,
    was_full_fallback BOOLEAN NOT NULL DEFAULT FALSE,
    error_message   TEXT,
    status          TEXT NOT NULL
                     CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED', 'SKIPPED')),
    initiated_by    TEXT
                     CHECK (initiated_by IN ('SCHEDULER', 'MANUAL', 'INITIAL', 'SELF_MONITOR', 'SCHEDULER_FUSED')),
    freshness_deadline TIMESTAMPTZ,
    tick_watermark_lsn PG_LSN,
    fixpoint_iteration INT
);

CREATE INDEX IF NOT EXISTS idx_hist_pgt_ts ON pgtrickle.pgt_refresh_history (pgt_id, data_timestamp);
CREATE INDEX IF NOT EXISTS idx_hist_pgt_start ON pgtrickle.pgt_refresh_history (pgt_id, start_time);

CREATE TABLE IF NOT EXISTS pgtrickle.pgt_change_tracking (
    source_relid        OID PRIMARY KEY,
    slot_name           TEXT NOT NULL,
    last_consumed_lsn   PG_LSN,
    tracked_by_pgt_ids   BIGINT[],
    
    
    source_stable_name  TEXT,
    
    source_placement    TEXT NOT NULL DEFAULT 'local',
    
    frontier_per_node   JSONB
);

CREATE TABLE IF NOT EXISTS pgtrickle.pgt_scheduler_jobs (
    job_id          BIGSERIAL PRIMARY KEY,
    dag_version     BIGINT NOT NULL,
    unit_key        TEXT NOT NULL,
    unit_kind       TEXT NOT NULL
                     CHECK (unit_kind IN ('singleton', 'atomic_group', 'immediate_closure', 'cyclic_scc', 'repeatable_read_group', 'fused_chain')),
    member_pgt_ids  BIGINT[] NOT NULL,
    root_pgt_id     BIGINT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'QUEUED'
                     CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED',
                                       'RETRYABLE_FAILED', 'PERMANENT_FAILED', 'CANCELLED')),
    scheduler_pid   INT NOT NULL,
    worker_pid      INT,
    attempt_no      INT NOT NULL DEFAULT 1,
    enqueued_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    outcome_detail  TEXT,
    retryable       BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_sched_jobs_status_enqueued
    ON pgtrickle.pgt_scheduler_jobs (status, enqueued_at);
CREATE INDEX IF NOT EXISTS idx_sched_jobs_unit_status
    ON pgtrickle.pgt_scheduler_jobs (unit_key, status);
CREATE INDEX IF NOT EXISTS idx_sched_jobs_finished
    ON pgtrickle.pgt_scheduler_jobs (finished_at)
    WHERE finished_at IS NOT NULL;

CREATE OR REPLACE FUNCTION pgtrickle.parse_duration_seconds(input TEXT)
RETURNS BIGINT LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    s TEXT := trim(input);
    total BIGINT := 0;
    num TEXT := '';
    c CHAR;
BEGIN
    IF s IS NULL OR s = '' THEN RETURN NULL; END IF;
    FOR i IN 1..length(s) LOOP
        c := substr(s, i, 1);
        IF c BETWEEN '0' AND '9' THEN
            num := num || c;
        ELSIF c = 'h' THEN
            total := total + num::bigint * 3600; num := '';
        ELSIF c = 'm' THEN
            total := total + num::bigint * 60; num := '';
        ELSIF c = 's' THEN
            total := total + num::bigint; num := '';
        ELSIF c = 'd' THEN
            total := total + num::bigint * 86400; num := '';
        END IF;
    END LOOP;
    IF num <> '' THEN total := total + num::bigint; END IF;
    RETURN total;
END; $$;

CREATE OR REPLACE VIEW pgtrickle.stream_tables_info AS
SELECT st.*,
       now() - st.last_refresh_at AS staleness,
       CASE WHEN st.schedule IS NOT NULL
                 AND st.schedule !~ '[\s@]'
            THEN EXTRACT(EPOCH FROM (now() - st.last_refresh_at)) >
                 pgtrickle.parse_duration_seconds(st.schedule)
            ELSE NULL::boolean
       END AS stale,
       CASE WHEN st.topk_limit IS NOT NULL THEN TRUE ELSE FALSE END AS is_topk
FROM pgtrickle.pgt_stream_tables st;

CREATE OR REPLACE VIEW pgtrickle.pg_stat_stream_tables AS
SELECT
    st.pgt_id,
    st.pgt_schema,
    st.pgt_name,
    st.status,
    st.refresh_mode,
    st.is_populated,
    st.data_timestamp,
    st.schedule,
    now() - st.last_refresh_at AS staleness,
    CASE WHEN st.schedule IS NOT NULL AND st.last_refresh_at IS NOT NULL
              AND st.schedule !~ '[\s@]'
         THEN EXTRACT(EPOCH FROM (now() - st.last_refresh_at)) >
              pgtrickle.parse_duration_seconds(st.schedule)
         ELSE NULL::boolean
    END AS stale,
    st.consecutive_errors,
    st.needs_reinit,
    st.last_refresh_at,
    COALESCE(stats.total_refreshes, 0) AS total_refreshes,
    COALESCE(stats.successful_refreshes, 0) AS successful_refreshes,
    COALESCE(stats.failed_refreshes, 0) AS failed_refreshes,
    COALESCE(stats.total_rows_inserted, 0) AS total_rows_inserted,
    COALESCE(stats.total_rows_deleted, 0) AS total_rows_deleted,
    stats.avg_duration_ms,
    stats.last_action,
    stats.last_status,
    (SELECT array_agg(DISTINCT d.cdc_mode ORDER BY d.cdc_mode)
     FROM pgtrickle.pgt_dependencies d
     WHERE d.pgt_id = st.pgt_id AND d.source_type = 'TABLE') AS cdc_modes,
    st.scc_id,
    st.last_fixpoint_iterations,
    st.refresh_tier
FROM pgtrickle.pgt_stream_tables st
LEFT JOIN LATERAL (
    SELECT
        count(*)::bigint AS total_refreshes,
        count(*) FILTER (WHERE h.status = 'COMPLETED')::bigint AS successful_refreshes,
        count(*) FILTER (WHERE h.status = 'FAILED')::bigint AS failed_refreshes,
        COALESCE(sum(h.rows_inserted), 0)::bigint AS total_rows_inserted,
        COALESCE(sum(h.rows_deleted), 0)::bigint AS total_rows_deleted,
        CASE WHEN count(*) FILTER (WHERE h.end_time IS NOT NULL) > 0
             THEN avg(EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000)
                  FILTER (WHERE h.end_time IS NOT NULL)
             ELSE NULL
        END::float8 AS avg_duration_ms,
        (SELECT h2.action FROM pgtrickle.pgt_refresh_history h2
         WHERE h2.pgt_id = st.pgt_id ORDER BY h2.refresh_id DESC LIMIT 1) AS last_action,
        (SELECT h2.status FROM pgtrickle.pgt_refresh_history h2
         WHERE h2.pgt_id = st.pgt_id ORDER BY h2.refresh_id DESC LIMIT 1) AS last_status,
        (SELECT h2.initiated_by FROM pgtrickle.pgt_refresh_history h2
         WHERE h2.pgt_id = st.pgt_id ORDER BY h2.refresh_id DESC LIMIT 1) AS last_initiated_by,
        (SELECT h2.freshness_deadline FROM pgtrickle.pgt_refresh_history h2
         WHERE h2.pgt_id = st.pgt_id ORDER BY h2.refresh_id DESC LIMIT 1) AS freshness_deadline
    FROM pgtrickle.pgt_refresh_history h
    WHERE h.pgt_id = st.pgt_id
) stats ON true;

CREATE OR REPLACE VIEW pgtrickle.quick_health AS
SELECT
    (SELECT count(*) FROM pgtrickle.pgt_stream_tables)::bigint
        AS total_stream_tables,
    (SELECT count(*) FROM pgtrickle.pgt_stream_tables
     WHERE status = 'ERROR' OR consecutive_errors > 0)::bigint
        AS error_tables,
    (SELECT count(*) FROM pgtrickle.pgt_stream_tables
     WHERE schedule IS NOT NULL
       AND schedule !~ '[\s@]'
       AND last_refresh_at IS NOT NULL
       AND EXTRACT(EPOCH FROM (now() - last_refresh_at)) >
           pgtrickle.parse_duration_seconds(schedule))::bigint
        AS stale_tables,
    (SELECT count(*) > 0 FROM pg_stat_activity
     WHERE backend_type = 'pg_trickle scheduler')
        AS scheduler_running,
    CASE
        WHEN (SELECT count(*) FROM pgtrickle.pgt_stream_tables) = 0 THEN 'EMPTY'
        WHEN (SELECT count(*) FROM pgtrickle.pgt_stream_tables WHERE status = 'SUSPENDED') > 0 THEN 'CRITICAL'
        WHEN (SELECT count(*) FROM pgtrickle.pgt_stream_tables WHERE status = 'ERROR' OR consecutive_errors > 0) > 0 THEN 'WARNING'
        WHEN (SELECT count(*) FROM pgtrickle.pgt_stream_tables
              WHERE schedule IS NOT NULL
                AND schedule !~ '[\s@]'
                AND last_refresh_at IS NOT NULL
                AND EXTRACT(EPOCH FROM (now() - last_refresh_at)) >
                    pgtrickle.parse_duration_seconds(schedule)) > 0 THEN 'WARNING'
        ELSE 'OK'
    END AS status;
"#
