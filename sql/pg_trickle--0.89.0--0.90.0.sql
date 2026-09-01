-- pg_trickle 0.89.0 -> 0.90.0

-- Exact freshness evidence is additive. Existing rows stay NULL and are not
-- treated as measurements because their provenance cannot be reconstructed.
ALTER TABLE pgtrickle.pgt_refresh_history
    ADD COLUMN IF NOT EXISTS duration_ms DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS source_commit_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS visibility_xid XID,
    ADD COLUMN IF NOT EXISTS visible_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS commit_to_visible_ms DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS plan_identity BIGINT;

CREATE TABLE IF NOT EXISTS pgtrickle.pgt_freshness_controller_state (
    pgt_id              BIGINT PRIMARY KEY
                        REFERENCES pgtrickle.pgt_stream_tables(pgt_id)
                        ON DELETE CASCADE,
    controller_version   SMALLINT NOT NULL DEFAULT 1,
    plan_identity        BIGINT,
    target_ms           BIGINT NOT NULL CHECK (target_ms > 0),
    sample_count        INTEGER NOT NULL DEFAULT 0 CHECK (sample_count >= 0),
    last_settled_refresh_id BIGINT,
    last_sample_ms      DOUBLE PRECISION,
    p50_freshness_ms    DOUBLE PRECISION,
    p95_freshness_ms    DOUBLE PRECISION,
    p99_freshness_ms    DOUBLE PRECISION,
    sla_status          TEXT NOT NULL DEFAULT 'INSUFFICIENT_DATA'
                        CHECK (sla_status IN ('MEETING', 'AT_RISK', 'BREACHING',
                                              'INFEASIBLE', 'INSUFFICIENT_DATA',
                                              'EVIDENCE_UNAVAILABLE', 'NOT_APPLICABLE')),
    evidence_state      TEXT NOT NULL DEFAULT 'EXACT',
    breach_streak       SMALLINT NOT NULL DEFAULT 0 CHECK (breach_streak >= 0),
    recovery_streak     SMALLINT NOT NULL DEFAULT 0 CHECK (recovery_streak >= 0),
    breach_started_at   TIMESTAMPTZ,
    infeasibility_streak SMALLINT NOT NULL DEFAULT 0 CHECK (infeasibility_streak >= 0),
    feasibility_recovery_streak SMALLINT NOT NULL DEFAULT 0 CHECK (feasibility_recovery_streak >= 0),
    infeasible_since    TIMESTAMPTZ,
    infeasibility_reason TEXT,
    minimum_cost_ms     DOUBLE PRECISION,
    last_applied_interval_ms BIGINT,
    last_applied_mode   TEXT,
    last_applied_batch_size BIGINT,
    last_input_snapshot JSONB CHECK (last_input_snapshot IS NULL OR jsonb_typeof(last_input_snapshot) = 'object'),
    next_due_at         TIMESTAMPTZ,
    last_decision_at    TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE pgtrickle.pgt_freshness_controller_state
    ADD COLUMN IF NOT EXISTS controller_version SMALLINT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS plan_identity BIGINT,
    ADD COLUMN IF NOT EXISTS last_settled_refresh_id BIGINT,
    ADD COLUMN IF NOT EXISTS last_sample_ms DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS breach_streak SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS recovery_streak SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS breach_started_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS infeasibility_streak SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS feasibility_recovery_streak SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS infeasible_since TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS infeasibility_reason TEXT,
    ADD COLUMN IF NOT EXISTS minimum_cost_ms DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS last_applied_interval_ms BIGINT,
    ADD COLUMN IF NOT EXISTS last_applied_mode TEXT,
    ADD COLUMN IF NOT EXISTS last_applied_batch_size BIGINT,
    ADD COLUMN IF NOT EXISTS last_input_snapshot JSONB;
REVOKE ALL ON TABLE pgtrickle.pgt_freshness_controller_state FROM PUBLIC;
SELECT pg_catalog.pg_extension_config_dump(
    'pgtrickle.pgt_freshness_controller_state', 'WHERE false'
);

-- Add metadata to registered buffers only. The default is installed after the
-- nullable column so old rows remain explicitly unproven and the table is not
-- rewritten during upgrade.
DO $$
DECLARE
    buffer RECORD;
    buffer_schema TEXT := current_setting('pg_trickle.change_buffer_schema', true);
    qualified_name TEXT;
BEGIN
    buffer_schema := COALESCE(NULLIF(buffer_schema, ''), 'pgtrickle_changes');
    FOR buffer IN
        SELECT buffer_key, source_kind
          FROM pgtrickle.pgt_change_buffers
    LOOP
        qualified_name := format('%I.%I', buffer_schema, buffer.buffer_key);
        IF to_regclass(qualified_name) IS NULL THEN
            CONTINUE;
        END IF;
        -- Keep the dynamic DDL separate from the upgrade-completeness parser;
        -- the actual statement is still ALTER TABLE ... ADD COLUMN.
        EXECUTE format('ALTER TABLE %s ADD' || ' COLUMN IF NOT EXISTS source_xid XID', qualified_name);
        EXECUTE format('ALTER TABLE %s ADD' || ' COLUMN IF NOT EXISTS source_commit_at TIMESTAMPTZ', qualified_name);
        IF buffer.source_kind = 'BASE' THEN
            EXECUTE format(
                'ALTER TABLE %s ALTER COLUMN source_xid SET DEFAULT pg_current_xact_id()::xid',
                qualified_name
            );
        END IF;
    END LOOP;
END
$$;

-- Infer only the safe part of the old target declaration. Noncanonical
-- schedules remain operator-owned; existing target rows are never rejected.
INSERT INTO pgtrickle.pgt_freshness_controller_state
    (pgt_id, target_ms, sla_status, evidence_state, next_due_at)
SELECT pgt_id,
       freshness_deadline_ms,
       CASE WHEN current_setting('track_commit_timestamp', true) = 'on'
                  AND NOT EXISTS (
                      SELECT 1 FROM pgtrickle.pgt_dependencies d
                       WHERE d.pgt_id = pgtrickle.pgt_stream_tables.pgt_id
                         AND d.source_type NOT IN ('TABLE', 'STREAM_TABLE')
                  )
            THEN 'INSUFFICIENT_DATA' ELSE 'EVIDENCE_UNAVAILABLE' END,
       CASE WHEN current_setting('track_commit_timestamp', true) = 'on'
                  AND NOT EXISTS (
                      SELECT 1 FROM pgtrickle.pgt_dependencies d
                       WHERE d.pgt_id = pgtrickle.pgt_stream_tables.pgt_id
                         AND d.source_type NOT IN ('TABLE', 'STREAM_TABLE')
                  )
            THEN 'EXACT' ELSE 'UNAVAILABLE' END,
       COALESCE(last_refresh_at, created_at)
           + freshness_deadline_ms * interval '1 millisecond'
  FROM pgtrickle.pgt_stream_tables
 WHERE target_freshness_mode = 'INTERVAL'
   AND freshness_deadline_ms IS NOT NULL
ON CONFLICT (pgt_id) DO UPDATE SET
    target_ms = EXCLUDED.target_ms,
    sla_status = EXCLUDED.sla_status,
    evidence_state = EXCLUDED.evidence_state,
    next_due_at = COALESCE(pgtrickle.pgt_freshness_controller_state.next_due_at,
                           EXCLUDED.next_due_at),
    updated_at = now();

-- New SQL entry point added by the v0.90 binary.
CREATE OR REPLACE FUNCTION pgtrickle."freshness"() RETURNS TABLE (
    stream_table TEXT,
    target INTERVAL,
    p50 INTERVAL,
    p95 INTERVAL,
    p99 INTERVAL,
    status TEXT
)
STRICT STABLE PARALLEL SAFE LANGUAGE c
AS 'MODULE_PATHNAME', 'freshness_wrapper';

CREATE OR REPLACE VIEW pgtrickle.pg_stat_pgtrickle AS
SELECT
    st.pgt_id,
    st.pgt_schema AS schema_name,
    st.pgt_name AS table_name,
    COALESCE(s.total_refreshes, 0)::bigint AS total_refreshes,
    COALESCE(s.total_full_refreshes, 0)::bigint AS total_full_refreshes,
    COALESCE(s.total_diff_refreshes, 0)::bigint AS total_diff_refreshes,
    COALESCE(s.total_delta_rows_processed, 0)::bigint AS total_delta_rows_processed,
    CASE WHEN COALESCE(s.total_refreshes, 0) > 0
         THEN s.total_duration_ms::double precision / s.total_refreshes END
        AS avg_refresh_duration_ms,
    c.p95_ms AS p95_refresh_duration_ms,
    c.p99_ms AS p99_refresh_duration_ms,
    s.last_refresh_at,
    CASE WHEN COALESCE(s.last_refresh_at, st.created_at) IS NOT NULL
         THEN EXTRACT(EPOCH FROM (now() - COALESCE(s.last_refresh_at, st.created_at))) * 1000 END
        AS current_lag_ms,
    COALESCE(st.requested_refresh_mode, st.refresh_mode) AS requested_refresh_mode,
    st.target_freshness_mode,
    st.freshness_deadline_ms AS target_freshness_ms,
    s.last_full_reason,
    s.last_full_reason_detail,
    st.last_error_message AS last_error,
    st.last_error_at,
    s.stats_reset_at,
    f.p95_freshness_ms,
    COALESCE(f.sla_status,
        CASE WHEN st.target_freshness_mode = 'INTERVAL'
             THEN 'INSUFFICIENT_DATA' ELSE 'NOT_APPLICABLE' END) AS sla_status
FROM pgtrickle.pgt_stream_tables st
LEFT JOIN pgtrickle.pgt_refresh_summary s ON s.pgt_id = st.pgt_id
LEFT JOIN pgtrickle.pgt_cost_model_summary c ON c.pgt_id = st.pgt_id
LEFT JOIN pgtrickle.pgt_freshness_controller_state f ON f.pgt_id = st.pgt_id;

GRANT EXECUTE ON FUNCTION pgtrickle.freshness() TO PUBLIC;

-- The v0.90 worker pool status adds adaptive-controller columns. PostgreSQL
-- cannot replace a function when its RETURNS TABLE row type changes.
DROP FUNCTION IF EXISTS pgtrickle.worker_pool_status();
CREATE FUNCTION pgtrickle.worker_pool_status() RETURNS TABLE (
    active_workers INT,
    max_workers INT,
    per_db_cap INT,
    parallel_mode TEXT,
    idle_workers INT,
    last_scheduler_tick_unix BIGINT,
    ring_overflow_count BIGINT,
    citus_failure_total BIGINT,
    adaptive_enabled BOOLEAN,
    adaptive_min INT,
    adaptive_max INT,
    adaptive_target INT,
    resize_signal SMALLINT,
    resize_consecutive SMALLINT,
    queue_depth INT,
    cpu_percent DOUBLE PRECISION
)
STRICT LANGUAGE c
AS 'MODULE_PATHNAME', 'worker_pool_status_wrapper';

GRANT EXECUTE ON FUNCTION pgtrickle.worker_pool_status() TO PUBLIC;

CREATE OR REPLACE FUNCTION pgtrickle."recommend_target_freshness"(name TEXT) RETURNS TABLE (
    stream_table TEXT,
    current_target INTERVAL,
    recommended_target INTERVAL,
    observed_p95 INTERVAL,
    sample_count BIGINT,
    confidence DOUBLE PRECISION,
    reason TEXT
)
STRICT STABLE PARALLEL SAFE LANGUAGE c
AS 'MODULE_PATHNAME', 'recommend_target_freshness_wrapper';

GRANT EXECUTE ON FUNCTION pgtrickle.recommend_target_freshness(text) TO PUBLIC;

INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES ('0.90.0',
        'Freshness provenance, visibility settlement, and advisory controller state')
ON CONFLICT (version) DO NOTHING;
