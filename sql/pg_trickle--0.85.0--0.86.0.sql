-- v0.86.0: Product UX and transparency catalog contracts.

ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS requested_refresh_mode TEXT NOT NULL DEFAULT 'DIFFERENTIAL',
    ADD COLUMN IF NOT EXISTS target_freshness_mode TEXT,
    ADD COLUMN IF NOT EXISTS refresh_reason TEXT,
    ADD COLUMN IF NOT EXISTS refresh_reason_detail TEXT;

UPDATE pgtrickle.pgt_stream_tables
   SET requested_refresh_mode = refresh_mode
 WHERE requested_refresh_mode IS NULL OR requested_refresh_mode = 'DIFFERENTIAL';

UPDATE pgtrickle.pgt_stream_tables
   SET target_freshness_mode = 'INTERVAL'
 WHERE target_freshness_mode IS NULL
   AND freshness_deadline_ms IS NOT NULL
   AND freshness_deadline_ms > 0;

ALTER TABLE pgtrickle.pgt_stream_tables
    ADD CONSTRAINT pgt_stream_tables_requested_refresh_mode_check
    CHECK (requested_refresh_mode IN ('AUTO', 'FULL', 'DIFFERENTIAL', 'IMMEDIATE'));

ALTER TABLE pgtrickle.pgt_stream_tables
    ADD CONSTRAINT pgt_stream_tables_target_freshness_mode_check
    CHECK (target_freshness_mode IS NULL OR target_freshness_mode IN ('INTERVAL', 'ON_COMMIT', 'MANUAL'));

ALTER TABLE pgtrickle.pgt_refresh_summary
    ADD COLUMN IF NOT EXISTS stats_reset_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS total_full_refreshes BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS total_diff_refreshes BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS total_delta_rows_processed BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_full_reason TEXT,
    ADD COLUMN IF NOT EXISTS last_full_reason_detail TEXT;

ALTER TABLE pgtrickle.pgt_refresh_history
    ADD COLUMN IF NOT EXISTS refresh_reason TEXT,
    ADD COLUMN IF NOT EXISTS refresh_reason_detail TEXT;

ALTER TABLE pgtrickle.pgt_cost_model_summary
    ADD COLUMN IF NOT EXISTS p95_ms DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS p99_ms DOUBLE PRECISION;

CREATE INDEX IF NOT EXISTS idx_hist_pgt_stats_window
    ON pgtrickle.pgt_refresh_history (pgt_id, start_time, status, action);

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
         THEN s.total_duration_ms::double precision / s.total_refreshes
    END AS avg_refresh_duration_ms,
    c.p95_ms AS p95_refresh_duration_ms,
    c.p99_ms AS p99_refresh_duration_ms,
    s.last_refresh_at,
    EXTRACT(EPOCH FROM (now() - COALESCE(s.last_refresh_at, st.created_at))) * 1000 AS current_lag_ms,
    COALESCE(st.requested_refresh_mode, st.refresh_mode) AS requested_refresh_mode,
    st.target_freshness_mode,
    st.freshness_deadline_ms AS target_freshness_ms,
    s.last_full_reason,
    s.last_full_reason_detail,
    st.last_error_message AS last_error,
    st.last_error_at,
    s.stats_reset_at
FROM pgtrickle.pgt_stream_tables st
LEFT JOIN pgtrickle.pgt_refresh_summary s ON s.pgt_id = st.pgt_id
LEFT JOIN pgtrickle.pgt_cost_model_summary c ON c.pgt_id = st.pgt_id;

-- Replace C wrappers whose signatures gained the v0.86.0 freshness argument.
DROP FUNCTION IF EXISTS pgtrickle."create_stream_table"(
    text, text, text, text, boolean, text, text, text, boolean, boolean,
    text, integer, double precision, text, boolean, text, integer
);
CREATE FUNCTION pgtrickle."create_stream_table"(
    "name" TEXT,
    "query" TEXT,
    "schedule" TEXT DEFAULT 'calculated',
    "refresh_mode" TEXT DEFAULT 'AUTO',
    "initialize" boolean DEFAULT true,
    "diamond_consistency" TEXT DEFAULT NULL,
    "diamond_schedule_policy" TEXT DEFAULT NULL,
    "cdc_mode" TEXT DEFAULT NULL,
    "append_only" boolean DEFAULT false,
    "pooler_compatibility_mode" boolean DEFAULT false,
    "partition_by" TEXT DEFAULT NULL,
    "max_differential_joins" integer DEFAULT NULL,
    "max_delta_fraction" double precision DEFAULT NULL,
    "output_distribution_column" TEXT DEFAULT NULL,
    "temporal" boolean DEFAULT false,
    "storage_backend" TEXT DEFAULT NULL,
    "fillfactor" integer DEFAULT NULL,
    "target_freshness" TEXT DEFAULT NULL
) RETURNS void
STRICT LANGUAGE c
AS 'MODULE_PATHNAME', 'create_stream_table_wrapper';

DROP FUNCTION IF EXISTS pgtrickle."create_stream_table_if_not_exists"(
    text, text, text, text, boolean, text, text, text, boolean, boolean,
    text, integer, double precision, text, boolean, text, integer
);
CREATE FUNCTION pgtrickle."create_stream_table_if_not_exists"(
    "name" TEXT,
    "query" TEXT,
    "schedule" TEXT DEFAULT 'calculated',
    "refresh_mode" TEXT DEFAULT 'AUTO',
    "initialize" boolean DEFAULT true,
    "diamond_consistency" TEXT DEFAULT NULL,
    "diamond_schedule_policy" TEXT DEFAULT NULL,
    "cdc_mode" TEXT DEFAULT NULL,
    "append_only" boolean DEFAULT false,
    "pooler_compatibility_mode" boolean DEFAULT false,
    "partition_by" TEXT DEFAULT NULL,
    "max_differential_joins" integer DEFAULT NULL,
    "max_delta_fraction" double precision DEFAULT NULL,
    "output_distribution_column" TEXT DEFAULT NULL,
    "temporal" boolean DEFAULT false,
    "storage_backend" TEXT DEFAULT NULL,
    "fillfactor" integer DEFAULT NULL,
    "target_freshness" TEXT DEFAULT NULL
) RETURNS void
STRICT LANGUAGE c
AS 'MODULE_PATHNAME', 'create_stream_table_if_not_exists_wrapper';

DROP FUNCTION IF EXISTS pgtrickle."alter_stream_table"(
    text, text, text, text, text, text, text, text, boolean, boolean,
    text, text, bigint, integer, text, integer, double precision, text,
    double precision
);
CREATE FUNCTION pgtrickle."alter_stream_table"(
    "name" TEXT,
    "query" TEXT DEFAULT NULL,
    "schedule" TEXT DEFAULT NULL,
    "refresh_mode" TEXT DEFAULT NULL,
    "status" TEXT DEFAULT NULL,
    "diamond_consistency" TEXT DEFAULT NULL,
    "diamond_schedule_policy" TEXT DEFAULT NULL,
    "cdc_mode" TEXT DEFAULT NULL,
    "append_only" boolean DEFAULT NULL,
    "pooler_compatibility_mode" boolean DEFAULT NULL,
    "tier" TEXT DEFAULT NULL,
    "fuse" TEXT DEFAULT NULL,
    "fuse_ceiling" bigint DEFAULT NULL,
    "fuse_sensitivity" integer DEFAULT NULL,
    "partition_by" TEXT DEFAULT NULL,
    "max_differential_joins" integer DEFAULT NULL,
    "max_delta_fraction" double precision DEFAULT NULL,
    "post_refresh_action" TEXT DEFAULT NULL,
    "reindex_drift_threshold" double precision DEFAULT NULL,
    "target_freshness" TEXT DEFAULT NULL
) RETURNS void
LANGUAGE c
AS 'MODULE_PATHNAME', 'alter_stream_table_wrapper';

REVOKE EXECUTE ON FUNCTION pgtrickle.alter_stream_table(
    text, text, text, text, text, text, text, text, boolean, boolean,
    text, text, bigint, integer, text, integer, double precision, text,
    double precision, text
) FROM PUBLIC;

DROP FUNCTION IF EXISTS pgtrickle."preview_stream_table"(text);
CREATE FUNCTION pgtrickle."preview_stream_table"(
    "query" TEXT,
    "schedule" TEXT DEFAULT 'calculated',
    "refresh_mode" TEXT DEFAULT 'AUTO',
    "target_freshness" TEXT DEFAULT NULL
) RETURNS TABLE ("property" TEXT, "value" TEXT)
LANGUAGE c
AS 'MODULE_PATHNAME', 'preview_stream_table_wrapper';

DROP FUNCTION IF EXISTS pgtrickle."get_refresh_history"(text, integer);
CREATE FUNCTION pgtrickle."get_refresh_history"(
    "name" TEXT,
    "max_rows" integer DEFAULT 20
) RETURNS TABLE (
    "refresh_id" bigint,
    "data_timestamp" timestamptz,
    "start_time" timestamptz,
    "end_time" timestamptz,
    "action" TEXT,
    "status" TEXT,
    "rows_inserted" bigint,
    "rows_updated" bigint,
    "rows_deleted" bigint,
    "duration_ms" double precision,
    "error_message" TEXT,
    "refresh_reason" TEXT,
    "refresh_reason_detail" TEXT
)
STRICT LANGUAGE c
AS 'MODULE_PATHNAME', 'get_refresh_history_wrapper';

CREATE OR REPLACE FUNCTION pgtrickle."explain"("name" TEXT)
RETURNS TEXT
STRICT LANGUAGE c
AS 'MODULE_PATHNAME', 'explain_wrapper';

CREATE OR REPLACE FUNCTION pgtrickle."explain_json"("name" TEXT)
RETURNS jsonb
STRICT LANGUAGE c
AS 'MODULE_PATHNAME', 'explain_json_wrapper';

CREATE OR REPLACE FUNCTION pgtrickle."stat_reset"("pgt_id" bigint)
RETURNS void
STRICT LANGUAGE c
AS 'MODULE_PATHNAME', 'stat_reset_wrapper';

CREATE OR REPLACE FUNCTION pgtrickle."stat_reset_all"()
RETURNS void
STRICT LANGUAGE c
AS 'MODULE_PATHNAME', 'stat_reset_all_wrapper';

REVOKE EXECUTE ON FUNCTION pgtrickle.stat_reset(bigint) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.stat_reset_all() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.explain(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.explain_json(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.get_refresh_history(text, integer) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.preview_stream_table(text, text, text, text) TO PUBLIC;
