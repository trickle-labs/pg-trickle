-- pg_trickle v0.108.0 -> v0.108.1
-- Expose broken IMMEDIATE maintenance cascades to upgraded installations.

CREATE OR REPLACE VIEW pgtrickle.quick_health AS
WITH broken_immediate AS (
    SELECT count(DISTINCT st.pgt_id)::bigint AS table_count
    FROM pgtrickle.pgt_stream_tables st
    JOIN pgtrickle.pgt_dependencies dep ON dep.pgt_id = st.pgt_id
    WHERE st.refresh_mode = 'IMMEDIATE'
      AND dep.source_type IN ('TABLE', 'STREAM_TABLE')
      AND 8 <> (
          SELECT count(*)
          FROM pg_catalog.pg_trigger t
          WHERE t.tgrelid = dep.source_relid
            AND t.tgname ~ (
                '^pgt_ivm_(before_(insert|update|delete|trunc)|after_(ins|upd|del|trunc))_'
                || st.pgt_id::text || '$'
            )
            AND NOT t.tgisinternal
            AND t.tgenabled IN ('O', 'A')
      )
)
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
        WHEN broken_immediate.table_count > 0 THEN 'CRITICAL'
        WHEN (SELECT count(*) FROM pgtrickle.pgt_stream_tables WHERE status = 'ERROR' OR consecutive_errors > 0) > 0 THEN 'WARNING'
        WHEN (SELECT count(*) FROM pgtrickle.pgt_stream_tables
              WHERE schedule IS NOT NULL
                AND schedule !~ '[\s@]'
                AND last_refresh_at IS NOT NULL
                AND EXTRACT(EPOCH FROM (now() - last_refresh_at)) >
                    pgtrickle.parse_duration_seconds(schedule)) > 0 THEN 'WARNING'
        ELSE 'OK'
    END AS status,
    broken_immediate.table_count AS broken_immediate_tables
FROM broken_immediate;

INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES ('0.108.1', 'Immediate cascade consistency and quick health diagnostics')
ON CONFLICT (version) DO NOTHING;
