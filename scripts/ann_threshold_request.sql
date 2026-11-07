-- Run with psql -X -w -v ann_stream='schema.table' -v ann_index='schema.index'
-- -f scripts/ann_threshold_request.sql. Do not use --single-transaction.
\set ON_ERROR_STOP on
\set AUTOCOMMIT on
\if :{?ann_stream}
\else
  \warn 'ann_stream is required'
  DO $worker$ BEGIN RAISE EXCEPTION 'ann_stream is required'; END $worker$;
\endif
\if :{?ann_index}
\else
  \warn 'ann_index is required'
  DO $worker$ BEGIN RAISE EXCEPTION 'ann_index is required'; END $worker$;
\endif
\if :{?ann_lock_timeout}
\else
  \set ann_lock_timeout '5s'
\endif
\if :{?ann_statement_timeout}
\else
  \set ann_statement_timeout '5min'
\endif

SET search_path = pg_catalog, pg_temp;
SELECT pg_catalog.set_config('lock_timeout', :'ann_lock_timeout', false) AS configured_lock_timeout \gset
SELECT pg_catalog.set_config('statement_timeout', :'ann_statement_timeout', false) AS configured_statement_timeout \gset

-- Capture OIDs and a quoted name before locking. The policy API revalidates
-- the target index and all privileges before recording any request.
SELECT c.oid::bigint AS stream_oid, i.oid::bigint AS index_oid,
       pg_catalog.format('%I.%I', n.nspname, c.relname) AS stream_qualified
FROM pgtrickle.pgt_stream_tables s
JOIN pg_catalog.pg_class c ON c.oid = s.pgt_relid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_class i ON i.oid = pg_catalog.to_regclass(:'ann_index')
WHERE c.oid = pg_catalog.to_regclass(:'ann_stream')
  AND pg_catalog.has_schema_privilege(n.oid, 'USAGE')
  AND pg_catalog.has_table_privilege(c.oid, 'SELECT')
  AND pg_catalog.has_table_privilege(c.oid, 'MAINTAIN')
\gset

BEGIN;
SELECT pg_catalog.format('LOCK TABLE %s IN SHARE ROW EXCLUSIVE MODE', :'stream_qualified') \gexec
-- The name may have been dropped or rebound before the lock was acquired.
SELECT CASE WHEN pg_catalog.to_regclass(:'stream_qualified')::oid = :'stream_oid'::bigint::oid
  THEN pg_catalog.format('ANALYZE %s', :'stream_qualified')
  ELSE 'DO $worker$ BEGIN RAISE EXCEPTION ''stream identity changed before lock''; END $worker$'
END \gexec

-- A nonpositive estimate or unknown watermark is not a healthy result.
SELECT CASE
    WHEN c.reltuples::bigint <= 0 THEN pg_catalog.jsonb_build_object('decision', 'unknown_estimate')
    WHEN s.ann_churn_generation IS NULL OR s.ann_churn_operations IS NULL
      THEN pg_catalog.jsonb_build_object('decision', 'unknown_work')
    ELSE pgtrickle.request_ann_maintenance_if_needed(
      c.oid, :'index_oid'::bigint::oid, c.reltuples::bigint,
      s.ann_churn_generation, s.ann_churn_operations)
    END AS decision
FROM pgtrickle.pgt_stream_tables s
JOIN pg_catalog.pg_class c ON c.oid = s.pgt_relid
WHERE s.pgt_relid = :'stream_oid'::bigint::oid
\gset
COMMIT;
SELECT :'decision'::jsonb AS ann_threshold_decision;
