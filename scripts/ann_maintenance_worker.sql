-- Run with psql -X -w -v request_id=... -f scripts/ann_maintenance_worker.sql.
-- Connection and TLS settings come from libpq; do not use --single-transaction.
\set ON_ERROR_STOP on
\set AUTOCOMMIT on
\if :{?request_id}
\else
  \warn 'request_id is required'
  DO $worker$ BEGIN RAISE EXCEPTION 'request_id is required'; END $worker$;
\endif
\if :{?ann_lock_timeout}
\else
  \set ann_lock_timeout '5s'
\endif
\if :{?ann_statement_timeout}
\else
  \set ann_statement_timeout '30min'
\endif

SET search_path = pg_catalog, pg_temp;
SELECT pg_catalog.set_config('lock_timeout', :'ann_lock_timeout', false) AS ann_set_lock_timeout \gset
SELECT pg_catalog.set_config('statement_timeout', :'ann_statement_timeout', false) AS ann_set_statement_timeout \gset
SELECT pgtrickle.claim_ann_maintenance(:'request_id'::bigint) AS ann_claim \gset

-- Revalidate and quote the catalog identity immediately before top-level DDL.
\set ON_ERROR_STOP off
WITH request AS MATERIALIZED (
    SELECT pgtrickle.ann_maintenance_request(:'request_id'::bigint) AS status
)
SELECT COALESCE((
    SELECT pg_catalog.format('REINDEX INDEX CONCURRENTLY %I.%I', n.nspname, c.relname)
    FROM request r
    JOIN pg_catalog.pg_class c ON c.oid = (r.status->>'index_oid')::oid
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE r.status->>'state' = 'RUNNING'
      AND r.status->>'identity_current' = 'true'
      AND r.status->>'cancel_requested' = 'false'
), '') AS ann_command \gset
\if :ERROR
  \set ann_failure_state :LAST_ERROR_SQLSTATE
  \set ON_ERROR_STOP on
  SELECT pgtrickle.finish_ann_maintenance(:'request_id'::bigint, false,
      pg_catalog.format('Request revalidation failed (SQLSTATE %s)', :'ann_failure_state'));
  DO $worker$ BEGIN RAISE EXCEPTION 'ANN maintenance revalidation failed; inspect request status'; END $worker$;
\endif
\set ON_ERROR_STOP on
SELECT (:'ann_command' <> '') AS ann_may_execute \gset
\if :ann_may_execute
  -- The generated statement is the only command passed to this top-level execution.
  \set ON_ERROR_STOP off
  SELECT :'ann_command' \gexec
  \if :ERROR
    \set ON_ERROR_STOP on
    SELECT pgtrickle.finish_ann_maintenance(:'request_id'::bigint, false,
        'Concurrent reindex failed; see psql diagnostics');
    DO $worker$ BEGIN RAISE EXCEPTION 'ANN maintenance failed; inspect request status'; END $worker$;
  \else
    \set ON_ERROR_STOP on
    SELECT pgtrickle.finish_ann_maintenance(:'request_id'::bigint, true, '');
  \endif
\else
  SELECT pgtrickle.finish_ann_maintenance(:'request_id'::bigint, false,
      'Index identity changed or cancellation was requested before execution');
  DO $worker$ BEGIN RAISE EXCEPTION 'ANN maintenance was not executed; inspect request status'; END $worker$;
\endif
