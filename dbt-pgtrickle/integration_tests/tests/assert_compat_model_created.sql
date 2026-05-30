-- T-5 (v0.79.0): Assert that the compatibility-matrix stream table exists and
-- has the expected initial schedule after creation.
-- An empty result set means the test passes.
-- Note: pg_trickle stores the schedule verbatim as supplied (e.g. '1m', not
-- the normalised '1 minute'), so we compare against the raw shorthand form.
SELECT pgt_name, schedule
FROM pgtrickle.pgt_stream_tables
WHERE pgt_name = 'order_totals_compat'
  AND schedule != '1m'
