-- T-5 (v0.79.0): Assert that after the alter flow (schedule changed to '3m')
-- the stream table catalog reflects the updated schedule.
-- Returns rows where the alter did NOT take effect.
-- An empty result set means the test passes.
-- Note: pg_trickle stores the schedule verbatim as supplied (e.g. '3m', not
-- the normalised '3 minutes'), so we compare against the raw shorthand form.
SELECT pgt_name, schedule
FROM pgtrickle.pgt_stream_tables
WHERE pgt_name = 'order_totals_compat'
  AND schedule != '3m'
