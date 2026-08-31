-- pg_trickle 0.87.17 -> 0.88.0
--
-- Cached plans from older releases lack the statistics identity used by the
-- v0.88 delta planner, so invalidate them after adding the cache metadata.

ALTER TABLE pgtrickle.pgt_template_cache
    ADD COLUMN IF NOT EXISTS statistics_epoch TEXT NOT NULL DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS planning_version INTEGER NOT NULL DEFAULT 1;

TRUNCATE TABLE pgtrickle.pgt_template_cache;

CREATE FUNCTION pgtrickle."explain_delta_plan"(pgt_id BIGINT)
RETURNS JSONB
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'explain_delta_plan_wrapper';

GRANT EXECUTE ON FUNCTION pgtrickle.explain_delta_plan(BIGINT) TO PUBLIC;
