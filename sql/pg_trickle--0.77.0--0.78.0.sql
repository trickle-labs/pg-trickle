-- pg_trickle 0.77.0 -> 0.78.0 upgrade migration
-- v0.78.0: DVM Engine Root-Cause Fixes + Scheduler Intelligence

-- Summary of changes in v0.78.0:
--
--   DVM-1: CASE/IN-list aggregate drift fix — append-only sources now take
--          the differential path (GROUP_RESCAN handles INSERT/DELETE only);
--          only mutable sources fall back to FULL.
--
--   DVM-2: Correlated aggregate scalar subquery rewrite — a CTE pre-
--          aggregation rewrite is attempted before the FULL fallback.
--          Queries that can be safely decorrelated now run differentially.
--
--   P-1: CORRELATED_SUBQUERY_DELTA_QUADRATIC reason code for unsafe
--        correlated aggregate patterns (auditable fallback path).
--
--   P-2: query_complexity_class catalog column — stores the OpTree-derived
--        complexity label at CREATE/ALTER time; back-filled lazily on first
--        refresh for pre-0.78.0 stream tables.
--
--   P-3: pgt_cost_model_summary table — one-row-per-ST cache of recent
--        refresh history aggregates.  The scheduler updates it in a single
--        batch query per tick, eliminating N per-ST history subqueries.
--
--   P-4: Placeholder resolver cache collision guard — canonical key stored
--        alongside hash; collisions trigger a rebuild with a WARNING.
--
--   T-2: TPC-H differential latency regression gate (test harness change
--        only — no schema change).
--
--   T-3: Nightly 300 s fuzz workflow (CI change only — no schema change).

-- P-2a: storage_fillfactor column (introduced in v0.78.0).
ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS storage_fillfactor INTEGER;

COMMENT ON COLUMN pgtrickle.pgt_stream_tables.storage_fillfactor IS
    'Optional FILLFACTOR override for the materialised view storage.  '
    'NULL means use the PostgreSQL default (100).';

-- P-2b: complexity class column for the stream tables catalog.
ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS query_complexity_class TEXT;

COMMENT ON COLUMN pgtrickle.pgt_stream_tables.query_complexity_class IS
    'OpTree-derived complexity label (Scan/Filter/Aggregate/Join/JoinAggregate). '
    'Populated at CREATE/ALTER time; back-filled lazily on first refresh. '
    'Used by the scheduler cost model and monitoring views.';

-- P-3: Cost model summary table — one row per stream table.
-- Populated by batch_update_cost_model_summary() on each scheduler tick.
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_cost_model_summary (
    pgt_id       BIGINT      NOT NULL
                             REFERENCES pgtrickle.pgt_stream_tables(pgt_id)
                             ON DELETE CASCADE,
    avg_full_ms  DOUBLE PRECISION,
    avg_diff_ms  DOUBLE PRECISION,
    sample_count INTEGER     NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pgt_cost_model_summary_pk PRIMARY KEY (pgt_id)
);

COMMENT ON TABLE pgtrickle.pgt_cost_model_summary IS
    'P-3 (v0.78.0): Per-stream-table summary of recent refresh performance. '
    'Updated in a single batch query per scheduler tick to avoid N per-ST '
    'history subqueries. avg_full_ms = avg FULL refresh ms; '
    'avg_diff_ms = avg ms per delta row for DIFFERENTIAL refreshes.';

-- Upgrade complete.  The extension binary must be updated to 0.78.0 for the
-- new scheduler and DVM features to take effect.
-- Safe to apply with zero downtime.
