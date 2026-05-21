-- pg_trickle 0.67.0 -> 0.68.0 upgrade migration
--
-- v0.68.0 — Assessment-13 Correctness & Durability Sprint
--
-- Changes in this release:
--
--   COR-001: Fused refresh audit trail
--     - Add 'SCHEDULER_FUSED' to the initiated_by CHECK constraint on
--       pgt_refresh_history. Previously, fused refreshes written with
--       initiated_by = 'SCHEDULER_FUSED' were silently rejected by the
--       CHECK constraint, leaving fused refreshes invisible in the audit log.
--
--   COR-003 / ARCH-001: change_buffer_durability GUC wired
--     - The pg_trickle.change_buffer_durability GUC is now honoured by
--       create_change_buffer_table() and create_st_change_buffer_table().
--     - pg_trickle.unlogged_buffers is kept as a compatibility alias and
--       now emits a WARNING advising migration to change_buffer_durability.
--
--   COR-004: DuckLake timestamp NULL fix
--     - timestamptz/timestamp columns are now exported as microsecond-epoch
--       integers rather than text strings, preventing silent NULL coercion.
--
--   SCAL-001: Pool path deletion
--     - The persistent worker pool (pool.rs) dead code path has been removed.
--       Dynamic workers remain the sole supported scheduling path.

-- ── COR-001: Add SCHEDULER_FUSED to initiated_by CHECK constraint ─────────

ALTER TABLE pgtrickle.pgt_refresh_history
    DROP CONSTRAINT IF EXISTS pgt_refresh_history_initiated_by_check;

ALTER TABLE pgtrickle.pgt_refresh_history
    ADD CONSTRAINT pgt_refresh_history_initiated_by_check
    CHECK (initiated_by IN (
        'SCHEDULER',
        'MANUAL',
        'INITIAL',
        'SELF_MONITOR',
        'SCHEDULER_FUSED'
    ));
