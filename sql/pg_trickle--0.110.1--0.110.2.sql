-- pg_trickle v0.110.1 -> v0.110.2
-- #1438: fence stale scheduled failure updates after stream-table recovery.
-- Historical jobs lack trustworthy enqueue-generation snapshots. Leave their
-- arrays empty so old jobs cannot overwrite an operator's successful resume.

ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS recovery_generation BIGINT NOT NULL DEFAULT 0
        CHECK (recovery_generation >= 0);

ALTER TABLE pgtrickle.pgt_scheduler_jobs
    ADD COLUMN IF NOT EXISTS member_recovery_generations BIGINT[] NOT NULL DEFAULT '{}'::BIGINT[];
