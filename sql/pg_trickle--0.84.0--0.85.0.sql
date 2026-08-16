-- v0.85.0 — scheduled refresh deadlines and typed outcomes

ALTER TABLE pgtrickle.pgt_refresh_history
    ADD COLUMN IF NOT EXISTS error_code text,
    ADD COLUMN IF NOT EXISTS error_sqlstate text,
    ADD COLUMN IF NOT EXISTS retryable boolean;

ALTER TABLE pgtrickle.pgt_scheduler_jobs
    ADD COLUMN IF NOT EXISTS outcome_code text,
    ADD COLUMN IF NOT EXISTS outcome_sqlstate text,
    ADD COLUMN IF NOT EXISTS worker_slot_generation bigint;

ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS self_heal_work_mem_percent smallint NOT NULL DEFAULT 100,
    ADD COLUMN IF NOT EXISTS self_heal_lock_backoff_exponent smallint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS self_heal_success_streak smallint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_error_code text,
    ADD COLUMN IF NOT EXISTS last_error_retryable boolean;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint
        WHERE conrelid = 'pgtrickle.pgt_refresh_history'::regclass
          AND conname = 'pgt_refresh_history_error_code_check'
    ) THEN
        ALTER TABLE pgtrickle.pgt_refresh_history
            ADD CONSTRAINT pgt_refresh_history_error_code_check
            CHECK (error_code IS NULL OR error_code IN
                ('LOCK_TIMEOUT', 'STATEMENT_TIMEOUT', 'DEADLOCK',
                 'SERIALIZATION', 'OUT_OF_MEMORY', 'CANCELLED',
                 'PERMANENT', 'UNKNOWN_RETRYABLE'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint
        WHERE conrelid = 'pgtrickle.pgt_scheduler_jobs'::regclass
          AND conname = 'pgt_scheduler_jobs_outcome_code_check'
    ) THEN
        ALTER TABLE pgtrickle.pgt_scheduler_jobs
            ADD CONSTRAINT pgt_scheduler_jobs_outcome_code_check
            CHECK (outcome_code IS NULL OR outcome_code IN
                ('LOCK_TIMEOUT', 'STATEMENT_TIMEOUT', 'DEADLOCK',
                 'SERIALIZATION', 'OUT_OF_MEMORY', 'CANCELLED',
                 'PERMANENT', 'UNKNOWN_RETRYABLE'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint
        WHERE conrelid = 'pgtrickle.pgt_stream_tables'::regclass
          AND conname = 'pgt_stream_tables_self_heal_work_mem_percent_check'
    ) THEN
        ALTER TABLE pgtrickle.pgt_stream_tables
            ADD CONSTRAINT pgt_stream_tables_self_heal_work_mem_percent_check
            CHECK (self_heal_work_mem_percent BETWEEN 25 AND 100);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint
        WHERE conrelid = 'pgtrickle.pgt_stream_tables'::regclass
          AND conname = 'pgt_stream_tables_self_heal_lock_backoff_check'
    ) THEN
        ALTER TABLE pgtrickle.pgt_stream_tables
            ADD CONSTRAINT pgt_stream_tables_self_heal_lock_backoff_check
            CHECK (self_heal_lock_backoff_exponent BETWEEN 0 AND 6);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint
        WHERE conrelid = 'pgtrickle.pgt_stream_tables'::regclass
          AND conname = 'pgt_stream_tables_self_heal_success_streak_check'
    ) THEN
        ALTER TABLE pgtrickle.pgt_stream_tables
            ADD CONSTRAINT pgt_stream_tables_self_heal_success_streak_check
            CHECK (self_heal_success_streak BETWEEN 0 AND 3);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint
        WHERE conrelid = 'pgtrickle.pgt_stream_tables'::regclass
          AND conname = 'pgt_stream_tables_last_error_code_check'
    ) THEN
        ALTER TABLE pgtrickle.pgt_stream_tables
            ADD CONSTRAINT pgt_stream_tables_last_error_code_check
            CHECK (last_error_code IS NULL OR last_error_code IN
                ('LOCK_TIMEOUT', 'STATEMENT_TIMEOUT', 'DEADLOCK',
                 'SERIALIZATION', 'OUT_OF_MEMORY', 'CANCELLED',
                 'PERMANENT', 'UNKNOWN_RETRYABLE'));
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_sched_jobs_terminal_finished
    ON pgtrickle.pgt_scheduler_jobs (finished_at, job_id)
    WHERE status IN ('SUCCEEDED', 'RETRYABLE_FAILED', 'PERMANENT_FAILED', 'CANCELLED');

CREATE INDEX IF NOT EXISTS idx_hist_start_time
    ON pgtrickle.pgt_refresh_history (start_time, refresh_id);

INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES ('0.85.0', 'Scheduler and resource resilience gate')
ON CONFLICT (version) DO NOTHING;

REVOKE EXECUTE ON FUNCTION pgtrickle.resume_after_drain() FROM PUBLIC;
