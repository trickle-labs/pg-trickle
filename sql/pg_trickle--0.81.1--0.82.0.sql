-- pg_trickle 0.81.1 -> 0.82.0 upgrade migration

SELECT set_config('pg_trickle.enabled', 'off', true);

-- The composite return type of this SQL-facing function gains rows_updated.
-- Drop the old signature before the v0.82.0 install SQL recreates it.
DROP FUNCTION IF EXISTS pgtrickle.get_refresh_history(text, integer);

ALTER TABLE pgtrickle.pgt_scheduler_jobs
    ADD COLUMN IF NOT EXISTS dispatch_tick_id BIGINT,
    ADD COLUMN IF NOT EXISTS tick_watermark_lsn PG_LSN;

ALTER TABLE pgtrickle.pgt_dependencies
    ADD COLUMN IF NOT EXISTS cutover_target TEXT
        CHECK (cutover_target IN ('TRIGGER', 'WAL')),
    ADD COLUMN IF NOT EXISTS cutover_lsn PG_LSN;

ALTER TABLE pgtrickle.pgt_refresh_history
    ADD COLUMN IF NOT EXISTS rows_updated BIGINT NOT NULL DEFAULT 0;

ALTER TABLE pgtrickle.pgt_refresh_summary
    ADD COLUMN IF NOT EXISTS total_rows_updated BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS pgtrickle.pgt_change_buffers (
    buffer_key       TEXT PRIMARY KEY,
    source_kind      TEXT NOT NULL CHECK (source_kind IN ('BASE', 'STREAM_TABLE')),
    source_id        BIGINT NOT NULL,
    durability       TEXT NOT NULL CHECK (durability IN ('logged', 'unlogged', 'sync')),
    sentinel_token   BIGINT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_kind, source_id)
);

-- Register existing buffers before differential refresh is re-enabled.  Use
-- the configured change-buffer schema; installations may override the
-- default pgtrickle_changes name.
UPDATE pgtrickle.pgt_scheduler_jobs
SET status = 'CANCELLED', finished_at = now(),
    outcome_detail = 'Cancelled during v0.82.0 upgrade: missing immutable tick bound'
WHERE status IN ('QUEUED', 'RUNNING')
  AND (dispatch_tick_id IS NULL OR tick_watermark_lsn IS NULL);

DO $$
DECLARE
    change_schema TEXT := COALESCE(
        NULLIF(current_setting('pg_trickle.change_buffer_schema', true), ''),
        'pgtrickle_changes'
    );
    r RECORD;
BEGIN
    FOR r IN
        SELECT 'changes_' || COALESCE(ct.source_stable_name, ct.source_relid::text) AS buffer_key,
               ct.source_relid::bigint AS source_id,
               CASE WHEN b.relpersistence = 'u' THEN 'unlogged'
                    WHEN current_setting('pg_trickle.change_buffer_durability', true) = 'sync'
                    THEN 'sync' ELSE 'logged' END AS durability
        FROM pgtrickle.pgt_change_tracking ct
        JOIN pg_catalog.pg_class b
          ON b.relname = 'changes_' || COALESCE(ct.source_stable_name, ct.source_relid::text)
        JOIN pg_catalog.pg_namespace bn ON bn.oid = b.relnamespace
        WHERE bn.nspname = change_schema
    LOOP
        INSERT INTO pgtrickle.pgt_change_buffers
            (buffer_key, source_kind, source_id, durability, sentinel_token)
        VALUES (r.buffer_key, 'BASE', r.source_id, r.durability, r.source_id)
        ON CONFLICT (source_kind, source_id) DO NOTHING;
    END LOOP;

    FOR r IN
        SELECT c.relname AS buffer_key,
               substring(c.relname FROM '^changes_pgt_([0-9]+)$')::bigint AS source_id,
               CASE WHEN c.relpersistence = 'u' THEN 'unlogged'
                    WHEN current_setting('pg_trickle.change_buffer_durability', true) = 'sync'
                    THEN 'sync' ELSE 'logged' END AS durability
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = change_schema
          AND c.relname ~ '^changes_pgt_[0-9]+$'
    LOOP
        INSERT INTO pgtrickle.pgt_change_buffers
            (buffer_key, source_kind, source_id, durability, sentinel_token)
        VALUES (r.buffer_key, 'STREAM_TABLE', r.source_id, r.durability, r.source_id)
        ON CONFLICT (source_kind, source_id) DO NOTHING;
    END LOOP;
END $$;

-- A pre-upgrade UNLOGGED buffer cannot prove that its history survived a crash.
UPDATE pgtrickle.pgt_stream_tables st
SET needs_reinit = TRUE, updated_at = now()
WHERE EXISTS (
    SELECT 1
    FROM pgtrickle.pgt_dependencies d
    JOIN pgtrickle.pgt_change_buffers b
      ON ((d.source_type IN ('TABLE', 'FOREIGN_TABLE', 'MATVIEW')
           AND b.source_kind = 'BASE' AND b.source_id = d.source_relid::bigint)
       OR (d.source_type = 'STREAM_TABLE'
           AND b.source_kind = 'STREAM_TABLE'
           AND b.source_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables u
                              WHERE u.pgt_relid = d.source_relid)))
    WHERE d.pgt_id = st.pgt_id AND b.durability = 'unlogged'
);

-- Add the durable control row to every registered pre-upgrade buffer.
DO $$
DECLARE
    b RECORD;
    change_schema TEXT := COALESCE(
        NULLIF(current_setting('pg_trickle.change_buffer_schema', true), ''),
        'pgtrickle_changes'
    );
BEGIN
    FOR b IN
        SELECT cb.buffer_key, cb.sentinel_token
        FROM pgtrickle.pgt_change_buffers cb
    LOOP
        EXECUTE format(
            'INSERT INTO %I.%I (lsn, action, pk_hash)
             SELECT ''0/0''::pg_lsn, ''S'', $1
             WHERE NOT EXISTS (
                 SELECT 1 FROM %I.%I
                 WHERE lsn = ''0/0''::pg_lsn AND action = ''S'' AND pk_hash = $1
             )',
            change_schema, b.buffer_key,
            change_schema, b.buffer_key
        ) USING b.sentinel_token;
    END LOOP;
END $$;

SELECT pg_catalog.pg_extension_config_dump('pgtrickle.pgt_change_buffers', '');
INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES ('0.82.0', 'Frontier visibility and CDC durability gate')
ON CONFLICT (version) DO NOTHING;
