-- pg_trickle 0.82.0 -> 0.83.0 upgrade migration
--
-- Composite row identity is intentionally breaking for persisted rows.
-- Source locks and buffer conversion happen in this transaction so a writer
-- cannot commit version-1 hashes after version-2 capture is enabled.

SELECT set_config('pg_trickle.enabled', 'off', true);

LOCK TABLE pgtrickle.pgt_stream_tables,
           pgtrickle.pgt_dependencies,
           pgtrickle.pgt_change_buffers
    IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS row_identity_version SMALLINT;

COMMENT ON COLUMN pgtrickle.pgt_stream_tables.row_identity_version IS
    'Composite row-identity encoding version. NULL is unknown; existing '
    'objects are conservatively marked version 1 until protected rebuild.';

ALTER TABLE pgtrickle.pgt_change_buffers
    ADD COLUMN IF NOT EXISTS row_identity_version SMALLINT;

COMMENT ON COLUMN pgtrickle.pgt_change_buffers.row_identity_version IS
    'Composite row-identity encoding used by buffer writers.';

-- DML against every tracked source and stream table is blocked while writers
-- are switched to the version-2 framing contract.
DO $$
DECLARE
    source_oid oid;
BEGIN
    FOR source_oid IN
        SELECT DISTINCT relid
        FROM (
            SELECT pgt_relid AS relid
            FROM pgtrickle.pgt_stream_tables
            UNION ALL
            SELECT source_relid AS relid
            FROM pgtrickle.pgt_dependencies
        ) tracked
        ORDER BY relid
    LOOP
        IF EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class
            WHERE oid = source_oid
              AND relkind IN ('r', 'p', 'v', 'm', 'f')
        ) THEN
            EXECUTE format(
                'LOCK TABLE %s IN SHARE ROW EXCLUSIVE MODE',
                source_oid::regclass
            );
        END IF;
    END LOOP;
END
$$;

CREATE TABLE IF NOT EXISTS pgtrickle.pgt_set_operation_states (
    pgt_id         BIGINT NOT NULL
                   REFERENCES pgtrickle.pgt_stream_tables(pgt_id)
                   ON DELETE CASCADE,
    node_ordinal   INTEGER NOT NULL,
    operation      TEXT NOT NULL CHECK (operation IN ('INTERSECT', 'EXCEPT')),
    is_all         BOOLEAN NOT NULL,
    state_relid    OID NOT NULL,
    schema_version SMALLINT NOT NULL,
    PRIMARY KEY (pgt_id, node_ordinal)
);
SELECT pg_catalog.pg_extension_config_dump('pgtrickle.pgt_set_operation_states', '');

-- Existing stream rows are not relabeled until protected FULL reinit.  This
-- makes the scheduler treat every pre-upgrade result as stale.
UPDATE pgtrickle.pgt_stream_tables
SET row_identity_version = 1,
    needs_reinit = TRUE,
    updated_at = now();

-- Drop all pending version-1 changes while retaining exactly the sentinel.
-- The source lock above prevents new trigger rows from being inserted during
-- this conversion. A missing registered relation aborts the whole upgrade.
DO $$
DECLARE
    buffer_schema text := COALESCE(
        current_setting('pg_trickle.change_buffer_schema', true),
        'pgtrickle_changes'
    );
    buffer record;
BEGIN
    FOR buffer IN
        SELECT buffer_key, sentinel_token
        FROM pgtrickle.pgt_change_buffers
        ORDER BY source_kind, source_id
    LOOP
        IF to_regclass(format('%I.%I', buffer_schema, buffer.buffer_key)) IS NULL THEN
            RAISE EXCEPTION
                'pg_trickle buffer %.% is registered but missing',
                buffer_schema, buffer.buffer_key;
        END IF;

        EXECUTE format(
            'LOCK TABLE %I.%I IN SHARE ROW EXCLUSIVE MODE',
            buffer_schema, buffer.buffer_key
        );
        EXECUTE format(
            'DELETE FROM %I.%I
             WHERE NOT (lsn = ''0/0''::pg_lsn
                        AND action = ''S''
                        AND pk_hash = $1)',
            buffer_schema, buffer.buffer_key
        ) USING buffer.sentinel_token;
        EXECUTE format(
            'INSERT INTO %I.%I (lsn, action, pk_hash)
             SELECT ''0/0''::pg_lsn, ''S'', $1
             WHERE NOT EXISTS (
                 SELECT 1 FROM %I.%I
                 WHERE lsn = ''0/0''::pg_lsn
                   AND action = ''S''
                   AND pk_hash = $1
             )',
            buffer_schema, buffer.buffer_key,
            buffer_schema, buffer.buffer_key
        ) USING buffer.sentinel_token;
    END LOOP;
END
$$;

-- Rebuild every existing CDC writer before buffers are marked version 2.
-- The function aborts on any writer failure, preserving the migration
-- transaction instead of allowing mixed identity encodings.
SELECT pgtrickle.rebuild_cdc_triggers();

UPDATE pgtrickle.pgt_change_buffers
SET row_identity_version = 2;

INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES (
    '0.83.0',
    'Versioned composite identity with locked CDC buffer conversion and reinit'
)
ON CONFLICT (version) DO NOTHING;
