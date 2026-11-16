-- pg_trickle v0.108.3 -> v0.109.0
-- Local WAL slots are removed only after their retirement has committed.
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_wal_slot_retirements (
    slot_name            TEXT PRIMARY KEY,
    source_relid         OID NOT NULL,
    capture_instance_id  TEXT NOT NULL,
    queued_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
REVOKE ALL ON TABLE pgtrickle.pgt_wal_slot_retirements FROM PUBLIC;
SELECT pg_catalog.pg_extension_config_dump(
    'pgtrickle.pgt_wal_slot_retirements', 'WHERE false'
);

-- v0.108.3 materializations predate the JSONB finite/non-finite state
-- retained by newer DIFFERENTIAL SUM/AVG operators. Do not advance their
-- frontiers using that new planner until a protected FULL rebuild has seeded
-- the missing state. Match conservatively: comments, whitespace, qualified
-- calls and existing aliases may surround the aggregate name.
-- No source rows, storage relations, publication bindings or CDC buffers are
-- dropped by this migration.
UPDATE pgtrickle.pgt_stream_tables AS st
   SET needs_reinit = TRUE, updated_at = now()
 WHERE st.refresh_mode = 'DIFFERENTIAL'
   AND st.defining_query ~* '(^|[^[:alnum:]_])(sum|avg)([^[:alnum:]_]|$)';
