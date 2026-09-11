-- pg_trickle v0.103.0 -> v0.104.0
-- Graph V1 and Delta V1 are stable public SQL contracts in v0.104.

ALTER TABLE pgtrickle.pgt_refresh_history
    DROP CONSTRAINT IF EXISTS pgt_refresh_history_initiated_by_check;

ALTER TABLE pgtrickle.pgt_refresh_history
    ADD CONSTRAINT pgt_refresh_history_initiated_by_check
    CHECK (initiated_by IN (
        'SCHEDULER', 'MANUAL', 'INITIAL', 'SELF_MONITOR', 'SCHEDULER_FUSED',
        'EXTERNAL_GRAPH'
    ));

INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES (
    '0.104.0',
    'Stable Graph V1 and Delta V1 contracts and final feature freeze'
)
ON CONFLICT (version) DO NOTHING;
