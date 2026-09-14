-- pg_trickle v0.105.3 -> v0.106.0
-- Executed package qualification and database workload evidence; no catalog objects change.

INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES (
    '0.106.0',
    'Executed package qualification and database workload evidence'
)
ON CONFLICT (version) DO NOTHING;
