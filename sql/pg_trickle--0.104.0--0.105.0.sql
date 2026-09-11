-- pg_trickle v0.104.0 -> v0.105.0
-- v0.105.0 adds qualification and release evidence only. No SQL objects change.

INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES (
    '0.105.0',
    'Qualification contract and candidate-bound release evidence'
)
ON CONFLICT (version) DO NOTHING;
