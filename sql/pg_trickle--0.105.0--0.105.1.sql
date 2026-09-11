-- pg_trickle v0.105.0 -> v0.105.1
-- v0.105.1 adds runtime qualification evidence only. No SQL objects change.

INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES (
    '0.105.1',
    'Runtime conformance and recovery qualification'
)
ON CONFLICT (version) DO NOTHING;
