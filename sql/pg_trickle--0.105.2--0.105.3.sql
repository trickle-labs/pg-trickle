-- pg_trickle v0.105.2 -> v0.105.3
-- DVM corrections and qualification metadata only; no catalog objects change.

INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES (
    '0.105.3',
    'DVM correctness corrections and release qualification'
)
ON CONFLICT (version) DO NOTHING;
