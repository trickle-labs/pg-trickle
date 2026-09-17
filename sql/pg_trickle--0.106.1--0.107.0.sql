-- pg_trickle v0.106.1 -> v0.107.0
-- Verified support and operator procedures; no catalog objects change.

INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES (
    '0.107.0',
    'Verified support and operator procedures'
)
ON CONFLICT (version) DO NOTHING;
