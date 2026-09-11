-- pg_trickle v0.105.1 -> v0.105.2
-- v0.105.2 adds package and field qualification evidence only. No SQL objects change.

INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES (
    '0.105.2',
    'Package, upgrade, performance, and field validation'
)
ON CONFLICT (version) DO NOTHING;
