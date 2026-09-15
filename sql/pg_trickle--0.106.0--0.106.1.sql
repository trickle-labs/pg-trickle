-- pg_trickle v0.106.0 -> v0.106.1
-- Same-pass differential graph maintenance; no catalog objects change.

INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES (
    '0.106.1',
    'Same-pass differential graph maintenance'
)
ON CONFLICT (version) DO NOTHING;
