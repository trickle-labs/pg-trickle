-- pg_trickle 0.87.6 -> 0.87.7 upgrade migration
-- v0.87.7: Security Context and Catalog Foundation (LSEC-1..LSEC-3).
--
-- Adds pgtrickle.pgt_stream_tables.defining_search_path: the exact
-- search_path a stream table's defining_query was authored under, used by
-- later releases to resolve the same names on every refresh. Fails before
-- any mutation if a stream table's storage relation is missing — a
-- deterministic owner-derived path cannot be computed without it.

DO $$
DECLARE
    missing_count INT;
BEGIN
    SELECT count(*) INTO missing_count
    FROM pgtrickle.pgt_stream_tables st
    WHERE NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c WHERE c.oid = st.pgt_relid
    );

    IF missing_count > 0 THEN
        RAISE EXCEPTION 'pg_trickle upgrade to 0.87.7 aborted: % stream table(s) '
            'reference a storage relation that no longer exists, so '
            'defining_search_path cannot be backfilled from its owner. '
            'Drop the affected stream table(s) with pgtrickle.drop_stream_table() '
            'before upgrading.', missing_count;
    END IF;
END
$$;

ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS defining_search_path TEXT;

UPDATE pgtrickle.pgt_stream_tables st
SET defining_search_path = quote_ident(pg_catalog.pg_get_userbyid(c.relowner)) || ', public'
FROM pg_catalog.pg_class c
WHERE c.oid = st.pgt_relid
  AND st.defining_search_path IS NULL;

ALTER TABLE pgtrickle.pgt_stream_tables
    ALTER COLUMN defining_search_path SET NOT NULL;

COMMENT ON COLUMN pgtrickle.pgt_stream_tables.defining_search_path IS
    'Exact search_path defining_query was resolved under (bare $user '
    'already expanded). Set at CREATE and on any ALTER that changes the '
    'query; preserved by configuration-only ALTERs and ownership transfer.';
