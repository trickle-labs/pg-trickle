-- pg_trickle 0.75.0 -> 0.76.0 upgrade migration
-- v0.76.0: RockLake Compatibility Certification + pg_mooncake stub removal

-- pg_mooncake removal (v0.76.0):
-- The pg_mooncake storage backend was never fully implemented — the string was
-- accepted and stored in pgt_stream_tables.storage_backend but no mooncake-
-- specific DDL was ever generated.  Any rows with storage_backend = 'pg_mooncake'
-- silently behaved as standard heap tables.
--
-- This migration normalises those rows to 'heap' (their actual behaviour) so
-- the catalog accurately reflects the storage in use.
--
-- If pg_trickle.columnar_backend was set to 'pg_mooncake' in postgresql.conf,
-- change it to 'none' (or 'citus' if Citus is available) before upgrading.

UPDATE pgtrickle.pgt_stream_tables
SET    storage_backend = 'heap'
WHERE  storage_backend = 'pg_mooncake';
