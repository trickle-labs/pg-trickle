-- pg_trickle 0.87.8 -> 0.87.9 upgrade migration
-- v0.87.9: Core Lifecycle Security (LSEC-7..LSEC-9, issue #941).
--
-- create_or_replace_stream_table, alter_stream_table, and drop_stream_table
-- become SECURITY DEFINER with a pinned search_path: a non-superuser stream
-- table owner with only the documented public API grants can now use these
-- three functions without direct privileges on pg_trickle's private catalog
-- or the pgtrickle_changes schema. Rust resolves every caller-controlled
-- name under the original caller's own search_path (never a hard-coded
-- `public` default and never `current_schema()` evaluated under the pinned
-- definer path), authorizes the complete drop-cascade plan against the
-- original caller before any mutation, and restores the exact pre-change
-- storage owner on every ALTER path that recreates the storage table.
--
-- ALTER FUNCTION preserves existing GRANT/REVOKE state, so no ACLs need to
-- be reissued here.

ALTER FUNCTION pgtrickle.create_or_replace_stream_table(
    TEXT, TEXT, TEXT, TEXT, bool, TEXT, TEXT, TEXT, bool, bool, TEXT, INT,
    double precision, TEXT, bool, TEXT
) SECURITY DEFINER; -- nosemgrep: sql.security-definer.present — search_path is pinned by the following ALTER FUNCTION.
ALTER FUNCTION pgtrickle.create_or_replace_stream_table(
    TEXT, TEXT, TEXT, TEXT, bool, TEXT, TEXT, TEXT, bool, bool, TEXT, INT,
    double precision, TEXT, bool, TEXT
) SET search_path = pgtrickle, pg_catalog, pg_temp;

ALTER FUNCTION pgtrickle.alter_stream_table(
    TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, bool, bool, TEXT, TEXT,
    bigint, INT, TEXT, INT, double precision, TEXT, double precision, TEXT
) SECURITY DEFINER; -- nosemgrep: sql.security-definer.present — search_path is pinned by the following ALTER FUNCTION.
ALTER FUNCTION pgtrickle.alter_stream_table(
    TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, bool, bool, TEXT, TEXT,
    bigint, INT, TEXT, INT, double precision, TEXT, double precision, TEXT
) SET search_path = pgtrickle, pg_catalog, pg_temp;

ALTER FUNCTION pgtrickle.drop_stream_table(TEXT, bool)
    SECURITY DEFINER; -- nosemgrep: sql.security-definer.present — search_path is pinned by the following ALTER FUNCTION.
ALTER FUNCTION pgtrickle.drop_stream_table(TEXT, bool)
    SET search_path = pgtrickle, pg_catalog, pg_temp;
