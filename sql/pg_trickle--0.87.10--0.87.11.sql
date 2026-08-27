-- pg_trickle 0.87.10 -> 0.87.11 upgrade migration
-- LSEC-13..LSEC-15: caller-checked snapshot targets and provenance-bound
-- snapshot restore/drop authorization.
--
-- Existing ACLs are preserved. The Rust entry points authorize the original
-- caller before touching private catalogs or snapshot relations.

ALTER FUNCTION pgtrickle.snapshot_stream_table(text, text)
    SECURITY DEFINER; -- nosemgrep: sql.security-definer.present — search_path is pinned by the following ALTER FUNCTION.
ALTER FUNCTION pgtrickle.snapshot_stream_table(text, text)
    SET search_path = pgtrickle, pg_catalog, pg_temp;
ALTER FUNCTION pgtrickle.restore_from_snapshot(text, text)
    SECURITY DEFINER; -- nosemgrep: sql.security-definer.present — search_path is pinned by the following ALTER FUNCTION.
ALTER FUNCTION pgtrickle.restore_from_snapshot(text, text)
    SET search_path = pgtrickle, pg_catalog, pg_temp;
ALTER FUNCTION pgtrickle.list_snapshots(text)
    SECURITY DEFINER; -- nosemgrep: sql.security-definer.present — search_path is pinned by the following ALTER FUNCTION.
ALTER FUNCTION pgtrickle.list_snapshots(text)
    SET search_path = pgtrickle, pg_catalog, pg_temp;
ALTER FUNCTION pgtrickle.drop_snapshot(text)
    SECURITY DEFINER; -- nosemgrep: sql.security-definer.present — search_path is pinned by the following ALTER FUNCTION.
ALTER FUNCTION pgtrickle.drop_snapshot(text)
    SET search_path = pgtrickle, pg_catalog, pg_temp;
