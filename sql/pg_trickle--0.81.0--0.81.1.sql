-- pg_trickle 0.81.0 -> 0.81.1 upgrade migration
-- #903: permit documented non-superuser stream-table creation while keeping
-- catalog and change-buffer objects private.

-- These functions use extension-owner privileges for private metadata, CDC,
-- and storage setup. Rust explicitly checks the invoking role's source SELECT
-- and target CREATE privileges and transfers the completed stream table to it.

ALTER FUNCTION pgtrickle.create_stream_table(
    TEXT, TEXT, TEXT, TEXT, bool, TEXT, TEXT, TEXT, bool, bool, TEXT, INT,
    double precision, TEXT, bool, TEXT, INT
) SECURITY DEFINER; -- nosemgrep: sql.security-definer.present — search_path is pinned by the following ALTER FUNCTION.
ALTER FUNCTION pgtrickle.create_stream_table(
    TEXT, TEXT, TEXT, TEXT, bool, TEXT, TEXT, TEXT, bool, bool, TEXT, INT,
    double precision, TEXT, bool, TEXT, INT
) SET search_path = pgtrickle, pg_catalog, pg_temp;

ALTER FUNCTION pgtrickle.create_stream_table_if_not_exists(
    TEXT, TEXT, TEXT, TEXT, bool, TEXT, TEXT, TEXT, bool, bool, TEXT, INT,
    double precision, TEXT, bool, TEXT, INT
) SECURITY DEFINER; -- nosemgrep: sql.security-definer.present — search_path is pinned by the following ALTER FUNCTION.
ALTER FUNCTION pgtrickle.create_stream_table_if_not_exists(
    TEXT, TEXT, TEXT, TEXT, bool, TEXT, TEXT, TEXT, bool, bool, TEXT, INT,
    double precision, TEXT, bool, TEXT, INT
) SET search_path = pgtrickle, pg_catalog, pg_temp;

ALTER FUNCTION pgtrickle.bulk_create(jsonb) SECURITY DEFINER; -- nosemgrep: sql.security-definer.present — search_path is pinned by the following ALTER FUNCTION.
ALTER FUNCTION pgtrickle.bulk_create(jsonb)
    SET search_path = pgtrickle, pg_catalog, pg_temp;

ALTER FUNCTION pgtrickle.create_stream_table_fast_append_only(
    TEXT, TEXT, TEXT, TEXT, TEXT, INT, double precision
) SECURITY DEFINER; -- nosemgrep: sql.security-definer.present — search_path is pinned by the following ALTER FUNCTION.
ALTER FUNCTION pgtrickle.create_stream_table_fast_append_only(
    TEXT, TEXT, TEXT, TEXT, TEXT, INT, double precision
) SET search_path = pgtrickle, pg_catalog, pg_temp;

ALTER FUNCTION pgtrickle.create_stream_table_realtime(
    TEXT, TEXT, TEXT, bool, TEXT, INT, double precision
) SECURITY DEFINER; -- nosemgrep: sql.security-definer.present — search_path is pinned by the following ALTER FUNCTION.
ALTER FUNCTION pgtrickle.create_stream_table_realtime(
    TEXT, TEXT, TEXT, bool, TEXT, INT, double precision
) SET search_path = pgtrickle, pg_catalog, pg_temp;

ALTER FUNCTION pgtrickle.create_stream_table_batch(
    TEXT, TEXT, TEXT, bool, TEXT, INT, double precision
) SECURITY DEFINER; -- nosemgrep: sql.security-definer.present — search_path is pinned by the following ALTER FUNCTION.
ALTER FUNCTION pgtrickle.create_stream_table_batch(
    TEXT, TEXT, TEXT, bool, TEXT, INT, double precision
) SET search_path = pgtrickle, pg_catalog, pg_temp;

ALTER FUNCTION pgtrickle.create_stream_table_cost_optimized(
    TEXT, TEXT, TEXT, bool, TEXT, INT, double precision
) SECURITY DEFINER; -- nosemgrep: sql.security-definer.present — search_path is pinned by the following ALTER FUNCTION.
ALTER FUNCTION pgtrickle.create_stream_table_cost_optimized(
    TEXT, TEXT, TEXT, bool, TEXT, INT, double precision
) SET search_path = pgtrickle, pg_catalog, pg_temp;

ALTER FUNCTION pgtrickle._on_ddl_end() SECURITY DEFINER; -- nosemgrep: sql.security-definer.present — search_path is pinned by the following ALTER FUNCTION.
ALTER FUNCTION pgtrickle._on_ddl_end()
    SET search_path = pgtrickle, pg_catalog, pg_temp;
REVOKE EXECUTE ON FUNCTION pgtrickle._on_ddl_end() FROM PUBLIC;

ALTER FUNCTION pgtrickle._on_sql_drop() SECURITY DEFINER; -- nosemgrep: sql.security-definer.present — search_path is pinned by the following ALTER FUNCTION.
ALTER FUNCTION pgtrickle._on_sql_drop()
    SET search_path = pgtrickle, pg_catalog, pg_temp;
REVOKE EXECUTE ON FUNCTION pgtrickle._on_sql_drop() FROM PUBLIC;
