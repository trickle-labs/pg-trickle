-- pg_trickle 0.81.0 -> 0.81.1 upgrade migration
-- #903: permit documented non-superuser stream-table creation while keeping
-- catalog and change-buffer objects private.

-- These functions use extension-owner privileges only for private metadata and
-- CDC setup. Rust switches back to the invoking role for output-table DDL and
-- the defining query, so normal source SELECT and target CREATE checks remain.

ALTER FUNCTION pgtrickle.create_stream_table(
    TEXT, TEXT, TEXT, TEXT, bool, TEXT, TEXT, TEXT, bool, bool, TEXT, INT,
    double precision, TEXT, bool, TEXT, INT
) SECURITY DEFINER;
ALTER FUNCTION pgtrickle.create_stream_table(
    TEXT, TEXT, TEXT, TEXT, bool, TEXT, TEXT, TEXT, bool, bool, TEXT, INT,
    double precision, TEXT, bool, TEXT, INT
) SET search_path = pgtrickle, pg_catalog, pg_temp;

ALTER FUNCTION pgtrickle.create_stream_table_if_not_exists(
    TEXT, TEXT, TEXT, TEXT, bool, TEXT, TEXT, TEXT, bool, bool, TEXT, INT,
    double precision, TEXT, bool, TEXT, INT
) SECURITY DEFINER;
ALTER FUNCTION pgtrickle.create_stream_table_if_not_exists(
    TEXT, TEXT, TEXT, TEXT, bool, TEXT, TEXT, TEXT, bool, bool, TEXT, INT,
    double precision, TEXT, bool, TEXT, INT
) SET search_path = pgtrickle, pg_catalog, pg_temp;

ALTER FUNCTION pgtrickle.bulk_create(jsonb) SECURITY DEFINER;
ALTER FUNCTION pgtrickle.bulk_create(jsonb)
    SET search_path = pgtrickle, pg_catalog, pg_temp;

ALTER FUNCTION pgtrickle.create_stream_table_fast_append_only(
    TEXT, TEXT, TEXT, TEXT, TEXT, INT, double precision
) SECURITY DEFINER;
ALTER FUNCTION pgtrickle.create_stream_table_fast_append_only(
    TEXT, TEXT, TEXT, TEXT, TEXT, INT, double precision
) SET search_path = pgtrickle, pg_catalog, pg_temp;

ALTER FUNCTION pgtrickle.create_stream_table_realtime(
    TEXT, TEXT, TEXT, bool, TEXT, INT, double precision
) SECURITY DEFINER;
ALTER FUNCTION pgtrickle.create_stream_table_realtime(
    TEXT, TEXT, TEXT, bool, TEXT, INT, double precision
) SET search_path = pgtrickle, pg_catalog, pg_temp;

ALTER FUNCTION pgtrickle.create_stream_table_batch(
    TEXT, TEXT, TEXT, bool, TEXT, INT, double precision
) SECURITY DEFINER;
ALTER FUNCTION pgtrickle.create_stream_table_batch(
    TEXT, TEXT, TEXT, bool, TEXT, INT, double precision
) SET search_path = pgtrickle, pg_catalog, pg_temp;

ALTER FUNCTION pgtrickle.create_stream_table_cost_optimized(
    TEXT, TEXT, TEXT, bool, TEXT, INT, double precision
) SECURITY DEFINER;
ALTER FUNCTION pgtrickle.create_stream_table_cost_optimized(
    TEXT, TEXT, TEXT, bool, TEXT, INT, double precision
) SET search_path = pgtrickle, pg_catalog, pg_temp;

ALTER FUNCTION pgtrickle._on_ddl_end() SECURITY DEFINER;
ALTER FUNCTION pgtrickle._on_ddl_end()
    SET search_path = pgtrickle, pg_catalog, pg_temp;
REVOKE EXECUTE ON FUNCTION pgtrickle._on_ddl_end() FROM PUBLIC;

ALTER FUNCTION pgtrickle._on_sql_drop() SECURITY DEFINER;
ALTER FUNCTION pgtrickle._on_sql_drop()
    SET search_path = pgtrickle, pg_catalog, pg_temp;
REVOKE EXECUTE ON FUNCTION pgtrickle._on_sql_drop() FROM PUBLIC;
