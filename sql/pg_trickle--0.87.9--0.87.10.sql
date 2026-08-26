-- pg_trickle 0.87.9 -> 0.87.10 upgrade migration
-- LSEC-10..LSEC-12: complete owner policy, atomic bulk lifecycle, and
-- fail-closed upgrade preflight.
--
-- The preflight runs before any ALTER FUNCTION or CREATE FUNCTION statement.
-- It is read-only; an exception aborts the extension update transaction and
-- leaves the installed catalog unchanged until the listed grants are fixed.

DO $$
DECLARE
    missing text;
BEGIN
    SELECT string_agg(
        format(
            'stream table %I.%I (owner %I), source %I.%I: missing %s. Remediation: %s',
            stream_schema,
            stream_name,
            owner_name,
            source_schema,
            source_name,
            array_to_string(missing_privileges, ', '),
            concat(
                CASE WHEN missing_select THEN
                    format('GRANT SELECT ON TABLE %I.%I TO %I; ', source_schema, source_name, owner_name)
                ELSE '' END,
                CASE WHEN missing_usage THEN
                    format('GRANT USAGE ON SCHEMA %I TO %I;', source_schema, owner_name)
                ELSE '' END
            )
        ),
        E'\n'
    ) INTO missing
    FROM (
        SELECT st.pgt_schema AS stream_schema,
               st.pgt_name AS stream_name,
               pg_get_userbyid(storage.relowner)::text AS owner_name,
               source_ns.nspname::text AS source_schema,
               source.relname::text AS source_name,
               NOT has_table_privilege(pg_get_userbyid(storage.relowner), source.oid, 'SELECT') AS missing_select,
               NOT has_schema_privilege(pg_get_userbyid(storage.relowner), source_ns.oid, 'USAGE') AS missing_usage,
               ARRAY_REMOVE(ARRAY[
                   CASE WHEN NOT has_table_privilege(pg_get_userbyid(storage.relowner), source.oid, 'SELECT') THEN 'SELECT' END,
                   CASE WHEN NOT has_schema_privilege(pg_get_userbyid(storage.relowner), source_ns.oid, 'USAGE') THEN 'USAGE ON SCHEMA' END
               ], NULL) AS missing_privileges
          FROM pgtrickle.pgt_stream_tables st
          JOIN pg_catalog.pg_class storage ON storage.oid = st.pgt_relid
          JOIN pgtrickle.pgt_dependencies dep ON dep.pgt_id = st.pgt_id
          JOIN pg_catalog.pg_class source ON source.oid = dep.source_relid
          JOIN pg_catalog.pg_namespace source_ns ON source_ns.oid = source.relnamespace
         WHERE dep.source_type IN ('TABLE', 'STREAM_TABLE', 'VIEW', 'FOREIGN_TABLE', 'MATVIEW')
    ) gaps
    WHERE missing_select OR missing_usage;

    IF missing IS NOT NULL THEN
        RAISE EXCEPTION
            'pg_trickle 0.87.10 upgrade preflight failed; fix these owner privileges before retrying:%',
            E'\n' || missing
            USING HINT = 'Apply the exact GRANT statements in the remediation text and rerun ALTER EXTENSION pg_trickle UPDATE.';
    END IF;
END
$$;

-- ALTER FUNCTION preserves existing ACLs. The Rust entry points capture the
-- outer caller and check the canonical stream-table owner before mutation.
ALTER FUNCTION pgtrickle.bulk_alter_stream_tables(text[], json)
    SECURITY DEFINER;
ALTER FUNCTION pgtrickle.bulk_alter_stream_tables(text[], json)
    SET search_path = pgtrickle, pg_catalog, pg_temp;
ALTER FUNCTION pgtrickle.bulk_drop_stream_tables(text[])
    SECURITY DEFINER;
ALTER FUNCTION pgtrickle.bulk_drop_stream_tables(text[])
    SET search_path = pgtrickle, pg_catalog, pg_temp;
ALTER FUNCTION pgtrickle.pause_stream_table(text)
    SECURITY DEFINER;
ALTER FUNCTION pgtrickle.pause_stream_table(text)
    SET search_path = pgtrickle, pg_catalog, pg_temp;
ALTER FUNCTION pgtrickle.refresh_stream_table(text)
    SECURITY DEFINER;
ALTER FUNCTION pgtrickle.refresh_stream_table(text)
    SET search_path = pgtrickle, pg_catalog, pg_temp;
ALTER FUNCTION pgtrickle.repair_stream_table(text)
    SECURITY DEFINER;
ALTER FUNCTION pgtrickle.repair_stream_table(text)
    SET search_path = pgtrickle, pg_catalog, pg_temp;
ALTER FUNCTION pgtrickle.reset_fuse(text, text)
    SECURITY DEFINER;
ALTER FUNCTION pgtrickle.reset_fuse(text, text)
    SET search_path = pgtrickle, pg_catalog, pg_temp;
ALTER FUNCTION pgtrickle.resume_stream_table(text)
    SECURITY DEFINER;
ALTER FUNCTION pgtrickle.resume_stream_table(text)
    SET search_path = pgtrickle, pg_catalog, pg_temp;
ALTER FUNCTION pgtrickle.set_stream_table_refresh_policy(text, text)
    SECURITY DEFINER;
ALTER FUNCTION pgtrickle.set_stream_table_refresh_policy(text, text)
    SET search_path = pgtrickle, pg_catalog, pg_temp;
ALTER FUNCTION pgtrickle.set_stream_table_storage_policy(text, boolean, text)
    SECURITY DEFINER;
ALTER FUNCTION pgtrickle.set_stream_table_storage_policy(text, boolean, text)
    SET search_path = pgtrickle, pg_catalog, pg_temp;
ALTER FUNCTION pgtrickle.stat_reset(bigint)
    SECURITY DEFINER;
ALTER FUNCTION pgtrickle.stat_reset(bigint)
    SET search_path = pgtrickle, pg_catalog, pg_temp;
ALTER FUNCTION pgtrickle.set_stream_table_sla(text, interval)
    SECURITY DEFINER;
ALTER FUNCTION pgtrickle.set_stream_table_sla(text, interval)
    SET search_path = pgtrickle, pg_catalog, pg_temp;

-- Added in 0.87.10; fresh installs get this from the generated archive.
CREATE FUNCTION pgtrickle."lifecycle_preflight"()
    RETURNS text
    STRICT
    LANGUAGE c
    AS 'MODULE_PATHNAME', 'lifecycle_preflight_wrapper';
REVOKE EXECUTE ON FUNCTION pgtrickle.lifecycle_preflight() FROM PUBLIC;
