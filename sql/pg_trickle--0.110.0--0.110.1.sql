-- pg_trickle v0.110.0 -> v0.110.1
-- #1418: preserve the pre-DROP child OIDs of tracked partitioned sources.
--
-- sql_drop fires after PostgreSQL deletes pg_inherits, so a dropped leaf OID
-- cannot be mapped to its parent from the live catalogs. Backfill existing
-- dependency snapshots without changing their schema_fingerprint; the Rust
-- code stores descendant OIDs outside the legacy fingerprint input.
--
-- Only previously tracked partitioned/inherited roots are updated. Source
-- tables with no children receive [] so a later empty ATTACH can extend it.

WITH RECURSIVE tracked_roots AS (
    SELECT DISTINCT d.source_relid AS root_oid
    FROM pgtrickle.pgt_dependencies d
    JOIN pg_catalog.pg_class root ON root.oid = d.source_relid
    WHERE d.source_type = 'TABLE'
      AND (root.relkind = 'p' OR EXISTS (
          SELECT 1 FROM pg_catalog.pg_inherits i
          WHERE i.inhparent = root.oid
      ))
),
descendants(root_oid, child_oid) AS (
    SELECT roots.root_oid, i.inhrelid
    FROM tracked_roots roots
    JOIN pg_catalog.pg_inherits i ON i.inhparent = roots.root_oid
    UNION ALL
    SELECT d.root_oid, i.inhrelid
    FROM descendants d
    JOIN pg_catalog.pg_inherits i ON i.inhparent = d.child_oid
),
child_sets AS (
    SELECT root_oid,
           pg_catalog.to_jsonb(
               array_agg(DISTINCT child_oid::bigint ORDER BY child_oid::bigint)
           ) AS child_oids
    FROM descendants
    GROUP BY root_oid
)
UPDATE pgtrickle.pgt_dependencies dep
SET column_snapshot = pg_catalog.jsonb_set(
    dep.column_snapshot,
    '{partition_descendant_oids}',
    COALESCE(children.child_oids, '[]'::jsonb),
    true
)
FROM tracked_roots roots
LEFT JOIN child_sets children ON children.root_oid = roots.root_oid
WHERE dep.source_relid = roots.root_oid
  AND dep.source_type = 'TABLE'
  AND pg_catalog.jsonb_typeof(dep.column_snapshot) = 'object';

-- Install the pre-DDL hook used for protected partition cuts and for the
-- first phase of concurrent DETACH. Existing streams remain unconfigured.
CREATE OR REPLACE FUNCTION pgtrickle._on_ddl_start()
RETURNS event_trigger
LANGUAGE c
AS 'MODULE_PATHNAME', 'pg_trickle_on_ddl_start_wrapper';
REVOKE EXECUTE ON FUNCTION pgtrickle._on_ddl_start() FROM PUBLIC;

DO $migration$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_event_trigger
        WHERE evtname = 'pg_trickle_ddl_start_tracker'
          AND (evtevent <> 'ddl_command_start'
               OR evtfoid <> 'pgtrickle._on_ddl_start()'::pg_catalog.regprocedure)
    ) THEN
        RAISE EXCEPTION 'event trigger pg_trickle_ddl_start_tracker has an unexpected definition';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_event_trigger
        WHERE evtname = 'pg_trickle_ddl_start_tracker'
    ) THEN
        CREATE EVENT TRIGGER pg_trickle_ddl_start_tracker
            ON ddl_command_start EXECUTE FUNCTION pgtrickle._on_ddl_start();
    END IF;
END
$migration$;

CREATE OR REPLACE FUNCTION pgtrickle.set_stream_table_partition_retention(
    name text, policy text, source_column text DEFAULT NULL, cutoff text DEFAULT NULL
) RETURNS void
LANGUAGE c SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'set_stream_table_partition_retention_wrapper';

CREATE OR REPLACE FUNCTION pgtrickle.get_stream_table_partition_retention(name text)
RETURNS jsonb
LANGUAGE c STRICT SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'get_stream_table_partition_retention_wrapper';

CREATE OR REPLACE FUNCTION pgtrickle.create_stream_table_with_retention(
    name text, query text, policy text, source_column text, cutoff text,
    schedule text DEFAULT 'calculated'
) RETURNS void
LANGUAGE c SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'create_stream_table_with_retention_wrapper';

REVOKE EXECUTE ON FUNCTION pgtrickle.set_stream_table_partition_retention(text, text, text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.get_stream_table_partition_retention(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.create_stream_table_with_retention(text, text, text, text, text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.set_stream_table_partition_retention(text, text, text, text) TO pgtrickle_admin;
GRANT EXECUTE ON FUNCTION pgtrickle.get_stream_table_partition_retention(text) TO pgtrickle_admin;
GRANT EXECUTE ON FUNCTION pgtrickle.create_stream_table_with_retention(text, text, text, text, text, text) TO pgtrickle_admin;
