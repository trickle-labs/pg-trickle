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
