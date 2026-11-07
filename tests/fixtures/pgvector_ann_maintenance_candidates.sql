-- Bind $1 to the stream-table name as regclass. Execute as the maintenance role.
WITH RECURSIVE storage(relid) AS (
    SELECT s.pgt_relid
    FROM pgtrickle.pgt_stream_tables s
    JOIN pg_class c ON c.oid = s.pgt_relid
    WHERE s.pgt_relid = $1::text::regclass
      AND has_schema_privilege(c.relnamespace, 'USAGE')
      AND has_table_privilege(c.oid, 'SELECT')
    UNION ALL
    SELECT i.inhrelid
    FROM pg_inherits i JOIN storage parent ON parent.relid = i.inhparent
)
SELECT idx.oid::bigint AS index_oid,
       format('%I.%I', ns.nspname, idx.relname) AS index_name,
       am.amname::text AS access_method
FROM storage s
JOIN pg_index i ON i.indrelid = s.relid
JOIN pg_class idx ON idx.oid = i.indexrelid
JOIN pg_namespace ns ON ns.oid = idx.relnamespace
JOIN pg_am am ON am.oid = idx.relam
WHERE idx.relkind = 'i' AND am.amname IN ('hnsw', 'ivfflat')
  AND i.indisvalid AND i.indisready AND i.indislive
  AND has_schema_privilege(ns.oid, 'USAGE')
  AND has_table_privilege(i.indrelid, 'MAINTAIN')
ORDER BY idx.oid;
