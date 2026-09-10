-- pg_trickle 0.101.0 -> 0.102.0 upgrade migration
--
-- v0.102.0 adds durable output-sensitive refresh evidence through the
-- existing refresh history detail field. No catalog columns or SQL objects
-- are added, so existing installations need no schema change.

SELECT 1;
