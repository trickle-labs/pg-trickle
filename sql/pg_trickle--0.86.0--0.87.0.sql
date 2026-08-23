-- v0.87.0: low-impact refresh execution and scheduler policy.
--
-- The new GUCs are registered by the shared library. No persistent catalog
-- shape changes are required: pipeline batches remain inside one outer
-- transaction and do not create durable continuation state.

INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES ('0.87.0', 'Bounded refresh pipeline, memory budget, and scheduler tenancy')
ON CONFLICT (version) DO NOTHING;
