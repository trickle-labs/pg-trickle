-- Run after tests/fixtures/pgvector_revision_pipeline.sql in a disposable DB.
CREATE SCHEMA live_rag;
CREATE TABLE live_rag.access (
    document_id bigint PRIMARY KEY REFERENCES embedding_demo.documents(id) ON DELETE CASCADE,
    tenant_id bigint NOT NULL,
    visible boolean NOT NULL DEFAULT true,
    model_version text NOT NULL DEFAULT 'model-v1'
);

-- This is the authoritative eligible corpus, not an approximate search result.
CREATE VIEW live_rag.truth AS
SELECT d.id, a.tenant_id, d.body, e.incarnation, e.revision,
       e.chunk_version, e.model_version, e.embedding::vector AS embedding
FROM embedding_demo.documents d
JOIN live_rag.access a ON a.document_id = d.id
JOIN embedding_demo.results e ON e.document_id = d.id
 AND e.incarnation = d.incarnation AND e.revision = d.revision
 AND e.chunk_version = d.chunk_version AND e.model_version = a.model_version
WHERE d.published AND a.visible;

CREATE TABLE live_rag.application AS SELECT * FROM live_rag.truth WITH NO DATA;
CREATE TABLE live_rag.periodic_full AS SELECT * FROM live_rag.truth WITH NO DATA;
CREATE UNIQUE INDEX application_id ON live_rag.application(id);
CREATE UNIQUE INDEX periodic_full_id ON live_rag.periodic_full(id);

SELECT pgtrickle.create_stream_table(
    'live_rag.differential', 'SELECT * FROM live_rag.truth', '1h', 'DIFFERENTIAL'
);

-- Application-maintained baseline: one batched delete/upsert for changed IDs.
CREATE FUNCTION live_rag.sync_application(p_ids bigint[]) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM live_rag.application c WHERE c.id = ANY(p_ids)
      AND NOT EXISTS (SELECT 1 FROM live_rag.truth t WHERE t.id = c.id);
    INSERT INTO live_rag.application
      SELECT * FROM live_rag.truth WHERE id = ANY(p_ids)
      ON CONFLICT (id) DO UPDATE SET
        tenant_id = EXCLUDED.tenant_id, body = EXCLUDED.body,
        incarnation = EXCLUDED.incarnation, revision = EXCLUDED.revision,
        chunk_version = EXCLUDED.chunk_version, model_version = EXCLUDED.model_version,
        embedding = EXCLUDED.embedding;
END $$;

CREATE FUNCTION live_rag.recompute_full(p_dimensions integer DEFAULT 3) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    rebuild_ann boolean := to_regclass('live_rag.periodic_full_ann') IS NOT NULL;
    rebuild_fts boolean := to_regclass('live_rag.periodic_full_fts') IS NOT NULL;
BEGIN
    IF p_dimensions NOT BETWEEN 1 AND 1536 THEN
        RAISE EXCEPTION 'vector dimensions must be between 1 and 1536';
    END IF;
    IF rebuild_ann THEN
        DROP INDEX live_rag.periodic_full_ann;
    END IF;
    IF rebuild_fts THEN
        DROP INDEX live_rag.periodic_full_fts;
    END IF;
    TRUNCATE live_rag.periodic_full;
    INSERT INTO live_rag.periodic_full SELECT * FROM live_rag.truth;
    -- A full corpus rebuild is faster with one bulk ANN/text index build.
    IF rebuild_ann THEN
        EXECUTE format(
            'CREATE INDEX periodic_full_ann ON live_rag.periodic_full USING hnsw ((embedding::vector(%s)) vector_cosine_ops)',
            p_dimensions
        );
    END IF;
    IF rebuild_fts THEN
        CREATE INDEX periodic_full_fts ON live_rag.periodic_full USING gin (to_tsvector('simple', body));
    END IF;
    ANALYZE live_rag.periodic_full;
END $$;

-- Search rechecks the authoritative revision, visibility and embedding while refresh lags.
CREATE VIEW live_rag.application_current AS
SELECT c.* FROM live_rag.application c JOIN live_rag.truth t
  ON (c.id, c.tenant_id, c.body, c.incarnation, c.revision, c.chunk_version, c.model_version, c.embedding)
   = (t.id, t.tenant_id, t.body, t.incarnation, t.revision, t.chunk_version, t.model_version, t.embedding);
CREATE VIEW live_rag.periodic_full_current AS
SELECT c.* FROM live_rag.periodic_full c JOIN live_rag.truth t
  ON (c.id, c.tenant_id, c.body, c.incarnation, c.revision, c.chunk_version, c.model_version, c.embedding)
   = (t.id, t.tenant_id, t.body, t.incarnation, t.revision, t.chunk_version, t.model_version, t.embedding);
CREATE VIEW live_rag.differential_current AS
SELECT c.* FROM live_rag.differential c JOIN live_rag.truth t
  ON (c.id, c.tenant_id, c.body, c.incarnation, c.revision, c.chunk_version, c.model_version, c.embedding)
   = (t.id, t.tenant_id, t.body, t.incarnation, t.revision, t.chunk_version, t.model_version, t.embedding);

-- Provision role OIDs as an administrator; readers cannot edit this mapping.
CREATE TABLE live_rag.tenant_readers (
    role_oid oid NOT NULL,
    tenant_id bigint NOT NULL,
    PRIMARY KEY (role_oid, tenant_id)
);
CREATE VIEW live_rag.application_authorized WITH (security_barrier = true) AS
SELECT c.* FROM live_rag.application_current c
WHERE EXISTS (SELECT 1 FROM live_rag.tenant_readers r
              WHERE r.role_oid = current_user::regrole::oid AND r.tenant_id = c.tenant_id);
CREATE VIEW live_rag.periodic_full_authorized WITH (security_barrier = true) AS
SELECT c.* FROM live_rag.periodic_full_current c
WHERE EXISTS (SELECT 1 FROM live_rag.tenant_readers r
              WHERE r.role_oid = current_user::regrole::oid AND r.tenant_id = c.tenant_id);
CREATE VIEW live_rag.differential_authorized WITH (security_barrier = true) AS
SELECT c.* FROM live_rag.differential_current c
WHERE EXISTS (SELECT 1 FROM live_rag.tenant_readers r
              WHERE r.role_oid = current_user::regrole::oid AND r.tenant_id = c.tenant_id);
REVOKE ALL ON ALL TABLES IN SCHEMA live_rag FROM PUBLIC;
