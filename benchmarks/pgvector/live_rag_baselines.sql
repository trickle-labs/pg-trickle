-- Run after tests/fixtures/pgvector_revision_pipeline.sql in a disposable DB.
CREATE SCHEMA live_rag;
CREATE TABLE live_rag.access (
    document_id bigint PRIMARY KEY REFERENCES embedding_demo.documents(id) ON DELETE CASCADE,
    tenant_id bigint NOT NULL,
    visible boolean NOT NULL DEFAULT true
);

-- This is the authoritative eligible corpus, not an approximate search result.
CREATE VIEW live_rag.truth AS
SELECT d.id, a.tenant_id, d.body, e.incarnation, e.revision,
       e.chunk_version, e.model_version, e.embedding::vector AS embedding
FROM embedding_demo.documents d
JOIN live_rag.access a ON a.document_id = d.id
JOIN embedding_demo.results e ON e.document_id = d.id
 AND e.incarnation = d.incarnation AND e.revision = d.revision
 AND e.chunk_version = d.chunk_version
WHERE d.published AND a.visible AND e.model_version = 'model-v1';

CREATE TABLE live_rag.application AS SELECT * FROM live_rag.truth WITH NO DATA;
CREATE TABLE live_rag.periodic_full AS SELECT * FROM live_rag.truth WITH NO DATA;
CREATE UNIQUE INDEX application_id ON live_rag.application(id);
CREATE UNIQUE INDEX periodic_full_id ON live_rag.periodic_full(id);

SELECT pgtrickle.create_stream_table(
    'live_rag.differential', 'SELECT * FROM live_rag.truth', '1h', 'DIFFERENTIAL'
);

CREATE INDEX application_ann ON live_rag.application USING hnsw ((embedding::vector(3)) vector_cosine_ops);
CREATE INDEX periodic_full_ann ON live_rag.periodic_full USING hnsw ((embedding::vector(3)) vector_cosine_ops);
CREATE INDEX differential_ann ON live_rag.differential USING hnsw ((embedding::vector(3)) vector_cosine_ops);
CREATE INDEX application_fts ON live_rag.application USING gin (to_tsvector('simple', body));
CREATE INDEX periodic_full_fts ON live_rag.periodic_full USING gin (to_tsvector('simple', body));
CREATE INDEX differential_fts ON live_rag.differential USING gin (to_tsvector('simple', body));

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

CREATE FUNCTION live_rag.recompute_full() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    TRUNCATE live_rag.periodic_full;
    INSERT INTO live_rag.periodic_full SELECT * FROM live_rag.truth;
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
