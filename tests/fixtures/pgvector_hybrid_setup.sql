CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE hybrid_documents (
    tenant_id uuid NOT NULL,
    id bigint NOT NULL,
    title text NOT NULL,
    body text NOT NULL,
    visible boolean NOT NULL,
    PRIMARY KEY (tenant_id, id)
);
CREATE TABLE hybrid_embeddings (
    tenant_id uuid NOT NULL,
    document_id bigint NOT NULL,
    embedding vector(3) NOT NULL,
    PRIMARY KEY (tenant_id, document_id),
    FOREIGN KEY (tenant_id, document_id) REFERENCES hybrid_documents (tenant_id, id)
);

SELECT pgtrickle.create_stream_table(
    'hybrid_search_corpus',
    $$SELECT d.tenant_id, d.id, d.title, d.body, e.embedding,
             to_tsvector('english'::regconfig, d.title || ' ' || d.body) AS fts
      FROM hybrid_documents d JOIN hybrid_embeddings e
        ON e.tenant_id = d.tenant_id AND e.document_id = d.id
      WHERE d.visible$$,
    '1h', 'DIFFERENTIAL'
);
CREATE INDEX hybrid_search_fts_idx ON hybrid_search_corpus USING gin (fts);
CREATE INDEX hybrid_search_cosine_idx ON hybrid_search_corpus
    USING hnsw ((embedding::vector(3)) vector_cosine_ops);

ALTER TABLE hybrid_search_corpus ENABLE ROW LEVEL SECURITY;
ALTER TABLE hybrid_search_corpus FORCE ROW LEVEL SECURITY;
CREATE POLICY hybrid_tenant_policy ON hybrid_search_corpus FOR SELECT
    USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
