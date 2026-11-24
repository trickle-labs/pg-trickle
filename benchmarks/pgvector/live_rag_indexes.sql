CREATE INDEX application_ann ON live_rag.application USING hnsw ((embedding::vector(3)) vector_cosine_ops);
CREATE INDEX periodic_full_ann ON live_rag.periodic_full USING hnsw ((embedding::vector(3)) vector_cosine_ops);
CREATE INDEX differential_ann ON live_rag.differential USING hnsw ((embedding::vector(3)) vector_cosine_ops);
CREATE INDEX application_fts ON live_rag.application USING gin (to_tsvector('simple', body));
CREATE INDEX periodic_full_fts ON live_rag.periodic_full USING gin (to_tsvector('simple', body));
CREATE INDEX differential_fts ON live_rag.differential USING gin (to_tsvector('simple', body));
