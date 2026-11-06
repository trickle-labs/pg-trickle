CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trickle;

CREATE TABLE documents (
    id bigint PRIMARY KEY,
    content text NOT NULL,
    embedding vector(3)
);
INSERT INTO documents VALUES
    (1, 'Hello world', '[1,0,0]'),
    (2, 'Another document', '[0,1,0]');

SELECT * FROM pgtrickle.embedding_stream_table(
    'doc_corpus', 'documents', 'embedding', refresh_interval => '1m'
);
SELECT pgtrickle.refresh_stream_table('doc_corpus');

SELECT id, content, embedding::vector(3) <-> '[1,0,0]'::vector AS distance
FROM doc_corpus
ORDER BY embedding::vector(3) <-> '[1,0,0]'::vector
LIMIT 5;
