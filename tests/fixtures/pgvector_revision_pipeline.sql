CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA embedding_demo;

CREATE TABLE embedding_demo.documents (
    id bigint PRIMARY KEY,
    incarnation uuid NOT NULL DEFAULT gen_random_uuid(),
    revision uuid NOT NULL DEFAULT gen_random_uuid(),
    body text NOT NULL,
    chunk_version integer NOT NULL DEFAULT 1,
    metadata text,
    published boolean NOT NULL DEFAULT true
);

CREATE FUNCTION embedding_demo.advance_revision() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        NEW.incarnation := gen_random_uuid();
        NEW.revision := gen_random_uuid();
        RETURN NEW;
    END IF;
    NEW.incarnation := OLD.incarnation;
    NEW.revision := OLD.revision;
    IF (NEW.body, NEW.chunk_version) IS DISTINCT FROM (OLD.body, OLD.chunk_version) THEN
        NEW.revision := gen_random_uuid();
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER documents_revision BEFORE INSERT OR UPDATE ON embedding_demo.documents
    FOR EACH ROW EXECUTE FUNCTION embedding_demo.advance_revision();

CREATE TABLE embedding_demo.jobs (
    job_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id bigint NOT NULL REFERENCES embedding_demo.documents(id) ON DELETE CASCADE,
    incarnation uuid NOT NULL,
    revision uuid NOT NULL,
    chunk_version integer NOT NULL,
    model_version text NOT NULL,
    state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'running', 'failed', 'done')),
    attempts integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_until timestamptz,
    claim_token uuid,
    last_error text,
    UNIQUE (incarnation, revision, chunk_version, model_version)
);
CREATE TABLE embedding_demo.results (
    document_id bigint NOT NULL REFERENCES embedding_demo.documents(id) ON DELETE CASCADE,
    incarnation uuid NOT NULL,
    revision uuid NOT NULL,
    chunk_version integer NOT NULL,
    model_version text NOT NULL,
    embedding vector(3) NOT NULL,
    PRIMARY KEY (incarnation, revision, chunk_version, model_version)
);

-- Called in the same application transaction as the source change.
CREATE FUNCTION embedding_demo.enqueue(p_document_id bigint, p_model text) RETURNS void
LANGUAGE sql AS $$
    INSERT INTO embedding_demo.jobs (document_id, incarnation, revision, chunk_version, model_version)
    SELECT id, incarnation, revision, chunk_version, p_model
    FROM embedding_demo.documents WHERE id = p_document_id AND published
    ON CONFLICT (incarnation, revision, chunk_version, model_version) DO NOTHING
$$;

-- Commit the claim before calling a provider. Expired claims may be reclaimed.
CREATE FUNCTION embedding_demo.claim(p_model text, p_lease interval)
RETURNS TABLE (claimed_job uuid, claimed_token uuid, source_body text)
LANGUAGE sql AS $$
    WITH candidate AS MATERIALIZED (
        SELECT j.job_id, d.body
        FROM embedding_demo.jobs j JOIN embedding_demo.documents d ON d.id = j.document_id
        WHERE j.model_version = p_model AND d.published
          AND (j.incarnation, j.revision, j.chunk_version) = (d.incarnation, d.revision, d.chunk_version)
          AND ((j.state IN ('pending', 'failed') AND j.available_at <= clock_timestamp())
               OR (j.state = 'running' AND j.lease_until <= clock_timestamp()))
        ORDER BY j.available_at, j.job_id
        FOR UPDATE OF j SKIP LOCKED LIMIT 1
    ), claimed AS (
        UPDATE embedding_demo.jobs j
        SET state = 'running', claim_token = gen_random_uuid(),
            lease_until = clock_timestamp() + p_lease, attempts = attempts + 1, last_error = NULL
        FROM candidate c WHERE j.job_id = c.job_id
        RETURNING j.job_id, j.claim_token
    )
    SELECT c.job_id, c.claim_token, candidate.body
    FROM claimed c JOIN candidate USING (job_id)
$$;

-- A stale, expired, deleted, or already completed claim cannot publish a result.
CREATE FUNCTION embedding_demo.complete(p_job uuid, p_token uuid, p_embedding vector)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE current_job embedding_demo.jobs%ROWTYPE;
BEGIN
    SELECT j.* INTO current_job
    FROM embedding_demo.jobs j JOIN embedding_demo.documents d ON d.id = j.document_id
    WHERE j.job_id = p_job AND j.claim_token = p_token AND j.state = 'running'
      AND j.lease_until > clock_timestamp() AND d.published
      AND (j.incarnation, j.revision, j.chunk_version) = (d.incarnation, d.revision, d.chunk_version)
    FOR UPDATE OF j, d;
    IF NOT FOUND THEN RETURN false; END IF;
    INSERT INTO embedding_demo.results
        (document_id, incarnation, revision, chunk_version, model_version, embedding)
    VALUES (current_job.document_id, current_job.incarnation, current_job.revision,
            current_job.chunk_version, current_job.model_version, p_embedding)
    ON CONFLICT (incarnation, revision, chunk_version, model_version) DO NOTHING;
    UPDATE embedding_demo.jobs SET state = 'done', lease_until = NULL, claim_token = NULL
    WHERE job_id = p_job;
    RETURN true;
END $$;

-- Record a sanitized error and retry later; a replaced claim cannot alter the job.
CREATE FUNCTION embedding_demo.fail(p_job uuid, p_token uuid, p_error text, p_retry interval)
RETURNS boolean LANGUAGE sql AS $$
    WITH failed AS (
        UPDATE embedding_demo.jobs
        SET state = 'failed', last_error = p_error, available_at = clock_timestamp() + p_retry,
            lease_until = NULL, claim_token = NULL
        WHERE job_id = p_job AND claim_token = p_token AND state = 'running'
          AND lease_until > clock_timestamp()
        RETURNING 1
    ) SELECT EXISTS (SELECT 1 FROM failed)
$$;

SELECT pgtrickle.create_stream_table(
    'embedding_demo.corpus_v1',
    $$SELECT d.id, d.body, e.incarnation, e.revision, e.chunk_version, e.model_version, e.embedding
      FROM embedding_demo.documents d JOIN embedding_demo.results e
        ON e.document_id = d.id AND e.incarnation = d.incarnation
       AND e.revision = d.revision AND e.chunk_version = d.chunk_version
      WHERE d.published AND e.model_version = 'model-v1'$$,
    '1h', 'DIFFERENTIAL'
);
SELECT pgtrickle.create_stream_table(
    'embedding_demo.corpus_v2',
    $$SELECT d.id, d.body, e.incarnation, e.revision, e.chunk_version, e.model_version, e.embedding
      FROM embedding_demo.documents d JOIN embedding_demo.results e
        ON e.document_id = d.id AND e.incarnation = d.incarnation
       AND e.revision = d.revision AND e.chunk_version = d.chunk_version
      WHERE d.published AND e.model_version = 'model-v2'$$,
    '1h', 'DIFFERENTIAL'
);
CREATE INDEX corpus_v1_ann ON embedding_demo.corpus_v1 USING hnsw ((embedding::vector(3)) vector_cosine_ops);
CREATE INDEX corpus_v2_ann ON embedding_demo.corpus_v2 USING hnsw ((embedding::vector(3)) vector_cosine_ops);
CREATE VIEW embedding_demo.search_corpus AS SELECT id, body, embedding FROM embedding_demo.corpus_v1;
