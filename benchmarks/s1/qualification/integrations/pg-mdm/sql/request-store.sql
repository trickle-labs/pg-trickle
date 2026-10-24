CREATE TABLE IF NOT EXISTS pgreact_mdm.policy_intent_packages (
    policy_revision text PRIMARY KEY
        REFERENCES pgreact_mdm.policy_packages(policy_revision),
    policy_package jsonb NOT NULL,
    policy_digest bytea NOT NULL CHECK (octet_length(policy_digest) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

DROP TRIGGER IF EXISTS policy_intent_packages_immutable
ON pgreact_mdm.policy_intent_packages;
CREATE TRIGGER policy_intent_packages_immutable
BEFORE UPDATE OR DELETE ON pgreact_mdm.policy_intent_packages
FOR EACH ROW EXECUTE FUNCTION pgreact_mdm.reject_package_mutation();

CREATE OR REPLACE FUNCTION pgreact_mdm.normalized_intent_package(package jsonb)
RETURNS jsonb
LANGUAGE SQL
IMMUTABLE
STRICT
SET search_path = pg_catalog, pg_temp
AS $function$
    SELECT jsonb_strip_nulls(jsonb_build_object(
        'applicability', NULLIF(package -> 'applicability', '{}'::jsonb),
        'deadline', NULLIF(package -> 'deadline', 'null'::jsonb),
        'routes', COALESCE((
            SELECT jsonb_agg(route ORDER BY
                route ->> 'reason_code',
                COALESCE(route ->> 'entity_name', ''),
                COALESCE(route ->> 'priority', ''),
                route ->> 'queue',
                COALESCE(route ->> 'action', 'ASSIGN_QUEUE'),
                COALESCE(route ->> 'level', ''))
            FROM jsonb_array_elements(package -> 'routes') AS item(route)),
            '[]'::jsonb)))
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.publish_intent_package(
    policy_revision text,
    package jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    validation jsonb;
    read_package jsonb;
    read_digest bytea;
    normalized jsonb;
    digest bytea;
    stored pgreact_mdm.policy_intent_packages%ROWTYPE;
BEGIN
    read_package := CASE WHEN $2 ? 'deadline' THEN $2 ELSE $2 ||
        '{"deadline":{"duration_seconds":3600,"min_seconds":60,"max_seconds":86400,"replace_existing_deadline":false}}'::jsonb
        END;
    validation := pgreact_mdm.validate_package($1, read_package);
    IF validation ->> 'state' <> 'valid' THEN
        RAISE EXCEPTION 'MDM_POLICY_INVALID: %', validation -> 'findings';
    END IF;
    normalized := pgreact_mdm.normalized_package($2);
    read_digest := pgreact_mdm.framed_digest(
        'pg_react/mdm-stewardship-policy/v1',
        jsonb_build_object(
            'canonical_encoding_version', 1,
            'policy_package', normalized,
            'policy_revision', $1));
    INSERT INTO pgreact_mdm.policy_packages(
        policy_revision, policy_package, policy_digest)
    VALUES ($1, normalized, read_digest)
    ON CONFLICT ON CONSTRAINT policy_packages_pkey DO NOTHING;
    IF NOT EXISTS (
        SELECT 1 FROM pgreact_mdm.policy_packages AS saved_package
        WHERE saved_package.policy_revision = $1
          AND saved_package.policy_package IS NOT DISTINCT FROM normalized
          AND saved_package.policy_digest IS NOT DISTINCT FROM read_digest) THEN
        RAISE EXCEPTION 'MDM_POLICY_IMMUTABLE: policy revision % already has a different read package', $1;
    END IF;
    normalized := pgreact_mdm.normalized_intent_package($2);
    digest := pgreact_mdm.framed_digest(
        'pg_react/mdm-stewardship-policy/v1',
        jsonb_build_object(
            'canonical_encoding_version', 1,
            'policy_package', normalized,
            'policy_revision', $1));

    INSERT INTO pgreact_mdm.policy_intent_packages(
        policy_revision, policy_package, policy_digest)
    VALUES ($1, normalized, digest)
    ON CONFLICT ON CONSTRAINT policy_intent_packages_pkey DO NOTHING;

    SELECT * INTO STRICT stored
    FROM pgreact_mdm.policy_intent_packages
    WHERE policy_intent_packages.policy_revision = $1;
    IF stored.policy_package IS DISTINCT FROM normalized
       OR stored.policy_digest IS DISTINCT FROM digest THEN
        RAISE EXCEPTION 'MDM_POLICY_IMMUTABLE: intent policy revision % already has a different canonical digest',
            $1;
    END IF;

    RETURN jsonb_build_object(
        'state', 'published',
        'policy_revision', stored.policy_revision,
        'canonical_encoding_version', 1,
        'policy_package', stored.policy_package,
        'policy_digest', encode(stored.policy_digest, 'hex'));
END
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.intent_policy_document(policy_revision text)
RETURNS jsonb
LANGUAGE SQL
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
    SELECT jsonb_build_object(
        'canonical_encoding_version', 1,
        'policy_package', policy_package,
        'policy_revision', policy_revision,
        'policy_digest', encode(policy_digest, 'hex'))
    FROM pgreact_mdm.policy_intent_packages
    WHERE pgreact_mdm.policy_intent_packages.policy_revision = $1
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.request_key_vector(
    binding_id uuid,
    policy_revision text,
    case_key bigint,
    lifecycle_generation bigint,
    action_revision bigint,
    consequence_identity text,
    escalation_level integer
)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
STRICT
SET search_path = pg_catalog, pg_temp
AS $function$
BEGIN
    IF octet_length(policy_revision) NOT BETWEEN 1 AND 256
        OR case_key < 1
       OR lifecycle_generation < 1
       OR action_revision < 1
       OR octet_length(consequence_identity) NOT BETWEEN 1 AND 256
       OR escalation_level < 0 THEN
        RAISE EXCEPTION 'MDM_REQUEST_KEY_INVALID: invalid key vector';
    END IF;
    RETURN jsonb_build_object(
        'action_revision', action_revision,
        'binding_id', binding_id::text,
        'canonical_encoding_version', 1,
        'case_key', case_key,
        'consequence_identity', consequence_identity,
        'escalation_level', escalation_level,
        'lifecycle_generation', lifecycle_generation,
        'policy_revision', policy_revision);
END
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.intent_request_key(
    binding_id uuid,
    policy_revision text,
    case_key bigint,
    lifecycle_generation bigint,
    action_revision bigint,
    consequence_identity text,
    escalation_level integer
)
RETURNS bytea
LANGUAGE SQL
IMMUTABLE
STRICT
SET search_path = pg_catalog, pg_temp
AS $function$
    SELECT pgreact_mdm.framed_digest(
        'pg_react/mdm-stewardship-request-key/v1',
        pgreact_mdm.request_key_vector($1, $2, $3, $4, $5, $6, $7))
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.intent_request_body(
    binding_id uuid,
    case_key bigint,
    action text,
    arguments jsonb,
    expected_review_version bigint,
    expected_definition_version bigint,
    expected_publication_revision bigint,
    expected_stewardship_epoch bigint,
    expected_evidence_basis_digest bytea,
    expected_action_revision bigint,
    expected_policy_digest bytea,
    policy_revision text,
    evaluation_ref text,
    work_ref text
)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
STRICT
SET search_path = pg_catalog, pg_temp
AS $function$
BEGIN
    IF action NOT IN ('ASSIGN_QUEUE', 'SET_DUE_AT', 'ESCALATE')
       OR jsonb_typeof(arguments) IS DISTINCT FROM 'object'
       OR octet_length(policy_revision) NOT BETWEEN 1 AND 256
       OR octet_length(evaluation_ref) NOT BETWEEN 1 AND 256
       OR octet_length(work_ref) NOT BETWEEN 1 AND 256
       OR octet_length(expected_evidence_basis_digest) <> 32
       OR octet_length(expected_policy_digest) <> 32
       OR case_key < 1
       OR expected_review_version < 1
       OR expected_definition_version < 1
       OR expected_publication_revision < 0
       OR expected_stewardship_epoch < 0
       OR expected_action_revision < 1 THEN
        RAISE EXCEPTION 'MDM_INTENT_BODY_INVALID: invalid request body';
    END IF;

    IF action = 'ASSIGN_QUEUE'
       AND (arguments - 'queue' <> '{}'::jsonb
            OR jsonb_typeof(arguments -> 'queue') IS DISTINCT FROM 'string'
            OR octet_length(arguments ->> 'queue') NOT BETWEEN 1 AND 63) THEN
        RAISE EXCEPTION 'MDM_INTENT_BODY_INVALID: ASSIGN_QUEUE requires one queue';
    ELSIF action = 'SET_DUE_AT'
       AND (arguments - 'due_at' <> '{}'::jsonb
            OR jsonb_typeof(arguments -> 'due_at') IS DISTINCT FROM 'string'
            OR NOT pg_input_is_valid(arguments ->> 'due_at', 'timestamp with time zone')) THEN
        RAISE EXCEPTION 'MDM_INTENT_BODY_INVALID: SET_DUE_AT requires one valid due_at';
    ELSIF action = 'ESCALATE'
       AND (arguments - 'level' <> '{}'::jsonb
            OR jsonb_typeof(arguments -> 'level') IS DISTINCT FROM 'number'
            OR (arguments ->> 'level') !~ '^[1-9][0-9]*$') THEN
        RAISE EXCEPTION 'MDM_INTENT_BODY_INVALID: ESCALATE requires one positive integer level';
    END IF;

    RETURN jsonb_build_object(
        'action', action,
        'arguments', arguments,
        'binding_id', binding_id::text,
        'case_key', case_key,
        'evaluation_ref', evaluation_ref,
        'expected_action_revision', expected_action_revision,
        'expected_definition_version', expected_definition_version,
        'expected_evidence_basis_digest', encode(expected_evidence_basis_digest, 'hex'),
        'expected_policy_digest', encode(expected_policy_digest, 'hex'),
        'expected_publication_revision', expected_publication_revision,
        'expected_review_version', expected_review_version,
        'expected_stewardship_epoch', expected_stewardship_epoch,
        'policy_revision', policy_revision,
        'work_ref', work_ref);
END
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.intent_request_digest(request_body jsonb)
RETURNS bytea
LANGUAGE SQL
IMMUTABLE
STRICT
SET search_path = pg_catalog, pg_temp
AS $function$
    SELECT pgreact_mdm.framed_digest('pg_mdm/policy-intent/v1', $1)
$function$;

CREATE TABLE IF NOT EXISTS pgreact_mdm.intent_requests (
    binding_id uuid NOT NULL,
    request_key bytea NOT NULL CHECK (octet_length(request_key) = 32),
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    request_body jsonb NOT NULL,
    work_ref text NOT NULL UNIQUE,
    policy_revision text NOT NULL,
    case_key bigint NOT NULL CHECK (case_key > 0),
    lifecycle_generation bigint NOT NULL CHECK (lifecycle_generation > 0),
    action_revision bigint NOT NULL CHECK (action_revision > 0),
    consequence_identity text NOT NULL,
    escalation_level integer NOT NULL CHECK (escalation_level >= 0),
    first_episode_id bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (binding_id, request_key)
);
GRANT CREATE ON SCHEMA pgreact_mdm TO mdm_helper_owner;
ALTER TABLE pgreact_mdm.intent_requests OWNER TO mdm_helper_owner;

CREATE TABLE IF NOT EXISTS pgreact_mdm.intent_attempts (
    episode_id bigint NOT NULL,
    attempt_no integer NOT NULL CHECK (attempt_no > 0),
    binding_id uuid NOT NULL,
    request_key bytea NOT NULL CHECK (octet_length(request_key) = 32),
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    request_body jsonb NOT NULL,
    work_ref text NOT NULL,
    receipt_id uuid,
    outcome text NOT NULL,
    reason_code text,
    case_key bigint NOT NULL,
    action_revision bigint NOT NULL,
    control jsonb,
    resulting_publication_revision bigint,
    attempted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (work_ref, attempt_no),
    FOREIGN KEY (binding_id, request_key)
        REFERENCES pgreact_mdm.intent_requests(binding_id, request_key)
);
DO $migration$
DECLARE
    primary_key_name name;
    primary_key_definition text;
BEGIN
    SELECT conname, pg_catalog.pg_get_constraintdef(oid)
    INTO primary_key_name, primary_key_definition
    FROM pg_catalog.pg_constraint
    WHERE conrelid = 'pgreact_mdm.intent_attempts'::regclass
      AND contype = 'p';
    IF primary_key_definition = 'PRIMARY KEY (episode_id, attempt_no)' THEN
        EXECUTE pg_catalog.format(
            'ALTER TABLE pgreact_mdm.intent_attempts DROP CONSTRAINT %I',
            primary_key_name);
        ALTER TABLE pgreact_mdm.intent_attempts
            ADD CONSTRAINT intent_attempts_pkey PRIMARY KEY (work_ref, attempt_no);
    ELSIF primary_key_definition IS DISTINCT FROM 'PRIMARY KEY (work_ref, attempt_no)' THEN
        RAISE EXCEPTION 'unexpected intent_attempts primary key: %',
            primary_key_definition;
    END IF;
END;
$migration$;
ALTER TABLE pgreact_mdm.intent_attempts OWNER TO mdm_helper_owner;
REVOKE CREATE ON SCHEMA pgreact_mdm FROM mdm_helper_owner;

CREATE OR REPLACE FUNCTION pgreact_mdm.reject_intent_record_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $function$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        MESSAGE = 'MDM_INTENT_RECORD_RETAINED: request and attempt records are append-only';
END
$function$;
ALTER FUNCTION pgreact_mdm.reject_intent_record_mutation() OWNER TO mdm_helper_owner;
REVOKE ALL ON FUNCTION pgreact_mdm.reject_intent_record_mutation() FROM PUBLIC;

DROP TRIGGER IF EXISTS intent_requests_retained ON pgreact_mdm.intent_requests;
CREATE TRIGGER intent_requests_retained
BEFORE UPDATE OR DELETE ON pgreact_mdm.intent_requests
FOR EACH ROW EXECUTE FUNCTION pgreact_mdm.reject_intent_record_mutation();
DROP TRIGGER IF EXISTS intent_requests_no_truncate ON pgreact_mdm.intent_requests;
CREATE TRIGGER intent_requests_no_truncate
BEFORE TRUNCATE ON pgreact_mdm.intent_requests
FOR EACH STATEMENT EXECUTE FUNCTION pgreact_mdm.reject_intent_record_mutation();
DROP TRIGGER IF EXISTS intent_attempts_retained ON pgreact_mdm.intent_attempts;
CREATE TRIGGER intent_attempts_retained
BEFORE UPDATE OR DELETE ON pgreact_mdm.intent_attempts
FOR EACH ROW EXECUTE FUNCTION pgreact_mdm.reject_intent_record_mutation();
DROP TRIGGER IF EXISTS intent_attempts_no_truncate ON pgreact_mdm.intent_attempts;
CREATE TRIGGER intent_attempts_no_truncate
BEFORE TRUNCATE ON pgreact_mdm.intent_attempts
FOR EACH STATEMENT EXECUTE FUNCTION pgreact_mdm.reject_intent_record_mutation();

REVOKE ALL ON TABLE pgreact_mdm.policy_intent_packages,
    pgreact_mdm.intent_requests,
    pgreact_mdm.intent_attempts FROM PUBLIC;
REVOKE ALL ON FUNCTION pgreact_mdm.normalized_intent_package(jsonb),
    pgreact_mdm.publish_intent_package(text, jsonb),
    pgreact_mdm.intent_policy_document(text),
    pgreact_mdm.request_key_vector(uuid, text, bigint, bigint, bigint, text, integer),
    pgreact_mdm.intent_request_key(uuid, text, bigint, bigint, bigint, text, integer),
    pgreact_mdm.intent_request_body(uuid, bigint, text, jsonb, bigint, bigint, bigint, bigint, bytea, bigint, bytea, text, text, text),
    pgreact_mdm.intent_request_digest(jsonb) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION pgreact_mdm.intent_policy_document(text),
    pgreact_mdm.publish_intent_package(text, jsonb),
    pgreact_mdm.request_key_vector(uuid, text, bigint, bigint, bigint, text, integer),
    pgreact_mdm.intent_request_key(uuid, text, bigint, bigint, bigint, text, integer),
    pgreact_mdm.intent_request_body(uuid, bigint, text, jsonb, bigint, bigint, bigint, bigint, bytea, bigint, bytea, text, text, text),
    pgreact_mdm.intent_request_digest(jsonb) TO PUBLIC;
