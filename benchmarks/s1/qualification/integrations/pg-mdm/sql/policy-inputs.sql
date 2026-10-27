-- v0.47.0 read-only MDM policy package.
-- The source relation is supplied by the caller so fixture qualification never
-- impersonates mdm_steward.policy_cases_v1.

CREATE SCHEMA IF NOT EXISTS pgreact_mdm;

DO $$
BEGIN
    IF to_regtype('pgreact_mdm.policy_case') IS NULL THEN
        CREATE TYPE pgreact_mdm.policy_case AS (
            case_key bigint,
            review_id uuid,
            entity_name name,
            issue_key bytea,
            occurrence integer,
            status text,
            severity text,
            reason_code text,
            approved_metadata jsonb,
            permitted_actions text[],
            assigned_queue name,
            due_at timestamptz,
            escalation_level integer,
            manual_assignment_protected boolean,
            opened_at timestamptz,
            opened_at_source text,
            resolved_at timestamptz,
            review_version bigint,
            definition_version bigint,
            publication_revision bigint,
            stewardship_epoch bigint,
            evidence_basis_digest bytea,
            action_revision bigint,
            pending_stewardship boolean,
            last_observed_at timestamptz
        );
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS pgreact_mdm.policy_packages (
    policy_revision text PRIMARY KEY,
    policy_package jsonb NOT NULL,
    policy_digest bytea NOT NULL CHECK (octet_length(policy_digest) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE OR REPLACE FUNCTION pgreact_mdm.reject_package_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $function$
BEGIN
    RAISE EXCEPTION 'MDM_POLICY_IMMUTABLE: published policy packages cannot be changed';
END
$function$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgrelid = 'pgreact_mdm.policy_packages'::regclass
          AND tgname = 'policy_packages_immutable') THEN
        CREATE TRIGGER policy_packages_immutable
        BEFORE UPDATE OR DELETE ON pgreact_mdm.policy_packages
        FOR EACH ROW EXECUTE FUNCTION pgreact_mdm.reject_package_mutation();
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION pgreact_mdm.canonical_json(value jsonb)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
STRICT
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    kind text := jsonb_typeof(value);
    result text;
BEGIN
    IF kind = 'object' THEN
        SELECT '{' || COALESCE(string_agg(
                   to_jsonb(item.key)::text || ':' ||
                   pgreact_mdm.canonical_json(item.value),
                   ',' ORDER BY convert_to(item.key, 'UTF8')), '') || '}'
        INTO result
        FROM jsonb_each(value) AS item;
        RETURN result;
    ELSIF kind = 'array' THEN
        SELECT '[' || COALESCE(string_agg(
                   pgreact_mdm.canonical_json(item.value),
                   ',' ORDER BY item.ordinality), '') || ']'
        INTO result
        FROM jsonb_array_elements(value) WITH ORDINALITY AS item(value, ordinality);
        RETURN result;
    END IF;
    RETURN value::text;
END
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.framed_digest(domain text, body jsonb)
RETURNS bytea
LANGUAGE SQL
IMMUTABLE
STRICT
SET search_path = pg_catalog, pg_temp
AS $function$
    SELECT sha256(
        int4send(octet_length(convert_to($1, 'UTF8'))) ||
        convert_to($1, 'UTF8') ||
        int4send(octet_length(convert_to(pgreact_mdm.canonical_json($2), 'UTF8'))) ||
        convert_to(pgreact_mdm.canonical_json($2), 'UTF8'))
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.normalized_package(package jsonb)
RETURNS jsonb
LANGUAGE SQL
IMMUTABLE
STRICT
SET search_path = pg_catalog, pg_temp
AS $function$
    SELECT jsonb_build_object(
        'applicability', COALESCE(package -> 'applicability', '{}'::jsonb),
        'deadline', package -> 'deadline',
        'routes', COALESCE((
            SELECT jsonb_agg(route ORDER BY
                route ->> 'reason_code',
                COALESCE(route ->> 'entity_name', ''),
                COALESCE(route ->> 'priority', ''),
                route ->> 'queue',
                COALESCE(route ->> 'action', 'ASSIGN_QUEUE'),
                COALESCE(route ->> 'level', ''))
            FROM jsonb_array_elements(package -> 'routes') AS item(route)),
            '[]'::jsonb))
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.validate_package(
    policy_revision text,
    package jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    findings jsonb := '[]'::jsonb;
    route jsonb;
    duration text;
    minimum text;
    maximum text;
    route_count integer;
    duplicate_count integer;
    field_name text;
    normalized jsonb;
BEGIN
    IF policy_revision IS NULL OR octet_length(policy_revision) = 0
       OR octet_length(policy_revision) > 256 THEN
        findings := findings || jsonb_build_array(jsonb_build_object(
            'code', 'POLICY_REVISION_INVALID',
            'message', 'policy revision must be non-empty UTF-8 text of at most 256 bytes'));
    END IF;
    IF jsonb_typeof(package) IS DISTINCT FROM 'object' THEN
        findings := findings || jsonb_build_array(jsonb_build_object(
            'code', 'POLICY_PACKAGE_INVALID',
            'message', 'policy package must be a JSON object'));
        RETURN jsonb_build_object('state', 'invalid', 'findings', findings);
    END IF;
    FOR field_name IN SELECT key FROM jsonb_object_keys(package) AS item(key) LOOP
        IF field_name NOT IN ('routes', 'deadline', 'applicability') THEN
            findings := findings || jsonb_build_array(jsonb_build_object(
                'code', 'POLICY_FIELD_UNKNOWN',
                'field', field_name,
                'message', 'policy package contains an unknown field'));
        END IF;
    END LOOP;
    IF jsonb_typeof(package -> 'routes') IS DISTINCT FROM 'array' THEN
        findings := findings || jsonb_build_array(jsonb_build_object(
            'code', 'POLICY_ROUTES_INVALID',
            'message', 'routes must be a JSON array'));
    ELSE
        SELECT count(*) INTO route_count
        FROM jsonb_array_elements(package -> 'routes') AS item(route);
        IF route_count = 0 THEN
            findings := findings || jsonb_build_array(jsonb_build_object(
                'code', 'POLICY_ROUTES_EMPTY',
                'message', 'at least one route is required'));
        END IF;
        FOR route IN SELECT value FROM jsonb_array_elements(package -> 'routes') LOOP
            IF jsonb_typeof(route) = 'object' THEN
                FOR field_name IN SELECT key FROM jsonb_object_keys(route) AS item(key) LOOP
                    IF field_name NOT IN ('reason_code', 'entity_name', 'queue', 'priority', 'action', 'level') THEN
                        findings := findings || jsonb_build_array(jsonb_build_object(
                            'code', 'POLICY_ROUTE_FIELD_UNKNOWN',
                            'field', field_name,
                            'message', 'route contains an unknown field'));
                    END IF;
                END LOOP;
            END IF;
            IF jsonb_typeof(route) IS DISTINCT FROM 'object'
               OR NULLIF(btrim(route ->> 'reason_code'), '') IS NULL
               OR (COALESCE(route ->> 'action', 'ASSIGN_QUEUE') = 'ASSIGN_QUEUE'
                   AND NULLIF(btrim(route ->> 'queue'), '') IS NULL)
               OR (route ? 'priority' AND COALESCE(route ->> 'priority', '') !~ '^[0-9]+$')
               OR (CASE WHEN COALESCE(route ->> 'priority', '0') ~ '^[0-9]+$'
                        THEN COALESCE(route ->> 'priority', '0')::numeric > 2147483647
                        ELSE false END)
               OR COALESCE(route ->> 'action', 'ASSIGN_QUEUE') NOT IN
                    ('ASSIGN_QUEUE', 'SET_DUE_AT', 'ESCALATE')
               OR (COALESCE(route ->> 'action', 'ASSIGN_QUEUE') = 'ESCALATE'
                   AND (COALESCE(route ->> 'level', '') !~ '^[1-9][0-9]*$'
                        OR (CASE WHEN COALESCE(route ->> 'level', '') ~ '^[1-9][0-9]*$'
                                 THEN (route ->> 'level')::numeric > 2147483647
                                 ELSE false END)))
               OR (COALESCE(route ->> 'action', 'ASSIGN_QUEUE') <> 'ESCALATE'
                   AND route ? 'level') THEN
                findings := findings || jsonb_build_array(jsonb_build_object(
                    'code', 'POLICY_ROUTE_INVALID',
                    'route', route,
                    'message', 'route requires a reason code, supported action, action-specific arguments, and an optional nonnegative priority'));
            END IF;
        END LOOP;
        SELECT count(*) INTO duplicate_count
        FROM (
            SELECT item.route ->> 'reason_code', item.route ->> 'entity_name',
                   item.route ->> 'queue', item.route ->> 'priority', item.route ->> 'level',
                   COALESCE(item.route ->> 'action', 'ASSIGN_QUEUE')
            FROM jsonb_array_elements(package -> 'routes') AS item(route)
            GROUP BY 1, 2, 3, 4, 5, 6
            HAVING count(*) > 1
        ) duplicates;
        IF duplicate_count > 0 THEN
            findings := findings || jsonb_build_array(jsonb_build_object(
                'code', 'POLICY_ROUTE_DUPLICATE',
                'message', 'duplicate route candidates are not accepted'));
        END IF;
    END IF;
    IF jsonb_typeof(package -> 'deadline') IS DISTINCT FROM 'object' THEN
        findings := findings || jsonb_build_array(jsonb_build_object(
            'code', 'POLICY_DEADLINE_INVALID',
            'message', 'deadline must be a JSON object'));
    ELSE
        duration := package -> 'deadline' ->> 'duration_seconds';
        minimum := package -> 'deadline' ->> 'min_seconds';
        maximum := package -> 'deadline' ->> 'max_seconds';
        FOR field_name IN SELECT key FROM jsonb_object_keys(package -> 'deadline') AS item(key) LOOP
            IF field_name NOT IN ('duration_seconds', 'min_seconds', 'max_seconds',
                                  'replace_existing_deadline') THEN
                findings := findings || jsonb_build_array(jsonb_build_object(
                    'code', 'POLICY_DEADLINE_FIELD_UNKNOWN',
                    'field', field_name,
                    'message', 'deadline contains an unknown field'));
            END IF;
        END LOOP;
        IF duration IS NULL OR duration !~ '^[0-9]+$'
           OR minimum IS NULL OR minimum !~ '^[0-9]+$'
           OR maximum IS NULL OR maximum !~ '^[0-9]+$'
           OR duration::numeric > 9223372036854
           OR minimum::numeric > 9223372036854
           OR maximum::numeric > 9223372036854
           OR duration::numeric < minimum::numeric
           OR duration::numeric > maximum::numeric THEN
            findings := findings || jsonb_build_array(jsonb_build_object(
                'code', 'POLICY_DEADLINE_BOUNDS',
                'message', 'duration_seconds must be an integer within min_seconds and max_seconds'));
        END IF;
        IF jsonb_typeof(package -> 'deadline' -> 'replace_existing_deadline') IS DISTINCT FROM 'boolean' THEN
            findings := findings || jsonb_build_array(jsonb_build_object(
                'code', 'POLICY_DEADLINE_REPLACEMENT',
                'message', 'replace_existing_deadline must be an explicit boolean'));
        END IF;
    END IF;
    IF package ? 'applicability' AND jsonb_typeof(package -> 'applicability') IS DISTINCT FROM 'object' THEN
        findings := findings || jsonb_build_array(jsonb_build_object(
            'code', 'POLICY_APPLICABILITY_INVALID',
            'message', 'applicability must be a JSON object'));
    ELSIF package ? 'applicability' THEN
        FOR field_name IN SELECT key FROM jsonb_object_keys(package -> 'applicability') AS item(key) LOOP
            IF field_name NOT IN ('statuses', 'reason_codes', 'entity_names')
               OR jsonb_typeof(package -> 'applicability' -> field_name) IS DISTINCT FROM 'array' THEN
                findings := findings || jsonb_build_array(jsonb_build_object(
                    'code', 'POLICY_APPLICABILITY_INVALID',
                    'field', field_name,
                    'message', 'applicability fields must be supported arrays'));
            END IF;
        END LOOP;
    END IF;
    normalized := pgreact_mdm.normalized_package(package);
    RETURN jsonb_build_object(
        'state', CASE WHEN jsonb_array_length(findings) = 0 THEN 'valid' ELSE 'invalid' END,
        'findings', findings,
        'normalized', normalized,
        'policy_digest', encode(pgreact_mdm.framed_digest(
            'pg_react/mdm-stewardship-policy/v1',
            jsonb_build_object(
                'canonical_encoding_version', 1,
                'policy_package', normalized,
                'policy_revision', policy_revision)), 'hex'));
END
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.publish_package(
    policy_revision text,
    package jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    validation jsonb := pgreact_mdm.validate_package($1, package);
    digest bytea := decode(validation ->> 'policy_digest', 'hex');
    stored pgreact_mdm.policy_packages%ROWTYPE;
BEGIN
    IF validation ->> 'state' <> 'valid' THEN
        RAISE EXCEPTION 'MDM_POLICY_INVALID: %', validation -> 'findings';
    END IF;
    INSERT INTO pgreact_mdm.policy_packages(policy_revision, policy_package, policy_digest)
    VALUES ($1, validation -> 'normalized', digest)
    ON CONFLICT ON CONSTRAINT policy_packages_pkey DO NOTHING;
    SELECT * INTO STRICT stored
    FROM pgreact_mdm.policy_packages
    WHERE policy_packages.policy_revision = $1;
    IF stored.policy_digest <> digest THEN
        RAISE EXCEPTION 'MDM_POLICY_IMMUTABLE: policy revision % already has a different digest',
            $1;
    END IF;
    RETURN jsonb_build_object(
        'state', 'published',
        'policy_revision', stored.policy_revision,
        'policy_digest', encode(stored.policy_digest, 'hex'),
        'policy_package', stored.policy_package);
END
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.policy_document(policy_revision text)
RETURNS jsonb
LANGUAGE SQL
STABLE
SET search_path = pg_catalog, pg_temp
AS $function$
    SELECT jsonb_build_object(
        'canonical_encoding_version', 1,
        'policy_package', policy_package,
        'policy_revision', policy_revision,
        'policy_digest', encode(policy_digest, 'hex'))
    FROM pgreact_mdm.policy_packages
    WHERE pgreact_mdm.policy_packages.policy_revision = $1
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.assert_source(source_relation regclass)
RETURNS void
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    relation_kind char;
    has_rls boolean;
    relation_name text := source_relation::text;
    column_spec text[];
    actual_type text;
    required_columns constant text[][] := ARRAY[
        ['case_key', 'bigint'], ['review_id', 'uuid'], ['entity_name', 'name'],
        ['issue_key', 'bytea'], ['occurrence', 'integer'], ['status', 'text'],
        ['severity', 'text'], ['reason_code', 'text'], ['approved_metadata', 'jsonb'],
        ['permitted_actions', 'text[]'], ['assigned_queue', 'name'], ['due_at', 'timestamp with time zone'],
        ['escalation_level', 'integer'], ['manual_assignment_protected', 'boolean'],
        ['opened_at', 'timestamp with time zone'], ['opened_at_source', 'text'],
        ['resolved_at', 'timestamp with time zone'], ['review_version', 'bigint'],
        ['definition_version', 'bigint'], ['publication_revision', 'bigint'],
        ['stewardship_epoch', 'bigint'], ['evidence_basis_digest', 'bytea'],
        ['action_revision', 'bigint'], ['pending_stewardship', 'boolean'],
        ['last_observed_at', 'timestamp with time zone']
    ];
BEGIN
    IF source_relation IS NULL THEN
        RAISE EXCEPTION 'MDM_INPUT_SOURCE: source relation is required';
    END IF;
    SELECT c.relkind
    INTO relation_kind
    FROM pg_class AS c
    WHERE c.oid = source_relation;
    IF relation_kind IS NULL OR relation_kind NOT IN ('r', 'v', 'm') THEN
        RAISE EXCEPTION 'MDM_INPUT_SOURCE: % is not a table, view, or materialized view', relation_name;
    END IF;
    WITH RECURSIVE relation_tree(oid) AS (
        SELECT source_relation
        UNION
        SELECT dependency.refobjid
        FROM relation_tree AS parent
        JOIN pg_rewrite AS rewrite ON rewrite.ev_class = parent.oid
        JOIN pg_depend AS dependency
          ON dependency.classid = 'pg_rewrite'::regclass
         AND dependency.objid = rewrite.oid
         AND dependency.refclassid = 'pg_class'::regclass
    )
    SELECT EXISTS (
        SELECT 1
        FROM relation_tree
        JOIN pg_class AS c ON c.oid = relation_tree.oid
        WHERE c.relrowsecurity)
    INTO has_rls;
    IF has_rls THEN
        RAISE EXCEPTION 'MDM_INPUT_RLS_UNSUPPORTED: % uses row-level security', relation_name;
    END IF;
    IF relation_name NOT IN ('mdm_steward.policy_cases_v1', 'mdm_fixture.policy_cases_v1',
                             'pgreact_mdm.authorized_policy_cases_v1',
                             'pgreact_mdm.intent_deployer_policy_cases_v1') THEN
        RAISE EXCEPTION 'MDM_INPUT_SOURCE: % is not the approved policy-case projection', relation_name;
    END IF;
    IF NOT has_table_privilege(current_user, source_relation, 'SELECT') THEN
        RAISE EXCEPTION 'MDM_INPUT_UNAUTHORIZED: % is not readable by %', relation_name, current_user;
    END IF;
    FOREACH column_spec SLICE 1 IN ARRAY required_columns LOOP
        SELECT a.atttypid::regtype::text
        INTO actual_type
        FROM pg_attribute AS a
        WHERE a.attrelid = source_relation
          AND a.attname = column_spec[1]
          AND a.attnum > 0
          AND NOT a.attisdropped;
        IF actual_type IS NULL OR actual_type <> column_spec[2] THEN
            RAISE EXCEPTION 'MDM_INPUT_CONTRACT: %.% must have type %, found %',
                relation_name, column_spec[1], column_spec[2], COALESCE(actual_type, '<missing>');
        END IF;
    END LOOP;
END
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.validate_inputs(source_relation regclass)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    findings jsonb := '[]'::jsonb;
    null_keys bigint;
    duplicate_keys bigint;
    duplicate_reviews bigint;
    invalid_rows bigint;
    invalid_actions bigint;
    unsorted_actions bigint;
    unknown_opening bigint;
BEGIN
    PERFORM pgreact_mdm.assert_source(source_relation);
    EXECUTE format($query$
        SELECT count(*) FILTER (WHERE case_key IS NULL OR case_key <= 0),
               count(*) - count(DISTINCT case_key),
               count(*) - count(DISTINCT review_id),
               count(*) FILTER (WHERE review_id IS NULL
                                  OR entity_name IS NULL
                                  OR issue_key IS NULL
                                  OR status NOT IN ('open', 'resolved')
                                  OR occurrence <= 0
                                  OR octet_length(issue_key) IS DISTINCT FROM 32
                                  OR octet_length(evidence_basis_digest) IS DISTINCT FROM 32
                                  OR approved_metadata IS NULL
                                  OR approved_metadata <> '{}'::jsonb
                                  OR opened_at_source IS NULL
                                  OR opened_at_source NOT IN ('publication', 'administrator', 'unknown')
                                  OR review_version <= 0
                                  OR definition_version <= 0
                                  OR publication_revision < 0
                                  OR stewardship_epoch < 0
                                  OR action_revision <= 0
                                  OR pending_stewardship IS NULL
                                  OR manual_assignment_protected IS NULL
                                  OR last_observed_at IS NULL),
                count(*) FILTER (WHERE permitted_actions IS NULL
                                  OR array_position(permitted_actions, NULL) IS NOT NULL
                                  OR NOT permitted_actions <@ ARRAY['ASSIGN_QUEUE', 'SET_DUE_AT', 'ESCALATE']::text[]),
               count(*) FILTER (WHERE permitted_actions IS NOT NULL
                                  AND permitted_actions <> ARRAY(
                                      SELECT DISTINCT unnest(permitted_actions) ORDER BY 1)),
               count(*) FILTER (WHERE status = 'open' AND opened_at IS NULL
                                  AND opened_at_source = 'unknown')
        FROM %s
    $query$, source_relation)
    INTO null_keys, duplicate_keys, duplicate_reviews, invalid_rows, invalid_actions,
         unsorted_actions, unknown_opening;
    IF null_keys > 0 THEN
        findings := findings || jsonb_build_array(jsonb_build_object(
            'code', 'MDM_INPUT_KEY_INVALID', 'rows', null_keys,
            'message', 'case_key must be positive and non-null'));
    END IF;
    IF duplicate_keys > 0 THEN
        findings := findings || jsonb_build_array(jsonb_build_object(
            'code', 'MDM_INPUT_KEY_DUPLICATE', 'rows', duplicate_keys,
            'message', 'case_key must be unique'));
    END IF;
    IF duplicate_reviews > 0 THEN
        findings := findings || jsonb_build_array(jsonb_build_object(
            'code', 'MDM_INPUT_REVIEW_DUPLICATE', 'rows', duplicate_reviews,
            'message', 'review_id must be unique'));
    END IF;
    IF invalid_rows > 0 THEN
        findings := findings || jsonb_build_array(jsonb_build_object(
            'code', 'MDM_INPUT_VALUE_INVALID', 'rows', invalid_rows,
            'message', 'mandatory case values violate the signed contract'));
    END IF;
    IF invalid_actions > 0 THEN
        findings := findings || jsonb_build_array(jsonb_build_object(
            'code', 'MDM_INPUT_ACTIONS_INVALID', 'rows', invalid_actions,
            'message', 'permitted_actions must be a supported subset'));
    END IF;
    IF unsorted_actions > 0 THEN
        findings := findings || jsonb_build_array(jsonb_build_object(
            'code', 'MDM_INPUT_ACTIONS_UNSORTED', 'rows', unsorted_actions,
            'message', 'permitted_actions must be sorted and distinct'));
    END IF;
    IF unknown_opening > 0 THEN
        findings := findings || jsonb_build_array(jsonb_build_object(
            'code', 'MDM_INPUT_OPENING_UNKNOWN', 'rows', unknown_opening,
            'message', 'open cases without an authorized opening time cannot receive a due-date proposal'));
    END IF;
    RETURN jsonb_build_object(
        'state', CASE WHEN NOT EXISTS (
                      SELECT 1
                      FROM jsonb_array_elements(findings) AS item
                      WHERE item ->> 'code' <> 'MDM_INPUT_OPENING_UNKNOWN')
                     THEN 'valid' ELSE 'invalid' END,
        'findings', findings,
        'source_relation', source_relation::text);
END
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.policy_inputs(source_relation regclass)
RETURNS SETOF pgreact_mdm.policy_case
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    validation jsonb;
BEGIN
    validation := pgreact_mdm.validate_inputs(source_relation);
    IF validation ->> 'state' <> 'valid' THEN
        RAISE EXCEPTION 'MDM_INPUT_INVALID: %',
            validation -> 'findings';
    END IF;
    RETURN QUERY EXECUTE format(
        'SELECT case_key, review_id, entity_name, issue_key, occurrence, status,
                severity, reason_code, approved_metadata, permitted_actions,
                assigned_queue, due_at, escalation_level,
                manual_assignment_protected, opened_at, opened_at_source,
                resolved_at, review_version, definition_version,
                publication_revision, stewardship_epoch, evidence_basis_digest,
                action_revision, pending_stewardship, last_observed_at
           FROM %s
          ORDER BY case_key', source_relation);
END
$function$;

REVOKE ALL ON TABLE pgreact_mdm.policy_packages FROM PUBLIC;
GRANT SELECT ON TABLE pgreact_mdm.policy_packages TO PUBLIC;

COMMENT ON SCHEMA pgreact_mdm IS
    'v0.47 read-only MDM policy package; it never submits MDM intents';
COMMENT ON TABLE pgreact_mdm.policy_packages IS
    'Immutable React-owned policy revisions and their MDM-STEWARDSHIP/1 digests';
