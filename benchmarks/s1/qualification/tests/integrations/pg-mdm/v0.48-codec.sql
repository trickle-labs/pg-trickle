\set ON_ERROR_STOP on
SET TIME ZONE 'UTC';
DO $fixture$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'mdm_helper_owner'
    ) THEN
        CREATE ROLE mdm_helper_owner NOLOGIN NOSUPERUSER NOBYPASSRLS NOINHERIT;
    END IF;
END
$fixture$;
\ir ../../../integrations/pg-mdm/sql/request-store.sql

BEGIN;

DO $test$
DECLARE
    package jsonb := '{"routes":[{"queue":"priority","reason_code":"POSSIBLE_DUPLICATE"}]}'::jsonb;
    published jsonb;
    request_body jsonb;
    key_bytes bytea;
    audit_key bytea;
    stored pgreact_mdm.intent_requests%ROWTYPE;
    changed_body jsonb;
    audit_body jsonb;
BEGIN
    published := pgreact_mdm.publish_intent_package('v0.48-codec', package);
    IF published IS DISTINCT FROM '{
        "state":"published",
        "policy_revision":"v0.48-codec",
        "canonical_encoding_version":1,
        "policy_package":{"routes":[{"queue":"priority","reason_code":"POSSIBLE_DUPLICATE"}]},
        "policy_digest":"f54e66a4789ff2208db5457778d1ae2fde5a1dc09569c4158551058d33bd0a50"
    }'::jsonb THEN
        RAISE EXCEPTION 'policy vector mismatch: %', published;
    END IF;

    key_bytes := pgreact_mdm.intent_request_key(
        '30000000-0000-4000-8000-000000000001'::uuid,
        'policy-1', 7, 1, 2, 'assign_priority', 0);
    IF encode(key_bytes, 'hex') <> '3ebd5539467befabbc0492e51e49fe2dc866a225cc73a36256d0a95a06b06d4e' THEN
        RAISE EXCEPTION 'request-key vector mismatch: %', encode(key_bytes, 'hex');
    END IF;
    IF key_bytes IS NOT DISTINCT FROM pgreact_mdm.intent_request_key(
           '30000000-0000-4000-8000-000000000002'::uuid,
           'policy-1', 7, 1, 2, 'assign_priority', 0)
       OR key_bytes IS NOT DISTINCT FROM pgreact_mdm.intent_request_key(
           '30000000-0000-4000-8000-000000000001'::uuid,
           'policy-2', 7, 1, 2, 'assign_priority', 0)
       OR key_bytes IS NOT DISTINCT FROM pgreact_mdm.intent_request_key(
           '30000000-0000-4000-8000-000000000001'::uuid,
           'policy-1', 8, 1, 2, 'assign_priority', 0)
       OR key_bytes IS NOT DISTINCT FROM pgreact_mdm.intent_request_key(
           '30000000-0000-4000-8000-000000000001'::uuid,
           'policy-1', 7, 1, 3, 'assign_priority', 0)
       OR key_bytes IS NOT DISTINCT FROM pgreact_mdm.intent_request_key(
           '30000000-0000-4000-8000-000000000001'::uuid,
           'policy-1', 7, 2, 2, 'assign_priority', 0)
       OR key_bytes IS NOT DISTINCT FROM pgreact_mdm.intent_request_key(
           '30000000-0000-4000-8000-000000000001'::uuid,
           'policy-1', 7, 1, 2, 'assign_urgent', 0)
       OR key_bytes IS NOT DISTINCT FROM pgreact_mdm.intent_request_key(
           '30000000-0000-4000-8000-000000000001'::uuid,
           'policy-1', 7, 1, 2, 'assign_priority', 1) THEN
        RAISE EXCEPTION 'action-driving identity change reused the old request key';
    END IF;
    request_body := pgreact_mdm.intent_request_body(
        '30000000-0000-4000-8000-000000000001'::uuid,
        7,
        'ASSIGN_QUEUE',
        '{"queue":"priority"}'::jsonb,
        4, 3, 5, 8,
        decode(repeat('ab', 32), 'hex'),
        2,
        decode('741ea9560a69ba3185eaa34760ba38d473aa43daa8e8c530c6c6c2ce867c2614', 'hex'),
        'policy-1', 'eval-1', 'work-1');
    -- Evaluation and work references are audit correlation, not action identity.
    audit_key := pgreact_mdm.intent_request_key(
        '30000000-0000-4000-8000-000000000001'::uuid,
        'policy-1', 7, 1, 2, 'assign_priority', 0);
    audit_body := jsonb_set(
        jsonb_set(request_body, '{evaluation_ref}', '"eval-2"'::jsonb),
        '{work_ref}', '"work-2"'::jsonb);
    IF audit_body IS NOT DISTINCT FROM request_body
       OR audit_key IS DISTINCT FROM key_bytes THEN
        RAISE EXCEPTION 'audit-only correlation changed request identity';
    END IF;
    IF pgreact_mdm.canonical_json(request_body) <> '{"action":"ASSIGN_QUEUE","arguments":{"queue":"priority"},"binding_id":"30000000-0000-4000-8000-000000000001","case_key":7,"evaluation_ref":"eval-1","expected_action_revision":2,"expected_definition_version":3,"expected_evidence_basis_digest":"abababababababababababababababababababababababababababababababab","expected_policy_digest":"741ea9560a69ba3185eaa34760ba38d473aa43daa8e8c530c6c6c2ce867c2614","expected_publication_revision":5,"expected_review_version":4,"expected_stewardship_epoch":8,"policy_revision":"policy-1","work_ref":"work-1"}' THEN
        RAISE EXCEPTION 'intent body vector mismatch: %', pgreact_mdm.canonical_json(request_body);
    END IF;
    IF encode(pgreact_mdm.intent_request_digest(request_body), 'hex') <> '5d1c1af60b706e1e2e6b45b784836793f51fe38144caac815830d02fefa82f99' THEN
        RAISE EXCEPTION 'intent body digest vector mismatch: %',
            encode(pgreact_mdm.intent_request_digest(request_body), 'hex');
    END IF;

    INSERT INTO pgreact_mdm.intent_requests(
        binding_id, request_key, request_digest, request_body, work_ref,
        policy_revision, case_key, lifecycle_generation, action_revision,
        consequence_identity, escalation_level, first_episode_id)
    VALUES (
        '30000000-0000-4000-8000-000000000001', key_bytes,
        pgreact_mdm.intent_request_digest(request_body), request_body,
        'retry-vector-work', 'policy-1', 7, 1, 2, 'assign_priority', 0, 999);
    SELECT * INTO STRICT stored
    FROM pgreact_mdm.intent_requests
    WHERE work_ref = 'retry-vector-work';
    IF stored.request_body IS DISTINCT FROM request_body
       OR stored.request_key IS DISTINCT FROM key_bytes
       OR stored.policy_revision <> 'policy-1'
       OR stored.case_key <> 7
       OR stored.lifecycle_generation <> 1
       OR stored.action_revision <> 2
       OR stored.consequence_identity <> 'assign_priority'
       OR stored.escalation_level <> 0 THEN
        RAISE EXCEPTION 'persisted work vector mismatch: %', row_to_json(stored);
    END IF;

    changed_body := jsonb_set(request_body, '{arguments,queue}', '"urgent"'::jsonb);
    INSERT INTO pgreact_mdm.intent_requests(
        binding_id, request_key, request_digest, request_body, work_ref,
        policy_revision, case_key, lifecycle_generation, action_revision,
        consequence_identity, escalation_level, first_episode_id)
    VALUES (
        '30000000-0000-4000-8000-000000000001', key_bytes,
        pgreact_mdm.intent_request_digest(changed_body), changed_body,
        'changed-body-work', 'policy-1', 7, 1, 2, 'assign_priority', 0, 1000)
    ON CONFLICT (binding_id, request_key) DO NOTHING;
    SELECT * INTO STRICT stored
    FROM pgreact_mdm.intent_requests
    WHERE binding_id = '30000000-0000-4000-8000-000000000001'
      AND pgreact_mdm.intent_requests.request_key = key_bytes;
    IF stored.request_body IS DISTINCT FROM request_body
       OR stored.work_ref <> 'retry-vector-work' THEN
        RAISE EXCEPTION 'changed body replaced persisted request under the same key: %', row_to_json(stored);
    END IF;
END
$test$;

SELECT 'v0.48 canonical digest, request-key, body, and retry checks: PASS' AS result;
ROLLBACK;
