\set ON_ERROR_STOP on
SET TIME ZONE 'UTC';
\ir ../../../../integrations/pg-mdm/sql/request-store.sql
CREATE SCHEMA s1_boundary;
CREATE TABLE s1_boundary.source (case_key bigint PRIMARY KEY, deadline timestamptz NOT NULL);
INSERT INTO s1_boundary.source VALUES (7, '2026-10-02 00:00:00+00');
CREATE VIEW s1_boundary.candidate AS SELECT case_key, deadline FROM s1_boundary.source;
CREATE TABLE s1_boundary.work (
    context pgreact.activation_context NOT NULL,
    due_at timestamptz NOT NULL,
    request_key bytea NOT NULL,
    request_body jsonb NOT NULL,
    request_digest bytea NOT NULL
);
CREATE FUNCTION s1_boundary.activate(
    context pgreact.activation_context, candidate s1_boundary.candidate
) RETURNS void LANGUAGE plpgsql AS $callback$
DECLARE request_body jsonb;
BEGIN
    request_body := pgreact_mdm.intent_request_body(
        '30000000-0000-4000-8000-000000000001'::uuid,
        ($2).case_key,
        'ASSIGN_QUEUE',
        '{"queue":"priority"}'::jsonb,
        4, 3, 5, 8,
        decode(repeat('ab', 32), 'hex'),
        2,
        decode('741ea9560a69ba3185eaa34760ba38d473aa43daa8e8c530c6c6c2ce867c2614', 'hex'),
        'policy-1', 'eval-1', 'work-1');
    INSERT INTO s1_boundary.work
    VALUES ($1, ($2).deadline,
        pgreact_mdm.intent_request_key(
            '30000000-0000-4000-8000-000000000001'::uuid,
            'policy-1', ($2).case_key, 1, 2, 'assign_priority', 0),
        request_body, pgreact_mdm.intent_request_digest(request_body));
END
$callback$;
SELECT pgreact_api.author_deadline_rule(
    's1-boundary', 's1_boundary.candidate'::regclass, 'case_key', 'deadline', 'COMMAND',
    's1_boundary.activate(pgreact.activation_context,s1_boundary.candidate)');
SELECT pgreact_api.run('2026-10-01 23:59:59.999999+00'::timestamptz);
DO $before$
BEGIN
    IF pgreact_api.deadline_history('s1-boundary') IS DISTINCT FROM
        jsonb_build_object('contract_version', 2, 'rule_name', 's1-boundary', 'events', '[]'::jsonb)
       OR EXISTS (SELECT 1 FROM s1_boundary.work)
       OR (SELECT jsonb_agg(to_jsonb(c) ORDER BY case_key)
           FROM s1_boundary.candidate c) IS DISTINCT FROM jsonb_build_array(
            jsonb_build_object('case_key', 7, 'deadline', '2026-10-02 00:00:00+00'::timestamptz)) THEN
        RAISE EXCEPTION 'condition, work identity or history changed one microsecond before equality';
    END IF;
END
$before$;
SELECT pgreact_api.run('2026-10-02 00:00:00+00'::timestamptz);
DO $execute$
DECLARE claimed record; result text;
BEGIN
    SELECT * INTO STRICT claimed
    FROM pgreact.claim_episode(
        (SELECT rule_version_id FROM pgreact.rules WHERE rule_name = 's1-boundary'),
        's1-boundary-test', 60);
    result := pgreact.execute_claimed_episode(
        claimed.episode_id, 's1-boundary-test', claimed.lease_token);
    IF result <> 'COMPLETED' THEN
        RAISE EXCEPTION 'equality work did not complete: %', result;
    END IF;
END
$execute$;
DO $work$
DECLARE work_row s1_boundary.work%ROWTYPE;
BEGIN
    SELECT * INTO STRICT work_row FROM s1_boundary.work;
    IF work_row.due_at IS DISTINCT FROM '2026-10-02 00:00:00+00'::timestamptz
       OR encode(work_row.request_key, 'hex') <> '3ebd5539467befabbc0492e51e49fe2dc866a225cc73a36256d0a95a06b06d4e'
       OR pgreact_mdm.canonical_json(work_row.request_body) <> '{"action":"ASSIGN_QUEUE","arguments":{"queue":"priority"},"binding_id":"30000000-0000-4000-8000-000000000001","case_key":7,"evaluation_ref":"eval-1","expected_action_revision":2,"expected_definition_version":3,"expected_evidence_basis_digest":"abababababababababababababababababababababababababababababababab","expected_policy_digest":"741ea9560a69ba3185eaa34760ba38d473aa43daa8e8c530c6c6c2ce867c2614","expected_publication_revision":5,"expected_review_version":4,"expected_stewardship_epoch":8,"policy_revision":"policy-1","work_ref":"work-1"}'
       OR encode(work_row.request_digest, 'hex') <> '5d1c1af60b706e1e2e6b45b784836793f51fe38144caac815830d02fefa82f99' THEN
        RAISE EXCEPTION 'equality produced a different complete request identity or due instant: %',
            to_jsonb(work_row);
    END IF;
END
$work$;
CREATE TABLE s1_boundary.expected_work AS
SELECT jsonb_agg(jsonb_build_object(
    'context', to_jsonb(w.context), 'due_at', w.due_at,
    'request_key', encode(w.request_key, 'hex'),
    'request_bytes', pgreact_mdm.canonical_json(w.request_body),
    'request_digest', encode(w.request_digest, 'hex')) ORDER BY (w.context).episode_id) AS vector
FROM s1_boundary.work AS w;
CREATE TABLE s1_boundary.expected AS
SELECT jsonb_build_object('contract_version', 2, 'rule_name', 's1-boundary',
    'events', jsonb_build_array(jsonb_build_object(
        'rule_name', 's1-boundary', 'semantic_key', 7,
        'generation', 1, 'revision', 0, 'event_kind', 'ACTIVATE',
        'declared_deadline', '2026-10-02 00:00:00+00'::timestamptz,
        'clock_frontier', '2026-10-02 00:00:00+00'::timestamptz))) AS history;
DO $equal$
BEGIN
    IF pgreact_api.deadline_history('s1-boundary') IS DISTINCT FROM
        (SELECT history FROM s1_boundary.expected) THEN
        RAISE EXCEPTION 'equality did not produce the exact expected deadline event: %',
            pgreact_api.deadline_history('s1-boundary');
    END IF;
END
$equal$;
SELECT pgreact_api.run('2026-10-02 00:00:00.000001+00'::timestamptz);
SELECT pgreact_api.run('2026-10-01 17:00:00.000001-07'::timestamptz);
SELECT pgreact_api.run('2026-10-01 23:59:59.999999+00'::timestamptz);
DO $stable$
DECLARE actual_work jsonb;
BEGIN
    SELECT jsonb_agg(jsonb_build_object(
        'context', to_jsonb(w.context), 'due_at', w.due_at,
        'request_key', encode(w.request_key, 'hex'),
        'request_bytes', pgreact_mdm.canonical_json(w.request_body),
        'request_digest', encode(w.request_digest, 'hex')) ORDER BY (w.context).episode_id)
    INTO actual_work FROM s1_boundary.work AS w;
    IF pgreact_api.deadline_history('s1-boundary') IS DISTINCT FROM
        (SELECT history FROM s1_boundary.expected)
       OR (SELECT jsonb_agg(to_jsonb(s) ORDER BY case_key) FROM s1_boundary.source s)
          IS DISTINCT FROM jsonb_build_array(jsonb_build_object(
            'case_key', 7, 'deadline', '2026-10-02 00:00:00+00'::timestamptz))
       OR (SELECT jsonb_agg(to_jsonb(c) ORDER BY case_key)
           FROM s1_boundary.candidate c) IS DISTINCT FROM jsonb_build_array(
            jsonb_build_object('case_key', 7, 'deadline', '2026-10-02 00:00:00+00'::timestamptz))
       OR actual_work IS DISTINCT FROM (SELECT vector FROM s1_boundary.expected_work) THEN
        RAISE EXCEPTION 'after/equivalent/backward samples changed the complete condition, work identity, due instant or history: %',
            jsonb_build_object('work', actual_work,
                'history', pgreact_api.deadline_history('s1-boundary'));
    END IF;
END
$stable$;
SELECT jsonb_build_object(
    'condition', (SELECT jsonb_agg(to_jsonb(c) ORDER BY case_key) FROM s1_boundary.candidate c),
    'work', (SELECT vector FROM s1_boundary.expected_work),
    'history', pgreact_api.deadline_history('s1-boundary'),
    'frontier', (SELECT frontier FROM pgreact_internal.clock_frontier)) AS s1_boundary_evidence;
SELECT 'S1 before/equal/after, timezone equivalence, canonical request identity and monotone frontier: PASS' AS result;
