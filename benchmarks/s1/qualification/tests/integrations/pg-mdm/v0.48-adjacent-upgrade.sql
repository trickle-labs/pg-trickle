\set ON_ERROR_STOP on

CREATE SCHEMA v048_adjacent_upgrade;
CREATE SCHEMA IF NOT EXISTS pgreact_mdm;

CREATE TABLE pgreact_mdm.intent_holds (
    binding_id uuid NOT NULL,
    case_key bigint NOT NULL,
    policy_revision text NOT NULL,
    action text NOT NULL,
    reason_code text NOT NULL,
    held_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (binding_id, case_key, policy_revision, action)
);
CREATE TEMP TABLE v048_legacy_hold_before AS
SELECT
    '30000000-0000-4000-8000-000000000001'::uuid AS binding_id,
    case_row.case_key,
    'legacy-policy'::text AS policy_revision,
    'ASSIGN_QUEUE'::text AS action,
    'LEGACY_HOLD'::text AS reason_code,
    statement_timestamp() AS held_at,
    case_row.action_revision AS expected_action_revision
FROM mdm_steward.policy_cases_v1 AS case_row
WHERE case_row.status = 'open'
ORDER BY case_row.case_key
LIMIT 1;
INSERT INTO pgreact_mdm.intent_holds(
    binding_id, case_key, policy_revision, action, reason_code, held_at)
SELECT binding_id, case_key, policy_revision, action, reason_code, held_at
FROM v048_legacy_hold_before;

CREATE TABLE v048_adjacent_upgrade.routes (
    subject_id bigint NOT NULL,
    candidate_id bigint PRIMARY KEY,
    priority bigint NOT NULL,
    result text NOT NULL
);
INSERT INTO v048_adjacent_upgrade.routes VALUES
    (1, 1001, 1, 'ready'),
    (1, 1002, 2, 'fallback');

CREATE TEMP TABLE v048_upgrade_packages_before AS
SELECT policy_revision, policy_digest
FROM pgreact_mdm.policy_packages;

DO $upgrade_fixture$
DECLARE
    declaration pgreact_api.declaration;
    preview jsonb;
    deployed jsonb;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM v048_upgrade_packages_before) THEN
        RAISE EXCEPTION 'adjacent-upgrade fixture requires a v0.47 policy package';
    END IF;
    declaration := pgreact.decision(
        'v0.48-adjacent-routing', 'v048_adjacent_upgrade.routes'::regclass,
        'subject_id'::name, 'candidate_id'::name, 'priority'::name,
        ARRAY['result']::name[], statement_timestamp());
    preview := pgreact.preview(declaration);
    deployed := pgreact.deploy(declaration, jsonb_build_object(
        'preview_digest', preview -> 'summary' ->> 'preview_digest'));
    IF deployed ->> 'state' IS DISTINCT FROM 'deployed' THEN
        RAISE EXCEPTION 'ordinary decision setup failed: %', deployed;
    END IF;
END
$upgrade_fixture$;

SELECT pgreact.run(statement_timestamp() + interval '1 minute');
CREATE TEMP TABLE v048_upgrade_decision_before AS
SELECT jsonb_agg(jsonb_build_object(
    'subject_key', subject_key,
    'state', state,
    'winner_candidate', winner_candidate,
    'winner_priority', winner_priority,
    'winner_result', winner_result,
    'claimable', claimable,
    'generation', generation,
    'revision', revision,
    'competitors', competitors)
    ORDER BY subject_key) AS rows
FROM pgreact.decision_winners
WHERE program_name = 'v0.48-adjacent-routing';

DO $upgrade_before$
BEGIN
    IF (SELECT rows FROM v048_upgrade_decision_before) IS DISTINCT FROM
       '[{"subject_key":1,"state":"WINNER","winner_candidate":1001,"winner_priority":1,"winner_result":{"result":"ready"},"claimable":true,"generation":1,"revision":0,"competitors":[{"candidate":1001,"priority":1,"result":{"result":"ready"}},{"candidate":1002,"priority":2,"result":{"result":"fallback"}}]}]'::jsonb THEN
        RAISE EXCEPTION 'ordinary decision did not produce its exact pre-upgrade result: %',
            (SELECT rows FROM v048_upgrade_decision_before);
    END IF;
END
$upgrade_before$;

\ir ../../../integrations/pg-mdm/sql/request-store.sql
\ir ../../../integrations/pg-mdm/sql/intent-worker.sql

DO $upgrade_after$
DECLARE actual_decision jsonb;
BEGIN
    SELECT jsonb_agg(jsonb_build_object(
        'subject_key', subject_key,
        'state', state,
        'winner_candidate', winner_candidate,
        'winner_priority', winner_priority,
        'winner_result', winner_result,
        'claimable', claimable,
        'generation', generation,
        'revision', revision,
        'competitors', competitors)
        ORDER BY subject_key)
    INTO actual_decision
    FROM pgreact.decision_winners
    WHERE program_name = 'v0.48-adjacent-routing';
    IF actual_decision IS DISTINCT FROM
       (SELECT rows FROM v048_upgrade_decision_before) THEN
        RAISE EXCEPTION 'ordinary decision changed across the adapter upgrade: %',
            actual_decision;
    END IF;
    IF EXISTS (
        (SELECT policy_revision, policy_digest
         FROM pgreact_mdm.policy_packages
         EXCEPT
         SELECT policy_revision, policy_digest
         FROM v048_upgrade_packages_before)
        UNION ALL
        (SELECT policy_revision, policy_digest
         FROM v048_upgrade_packages_before
         EXCEPT
         SELECT policy_revision, policy_digest
         FROM pgreact_mdm.policy_packages)) THEN
        RAISE EXCEPTION 'v0.48 adapter upgrade changed existing v0.47 policy packages';
    END IF;
    IF EXISTS (SELECT 1 FROM pgreact_mdm.policy_intent_bindings WHERE enabled)
       OR EXISTS (SELECT 1 FROM pgreact_mdm.intent_requests)
       OR EXISTS (SELECT 1 FROM pgreact_mdm.intent_attempts) THEN
        RAISE EXCEPTION 'v0.48 adapter upgrade admitted MDM work before explicit binding enablement';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM v048_legacy_hold_before AS before_hold
        JOIN pgreact_mdm.intent_holds AS after_hold
          ON after_hold.binding_id = before_hold.binding_id
         AND after_hold.case_key = before_hold.case_key
         AND after_hold.policy_revision = before_hold.policy_revision
         AND after_hold.action = before_hold.action
         AND after_hold.reason_code = before_hold.reason_code
         AND after_hold.held_at = before_hold.held_at
         AND after_hold.expected_action_revision = before_hold.expected_action_revision) THEN
        RAISE EXCEPTION 'v0.48 adapter upgrade did not preserve the populated legacy hold';
    END IF;
    PERFORM pgreact_api.pause_decision_program('v0.48-adjacent-routing');
END
$upgrade_after$;

DROP TABLE v048_adjacent_upgrade.routes;
DROP SCHEMA v048_adjacent_upgrade;
DROP TABLE v048_upgrade_packages_before;
DROP TABLE v048_upgrade_decision_before;
DROP TABLE v048_legacy_hold_before;
SELECT 'v0.48 adjacent upgrade preserved admission-off and ordinary decision behavior: PASS' AS result;
