\set ON_ERROR_STOP on
SET TIME ZONE 'UTC';

\ir ../../../showcase/mdm-stewardship/01-fixture.sql
\ir ../../../integrations/pg-mdm/sql/policy-inputs.sql
\ir ../../../integrations/pg-mdm/sql/routing.sql
\ir ../../../integrations/pg-mdm/sql/deadline-preview.sql
\ir ../../../integrations/pg-mdm/sql/comparison.sql

SELECT pgreact_mdm.publish_package(
    'policy-1',
    '{
       "applicability": {},
       "deadline": {"duration_seconds":3600,"min_seconds":60,"max_seconds":86400,"replace_existing_deadline":false},
       "routes": [
         {"reason_code":"POSSIBLE_DUPLICATE","queue":"priority","priority":1,"action":"ASSIGN_QUEUE"},
         {"reason_code":"TIED_ROUTE","queue":"alpha","priority":1,"action":"ASSIGN_QUEUE"},
         {"reason_code":"TIED_ROUTE","queue":"beta","priority":1,"action":"ASSIGN_QUEUE"},
         {"reason_code":"MANUAL_ROUTE","queue":"protected","priority":1,"action":"ASSIGN_QUEUE"},
         {"reason_code":"DEADLINE_ONLY","queue":"deadline","priority":1,"action":"ASSIGN_QUEUE"},
         {"reason_code":"EXISTING_DEADLINE","queue":"deadline","priority":1,"action":"ASSIGN_QUEUE"}
       ]
     }'::jsonb) AS published
\gset

SELECT pgreact_mdm.publish_package(
    'policy-2',
    '{
       "applicability": {},
       "deadline": {"duration_seconds":7200,"min_seconds":60,"max_seconds":86400,"replace_existing_deadline":true},
       "routes": [
         {"reason_code":"POSSIBLE_DUPLICATE","queue":"urgent","priority":1,"action":"ASSIGN_QUEUE"},
         {"reason_code":"TIED_ROUTE","queue":"alpha","priority":1,"action":"ASSIGN_QUEUE"},
         {"reason_code":"TIED_ROUTE","queue":"beta","priority":1,"action":"ASSIGN_QUEUE"},
         {"reason_code":"MANUAL_ROUTE","queue":"protected","priority":1,"action":"ASSIGN_QUEUE"},
         {"reason_code":"DEADLINE_ONLY","queue":"deadline","priority":1,"action":"ASSIGN_QUEUE"},
         {"reason_code":"EXISTING_DEADLINE","queue":"deadline","priority":1,"action":"ASSIGN_QUEUE"}
       ]
     }'::jsonb) AS published
\gset

DO $$
DECLARE
    actual jsonb;
BEGIN
    SELECT pgreact_mdm.validate_inputs('mdm_fixture.policy_cases_v1'::regclass)
    INTO actual;
    IF actual IS DISTINCT FROM jsonb_build_object(
        'state', 'valid',
        'findings', jsonb_build_array(jsonb_build_object(
            'code', 'MDM_INPUT_OPENING_UNKNOWN',
            'rows', 1,
            'message', 'open cases without an authorized opening time cannot receive a due-date proposal')),
        'source_relation', 'mdm_fixture.policy_cases_v1') THEN
        RAISE EXCEPTION 'v0.47 input validation transcript mismatch: %', actual;
    END IF;
END
$$;

DROP VIEW IF EXISTS mdm_fixture.rls_policy_view;
DROP TABLE IF EXISTS mdm_fixture.rls_policy_cases;
CREATE TABLE mdm_fixture.rls_policy_cases (LIKE mdm_fixture.policy_cases_v1 INCLUDING ALL);
ALTER TABLE mdm_fixture.rls_policy_cases ENABLE ROW LEVEL SECURITY;
CREATE OR REPLACE VIEW mdm_fixture.rls_policy_view AS
SELECT * FROM mdm_fixture.rls_policy_cases;

DO $$
BEGIN
    BEGIN
        PERFORM pgreact_mdm.validate_inputs('mdm_fixture.rls_policy_view'::regclass);
        RAISE EXCEPTION 'v0.47 RLS source unexpectedly accepted';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM <> 'MDM_INPUT_RLS_UNSUPPORTED: mdm_fixture.rls_policy_view uses row-level security' THEN
            RAISE;
        END IF;
    END;
END
$$;

DO $$
BEGIN
    BEGIN
        PERFORM pgreact_mdm.publish_package(
            'policy-1',
            '{"routes":[{"reason_code":"POSSIBLE_DUPLICATE","queue":"other","priority":1}],"deadline":{"duration_seconds":3600,"min_seconds":60,"max_seconds":86400,"replace_existing_deadline":false}}'::jsonb);
        RAISE EXCEPTION 'v0.47 immutable package unexpectedly changed';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM NOT LIKE 'MDM_POLICY_IMMUTABLE:%' THEN
            RAISE;
        END IF;
    END;
END
$$;

DO $$
DECLARE
    actual jsonb;
BEGIN
    SELECT pgreact_mdm.compare_population(
        'mdm_fixture.policy_cases_v1'::regclass,
        'policy-1', 'policy-2',
        '2026-09-21 12:00:00+00',
        '{"id":"fixture-replacement","key_min":1,"key_max":7,"expected_count":7}'::jsonb)
    INTO actual;
    IF actual ->> 'state' <> 'complete'
       OR actual -> 'rows' -> 0 ->> 'changed' <> 'true'
       OR actual -> 'rows' -> 1 ->> 'changed' <> 'false'
       OR actual -> 'rows' -> 6 ->> 'changed' <> 'true' THEN
        RAISE EXCEPTION 'v0.47 replacement comparison transcript mismatch: %', actual;
    END IF;
END
$$;

DO $$
DECLARE
    actual jsonb;
    expected jsonb := $expected$[
      {"action":"ASSIGN_QUEUE","case_key":1,"decision":"WINNER","priority":1,"review_id":"40000000-0000-4000-8000-000000000001","competitors":[{"queue":"priority","action":"ASSIGN_QUEUE","priority":1,"entity_name":null,"reason_code":"POSSIBLE_DUPLICATE"}],"explanation":{"case_key":1,"decision":"WINNER","review_id":"40000000-0000-4000-8000-000000000001","competitors":[{"queue":"priority","action":"ASSIGN_QUEUE","priority":1,"entity_name":null,"reason_code":"POSSIBLE_DUPLICATE"}],"selected_queue":"priority","applicable":true,"reason":"best allowed candidate","pending_stewardship":false,"manual_assignment_protected":false},"reason_code":"POSSIBLE_DUPLICATE","current_queue":"general","selected_queue":"priority","policy_revision":"policy-1"},
      {"action":null,"case_key":2,"decision":"AMBIGUOUS","priority":null,"review_id":"40000000-0000-4000-8000-000000000002","competitors":[{"queue":"alpha","action":"ASSIGN_QUEUE","priority":1,"entity_name":null,"reason_code":"TIED_ROUTE"},{"queue":"beta","action":"ASSIGN_QUEUE","priority":1,"entity_name":null,"reason_code":"TIED_ROUTE"}],"explanation":{"case_key":2,"decision":"AMBIGUOUS","review_id":"40000000-0000-4000-8000-000000000002","competitors":[{"queue":"alpha","action":"ASSIGN_QUEUE","priority":1,"entity_name":null,"reason_code":"TIED_ROUTE"},{"queue":"beta","action":"ASSIGN_QUEUE","priority":1,"entity_name":null,"reason_code":"TIED_ROUTE"}],"selected_queue":null,"applicable":true,"reason":"equal best priorities","pending_stewardship":false,"manual_assignment_protected":false},"reason_code":"TIED_ROUTE","current_queue":null,"selected_queue":null,"policy_revision":"policy-1"},
      {"action":null,"case_key":3,"decision":"NO_CANDIDATE","priority":null,"review_id":"40000000-0000-4000-8000-000000000003","competitors":[],"explanation":{"case_key":3,"decision":"NO_CANDIDATE","review_id":"40000000-0000-4000-8000-000000000003","competitors":[],"selected_queue":null,"applicable":true,"reason":"no applicable route candidate","pending_stewardship":false,"manual_assignment_protected":false},"reason_code":"UNMATCHED","current_queue":null,"selected_queue":null,"policy_revision":"policy-1"},
      {"action":null,"case_key":4,"decision":"PROTECTED","priority":null,"review_id":"40000000-0000-4000-8000-000000000004","competitors":[{"queue":"protected","action":"ASSIGN_QUEUE","priority":1,"entity_name":null,"reason_code":"MANUAL_ROUTE"}],"explanation":{"case_key":4,"decision":"PROTECTED","review_id":"40000000-0000-4000-8000-000000000004","competitors":[{"queue":"protected","action":"ASSIGN_QUEUE","priority":1,"entity_name":null,"reason_code":"MANUAL_ROUTE"}],"selected_queue":null,"applicable":true,"reason":"manual assignment is protected","pending_stewardship":false,"manual_assignment_protected":true},"reason_code":"MANUAL_ROUTE","current_queue":"manual","selected_queue":null,"policy_revision":"policy-1"},
      {"action":null,"case_key":5,"decision":"NO_CANDIDATE","priority":null,"review_id":"40000000-0000-4000-8000-000000000005","competitors":[],"explanation":{"case_key":5,"decision":"NO_CANDIDATE","review_id":"40000000-0000-4000-8000-000000000005","competitors":[],"selected_queue":null,"applicable":true,"reason":"no applicable route candidate","pending_stewardship":false,"manual_assignment_protected":false},"reason_code":"DEADLINE_ONLY","current_queue":null,"selected_queue":null,"policy_revision":"policy-1"},
      {"action":null,"case_key":6,"decision":"NO_CANDIDATE","priority":null,"review_id":"40000000-0000-4000-8000-000000000006","competitors":[],"explanation":{"case_key":6,"decision":"NO_CANDIDATE","review_id":"40000000-0000-4000-8000-000000000006","competitors":[],"selected_queue":null,"applicable":true,"reason":"no applicable route candidate","pending_stewardship":false,"manual_assignment_protected":false},"reason_code":"EXISTING_DEADLINE","current_queue":null,"selected_queue":null,"policy_revision":"policy-1"},
      {"action":null,"case_key":7,"decision":"NO_OP","priority":null,"review_id":"40000000-0000-4000-8000-000000000007","competitors":[{"queue":"priority","action":"ASSIGN_QUEUE","priority":1,"entity_name":null,"reason_code":"POSSIBLE_DUPLICATE"}],"explanation":{"case_key":7,"decision":"NO_OP","review_id":"40000000-0000-4000-8000-000000000007","competitors":[{"queue":"priority","action":"ASSIGN_QUEUE","priority":1,"entity_name":null,"reason_code":"POSSIBLE_DUPLICATE"}],"selected_queue":null,"applicable":true,"reason":"selected queue matches current queue","pending_stewardship":false,"manual_assignment_protected":false},"reason_code":"POSSIBLE_DUPLICATE","current_queue":"priority","selected_queue":null,"policy_revision":"policy-1"}
    ]$expected$::jsonb;
BEGIN
    SELECT jsonb_agg(to_jsonb(result) ORDER BY result.case_key)
    INTO actual
    FROM pgreact_mdm.route_cases('mdm_fixture.policy_cases_v1'::regclass, 'policy-1') AS result;
    IF actual IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'v0.47 routing transcript mismatch: %', actual;
    END IF;
END
$$;

DO $$
DECLARE
    actual jsonb;
BEGIN
    SELECT jsonb_object_agg(case_key::text, jsonb_build_object(
        'decision', decision,
        'proposed_due_at', proposed_due_at,
        'reason', reason,
        'existing_due_at', existing_due_at))
    INTO actual
    FROM (
        SELECT * FROM pgreact_mdm.deadline_preview(
            'mdm_fixture.policy_cases_v1'::regclass,
            'policy-1',
            '2026-09-21 12:00:00+00')
        WHERE case_key IN (1, 5, 6)
    ) selected;
    IF actual IS DISTINCT FROM jsonb_build_object(
        '1', jsonb_build_object('decision', 'PROPOSED', 'proposed_due_at', '2026-09-21 09:00:00+00'::timestamptz, 'reason', 'elapsed duration from immutable occurrence opening time', 'existing_due_at', NULL),
        '5', jsonb_build_object('decision', 'OPENED_AT_UNKNOWN', 'proposed_due_at', NULL, 'reason', 'opening time is unavailable or unaudited', 'existing_due_at', NULL),
        '6', jsonb_build_object('decision', 'PRESERVED', 'proposed_due_at', NULL, 'reason', 'existing deadline is preserved', 'existing_due_at', '2026-09-21 10:00:00+00'::timestamptz)) THEN
        RAISE EXCEPTION 'v0.47 deadline transcript mismatch: %', actual;
    END IF;
END
$$;

DO $$
DECLARE
    actual jsonb;
BEGIN
    SELECT pgreact_mdm.compare_population(
        'mdm_fixture.policy_cases_v1'::regclass,
        'policy-1', 'policy-1',
        '2026-09-21 12:00:00+00',
        '{"id":"fixture-all","key_min":1,"key_max":7,"expected_count":7}'::jsonb)
    INTO actual;
    IF actual ->> 'state' <> 'complete'
       OR actual -> 'coverage' IS DISTINCT FROM jsonb_build_object(
           'population_id', 'fixture-all', 'key_min', 1, 'key_max', 7,
           'expected_count', 7, 'observed_count', 7, 'partial', false,
           'snapshot', 'one READ COMMITTED statement per read')
       OR actual ? 'no_effect'
       OR jsonb_array_length(actual -> 'rows') <> 7 THEN
        RAISE EXCEPTION 'v0.47 comparison transcript mismatch: %', actual;
    END IF;
END
$$;

DO $$
DECLARE
    actual jsonb;
BEGIN
    SELECT pgreact_mdm.compare_population(
        'mdm_fixture.policy_cases_v1'::regclass,
        'policy-1', 'policy-1',
        '2026-09-21 12:00:00+00',
        '{"id":"fixture-partial","key_min":1,"key_max":7,"expected_count":8}'::jsonb)
    INTO actual;
    IF actual ->> 'state' <> 'partial'
       OR actual -> 'coverage' ->> 'observed_count' <> '7'
       OR actual -> 'coverage' ->> 'partial' <> 'true' THEN
        RAISE EXCEPTION 'v0.47 partial comparison transcript mismatch: %', actual;
    END IF;
END
$$;

DO $$
DECLARE
    first_partition jsonb;
    second_partition jsonb;
    missing_partition jsonb;
    partition_keys jsonb;
    source_keys jsonb;
BEGIN
    SELECT pgreact_mdm.compare_population(
        'mdm_fixture.policy_cases_v1'::regclass, 'policy-1', 'policy-2',
        '2026-09-21 12:00:00+00',
        '{"id":"first","membership":[1,2,3],"expected_count":3}'::jsonb)
    INTO first_partition;
    SELECT pgreact_mdm.compare_population(
        'mdm_fixture.policy_cases_v1'::regclass, 'policy-1', 'policy-2',
        '2026-09-21 12:00:00+00',
        '{"id":"second","membership":[4,5,6,7],"expected_count":4}'::jsonb)
    INTO second_partition;
    SELECT jsonb_agg(case_key ORDER BY case_key) INTO source_keys
    FROM mdm_fixture.policy_cases_v1;
    SELECT jsonb_agg((row ->> 'case_key')::bigint ORDER BY (row ->> 'case_key')::bigint)
    INTO partition_keys
    FROM jsonb_array_elements((first_partition -> 'rows') || (second_partition -> 'rows')) AS item(row);
    IF first_partition ->> 'state' <> 'complete'
       OR second_partition ->> 'state' <> 'complete'
       OR partition_keys IS DISTINCT FROM source_keys THEN
        RAISE EXCEPTION 'v0.47 exact partition coverage mismatch: %, %, %',
            first_partition, second_partition, partition_keys;
    END IF;
    SELECT pgreact_mdm.compare_population(
        'mdm_fixture.policy_cases_v1'::regclass, 'policy-1', 'policy-2',
        '2026-09-21 12:00:00+00',
        '{"id":"missing","membership":[1,3,8],"expected_count":3}'::jsonb)
    INTO missing_partition;
    IF missing_partition ->> 'state' <> 'partial'
       OR missing_partition -> 'coverage' ->> 'observed_count' <> '2' THEN
        RAISE EXCEPTION 'v0.47 missing membership was not partial: %', missing_partition;
    END IF;
    BEGIN
        PERFORM pgreact_mdm.compare_population(
            'mdm_fixture.policy_cases_v1'::regclass, 'policy-1', 'policy-2',
            '2026-09-21 12:00:00+00',
            '{"id":"bad-size","membership":[1,2],"expected_count":1}'::jsonb);
        RAISE EXCEPTION 'v0.47 inconsistent membership count was accepted';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM <> 'MDM_COMPARISON_POPULATION: expected_count must match membership size' THEN
            RAISE;
        END IF;
    END;
END
$$;

DO $$
BEGIN
    BEGIN
        PERFORM pgreact_mdm.publish_package(
            'invalid-deadline',
            '{"routes":[{"reason_code":"POSSIBLE_DUPLICATE","queue":"priority","priority":1}],"deadline":{"duration_seconds":30,"min_seconds":60,"max_seconds":86400,"replace_existing_deadline":false}}'::jsonb);
        RAISE EXCEPTION 'v0.47 invalid deadline unexpectedly accepted';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM NOT LIKE 'MDM_POLICY_INVALID:%POLICY_DEADLINE_BOUNDS%' THEN
            RAISE;
        END IF;
    END;
END
$$;

DO $$
BEGIN
    IF to_regnamespace('mdm_steward') IS NOT NULL THEN
        RAISE EXCEPTION 'v0.47 fixture must run without an installed mdm_steward schema';
    END IF;
END
$$;

BEGIN;
CREATE SCHEMA mdm_steward;
DROP TABLE IF EXISTS mdm_steward.policy_cases_v1;
CREATE TABLE mdm_steward.policy_cases_v1 (LIKE mdm_fixture.policy_cases_v1 INCLUDING ALL);
ALTER TABLE mdm_steward.policy_cases_v1
    ALTER COLUMN case_key TYPE text USING case_key::text;

DO $$
BEGIN
    BEGIN
        PERFORM pgreact_mdm.validate_inputs('mdm_steward.policy_cases_v1'::regclass);
        RAISE EXCEPTION 'v0.47 invalid contract type unexpectedly accepted';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM NOT LIKE 'MDM_INPUT_CONTRACT:%case_key must have type bigint, found text' THEN
            RAISE;
        END IF;
    END;
END
$$;

DO $$
BEGIN
    BEGIN
        PERFORM pgreact_mdm.compare_population(
            'mdm_fixture.policy_cases_v1'::regclass,
            'policy-1', 'policy-1',
            '2026-09-21 12:00:00+00',
            '{"id":"invalid-range","key_min":0,"key_max":7,"expected_count":7}'::jsonb);
        RAISE EXCEPTION 'v0.47 invalid key range unexpectedly accepted';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM <> 'MDM_COMPARISON_POPULATION: invalid key range' THEN
            RAISE;
        END IF;
    END;
END
$$;

ROLLBACK;

CREATE TEMP TABLE v047_role_created(created boolean) ON COMMIT PRESERVE ROWS;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgrex_v047_reader') THEN
        CREATE ROLE pgrex_v047_reader NOLOGIN;
        INSERT INTO v047_role_created VALUES (true);
    END IF;
END
$$;

GRANT USAGE ON SCHEMA mdm_fixture, pgreact_mdm TO pgrex_v047_reader;
GRANT SELECT ON mdm_fixture.policy_cases_v1 TO pgrex_v047_reader;

CREATE TEMP TABLE v047_before AS
SELECT 'source' AS object_name,
       jsonb_agg(to_jsonb(source_row) ORDER BY source_row.case_key) AS state
FROM mdm_fixture.policy_cases_v1 AS source_row
UNION ALL
SELECT 'packages',
       jsonb_agg(to_jsonb(package_row) ORDER BY package_row.policy_revision)
FROM pgreact_mdm.policy_packages AS package_row;

SET ROLE pgrex_v047_reader;
WITH actual AS (
    SELECT pgreact_mdm.compare_population(
        'mdm_fixture.policy_cases_v1'::regclass,
        'policy-1', 'policy-1',
        '2026-09-21 12:00:00+00',
        '{"id":"reader-complete","key_min":1,"key_max":7,"expected_count":7}'::jsonb) AS result)
SELECT CASE
    WHEN (result ->> 'state') = 'complete'
     AND (result -> 'coverage' ->> 'observed_count') = '7'
    AND NOT result ? 'no_effect'
    THEN 'v0.47 normal-role comparison passed'
    ELSE current_setting('pgreact_v047_missing_setting')
END AS result
FROM actual;
RESET ROLE;

DO $$
DECLARE
    before_source jsonb;
    before_packages jsonb;
    after_source jsonb;
    after_packages jsonb;
    is_security_definer boolean;
BEGIN
    SELECT prosecdef INTO is_security_definer
    FROM pg_proc
    WHERE oid = 'pgreact_mdm.compare_population(regclass,text,text,timestamptz,jsonb)'::regprocedure;
    IF is_security_definer THEN
        RAISE EXCEPTION 'v0.47 comparison unexpectedly runs as SECURITY DEFINER';
    END IF;

    SELECT state INTO before_source FROM v047_before WHERE object_name = 'source';
    SELECT state INTO before_packages FROM v047_before WHERE object_name = 'packages';
    SELECT jsonb_agg(to_jsonb(source_row) ORDER BY source_row.case_key)
    INTO after_source FROM mdm_fixture.policy_cases_v1 AS source_row;
    SELECT jsonb_agg(to_jsonb(package_row) ORDER BY package_row.policy_revision)
    INTO after_packages FROM pgreact_mdm.policy_packages AS package_row;
    IF before_source IS DISTINCT FROM after_source
       OR before_packages IS DISTINCT FROM after_packages THEN
        RAISE EXCEPTION 'v0.47 read-only comparison changed durable state';
    END IF;
END
$$;

REVOKE USAGE ON SCHEMA mdm_fixture, pgreact_mdm FROM pgrex_v047_reader;
REVOKE SELECT ON mdm_fixture.policy_cases_v1 FROM pgrex_v047_reader;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM v047_role_created WHERE created) THEN
        DROP ROLE pgrex_v047_reader;
    END IF;
END
$$;

SELECT 'v0.47 read-only policy package passed' AS result;
