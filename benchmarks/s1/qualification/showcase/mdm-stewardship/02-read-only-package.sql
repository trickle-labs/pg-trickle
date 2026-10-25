\set ON_ERROR_STOP on
SET TIME ZONE 'UTC';

\ir ../../integrations/pg-mdm/sql/policy-inputs.sql
\ir ../../integrations/pg-mdm/sql/routing.sql
\ir ../../integrations/pg-mdm/sql/deadline-preview.sql
\ir ../../integrations/pg-mdm/sql/comparison.sql

SELECT pgreact_mdm.publish_package(
    'policy-1',
    '{
       "applicability": {},
       "deadline": {
         "duration_seconds": 3600,
         "min_seconds": 60,
         "max_seconds": 86400,
         "replace_existing_deadline": false
       },
       "routes": [
         {"reason_code":"POSSIBLE_DUPLICATE","queue":"priority","priority":1,"action":"ASSIGN_QUEUE"},
         {"reason_code":"TIED_ROUTE","queue":"alpha","priority":1,"action":"ASSIGN_QUEUE"},
         {"reason_code":"TIED_ROUTE","queue":"beta","priority":1,"action":"ASSIGN_QUEUE"},
         {"reason_code":"MANUAL_ROUTE","queue":"protected","priority":1,"action":"ASSIGN_QUEUE"},
         {"reason_code":"DEADLINE_ONLY","queue":"deadline","priority":1,"action":"ASSIGN_QUEUE"},
         {"reason_code":"EXISTING_DEADLINE","queue":"deadline","priority":1,"action":"ASSIGN_QUEUE"}
       ]
     }'::jsonb) AS published;

SELECT pgreact_mdm.publish_package(
    'policy-2',
    '{
       "applicability": {},
       "deadline": {
         "duration_seconds": 7200,
         "min_seconds": 60,
         "max_seconds": 86400,
         "replace_existing_deadline": true
       },
       "routes": [
         {"reason_code":"POSSIBLE_DUPLICATE","queue":"urgent","priority":1,"action":"ASSIGN_QUEUE"},
         {"reason_code":"TIED_ROUTE","queue":"alpha","priority":1,"action":"ASSIGN_QUEUE"},
         {"reason_code":"TIED_ROUTE","queue":"beta","priority":1,"action":"ASSIGN_QUEUE"},
         {"reason_code":"MANUAL_ROUTE","queue":"protected","priority":1,"action":"ASSIGN_QUEUE"},
         {"reason_code":"DEADLINE_ONLY","queue":"deadline","priority":1,"action":"ASSIGN_QUEUE"},
         {"reason_code":"EXISTING_DEADLINE","queue":"deadline","priority":1,"action":"ASSIGN_QUEUE"}
       ]
     }'::jsonb) AS published;

SELECT jsonb_agg(to_jsonb(result) ORDER BY result.case_key) AS routing
FROM pgreact_mdm.route_cases('mdm_fixture.policy_cases_v1'::regclass, 'policy-1') AS result;

SELECT jsonb_agg(to_jsonb(result) ORDER BY result.case_key) AS deadlines
FROM pgreact_mdm.deadline_preview(
    'mdm_fixture.policy_cases_v1'::regclass,
    'policy-1',
    '2026-09-21 12:00:00+00') AS result;

SELECT pgreact_mdm.compare_population(
    'mdm_fixture.policy_cases_v1'::regclass,
    'policy-1',
    'policy-2',
    '2026-09-21 12:00:00+00',
    '{"id":"fixture-all","key_min":1,"key_max":7,"expected_count":7}'::jsonb) AS comparison;
