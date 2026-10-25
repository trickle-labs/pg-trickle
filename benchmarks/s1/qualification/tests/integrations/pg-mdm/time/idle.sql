\set ON_ERROR_STOP on
SET TIME ZONE 'UTC';
\ir ../../../../integrations/pg-mdm/sql/policy-inputs.sql
\ir ../../../../integrations/pg-mdm/sql/routing.sql
\ir ../../../../integrations/pg-mdm/sql/deadline-preview.sql
\ir ../../../../integrations/pg-mdm/sql/comparison.sql
\ir ../../../../integrations/pg-mdm/sql/typed-flow.sql
\ir ../../../../integrations/pg-mdm/sql/request-store.sql
\ir ../../../../integrations/pg-mdm/sql/intent-worker.sql
GRANT USAGE ON SCHEMA pgreact_mdm TO mdm_legacy_administrator, mdm_administrator;

CREATE ROLE mdm_s1_runner LOGIN NOSUPERUSER NOBYPASSRLS NOINHERIT;
CREATE ROLE mdm_s1_runtime LOGIN NOSUPERUSER NOBYPASSRLS INHERIT;
CREATE ROLE s1_operator NOLOGIN;
CREATE ROLE s1_reader NOLOGIN;
CREATE ROLE s1_advanced_reader NOLOGIN;
GRANT pgreact_mdm_worker TO mdm_s1_runner WITH SET TRUE, INHERIT FALSE;
GRANT pgreact_mdm_worker TO mdm_s1_runtime WITH SET TRUE, INHERIT TRUE;
GRANT USAGE ON SCHEMA pgreact_mdm TO mdm_s1_runner;
GRANT CONNECT ON DATABASE s1_idle TO mdm_s1_runtime;
GRANT SELECT ON pgreact_mdm.intent_deployer_policy_cases_v1,
    pgreact_mdm.intent_escalation_candidates,
    pgreact_mdm.policy_intent_bindings,
    pgreact_mdm.authorized_policy_cases_v1 TO mdm_s1_runner;
GRANT EXECUTE ON FUNCTION pgreact_mdm.submit_escalation_intent(
    pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1),
    pgreact_mdm.submit_intent_as_worker(pgreact.activation_context, jsonb)
    TO mdm_s1_runner;
SELECT pgreact_api.configure_roles('mdm_s1_runner', 's1_operator',
    'pgreact_mdm_worker', 's1_reader', 's1_advanced_reader');
GRANT USAGE, CREATE ON SCHEMA pgreact_runtime TO mdm_s1_runner;
SELECT pgreact_mdm.configure_intent_deployer('mdm_m2_runner');
CREATE SCHEMA s1_audit;
CREATE TABLE s1_audit.source (
    case_key bigint PRIMARY KEY,
    deadline timestamptz NOT NULL
);
CREATE VIEW s1_audit.candidate AS SELECT case_key, deadline FROM s1_audit.source;
GRANT USAGE, CREATE ON SCHEMA s1_audit TO mdm_s1_runner;
GRANT USAGE ON SCHEMA pgtrickle TO mdm_s1_runner;
GRANT SELECT ON s1_audit.source, s1_audit.candidate TO mdm_s1_runner;
ALTER VIEW s1_audit.candidate OWNER TO mdm_s1_runner;

SELECT case_key, reason_code, action_revision, COALESCE(assigned_queue::text, '') AS assigned_queue,
       escalation_level, manual_assignment_protected,
       clock_timestamp() + interval '30 seconds' AS deadline
FROM mdm_steward.policy_cases_v1
WHERE case_key = (SELECT idle_case_key FROM public.s1_workload_config)
  AND entity_name = 'policy_qualification' AND status = 'open'
  AND NOT pending_stewardship AND opened_at IS NOT NULL
  AND escalation_level = 1 AND 'ESCALATE' = ANY(permitted_actions)
ORDER BY case_key LIMIT 1
\gset
INSERT INTO s1_audit.source VALUES (:case_key, :'deadline');
SELECT pgreact_mdm.publish_intent_package('s1-idle-feasibility',
    jsonb_build_object('routes', jsonb_build_array(jsonb_build_object(
        'entity_name', 'policy_qualification', 'reason_code', :'reason_code',
        'action', 'ESCALATE', 'level', 2, 'priority', 0))));

SELECT EXISTS (
    SELECT 1 FROM pgreact_mdm.policy_intent_bindings
    WHERE entity_name = 'policy_qualification' AND enabled
) AS existing_binding
\gset
\if :existing_binding
SELECT binding_id::text AS prior_binding_id, binding_version AS prior_binding_version
FROM pgreact_mdm.policy_intent_bindings
WHERE entity_name = 'policy_qualification' AND enabled
\gset
\endif
SET SESSION AUTHORIZATION mdm_legacy_login;
SET ROLE mdm_legacy_administrator;
SELECT mdm_steward.set_case_controls(
    :case_key, NULLIF(:'assigned_queue', '')::name, :'deadline'::timestamptz,
    :escalation_level, :'manual_assignment_protected'::boolean,
    :action_revision, 'S1 future-deadline fixture');
\if :existing_binding
SELECT * FROM pgreact_mdm.replace_intent_binding(
    :'prior_binding_id'::uuid, :'prior_binding_version'::bigint,
    's1-idle-feasibility', ARRAY['ESCALATE']::text[],
    ARRAY[]::text[], NULL, 2);
\else
SELECT * FROM pgreact_mdm.create_intent_binding(
    'policy_qualification', 's1-idle-feasibility',
    ARRAY['ESCALATE']::text[], ARRAY[]::text[], NULL, 2);
\endif
RESET ROLE;
RESET SESSION AUTHORIZATION;
SELECT pgreact_mdm.sync_intent_case_identities();

CREATE TABLE s1_audit.callback_failure_rules (rule_id uuid PRIMARY KEY);
GRANT SELECT ON s1_audit.callback_failure_rules TO mdm_s1_runner;
CREATE FUNCTION s1_audit.deliver(
    context pgreact.activation_context, candidate s1_audit.candidate
)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $callback$
DECLARE current_case pgreact_mdm.intent_deployer_policy_cases_v1;
BEGIN
    RAISE WARNING 'registered-S1 callback entry rule_id=% session_user=% current_user=%',
        ($1).rule_id, session_user, current_user;
    IF EXISTS (SELECT 1 FROM s1_audit.callback_failure_rules AS failure_rule
               WHERE failure_rule.rule_id = ($1).rule_id) THEN
        RAISE EXCEPTION 'injected registered S1 callback failure';
    END IF;
    SELECT policy_case.* INTO STRICT current_case
    FROM pgreact_mdm.intent_deployer_policy_cases_v1 AS policy_case
    WHERE policy_case.case_key = ($2).case_key;
    PERFORM pgreact_mdm.submit_escalation_intent($1, current_case);
END
$callback$;
ALTER FUNCTION s1_audit.deliver(pgreact.activation_context, s1_audit.candidate)
    OWNER TO mdm_s1_runner;
REVOKE CREATE ON SCHEMA s1_audit FROM mdm_s1_runner;
REVOKE ALL ON FUNCTION s1_audit.deliver(
    pgreact.activation_context, s1_audit.candidate) FROM PUBLIC;
SET SESSION AUTHORIZATION mdm_s1_runner;
SELECT pgreact_api.author_deadline_rule(
    's1-future-idle', 's1_audit.candidate'::regclass,
    'case_key', 'deadline', 'COMMAND',
    's1_audit.deliver(pgreact.activation_context,s1_audit.candidate)');
RESET SESSION AUTHORIZATION;

CREATE ROLE mdm_s1_rotated_worker NOLOGIN;
DO $worker_grants$
DECLARE
    stream_relation oid;
    stream_owner name;
BEGIN
    SELECT version.match_relid,
           pg_catalog.pg_get_userbyid(relation.relowner)
      INTO STRICT stream_relation, stream_owner
    FROM pgreact_internal.rule_versions AS version
    JOIN pg_catalog.pg_class AS relation ON relation.oid = version.match_relid
    WHERE version.match_name LIKE 'pgreact_runtime.%';

    IF stream_owner <> 'mdm_s1_runner'
       OR NOT pg_catalog.has_table_privilege('pgreact_mdm_worker', stream_relation, 'MAINTAIN')
       OR pg_catalog.has_table_privilege('pgreact_mdm_worker', 's1_audit.source', 'SELECT')
       OR pg_catalog.has_schema_privilege('pgreact_mdm_worker', 's1_audit', 'USAGE') THEN
        RAISE EXCEPTION 'worker refresh grant must preserve stream owner and withhold source access';
    END IF;
END
$worker_grants$;

SELECT pgreact_api.configure_roles('mdm_s1_runner', 's1_operator',
    'mdm_s1_rotated_worker', 's1_reader', 's1_advanced_reader');
DO $worker_rotation$
DECLARE stream_relation oid;
BEGIN
    SELECT match_relid INTO STRICT stream_relation
    FROM pgreact_internal.rule_versions
    WHERE match_name LIKE 'pgreact_runtime.%';

    IF pg_catalog.has_table_privilege('pgreact_mdm_worker', stream_relation, 'MAINTAIN')
       OR NOT pg_catalog.has_table_privilege('mdm_s1_rotated_worker', stream_relation, 'MAINTAIN')
       OR pg_catalog.has_table_privilege('mdm_s1_rotated_worker', 's1_audit.source', 'SELECT') THEN
        RAISE EXCEPTION 'worker rotation did not move only stream MAINTAIN';
    END IF;
END
$worker_rotation$;

SELECT pgreact_api.configure_roles('mdm_s1_runner', 's1_operator',
    'pgreact_mdm_worker', 's1_reader', 's1_advanced_reader');
DO $worker_restore$
DECLARE stream_relation oid;
BEGIN
    SELECT match_relid INTO STRICT stream_relation
    FROM pgreact_internal.rule_versions
    WHERE match_name LIKE 'pgreact_runtime.%';

    IF NOT pg_catalog.has_table_privilege('pgreact_mdm_worker', stream_relation, 'MAINTAIN')
       OR pg_catalog.has_table_privilege('mdm_s1_rotated_worker', stream_relation, 'MAINTAIN') THEN
        RAISE EXCEPTION 'restored worker role vector differs';
    END IF;
END
$worker_restore$;

DO $before$
BEGIN
    IF EXISTS (SELECT 1 FROM mdm_steward.policy_receipts_v1
        WHERE request_body->>'policy_revision' = 's1-idle-feasibility')
       OR EXISTS (SELECT 1 FROM s1_audit.source WHERE deadline <= clock_timestamp()) THEN
        RAISE EXCEPTION 'S1 requires an unexpired future deadline and no prior audit receipt';
    END IF;
END
$before$;
SET SESSION AUTHORIZATION mdm_s1_runner;
SELECT pgreact_api.author_deadline_rule(
    's1-callback-failure', 's1_audit.candidate'::regclass,
    'case_key', 'deadline', 'COMMAND',
    's1_audit.deliver(pgreact.activation_context,s1_audit.candidate)');
RESET SESSION AUTHORIZATION;
INSERT INTO s1_audit.callback_failure_rules
SELECT rule.rule_id FROM pgreact_internal.rules AS rule
WHERE rule.rule_name = 's1-callback-failure';
