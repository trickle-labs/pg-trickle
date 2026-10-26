\set ON_ERROR_STOP on
SET TIME ZONE 'UTC';

CREATE TABLE s1_audit.workload_config (
    cohort text PRIMARY KEY,
    case_count integer NOT NULL,
    churn_per_minute integer NOT NULL,
    initial_case_key bigint NOT NULL,
    source_id_start bigint NOT NULL,
    created_at timestamptz NOT NULL,
    due_at timestamptz
);
INSERT INTO s1_audit.workload_config
SELECT 'issue8-s1-baseline', c.case_count, :'churn_per_minute'::integer,
       c.initial_case_key, c.source_id_start, c.created_at, NULL
FROM public.s1_workload_config c;

CREATE TABLE s1_audit.workload_source_rows (
    id bigint PRIMARY KEY,
    display_name text NOT NULL,
    email_address text NOT NULL,
    updated_at timestamptz NOT NULL
);
INSERT INTO s1_audit.workload_source_rows
SELECT s.id, s.display_name, s.email_address, s.updated_at
FROM public.policy_qualification_source s
JOIN s1_audit.workload_config c
  ON s.id >= c.source_id_start
 AND s.id < c.source_id_start + 4 * c.case_count
ORDER BY s.id;

CREATE TABLE s1_audit.workload_cases (
    case_key bigint PRIMARY KEY,
    expected_action text NOT NULL,
    expected_level integer NOT NULL,
    expected_due_at timestamptz NOT NULL,
    case_state jsonb NOT NULL
);
INSERT INTO s1_audit.workload_cases
SELECT c.case_key, 'ESCALATE', c.escalation_level + 2,
       'infinity'::timestamptz, to_jsonb(c)
FROM mdm_steward.policy_cases_v1 c
JOIN s1_audit.workload_config w ON c.case_key > w.initial_case_key;

DO $population$
DECLARE expected integer;
    actual integer;
BEGIN
    SELECT case_count INTO STRICT expected
    FROM s1_audit.workload_config WHERE cohort = 'issue8-s1-baseline';
    SELECT count(*) INTO actual FROM s1_audit.workload_cases;
    IF actual <> expected OR EXISTS (
        SELECT 1 FROM s1_audit.workload_cases w
        JOIN mdm_steward.policy_cases_v1 c USING (case_key)
        WHERE c.status <> 'open' OR c.pending_stewardship
           OR c.manual_assignment_protected OR c.escalation_level <> 0
           OR NOT ('ESCALATE' = ANY(c.permitted_actions))
    ) THEN
        RAISE EXCEPTION 'S1 baseline population differs from the exact eligible cohort: expected %, found %',
            expected, actual;
    END IF;
END
$population$;

UPDATE s1_audit.workload_config
SET due_at = clock_timestamp() + interval '30 seconds'
WHERE cohort = 'issue8-s1-baseline';
GRANT USAGE ON SCHEMA s1_audit TO mdm_legacy_administrator;
GRANT SELECT ON s1_audit.workload_config TO mdm_legacy_administrator;
SET SESSION AUTHORIZATION mdm_legacy_login;
SET ROLE mdm_legacy_administrator;
DO $controls$
DECLARE workload_due timestamptz;
    case_row record;
BEGIN
    SELECT due_at INTO STRICT workload_due
    FROM s1_audit.workload_config WHERE cohort = 'issue8-s1-baseline';
    FOR case_row IN
        SELECT c.case_key, c.assigned_queue, c.escalation_level,
               c.manual_assignment_protected, c.action_revision
        FROM mdm_steward.policy_cases_v1 c
        JOIN s1_audit.workload_config w ON c.case_key > w.initial_case_key
        ORDER BY c.case_key
    LOOP
        PERFORM mdm_steward.set_case_controls(
            case_row.case_key, case_row.assigned_queue, workload_due,
            case_row.escalation_level + 1, case_row.manual_assignment_protected,
            case_row.action_revision, 'S1 1000-case due-burst baseline');
    END LOOP;
END
$controls$;
RESET ROLE;
RESET SESSION AUTHORIZATION;

SELECT pgreact_mdm.sync_intent_case_identities();
UPDATE s1_audit.workload_cases w
SET expected_due_at = config.due_at,
    case_state = to_jsonb(c)
FROM s1_audit.workload_config config
JOIN mdm_steward.policy_cases_v1 c ON c.case_key > config.initial_case_key
WHERE config.cohort = 'issue8-s1-baseline'
  AND w.case_key = c.case_key;
INSERT INTO s1_audit.source(case_key, deadline)
SELECT case_key, expected_due_at
FROM s1_audit.workload_cases
ORDER BY case_key;

CREATE TABLE s1_audit.churn_admissions (
    batch integer PRIMARY KEY,
    started_at timestamptz NOT NULL,
    completed_at timestamptz NOT NULL,
    case_key bigint NOT NULL,
    source_rows jsonb NOT NULL,
    case_state_before jsonb NOT NULL,
    case_state jsonb NOT NULL,
    due_at timestamptz NOT NULL,
    expected_action text NOT NULL,
    expected_level integer NOT NULL
);
GRANT INSERT, SELECT ON public.policy_qualification_source TO mdm_legacy_administrator;
GRANT USAGE ON SCHEMA mdm_steward TO mdm_legacy_administrator;
GRANT SELECT ON mdm_steward.policy_cases_v1 TO mdm_legacy_administrator;
GRANT INSERT, SELECT ON s1_audit.source, s1_audit.churn_admissions TO mdm_legacy_administrator;

CREATE FUNCTION s1_audit.admit_churn_case(batch integer)
RETURNS jsonb
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $churn$
DECLARE started timestamptz := clock_timestamp();
    initial_case_key bigint;
    new_case_count integer;
    new_case_key bigint;
    source_id bigint;
    due_at timestamptz := clock_timestamp() + interval '1 day';
    case_row mdm_steward.policy_cases_v1%ROWTYPE;
    case_state_before jsonb;
    source_rows jsonb;
    result jsonb;
BEGIN
    SELECT COALESCE(max(case_key), 0) INTO initial_case_key
    FROM mdm_steward.policy_cases_v1;
    SELECT COALESCE(max(id), 0) + 1 INTO source_id
    FROM public.policy_qualification_source;
    INSERT INTO public.policy_qualification_source VALUES
        (source_id, format('Issue 8 Churn %s%s Alpha',
            chr(65 + ((batch - 1) / 26)::integer),
            chr(65 + ((batch - 1) % 26)::integer)),
         format('issue8-churn-%s-one@example.test', batch), clock_timestamp()),
        (source_id + 1, format('Issue 8 Churn %s%s Alpha',
            chr(65 + ((batch - 1) / 26)::integer),
            chr(65 + ((batch - 1) % 26)::integer)),
         format('issue8-churn-%s-pair@example.test', batch), clock_timestamp()),
        (source_id + 2, format('Issue 8 Churn %s%s Beta',
            chr(65 + ((batch - 1) / 26)::integer),
            chr(65 + ((batch - 1) % 26)::integer)),
         format('issue8-churn-%s-two@example.test', batch), clock_timestamp()),
        (source_id + 3, format('Issue 8 Churn %s%s Beta',
            chr(65 + ((batch - 1) / 26)::integer),
            chr(65 + ((batch - 1) % 26)::integer)),
         format('issue8-churn-%s-pair@example.test', batch), clock_timestamp());
    result := mdm.refresh('policy_qualification', 'ALLOW');
    IF result ->> 'changed' IS DISTINCT FROM 'true' THEN
        RAISE EXCEPTION 'S1 churn case % did not publish: %', batch, result;
    END IF;
    SELECT count(*), max(case_key) INTO new_case_count, new_case_key
    FROM mdm_steward.policy_cases_v1
    WHERE case_key > initial_case_key AND entity_name = 'policy_qualification';
    IF new_case_count <> 1 THEN
        RAISE EXCEPTION 'S1 churn case % produced % policy cases', batch, new_case_count;
    END IF;
    SELECT * INTO STRICT case_row FROM mdm_steward.policy_cases_v1
    WHERE case_key = new_case_key;
    IF case_row.status <> 'open' OR case_row.pending_stewardship
       OR case_row.manual_assignment_protected OR case_row.escalation_level <> 0
       OR NOT ('ESCALATE' = ANY(case_row.permitted_actions)) THEN
        RAISE EXCEPTION 'S1 churn case % has unexpected admission state: %', batch, to_jsonb(case_row);
    END IF;
    case_state_before := to_jsonb(case_row);
    PERFORM mdm_steward.set_case_controls(
        case_row.case_key, case_row.assigned_queue, due_at,
        case_row.escalation_level + 1, case_row.manual_assignment_protected,
        case_row.action_revision, 'S1 60-case/minute churn baseline');
    SELECT * INTO STRICT case_row FROM mdm_steward.policy_cases_v1
    WHERE case_key = new_case_key;
    SELECT jsonb_agg(to_jsonb(s) ORDER BY id) INTO source_rows
    FROM public.policy_qualification_source s
    WHERE id BETWEEN source_id AND source_id + 3;
    INSERT INTO s1_audit.source(case_key, deadline)
    VALUES (case_row.case_key, due_at);
    INSERT INTO s1_audit.churn_admissions
    VALUES (batch, started, clock_timestamp(), case_row.case_key, source_rows,
            case_state_before, to_jsonb(case_row), due_at, 'ESCALATE', case_row.escalation_level + 1);
    RETURN jsonb_build_object('batch', batch, 'case_key', case_row.case_key,
        'started_at', started, 'completed_at', clock_timestamp(),
        'due_at', due_at, 'refresh', result);
END
$churn$;
REVOKE ALL ON FUNCTION s1_audit.admit_churn_case(integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION s1_audit.admit_churn_case(integer) TO mdm_legacy_administrator;

SELECT 'S1 exact eligible due-burst population prepared: PASS' AS result;
