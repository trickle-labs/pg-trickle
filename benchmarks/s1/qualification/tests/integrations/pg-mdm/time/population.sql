\set ON_ERROR_STOP on
SET TIME ZONE 'UTC';

CREATE TABLE public.s1_workload_config (
    case_count integer NOT NULL,
    initial_case_key bigint NOT NULL,
    source_id_start bigint NOT NULL,
    created_at timestamptz NOT NULL,
    idle_case_key bigint
);
INSERT INTO public.s1_workload_config
SELECT :'case_count'::integer,
       COALESCE((SELECT max(case_key) FROM mdm_steward.policy_cases_v1), 0),
       COALESCE((SELECT max(id) FROM public.policy_qualification_source), 0) + 1,
       clock_timestamp();

INSERT INTO public.policy_qualification_source
SELECT c.source_id_start + member_no - 1,
       format('Issue 8 S1 Idle %s', CASE WHEN member_no <= 2 THEN 'Alpha' ELSE 'Beta' END),
       CASE member_no WHEN 1 THEN 's1-idle-one@example.test'
                      WHEN 2 THEN 's1-idle-pair@example.test'
                      WHEN 3 THEN 's1-idle-two@example.test'
                      ELSE 's1-idle-pair@example.test' END,
       c.created_at
FROM public.s1_workload_config c
CROSS JOIN generate_series(1, 4) member_no;

GRANT SELECT, UPDATE ON public.s1_workload_config TO mdm_legacy_administrator;
GRANT USAGE ON SCHEMA mdm_steward TO mdm_legacy_administrator;
GRANT SELECT ON mdm_steward.policy_cases_v1 TO mdm_legacy_administrator;
SET SESSION AUTHORIZATION mdm_legacy_login;
SET ROLE mdm_legacy_administrator;
DO $idle_case$
DECLARE result jsonb;
    initial_key bigint;
    new_cases integer;
    idle_case mdm_steward.policy_cases_v1%ROWTYPE;
BEGIN
    SELECT c.initial_case_key INTO STRICT initial_key FROM public.s1_workload_config c;
    result := mdm.refresh('policy_qualification', 'ALLOW');
    IF result ->> 'changed' IS DISTINCT FROM 'true' THEN
        RAISE EXCEPTION 'S1 idle case did not publish: %', result;
    END IF;
    SELECT count(*) INTO new_cases FROM mdm_steward.policy_cases_v1
    WHERE case_key > initial_key AND entity_name = 'policy_qualification';
    IF new_cases <> 1 THEN
        RAISE EXCEPTION 'S1 idle fixture produced % policy cases', new_cases;
    END IF;
    SELECT * INTO STRICT idle_case FROM mdm_steward.policy_cases_v1
    WHERE case_key > initial_key AND entity_name = 'policy_qualification';
    IF idle_case.status <> 'open' OR idle_case.pending_stewardship
       OR idle_case.manual_assignment_protected OR idle_case.escalation_level <> 0
       OR NOT ('ESCALATE' = ANY(idle_case.permitted_actions)) THEN
        RAISE EXCEPTION 'S1 idle fixture has unexpected admission state: %', to_jsonb(idle_case);
    END IF;
    PERFORM mdm_steward.set_case_controls(
        idle_case.case_key, idle_case.assigned_queue, clock_timestamp() + interval '1 day',
        1, idle_case.manual_assignment_protected, idle_case.action_revision,
        'S1 separate future-deadline feasibility case');
    UPDATE public.s1_workload_config SET idle_case_key = idle_case.case_key;
END
$idle_case$;
RESET ROLE;
RESET SESSION AUTHORIZATION;

UPDATE public.s1_workload_config
SET initial_case_key = (SELECT max(case_key) FROM mdm_steward.policy_cases_v1),
    source_id_start = (SELECT max(id) + 1 FROM public.policy_qualification_source),
    created_at = clock_timestamp();

INSERT INTO public.policy_qualification_source
SELECT c.source_id_start + 4 * (group_no - 1) + member_no - 1,
       format('Issue 8 Baseline %s%s%s %s',
              chr(65 + ((group_no - 1) / 676)::integer),
              chr(65 + (((group_no - 1) / 26)::integer % 26)),
              chr(65 + ((group_no - 1) % 26)::integer),
              CASE WHEN member_no <= 2 THEN 'Alpha' ELSE 'Beta' END),
       format('issue8-baseline-%s-%s@example.test', group_no,
              CASE member_no WHEN 1 THEN 'one' WHEN 2 THEN 'pair'
                             WHEN 3 THEN 'two' ELSE 'pair' END),
       c.created_at
FROM public.s1_workload_config c
CROSS JOIN generate_series(1, c.case_count) group_no
CROSS JOIN generate_series(1, 4) member_no;

SET SESSION AUTHORIZATION mdm_legacy_login;
SET ROLE mdm_legacy_administrator;
DO $refresh$
DECLARE result jsonb;
BEGIN
    result := mdm.refresh('policy_qualification', 'ALLOW');
    IF result ->> 'changed' IS DISTINCT FROM 'true' THEN
        RAISE EXCEPTION 'S1 baseline population did not publish: %', result;
    END IF;
END
$refresh$;
RESET ROLE;
RESET SESSION AUTHORIZATION;

DO $population$
DECLARE expected integer;
    actual integer;
BEGIN
    SELECT case_count INTO STRICT expected
    FROM public.s1_workload_config;
    SELECT count(*) INTO actual
    FROM mdm_steward.policy_cases_v1 c
    WHERE c.case_key > (SELECT initial_case_key FROM public.s1_workload_config)
      AND c.entity_name = 'policy_qualification';
    IF actual <> expected OR EXISTS (
        SELECT 1 FROM mdm_steward.policy_cases_v1 c
        WHERE c.case_key > (SELECT initial_case_key FROM public.s1_workload_config)
          AND c.entity_name = 'policy_qualification'
          AND (c.status <> 'open' OR c.pending_stewardship
               OR c.manual_assignment_protected OR c.escalation_level <> 0
               OR NOT ('ESCALATE' = ANY(c.permitted_actions)))
    ) THEN
        RAISE EXCEPTION 'S1 baseline population differs from the exact eligible cohort: expected %, found %',
            expected, actual;
    END IF;
END
$population$;

SELECT 'S1 exact eligible MDM cohort populated before temporal runtime: PASS' AS result;
