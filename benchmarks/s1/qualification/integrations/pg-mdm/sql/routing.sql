CREATE OR REPLACE FUNCTION pgreact_mdm.route_cases(
    source_relation regclass,
    target_revision text,
    target_entity text
)
RETURNS TABLE(
    case_key bigint,
    review_id uuid,
    policy_revision text,
    reason_code text,
    current_queue name,
    selected_queue name,
    decision text,
    action text,
    priority integer,
    competitors jsonb,
    explanation jsonb
)
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    package jsonb;
BEGIN
    PERFORM pgreact_mdm.assert_source(source_relation);
    IF pgreact_mdm.validate_inputs(source_relation) ->> 'state' <> 'valid' THEN
        RAISE EXCEPTION 'MDM_INPUT_INVALID: %',
            pgreact_mdm.validate_inputs(source_relation) -> 'findings';
    END IF;
    SELECT policy_package
    INTO package
    FROM pgreact_mdm.policy_packages
    WHERE pgreact_mdm.policy_packages.policy_revision = target_revision;
    IF package IS NULL THEN
        RAISE EXCEPTION 'MDM_POLICY_NOT_FOUND: policy revision %', target_revision;
    END IF;
    RETURN QUERY EXECUTE format($query$
        WITH source_rows AS (
            SELECT * FROM %s
            WHERE $3 IS NULL OR entity_name::text = $3
        ), candidate_rows AS (
            SELECT c.case_key, c.review_id, c.reason_code, c.assigned_queue,
                   c.due_at, c.escalation_level,
                   c.manual_assignment_protected, c.pending_stewardship,
                   c.status, c.entity_name,
                   CASE WHEN m.route IS NULL THEN NULL ELSE
                       jsonb_build_object(
                           'reason_code', m.route ->> 'reason_code',
                           'entity_name', NULLIF(m.route ->> 'entity_name', ''),
                           'queue', m.route ->> 'queue',
                           'priority', COALESCE((m.route ->> 'priority')::integer, 0),
                           'action', COALESCE(m.route ->> 'action', 'ASSIGN_QUEUE'))
                       || CASE WHEN COALESCE(m.route ->> 'action', 'ASSIGN_QUEUE') = 'ESCALATE'
                               THEN jsonb_build_object('level', (m.route ->> 'level')::integer)
                               ELSE '{}'::jsonb END
                   END AS candidate,
                   CASE WHEN m.route IS NOT NULL
                        THEN COALESCE((m.route ->> 'priority')::integer, 0) END AS candidate_priority,
                   CASE WHEN COALESCE(m.route ->> 'action', 'ASSIGN_QUEUE') = 'ESCALATE'
                        THEN (m.route ->> 'level')::integer END AS candidate_level,
                   CASE WHEN m.route IS NOT NULL
                        THEN COALESCE(m.route ->> 'action', 'ASSIGN_QUEUE') END AS candidate_action,
                   m.route ->> 'queue' AS candidate_queue
            FROM source_rows AS c
            LEFT JOIN LATERAL (
                SELECT route
                FROM jsonb_array_elements($1 -> 'routes') AS item(route)
                WHERE c.status = 'open'
                  AND c.reason_code = route ->> 'reason_code'
                  AND (route ->> 'entity_name' IS NULL
                       OR route ->> 'entity_name' = c.entity_name::text)
                  AND (NOT ($1 -> 'applicability' ? 'statuses')
                       OR c.status = ANY(ARRAY(
                           SELECT jsonb_array_elements_text($1 -> 'applicability' -> 'statuses'))))
                  AND (NOT ($1 -> 'applicability' ? 'reason_codes')
                       OR c.reason_code = ANY(ARRAY(
                           SELECT jsonb_array_elements_text($1 -> 'applicability' -> 'reason_codes'))))
                  AND (NOT ($1 -> 'applicability' ? 'entity_names')
                       OR c.entity_name::text = ANY(ARRAY(
                           SELECT jsonb_array_elements_text($1 -> 'applicability' -> 'entity_names'))))
                  AND c.permitted_actions @> ARRAY[
                        COALESCE(route ->> 'action', 'ASSIGN_QUEUE')]::text[]
            ) AS m ON true
        ), scored AS (
            SELECT c.*, min(candidate_priority) OVER (PARTITION BY case_key) AS best_priority
            FROM candidate_rows AS c
        ), summarized AS (
            SELECT case_key, review_id, reason_code, assigned_queue, due_at,
                   escalation_level, manual_assignment_protected,
                   pending_stewardship, status, best_priority,
                   count(*) FILTER (WHERE candidate_priority = best_priority) AS best_count,
                   (array_agg(candidate_queue ORDER BY candidate_priority,
                       candidate_queue, candidate ->> 'action') FILTER (
                       WHERE candidate_priority = best_priority))[1]::name AS best_queue,
                   (array_agg(candidate_action ORDER BY candidate_priority,
                       candidate_queue, candidate ->> 'action') FILTER (
                       WHERE candidate_priority = best_priority))[1] AS best_action,
                   (array_agg(candidate_level ORDER BY candidate_priority,
                       candidate_queue, candidate ->> 'action') FILTER (
                       WHERE candidate_priority = best_priority))[1] AS best_level,
                   COALESCE(jsonb_agg(candidate ORDER BY candidate_priority,
                       candidate_queue, candidate ->> 'action') FILTER (
                       WHERE candidate IS NOT NULL), '[]'::jsonb) AS competitors
            FROM scored
            GROUP BY case_key, review_id, reason_code, assigned_queue, due_at,
                     escalation_level, manual_assignment_protected,
                     pending_stewardship, status, best_priority
        ), decided AS (
            SELECT s.*,
                   CASE
                       WHEN s.status <> 'open' OR s.pending_stewardship THEN 'INELIGIBLE'
                       WHEN s.best_priority IS NULL THEN 'NO_CANDIDATE'
                       WHEN s.best_count > 1 THEN 'AMBIGUOUS'
                       WHEN s.best_action = 'ESCALATE'
                        AND (s.due_at IS NULL OR s.due_at > statement_timestamp()
                             OR s.best_level <> s.escalation_level + 1) THEN 'INELIGIBLE'
                       WHEN s.best_action = 'ESCALATE' THEN 'WINNER'
                       WHEN s.manual_assignment_protected THEN 'PROTECTED'
                       WHEN s.assigned_queue IS NOT DISTINCT FROM s.best_queue THEN 'NO_OP'
                       ELSE 'WINNER'
                   END AS decision
            FROM summarized AS s
        )
        SELECT d.case_key, d.review_id, $2, d.reason_code, d.assigned_queue,
               CASE WHEN d.decision = 'WINNER' AND d.best_action = 'ASSIGN_QUEUE'
                    THEN d.best_queue ELSE NULL END,
               d.decision,
               CASE WHEN d.decision = 'WINNER' THEN d.best_action ELSE NULL END,
               CASE WHEN d.decision = 'WINNER' THEN d.best_priority ELSE NULL END,
               d.competitors,
               jsonb_build_object(
                   'case_key', d.case_key,
                   'review_id', d.review_id,
                   'decision', d.decision,
                   'selected_queue', CASE WHEN d.decision = 'WINNER' AND d.best_action = 'ASSIGN_QUEUE'
                                          THEN d.best_queue ELSE NULL END,
                   'applicable', d.status = 'open' AND NOT d.pending_stewardship,
                   'reason', CASE d.decision
                       WHEN 'WINNER' THEN 'best allowed candidate'
                       WHEN 'AMBIGUOUS' THEN 'equal best priorities'
                       WHEN 'NO_CANDIDATE' THEN 'no applicable route candidate'
                       WHEN 'PROTECTED' THEN 'manual assignment is protected'
                       WHEN 'NO_OP' THEN 'selected queue matches current queue'
                       ELSE 'case is not eligible for automation'
                   END,
                   'manual_assignment_protected', d.manual_assignment_protected,
                   'pending_stewardship', d.pending_stewardship,
                   'competitors', d.competitors)
                || CASE WHEN d.decision = 'WINNER' AND d.best_action = 'ESCALATE'
                        THEN jsonb_build_object('escalation_level', d.best_level)
                        ELSE '{}'::jsonb END
        FROM decided AS d
        ORDER BY d.case_key
    $query$, source_relation)
    USING package, target_revision, target_entity;
END
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.route_cases(
    source_relation regclass,
    target_revision text
)
RETURNS TABLE(
    case_key bigint,
    review_id uuid,
    policy_revision text,
    reason_code text,
    current_queue name,
    selected_queue name,
    decision text,
    action text,
    priority integer,
    competitors jsonb,
    explanation jsonb
)
LANGUAGE SQL
STABLE
SET search_path = pg_catalog, pg_temp
AS $function$
    SELECT * FROM pgreact_mdm.route_cases($1, $2, NULL::text)
$function$;

COMMENT ON FUNCTION pgreact_mdm.route_cases(regclass, text, text) IS
    'v0.47 deterministic MDM routing explanation; ties are ambiguous and never lexical winners';
COMMENT ON FUNCTION pgreact_mdm.route_cases(regclass, text) IS
    'v0.47 deterministic routing across the complete supplied policy relation';
