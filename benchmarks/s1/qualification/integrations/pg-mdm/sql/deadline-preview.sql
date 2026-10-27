CREATE OR REPLACE FUNCTION pgreact_mdm.deadline_preview(
    source_relation regclass,
    target_revision text,
    captured_at timestamptz,
    target_entity text
)
RETURNS TABLE(
    case_key bigint,
    review_id uuid,
    policy_revision text,
    evaluated_at timestamptz,
    existing_due_at timestamptz,
    proposed_due_at timestamptz,
    decision text,
    reason text,
    explanation jsonb
)
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    package jsonb;
BEGIN
    IF captured_at IS NULL THEN
        RAISE EXCEPTION 'MDM_DEADLINE_CAPTURE: captured_at is required';
    END IF;
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
        SELECT c.case_key, c.review_id, $2, $3::timestamptz, c.due_at,
               CASE
                   WHEN c.status = 'open'
                    AND c.opened_at IS NOT NULL
                    AND c.opened_at_source <> 'unknown'
                    AND c.permitted_actions @> ARRAY['SET_DUE_AT']::text[]
                    AND (c.due_at IS NULL OR ($1 -> 'deadline' ->> 'replace_existing_deadline')::boolean)
                   THEN c.opened_at + (($1 -> 'deadline' ->> 'duration_seconds')::bigint * interval '1 second')
                   ELSE NULL
               END,
               CASE
                   WHEN c.status <> 'open' THEN 'INELIGIBLE'
                   WHEN NOT c.permitted_actions @> ARRAY['SET_DUE_AT']::text[] THEN 'INELIGIBLE'
                   WHEN c.opened_at IS NULL OR c.opened_at_source = 'unknown' THEN 'OPENED_AT_UNKNOWN'
                   WHEN c.due_at IS NOT NULL
                    AND NOT ($1 -> 'deadline' ->> 'replace_existing_deadline')::boolean THEN 'PRESERVED'
                   WHEN c.due_at IS NULL THEN 'PROPOSED'
                   ELSE 'REPLACEMENT'
               END,
               CASE
                   WHEN c.status <> 'open' THEN 'case is not open'
                   WHEN NOT c.permitted_actions @> ARRAY['SET_DUE_AT']::text[] THEN 'SET_DUE_AT is not permitted for this case'
                   WHEN c.opened_at IS NULL OR c.opened_at_source = 'unknown' THEN 'opening time is unavailable or unaudited'
                   WHEN c.due_at IS NOT NULL
                    AND NOT ($1 -> 'deadline' ->> 'replace_existing_deadline')::boolean THEN 'existing deadline is preserved'
                   WHEN c.due_at IS NULL THEN 'elapsed duration from immutable occurrence opening time'
                   ELSE 'explicit replacement authority is present in the package'
               END,
               jsonb_build_object(
                   'case_key', c.case_key,
                   'review_id', c.review_id,
                   'opened_at', c.opened_at,
                   'opened_at_source', c.opened_at_source,
                   'duration_seconds', ($1 -> 'deadline' ->> 'duration_seconds')::bigint,
                   'captured_at', $3::timestamptz)
        FROM %s AS c
        WHERE $4 IS NULL OR c.entity_name::text = $4
        ORDER BY c.case_key
    $query$, source_relation)
    USING package, target_revision, captured_at, target_entity;
END
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.deadline_preview(
    source_relation regclass,
    target_revision text,
    captured_at timestamptz
)
RETURNS TABLE(
    case_key bigint,
    review_id uuid,
    policy_revision text,
    evaluated_at timestamptz,
    existing_due_at timestamptz,
    proposed_due_at timestamptz,
    decision text,
    reason text,
    explanation jsonb
)
LANGUAGE SQL
STABLE
SET search_path = pg_catalog, pg_temp
AS $function$
    SELECT * FROM pgreact_mdm.deadline_preview($1, $2, $3, NULL::text)
$function$;

COMMENT ON FUNCTION pgreact_mdm.deadline_preview(regclass, text, timestamptz, text) IS
    'v0.47 elapsed-time deadline proposal using an explicit captured evaluation time';
COMMENT ON FUNCTION pgreact_mdm.deadline_preview(regclass, text, timestamptz) IS
    'v0.47 elapsed-time deadline proposal across the complete supplied policy relation';
