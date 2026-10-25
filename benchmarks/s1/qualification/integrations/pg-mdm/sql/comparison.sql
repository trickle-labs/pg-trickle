CREATE OR REPLACE FUNCTION pgreact_mdm.compare_population(
    source_relation regclass,
    current_policy_revision text,
    proposed_policy_revision text,
    captured_at timestamptz,
    population jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    membership bigint[];
    key_min bigint;
    key_max bigint;
    expected_count bigint;
    observed_count bigint;
    selected_keys bigint[];
    evaluated_keys bigint[];
    rows jsonb;
    population_id text := NULLIF(population ->> 'id', '');
    has_membership boolean := population ? 'membership';
    has_range boolean := population ? 'key_min' OR population ? 'key_max';
    current_package jsonb;
    proposed_package jsonb;
BEGIN
    IF captured_at IS NULL THEN
        RAISE EXCEPTION 'MDM_COMPARISON_CAPTURE: captured_at is required';
    END IF;
    IF population_id IS NULL THEN
        RAISE EXCEPTION 'MDM_COMPARISON_POPULATION: id is required';
    END IF;
    IF has_membership = has_range THEN
        RAISE EXCEPTION 'MDM_COMPARISON_POPULATION: provide exactly one membership or key range';
    END IF;
    IF population ->> 'expected_count' IS NULL
       OR population ->> 'expected_count' !~ '^[0-9]+$' THEN
        RAISE EXCEPTION 'MDM_COMPARISON_POPULATION: expected_count must be a nonnegative integer';
    END IF;
    expected_count := (population ->> 'expected_count')::bigint;
    PERFORM pgreact_mdm.assert_source(source_relation);
    IF pgreact_mdm.validate_inputs(source_relation) ->> 'state' <> 'valid' THEN
        RAISE EXCEPTION 'MDM_INPUT_INVALID: %',
            pgreact_mdm.validate_inputs(source_relation) -> 'findings';
    END IF;
    SELECT policy_package INTO current_package
    FROM pgreact_mdm.policy_packages
    WHERE policy_revision = current_policy_revision;
    SELECT policy_package INTO proposed_package
    FROM pgreact_mdm.policy_packages
    WHERE policy_revision = proposed_policy_revision;
    IF current_package IS NULL OR proposed_package IS NULL THEN
        RAISE EXCEPTION 'MDM_POLICY_NOT_FOUND: comparison revisions % and % must be published',
            current_policy_revision, proposed_policy_revision;
    END IF;
    IF has_membership THEN
        IF jsonb_typeof(population -> 'membership') IS DISTINCT FROM 'array' THEN
            RAISE EXCEPTION 'MDM_COMPARISON_POPULATION: membership must be an array';
        END IF;
        membership := ARRAY(
            SELECT value::bigint
            FROM jsonb_array_elements_text(population -> 'membership') AS item(value));
        IF cardinality(membership) <> (
            SELECT count(DISTINCT value)
            FROM unnest(membership) AS item(value)) THEN
            RAISE EXCEPTION 'MDM_COMPARISON_POPULATION: membership contains duplicates';
        END IF;
        IF expected_count <> cardinality(membership) THEN
            RAISE EXCEPTION 'MDM_COMPARISON_POPULATION: expected_count must match membership size';
        END IF;
        SELECT array_agg(c.case_key ORDER BY c.case_key) INTO selected_keys
        FROM pgreact_mdm.policy_inputs(source_relation) AS c
        WHERE c.case_key = ANY(membership);
    ELSE
        IF population ->> 'key_min' !~ '^[0-9]+$'
           OR population ->> 'key_max' !~ '^[0-9]+$' THEN
            RAISE EXCEPTION 'MDM_COMPARISON_POPULATION: key_min and key_max must be positive integers';
        END IF;
        key_min := (population ->> 'key_min')::bigint;
        key_max := (population ->> 'key_max')::bigint;
        IF key_min <= 0 OR key_max < key_min THEN
            RAISE EXCEPTION 'MDM_COMPARISON_POPULATION: invalid key range';
        END IF;
        SELECT array_agg(c.case_key ORDER BY c.case_key) INTO selected_keys
        FROM pgreact_mdm.policy_inputs(source_relation) AS c
        WHERE c.case_key BETWEEN key_min AND key_max;
        membership := NULL;
    END IF;
    observed_count := COALESCE(cardinality(selected_keys), 0);
    EXECUTE $query$
        WITH current_routes AS (
            SELECT * FROM pgreact_mdm.route_cases($1, $2)
        ), proposed_routes AS (
            SELECT * FROM pgreact_mdm.route_cases($1, $3)
        ), current_deadlines AS (
            SELECT * FROM pgreact_mdm.deadline_preview($1, $2, $4)
        ), proposed_deadlines AS (
            SELECT * FROM pgreact_mdm.deadline_preview($1, $3, $4)
        ), joined AS (
            SELECT c.case_key,
                   jsonb_build_object(
                       'route', jsonb_build_object(
                           'decision', c.decision,
                           'selected_queue', c.selected_queue,
                           'action', c.action,
                           'priority', c.priority,
                           'competitors', c.competitors),
                       'deadline', jsonb_build_object(
                           'decision', cd.decision,
                           'proposed_due_at', cd.proposed_due_at,
                           'reason', cd.reason)) AS current_result,
                   jsonb_build_object(
                       'route', jsonb_build_object(
                           'decision', p.decision,
                           'selected_queue', p.selected_queue,
                           'action', p.action,
                           'priority', p.priority,
                           'competitors', p.competitors),
                       'deadline', jsonb_build_object(
                           'decision', pd.decision,
                           'proposed_due_at', pd.proposed_due_at,
                           'reason', pd.reason)) AS proposed_result
            FROM current_routes AS c
            JOIN proposed_routes AS p USING (case_key)
            JOIN current_deadlines AS cd USING (case_key)
            JOIN proposed_deadlines AS pd USING (case_key)
            WHERE CASE WHEN $5::bigint[] IS NOT NULL
                       THEN c.case_key = ANY($5::bigint[])
                       ELSE c.case_key BETWEEN $6 AND $7 END
        )
        SELECT COALESCE(jsonb_agg(jsonb_build_object(
                   'case_key', case_key,
                   'current', current_result,
                   'proposed', proposed_result,
                   'changed', current_result IS DISTINCT FROM proposed_result)
                   ORDER BY case_key), '[]'::jsonb)
        FROM joined
    $query$
    INTO rows
    USING source_relation, current_policy_revision, proposed_policy_revision,
          captured_at, membership, key_min, key_max;
    SELECT array_agg((item.value ->> 'case_key')::bigint
                     ORDER BY (item.value ->> 'case_key')::bigint)
    INTO evaluated_keys
    FROM jsonb_array_elements(rows) AS item(value);
    IF selected_keys IS DISTINCT FROM evaluated_keys THEN
        RAISE EXCEPTION 'MDM_COMPARISON_INCOMPLETE: evaluated rows differ from selected cases';
    END IF;
    RETURN jsonb_build_object(
        'state', CASE WHEN observed_count = expected_count THEN 'complete' ELSE 'partial' END,
        'read_only', true,
        'captured_at', captured_at,
        'policy_revisions', jsonb_build_object(
            'current', current_policy_revision, 'proposed', proposed_policy_revision),
        'coverage', jsonb_strip_nulls(jsonb_build_object(
            'population_id', population_id,
            'membership', membership,
            'key_min', key_min,
            'key_max', key_max,
            'expected_count', expected_count,
            'observed_count', observed_count,
            'partial', observed_count <> expected_count,
            'snapshot', 'one READ COMMITTED statement per read')),
        'rows', rows);
END
$function$;

COMMENT ON FUNCTION pgreact_mdm.compare_population(regclass, text, text, timestamptz, jsonb) IS
    'v0.47 bounded read-only comparison with explicit population coverage';
