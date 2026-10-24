-- Requires the pg-react core extension. The read-only adapter remains usable
-- without this bridge for fixture and contract qualification.

CREATE OR REPLACE FUNCTION pgreact_mdm.review_declaration(
    policy_name text,
    policy_revision text,
    member pgreact_api.declaration,
    source_relation regclass,
    subject_keys name[],
    valid_from timestamptz
)
RETURNS pgreact_api.declaration
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    package_digest bytea;
BEGIN
    SELECT policy_digest
    INTO package_digest
    FROM pgreact_mdm.policy_packages
    WHERE pgreact_mdm.policy_packages.policy_revision = $2;
    IF package_digest IS NULL THEN
        RAISE EXCEPTION 'MDM_POLICY_NOT_FOUND: policy revision %', $2;
    END IF;
    RETURN pgreact.policy_set(
        name           => $1,
        version        => encode(package_digest, 'hex'),
        members        => ARRAY[$3]::pgreact_api.declaration[],
        applicability  => $4,
        subject_keys   => $5,
        support        => ARRAY[
            pgreact.shared_condition(
                'mdm-policy-inputs', $4, $5, 'FULL')
        ]::pgreact_api.declaration[],
        dependencies   => '[]'::jsonb,
        valid_from     => $6,
        evidence_limit => 100);
END
$function$;

COMMENT ON FUNCTION pgreact_mdm.review_declaration(text, text, pgreact_api.declaration, regclass, name[], timestamptz) IS
    'Builds the v0.47 adapter declaration for pg-react validate, preview, review, and compare';
