\set ON_ERROR_STOP on
\ir ../../../integrations/pg-mdm/sql/typed-flow.sql

DO $$
DECLARE
    member pgreact_api.declaration;
    first_declaration pgreact_api.declaration;
    second_declaration pgreact_api.declaration;
    first_digest text;
    second_digest text;
    preview jsonb;
    review text;
BEGIN
    member := pgreact_api.declaration('rule', 'mdm-fixture-rule',
        jsonb_build_object('condition', 'mdm_fixture.policy_cases_v1',
            'semantic_key', 'case_key', 'kind', 'CONSTRAINT', 'salience', 10));
    SELECT encode(policy_digest, 'hex') INTO first_digest
    FROM pgreact_mdm.policy_packages WHERE policy_revision = 'policy-1';
    SELECT encode(policy_digest, 'hex') INTO second_digest
    FROM pgreact_mdm.policy_packages WHERE policy_revision = 'policy-2';
    first_declaration := pgreact_mdm.review_declaration('mdm-fixture', 'policy-1', member,
        'mdm_fixture.policy_cases_v1'::regclass, ARRAY['case_key'::name],
        '2026-09-21 12:00:00+00');
    second_declaration := pgreact_mdm.review_declaration('mdm-fixture', 'policy-2', member,
        'mdm_fixture.policy_cases_v1'::regclass, ARRAY['case_key'::name],
        '2026-09-21 12:00:00+00');
    IF (first_declaration).spec ->> 'version' IS DISTINCT FROM first_digest
       OR (second_declaration).spec ->> 'version' IS DISTINCT FROM second_digest
       OR first_digest = second_digest THEN
        RAISE EXCEPTION 'v0.47 typed package identity mismatch';
    END IF;
    preview := pgreact.preview(first_declaration);
    review := pgreact.review_token(preview);
    BEGIN
        PERFORM pgreact.deploy(second_declaration, review);
        RAISE EXCEPTION 'v0.47 accepted a review for a changed package identity';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM <> 'M54_REVIEW_TOKEN_MISMATCH: token declaration digest does not match' THEN
            RAISE;
        END IF;
    END;
END
$$;

SELECT 'v0.47 typed package identity passed' AS result;
