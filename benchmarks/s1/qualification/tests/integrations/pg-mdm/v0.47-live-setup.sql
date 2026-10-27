\set ON_ERROR_STOP on

\connect foundation postgres
DROP TABLE IF EXISTS public.review_admission_source CASCADE;
CREATE TABLE public.review_admission_source (
    id bigint PRIMARY KEY,
    display_name text NOT NULL,
    updated_at timestamptz NOT NULL
);
GRANT USAGE ON SCHEMA public TO mdm_administrator;
GRANT SELECT, MAINTAIN ON public.review_admission_source TO mdm_administrator;

\connect foundation mdm_test_login
SET ROLE mdm_administrator;
WITH proposed AS (
    SELECT mdm.entity(
        name => 'review_admission_live',
        sources => ARRAY[
            mdm.source(
                name => 'admission_source',
                relation => 'public.review_admission_source'::regclass,
                source_id => ARRAY['id'],
                mode => 'tracked',
                fields => jsonb_build_object('name', 'display_name'),
                row_changed_at => 'updated_at')
        ],
        fields => ARRAY[
            mdm.field(name => 'name', type => 'text', cleaner => 'company_name')
        ],
        matches => ARRAY[
            mdm.match(
                name => 'same_name',
                fields => ARRAY['name'],
                comparison => 'exact',
                strength => 'strong',
                evidence_group => 'name',
                candidate => jsonb_build_object('kind', 'token', 'field', 'name', 'min_length', 5))
        ],
        golden_values => ARRAY[
            mdm.golden_value(field => 'name', policy => 'prefer_source',
                sources => ARRAY['admission_source'])
        ]) AS definition
)
SELECT desired_version = 1 AND changed AS created
FROM mdm.create((SELECT definition FROM proposed), NULL, 'live admission review fixture')
\gset
\if :created
\else
\quit 1
\endif
RESET ROLE;

\connect foundation postgres
INSERT INTO public.review_admission_source VALUES
    (1, 'Admission Pair', statement_timestamp()),
    (2, 'Admission Pair', statement_timestamp()),
    (3, 'Admission Pair', statement_timestamp());

\connect foundation mdm_test_login
SET ROLE mdm_administrator;
SELECT mdm.refresh('review_admission_live', 'ALLOW');
RESET ROLE;
