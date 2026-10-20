-- React #11: a nested projection of an authorized due date joined by case key.
BEGIN;
SET pg_trickle.differential_max_change_ratio = 1;
CREATE SCHEMA using_repro;
CREATE TABLE using_repro.cases (
    case_key bigint PRIMARY KEY,
    tenant bigint NOT NULL,
    due_at timestamptz
);
CREATE TABLE using_repro.selected (
    case_key bigint PRIMARY KEY,
    tenant bigint NOT NULL
);
INSERT INTO using_repro.cases VALUES
    (1, 10, '2026-10-20 10:00:00+00'),
    (2, 10, NULL), (3, 20, '2026-10-21 10:00:00+00');
INSERT INTO using_repro.selected VALUES (1, 10), (2, 99), (4, 20);
CREATE VIEW using_repro.projection AS
SELECT case_key, tenant, due_at FROM using_repro.cases;
CREATE VIEW using_repro.candidate AS
SELECT c.case_key, c.due_at AS deadline
FROM using_repro.projection c JOIN using_repro.selected s USING (case_key, tenant);

DO $roles$
BEGIN
    CREATE ROLE view_using_owner NOSUPERUSER NOBYPASSRLS NOLOGIN;
    CREATE ROLE view_using_worker NOSUPERUSER NOBYPASSRLS NOLOGIN;
END
$roles$;
GRANT USAGE, CREATE ON SCHEMA using_repro TO view_using_owner;
GRANT USAGE ON SCHEMA pgtrickle TO view_using_owner, view_using_worker;
GRANT SELECT ON ALL TABLES IN SCHEMA using_repro TO view_using_owner;
GRANT EXECUTE ON FUNCTION pgtrickle.create_stream_table(
    text, text, text, text, boolean, text, text, text, boolean, boolean,
    text, integer, double precision, text, boolean, text, integer, text, text
) TO view_using_owner;
GRANT EXECUTE ON FUNCTION pgtrickle.refresh_stream_table(text)
TO view_using_owner, view_using_worker;

SET ROLE view_using_owner;
SELECT pgtrickle.create_stream_table('using_repro.result',
    'SELECT * FROM using_repro.candidate', '1h', 'DIFFERENTIAL');
GRANT MAINTAIN ON using_repro.result TO view_using_worker;
RESET ROLE;

CREATE FUNCTION using_repro.assert_exact() RETURNS void LANGUAGE plpgsql AS $check$
BEGIN
    IF EXISTS (
        (SELECT case_key, deadline FROM using_repro.result
         EXCEPT ALL SELECT * FROM using_repro.candidate)
        UNION ALL
        (SELECT * FROM using_repro.candidate
         EXCEPT ALL SELECT case_key, deadline FROM using_repro.result)
    ) THEN
        RAISE EXCEPTION 'USING stream differs from its defining query';
    END IF;
    IF (SELECT pg_get_userbyid(relowner) FROM pg_class
        WHERE oid = 'using_repro.result'::regclass) <> 'view_using_owner'
       OR NOT has_table_privilege('view_using_worker', 'using_repro.result', 'MAINTAIN')
       OR has_table_privilege('view_using_worker', 'using_repro.cases', 'SELECT')
       OR has_schema_privilege('view_using_worker', 'using_repro', 'USAGE') THEN
        RAISE EXCEPTION 'USING stream ownership or restricted worker privileges changed';
    END IF;
END
$check$;
SELECT using_repro.assert_exact();

-- Mismatching keys must never become a cross join during refresh.
UPDATE using_repro.cases SET due_at = due_at + interval '1 hour' WHERE case_key = 1;
INSERT INTO using_repro.selected VALUES (3, 20);
SET ROLE view_using_worker;
SELECT pgtrickle.refresh_stream_table('using_repro.result');
RESET ROLE;
SELECT using_repro.assert_exact();
UPDATE using_repro.selected SET tenant = 10 WHERE case_key = 2;
DELETE FROM using_repro.selected WHERE case_key = 1;
SET ROLE view_using_worker;
SELECT pgtrickle.refresh_stream_table('using_repro.result');
RESET ROLE;
SELECT using_repro.assert_exact();
COMMIT;

BEGIN;
UPDATE using_repro.cases SET due_at = '2027-01-01 00:00:00+00' WHERE case_key = 3;
SET LOCAL ROLE view_using_worker;
SELECT pgtrickle.refresh_stream_table('using_repro.result');
RESET ROLE;
SELECT using_repro.assert_exact();
ROLLBACK;
SELECT using_repro.assert_exact();
UPDATE using_repro.cases SET due_at = '2028-01-01 00:00:00+00' WHERE case_key = 3;
SET ROLE view_using_worker;
SELECT pgtrickle.refresh_stream_table('using_repro.result');
RESET ROLE;
SELECT using_repro.assert_exact();
COMMIT;

BEGIN;
SET LOCAL ROLE view_using_owner;
SELECT pgtrickle.create_stream_table('using_repro.rolled_back',
    'SELECT * FROM using_repro.candidate', '1h', 'DIFFERENTIAL');
ROLLBACK;
DO $rollback$
BEGIN
    IF to_regclass('using_repro.rolled_back') IS NOT NULL
       OR EXISTS (SELECT 1 FROM pgtrickle.pgt_stream_tables
                  WHERE pgt_schema = 'using_repro' AND pgt_name = 'rolled_back') THEN
        RAISE EXCEPTION 'rolled-back USING creation left storage or metadata';
    END IF;
    IF (SELECT count(*) FROM pgtrickle.pgt_refresh_history
        WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables
                        WHERE pgt_schema = 'using_repro' AND pgt_name = 'result')
          AND action = 'DIFFERENTIAL' AND status = 'COMPLETED'
          AND NOT was_full_fallback) <> 3 THEN
        RAISE EXCEPTION 'USING refreshes fell back or rollback retained refresh history';
    END IF;
END
$rollback$;
