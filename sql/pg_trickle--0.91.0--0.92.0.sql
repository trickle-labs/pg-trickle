-- pg_trickle 0.91.0 -> 0.92.0
--
-- v0.92.0 persists capture ownership, fails closed after clone/restore
-- identity changes, and adds an explicit upgrade drain boundary.

CREATE TABLE IF NOT EXISTS pgtrickle.pgt_capture_instance (
    singleton                   BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    instance_id                 TEXT NOT NULL,
    database_oid                OID NOT NULL,
    system_identifier           TEXT NOT NULL,
    state                       TEXT NOT NULL DEFAULT 'ACTIVE'
                                CHECK (state IN ('ACTIVE', 'QUIESCED', 'QUARANTINED')),
    observed_database_oid       OID,
    observed_system_identifier  TEXT,
    quarantine_reason           TEXT,
    initialized_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);

REVOKE ALL ON TABLE pgtrickle.pgt_capture_instance FROM PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('pgtrickle.pgt_capture_instance', '');

-- The old PL/pgSQL compatibility helpers returned void. Replace them
-- explicitly because PostgreSQL does not allow changing a function return
-- type with CREATE OR REPLACE FUNCTION.
DROP FUNCTION IF EXISTS pgtrickle.pause_all();
DROP FUNCTION IF EXISTS pgtrickle.resume_all();

CREATE OR REPLACE FUNCTION pgtrickle."capture_instance_status"()
RETURNS TEXT
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'capture_instance_status_wrapper';

CREATE OR REPLACE FUNCTION pgtrickle."validate_recovery"()
RETURNS TEXT
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'validate_recovery_wrapper';

CREATE OR REPLACE FUNCTION pgtrickle."quiesce"(
    "timeout_s" INT DEFAULT 60
)
RETURNS bool
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'quiesce_wrapper';

CREATE OR REPLACE FUNCTION pgtrickle."resume_all"()
RETURNS bool
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'resume_all_wrapper';

CREATE OR REPLACE FUNCTION pgtrickle."pause_all"()
RETURNS bool
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'pause_all_wrapper';

CREATE OR REPLACE FUNCTION pgtrickle."recover_capture_instance"()
RETURNS TEXT
STRICT
SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c
AS 'MODULE_PATHNAME', 'recover_capture_instance_wrapper';

CREATE OR REPLACE FUNCTION pgtrickle."preflight_upgrade"()
RETURNS TEXT
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'preflight_upgrade_wrapper';

REVOKE EXECUTE ON FUNCTION pgtrickle.capture_instance_status() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.quiesce(integer) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.pause_all() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.recover_capture_instance() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.resume_all() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.capture_instance_status() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.preflight_upgrade() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.validate_recovery() TO PUBLIC;

INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES (
    '0.92.0',
    'Capture ownership isolation, fail-closed CDC recovery, and upgrade quiescence'
)
ON CONFLICT (version) DO NOTHING;
