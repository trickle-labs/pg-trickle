DO $role$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'pgreact_mdm_worker') THEN
        EXECUTE 'CREATE ROLE pgreact_mdm_worker NOLOGIN NOSUPERUSER NOBYPASSRLS NOINHERIT NOCREATEDB NOCREATEROLE NOREPLICATION';
    END IF;
END
$role$;

ALTER ROLE pgreact_mdm_worker
    NOLOGIN NOSUPERUSER NOBYPASSRLS NOINHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
GRANT USAGE ON SCHEMA pgreact, pgreact_mdm, mdm_steward
    TO mdm_helper_owner WITH GRANT OPTION;
GRANT CREATE ON SCHEMA pgreact_mdm TO mdm_helper_owner;

CREATE TABLE IF NOT EXISTS pgreact_mdm.policy_intent_bindings (
    binding_id uuid PRIMARY KEY,
    entity_name name NOT NULL,
    entity_execution_role name NOT NULL,
    policy_revision text NOT NULL
        REFERENCES pgreact_mdm.policy_intent_packages(policy_revision),
    policy_digest bytea NOT NULL CHECK (octet_length(policy_digest) = 32),
    allowed_actions text[] NOT NULL,
    allowed_queues text[] NOT NULL,
    max_due_interval interval,
    max_escalation_level integer NOT NULL CHECK (max_escalation_level >= 0),
    binding_version bigint NOT NULL CHECK (binding_version > 0),
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (allowed_actions <@ ARRAY['ASSIGN_QUEUE', 'SET_DUE_AT', 'ESCALATE']::text[])
);
ALTER TABLE pgreact_mdm.policy_intent_bindings OWNER TO mdm_helper_owner;

CREATE UNIQUE INDEX IF NOT EXISTS policy_intent_one_enabled_binding_per_entity
    ON pgreact_mdm.policy_intent_bindings(entity_name) WHERE enabled;

CREATE UNLOGGED TABLE IF NOT EXISTS pgreact_mdm.policy_intent_runtime (
    binding_id uuid PRIMARY KEY,
    database_oid oid NOT NULL,
    worker_role_oid oid NOT NULL,
    runtime_version bigint NOT NULL CHECK (runtime_version > 0),
    activated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
ALTER TABLE pgreact_mdm.policy_intent_runtime OWNER TO mdm_helper_owner;

CREATE TABLE IF NOT EXISTS pgreact_mdm.intent_case_identities (
    case_identity bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entity_name name NOT NULL,
    case_key bigint NOT NULL,
    UNIQUE (entity_name, case_key)
);
ALTER TABLE pgreact_mdm.intent_case_identities OWNER TO mdm_helper_owner;

CREATE TABLE IF NOT EXISTS pgreact_mdm.intent_holds (
    binding_id uuid NOT NULL,
    case_key bigint NOT NULL,
    policy_revision text NOT NULL,
    action text NOT NULL,
    expected_action_revision bigint NOT NULL CHECK (expected_action_revision > 0),
    reason_code text NOT NULL,
    held_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (binding_id, case_key, policy_revision, action, expected_action_revision)
);
ALTER TABLE pgreact_mdm.intent_holds OWNER TO mdm_helper_owner;

DO $intent_holds_upgrade$
DECLARE
    has_action_revision boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_attribute
        WHERE attrelid = 'pgreact_mdm.intent_holds'::regclass
          AND attname = 'expected_action_revision'
          AND NOT attisdropped)
    INTO has_action_revision;
    IF NOT has_action_revision THEN
        ALTER TABLE pgreact_mdm.intent_holds
            ADD COLUMN expected_action_revision bigint;
        UPDATE pgreact_mdm.intent_holds AS hold
        SET expected_action_revision = policy_case.action_revision
        FROM mdm_steward.policy_cases_v1 AS policy_case
        WHERE policy_case.case_key = hold.case_key
          AND hold.expected_action_revision IS NULL;
        IF EXISTS (
            SELECT 1
            FROM pgreact_mdm.intent_holds
            WHERE expected_action_revision IS NULL) THEN
            RAISE EXCEPTION
                'MDM_INTENT_HOLD_MIGRATION: cannot derive action revision for retained hold';
        END IF;
        ALTER TABLE pgreact_mdm.intent_holds
            ALTER COLUMN expected_action_revision SET NOT NULL;
        ALTER TABLE pgreact_mdm.intent_holds
            ADD CONSTRAINT intent_holds_expected_action_revision_check
            CHECK (expected_action_revision > 0);
        ALTER TABLE pgreact_mdm.intent_holds
            DROP CONSTRAINT IF EXISTS intent_holds_pkey;
        ALTER TABLE pgreact_mdm.intent_holds
            ADD CONSTRAINT intent_holds_pkey PRIMARY KEY (
                binding_id, case_key, policy_revision, action,
                expected_action_revision);
    END IF;
END
$intent_holds_upgrade$;
REVOKE CREATE ON SCHEMA pgreact_mdm FROM mdm_helper_owner;

CREATE OR REPLACE VIEW pgreact_mdm.delivery_inspection_v1 AS
SELECT request.work_ref,
       binding.binding_id,
       binding.binding_version,
       binding.entity_name::text AS entity_name,
       request.policy_revision,
       encode(binding.policy_digest, 'hex') AS policy_digest,
       request.case_key,
       request.lifecycle_generation,
       request.action_revision,
       request.request_body ->> 'action' AS action,
       request.created_at AS requested_at,
       COALESCE(attempt.outcome, 'PENDING') AS delivery_outcome,
       attempt.reason_code AS delivery_reason_code,
       attempt.attempted_at,
       receipt.receipt_id AS mdm_receipt_id,
       receipt.outcome AS mdm_outcome,
       receipt.reason_code AS mdm_reason_code,
       receipt.resulting_publication_revision,
       case_row.status AS current_case_status,
       case_row.publication_revision AS current_publication_revision,
       CASE WHEN hold.reason_code IS NULL THEN 'NONE' ELSE 'HELD' END AS remediation_state,
       hold.reason_code AS remediation_reason_code,
       hold.held_at AS remediation_at
FROM pgreact_mdm.intent_requests AS request
JOIN pgreact_mdm.policy_intent_bindings AS binding USING (binding_id)
LEFT JOIN LATERAL (
    SELECT saved_attempt.*
    FROM pgreact_mdm.intent_attempts AS saved_attempt
    WHERE saved_attempt.binding_id = request.binding_id
      AND saved_attempt.request_key = request.request_key
    ORDER BY saved_attempt.attempted_at DESC,
             saved_attempt.episode_id DESC,
             saved_attempt.attempt_no DESC
    LIMIT 1
) AS attempt ON true
LEFT JOIN mdm_steward.policy_receipts_v1 AS receipt
  ON receipt.receipt_id = attempt.receipt_id
LEFT JOIN mdm_steward.policy_cases_v1 AS case_row
  ON case_row.entity_name = binding.entity_name
 AND case_row.case_key = request.case_key
LEFT JOIN LATERAL (
    SELECT saved_hold.reason_code, saved_hold.held_at
    FROM pgreact_mdm.intent_holds AS saved_hold
    WHERE saved_hold.binding_id = request.binding_id
      AND saved_hold.case_key = request.case_key
      AND saved_hold.policy_revision = request.policy_revision
      AND saved_hold.action = request.request_body ->> 'action'
      AND saved_hold.expected_action_revision = request.action_revision
    ORDER BY saved_hold.held_at DESC
    LIMIT 1
) AS hold ON true;
COMMENT ON VIEW pgreact_mdm.delivery_inspection_v1 IS
    'Versioned read-only delivery, MDM receipt, policy, case, and remediation inspection.';
REVOKE ALL ON pgreact_mdm.delivery_inspection_v1 FROM PUBLIC;
GRANT SELECT ON pgreact_mdm.delivery_inspection_v1 TO PUBLIC;

CREATE OR REPLACE FUNCTION pgreact_mdm.intent_receipt_exists(
    request_binding_id uuid,
    request_key bytea,
    expected_receipt_id uuid
)
RETURNS boolean
LANGUAGE SQL STABLE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
    SELECT EXISTS (
        SELECT 1
        FROM mdm_steward.policy_receipts_v1 AS receipt
        WHERE receipt.binding_id = $1
          AND receipt.request_key = $2
          AND receipt.receipt_id = $3)
$function$;
ALTER FUNCTION pgreact_mdm.intent_receipt_exists(uuid, bytea, uuid)
    OWNER TO mdm_helper_owner;
REVOKE ALL ON FUNCTION pgreact_mdm.intent_receipt_exists(uuid, bytea, uuid)
    FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgreact_mdm.intent_receipt_exists(uuid, bytea, uuid)
    TO pgreact_mdm_worker;

CREATE OR REPLACE FUNCTION pgreact_mdm.intent_binding_config(binding_id uuid)
RETURNS SETOF pgreact_mdm.policy_intent_bindings
LANGUAGE SQL
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
    SELECT * FROM pgreact_mdm.policy_intent_bindings
    WHERE pgreact_mdm.policy_intent_bindings.binding_id = $1
$function$;
GRANT EXECUTE ON FUNCTION pgreact_mdm.intent_binding_config(uuid) TO PUBLIC;

CREATE OR REPLACE FUNCTION pgreact_mdm.verify_policy_worker(entity_execution_role name)
RETURNS void
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    worker_oid oid;
    entity_oid oid;
    helper_oid oid;
BEGIN
    SELECT oid INTO STRICT worker_oid
    FROM pg_catalog.pg_roles
    WHERE rolname = 'pgreact_mdm_worker'
      AND NOT rolcanlogin AND NOT rolsuper AND NOT rolbypassrls
      AND NOT rolinherit AND NOT rolcreatedb AND NOT rolcreaterole
      AND NOT rolreplication;
    SELECT oid INTO STRICT entity_oid
    FROM pg_catalog.pg_roles
    WHERE rolname = entity_execution_role;
    SELECT oid INTO helper_oid
    FROM pg_catalog.pg_roles
    WHERE rolname = 'mdm_helper_owner';
    IF helper_oid IS NULL OR pg_catalog.pg_has_role(worker_oid, helper_oid, 'MEMBER')
       OR pg_catalog.pg_has_role(worker_oid, entity_oid, 'MEMBER') THEN
        RAISE EXCEPTION 'MDM_WORKER_ROLE_MEMBERSHIP: worker reaches an MDM helper or entity execution role';
    END IF;
END
$function$;
GRANT EXECUTE ON FUNCTION pgreact_mdm.verify_policy_worker(name) TO PUBLIC;

CREATE OR REPLACE FUNCTION pgreact_mdm.store_intent_binding(
    binding_id uuid,
    entity_name name,
    entity_execution_role name,
    policy_revision text,
    policy_digest bytea,
    allowed_actions text[],
    allowed_queues text[],
    max_due_interval interval,
    max_escalation_level integer,
    binding_version bigint,
    runtime_version bigint,
    replaced_binding_id uuid,
    actor_role name
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
BEGIN
    IF NOT pg_catalog.pg_has_role(session_user, actor_role, 'MEMBER') THEN
        RAISE EXCEPTION 'MDM_BINDING_ACTOR: caller cannot act as %', actor_role;
    END IF;
    IF entity_execution_role <> actor_role THEN
        RAISE EXCEPTION 'MDM_BINDING_ACTOR: caller does not own the binding';
    END IF;
    IF replaced_binding_id IS NOT NULL THEN
        UPDATE pgreact_mdm.policy_intent_bindings
        SET enabled = false
        WHERE pgreact_mdm.policy_intent_bindings.binding_id = replaced_binding_id;
        DELETE FROM pgreact_mdm.policy_intent_runtime
        WHERE pgreact_mdm.policy_intent_runtime.binding_id = replaced_binding_id;
    END IF;
    INSERT INTO pgreact_mdm.policy_intent_bindings(
        binding_id, entity_name, entity_execution_role, policy_revision, policy_digest,
        allowed_actions, allowed_queues, max_due_interval,
        max_escalation_level, binding_version)
    VALUES (
        binding_id, entity_name, entity_execution_role, policy_revision, policy_digest,
        allowed_actions, allowed_queues, max_due_interval,
        max_escalation_level, binding_version);
    PERFORM pgreact_mdm.store_intent_runtime(
        binding_id, runtime_version, true, actor_role);
END
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.store_intent_runtime(
    binding_id uuid,
    runtime_version bigint,
    active boolean,
    actor_role name
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    binding_role name;
BEGIN
    IF NOT pg_catalog.pg_has_role(session_user, actor_role, 'MEMBER') THEN
        RAISE EXCEPTION 'MDM_BINDING_ACTOR: caller cannot act as %', actor_role;
    END IF;
    SELECT entity_execution_role INTO STRICT binding_role
    FROM pgreact_mdm.policy_intent_bindings
    WHERE pgreact_mdm.policy_intent_bindings.binding_id = store_intent_runtime.binding_id;
    IF binding_role <> actor_role THEN
        RAISE EXCEPTION 'MDM_BINDING_ACTOR: caller does not own binding %', binding_id;
    END IF;
    IF active THEN
        INSERT INTO pgreact_mdm.policy_intent_runtime(
            binding_id, database_oid, worker_role_oid, runtime_version)
        SELECT binding_id, database.oid, worker.oid, runtime_version
        FROM pg_catalog.pg_database AS database
        CROSS JOIN pg_catalog.pg_roles AS worker
        WHERE database.datname = current_database()
          AND worker.rolname = 'pgreact_mdm_worker'
        ON CONFLICT ON CONSTRAINT policy_intent_runtime_pkey DO UPDATE
        SET database_oid = EXCLUDED.database_oid,
            worker_role_oid = EXCLUDED.worker_role_oid,
            runtime_version = EXCLUDED.runtime_version,
            activated_at = clock_timestamp();
        UPDATE pgreact_mdm.policy_intent_bindings
        SET enabled = true
        WHERE pgreact_mdm.policy_intent_bindings.binding_id = store_intent_runtime.binding_id;
    ELSE
        DELETE FROM pgreact_mdm.policy_intent_runtime
        WHERE pgreact_mdm.policy_intent_runtime.binding_id = store_intent_runtime.binding_id;
    END IF;
END
$function$;

REVOKE ALL ON FUNCTION pgreact_mdm.store_intent_binding(uuid, name, name, text, bytea, text[], text[], interval, integer, bigint, bigint, uuid, name),
    pgreact_mdm.store_intent_runtime(uuid, bigint, boolean, name) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgreact_mdm.store_intent_binding(uuid, name, name, text, bytea, text[], text[], interval, integer, bigint, bigint, uuid, name),
    pgreact_mdm.store_intent_runtime(uuid, bigint, boolean, name) TO PUBLIC;

CREATE OR REPLACE FUNCTION pgreact_mdm.configure_policy_worker_privileges()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
BEGIN
    EXECUTE 'GRANT USAGE ON SCHEMA pgreact, pgreact_mdm, mdm_steward TO pgreact_mdm_worker';
    EXECUTE 'GRANT SELECT ON TABLE pgreact_mdm.policy_packages TO pgreact_mdm_worker';
    EXECUTE 'GRANT SELECT ON TABLE pgreact_mdm.authorized_policy_cases_v1 TO pgreact_mdm_worker';
    EXECUTE 'GRANT SELECT ON TABLE pgreact_mdm.intent_queue_candidates, pgreact_mdm.intent_due_candidates, pgreact_mdm.intent_escalation_candidates TO pgreact_mdm_worker';
    EXECUTE 'GRANT SELECT ON TABLE pgreact_mdm.policy_intent_bindings, pgreact_mdm.policy_intent_runtime TO pgreact_mdm_worker';
    EXECUTE 'GRANT SELECT, INSERT ON TABLE pgreact_mdm.intent_requests, pgreact_mdm.intent_attempts, pgreact_mdm.intent_holds TO pgreact_mdm_worker';
    EXECUTE 'GRANT EXECUTE ON FUNCTION pgreact_mdm.submit_queue_intent(pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1), pgreact_mdm.submit_due_intent(pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1), pgreact_mdm.submit_escalation_intent(pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1), pgreact_mdm.change_queue_intent(pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1, pgreact_mdm.intent_deployer_policy_cases_v1), pgreact_mdm.change_due_intent(pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1, pgreact_mdm.intent_deployer_policy_cases_v1), pgreact_mdm.change_escalation_intent(pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1, pgreact_mdm.intent_deployer_policy_cases_v1), pgreact_mdm.execute_intent_episode(uuid, text) TO pgreact_mdm_worker';
    EXECUTE 'REVOKE ALL ON TABLE mdm_steward.policy_cases_v1, mdm_steward.policy_bindings_v1, mdm_steward.policy_receipts_v1, mdm_internal.policy_binding_runtime FROM pgreact_mdm_worker';
    EXECUTE 'GRANT EXECUTE ON FUNCTION mdm_steward.submit_policy_intent(uuid, bytea, bigint, text, jsonb, bigint, bigint, bigint, bigint, bytea, bigint, bytea, text, text, text) TO pgreact_mdm_worker';
    EXECUTE 'REVOKE ALL ON TABLE mdm_steward.policy_receipts_v1 FROM pgreact_mdm_worker';
END
$function$;
GRANT CREATE ON SCHEMA pgreact_mdm TO mdm_helper_owner;
ALTER FUNCTION pgreact_mdm.configure_policy_worker_privileges() OWNER TO mdm_helper_owner;
REVOKE CREATE ON SCHEMA pgreact_mdm FROM mdm_helper_owner;
REVOKE ALL ON FUNCTION pgreact_mdm.configure_policy_worker_privileges() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgreact_mdm.configure_policy_worker_privileges() TO PUBLIC;

CREATE OR REPLACE FUNCTION pgreact_mdm.create_intent_binding(
    entity_name text,
    policy_revision text,
    allowed_actions text[],
    allowed_queues text[],
    max_due_interval interval,
    max_escalation_level integer
)
RETURNS TABLE(binding_id uuid, binding_version bigint)
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    digest bytea;
    runtime_version bigint;
BEGIN
    PERFORM pgreact_mdm.verify_policy_worker(current_user::name);
    SELECT decode(
        pgreact_mdm.intent_policy_document($2) ->> 'policy_digest', 'hex')
    INTO STRICT digest;
    SELECT created.binding_id, created.binding_version
    INTO STRICT binding_id, binding_version
    FROM mdm_admin.create_policy_binding(
        $1, 'pgreact_mdm_worker', digest, $3, $4, $5, $6) AS created;
    runtime_version := mdm_admin.set_policy_binding_state(
        binding_id, 1, 'active', 'pg-react M2 activation');
    PERFORM pgreact_mdm.configure_policy_worker_privileges();
    PERFORM pgreact_mdm.store_intent_binding(
        binding_id, $1::name, current_user::name, $2, digest, $3, $4, $5, $6,
        binding_version, runtime_version, NULL, current_user::name);
    RETURN NEXT;
END
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.replace_intent_binding(
    old_binding_id uuid,
    expected_binding_version bigint,
    policy_revision text,
    allowed_actions text[],
    allowed_queues text[],
    max_due_interval interval,
    max_escalation_level integer
)
RETURNS TABLE(binding_id uuid, binding_version bigint)
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    old_entity name;
    old_execution_role name;
    digest bytea;
    runtime_version bigint;
BEGIN
    PERFORM pgreact_mdm.verify_policy_worker(current_user::name);
    SELECT binding.entity_name, binding.entity_execution_role
    INTO STRICT old_entity, old_execution_role
    FROM pgreact_mdm.intent_binding_config($1) AS binding
    WHERE binding.enabled;
    IF old_execution_role <> current_user::name THEN
        RAISE EXCEPTION 'MDM_BINDING_ACTOR: caller does not own binding %', $1;
    END IF;
    SELECT decode(
        pgreact_mdm.intent_policy_document($3) ->> 'policy_digest', 'hex')
    INTO STRICT digest;
    SELECT replaced.binding_id, replaced.binding_version
    INTO STRICT binding_id, binding_version
    FROM mdm_admin.replace_policy_binding(
        $1, $2, digest, $4, $5, $6, $7) AS replaced;
    runtime_version := mdm_admin.set_policy_binding_state(
        binding_id, 1, 'active', 'pg-react M2 replacement activation');
    PERFORM pgreact_mdm.configure_policy_worker_privileges();
    PERFORM pgreact_mdm.store_intent_binding(
        binding_id, old_entity, old_execution_role, $3, digest, $4, $5, $6, $7,
        binding_version, runtime_version, $1, current_user::name);
    RETURN NEXT;
END
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.reconcile_intent_binding(
    binding_id uuid,
    expected_runtime_version bigint
)
RETURNS bigint
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    binding pgreact_mdm.policy_intent_bindings%ROWTYPE;
    runtime_version bigint;
BEGIN
    SELECT * INTO STRICT binding
    FROM pgreact_mdm.intent_binding_config($1) AS config;
    PERFORM pgreact_mdm.verify_policy_worker(current_user::name);
    IF binding.entity_execution_role <> current_user::name THEN
        RAISE EXCEPTION 'MDM_BINDING_ACTOR: caller does not own binding %', $1;
    END IF;
    runtime_version := mdm_admin.set_policy_binding_state(
        $1, $2, 'active', 'pg-react M2 runtime reconciliation');
    PERFORM pgreact_mdm.configure_policy_worker_privileges();
    PERFORM pgreact_mdm.store_intent_runtime(
        $1, runtime_version, true, current_user::name);
    RETURN runtime_version;
END
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.pause_intent_binding(
    binding_id uuid,
    expected_runtime_version bigint
)
RETURNS bigint
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    binding pgreact_mdm.policy_intent_bindings%ROWTYPE;
    runtime_version bigint;
BEGIN
    SELECT * INTO STRICT binding
    FROM pgreact_mdm.intent_binding_config($1) AS config;
    PERFORM pgreact_mdm.verify_policy_worker(current_user::name);
    IF binding.entity_execution_role <> current_user::name THEN
        RAISE EXCEPTION 'MDM_BINDING_ACTOR: caller does not own binding %', $1;
    END IF;
    runtime_version := mdm_admin.set_policy_binding_state(
        $1, $2, 'paused', 'pg-react M2 operator pause');
    PERFORM pgreact_mdm.store_intent_runtime(
        $1, runtime_version, false, current_user::name);
    RETURN runtime_version;
END
$function$;

CREATE OR REPLACE VIEW pgreact_mdm.authorized_policy_cases_v1 AS
SELECT policy_case.*
FROM mdm_steward.policy_cases_v1 AS policy_case
JOIN mdm_internal.entities AS entity
  ON entity.entity_name = policy_case.entity_name
JOIN mdm_steward.policy_bindings_v1 AS binding
  ON binding.entity_id = entity.entity_id
 AND binding.automation_role_name = 'pgreact_mdm_worker'
 AND binding.replaced_by IS NULL
JOIN mdm_internal.policy_binding_runtime AS runtime USING (binding_id)
WHERE runtime.database_oid = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())
  AND runtime.automation_role_oid = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = 'pgreact_mdm_worker')
  AND runtime.state = 'active';
REVOKE ALL ON pgreact_mdm.authorized_policy_cases_v1 FROM PUBLIC;
GRANT SELECT ON pgreact_mdm.authorized_policy_cases_v1 TO mdm_helper_owner WITH GRANT OPTION;

CREATE OR REPLACE FUNCTION pgreact_mdm.sync_intent_case_identities()
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE inserted bigint;
BEGIN
    INSERT INTO pgreact_mdm.intent_case_identities(entity_name, case_key)
    SELECT binding.entity_name, policy_case.case_key
    FROM pgreact_mdm.policy_intent_bindings AS binding
    JOIN pgreact_mdm.authorized_policy_cases_v1 AS policy_case
      ON policy_case.entity_name = binding.entity_name
    WHERE binding.enabled
    ON CONFLICT (entity_name, case_key) DO NOTHING;
    GET DIAGNOSTICS inserted = ROW_COUNT;
    RETURN inserted;
END
$function$;
ALTER FUNCTION pgreact_mdm.sync_intent_case_identities() OWNER TO mdm_helper_owner;
REVOKE ALL ON FUNCTION pgreact_mdm.sync_intent_case_identities() FROM PUBLIC;

-- last_observed_at changes on observation-only publication and is not action state.
CREATE OR REPLACE VIEW pgreact_mdm.intent_deployer_policy_cases_v1 AS
SELECT policy_case.case_key, policy_case.entity_name::text AS entity_name,
       case_identity_map.case_identity,
       policy_case.status,
       policy_case.severity, policy_case.reason_code,
       policy_case.approved_metadata::text AS approved_metadata,
       policy_case.assigned_queue::text AS assigned_queue,
       policy_case.due_at, policy_case.escalation_level,
       policy_case.manual_assignment_protected, policy_case.opened_at,
       policy_case.opened_at_source, policy_case.resolved_at,
       policy_case.review_version, policy_case.definition_version,
       policy_case.publication_revision, policy_case.stewardship_epoch,
       policy_case.evidence_basis_digest, policy_case.action_revision,
       policy_case.pending_stewardship, policy_case.last_observed_at
FROM mdm_steward.policy_cases_v1 AS policy_case
JOIN pgreact_mdm.policy_intent_bindings AS binding
  ON binding.entity_name = policy_case.entity_name
 AND binding.enabled
JOIN pgreact_mdm.policy_intent_runtime AS runtime USING (binding_id)
JOIN pgreact_mdm.intent_case_identities AS case_identity_map
  ON case_identity_map.entity_name = policy_case.entity_name
 AND case_identity_map.case_key = policy_case.case_key;
REVOKE ALL ON pgreact_mdm.intent_deployer_policy_cases_v1 FROM PUBLIC;

CREATE OR REPLACE VIEW pgreact_mdm.intent_queue_candidates AS
SELECT route.case_key, binding.binding_id, binding.policy_revision,
       encode(binding.policy_digest, 'hex') AS policy_digest, 'ASSIGN_QUEUE'::text AS action,
       jsonb_build_object('queue', route.selected_queue::text) AS arguments,
       'assign_' || route.selected_queue::text AS consequence_identity,
       0::integer AS escalation_level
FROM pgreact_mdm.policy_intent_bindings AS binding
JOIN pgreact_mdm.policy_intent_runtime AS runtime USING (binding_id)
JOIN pgreact_mdm.policy_intent_packages AS package
  ON package.policy_revision = binding.policy_revision
CROSS JOIN LATERAL pgreact_mdm.route_cases(
    'pgreact_mdm.authorized_policy_cases_v1'::regclass,
    binding.policy_revision, binding.entity_name::text) AS route
WHERE binding.enabled
  AND runtime.database_oid = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())
  AND runtime.worker_role_oid = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = 'pgreact_mdm_worker')
  AND 'ASSIGN_QUEUE' = ANY(binding.allowed_actions)
  AND EXISTS (
      SELECT 1 FROM pgreact_mdm.authorized_policy_cases_v1 AS policy_case
      WHERE policy_case.case_key = route.case_key
        AND policy_case.entity_name = binding.entity_name)
  AND route.decision = 'WINNER'
  AND route.action = 'ASSIGN_QUEUE'
  AND route.current_queue IS DISTINCT FROM route.selected_queue
  AND route.selected_queue::text = ANY(binding.allowed_queues);

CREATE OR REPLACE VIEW pgreact_mdm.intent_due_candidates AS
SELECT due.case_key, binding.binding_id, binding.policy_revision,
       encode(binding.policy_digest, 'hex') AS policy_digest, 'SET_DUE_AT'::text AS action,
       jsonb_build_object(
           'due_at', pg_catalog.to_char(
               due.proposed_due_at AT TIME ZONE 'UTC',
               'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')) AS arguments,
       'set_due_at'::text AS consequence_identity,
       0::integer AS escalation_level
FROM pgreact_mdm.policy_intent_bindings AS binding
JOIN pgreact_mdm.policy_intent_runtime AS runtime USING (binding_id)
JOIN pgreact_mdm.policy_intent_packages AS package
  ON package.policy_revision = binding.policy_revision
CROSS JOIN LATERAL pgreact_mdm.deadline_preview(
    'pgreact_mdm.authorized_policy_cases_v1'::regclass,
    binding.policy_revision, transaction_timestamp(),
    binding.entity_name::text) AS due
WHERE binding.enabled
  AND runtime.database_oid = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())
  AND runtime.worker_role_oid = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = 'pgreact_mdm_worker')
  AND 'SET_DUE_AT' = ANY(binding.allowed_actions)
  AND pg_catalog.jsonb_typeof(package.policy_package -> 'deadline') = 'object'
  AND due.decision IN ('PROPOSED', 'REPLACEMENT')
  AND due.proposed_due_at IS DISTINCT FROM due.existing_due_at
  AND (binding.max_due_interval IS NULL OR due.proposed_due_at <= (
      SELECT opened_at + binding.max_due_interval
      FROM pgreact_mdm.authorized_policy_cases_v1 AS policy_case
      WHERE policy_case.case_key = due.case_key
        AND policy_case.entity_name = binding.entity_name));

CREATE OR REPLACE VIEW pgreact_mdm.intent_escalation_candidates AS
SELECT route.case_key, binding.binding_id, binding.policy_revision,
       encode(binding.policy_digest, 'hex') AS policy_digest, 'ESCALATE'::text AS action,
       jsonb_build_object('level', (route.explanation ->> 'escalation_level')::integer) AS arguments,
       'escalate_level_' || (route.explanation ->> 'escalation_level') AS consequence_identity,
       (route.explanation ->> 'escalation_level')::integer AS escalation_level
FROM pgreact_mdm.policy_intent_bindings AS binding
JOIN pgreact_mdm.policy_intent_runtime AS runtime USING (binding_id)
JOIN pgreact_mdm.policy_intent_packages AS package
  ON package.policy_revision = binding.policy_revision
CROSS JOIN LATERAL pgreact_mdm.route_cases(
    'pgreact_mdm.authorized_policy_cases_v1'::regclass,
    binding.policy_revision, binding.entity_name::text) AS route
WHERE binding.enabled
  AND runtime.database_oid = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())
  AND runtime.worker_role_oid = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = 'pgreact_mdm_worker')
  AND 'ESCALATE' = ANY(binding.allowed_actions)
  AND EXISTS (
      SELECT 1 FROM pgreact_mdm.authorized_policy_cases_v1 AS policy_case
      WHERE policy_case.case_key = route.case_key
        AND policy_case.entity_name = binding.entity_name)
  AND route.decision = 'WINNER'
  AND route.action = 'ESCALATE'
  AND (route.explanation ->> 'escalation_level')::integer <= binding.max_escalation_level;

CREATE OR REPLACE FUNCTION pgreact_mdm.submit_intent(
    context pgreact.activation_context,
    candidate jsonb
)
RETURNS void
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    binding pgreact_mdm.policy_intent_bindings%ROWTYPE;
    runtime pgreact_mdm.policy_intent_runtime%ROWTYPE;
    policy_case mdm_steward.policy_cases_v1%ROWTYPE;
    saved pgreact_mdm.intent_requests%ROWTYPE;
    body jsonb;
    requested_body jsonb;
    key bytea;
    digest bytea;
    v_work_ref text := 'pgreact:' || (context).rule_version_id::text || ':' || (context).episode_id::text;
    response record;
    validation_count integer;
    conflict boolean := false;
BEGIN
    SELECT * INTO STRICT binding
    FROM pgreact_mdm.policy_intent_bindings
    WHERE pgreact_mdm.policy_intent_bindings.binding_id = (candidate ->> 'binding_id')::uuid
      AND enabled;
    SELECT * INTO STRICT runtime
    FROM pgreact_mdm.policy_intent_runtime
    WHERE pgreact_mdm.policy_intent_runtime.binding_id = binding.binding_id;
    IF runtime.database_oid <> (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())
       OR runtime.worker_role_oid <> (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = 'pgreact_mdm_worker')
       OR binding.policy_revision <> (candidate ->> 'policy_revision')
       OR binding.policy_digest <> decode(candidate ->> 'policy_digest', 'hex') THEN
        RETURN;
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(v_work_ref, 5788046901200002));
    SELECT * INTO saved
    FROM pgreact_mdm.intent_requests
    WHERE pgreact_mdm.intent_requests.work_ref = v_work_ref;
    IF FOUND THEN
        key := saved.request_key;
        body := saved.request_body;
        digest := saved.request_digest;
        IF saved.binding_id <> binding.binding_id THEN
            RAISE EXCEPTION 'MDM_WORK_BINDING_CHANGED: persisted work belongs to another binding';
        END IF;
        IF saved.first_episode_id <> (context).episode_id
           OR saved.policy_revision <> binding.policy_revision THEN
            RAISE EXCEPTION 'MDM_WORK_CORRELATION: persisted work reference has changed identity';
        END IF;
        requested_body := pgreact_mdm.intent_request_body(
            binding.binding_id, (candidate ->> 'case_key')::bigint,
            candidate ->> 'action', candidate -> 'arguments',
            (body ->> 'expected_review_version')::bigint,
            (body ->> 'expected_definition_version')::bigint,
            (body ->> 'expected_publication_revision')::bigint,
            (body ->> 'expected_stewardship_epoch')::bigint,
            decode(body ->> 'expected_evidence_basis_digest', 'hex'),
            (body ->> 'expected_action_revision')::bigint,
            decode(body ->> 'expected_policy_digest', 'hex'),
            saved.policy_revision, body ->> 'evaluation_ref', v_work_ref);
        IF saved.lifecycle_generation <> (context).generation
           OR saved.consequence_identity <> (candidate ->> 'consequence_identity')
           OR saved.escalation_level <> (candidate ->> 'escalation_level')::integer
           OR requested_body IS DISTINCT FROM body THEN
            conflict := true;
        END IF;
    ELSE
        SELECT count(*) INTO validation_count
        FROM pgreact_mdm.authorized_policy_cases_v1 AS current_case
        WHERE current_case.case_key = (candidate ->> 'case_key')::bigint
          AND current_case.entity_name = binding.entity_name
          AND CASE candidate ->> 'action'
              WHEN 'ASSIGN_QUEUE' THEN EXISTS (
                  SELECT 1 FROM pgreact_mdm.intent_queue_candidates AS intent
                  WHERE intent.binding_id = binding.binding_id
                    AND intent.case_key = current_case.case_key
                    AND intent.arguments ->> 'queue' = candidate -> 'arguments' ->> 'queue')
              WHEN 'SET_DUE_AT' THEN EXISTS (
                  SELECT 1 FROM pgreact_mdm.intent_due_candidates AS intent
                  WHERE intent.binding_id = binding.binding_id
                    AND intent.case_key = current_case.case_key
                    AND intent.arguments ->> 'due_at' = candidate -> 'arguments' ->> 'due_at')
              WHEN 'ESCALATE' THEN EXISTS (
                  SELECT 1 FROM pgreact_mdm.intent_escalation_candidates AS intent
                  WHERE intent.binding_id = binding.binding_id
                    AND intent.case_key = current_case.case_key
                    AND intent.arguments ->> 'level' = candidate -> 'arguments' ->> 'level')
              ELSE false
              END;
        IF validation_count = 0 THEN
            RETURN;
        END IF;
        SELECT * INTO STRICT policy_case
        FROM pgreact_mdm.authorized_policy_cases_v1
        WHERE case_key = (candidate ->> 'case_key')::bigint
          AND entity_name = binding.entity_name;
        key := pgreact_mdm.intent_request_key(
            binding.binding_id, binding.policy_revision, policy_case.case_key,
            (context).generation, policy_case.action_revision,
            candidate ->> 'consequence_identity', (candidate ->> 'escalation_level')::integer);
        body := pgreact_mdm.intent_request_body(
            binding.binding_id, policy_case.case_key,
            candidate ->> 'action', candidate -> 'arguments',
            policy_case.review_version, policy_case.definition_version,
            policy_case.publication_revision, policy_case.stewardship_epoch,
            policy_case.evidence_basis_digest, policy_case.action_revision,
            binding.policy_digest, binding.policy_revision,
            'pgreact:' || (context).activation_id::text,
            v_work_ref);
        digest := pgreact_mdm.intent_request_digest(body);
        IF EXISTS (
            SELECT 1 FROM pgreact_mdm.intent_holds AS hold
            WHERE hold.binding_id = binding.binding_id
              AND hold.case_key = policy_case.case_key
              AND hold.policy_revision = binding.policy_revision
              AND hold.expected_action_revision = policy_case.action_revision) THEN
            RETURN;
        END IF;
        INSERT INTO pgreact_mdm.intent_requests(
            binding_id, request_key, request_digest, request_body,
            work_ref, policy_revision, case_key, lifecycle_generation,
            action_revision, consequence_identity, escalation_level,
            first_episode_id)
        VALUES (
            binding.binding_id, key, digest, body,
            v_work_ref, binding.policy_revision, policy_case.case_key,
            (context).generation, policy_case.action_revision,
            candidate ->> 'consequence_identity',
            (candidate ->> 'escalation_level')::integer,
            (context).episode_id)
        ON CONFLICT (binding_id, request_key) DO NOTHING;
        SELECT * INTO STRICT saved
        FROM pgreact_mdm.intent_requests
        WHERE pgreact_mdm.intent_requests.binding_id = binding.binding_id
          AND pgreact_mdm.intent_requests.request_key = key;
        IF saved.request_body IS DISTINCT FROM body
           OR saved.policy_revision <> binding.policy_revision
           OR saved.case_key <> policy_case.case_key
           OR saved.work_ref <> v_work_ref
           OR saved.lifecycle_generation <> (context).generation
           OR saved.action_revision <> policy_case.action_revision
           OR saved.consequence_identity <> (candidate ->> 'consequence_identity')
           OR saved.escalation_level <> (candidate ->> 'escalation_level')::integer THEN
            conflict := true;
        ELSE
            body := saved.request_body;
            digest := saved.request_digest;
        END IF;
    END IF;

    IF conflict THEN
        INSERT INTO pgreact_mdm.intent_attempts(
            episode_id, attempt_no, binding_id, request_key, request_digest,
            request_body, work_ref, outcome, reason_code, case_key, action_revision)
        VALUES ((context).episode_id, (context).attempt_no, binding.binding_id,
            key, digest, body, v_work_ref, 'IDEMPOTENCY_CONFLICT',
            'REQUEST_KEY_BODY_MISMATCH', (body ->> 'case_key')::bigint,
            (body ->> 'expected_action_revision')::bigint);
        INSERT INTO pgreact_mdm.intent_holds(
            binding_id, case_key, policy_revision, action,
            expected_action_revision, reason_code)
        VALUES (
            binding.binding_id, (body ->> 'case_key')::bigint,
            binding.policy_revision, body ->> 'action',
            (body ->> 'expected_action_revision')::bigint,
            'REQUEST_KEY_BODY_MISMATCH')
        ON CONFLICT (
            binding_id, case_key, policy_revision, action, expected_action_revision)
        DO NOTHING;
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pgreact_mdm.intent_attempts AS previous_attempt
        WHERE previous_attempt.binding_id = binding.binding_id
          AND previous_attempt.request_key = key
          AND previous_attempt.receipt_id IS NOT NULL
          AND NOT pgreact_mdm.intent_receipt_exists(
              previous_attempt.binding_id,
              previous_attempt.request_key,
              previous_attempt.receipt_id)) THEN
        INSERT INTO pgreact_mdm.intent_attempts(
            episode_id, attempt_no, binding_id, request_key, request_digest,
            request_body, work_ref, outcome, reason_code, case_key, action_revision)
        VALUES (
            (context).episode_id, (context).attempt_no, binding.binding_id,
            key, digest, body, v_work_ref, 'RECOVERY_BLOCKED',
            'MDM_RECEIPT_MISSING', (body ->> 'case_key')::bigint,
            (body ->> 'expected_action_revision')::bigint);
        INSERT INTO pgreact_mdm.intent_holds(
            binding_id, case_key, policy_revision, action,
            expected_action_revision, reason_code)
        VALUES (
            binding.binding_id, (body ->> 'case_key')::bigint,
            binding.policy_revision, body ->> 'action',
            (body ->> 'expected_action_revision')::bigint,
            'MDM_RECEIPT_MISSING')
        ON CONFLICT (
            binding_id, case_key, policy_revision, action, expected_action_revision)
        DO NOTHING;
        RETURN;
    END IF;

    SELECT * INTO STRICT response
    FROM mdm_steward.submit_policy_intent(
        binding.binding_id, key, (body ->> 'case_key')::bigint,
        body ->> 'action', body -> 'arguments',
        (body ->> 'expected_review_version')::bigint,
        (body ->> 'expected_definition_version')::bigint,
        (body ->> 'expected_publication_revision')::bigint,
        (body ->> 'expected_stewardship_epoch')::bigint,
        decode(body ->> 'expected_evidence_basis_digest', 'hex'),
        (body ->> 'expected_action_revision')::bigint,
        decode(body ->> 'expected_policy_digest', 'hex'),
        body ->> 'policy_revision', body ->> 'evaluation_ref', body ->> 'work_ref');
    INSERT INTO pgreact_mdm.intent_attempts(
        episode_id, attempt_no, binding_id, request_key, request_digest,
        request_body, work_ref, receipt_id, outcome, reason_code,
        case_key, action_revision, control, resulting_publication_revision)
    VALUES (
        (context).episode_id, (context).attempt_no, binding.binding_id,
        key, digest, body, v_work_ref, response.receipt_id,
        response.outcome, response.reason_code, response.case_key,
        response.action_revision, response.control,
        response.resulting_publication_revision);
    IF response.outcome IS DISTINCT FROM 'APPLIED_CONTROL'
       AND response.outcome IS DISTINCT FROM 'NO_CHANGE' THEN
        INSERT INTO pgreact_mdm.intent_holds(
            binding_id, case_key, policy_revision, action,
            expected_action_revision, reason_code)
        VALUES (
            binding.binding_id, response.case_key, binding.policy_revision,
            body ->> 'action', (body ->> 'expected_action_revision')::bigint,
            CASE WHEN response.outcome IN (
                'STALE_CASE', 'CASE_CLOSED', 'ACTION_DENIED',
                'MANUAL_PROTECTION', 'OPENED_AT_UNKNOWN', 'LIMIT_EXCEEDED',
                'BINDING_PAUSED', 'BINDING_REPLACED', 'POLICY_MISMATCH',
                'PENDING_STEWARDSHIP', 'IDEMPOTENCY_CONFLICT')
            THEN COALESCE(response.reason_code, response.outcome)
            ELSE 'UNKNOWN_MDM_OUTCOME' END)
        ON CONFLICT (
            binding_id, case_key, policy_revision, action, expected_action_revision)
        DO NOTHING;
    END IF;
END
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.submit_intent_as_worker(
    context pgreact.activation_context,
    candidate jsonb
)
RETURNS void
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
BEGIN
    PERFORM pgreact_mdm.submit_intent($1, $2);
END
$function$;
ALTER FUNCTION pgreact_mdm.submit_intent_as_worker(
    pgreact.activation_context, jsonb) OWNER TO pgreact_mdm_worker;
REVOKE ALL ON FUNCTION pgreact_mdm.submit_intent_as_worker(
    pgreact.activation_context, jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgreact_mdm.submit_intent(
    pgreact.activation_context, jsonb) TO pgreact_mdm_worker;

CREATE OR REPLACE FUNCTION pgreact_mdm.submit_queue_intent(
    context pgreact.activation_context,
    policy_case pgreact_mdm.intent_deployer_policy_cases_v1
)
RETURNS void
LANGUAGE plpgsql VOLATILE
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE candidate jsonb;
BEGIN
    SELECT to_jsonb(intent) INTO candidate
    FROM pgreact_mdm.intent_queue_candidates AS intent
    JOIN pgreact_mdm.policy_intent_bindings AS binding USING (binding_id)
    WHERE intent.case_key = ($2).case_key
      AND binding.entity_name::text = ($2).entity_name
    ORDER BY intent.binding_id
    LIMIT 1;
    IF FOUND THEN
        PERFORM pgreact_mdm.submit_intent_as_worker($1, candidate);
    END IF;
END
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.submit_due_intent(
    context pgreact.activation_context,
    policy_case pgreact_mdm.intent_deployer_policy_cases_v1
)
RETURNS void
LANGUAGE plpgsql VOLATILE
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE candidate jsonb;
BEGIN
    SELECT to_jsonb(intent) INTO candidate
    FROM pgreact_mdm.intent_due_candidates AS intent
    JOIN pgreact_mdm.policy_intent_bindings AS binding USING (binding_id)
    WHERE intent.case_key = ($2).case_key
      AND binding.entity_name::text = ($2).entity_name
    ORDER BY intent.binding_id
    LIMIT 1;
    IF FOUND THEN
        PERFORM pgreact_mdm.submit_intent_as_worker($1, candidate);
    END IF;
END
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.submit_escalation_intent(
    context pgreact.activation_context,
    policy_case pgreact_mdm.intent_deployer_policy_cases_v1
)
RETURNS void
LANGUAGE plpgsql VOLATILE
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE candidate jsonb;
BEGIN
    SELECT to_jsonb(intent) INTO candidate
    FROM pgreact_mdm.intent_escalation_candidates AS intent
    JOIN pgreact_mdm.policy_intent_bindings AS binding USING (binding_id)
    WHERE intent.case_key = ($2).case_key
      AND binding.entity_name::text = ($2).entity_name
    ORDER BY intent.binding_id
    LIMIT 1;
    IF FOUND THEN
        PERFORM pgreact_mdm.submit_intent_as_worker($1, candidate);
    END IF;
END
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.change_queue_intent(
    context pgreact.activation_context,
    previous_case pgreact_mdm.intent_deployer_policy_cases_v1,
    policy_case pgreact_mdm.intent_deployer_policy_cases_v1
)
RETURNS void LANGUAGE SQL VOLATILE
SET search_path = pg_catalog, pg_temp
AS $function$
    SELECT pgreact_mdm.submit_queue_intent($1, $3)
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.change_due_intent(
    context pgreact.activation_context,
    previous_case pgreact_mdm.intent_deployer_policy_cases_v1,
    policy_case pgreact_mdm.intent_deployer_policy_cases_v1
)
RETURNS void LANGUAGE SQL VOLATILE
SET search_path = pg_catalog, pg_temp
AS $function$
    SELECT pgreact_mdm.submit_due_intent($1, $3)
$function$;

CREATE OR REPLACE FUNCTION pgreact_mdm.change_escalation_intent(
    context pgreact.activation_context,
    previous_case pgreact_mdm.intent_deployer_policy_cases_v1,
    policy_case pgreact_mdm.intent_deployer_policy_cases_v1
)
RETURNS void LANGUAGE SQL VOLATILE
SET search_path = pg_catalog, pg_temp
AS $function$
    SELECT pgreact_mdm.submit_escalation_intent($1, $3)
$function$;

GRANT CREATE ON SCHEMA pgreact_mdm TO pgreact_mdm_worker;
ALTER FUNCTION pgreact_mdm.submit_intent(pgreact.activation_context, jsonb) OWNER TO pgreact_mdm_worker;
REVOKE CREATE ON SCHEMA pgreact_mdm FROM pgreact_mdm_worker;
GRANT EXECUTE ON FUNCTION pgreact_mdm.submit_intent(pgreact.activation_context, jsonb) TO CURRENT_USER;

GRANT USAGE ON SCHEMA pgreact, pgreact_mdm, mdm_steward TO pgreact_mdm_worker;
GRANT USAGE ON SCHEMA mdm_steward TO mdm_helper_owner WITH GRANT OPTION;
GRANT SELECT ON TABLE pgreact_mdm.policy_packages TO mdm_helper_owner WITH GRANT OPTION;
GRANT SELECT ON TABLE pgreact_mdm.policy_packages TO pgreact_mdm_worker;
GRANT SELECT ON pgreact_mdm.intent_queue_candidates,
    pgreact_mdm.intent_due_candidates,
    pgreact_mdm.intent_escalation_candidates TO mdm_helper_owner WITH GRANT OPTION;
REVOKE ALL ON TABLE mdm_steward.policy_cases_v1,
    mdm_steward.policy_bindings_v1,
    mdm_steward.policy_receipts_v1,
    mdm_internal.policy_binding_runtime FROM pgreact_mdm_worker;
REVOKE ALL ON SCHEMA mdm_admin, mdm_internal FROM pgreact_mdm_worker;
GRANT SELECT ON pgreact_mdm.policy_intent_bindings,
    pgreact_mdm.policy_intent_runtime TO pgreact_mdm_worker;
GRANT SELECT, INSERT ON pgreact_mdm.intent_requests,
    pgreact_mdm.intent_attempts,
    pgreact_mdm.intent_holds TO pgreact_mdm_worker;
GRANT SELECT ON pgreact_mdm.authorized_policy_cases_v1,
    pgreact_mdm.intent_queue_candidates,
    pgreact_mdm.intent_due_candidates,
    pgreact_mdm.intent_escalation_candidates TO pgreact_mdm_worker;
REVOKE ALL ON TABLE pgreact_mdm.policy_intent_packages FROM pgreact_mdm_worker;
GRANT EXECUTE ON FUNCTION mdm_steward.submit_policy_intent(
    uuid, bytea, bigint, text, jsonb, bigint, bigint, bigint, bigint,
    bytea, bigint, bytea, text, text, text) TO mdm_helper_owner WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION mdm_steward.submit_policy_intent(
    uuid, bytea, bigint, text, jsonb, bigint, bigint, bigint, bigint,
    bytea, bigint, bytea, text, text, text) TO pgreact_mdm_worker;
GRANT EXECUTE ON FUNCTION pgreact_mdm.submit_queue_intent(
    pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1),
    pgreact_mdm.submit_due_intent(
    pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1),
    pgreact_mdm.submit_escalation_intent(
    pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1)
    TO pgreact_mdm_worker;
GRANT EXECUTE ON FUNCTION pgreact_mdm.change_queue_intent(
    pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1,
    pgreact_mdm.intent_deployer_policy_cases_v1),
    pgreact_mdm.change_due_intent(
    pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1,
    pgreact_mdm.intent_deployer_policy_cases_v1),
    pgreact_mdm.change_escalation_intent(
    pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1,
    pgreact_mdm.intent_deployer_policy_cases_v1) TO pgreact_mdm_worker;
REVOKE ALL ON FUNCTION pgreact_mdm.submit_intent(pgreact.activation_context, jsonb),
    pgreact_mdm.submit_queue_intent(pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1),
    pgreact_mdm.submit_due_intent(pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1),
    pgreact_mdm.submit_escalation_intent(pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1),
    pgreact_mdm.change_queue_intent(pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1, pgreact_mdm.intent_deployer_policy_cases_v1),
    pgreact_mdm.change_due_intent(pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1, pgreact_mdm.intent_deployer_policy_cases_v1),
    pgreact_mdm.change_escalation_intent(pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1, pgreact_mdm.intent_deployer_policy_cases_v1) FROM PUBLIC;

CREATE OR REPLACE FUNCTION pgreact_mdm.execute_intent_episode(
    rule_version_id uuid,
    worker_id text
)
RETURNS bigint
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    version pgreact_internal.rule_versions%ROWTYPE;
    runner pg_catalog.pg_roles%ROWTYPE;
    claimed record;
BEGIN
    SELECT * INTO STRICT runner
    FROM pg_catalog.pg_roles
    WHERE rolname = session_user
      AND NOT rolsuper AND NOT rolbypassrls;
    IF NOT pg_catalog.pg_has_role(runner.oid, 'pgreact_mdm_worker', 'SET') THEN
        RAISE EXCEPTION 'MDM_RUNNER_ROLE: session user cannot set the policy worker role';
    END IF;
    SELECT * INTO STRICT version
    FROM pgreact_internal.rule_versions
    WHERE pgreact_internal.rule_versions.rule_version_id = $1;
    IF version.owner_oid <> runner.oid
       OR version.state <> 'ACTIVE'
       OR version.consequence_oid IS NULL
       OR version.consequence_oid NOT IN (
           'pgreact_mdm.submit_queue_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure::oid,
           'pgreact_mdm.submit_due_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure::oid,
           'pgreact_mdm.submit_escalation_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure::oid,
           'pgreact_mdm.change_queue_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure::oid,
           'pgreact_mdm.change_due_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure::oid,
           'pgreact_mdm.change_escalation_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure::oid) THEN
        RAISE EXCEPTION 'MDM_RUNNER_RULE: rule is not an active intent rule owned by %', session_user;
    END IF;
    SELECT * INTO claimed FROM pgreact.claim_episode_m31_base($1, $2);
    IF NOT FOUND THEN RETURN NULL; END IF;
    PERFORM pgreact.execute_claimed_episode_m31_base(
        claimed.episode_id, $2, claimed.lease_token);
    RETURN claimed.episode_id;
END
$function$;
GRANT EXECUTE ON FUNCTION pgreact_mdm.submit_queue_intent(
    pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1),
    pgreact_mdm.submit_due_intent(
    pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1),
    pgreact_mdm.submit_escalation_intent(
    pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1),
    pgreact_mdm.change_queue_intent(
    pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1,
    pgreact_mdm.intent_deployer_policy_cases_v1),
    pgreact_mdm.change_due_intent(
    pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1,
    pgreact_mdm.intent_deployer_policy_cases_v1),
    pgreact_mdm.change_escalation_intent(
    pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1,
    pgreact_mdm.intent_deployer_policy_cases_v1),
    pgreact_mdm.execute_intent_episode(uuid, text)
    TO mdm_helper_owner WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION pgreact_mdm.execute_intent_episode(uuid, text)
    TO pgreact_mdm_worker;

CREATE OR REPLACE FUNCTION pgreact_mdm.configure_intent_deployer(deployer name)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    role_row pg_catalog.pg_roles%ROWTYPE;
    dispatcher record;
BEGIN
    SELECT * INTO STRICT role_row
    FROM pg_catalog.pg_roles
    WHERE rolname = $1
      AND rolcanlogin AND NOT rolsuper AND NOT rolbypassrls;
    IF NOT pg_catalog.pg_has_role(role_row.oid, 'pgreact_mdm_worker', 'SET') THEN
        RAISE EXCEPTION 'MDM_DEPLOYER_ROLE: deployer must be able to set pgreact_mdm_worker';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM pgreact_internal.rule_versions AS version
        WHERE version.state <> 'REMOVED'
          AND version.consequence_oid IN (
              'pgreact_mdm.submit_queue_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure::oid,
              'pgreact_mdm.submit_due_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure::oid,
              'pgreact_mdm.submit_escalation_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure::oid,
              'pgreact_mdm.change_queue_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure::oid,
              'pgreact_mdm.change_due_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure::oid,
              'pgreact_mdm.change_escalation_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure::oid)
          AND version.owner_oid <> role_row.oid) THEN
        RAISE EXCEPTION 'MDM_DEPLOYER_CHANGE: pause and remove existing intent rules before changing deployer';
    END IF;

    EXECUTE format('GRANT USAGE ON SCHEMA pgreact, pgreact_api, pgreact_mdm, pgtrickle, mdm_steward TO %I', $1);
    EXECUTE format('GRANT USAGE, CREATE ON SCHEMA pgreact_runtime TO %I', $1);
    EXECUTE format('GRANT SELECT ON TABLE mdm_steward.policy_cases_v1 TO %I', $1);
    EXECUTE format('GRANT SELECT ON TABLE pgreact_mdm.intent_case_identities TO %I', $1);
    EXECUTE format('GRANT SELECT ON TABLE pgreact_mdm.policy_packages, pgreact_mdm.policy_intent_packages, pgreact_mdm.policy_intent_bindings, pgreact_mdm.policy_intent_runtime TO %I', $1);
    EXECUTE format('GRANT SELECT ON TABLE pgreact.rules TO %I', $1);
    EXECUTE format('GRANT SELECT ON TABLE pgreact_mdm.authorized_policy_cases_v1, pgreact_mdm.intent_deployer_policy_cases_v1, pgreact_mdm.intent_queue_candidates, pgreact_mdm.intent_due_candidates, pgreact_mdm.intent_escalation_candidates TO %I', $1);
    EXECUTE format('GRANT CREATE ON SCHEMA pgreact_mdm TO %I', $1);
    IF (SELECT relowner FROM pg_catalog.pg_class
        WHERE oid = 'pgreact_mdm.intent_deployer_policy_cases_v1'::regclass) <> role_row.oid THEN
        EXECUTE format('ALTER VIEW pgreact_mdm.intent_deployer_policy_cases_v1 OWNER TO %I', $1);
    END IF;
    IF (SELECT relowner FROM pg_catalog.pg_class
        WHERE oid = 'pgreact_mdm.intent_queue_candidates'::regclass) <> role_row.oid THEN
        EXECUTE format('ALTER VIEW pgreact_mdm.intent_queue_candidates OWNER TO %I', $1);
    END IF;
    IF (SELECT relowner FROM pg_catalog.pg_class
        WHERE oid = 'pgreact_mdm.intent_due_candidates'::regclass) <> role_row.oid THEN
        EXECUTE format('ALTER VIEW pgreact_mdm.intent_due_candidates OWNER TO %I', $1);
    END IF;
    IF (SELECT relowner FROM pg_catalog.pg_class
        WHERE oid = 'pgreact_mdm.intent_escalation_candidates'::regclass) <> role_row.oid THEN
        EXECUTE format('ALTER VIEW pgreact_mdm.intent_escalation_candidates OWNER TO %I', $1);
    END IF;
    EXECUTE format('ALTER FUNCTION pgreact_mdm.submit_queue_intent(pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1) OWNER TO %I', $1);
    EXECUTE format('ALTER FUNCTION pgreact_mdm.submit_due_intent(pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1) OWNER TO %I', $1);
    EXECUTE format('ALTER FUNCTION pgreact_mdm.submit_escalation_intent(pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1) OWNER TO %I', $1);
    EXECUTE format('ALTER FUNCTION pgreact_mdm.change_queue_intent(pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1, pgreact_mdm.intent_deployer_policy_cases_v1) OWNER TO %I', $1);
    EXECUTE format('ALTER FUNCTION pgreact_mdm.change_due_intent(pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1, pgreact_mdm.intent_deployer_policy_cases_v1) OWNER TO %I', $1);
    EXECUTE format('ALTER FUNCTION pgreact_mdm.change_escalation_intent(pgreact.activation_context, pgreact_mdm.intent_deployer_policy_cases_v1, pgreact_mdm.intent_deployer_policy_cases_v1) OWNER TO %I', $1);
    EXECUTE 'GRANT CREATE ON SCHEMA pgreact_runtime TO pgreact_mdm_worker';
    FOR dispatcher IN
        SELECT namespace.nspname, procedure.proname,
               pg_catalog.pg_get_function_identity_arguments(procedure.oid) AS arguments
        FROM pgreact_internal.rule_versions AS version
        JOIN pg_catalog.pg_proc AS procedure
          ON procedure.oid = version.dispatcher_oid
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = procedure.pronamespace
        WHERE version.state = 'ACTIVE'
          AND version.owner_oid = role_row.oid
          AND version.consequence_oid IN (
              'pgreact_mdm.submit_queue_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure::oid,
              'pgreact_mdm.submit_due_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure::oid,
              'pgreact_mdm.submit_escalation_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure::oid,
              'pgreact_mdm.change_queue_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure::oid,
              'pgreact_mdm.change_due_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure::oid,
              'pgreact_mdm.change_escalation_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure::oid)
    LOOP
        EXECUTE format(
            'ALTER FUNCTION %I.%I(%s) OWNER TO pgreact_mdm_worker',
            dispatcher.nspname, dispatcher.proname, dispatcher.arguments);
    END LOOP;
    EXECUTE 'REVOKE CREATE ON SCHEMA pgreact_runtime FROM pgreact_mdm_worker';
    EXECUTE format('REVOKE CREATE ON SCHEMA pgreact_mdm FROM %I', $1);
    EXECUTE format('GRANT EXECUTE ON FUNCTION pgreact.preview(pgreact_api.declaration, jsonb), pgreact.review_token(jsonb), pgreact.deploy(pgreact_api.declaration, text, jsonb), pgreact_mdm.submit_intent(pgreact.activation_context, jsonb), pgreact_mdm.submit_intent_as_worker(pgreact.activation_context, jsonb), pgreact_mdm.execute_intent_episode(uuid, text), pgreact_mdm.sync_intent_case_identities(), pgreact.refresh_rule(uuid) TO %I', $1);
END
$function$;

REVOKE ALL ON FUNCTION pgreact_mdm.configure_intent_deployer(name),
    pgreact_mdm.execute_intent_episode(uuid, text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION pgreact_mdm.intent_declaration(
    policy_name text,
    source_relation regclass,
    valid_from timestamptz
)
RETURNS pgreact_api.declaration
LANGUAGE plpgsql
SECURITY DEFINER
VOLATILE
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    package_set text;
    queue_rule pgreact_api.declaration;
    due_rule pgreact_api.declaration;
    escalation_rule pgreact_api.declaration;
BEGIN
    SELECT encode(pg_catalog.sha256(convert_to(
        pgreact_mdm.canonical_json(COALESCE(jsonb_agg(jsonb_build_object(
            'policy_revision', policy_revision,
            'policy_digest', encode(policy_digest, 'hex'))
            ORDER BY policy_revision), '[]'::jsonb)), 'UTF8')), 'hex')
    INTO package_set
    FROM pgreact_mdm.policy_intent_packages;
    package_set := encode(pg_catalog.sha256(convert_to(
        'pg_react/mdm-stewardship-worker/v0.48.0:' || package_set, 'UTF8')), 'hex');

    queue_rule := pgreact.rule(
        name => policy_name || '-queue',
        condition => 'pgreact_mdm.intent_deployer_policy_cases_v1'::regclass,
        semantic_key => 'case_identity', kind => 'COMMAND',
        on_activate => 'pgreact_mdm.submit_queue_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure,
        on_change => 'pgreact_mdm.change_queue_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure,
        change_columns => ARRAY['action_revision'],
        bootstrap_policy => 'SEED_CURRENT', salience => 10,
        max_attempts => 5, initial_backoff_seconds => 1,
        backoff_multiplier => 2, max_backoff_seconds => 30);
    due_rule := pgreact.rule(
        name => policy_name || '-due',
        condition => 'pgreact_mdm.intent_deployer_policy_cases_v1'::regclass,
        semantic_key => 'case_identity', kind => 'COMMAND',
        on_activate => 'pgreact_mdm.submit_due_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure,
        on_change => 'pgreact_mdm.change_due_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure,
        change_columns => ARRAY['action_revision'],
        bootstrap_policy => 'SEED_CURRENT', salience => 10,
        max_attempts => 5, initial_backoff_seconds => 1,
        backoff_multiplier => 2, max_backoff_seconds => 30);
    escalation_rule := pgreact.rule(
        name => policy_name || '-escalation',
        condition => 'pgreact_mdm.intent_deployer_policy_cases_v1'::regclass,
        semantic_key => 'case_identity', kind => 'COMMAND',
        on_activate => 'pgreact_mdm.submit_escalation_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure,
        on_change => 'pgreact_mdm.change_escalation_intent(pgreact.activation_context,pgreact_mdm.intent_deployer_policy_cases_v1,pgreact_mdm.intent_deployer_policy_cases_v1)'::regprocedure,
        change_columns => ARRAY['action_revision'],
        bootstrap_policy => 'SEED_CURRENT', salience => 10,
        max_attempts => 5, initial_backoff_seconds => 1,
        backoff_multiplier => 2, max_backoff_seconds => 30);
    RETURN pgreact.policy_set(
        name => policy_name,
        version => package_set,
        members => ARRAY[queue_rule, due_rule, escalation_rule]::pgreact_api.declaration[],
        applicability => source_relation,
        subject_keys => ARRAY['case_key'::name],
        support => ARRAY[pgreact.shared_condition(
            'mdm-policy-inputs', source_relation, ARRAY['case_key'::name], 'FULL')]
            ::pgreact_api.declaration[],
        dependencies => '[]'::jsonb,
        valid_from => valid_from,
        evidence_limit => 100);
END
$function$;

REVOKE ALL ON FUNCTION pgreact_mdm.create_intent_binding(text, text, text[], text[], interval, integer),
    pgreact_mdm.replace_intent_binding(uuid, bigint, text, text[], text[], interval, integer),
    pgreact_mdm.reconcile_intent_binding(uuid, bigint),
    pgreact_mdm.pause_intent_binding(uuid, bigint),
    pgreact_mdm.intent_declaration(text, regclass, timestamptz) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgreact_mdm.intent_declaration(text, regclass, timestamptz) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgreact_mdm.create_intent_binding(text, text, text[], text[], interval, integer),
    pgreact_mdm.replace_intent_binding(uuid, bigint, text, text[], text[], interval, integer),
    pgreact_mdm.reconcile_intent_binding(uuid, bigint),
    pgreact_mdm.pause_intent_binding(uuid, bigint) TO PUBLIC;
