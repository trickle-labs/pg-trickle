#!/usr/bin/env python3
"""Run the S1 entry audit in one disposable joint-stack container."""
import copy
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

from bootstrap_compat import adapt_bootstrap


def run_docker_exec_with_launch_retry(args, *, timeout=300, on_retry=None):
    """Retry only OCI exec errors proving the container command never launched.

    No retry is permitted for a started psql process (including SQL errors),
    Docker daemon failures, timeouts, or any other uncertain-execution case.
    """
    for attempt in range(3):
        result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        did_not_start = (len(args) >= 4 and tuple(args[:2]) == ("docker", "exec")
                         and result.returncode != 0 and not result.stdout
                         and result.stderr.strip() ==
                         "OCI runtime exec failed: exec failed: unable to start container process: "
                         "procReady not received")
        if not did_not_start or attempt == 2:
            return result
        if on_retry is not None:
            on_retry(attempt + 1, args)
        time.sleep(1 + attempt)


def main():
    image, output = sys.argv[1:3]
    maintenance = sys.argv[3:] == ["--maintenance"]
    if sys.argv[3:] and not maintenance:
        raise ValueError("only --maintenance is supported after IMAGE OUTPUT")
    batch_size = int(os.environ.get("PGREACT_S1_BATCH_SIZE", "32"))
    if not 1 <= batch_size <= 1000:
        raise ValueError("PGREACT_S1_BATCH_SIZE must be between 1 and 1000")
    churn_per_minute = 60
    churn_window_minutes = 5
    profile_case_count = os.environ.get("R53_PROFILE_CASE_COUNT")
    profile_case_count = int(profile_case_count) if profile_case_count else None
    if profile_case_count is not None and not 1 <= profile_case_count <= 300:
        raise ValueError("R53_PROFILE_CASE_COUNT must be between 1 and 300")
    churn_case_count = profile_case_count or churn_per_minute * churn_window_minutes
    if profile_case_count is not None:
        churn_window_minutes = churn_case_count / churn_per_minute
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    root = Path(__file__).resolve().parents[4]
    container = "pgreact-s1-" + uuid.uuid4().hex[:12]
    observations = {"image": image, "checks": [], "entry_decision": "INCOMPLETE"}
    if profile_case_count is not None:
        observations["diagnostic_profile"] = {
            "profile_only": True,
            "churn_cases": profile_case_count,
            "nominal_rate_target_cases_per_minute": churn_per_minute,
            "acceptance_rate_assessed": False,
            "instrumentation": "auto_explain nested statements, analyze, buffers; 10ms threshold; timing off",
        }
    containers = []
    active_database = ["foundation"]
    created = False
    churn_progress = output / "churn-progress.jsonl"
    profile_log = output / "auto-explain-churn-psql.log"
    churn_completed_batches = 0
    churn_phase_started = False

    def record_docker_exec_retry(attempt, args):
        # Retain a bounded, non-sensitive retry receipt in the exact-run evidence.
        observations.setdefault("docker_exec_launch_retries", []).append({
            "attempt": attempt, "container": args[2], "program": args[3],
            "reason": "OCI exec process did not start",
        })
        print("RETRY: OCI docker exec not started; attempt " + str(attempt), flush=True)

    def command(*args):
        result = run_docker_exec_with_launch_retry(args, on_retry=record_docker_exec_retry)
        if result.returncode:
            raise RuntimeError(" ".join(args) + "\n" + result.stdout + result.stderr)
        return result.stdout

    def sql(query, database=None):
        database = database or active_database[0]
        return command("docker", "exec", container, "psql", "-XAtq", "-U",
                       "postgres", "-d", database, "-v", "ON_ERROR_STOP=1", "-c", query).strip()

    def profiled_sql(query, database=None):
        database = database or active_database[0]
        result = run_docker_exec_with_launch_retry(
            ["docker", "exec", container, "psql", "-XAtq", "-U", "postgres", "-d",
             database, "-v", "ON_ERROR_STOP=1", "-c", query],
            on_retry=record_docker_exec_retry)
        if result.returncode:
            raise RuntimeError(query + "\n" + result.stdout + result.stderr)
        with profile_log.open("a") as log:
            log.write(result.stderr)
        return result.stdout.strip()

    def timestamp(value):
        if value[-3] in "+-" and value[-2:].isdigit():
            value += ":00"
        value = value.replace(" ", "T", 1)
        layout = "%Y-%m-%dT%H:%M:%S.%f%z" if "." in value else "%Y-%m-%dT%H:%M:%S%z"
        return datetime.datetime.strptime(value, layout)

    def fixture(name, file, database="foundation", variables=()):
        started = time.monotonic()
        result = command("docker", "exec", container, "psql", "-XAtq", "-U",
                         "postgres", "-d", database, "-v", "ON_ERROR_STOP=1", *variables, "-f", file)
        (output / (name + ".log")).write_text(result)
        observations["checks"].append({"name": name, "result": "passed",
                                       "seconds": time.monotonic() - started})
        print("PASS: " + name, flush=True)

    def cases():
        return json.loads(sql("SELECT jsonb_agg(to_jsonb(c) ORDER BY c.case_key) "
                              "FROM mdm_steward.policy_cases_v1 c"))

    def database_metrics():
        return json.loads(sql("SELECT jsonb_build_object("
            "'captured_at', clock_timestamp(), "
            "'database_bytes', pg_database_size(current_database()), "
            "'database_stats', (SELECT to_jsonb(s) FROM pg_stat_database s "
            "WHERE s.datname = current_database()), "
            "'wal_bytes', (SELECT wal_bytes FROM pg_stat_wal), "
            "'relation_sizes', (SELECT COALESCE(jsonb_agg(jsonb_build_object("
            "'relation', n.nspname || '.' || c.relname, "
            "'bytes', pg_total_relation_size(c.oid)) ORDER BY n.nspname, c.relname), '[]') "
            "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relkind = 'r' AND n.nspname IN "
            "('mdm_steward','pgreact','pgreact_internal','pgreact_mdm')))"))

    def workload_episode_attempts():
        return json.loads(sql(
            "SELECT COALESCE(jsonb_agg(jsonb_build_object("
            "'case_key', w.case_key, 'activation', to_jsonb(a), 'episode', to_jsonb(e), "
            "'attempts', COALESCE((SELECT jsonb_agg(to_jsonb(x) ORDER BY x.attempt_no) "
            "FROM pgreact.attempts x WHERE x.episode_id=e.episode_id), '[]'::jsonb)) "
            "ORDER BY w.case_key), '[]'::jsonb) "
            "FROM s1_audit.workload_cases w "
            "LEFT JOIN pgreact.rules r ON r.rule_name='s1-future-idle' AND r.state='ACTIVE' "
            "LEFT JOIN pgreact.activations a ON a.rule_version_id=r.rule_version_id "
            "AND a.active AND a.semantic_key=w.case_key "
            "LEFT JOIN pgreact.episodes e ON e.rule_version_id=a.rule_version_id "
            "AND e.activation_id=a.activation_id"))

    try:
        observations["image_id"] = command("docker", "image", "inspect", image,
                                           "--format", "{{.Id}}").strip()
        command("docker", "run", "--detach", "--name", container, "--network", "none",
                "--env", "POSTGRES_HOST_AUTH_METHOD=trust", "--volume", str(root) + ":/work:ro",
                image, "postgres", "-c", "shared_preload_libraries=pg_trickle,pg_react",
                "-c", "pg_trickle.enabled=off", "-c", "pg_trickle.cdc_mode=trigger",
                "-c", "pg_trickle.differential_max_change_ratio=1.0",
                "-c", "max_locks_per_transaction=256", "-c", "shared_buffers=8MB",
                "-c", "max_connections=20", "-c", "deadlock_timeout=1s",
                "-c", "log_lock_waits=on")
        containers.append(container)
        created = True
        print("CONTAINER: " + container, flush=True)
        for _ in range(60):
            result = subprocess.run(["docker", "exec", container, "pg_isready", "-U", "postgres"],
                                    capture_output=True)
            if result.returncode == 0 and "PostgreSQL init process complete" in command("docker", "logs", container):
                break
            time.sleep(1)
        else:
            raise RuntimeError("PostgreSQL readiness exceeded 60 seconds")
        setting_rows = json.loads(sql(
            "SELECT jsonb_agg(jsonb_build_object('name',name,'setting',setting,'unit',unit) "
            "ORDER BY name) FROM pg_settings WHERE name = ANY(ARRAY["
            "'shared_preload_libraries','pg_trickle.enabled','pg_trickle.cdc_mode',"
            "'pg_trickle.differential_max_change_ratio','max_locks_per_transaction',"
            "'shared_buffers','max_connections','deadlock_timeout','log_lock_waits'])", "postgres"))
        settings = {row["name"]: row for row in setting_rows}
        observations["postgres_settings"] = settings
        assert settings["shared_preload_libraries"]["setting"].replace(" ", "") == "pg_trickle,pg_react", settings
        assert settings["pg_trickle.enabled"]["setting"] == "off", settings
        assert settings["pg_trickle.cdc_mode"]["setting"] == "trigger", settings
        assert float(settings["pg_trickle.differential_max_change_ratio"]["setting"]) == 1.0, settings
        assert int(settings["max_locks_per_transaction"]["setting"]) == 256, settings
        assert int(settings["shared_buffers"]["setting"]) * 8192 == 8 * 1024 * 1024, settings
        assert int(settings["max_connections"]["setting"]) == 20, settings
        assert int(settings["deadlock_timeout"]["setting"]) == 1000, settings
        assert settings["log_lock_waits"]["setting"] == "on", settings
        source_revision = command("docker", "image", "inspect", image,
            "--format", '{{ index .Config.Labels "pg_mdm.source_revision" }}').strip()
        assert source_revision == "eef7937180d504e62ce1076074bbeed8cef5d5a7", source_revision
        capabilities = json.loads(sql("SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY capability), '[]') "
                                      "FROM pgtrickle.integration_capabilities() c", "postgres"))
        graph = next((row for row in capabilities if row["capability"] == "external_graph_refresh"), None)
        required_features = ["stable_row_identity_encoder_v3", "custom_table_srf_out_columns",
                             "lateral_immutable_composite_function"]
        advertised_features = (graph or {}).get("details", {}).get("differential_features")
        observations["mdm_pgtrickle_compatibility"] = {
            "mdm_source_revision": source_revision, "required_differential_features": required_features,
            "advertised_differential_features": advertised_features,
            "compatible": advertised_features == required_features}
        if advertised_features != required_features:
            observations.update(entry_decision="INPUT_BLOCKED",
                blocker="pg_mdm 0.14.1 and the installed pg_trickle capability profile are incompatible")
            observations["remaining_inputs"] = [
                "a released pg_mdm adapter compatible with pg_trickle 0.108.3, or an approved supported artifact pair"]
            print("INPUT_BLOCKED: installed pg_mdm rejects the pg_trickle differential capability profile", flush=True)
            return
        react_admission = json.loads(sql("SELECT pgreact_internal.pgtrickle_integration_status()", "postgres"))
        assert react_admission["extension_version"] == "0.108.3" and react_admission["qualified"], react_admission
        rejected_admission = json.loads(sql(
            "BEGIN; SET LOCAL pg_trickle.enabled=on; "
            "SELECT pgreact_internal.pgtrickle_integration_status(); ROLLBACK", "postgres"))
        assert rejected_admission["qualified"] is False, rejected_admission
        observations["react_runtime_admission"] = {"qualified": react_admission,
                                                  "scheduler_enabled_rejected": rejected_admission}
        upstream_fixture = output / "mdm-e2e-upstream.sql"
        command("docker", "cp", container + ":/tests/e2e.sql", str(upstream_fixture))
        original = upstream_fixture.read_bytes()
        original_sha = hashlib.sha256(original).hexdigest()
        assert original_sha == "b72d8aa270369db31dfc2333c120ea6873eaa6d15fad0ac3398800c9b9d607f0", original_sha
        assert b"CREATE EXTENSION pg_trickle VERSION '0.108.3';" in original
        assert b"VERSION '0.105.1'" not in original and b"0.108.0" not in original
        observations["mdm_fixture"] = {
            "source_revision": source_revision, "upstream_sha256": original_sha,
            "pg_trickle_version": "0.108.3",
            "adaptation": "two helper-audit strategy assertions also accept exact affected-record deltas (#1171)"}
        bootstrap = output / "mdm-e2e-bootstrap.sql"
        bootstrap.write_bytes(adapt_bootstrap(original))
        observations["mdm_fixture"]["adapted_sha256"] = hashlib.sha256(bootstrap.read_bytes()).hexdigest()
        fixture("mdm-bootstrap", "/work/" + bootstrap.relative_to(root).as_posix(), "postgres")
        fixture("mdm-policy-fixtures", "/tests/e2e_policy.sql")
        observations["mdm_policy_fixture_cases"] = json.loads(sql(
            "SELECT jsonb_agg(to_jsonb(c) ORDER BY case_key) FROM mdm_steward.policy_cases_v1 c", "foundation"))
        observations["mdm_policy_fixture_receipts"] = json.loads(sql(
            "SELECT jsonb_agg(to_jsonb(r) ORDER BY created_at,receipt_id) FROM mdm_steward.policy_receipts_v1 r", "foundation"))
        observations["fixture_sha256"] = ["{}  {}".format(original_sha, upstream_fixture)] + command("docker", "exec", container, "sha256sum",
            "/tests/e2e_policy.sql", "/usr/lib/postgresql/18/lib/pg_react.so",
            "/usr/lib/postgresql/18/lib/pg_mdm.so", "/usr/lib/postgresql/18/lib/pg_trickle.so").splitlines()
        sql("ALTER DATABASE foundation ALLOW_CONNECTIONS false", "postgres")
        sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='foundation'", "postgres")
        try:
            for database in ("s1_upgrade",) if maintenance else ():
                command("docker", "exec", container, "createdb", "-U", "postgres",
                        "--template=foundation", database)
        finally:
            sql("ALTER DATABASE foundation ALLOW_CONNECTIONS true", "postgres")
        sql("CREATE EXTENSION pg_react VERSION '0.46.1'", "foundation")
        if maintenance:
            sql("CREATE EXTENSION pg_react VERSION '0.46.0'", "s1_upgrade")
            fixture("s1-populated-upgrade", "/work/tests/integrations/pg-mdm/time/maintenance.sql",
                    "s1_upgrade", ("-v", "upgrade=true", "-v", "direct=false", "-v", "setup_roles=true"))
            observations["maintenance"] = {"s1_upgrade": json.loads(sql(
                "SELECT jsonb_build_object('before',to_jsonb(b),'after',to_jsonb(a),'result',r.vector) "
                "FROM claim_maintenance.before_upgrade b, claim_maintenance.after_upgrade a, "
                "claim_maintenance.result r", "s1_upgrade"))}
        for name in ("v0.47", "v0.47-typed", "v0.48-codec"):
            fixture(name, "/work/tests/integrations/pg-mdm/" + name + ".sql", "postgres")
        for name in ("v0.47-live-setup", "v0.47-live", "v0.48-codec", "v0.48-live", "v0.48-security"):
            fixture(name, "/work/tests/integrations/pg-mdm/" + name + ".sql")
        observations["foundation_receipts"] = json.loads(sql(
            "SELECT jsonb_agg(to_jsonb(r) ORDER BY created_at,receipt_id) FROM mdm_steward.policy_receipts_v1 r", "foundation"))
        if maintenance:
            for database in ("core_claim",):
                command("docker", "exec", container, "createdb", "-U", "postgres", database)
                sql("CREATE EXTENSION pg_trickle; CREATE EXTENSION pg_react VERSION '0.46.1'", database)
                fixture(database, "/work/tests/integrations/pg-mdm/time/maintenance.sql", database,
                        ("-v", "upgrade=false", "-v", "direct=true", "-v", "setup_roles=true"))
                observations["maintenance"][database] = json.loads(sql(
                    "SELECT jsonb_build_object('before',to_jsonb(b),'after',to_jsonb(a),'result',r.vector) "
                    "FROM claim_maintenance.before_upgrade b, claim_maintenance.after_upgrade a, "
                    "claim_maintenance.result r", database))
            assert observations["maintenance"]["core_claim"]["after"]["api"] == observations["maintenance"]["s1_upgrade"]["after"]["api"], "fresh/upgrade public callable inventories differ"
        command("docker", "exec", "--user", "postgres", container, "sh",
                "/work/tests/integrations/pg-mdm/v0.48-admission.sh", "s1_missing_mdm")
        observations["checks"].append({"name": "missing-mdm-admission", "result": "passed"})
        boundary_cases = json.loads(sql(
            "SELECT jsonb_agg(to_jsonb(c) ORDER BY c.case_key) FROM mdm_steward.policy_cases_v1 c", "foundation"))
        boundary_receipts = json.loads(sql(
            "SELECT jsonb_agg(to_jsonb(r) ORDER BY created_at,receipt_id) "
            "FROM mdm_steward.policy_receipts_v1 r", "foundation"))
        sql("ALTER DATABASE foundation ALLOW_CONNECTIONS false", "postgres")
        sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='foundation'", "postgres")
        try:
            command("docker", "exec", container, "createdb", "-U", "postgres",
                    "--template=foundation", "s1_boundary")
        finally:
            sql("ALTER DATABASE foundation ALLOW_CONNECTIONS true", "postgres")
        observations["s1_boundary_clone_capture_recovery"] = sql(
            "SELECT pgtrickle.recover_capture_instance()", "s1_boundary")
        assert json.loads(sql(
            "SELECT jsonb_agg(to_jsonb(c) ORDER BY c.case_key) "
            "FROM mdm_steward.policy_cases_v1 c", "s1_boundary")) == boundary_cases
        assert json.loads(sql(
            "SELECT jsonb_agg(to_jsonb(r) ORDER BY created_at,receipt_id) "
            "FROM mdm_steward.policy_receipts_v1 r", "s1_boundary")) == boundary_receipts
        sql("DROP EXTENSION pg_react CASCADE; CREATE EXTENSION pg_react VERSION '0.46.1'", "s1_boundary")
        observations["s1_boundary_pg_react_reset"] = "isolated boundary samples from inherited MDM and React state"
        fixture("sample-boundaries", "/work/tests/integrations/pg-mdm/time/boundaries.sql", "s1_boundary")
        container = "pgreact-s1-" + uuid.uuid4().hex[:12]
        active_database[0] = "s1_idle"
        command("docker", "run", "--detach", "--name", container, "--network", "none",
                "--env", "POSTGRES_HOST_AUTH_METHOD=trust", "--volume", str(root) + ":/work:ro",
                image, "postgres", "-c", "shared_preload_libraries=pg_trickle,pg_react",
                "-c", "pg_trickle.enabled=off", "-c", "pg_trickle.cdc_mode=trigger",
                "-c", "pg_trickle.differential_max_change_ratio=1.0",
                "-c", "max_locks_per_transaction=256", "-c", "shared_buffers=8MB",
                "-c", "max_connections=20", "-c", "deadlock_timeout=1s",
                "-c", "log_lock_waits=on")
        containers.append(container)
        print("CONTAINER: " + container, flush=True)
        for _ in range(60):
            result = subprocess.run(["docker", "exec", container, "pg_isready", "-U", "postgres"],
                                    capture_output=True)
            if result.returncode == 0 and "PostgreSQL init process complete" in command("docker", "logs", container):
                break
            time.sleep(1)
        else:
            raise RuntimeError("fresh S1 PostgreSQL readiness exceeded 60 seconds")
        s1_settings_rows = json.loads(sql(
            "SELECT jsonb_agg(jsonb_build_object('name',name,'setting',setting,'unit',unit) "
            "ORDER BY name) FROM pg_settings WHERE name = ANY(ARRAY["
            "'shared_preload_libraries','pg_trickle.enabled','pg_trickle.cdc_mode',"
            "'pg_trickle.differential_max_change_ratio','max_locks_per_transaction',"
            "'shared_buffers','max_connections','deadlock_timeout','log_lock_waits'])", "postgres"))
        s1_settings = {row["name"]: row for row in s1_settings_rows}
        observations["s1_idle_postgres_settings"] = s1_settings
        assert s1_settings["shared_preload_libraries"]["setting"].replace(" ", "") == "pg_trickle,pg_react", s1_settings
        assert s1_settings["pg_trickle.enabled"]["setting"] == "off", s1_settings
        assert s1_settings["pg_trickle.cdc_mode"]["setting"] == "trigger", s1_settings
        assert float(s1_settings["pg_trickle.differential_max_change_ratio"]["setting"]) == 1.0, s1_settings
        assert int(s1_settings["max_locks_per_transaction"]["setting"]) == 256, s1_settings
        assert int(s1_settings["shared_buffers"]["setting"]) * 8192 == 8 * 1024 * 1024, s1_settings
        assert int(s1_settings["max_connections"]["setting"]) == 20, s1_settings
        assert int(s1_settings["deadlock_timeout"]["setting"]) == 1000, s1_settings
        assert s1_settings["log_lock_waits"]["setting"] == "on", s1_settings
        s1_source_revision = command("docker", "image", "inspect", image,
            "--format", '{{ index .Config.Labels "pg_mdm.source_revision" }}').strip()
        assert s1_source_revision == source_revision
        s1_e2e_source = output / "mdm-e2e-s1-idle-upstream.sql"
        command("docker", "cp", container + ":/tests/e2e.sql", str(s1_e2e_source))
        s1_e2e_bytes = s1_e2e_source.read_bytes()
        s1_e2e_sha = hashlib.sha256(s1_e2e_bytes).hexdigest()
        assert s1_e2e_sha == original_sha
        assert b"CREATE DATABASE foundation;" in s1_e2e_bytes
        assert b"\\connect foundation" in s1_e2e_bytes
        s1_e2e_adapted = adapt_bootstrap(s1_e2e_bytes).replace(b"CREATE DATABASE foundation;", b"CREATE DATABASE s1_idle;")
        s1_e2e_adapted = s1_e2e_adapted.replace(b"\\connect foundation", b"\\connect s1_idle")
        s1_e2e_path = output / "mdm-e2e-s1-idle.sql"
        s1_e2e_path.write_bytes(s1_e2e_adapted)
        s1_e2e_adapted_sha = hashlib.sha256(s1_e2e_adapted).hexdigest()
        fixture("s1-idle-mdm-bootstrap", "/work/" + s1_e2e_path.relative_to(root).as_posix(), "postgres")
        s1_policy_source = output / "mdm-e2e-policy-s1-idle-upstream.sql"
        command("docker", "cp", container + ":/tests/e2e_policy.sql", str(s1_policy_source))
        s1_policy_bytes = s1_policy_source.read_bytes()
        s1_policy_sha = hashlib.sha256(s1_policy_bytes).hexdigest()
        assert b"\\connect foundation" in s1_policy_bytes
        s1_policy_adapted = s1_policy_bytes.replace(b"\\connect foundation", b"\\connect s1_idle")
        s1_policy_path = output / "mdm-e2e-policy-s1-idle.sql"
        s1_policy_path.write_bytes(s1_policy_adapted)
        s1_policy_adapted_sha = hashlib.sha256(s1_policy_adapted).hexdigest()
        fixture("s1-idle-mdm-policy-fixtures", "/work/" + s1_policy_path.relative_to(root).as_posix(), "s1_idle")
        observations["s1_idle_fresh_database"] = {
            "container": container, "image_id": observations["image_id"],
            "mdm_source_revision": s1_source_revision,
            "bootstrap_upstream_sha256": s1_e2e_sha,
            "bootstrap_adapted_sha256": s1_e2e_adapted_sha,
            "policy_upstream_sha256": s1_policy_sha,
            "policy_adapted_sha256": s1_policy_adapted_sha,
            "adaptation": "fresh container and s1_idle database; only psql database switches and CREATE DATABASE target are renamed"}
        observations["fixture_sha256"].extend([
            "{}  {}".format(s1_e2e_sha, s1_e2e_source),
            "{}  {}".format(s1_e2e_adapted_sha, s1_e2e_path),
            "{}  {}".format(s1_policy_sha, s1_policy_source),
            "{}  {}".format(s1_policy_adapted_sha, s1_policy_path)])
        observations["s1_idle_policy_fixture_cases"] = json.loads(sql(
            "SELECT jsonb_agg(to_jsonb(c) ORDER BY case_key) FROM mdm_steward.policy_cases_v1 c", "s1_idle"))
        observations["s1_idle_policy_fixture_receipts"] = json.loads(sql(
            "SELECT jsonb_agg(to_jsonb(r) ORDER BY created_at,receipt_id) FROM mdm_steward.policy_receipts_v1 r", "s1_idle"))
        sql("CREATE EXTENSION pg_react VERSION '0.46.1'", "s1_idle")
        setup_source = root / "tests/integrations/pg-mdm/v0.47-live-setup.sql"
        setup_bytes = setup_source.read_bytes()
        setup_sha = hashlib.sha256(setup_bytes).hexdigest()
        assert b"\\connect foundation" in setup_bytes
        setup_adapted = setup_bytes.replace(b"\\connect foundation", b"\\connect s1_idle")
        setup_path = output / "v0.47-live-setup-s1-idle.sql"
        setup_path.write_bytes(setup_adapted)
        setup_adapted_sha = hashlib.sha256(setup_adapted).hexdigest()
        fixture("s1-v0.47-live-setup", "/work/" + setup_path.relative_to(root).as_posix(), "s1_idle")
        observations["fixture_sha256"].extend([
            "{}  {}".format(setup_sha, setup_source),
            "{}  {}".format(setup_adapted_sha, setup_path)])
        for name in ("v0.47-live", "v0.48-codec", "v0.48-live", "v0.48-security"):
            fixture("s1-" + name, "/work/tests/integrations/pg-mdm/" + name + ".sql", "s1_idle")
        observations["s1_idle_compatibility_fixtures"] = [
            "v0.47-live-setup", "v0.47-live", "v0.48-codec", "v0.48-live", "v0.48-security"]
        sql("DROP EXTENSION pg_react CASCADE; CREATE EXTENSION pg_react VERSION '0.46.1'", "s1_idle")
        observations["s1_idle_pg_react_reset"] = "reset live-fixture React runtime state before S1 setup"
        observations["s1_idle_pre_workload_receipts"] = json.loads(sql(
            "SELECT jsonb_agg(to_jsonb(r) ORDER BY created_at,receipt_id) FROM mdm_steward.policy_receipts_v1 r", "s1_idle"))
        fixture("s1-workload-population", "/work/tests/integrations/pg-mdm/time/population.sql",
                "s1_idle", ("-v", "case_count=1000"))
        observations["workload_population"] = json.loads(sql(
            "SELECT to_jsonb(c) FROM public.s1_workload_config c", "s1_idle"))
        observations["mdm_entry_cases"] = json.loads(sql(
            "SELECT jsonb_agg(to_jsonb(c) ORDER BY case_key) FROM mdm_steward.policy_cases_v1 c", "s1_idle"))
        observations["mdm_entry_receipts"] = observations["s1_idle_pre_workload_receipts"]
        fixture("idle-setup", "/work/tests/integrations/pg-mdm/time/idle.sql", "s1_idle")
        observations["registered_s1_rules"] = json.loads(sql(
            "SELECT COALESCE(jsonb_agg(jsonb_build_object('rule_name',r.rule_name,'rule_id',r.rule_id,"
            "'rule_version_id',v.rule_version_id,'match_name',v.match_name) ORDER BY r.rule_name),'[]') "
            "FROM pgreact_internal.rules r JOIN pgreact_internal.rule_versions v USING(rule_id) "
            "WHERE r.rule_name IN ('s1-callback-failure','s1-future-idle')"))
        observations["callback_failure_rule_ids"] = json.loads(sql(
            "SELECT COALESCE(jsonb_agg(to_jsonb(f) ORDER BY rule_id),'[]') "
            "FROM s1_audit.callback_failure_rules f"))
        assert [row["rule_name"] for row in observations["registered_s1_rules"]] == [
            "s1-callback-failure", "s1-future-idle"], observations["registered_s1_rules"]
        assert len(observations["callback_failure_rule_ids"]) == 1, observations["callback_failure_rule_ids"]
        assert observations["callback_failure_rule_ids"][0]["rule_id"] == next(
            row["rule_id"] for row in observations["registered_s1_rules"]
            if row["rule_name"] == "s1-callback-failure")
        observations["installed_extensions"] = json.loads(sql(
            "SELECT jsonb_agg(jsonb_build_object('name',extname,'version',extversion) ORDER BY extname) "
            "FROM pg_extension"))
        observations["server_version_num"] = int(sql("SELECT current_setting('server_version_num')"))
        observations["installed_functions"] = json.loads(sql(
            "SELECT jsonb_agg(jsonb_build_object('identity',p.oid::regprocedure::text,"
            "'owner',pg_get_userbyid(p.proowner),'security_definer',p.prosecdef,'acl',p.proacl::text,"
            "'result',pg_get_function_result(p.oid),"
            "'definition_sha256',encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex')) "
            "ORDER BY p.oid::regprocedure::text) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
            "WHERE n.nspname IN ('pgreact_api','pgreact_mdm','mdm_steward') AND p.prokind='f' "
            "AND p.proname IN ('author_deadline_rule','author_temporal_rule','validate_deadline_rule',"
            "'run','managed_status','managed_cycle','deadline_history','temporal_status','temporal_history',"
            "'submit_policy_intent','submit_escalation_intent','submit_intent_as_worker')"))
        before = cases()
        source = json.loads(sql("SELECT jsonb_agg(to_jsonb(s) ORDER BY s.id) "
                                "FROM public.policy_qualification_source s"))
        temporal_source = json.loads(sql("SELECT jsonb_agg(to_jsonb(s) ORDER BY s.case_key) "
                                         "FROM s1_audit.source s"))
        assert len(temporal_source) == 1, temporal_source
        case_key = temporal_source[0]["case_key"]
        deadline = timestamp(temporal_source[0]["deadline"])
        expected = copy.deepcopy(before)
        for row in expected:
            if row["case_key"] == case_key:
                assert row["escalation_level"] == 1 and row["due_at"] == temporal_source[0]["deadline"]
                row["escalation_level"] = 2
                row["action_revision"] += 1
                expected_revision = row["action_revision"]
        expected_control = json.loads(sql(
            "SELECT jsonb_build_object('assigned_queue',c.assigned_queue,'due_at',c.due_at::text,"
            "'escalation_level',2,'manual_assignment_protected',c.manual_assignment_protected) "
            "FROM mdm_steward.policy_cases_v1 c WHERE c.case_key=" + str(case_key)))
        binding = json.loads(sql("SELECT jsonb_build_object('binding_id',binding_id,"
            "'binding_version',binding_version,'policy_digest',encode(policy_digest,'hex')) "
            "FROM pgreact_mdm.policy_intent_bindings "
            "WHERE policy_revision='s1-idle-feasibility' AND enabled"))
        uuid.UUID(binding["binding_id"])
        assert deadline > datetime.datetime.now(datetime.timezone.utc), temporal_source
        observations.update(cases_before=before, business_source=source,
                            temporal_source=temporal_source, cases_expected=expected)
        sql("ALTER SYSTEM SET pg_react.databases = 's1_idle'")
        sql("ALTER SYSTEM SET pg_react.worker_role = 'mdm_s1_runtime'")
        sql("ALTER SYSTEM SET pg_react.batch_size = '" + str(batch_size) + "'")
        command("docker", "restart", container)
        end = time.monotonic() + 120
        receipts = []
        while time.monotonic() < end:
            ready = subprocess.run(["docker", "exec", container, "pg_isready", "-U", "postgres"],
                                   capture_output=True)
            if ready.returncode:
                time.sleep(0.2)
                continue
            receipts = json.loads(sql("SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY r.receipt_id), '[]') "
                "FROM mdm_steward.policy_receipts_v1 r WHERE request_body->>'policy_revision'='s1-idle-feasibility'"))
            status = json.loads(sql("SELECT pgreact_api.managed_status()"))
            detail = (status.get("process") or {}).get("detail") or ""
            if receipts or (detail.startswith("42702:") and '"rule_version_id" is ambiguous' in detail):
                break
            if detail.startswith("permission denied: must be owner of stream table ") and \
                    datetime.datetime.now(datetime.timezone.utc) >= deadline:
                break
            failed = sql("SELECT EXISTS (SELECT 1 FROM pgreact.attempts "
                         "WHERE name='s1-future-idle' AND status='FAILED' "
                         "AND error_message <> 'injected registered S1 callback failure')")
            if failed == "t" and datetime.datetime.now(datetime.timezone.utc) >= deadline:
                break
            time.sleep(0.2)
        observations["runtime_settings"] = json.loads(sql(
            "SELECT jsonb_object_agg(name,setting) FROM pg_settings WHERE name IN "
            "('pg_react.databases','pg_react.worker_role','pg_react.poll_interval_ms',"
            "'pg_react.batch_size','pg_react.max_pending_jobs','max_worker_processes')"))
        observations["managed_status"] = json.loads(sql("SELECT pgreact_api.managed_status()"))
        observations["work"] = json.loads(sql("SELECT COALESCE(jsonb_agg(to_jsonb(w) ORDER BY work_id),'[]') "
                                              "FROM pgreact.work w WHERE name='s1-future-idle'"))
        observations["attempts"] = json.loads(sql("SELECT COALESCE(jsonb_agg(to_jsonb(a) ORDER BY execution_id),'[]') "
                                                  "FROM pgreact.attempts a WHERE name='s1-future-idle'"))
        if not receipts and ((observations["managed_status"].get("process") or {}).get("detail") or "").startswith("42702:"):
            query = "SELECT * FROM pgreact.claim('s1-independent-diagnostic',32,interval '60 seconds',NULL::text[])"
            failure = subprocess.run(["docker", "exec", container, "psql", "-XAtq", "-U", "postgres",
                "-d", "s1_idle", "-v", "VERBOSITY=verbose", "-c", query], capture_output=True, text=True)
            observations["generic_claim_failure"] = {"query": query, "exit": failure.returncode,
                                                    "stdout": failure.stdout, "stderr": failure.stderr}
            assert failure.returncode != 0 and "42702" in failure.stderr, failure.stderr
            assert 'column reference "rule_version_id" is ambiguous' in failure.stderr, failure.stderr
            assert "pgreact.claim(text,integer,interval,text[])" in failure.stderr, failure.stderr
            observations["generic_claim_definition"] = sql(
                "SELECT pg_get_functiondef('pgreact.claim(text,integer,interval,text[])'::regprocedure)")
            observations["cases_actual"] = cases()
            assert observations["cases_actual"] == before, "generic STOP changed the complete MDM case population"
            assert json.loads(sql("SELECT jsonb_agg(to_jsonb(s) ORDER BY s.id) "
                                  "FROM public.policy_qualification_source s")) == source
            assert json.loads(sql("SELECT jsonb_agg(to_jsonb(s) ORDER BY s.case_key) "
                                  "FROM s1_audit.source s")) == temporal_source
            observations["receipts_actual"] = json.loads(sql(
                "SELECT jsonb_agg(to_jsonb(r) ORDER BY created_at,receipt_id) FROM mdm_steward.policy_receipts_v1 r"))
            assert observations["receipts_actual"] == observations["mdm_entry_receipts"], "generic STOP changed retained public receipts"
            observations["entry_decision"] = "STOP"
            observations["stop_reason"] = "The installed generic managed claim operation fails before dispatch; working idle-time support is unavailable on this exact artifact."
            observations["conditional_work_not_performed"] = ["1000-case due/churn measurements", "numeric envelope/sign-off",
                "downstream S2/S3 implementation and qualification"]
            observations["checks"].append({"name": "generic-claim-unavailability", "result": "confirmed",
                                            "sqlstate": "42702"})
            print("STOP: independently confirmed generic managed claim defect 42702; re-estimation required", flush=True)
            return
        if not receipts:
            observations["cases_actual"] = cases()
            observations["receipts_actual"] = json.loads(sql(
                "SELECT jsonb_agg(to_jsonb(r) ORDER BY created_at,receipt_id) FROM mdm_steward.policy_receipts_v1 r"))
            assert observations["cases_actual"] == before, "blocked worker changed MDM cases"
            assert observations["receipts_actual"] == observations["mdm_entry_receipts"], "blocked worker changed MDM receipts"
            assert json.loads(sql("SELECT jsonb_agg(to_jsonb(s) ORDER BY s.id) "
                                  "FROM public.policy_qualification_source s")) == source
            assert json.loads(sql("SELECT jsonb_agg(to_jsonb(s) ORDER BY s.case_key) "
                                  "FROM s1_audit.source s")) == temporal_source
            observations["blocked_populations_unchanged"] = True
        assert len(receipts) == 1, {key: observations[key] for key in ("runtime_settings", "managed_status", "work", "attempts")}
        receipt = receipts[0]
        assert receipt["case_key"] == case_key and receipt["outcome"] == "APPLIED_CONTROL", receipts
        assert receipt["reason_code"] == "CONTROL_APPLIED" and receipt["resulting_publication_revision"] is None, receipts
        assert receipt["actor"] == "pgreact_mdm_worker" and receipt["action_revision"] == expected_revision, receipts
        assert receipt["control"] == expected_control, receipts
        assert receipt["request_body"]["action"] == "ESCALATE" and receipt["request_body"]["arguments"] == {"level": 2}, receipts
        delivered_at = timestamp(receipt["created_at"])
        assert delivered_at >= deadline, receipts
        actual = cases()
        assert actual == expected, {"expected": expected, "actual": actual}
        assert json.loads(sql("SELECT jsonb_agg(to_jsonb(s) ORDER BY s.id) "
                              "FROM public.policy_qualification_source s")) == source
        assert json.loads(sql("SELECT jsonb_agg(to_jsonb(s) ORDER BY s.case_key) "
                              "FROM s1_audit.source s")) == temporal_source
        matches = json.loads(sql("SELECT jsonb_agg(to_jsonb(m)) FROM pgreact.matches m "
                                 "WHERE name='s1-future-idle'"))
        assert len(matches) == 1 and matches[0]["active"] and matches[0]["generation"] == 1, matches
        activation = matches[0]
        uuid.UUID(activation["activation_id"])
        uuid.UUID(activation["rule_version_id"])
        status = json.loads(sql("SELECT pgreact_api.managed_status()"))
        work, attempts = observations["work"], observations["attempts"]
        assert [{k: v for k, v in row.items() if k not in ("work_id", "updated_at")}
                for row in work] == [{"kind": "rule", "name": "s1-future-idle", "version": "1",
                                     "state": "COMPLETED", "claimable": False}], work
        worker = "pg-react-managed/s1_idle/" + str(status["process"]["pid"])
        assert [{k: v for k, v in row.items() if k not in
                 ("execution_id", "episode_id", "started_at", "finished_at")}
                for row in attempts] == [{"attempt_no": 1, "worker_id": worker, "status": "COMPLETED",
                    "error_message": None, "error_code": None, "event_kind": "ACTIVATE",
                    "name": "s1-future-idle"}], attempts
        assert int(work[0]["work_id"]) == attempts[0]["episode_id"] > 0
        assert isinstance(attempts[0]["execution_id"], int) and attempts[0]["execution_id"] > 0
        started_at = timestamp(attempts[0]["started_at"])
        finished_at = timestamp(attempts[0]["finished_at"])
        updated_at = timestamp(work[0]["updated_at"])
        # MDM receipt creation and core attempt timestamps are separate writes.
        assert deadline <= started_at <= finished_at <= updated_at
        prior_case = next(row for row in before if row["case_key"] == case_key)
        expected_body = {
            "action": "ESCALATE", "arguments": {"level": 2}, "case_key": case_key,
            "binding_id": binding["binding_id"], "policy_revision": "s1-idle-feasibility",
            "expected_policy_digest": binding["policy_digest"],
            "expected_evidence_basis_digest": prior_case["evidence_basis_digest"][2:],
            "evaluation_ref": "pgreact:" + activation["activation_id"],
            "work_ref": "pgreact:" + activation["rule_version_id"] + ":" + work[0]["work_id"],
            **{"expected_" + name: prior_case[name] for name in ("review_version", "definition_version",
                "publication_revision", "stewardship_epoch", "action_revision")}}
        request_key = sql("SELECT encode(pgreact_mdm.intent_request_key('{}'::uuid,"
            "'s1-idle-feasibility',{},1,{},'escalate_level_2',2),'hex')".format(
                binding["binding_id"], int(case_key), int(prior_case["action_revision"])))
        request_digest = sql("SELECT encode(pgreact_mdm.intent_request_digest('" +
            json.dumps(expected_body).replace("'", "''") + "'::jsonb),'hex')")
        uuid.UUID(receipt["receipt_id"])
        expected_receipt = {"receipt_id": receipt["receipt_id"], "created_at": receipt["created_at"],
            "actor": "pgreact_mdm_worker", "selected_role_name": "pgreact_mdm_worker",
            "session_role_name": "mdm_s1_runtime", "action": "ESCALATE", "outcome": "APPLIED_CONTROL",
            "reason_code": "CONTROL_APPLIED", "control": expected_control, "case_key": case_key,
            "binding_id": binding["binding_id"], "binding_version": binding["binding_version"],
            "action_revision": expected_revision, "resulting_publication_revision": None,
            "request_body": expected_body, "request_key": "\\x" + request_key,
            "request_digest": "\\x" + request_digest}
        assert receipts == [expected_receipt], {"expected": expected_receipt, "actual": receipts}
        retained_receipts = json.loads(sql("SELECT jsonb_agg(to_jsonb(r) ORDER BY created_at,receipt_id) "
                                          "FROM mdm_steward.policy_receipts_v1 r"))
        assert retained_receipts == observations["mdm_entry_receipts"] + [expected_receipt]
        failure_attempts = []
        end = time.monotonic() + 30
        while time.monotonic() < end:
            failure_attempts = json.loads(sql(
                "SELECT COALESCE(jsonb_agg(to_jsonb(a) ORDER BY execution_id),'[]') "
                "FROM pgreact.attempts a WHERE name='s1-callback-failure'"))
            if failure_attempts:
                break
            time.sleep(0.2)
        observations["all_s1_work"] = json.loads(sql(
            "SELECT COALESCE(jsonb_agg(to_jsonb(w) ORDER BY work_id),'[]') "
            "FROM pgreact.work w WHERE name IN ('s1-callback-failure','s1-future-idle')"))
        observations["all_s1_attempts"] = json.loads(sql(
            "SELECT COALESCE(jsonb_agg(to_jsonb(a) ORDER BY execution_id),'[]') "
            "FROM pgreact.attempts a WHERE name IN ('s1-callback-failure','s1-future-idle')"))
        assert len(failure_attempts) == 1, failure_attempts
        callback_failure = "injected registered S1 callback failure"
        assert {key: failure_attempts[0][key] for key in
                ("attempt_no", "status", "error_code", "error_message", "event_kind", "name")} == {
                    "attempt_no": 1, "status": "FAILED", "error_code": "P0001",
                    "error_message": callback_failure,
                    "event_kind": "ACTIVATE", "name": "s1-callback-failure"}, failure_attempts
        failure_work = json.loads(sql(
            "SELECT COALESCE(jsonb_agg(to_jsonb(w) ORDER BY work_id),'[]') "
            "FROM pgreact.work w WHERE name='s1-callback-failure'"))
        assert [{key: row[key] for key in ("kind", "name", "version", "state", "claimable")}
                for row in failure_work] == [{"kind": "rule", "name": "s1-callback-failure",
                    "version": "1", "state": "FAILED", "claimable": False}], failure_work
        callback_status = json.loads(sql("SELECT pgreact_api.managed_status()"))
        failure_finished = timestamp(failure_attempts[0]["finished_at"])
        after_error_cycle = failure_finished + datetime.timedelta(seconds=1.5)
        end = time.monotonic() + 30
        while time.monotonic() < end:
            callback_status = json.loads(sql("SELECT pgreact_api.managed_status()"))
            heartbeat = timestamp(callback_status["process"]["heartbeat_at"])
            if callback_status["process"]["state"] == "ready" and heartbeat > after_error_cycle:
                break
            time.sleep(0.2)
        assert callback_status["process"]["state"] == "ready", callback_status
        assert timestamp(callback_status["process"]["heartbeat_at"]) > after_error_cycle, callback_status
        logs = subprocess.run(["docker", "logs", "--tail", "1000", container],
                              capture_output=True, text=True, timeout=30)
        assert not logs.returncode, logs.stderr
        worker_logs = logs.stdout + logs.stderr
        callback_identity = ("registered-S1 callback entry rule_id=" +
                             observations["callback_failure_rule_ids"][0]["rule_id"] +
                             " session_user=mdm_s1_runtime "
                             "current_user=mdm_s1_runner")
        identity_at = worker_logs.find(callback_identity)
        assert failure_attempts[0]["error_message"] == callback_failure, failure_attempts
        assert identity_at >= 0, worker_logs
        assert "managed worker must begin each cycle as its configured login" not in worker_logs, worker_logs
        assert "managed cycle must restore its configured login role" not in worker_logs, worker_logs
        assert cases() == actual
        assert json.loads(sql("SELECT jsonb_agg(to_jsonb(r) ORDER BY created_at,receipt_id) "
                              "FROM mdm_steward.policy_receipts_v1 r")) == retained_receipts
        assert json.loads(sql("SELECT jsonb_agg(to_jsonb(s) ORDER BY s.id) "
                              "FROM public.policy_qualification_source s")) == source
        assert json.loads(sql("SELECT jsonb_agg(to_jsonb(s) ORDER BY s.case_key) "
                              "FROM s1_audit.source s")) == temporal_source
        observations["callback_error_reset"] = {"managed_worker": True,
            "failed_registered_callback": "s1_audit.deliver(pgreact.activation_context,s1_audit.candidate)",
            "failure_attempts": failure_attempts, "failure_work": failure_work,
            "callback_identity": callback_identity, "post_error_managed_status": callback_status,
            "subsequent_cycle_after_error": True,
            "worker_role_identity": {"actor": receipt["actor"],
                "selected_role_name": receipt["selected_role_name"],
                "session_role_name": receipt["session_role_name"]},
            "public_state_unchanged": True, "transaction_path": "BackgroundWorker::transaction"}
        observations["checks"].append({"name": "callback-error-role-reset", "result": "passed"})
        assert json.loads(sql("SELECT jsonb_agg(to_jsonb(r) ORDER BY created_at,receipt_id) "
                              "FROM mdm_steward.policy_receipts_v1 r")) == retained_receipts
        sql("CREATE ROLE mdm_s1_runtime_no_set LOGIN")
        sql("GRANT CONNECT ON DATABASE s1_idle TO mdm_s1_runtime_no_set")
        sql("GRANT USAGE ON SCHEMA pgreact_api TO mdm_s1_runtime_no_set")
        sql("ALTER SYSTEM SET pg_react.worker_role = 'mdm_s1_runtime_no_set'")
        sql("ALTER SYSTEM SET pg_react.databases = 's1_idle'")
        command("docker", "restart", container)
        end = time.monotonic() + 20
        rejection_lines = []
        while time.monotonic() < end:
            ready = subprocess.run(["docker", "exec", container, "pg_isready", "-U", "postgres"],
                                   capture_output=True)
            if not ready.returncode:
                logs = subprocess.run(["docker", "logs", "--tail", "200", container],
                                      capture_output=True, text=True, timeout=30)
                rejection_lines = [line for line in (logs.stdout + logs.stderr).splitlines()
                                   if "managed worker role must be a unique SET-capable function grantee" in line]
                if rejection_lines:
                    break
            time.sleep(0.2)
        assert rejection_lines, "worker without SET membership was not rejected by the managed path"
        assert sql("SELECT pg_catalog.pg_has_role('mdm_s1_runtime_no_set',"
                   "'pgreact_mdm_worker','SET')") == "f"
        assert sql("SELECT current_setting('pg_react.worker_role')") == "mdm_s1_runtime_no_set"
        assert json.loads(sql("SELECT jsonb_agg(to_jsonb(r) ORDER BY created_at,receipt_id) "
                              "FROM mdm_steward.policy_receipts_v1 r")) == retained_receipts
        assert cases() == actual
        assert json.loads(sql("SELECT jsonb_agg(to_jsonb(s) ORDER BY s.id) "
                              "FROM public.policy_qualification_source s")) == source
        assert json.loads(sql("SELECT jsonb_agg(to_jsonb(s) ORDER BY s.case_key) "
                              "FROM s1_audit.source s")) == temporal_source
        assert json.loads(sql("SELECT COALESCE(jsonb_agg(to_jsonb(w) ORDER BY work_id),'[]') "
                              "FROM pgreact.work w WHERE name='s1-future-idle'")) == work
        assert json.loads(sql("SELECT COALESCE(jsonb_agg(to_jsonb(a) ORDER BY execution_id),'[]') "
                              "FROM pgreact.attempts a WHERE name='s1-future-idle'")) == attempts
        observations["no_set_membership"] = {"worker_login": "mdm_s1_runtime_no_set",
            "configured_role": "pgreact_mdm_worker", "set_membership": False,
            "rejection": rejection_lines[-1], "public_state_unchanged": True}
        observations["checks"].append({"name": "worker-missing-set-membership", "result": "passed"})
        observations.update(receipt_expected=expected_receipt, matches=matches, idle_binding=binding)
        observations.update(cases_actual=actual, receipts=receipts,
                            deadline_to_receipt_seconds=(delivered_at - deadline).total_seconds(),
                            managed_status=json.loads(sql("SELECT pgreact_api.managed_status()")),
                            deadline_history=json.loads(sql("SELECT pgreact_api.deadline_history('s1-future-idle')")))
        observations["checks"].append({"name": "future-idle-mdm-receipt", "result": "passed"})

        observations["workload"] = {
            "declared": {"cohort": "issue8-s1-baseline", "open_cases": 1000,
                         "simultaneous_due_burst": 1000,
                         "new_cases_per_minute": churn_per_minute,
                         "churn_window_minutes": churn_window_minutes,
                         "churn_case_count": churn_case_count,
                         "case_admission_level": 0, "due_burst_pre_action_level": 1,
                         "expected_action": "ESCALATE",
                         "expected_level": 2, "admission_role": "mdm_legacy_administrator",
                         "worker_login": "mdm_s1_runtime",
                         "worker_set_role": "pgreact_mdm_worker"},
            "metrics_before": database_metrics()}
        fixture("s1-workload-input", "/work/tests/integrations/pg-mdm/time/workload.sql",
                "s1_idle", ("-v", "case_count=1000", "-v",
                            "churn_per_minute={}".format(churn_per_minute)))
        sql("ALTER SYSTEM SET pg_react.worker_role = 'mdm_s1_runtime'")
        sql("ALTER SYSTEM SET pg_react.databases = 's1_idle'")
        command("docker", "restart", container)
        for _ in range(60):
            ready = subprocess.run(["docker", "exec", container, "pg_isready", "-U", "postgres"],
                                   capture_output=True)
            if ready.returncode == 0:
                break
            time.sleep(1)
        else:
            raise RuntimeError("managed worker restart exceeded 60 seconds")
        assert sql("SELECT current_setting('pg_react.worker_role')") == "mdm_s1_runtime"
        route_plan_started = time.monotonic()
        route_plan_query = (
            "LOAD 'auto_explain'; "
            "SET auto_explain.log_min_duration=0; "
            "SET auto_explain.log_analyze=on; "
            "SET auto_explain.log_nested_statements=on; "
            "SET auto_explain.log_buffers=on; "
            "SET auto_explain.log_level='notice'; "
            "SELECT count(*) FROM pgreact_mdm.route_cases("
            "'pgreact_mdm.authorized_policy_cases_v1'::regclass, "
            "'s1-idle-feasibility', NULL::text)")
        route_plan_result = run_docker_exec_with_launch_retry(
            ["docker", "exec", container, "psql", "-XAtq", "-U", "postgres",
             "-d", active_database[0], "-v", "ON_ERROR_STOP=1", "-c", route_plan_query],
            on_retry=record_docker_exec_retry)
        if route_plan_result.returncode:
            raise RuntimeError(route_plan_query + "\n" + route_plan_result.stderr)
        route_plan_count = int(route_plan_result.stdout.strip().splitlines()[-1])
        route_plan_log = "psql stderr (auto_explain NOTICE)"
        route_plan = route_plan_result.stderr + route_plan_result.stdout
        plan_path = output / "route-cases-plan.log"
        plan_path.write_text(route_plan)
        observations["workload"]["route_query_plan"] = {
            "rows": route_plan_count,
            "seconds": time.monotonic() - route_plan_started,
            "path": plan_path.name,
            "server_log": route_plan_log,
            "nested_plan_captured": "WITH source_rows AS" in route_plan,
            "sha256": hashlib.sha256(route_plan.encode()).hexdigest()}
        assert route_plan_count >= 1000, route_plan_count
        assert "WITH source_rows AS" in route_plan, {
            "capture": route_plan_log, "tail": route_plan[-4000:]}
        observations["workload"]["configuration"] = json.loads(sql(
            "SELECT to_jsonb(c) FROM s1_audit.workload_config c"))
        observations["workload"]["configuration"]["managed_batch_size"] = batch_size
        burst_cases = json.loads(sql(
            "SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY c.case_key),'[]') "
            "FROM s1_audit.workload_cases c"))
        assert len(burst_cases) == 1000, len(burst_cases)
        observations["workload"]["cases_before_action"] = burst_cases
        burst_keys = {case["case_key"] for case in burst_cases}
        due_at = timestamp(observations["workload"]["configuration"]["due_at"])
        sample_query = "SELECT jsonb_build_object(" \
            "'captured_at', clock_timestamp(), " \
            "'receipt_count', (SELECT count(*) FROM mdm_steward.policy_receipts_v1 r " \
            "JOIN s1_audit.workload_cases c USING (case_key) WHERE r.action='ESCALATE'), " \
            "'completed_work', (SELECT count(*) FROM pgreact.work WHERE name='s1-future-idle' AND state='COMPLETED'), " \
            "'pending_work', (SELECT count(*) FROM pgreact.work WHERE name='s1-future-idle' AND state IN ('PENDING','RETRY_WAIT','LEASED')), " \
            "'claimable_work', (SELECT count(*) FROM pgreact.work WHERE name='s1-future-idle' AND claimable), " \
            "'attempt_count', (SELECT count(*) FROM pgreact.attempts WHERE name='s1-future-idle'), " \
            "'wal_bytes', (SELECT wal_bytes FROM pg_stat_wal), " \
            "'database_bytes', pg_database_size(current_database()), " \
            "'rows_changed', (SELECT tup_inserted+tup_updated+tup_deleted FROM pg_stat_database " \
            "WHERE datname=current_database()))"
        observations["workload"]["queue_samples"] = []
        metrics_before_due_burst = database_metrics()
        observations["workload"]["metrics_before_due_burst"] = metrics_before_due_burst
        due_started = time.monotonic()
        while True:
            sample = json.loads(sql(sample_query))
            observations["workload"]["queue_samples"].append(sample)
            if sample["receipt_count"] >= 1000:
                break
            if time.monotonic() - due_started > 900:
                receipts_at_timeout = json.loads(sql(
                    "SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY r.created_at,r.receipt_id),'[]') "
                    "FROM mdm_steward.policy_receipts_v1 r "
                    "JOIN s1_audit.workload_cases c USING (case_key)"))
                cases_at_timeout = json.loads(sql(
                    "SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY c.case_key),'[]') "
                    "FROM mdm_steward.policy_cases_v1 c "
                    "JOIN s1_audit.workload_cases w USING (case_key)"))
                observations["workload"].update(
                    cases_at_timeout=cases_at_timeout,
                    receipts_at_timeout=receipts_at_timeout,
                    episode_attempts_at_timeout=workload_episode_attempts(),
                    due_to_receipt_seconds_at_timeout=[
                        (timestamp(row["created_at"]) - due_at).total_seconds()
                        for row in receipts_at_timeout])
                raise RuntimeError("1000-case due-burst receipt synchronization exceeded 900 seconds")
            time.sleep(0.5)
        observations["workload"]["due_drain_sample"] = sample
        assert sample["pending_work"] == 0 and sample["claimable_work"] == 0, sample
        workload_receipts = json.loads(sql(
            "SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY r.created_at,r.receipt_id),'[]') "
            "FROM mdm_steward.policy_receipts_v1 r JOIN s1_audit.workload_cases c USING (case_key)"))
        workload_cases_after = json.loads(sql(
            "SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY c.case_key),'[]') "
            "FROM mdm_steward.policy_cases_v1 c JOIN s1_audit.workload_cases w USING (case_key)"))
        assert len(workload_receipts) == 1000, len(workload_receipts)
        assert {receipt["case_key"] for receipt in workload_receipts} == burst_keys
        assert len({receipt["receipt_id"] for receipt in workload_receipts}) == 1000
        expected_cases_after = []
        for before_case in burst_cases:
            expected_case = dict(before_case["case_state"])
            expected_case["escalation_level"] = before_case["expected_level"]
            expected_case["action_revision"] = int(expected_case["action_revision"]) + 1
            expected_cases_after.append(expected_case)
        assert workload_cases_after == expected_cases_after, {
            "expected": expected_cases_after, "actual": workload_cases_after}
        # Receipt IDs and creation times are runtime-generated; the public contract checks them below.
        expected_workload_receipts = json.loads(sql(
            "WITH policy_binding AS ("
            " SELECT binding_id, binding_version, policy_revision, policy_digest "
            " FROM pgreact_mdm.policy_intent_bindings "
            " WHERE policy_revision='s1-idle-feasibility' AND enabled), "
            "execution AS ("
            " SELECT w.case_key, w.expected_level, w.case_state, b.binding_id, "
            " b.binding_version, b.policy_revision, encode(b.policy_digest,'hex') AS policy_digest, "
            " a.activation_id, a.rule_version_id, e.episode_id, r.receipt_id, r.created_at "
            " FROM s1_audit.workload_cases w CROSS JOIN policy_binding b "
            " JOIN pgreact.rules rule ON rule.rule_name='s1-future-idle' AND rule.state='ACTIVE' "
            " JOIN pgreact.activations a ON a.rule_version_id=rule.rule_version_id "
            " AND a.active AND a.semantic_key=w.case_key "
            " JOIN pgreact.episodes e ON e.rule_version_id=a.rule_version_id "
            " AND e.activation_id=a.activation_id "
            " JOIN mdm_steward.policy_receipts_v1 r ON r.case_key=w.case_key AND r.action='ESCALATE'), "
            "expected_request AS ("
            " SELECT x.*, jsonb_build_object('action','ESCALATE',"
            " 'arguments',jsonb_build_object('level',expected_level),'case_key',case_key,"
            " 'binding_id',binding_id,'policy_revision',policy_revision,"
            " 'expected_policy_digest',policy_digest,"
            " 'expected_evidence_basis_digest',substring(case_state->>'evidence_basis_digest' from 3),"
            " 'evaluation_ref','pgreact:'||activation_id,"
            " 'work_ref','pgreact:'||rule_version_id||':'||episode_id::text,"
            " 'expected_review_version',case_state->'review_version',"
            " 'expected_definition_version',case_state->'definition_version',"
            " 'expected_publication_revision',case_state->'publication_revision',"
            " 'expected_stewardship_epoch',case_state->'stewardship_epoch',"
            " 'expected_action_revision',case_state->'action_revision') AS request_body "
            " FROM execution x) "
            "SELECT COALESCE(jsonb_agg(jsonb_build_object("
            " 'receipt_id',receipt_id,'created_at',created_at,"
            " 'actor','pgreact_mdm_worker','selected_role_name','pgreact_mdm_worker',"
            " 'session_role_name','mdm_s1_runtime','action','ESCALATE',"
            " 'outcome','APPLIED_CONTROL','reason_code','CONTROL_APPLIED',"
            " 'control',jsonb_build_object('assigned_queue',case_state->'assigned_queue',"
            " 'due_at',rtrim(rtrim(to_char((case_state->>'due_at')::timestamptz AT TIME ZONE 'UTC',"
            " 'YYYY-MM-DD HH24:MI:SS.US'),'0'),'.')||'+00','escalation_level',expected_level,"
            " 'manual_assignment_protected',case_state->'manual_assignment_protected'),"
            " 'case_key',case_key,'binding_id',binding_id,'binding_version',binding_version,"
            " 'action_revision',(case_state->>'action_revision')::integer+1,"
            " 'resulting_publication_revision',NULL,'request_body',request_body,"
            " 'request_key',pgreact_mdm.intent_request_key(binding_id,policy_revision,"
            " case_key,binding_version,(case_state->>'action_revision')::bigint,"
            " 'escalate_level_2',expected_level),"
            " 'request_digest',pgreact_mdm.intent_request_digest(request_body)) "
            " ORDER BY created_at,receipt_id),'[]'::jsonb) FROM expected_request"))
        assert workload_receipts == expected_workload_receipts, {
            "expected": expected_workload_receipts, "actual": workload_receipts}
        assert all(str(uuid.UUID(row["receipt_id"])) == row["receipt_id"]
                   for row in workload_receipts)
        due_to_receipt_seconds = [
            (timestamp(row["created_at"]) - due_at).total_seconds()
            for row in workload_receipts]
        assert all(seconds >= 0 for seconds in due_to_receipt_seconds), due_to_receipt_seconds
        due_latency_p95 = sorted(due_to_receipt_seconds)[
            (95 * len(due_to_receipt_seconds) + 99) // 100 - 1]
        metrics_after_due_burst = database_metrics()
        due_resource_delta = {
            "wal_bytes": metrics_after_due_burst["wal_bytes"] - metrics_before_due_burst["wal_bytes"],
            "database_bytes": metrics_after_due_burst["database_bytes"] - metrics_before_due_burst["database_bytes"]}
        observations["workload"].update(
            cases_before_action=burst_cases, cases_after_action=workload_cases_after,
            receipts=workload_receipts,
            episode_attempts=workload_episode_attempts(),
            due_to_receipt_seconds=due_to_receipt_seconds,
            due_latency_p95_seconds=due_latency_p95,
            due_latency_max_seconds=max(due_to_receipt_seconds),
            due_resource_delta_bytes=due_resource_delta,
            metrics_after_due_burst=metrics_after_due_burst)
        latency_check = {"name": "1000-case-due-receipt-latency",
                         "p95_limit_seconds": 150,
                         "max_limit_seconds": 180,
                         "p95_seconds": due_latency_p95,
                         "max_seconds": max(due_to_receipt_seconds),
                         "result": "passed" if due_latency_p95 <= 150 and
                                   max(due_to_receipt_seconds) <= 180 else "failed"}
        observations["checks"].append(latency_check)
        assert latency_check["result"] == "passed", latency_check
        storage_limit_bytes = 10 * 1024 * 1024 * 1024
        storage_growth_bytes = (max(0, due_resource_delta["wal_bytes"])
                                + max(0, due_resource_delta["database_bytes"]))
        resource_check = {"name": "1000-case-due-resource-growth",
                          "storage_limit_bytes": storage_limit_bytes,
                          "observed": due_resource_delta,
                          "storage_growth_bytes": storage_growth_bytes,
                          "result": "passed" if storage_growth_bytes <= storage_limit_bytes else "failed"}
        observations["checks"].append(resource_check)
        assert resource_check["result"] == "passed", resource_check

        churn_started = time.monotonic()
        churn_started_at = timestamp(sql("SELECT clock_timestamp()"))
        churn_case_key_start = int(sql(
            "SELECT COALESCE(max(case_key),0)+1 FROM mdm_steward.policy_cases_v1"))
        churn_source_id_start = int(sql(
            "SELECT COALESCE(max(id),0)+1 FROM public.policy_qualification_source"))
        churn_outputs = []
        churn_progress.touch()
        churn_phase_started = True
        for batch in range(1, churn_case_count + 1):
            target = churn_started + (batch - 1) * 60 / churn_per_minute
            delay = target - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            admit_query = (
                "LOAD 'auto_explain'; "
                "SET auto_explain.log_min_duration='10ms'; "
                "SET auto_explain.log_analyze=on; "
                "SET auto_explain.log_nested_statements=on; "
                "SET auto_explain.log_buffers=on; "
                "SET auto_explain.log_timing=off; "
                "SET auto_explain.log_level='notice'; "
                if profile_case_count is not None else "")
            admit_query += (
                "SET SESSION AUTHORIZATION mdm_legacy_login; SET ROLE mdm_legacy_administrator; "
                "SELECT s1_audit.admit_churn_case({}); RESET ROLE; RESET SESSION AUTHORIZATION".format(batch))
            output_row = (profiled_sql(admit_query) if profile_case_count is not None
                          else sql(admit_query))
            output_record = json.loads(output_row)
            churn_outputs.append(output_record)
            admission = json.loads(sql(
                "SELECT to_jsonb(c) FROM s1_audit.churn_admissions c WHERE c.batch = {}".format(batch)))
            source_rows = admission["source_rows"]
            first_source_id = source_rows[0]["id"]
            suffix = chr(65 + ((batch - 1) // 26)) + chr(65 + ((batch - 1) % 26))
            expected_source_rows = [
                {"id": first_source_id, "display_name": f"Issue 8 Churn {suffix} Alpha",
                 "email_address": f"issue8-churn-{batch}-one@example.test",
                 "updated_at": source_rows[0]["updated_at"]},
                {"id": first_source_id + 1, "display_name": f"Issue 8 Churn {suffix} Alpha",
                 "email_address": f"issue8-churn-{batch}-pair@example.test",
                 "updated_at": source_rows[1]["updated_at"]},
                {"id": first_source_id + 2, "display_name": f"Issue 8 Churn {suffix} Beta",
                 "email_address": f"issue8-churn-{batch}-two@example.test",
                 "updated_at": source_rows[2]["updated_at"]},
                {"id": first_source_id + 3, "display_name": f"Issue 8 Churn {suffix} Beta",
                 "email_address": f"issue8-churn-{batch}-pair@example.test",
                 "updated_at": source_rows[3]["updated_at"]}]
            expected_state = dict(admission["case_state_before"])
            expected_state["due_at"] = admission["due_at"]
            expected_state["escalation_level"] = int(expected_state["escalation_level"]) + 1
            expected_state["action_revision"] = int(expected_state["action_revision"]) + 1
            with churn_progress.open("a") as progress:
                progress.write(json.dumps({
                    "batch": batch,
                    "function_result": output_record,
                    "admission": admission,
                    "expected_case_state": expected_state,
                    "expected_source_rows": expected_source_rows,
                }, sort_keys=True) + "\n")
            churn_completed_batches = batch
        sql("SELECT pgreact_mdm.sync_intent_case_identities()")
        churn_admissions = json.loads(sql(
            "SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY c.batch),'[]') "
            "FROM s1_audit.churn_admissions c"))
        assert len(churn_admissions) == churn_case_count, len(churn_admissions)
        assert len({row["case_key"] for row in churn_admissions}) == churn_case_count
        expected_churn_states = []
        expected_churn_sources = []
        for admission in churn_admissions:
            batch = admission["batch"]
            assert admission["case_key"] == churn_case_key_start + batch - 1, admission
            suffix = chr(65 + ((batch - 1) // 26)) + chr(65 + ((batch - 1) % 26))
            source_rows = admission["source_rows"]
            first_source_id = source_rows[0]["id"]
            assert first_source_id == churn_source_id_start + 4 * (batch - 1), admission
            expected_source_rows = [
                {"id": first_source_id, "display_name": f"Issue 8 Churn {suffix} Alpha",
                 "email_address": f"issue8-churn-{batch}-one@example.test",
                 "updated_at": source_rows[0]["updated_at"]},
                {"id": first_source_id + 1, "display_name": f"Issue 8 Churn {suffix} Alpha",
                 "email_address": f"issue8-churn-{batch}-pair@example.test",
                 "updated_at": source_rows[1]["updated_at"]},
                {"id": first_source_id + 2, "display_name": f"Issue 8 Churn {suffix} Beta",
                 "email_address": f"issue8-churn-{batch}-two@example.test",
                 "updated_at": source_rows[2]["updated_at"]},
                {"id": first_source_id + 3, "display_name": f"Issue 8 Churn {suffix} Beta",
                 "email_address": f"issue8-churn-{batch}-pair@example.test",
                 "updated_at": source_rows[3]["updated_at"]}]
            assert source_rows == expected_source_rows, {
                "batch": batch, "expected": expected_source_rows, "actual": source_rows}
            assert all(timestamp(row["updated_at"]) >= timestamp(admission["started_at"])
                       and timestamp(row["updated_at"]) <= timestamp(admission["completed_at"])
                       for row in source_rows), source_rows
            expected_churn_sources.extend(expected_source_rows)
            expected_state = dict(admission["case_state_before"])
            expected_state["due_at"] = admission["due_at"]
            expected_state["escalation_level"] = int(expected_state["escalation_level"]) + 1
            expected_state["action_revision"] = int(expected_state["action_revision"]) + 1
            assert admission["expected_action"] == "ESCALATE"
            assert admission["expected_level"] == expected_state["escalation_level"] + 1
            assert admission["case_state"] == expected_state, {
                "batch": batch, "expected": expected_state, "actual": admission["case_state"]}
            expected_state["last_observed_at"] = admission["case_state"]["last_observed_at"]
            expected_churn_states.append(expected_state)
        expected_churn_sources.sort(key=lambda row: row["id"])
        expected_churn_states.sort(key=lambda row: row["case_key"])
        assert [row["id"] for row in expected_churn_sources] == list(
            range(churn_source_id_start, churn_source_id_start + 4 * churn_case_count))
        churn_sources_actual = json.loads(sql(
            "SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY s.id),'[]'::jsonb) "
            "FROM public.policy_qualification_source s "
            "JOIN s1_audit.churn_admissions a ON EXISTS ("
            " SELECT 1 FROM jsonb_array_elements(a.source_rows) x WHERE (x->>'id')::bigint=s.id)"))
        assert churn_sources_actual == expected_churn_sources, {
            "expected": expected_churn_sources, "actual": churn_sources_actual}
        churn_cases_actual = json.loads(sql(
            "SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY c.case_key),'[]'::jsonb) "
            "FROM mdm_steward.policy_cases_v1 c "
            "JOIN s1_audit.churn_admissions a USING (case_key)"))
        churn_state_snapshot_at = sql("SELECT clock_timestamp()")
        actual_by_case = {row["case_key"]: row for row in churn_cases_actual}
        admission_times = {
            admission["case_key"]: admission["case_state"]["last_observed_at"]
            for admission in churn_admissions}
        refresh_times = set()
        for case_key, actual in actual_by_case.items():
            observed_at = actual["last_observed_at"]
            admitted_at = admission_times[case_key]
            assert timestamp(observed_at) >= timestamp(admitted_at), {
                "case_key": case_key, "admitted_at": admitted_at,
                "actual_last_observed_at": observed_at}
            if observed_at != admitted_at:
                assert timestamp(observed_at) <= timestamp(churn_state_snapshot_at), {
                    "case_key": case_key, "snapshot_at": churn_state_snapshot_at,
                    "actual_last_observed_at": observed_at}
                refresh_times.add(observed_at)
        assert len(refresh_times) <= 1, sorted(refresh_times)
        for expected_state in expected_churn_states:
            expected_state["last_observed_at"] = actual_by_case[
                expected_state["case_key"]]["last_observed_at"]
        observations["workload"].update(
            churn_case_state_snapshot_at=churn_state_snapshot_at,
            churn_periodic_refresh_timestamps=sorted(refresh_times))
        assert churn_cases_actual == expected_churn_states, {
            "expected": expected_churn_states, "actual": churn_cases_actual}
        assert len(churn_outputs) == churn_case_count, len(churn_outputs)
        for function_output, admission in zip(churn_outputs, churn_admissions):
            assert set(function_output) == {"batch", "case_key", "started_at", "completed_at", "due_at", "refresh"}
            assert {key: function_output[key] for key in ("batch", "case_key", "started_at", "due_at")} == {
                "batch": admission["batch"], "case_key": admission["case_key"],
                "started_at": admission["started_at"], "due_at": admission["due_at"]}
            assert 0 <= (timestamp(function_output["completed_at"])
                         - timestamp(admission["completed_at"])).total_seconds() <= 2
            assert function_output["refresh"]["changed"] is True, function_output
        churn_seconds = (timestamp(churn_admissions[-1]["completed_at"])
                         - churn_started_at).total_seconds()
        assert churn_seconds > 0, churn_seconds
        observations["workload"].update(
            churn_admissions=churn_admissions, churn_function_results=churn_outputs,
            churn_case_key_start=churn_case_key_start,
            churn_source_id_start=churn_source_id_start,
            churn_elapsed_seconds=churn_seconds,
            observed_churn_cases_per_minute=len(churn_admissions) * 60 / churn_seconds,
            receipts_for_future_churn_cases=json.loads(sql(
                "SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY r.created_at,r.receipt_id),'[]') "
                "FROM mdm_steward.policy_receipts_v1 r JOIN s1_audit.churn_admissions c "
                "ON r.case_key=c.case_key")),
            metrics_after_churn=database_metrics(),
            retention_observation="automatic receipt/key deletion is not configured; sizing needs an owner horizon")
        if profile_case_count is not None:
            metrics_before = observations["workload"]["metrics_before"]
            metrics_after = observations["workload"]["metrics_after_churn"]
            storage_growth = (max(0, int(metrics_after["wal_bytes"]) - int(metrics_before["wal_bytes"]))
                              + max(0, int(metrics_after["database_bytes"])
                                    - int(metrics_before["database_bytes"])))
            profile_text = profile_log.read_text() if profile_log.is_file() else ""
            observations["diagnostic_profile"].update(
                combined_positive_wal_database_growth_bytes=storage_growth,
                storage_limit_bytes=10 * 1024 * 1024 * 1024,
                profile_log_path=profile_log.name,
                profile_log_bytes=len(profile_text.encode()),
                profile_log_sha256=hashlib.sha256(profile_text.encode()).hexdigest(),
                auto_explain_notice_count=profile_text.count("NOTICE:"))
            assert storage_growth <= 10 * 1024 * 1024 * 1024, storage_growth
        assert not observations["workload"]["receipts_for_future_churn_cases"]
        observed_rate = observations["workload"]["observed_churn_cases_per_minute"]
        rate_check = {"name": "1000-case-due-burst-and-60-per-minute-churn",
                      "target_cases_per_minute": churn_per_minute,
                      "observed_cases_per_minute": observed_rate,
                      "churn_cases": len(churn_admissions),
                      "window_minutes": churn_window_minutes,
                      "result": ("not_assessed_diagnostic_profile" if profile_case_count is not None
                                 else "passed" if observed_rate >= churn_per_minute else "failed")}
        observations["checks"].append(rate_check)
        if profile_case_count is None:
            assert observed_rate >= churn_per_minute, rate_check
        observations["entry_decision"] = ("DIAGNOSTIC_ONLY" if profile_case_count is not None
                                           else "INPUT_BLOCKED")
        observations["remaining_inputs"] = [
            "owner outcomes for non-next-level, disabled-binding, and malformed-level cases",
            "outage/recovery qualification and numeric envelope bound to the final candidate and baseline"]
        observations["workload"]["entry_gate_disposition"] = (
            "due burst and intake measured; outage/recovery and final envelope binding remain")
        if profile_case_count is not None:
            print("PROFILE_COMPLETE: focused churn SQL plans captured; qualification rate not assessed", flush=True)
        else:
            print("PASS: S1 1000-case due burst, 60/min churn, full receipts and resource vectors", flush=True)
            print("PASS: future idle deadline -> actual MDM receipt; registered callback failure and role reset; complete case/source vectors", flush=True)
    except BaseException as error:
        observations["failure"] = str(error)
        if created:
            logs = subprocess.run(["docker", "logs", "--tail", "100", container],
                                  capture_output=True, text=True, timeout=30)
            observations["failure_logs"] = logs.stdout + logs.stderr
        raise
    finally:
        if churn_progress.is_file():
            progress_bytes = churn_progress.read_bytes()
            observations.setdefault("workload", {}).update(
                churn_progress_path=churn_progress.name,
                churn_progress_sha256=hashlib.sha256(progress_bytes).hexdigest(),
                churn_batches_recorded=churn_completed_batches)
        if churn_phase_started:
            try:
                observations.setdefault("workload", {}).update(
                    churn_partial_admissions=json.loads(sql(
                        "SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY c.batch),'[]') "
                        "FROM s1_audit.churn_admissions c")),
                    churn_partial_sources=json.loads(sql(
                        "SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY s.id),'[]') "
                        "FROM public.policy_qualification_source s "
                        "WHERE EXISTS (SELECT 1 FROM s1_audit.churn_admissions a, "
                        "jsonb_array_elements(a.source_rows) x WHERE (x->>'id')::bigint=s.id)")),
                    churn_partial_cases=json.loads(sql(
                        "SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY c.case_key),'[]') "
                        "FROM mdm_steward.policy_cases_v1 c "
                        "JOIN s1_audit.churn_admissions a USING (case_key)")),
                    churn_partial_receipts=json.loads(sql(
                        "SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY r.created_at,r.receipt_id),'[]') "
                        "FROM mdm_steward.policy_receipts_v1 r "
                        "JOIN s1_audit.churn_admissions a USING (case_key)")))
            except Exception as snapshot_error:
                observations.setdefault("workload", {})[
                    "churn_partial_capture_error"] = str(snapshot_error)
        if containers:
            server_container = containers[-1]
            try:
                server_logs = subprocess.run(
                    ["docker", "logs", "--tail", "1000", server_container],
                    capture_output=True, text=True, timeout=30)
                server_text = server_logs.stdout + server_logs.stderr
                (output / "postgres-s1-server.log").write_text(server_text)
                observations["server_log_capture"] = {
                    "container": server_container, "exit_code": server_logs.returncode,
                    "sha256": hashlib.sha256(server_text.encode()).hexdigest()}
            except BaseException as log_error:
                observations["server_log_capture_error"] = str(log_error)
        (output / "observations.json").write_text(json.dumps(observations, indent=2) + "\n")
        for disposable in containers:
            command("docker", "rm", "--force", "--volumes", disposable)


if __name__ == "__main__":
    main()
