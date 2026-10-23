#!/usr/bin/env python3
"""Replay a stored DVM scenario without invoking its generator."""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

FORMAT_VERSION = 1


class ReplayFailure(RuntimeError):
    def __init__(self, failure_class: str, detail: str):
        super().__init__(f"{failure_class}: {detail}")
        self.failure_class = failure_class


def matches_negative_control(scenario: dict, failure: dict) -> bool:
    expected = scenario["negative_control"]
    return all(
        failure.get(field) == expected[f"expected_{field}"]
        for field in ("failure_class", "phase", "cycle", "mutation")
    )


def validate(scenario: dict) -> None:
    if scenario.get("format_version") != FORMAT_VERSION:
        raise ValueError(
            f"unsupported scenario format {scenario.get('format_version')!r}; "
            f"expected {FORMAT_VERSION}"
        )
    for path in (
        ("scenario_id",),
        ("generator_version",),
        ("schema", "name"),
        ("query", "stream_table"),
        ("query", "defining_query"),
        ("execution", "schedule"),
        ("execution", "requested_refresh_mode"),
    ):
        value = scenario
        for part in path:
            value = value.get(part) if isinstance(value, dict) else None
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{'.'.join(path)} must be a non-empty string")
    name = scenario["schema"]["name"]
    if not name.replace("_", "").isalnum() or not name[0].isalpha():
        raise ValueError(f"unsafe schema identifier: {name}")
    if not scenario["schema"].get("setup_sql") or not scenario.get("initial_data"):
        raise ValueError("scenario must contain setup SQL and initial data")
    if not scenario.get("cycles"):
        raise ValueError("scenario must contain at least one mutation cycle")
    if not scenario["query"].get("columns"):
        raise ValueError("query.columns must not be empty")
    if not isinstance(scenario["execution"].get("observe_numeric_scale", False), bool):
        raise ValueError("execution.observe_numeric_scale must be boolean")
    for cycle_index, cycle in enumerate(scenario["cycles"]):
        if not cycle.get("name") or not cycle.get("mutations"):
            raise ValueError(f"cycle {cycle_index} must have a name and mutations")
        for mutation_index, mutation in enumerate(cycle["mutations"]):
            if not mutation.get("sql", "").strip():
                raise ValueError(
                    f"cycle {cycle_index} mutation {mutation_index} has empty SQL"
                )
    if "negative_control" in scenario:
        expected = scenario["negative_control"]
        for field in ("failure_class", "phase", "cycle", "mutation"):
            if f"expected_{field}" not in expected:
                raise ValueError(f"negative_control.expected_{field} is required")


def load(path: Path) -> dict:
    with path.open(encoding="utf-8") as handle:
        scenario = json.load(handle)
    validate(scenario)
    return scenario


def replay_sql(scenario: dict) -> str:
    query = scenario["query"]
    execution = scenario["execution"]
    statements = [f"CREATE SCHEMA {scenario['schema']['name']}"]
    statements += scenario["schema"]["setup_sql"]
    statements += scenario["initial_data"]
    if execution.get("require_differential_history", False):
        statements += ["SET pg_trickle.refresh_strategy = 'differential'", "SET pg_trickle.differential_max_change_ratio = 0"]
    statements.append(
        "SELECT pgtrickle.create_stream_table("
        + f"'{query['stream_table']}', "
        + "$dvm$"
        + query["defining_query"]
        + "$dvm$"
        + f", '{execution['schedule']}', '{execution['requested_refresh_mode']}')"
    )
    statements.append(
        f"SELECT pgtrickle.refresh_stream_table('{query['stream_table']}')"
    )
    for cycle in scenario["cycles"]:
        statements.append(f"-- cycle: {cycle['name']}")
        statements.extend(mutation["sql"] for mutation in cycle["mutations"])
        statements.append(
            f"SELECT pgtrickle.refresh_stream_table('{query['stream_table']}')"
        )
    return ";\n".join(statements) + ";\n"


def psql(database: str | None, sql: str, tuples: bool = False) -> str:
    if shutil.which("psql") is None:
        raise RuntimeError(
            "psql is required for live replay; use --validate-only to inspect JSON"
        )
    command = ["psql", "--no-psqlrc", "--set", "ON_ERROR_STOP=1"]
    if tuples:
        command += ["--tuples-only", "--quiet"]
    if database:
        command += ["--dbname", database]
    result = subprocess.run(
        command,
        input=sql,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode:
        raise RuntimeError((result.stderr or result.stdout).strip())
    return result.stdout.strip()


def exact_check(scenario: dict, database: str | None) -> None:
    query = scenario["query"]
    columns = ", ".join(query["columns"])
    if scenario["execution"].get("observe_numeric_scale", False):
        relation = query["stream_table"].replace("'", "''")
        numeric = json.loads(psql(database,
            "SELECT coalesce(jsonb_agg(attname ORDER BY attnum),'[]'::jsonb)::text "
            "FROM pg_catalog.pg_attribute "
            f"WHERE attrelid='{relation}'::regclass AND attnum>0 AND NOT attisdropped AND atttypid=1700",
            tuples=True))
        admitted = [name for name in numeric if name in query["columns"]]
        if not admitted:
            raise ReplayFailure("OracleFailure", "numeric-scale observation has no admitted public numeric column")
        columns += ", " + ", ".join(
            'pg_catalog.scale("' + name.replace('"', '""') + '")' for name in admitted
        )
    actual = f"(SELECT {columns} FROM {query['stream_table']})"
    expected = f"(SELECT {columns} FROM ({query['defining_query']}) AS expected)"
    check = (
        "SELECT CASE WHEN NOT EXISTS ("
        f"(({actual}) EXCEPT ALL ({expected})) UNION ALL "
        f"(({expected}) EXCEPT ALL ({actual}))"
        ") THEN 'ok' ELSE 'mismatch' END"
    )
    if psql(database, check, tuples=True).strip() != "ok":
        raise ReplayFailure("MultisetMismatch", "exact row comparison failed")


def mode_check(scenario: dict, database: str | None) -> None:
    query = scenario["query"]
    expected = scenario["expected_capability"]
    if not expected.get("differential"):
        return
    name = query["stream_table"]
    mode = psql(
        database,
        "SELECT upper(coalesce(effective_refresh_mode, '')) "
        "FROM pgtrickle.pgt_stream_tables "
        f"WHERE pgt_schema || '.' || pgt_name = '{name}'",
        tuples=True,
    ).strip()
    if not mode.startswith(expected["expected_mode"].upper()):
        raise ReplayFailure(
            "SilentFallback", f"expected {expected['expected_mode']}, got {mode!r}"
        )


def execute_mutation(database: str | None, sql: str) -> int:
    # Execute the complete script in one session, preserving BEGIN/SAVEPOINT cuts.
    if re.match(r"^\s*SELECT\b", sql, flags=re.IGNORECASE):
        return int(psql(database, f"WITH changed AS ({sql.rstrip(';')}) SELECT count(*) FROM changed", tuples=True))
    output = psql(database, sql)
    return sum(int(match[2]) for line in output.splitlines()
               if (match := re.fullmatch(r"(INSERT \d+|UPDATE|DELETE) (\d+)", line.strip())))


def changed_route_check(scenario: dict, database: str | None, previous: int) -> None:
    name = scenario["query"]["stream_table"]
    observed = psql(database,
        "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h "
        "JOIN pgtrickle.pgt_stream_tables s ON s.pgt_id = h.pgt_id "
        f"WHERE s.pgt_schema || '.' || s.pgt_name = '{name}' AND h.refresh_id > {previous} "
        "AND h.status = 'COMPLETED' AND h.action = 'DIFFERENTIAL' AND NOT h.was_full_fallback)",
        tuples=True)
    if observed != "t":
        raise ReplayFailure("SilentFallback", "no fresh completed differential history without fallback")


def captured_source_cuts(scenario: dict, database: str | None) -> list[tuple[int, str]]:
    name = scenario["query"]["stream_table"].replace("'", "''")
    sources = json.loads(psql(database,
        "SELECT coalesce(json_agg(json_build_array(d.source_relid::bigint, "
        "format('%I.%I',current_setting('pg_trickle.change_buffer_schema'),"
        "'changes_' || ct.source_stable_name))), '[]') "
        "FROM pgtrickle.pgt_dependencies d JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) "
        "JOIN pgtrickle.pgt_change_tracking ct ON ct.source_relid=d.source_relid "
        f"WHERE s.pgt_schema || '.' || s.pgt_name='{name}'", tuples=True))
    if not sources:
        raise ReplayFailure("CoverageNotReached", "no captured sources")
    pending = " UNION ALL ".join(
        f"SELECT {int(oid)}::bigint AS oid,max(lsn)::text AS lsn FROM {buffer} "
        "WHERE action IN ('I','D') AND lsn > (SELECT "
        f"coalesce(frontier->'sources'->'{int(oid)}'->>'lsn','0/0')::pg_lsn "
        f"FROM pgtrickle.pgt_stream_tables WHERE pgt_schema || '.' || pgt_name='{name}')"
        for oid, buffer in sources)
    deadline = time.monotonic() + 5
    while True:
        cuts = json.loads(psql(database,
            "SELECT coalesce(json_agg(json_build_array(oid,lsn)), '[]') "
            f"FROM ({pending}) pending WHERE lsn IS NOT NULL", tuples=True))
        if cuts:
            return [(int(oid), lsn) for oid, lsn in cuts]
        if time.monotonic() >= deadline:
            raise ReplayFailure("CoverageNotReached", "no committed captured changes above the consumer cut")
        time.sleep(.01)


def source_cut_check(scenario: dict, database: str | None, cuts: list[tuple[int, str]]) -> None:
    name = scenario["query"]["stream_table"].replace("'", "''")
    for oid, lsn in cuts:
        observed = psql(database,
            f"SELECT coalesce(frontier->'sources'->'{oid}'->>'lsn','0/0')::pg_lsn >= '{lsn}'::pg_lsn "
            f"FROM pgtrickle.pgt_stream_tables WHERE pgt_schema || '.' || pgt_name='{name}'", tuples=True)
        if observed != "t":
            raise ReplayFailure("CoverageNotReached", f"source {oid} captured through {lsn}, but refresh held an earlier cut")


def artifact(scenario: dict, scenario_path: Path, failure: dict) -> Path:
    message = failure["detail"]
    directory = Path(os.environ.get("DVM_ARTIFACT_DIR", "artifacts/dvm-fuzz"))
    directory = directory / scenario["scenario_id"]
    directory.mkdir(parents=True, exist_ok=True)
    write = lambda name, text: (directory / name).write_text(text, encoding="utf-8")
    write("scenario.json", json.dumps(scenario, indent=2) + "\n")
    write("feature_vector.json", json.dumps(scenario["features"], indent=2) + "\n")
    write("setup.sql", ";\n".join(scenario["schema"]["setup_sql"]))
    write("defining_query.sql", scenario["query"]["defining_query"])
    write(
        "mutations.sql",
        ";\n".join(
            mutation["sql"]
            for cycle in scenario["cycles"]
            for mutation in cycle["mutations"]
        ),
    )
    write("replay.sql", replay_sql(scenario))
    write("replay.sh", f"just dvm-replay {scenario_path}\n")
    for name in (
        "actual_rows.jsonl",
        "expected_rows.jsonl",
        "extra_rows.jsonl",
        "missing_rows.jsonl",
        "actual_schema.json",
        "expected_schema.json",
        "generated_delta.sql",
    ):
        write(name, "")
    write(
        "coverage.json",
        json.dumps(
            {
                "snapshot_plans": [],
                "changed_leaf_buckets": [],
                "group_lifecycle_transitions": [],
                "outer_join_transitions": [],
                "p0_pairwise_complete": False,
                "available": False,
                "note": "Decision coverage is available when replay runs against the Rust E2E harness with pg_trickle.dvm_decision_trace enabled.",
            }
        )
        + "\n",
    )
    write(
        "dvm_trace.json",
        json.dumps(
            {
                "events": [],
                "available": False,
                "note": "Decision traces are emitted to the PostgreSQL log when pg_trickle.dvm_decision_trace is enabled.",
            }
        )
        + "\n",
    )
    write("postgres.log", message + "\n")
    write(
        "failure.json",
        json.dumps(
            {
                "scenario_id": scenario["scenario_id"],
                **failure,
                "generator_version": scenario["generator_version"],
            },
            indent=2,
        )
        + "\n",
    )
    write(
        "environment.txt",
        "\n".join(
            [
                f"package_version={os.environ.get('CARGO_PKG_VERSION', 'unknown')}",
                f"scenario_format={FORMAT_VERSION}",
                f"os={platform.system()}",
                f"arch={platform.machine()}",
            ]
        )
        + "\n",
    )
    return directory


def run(path: Path, database: str | None, expect_negative_control: bool = False) -> None:
    scenario = load(path)
    if expect_negative_control and "negative_control" not in scenario:
        raise ValueError("--expect-negative-control requires declared failure metadata")
    schema = scenario["schema"]["name"]
    query = scenario["query"]
    phase = "schema setup"
    cycle_index = mutation_index = None
    try:
        psql(database, f"CREATE SCHEMA {schema}")
        for label, statements in (
            ("setup SQL", scenario["schema"]["setup_sql"]),
            ("initial data", scenario["initial_data"]),
        ):
            for index, sql in enumerate(statements):
                phase = f"{label} {index}"
                psql(database, sql)
        phase = "stream-table admission"
        psql(
            database,
            "SELECT pgtrickle.create_stream_table("
            + f"'{query['stream_table']}', "
            + "$dvm$"
            + query["defining_query"]
            + "$dvm$"
            + f", '{scenario['execution']['schedule']}', "
            + f"'{scenario['execution']['requested_refresh_mode']}')",
        )
        refresh = f"SELECT pgtrickle.refresh_stream_table('{query['stream_table']}')"
        require_changed = scenario["execution"].get("require_differential_history", False)
        if require_changed:
            refresh = "SET pg_trickle.refresh_strategy = 'differential'; SET pg_trickle.differential_max_change_ratio = 0; " + refresh
        phase = "initial refresh"
        psql(database, refresh)
        phase = "initial refresh mode"
        mode_check(scenario, database)
        phase = "initial exact comparison"
        exact_check(scenario, database)
        for cycle_index, cycle in enumerate(scenario["cycles"]):
            previous = int(psql(database, "SELECT COALESCE(max(refresh_id), 0) FROM pgtrickle.pgt_refresh_history", tuples=True)) if require_changed else 0
            phase = f"{cycle['name']} mutation"
            for mutation_index, mutation in enumerate(cycle["mutations"]):
                affected = execute_mutation(database, mutation["sql"])
                if affected != mutation["expected_affected_rows"]:
                    raise ReplayFailure(
                        "GeneratorInvalid", f"expected {mutation['expected_affected_rows']} "
                        f"affected rows, got {affected}"
                    )
            mutation_index = None
            phase = "capture witness"
            cuts = captured_source_cuts(scenario, database) if require_changed else []
            phase = f"{cycle['name']} refresh"
            psql(database, refresh)
            phase = f"{cycle['name']} refresh mode"
            mode_check(scenario, database)
            if require_changed:
                phase = f"{cycle['name']} captured source cut"
                source_cut_check(scenario, database, cuts)
                phase = f"{cycle['name']} changed-data route"
                changed_route_check(scenario, database, previous)
            phase = f"{cycle['name']} exact comparison"
            exact_check(scenario, database)
    except (RuntimeError, ValueError, OSError) as error:
        failure = {
            "failure_class": error.failure_class if isinstance(error, ReplayFailure) else "ReplayError",
            "phase": phase,
            "cycle": cycle_index,
            "mutation": mutation_index,
            "detail": str(error),
        }
        directory = artifact(scenario, path, failure)
        if expect_negative_control and matches_negative_control(scenario, failure):
            return
        raise RuntimeError(f"{error}; artifact={directory}") from error
    finally:
        try:
            psql(database, f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        except RuntimeError:
            pass
    if expect_negative_control:
        raise RuntimeError("negative control passed without its declared failure")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("scenarios", nargs="+", type=Path)
    parser.add_argument("--database")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--expect-negative-control", action="store_true")
    args = parser.parse_args()
    try:
        for path in args.scenarios:
            scenario = load(path)
            if args.validate_only:
                print(f"validated {path} (format {scenario['format_version']})")
            else:
                run(path, args.database, args.expect_negative_control)
                print(f"replayed {path}")
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"dvm-replay: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
