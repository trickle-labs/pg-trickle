#!/usr/bin/env python3
"""Replay a stored DVM scenario without invoking its generator."""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

FORMAT_VERSION = 1


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
    for cycle_index, cycle in enumerate(scenario["cycles"]):
        if not cycle.get("name") or not cycle.get("mutations"):
            raise ValueError(f"cycle {cycle_index} must have a name and mutations")
        for mutation_index, mutation in enumerate(cycle["mutations"]):
            if not mutation.get("sql", "").strip():
                raise ValueError(
                    f"cycle {cycle_index} mutation {mutation_index} has empty SQL"
                )


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
    actual = f"(SELECT {columns} FROM {query['stream_table']})"
    expected = f"(SELECT {columns} FROM ({query['defining_query']}) AS expected)"
    check = (
        "SELECT CASE WHEN NOT EXISTS ("
        f"(({actual}) EXCEPT ALL ({expected})) UNION ALL "
        f"(({expected}) EXCEPT ALL ({actual}))"
        ") THEN 'ok' ELSE 'mismatch' END"
    )
    if psql(database, check, tuples=True).strip() != "ok":
        raise RuntimeError("MultisetMismatch: exact row comparison failed")


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
        raise RuntimeError(
            f"SilentFallback: expected {expected['expected_mode']}, got {mode!r}"
        )


def artifact(scenario: dict, scenario_path: Path, message: str) -> Path:
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
                "failure_class": "ProductFailure",
                "detail": message,
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


def run(path: Path, database: str | None) -> None:
    scenario = load(path)
    schema = scenario["schema"]["name"]
    query = scenario["query"]
    try:
        psql(database, f"CREATE SCHEMA {schema}")
        for sql in scenario["schema"]["setup_sql"] + scenario["initial_data"]:
            psql(database, sql)
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
        psql(database, refresh)
        mode_check(scenario, database)
        exact_check(scenario, database)
        for cycle in scenario["cycles"]:
            for mutation in cycle["mutations"]:
                count = psql(
                    database,
                    f"WITH changed AS ({mutation['sql'].rstrip(';')}) "
                    "SELECT count(*) FROM changed",
                    tuples=True,
                )
                affected = int(count)
                if affected != mutation["expected_affected_rows"]:
                    raise RuntimeError(
                        f"GeneratorInvalid: expected {mutation['expected_affected_rows']} "
                        f"affected rows, got {affected}"
                    )
            psql(database, refresh)
            mode_check(scenario, database)
            exact_check(scenario, database)
    except (RuntimeError, ValueError, OSError) as error:
        directory = artifact(scenario, path, str(error))
        raise RuntimeError(f"{error}; artifact={directory}") from error
    finally:
        try:
            psql(database, f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        except RuntimeError:
            pass


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("scenarios", nargs="+", type=Path)
    parser.add_argument("--database")
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    try:
        for path in args.scenarios:
            scenario = load(path)
            if args.validate_only:
                print(f"validated {path} (format {scenario['format_version']})")
            else:
                run(path, args.database)
                print(f"replayed {path}")
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"dvm-replay: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
