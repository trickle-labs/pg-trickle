#!/usr/bin/env python3
"""COR-20 machine-enforced release gate.

Runs every DVM correctness check the release process depends on and reports
a single pass/fail verdict. Steps that need a live database are SKIPPED
(not failed) when `--database` is omitted, so this still runs meaningfully
in a sandbox without Docker/Postgres while remaining a real gate in CI where
`--database` is passed.

Sibling tooling this gate leans on (owned by other engineers, not this
script): scripts/dvm_shrink.py, scripts/dvm_corpus_retain.py,
tests/e2e_dvm_shrink_tests.rs, tests/e2e_dvm_strategy_tests.rs.
"""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
from pathlib import Path

NEGATIVE_CONTROLS_DIR = Path("tests/corpus/dvm_negative_controls")


def run_step(name: str, command: str) -> tuple[bool, str]:
    """Run a shell step (mirrors the equivalent justfile recipe's commands
    directly, rather than shelling out to `just` itself -- CI runners in this
    repo never install `just`, only cargo/scripts, see ci.yml/fuzz-*.yml).
    Never raises; OSError becomes a failed step."""
    try:
        result = subprocess.run(
            command, shell=True, capture_output=True, text=True
        )
    except OSError as error:
        return False, f"{name}: could not launch {command!r}: {error}"
    output = (result.stdout or "") + (result.stderr or "")
    return result.returncode == 0, output


def extract_failure_class(output: str) -> str | None:
    """Pull the failure class out of dvm_replay.py's `dvm-replay: <class>: ...` line."""
    marker = "dvm-replay: "
    idx = output.find(marker)
    if idx == -1:
        return None
    return output[idx + len(marker):].split(":", 1)[0].strip()


def check_negative_controls(database: str | None) -> tuple[bool, str]:
    """Every scenario in tests/corpus/dvm_negative_controls/ must fail replay
    with its declared failure class. Without --database we can only confirm
    the JSON is well-formed and carries a negative_control block -- reported
    as a separate structural-only outcome, never claimed as live-verified.
    """
    files = sorted(NEGATIVE_CONTROLS_DIR.glob("*.json"))
    if not files:
        return False, f"no negative-control scenarios found in {NEGATIVE_CONTROLS_DIR}/"

    lines: list[str] = []
    all_ok = True
    for path in files:
        try:
            scenario = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            lines.append(f"FAIL {path.name}: could not parse JSON: {error}")
            all_ok = False
            continue

        negative_control = scenario.get("negative_control")
        expected_class = (
            negative_control.get("expected_failure_class")
            if isinstance(negative_control, dict)
            else None
        )
        if not expected_class:
            lines.append(
                f"FAIL {path.name}: missing negative_control.expected_failure_class "
                "(structural-only check)"
            )
            all_ok = False
            continue

        if database:
            passed, output = run_step(
                f"negative-control:{path.name}",
                "python3 scripts/dvm_replay.py "
                f"{shlex.quote(str(path))} --database {shlex.quote(database)}",
            )
            if passed:
                lines.append(
                    f"FAIL {path.name}: replay did NOT fail (expected {expected_class}) "
                    "-- detection silently broke"
                )
                all_ok = False
                continue
            actual_class = extract_failure_class(output)
            if actual_class != expected_class:
                lines.append(
                    f"FAIL {path.name}: replay failed with class {actual_class!r}, "
                    f"expected {expected_class!r}"
                )
                all_ok = False
            else:
                lines.append(f"PASS {path.name}: replay correctly failed with {expected_class}")
        else:
            passed, output = run_step(
                f"negative-control-structural:{path.name}",
                f"python3 scripts/dvm_replay.py {shlex.quote(str(path))} --validate-only",
            )
            if not passed:
                lines.append(f"FAIL {path.name}: --validate-only failed: {output.strip()}")
                all_ok = False
            else:
                lines.append(
                    f"PASS {path.name}: structural-only, not live-verified "
                    f"(declares {expected_class})"
                )
    return all_ok, "\n".join(lines)


# (name, shell command, needs_live_database)
# Each command mirrors the equivalent justfile recipe verbatim rather than
# invoking `just` itself (CI runners in this repo only ever install
# cargo/scripts, see ci.yml/fuzz-*.yml -- none of them shell out to `just`).
# corpus-replay and metamorphic-checks each need the full Docker E2E image
# regardless of this script's own --database flag, so they are skipped
# whenever --database is omitted here.
STEPS: list[tuple[str, str, bool]] = [
    ("fmt-check", "cargo fmt -- --check", False),
    (
        "corpus-replay",
        "python3 scripts/dvm_replay.py --validate-only tests/corpus/dvm_regressions/*.json "
        "&& ./scripts/run_e2e_tests.sh --test e2e_dvm_corpus_tests --no-capture",
        True,
    ),
    (
        "composition-matrix",
        "cargo test --test e2e_dvm_composition_tests --features pg18 "
        "test_v0873_matrix_matches_published_requirements "
        "&& cargo test --test e2e_dvm_composition_tests --features pg18 "
        "test_v0873_generated_queries_have_schemas_and_render_sql "
        "&& cargo test --test e2e_dvm_composition_tests --features pg18 "
        "semantic_floors_pass_and_report_missing_buckets",
        False,
    ),
    (
        "metamorphic-checks",
        "cargo test --test e2e_dvm_metamorphic_tests --features pg18",
        True,
    ),
    (
        "coverage-floors",
        "cargo test --lib --features pg18 dvm::schema "
        "&& cargo test --lib --features pg18 dvm::snapshot "
        "&& cargo test --lib --features pg18 "
        "dvm::diff::tests::test_decision_trace_records_declared_schema_and_snapshot_plan "
        "&& cargo test --test e2e_dvm_composition_tests --features pg18 "
        "semantic_floors_pass_and_report_missing_buckets",
        False,
    ),
    ("corpus-retention", "python3 scripts/dvm_corpus_retain.py", False),
    ("shrink-selftest", "python3 scripts/dvm_shrink.py --selftest", False),
]


def main() -> int:
    parser = argparse.ArgumentParser(description="COR-20 machine-enforced release gate")
    parser.add_argument("--database", help="Live database to exercise DB-dependent steps against")
    parser.add_argument(
        "--skip",
        action="append",
        default=[],
        metavar="STEP",
        help="Step name to skip (repeatable), for selecting a CI-tier subset",
    )
    args = parser.parse_args()
    skip = set(args.skip)

    results: list[tuple[str, str, str]] = []  # (name, PASS/FAIL/SKIP, output)

    for name, command, needs_db in STEPS:
        if name in skip:
            results.append((name, "SKIP", "skipped via --skip"))
            continue
        if needs_db and not args.database:
            results.append((name, "SKIP", "requires --database (needs live Docker E2E image)"))
            continue
        passed, output = run_step(name, command)
        results.append((name, "PASS" if passed else "FAIL", output))

    if "negative-controls" in skip:
        results.append(("negative-controls", "SKIP", "skipped via --skip"))
    else:
        passed, output = check_negative_controls(args.database)
        results.append(("negative-controls", "PASS" if passed else "FAIL", output))

    print("| step | result |")
    print("| --- | --- |")
    for name, status, _output in results:
        print(f"| {name} | {status} |")
    print()
    for name, status, output in results:
        if status == "FAIL":
            print(f"--- {name} output ---")
            print(output.strip())
            print()

    any_failed = any(status == "FAIL" for _name, status, _output in results)
    print("RELEASE GATE: FAIL" if any_failed else "RELEASE GATE: PASS")
    return 1 if any_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
