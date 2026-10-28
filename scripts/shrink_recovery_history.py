#!/usr/bin/env python3
"""Replay and reduce a tagged generated recovery history."""

from __future__ import annotations

import argparse
import copy
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import dvm_shrink  # noqa: E402

TEST_NAME = "test_lifecycle_seeded_restore_lag_and_crash_preserve_committed_journal"
TEST_TARGET = "e2e_wal_receipt_recovery_tests"
FAILURE_RE = re.compile(r"PGT_LIFECYCLE_FAILURE:([a-z0-9_]+)([^\r\n]*)")
LEGACY_MUTATION_RE = re.compile(
    r"INSERT INTO history_source VALUES \((-?\d+),'(-?\d+)'\)"
    r"|UPDATE history_source SET value='(-?\d+)' WHERE id=(-?\d+)"
    r"|DELETE FROM history_source WHERE id=(-?\d+)"
)


def failure_signature(output: str, returncode: int) -> str | None:
    """Accept only a tagged assertion from the recovery history test."""
    if returncode == 0:
        return None
    match = FAILURE_RE.search(output)
    if match is None:
        return None
    detail = match.group(2).split("; history:", 1)[0].strip(" ;")
    detail = re.sub(r"\bcycle=\d+\b;?\s*", "", detail).strip(" ;")
    return f"{match.group(1)}{'; ' + detail if detail else ''}"


def read_history(path: Path) -> tuple[list[dict], int, int]:
    events = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(events, list) or not events or not isinstance(events[0], dict):
        raise ValueError(f"{path} is not a lifecycle history JSON array")
    header = events[0]
    seed = header.get("seed")
    cycles = header.get("cycles")
    if (
        isinstance(seed, bool)
        or not isinstance(seed, int)
        or isinstance(cycles, bool)
        or not isinstance(cycles, int)
        or cycles < 1
    ):
        raise ValueError(f"{path} must record integer seed and positive cycle count")
    return events, seed, cycles


def mutation_plan(events: list[dict]) -> list[dict[str, int | str]]:
    """Read generated operations, including traces written before structured plans."""
    operations = []
    for event in events:
        if event.get("step") != "committed source+journal mutation":
            continue
        mutation = event.get("mutation")
        if mutation is None:
            match = LEGACY_MUTATION_RE.search(str(event.get("sql", "")))
            if match is None:
                raise ValueError("history contains a mutation that cannot be replayed safely")
            if match.group(1) is not None:
                kind, row_id, value = "insert", int(match.group(1)), int(match.group(2))
            elif match.group(3) is not None:
                kind, value, row_id = "update", int(match.group(3)), int(match.group(4))
            else:
                kind, row_id, value = "delete", int(match.group(5)), 0
        else:
            if not isinstance(mutation, dict) or mutation.get("kind") not in {
                "insert",
                "update",
                "delete",
            }:
                raise ValueError("history contains an invalid structured mutation")
            kind = mutation["kind"]
            row_id = mutation.get("id")
            value = mutation.get("value")
            if any(
                isinstance(number, bool) or not isinstance(number, int)
                for number in (row_id, value)
            ):
                raise ValueError("history mutation id and value must be integers")
        cycle = event.get("cycle", len(operations) + 1)
        if isinstance(cycle, bool) or not isinstance(cycle, int):
            raise ValueError("history mutation cycle must be an integer")
        operations.append(
            {"kind": kind, "id": row_id, "value": value, "source_cycle": cycle}
        )
    if not operations:
        raise ValueError("history contains no replayable committed mutations")
    return operations


def replay_scenario(operations: list[dict[str, int | str]]) -> dict:
    """Wrap paired source/journal mutations for the existing cycle shrinker."""
    # shortcut: SQL shape stays fixed, when a failure needs AST-level shrinking.
    return {
        "preserve_sql_boundaries": True,
        "initial_data": [],
        "cycles": [
            {
                "source_cycle": operation["source_cycle"],
                "mutations": [
                    {key: operation[key] for key in ("kind", "id", "value")}
                ],
            }
            for operation in operations
        ],
    }


def scenario_mutations(scenario: dict) -> list[dict[str, int | str]]:
    return [
        {**cycle["mutations"][0], "source_cycle": cycle["source_cycle"]}
        for cycle in scenario["cycles"]
    ]


def legal_mutation_plan(seed: int, operations: list[dict[str, int | str]]) -> bool:
    """Keep inserts unique and updates/deletes attached to a live generated row."""
    live_ids = {0} if seed & 1 else set()
    for operation in operations:
        row_id = operation["id"]
        if operation["kind"] == "insert":
            if row_id in live_ids:
                return False
            live_ids.add(row_id)
        elif row_id not in live_ids:
            return False
        elif operation["kind"] == "delete":
            live_ids.remove(row_id)
    return True


def available_path(path: Path) -> Path:
    if not path.exists():
        return path
    for suffix in range(2, 1000):
        candidate = path.with_name(f"{path.stem}-{suffix}{path.suffix}")
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"too many existing shrink artifacts for {path.name}")


def run_candidate(
    seed: int, mutations: list[dict[str, int | str]], artifact_dir: Path
) -> dict[str, object]:
    artifact_dir.mkdir(parents=True, exist_ok=False)
    plan_path = artifact_dir / "mutation-plan.json"
    plan_path.write_text(json.dumps(mutations, indent=2) + "\n", encoding="utf-8")
    env = os.environ.copy()
    env.update(
        {
            "PGS_PROP_CASES": "1",
            "PGS_PROP_SEED": f"0x{seed:016x}",
            "PGS_PROP_CYCLES": str(len(mutations)),
            "PGT_LIFECYCLE_REPLAY_PLAN": str(plan_path),
            "PGT_LIFECYCLE_ARTIFACT_DIR": str(artifact_dir),
        }
    )
    command = [
        "bash",
        str(ROOT / "scripts/run_e2e_tests.sh"),
        "--test",
        TEST_TARGET,
        "-E",
        f"test(={TEST_NAME})",
        "--retries",
        "0",
    ]
    result = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
        check=False,
    )
    (artifact_dir / "runtime.log").write_text(result.stdout, encoding="utf-8")
    return {
        "returncode": result.returncode,
        "failure_signature": failure_signature(result.stdout, result.returncode),
        "history": str(artifact_dir / f"history-{seed:016x}.json"),
        "replay_plan": str(plan_path),
    }


def shrink(history_path: Path) -> tuple[Path, Path, Path]:
    history_path = history_path.resolve()
    events, seed, configured_cycles = read_history(history_path)
    mutations = mutation_plan(events)
    if len(mutations) > configured_cycles:
        raise ValueError("history has more mutation events than its configured cycle count")
    output_dir = history_path.parent
    original_path = available_path(output_dir / f"history-{seed:016x}.original.json")
    shutil.copy2(history_path, original_path)
    attempts_dir = Path(
        tempfile.mkdtemp(prefix=f"history-{seed:016x}-shrink-", dir=output_dir)
    )
    if not legal_mutation_plan(seed, mutations):
        raise ValueError("recorded mutations do not form a legal generated row history")

    baseline = run_candidate(seed, mutations, attempts_dir / "baseline")
    signature = baseline["failure_signature"]
    if not isinstance(signature, str):
        raise RuntimeError(
            "the recorded mutation schedule did not reproduce a tagged recovery assertion; "
            f"the unmodified trace is preserved at {original_path} and output at "
            f"{attempts_dir / 'baseline' / 'runtime.log'}"
        )

    attempts: list[dict[str, object]] = [
        {
            "cycles": len(mutations),
            "source_cycles": [mutation["source_cycle"] for mutation in mutations],
            **baseline,
            "role": "reproduced original",
        }
    ]
    successful_runs = {json.dumps(mutations, sort_keys=True): baseline}

    def replay(candidate: dict) -> bool:
        candidate_mutations = scenario_mutations(candidate)
        source_cycles = [mutation["source_cycle"] for mutation in candidate_mutations]
        if not legal_mutation_plan(seed, candidate_mutations):
            plan_path = attempts_dir / f"rejected-{len(attempts):03d}.json"
            plan_path.write_text(
                json.dumps(candidate_mutations, indent=2) + "\n", encoding="utf-8"
            )
            attempts.append(
                {
                    "cycles": len(candidate_mutations),
                    "source_cycles": source_cycles,
                    "rejected_plan": str(plan_path),
                    "rejected": "generated row history would update/delete a missing row or duplicate an insert",
                }
            )
            return False
        result = run_candidate(
            seed,
            candidate_mutations,
            attempts_dir / f"attempt-{len(attempts):03d}",
        )
        attempts.append(
            {
                "cycles": len(candidate_mutations),
                "source_cycles": source_cycles,
                **result,
            }
        )
        if result["failure_signature"] != signature:
            return False
        successful_runs[json.dumps(candidate_mutations, sort_keys=True)] = result
        return True

    scenario = replay_scenario(mutations)
    while True:
        before = json.dumps(scenario, sort_keys=True)
        scenario = dvm_shrink.shrink_scenario(scenario, replay)
        for index in range(len(scenario["cycles"])):
            for field in ("id", "value"):
                if field == "value" and scenario["cycles"][index]["mutations"][0]["kind"] == "delete":
                    continue
                if scenario["cycles"][index]["mutations"][0][field] == 0:
                    continue
                candidate = copy.deepcopy(scenario)
                candidate["cycles"][index]["mutations"][0][field] = 0
                if replay(candidate):
                    scenario = candidate
        if json.dumps(scenario, sort_keys=True) == before:
            break

    minimized_mutations = scenario_mutations(scenario)
    final_run = successful_runs[json.dumps(minimized_mutations, sort_keys=True)]
    minimized_history = Path(final_run["history"])
    if not minimized_history.is_file():
        raise RuntimeError(
            f"matching failure did not retain its history artifact: {minimized_history}"
        )

    minimized, _, _ = read_history(minimized_history)
    minimized[0]["minimization"] = {
        "method": "cycle-and-operand",
        "failure_signature": signature,
        "configured_cycles": configured_cycles,
        "original_cycles": len(mutations),
        "minimized_cycles": len(minimized_mutations),
        "original_mutations": mutations,
        "minimized_mutations": minimized_mutations,
        "original_trace": original_path.name,
        "attempts": len(attempts) - 1,
    }
    minimized_path = available_path(output_dir / f"history-{seed:016x}.minimized.json")
    minimized_path.write_text(json.dumps(minimized, indent=2) + "\n", encoding="utf-8")

    report = {
        "seed": seed,
        "failure_signature": signature,
        "configured_cycles": configured_cycles,
        "original_cycles": len(mutations),
        "minimized_cycles": len(minimized_mutations),
        "original_mutations": mutations,
        "minimized_mutations": minimized_mutations,
        "original_trace": str(original_path),
        "minimized_trace": str(minimized_path),
        "attempts_directory": str(attempts_dir),
        "attempts": attempts,
    }
    report_path = available_path(output_dir / f"history-{seed:016x}.shrink-report.json")
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return original_path, minimized_path, report_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("history", type=Path, help="unminimized history JSON from a tagged failure")
    args = parser.parse_args()
    try:
        original, minimized, report = shrink(args.history)
    except (OSError, ValueError, RuntimeError) as error:
        print(f"recovery history shrink failed: {error}", file=sys.stderr)
        return 1
    print(f"original:   {original}")
    print(f"minimized:  {minimized}")
    print(f"report:     {report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
