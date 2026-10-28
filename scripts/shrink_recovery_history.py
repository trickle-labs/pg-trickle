#!/usr/bin/env python3
"""Replay a tagged recovery failure with the fewest generated mutation cycles."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parent.parent
TEST_NAME = "test_lifecycle_seeded_restore_lag_and_crash_preserve_committed_journal"
TEST_TARGET = "e2e_wal_receipt_recovery_tests"
FAILURE_RE = re.compile(r"PGT_LIFECYCLE_FAILURE:([a-z0-9_]+)([^\r\n]*)")


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


def minimum_cycle_prefix(
    cycles: int, expected: str, replay: Callable[[int], str | None]
) -> tuple[int, list[dict[str, object]]]:
    """Try every shorter prefix in order; the first matching detector is minimal."""
    # shortcut: no SQL reduction, when a smaller within-cycle reproducer is needed.
    attempts = []
    for candidate_cycles in range(1, cycles):
        observed = replay(candidate_cycles)
        attempts.append({"cycles": candidate_cycles, "failure_signature": observed})
        if observed == expected:
            return candidate_cycles, attempts
    return cycles, attempts


def read_history(path: Path) -> tuple[list[dict], int, int]:
    events = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(events, list) or not events or not isinstance(events[0], dict):
        raise ValueError(f"{path} is not a lifecycle history JSON array")
    header = events[0]
    seed = header.get("seed")
    cycles = header.get("cycles")
    if not isinstance(seed, int) or not isinstance(cycles, int) or cycles < 1:
        raise ValueError(f"{path} must record integer seed and positive cycle count")
    return events, seed, cycles


def available_path(path: Path) -> Path:
    if not path.exists():
        return path
    for suffix in range(2, 1000):
        candidate = path.with_name(f"{path.stem}-{suffix}{path.suffix}")
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"too many existing shrink artifacts for {path.name}")


def run_candidate(seed: int, cycles: int, artifact_dir: Path) -> dict[str, object]:
    artifact_dir.mkdir(parents=True, exist_ok=False)
    env = os.environ.copy()
    env.update(
        {
            "PGS_PROP_CASES": "1",
            "PGS_PROP_SEED": f"0x{seed:016x}",
            "PGS_PROP_CYCLES": str(cycles),
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
    }


def shrink(history_path: Path) -> tuple[Path, Path, Path]:
    history_path = history_path.resolve()
    original, seed, cycles = read_history(history_path)
    output_dir = history_path.parent
    original_path = available_path(output_dir / f"history-{seed:016x}.original.json")
    shutil.copy2(history_path, original_path)
    attempts_dir = Path(
        tempfile.mkdtemp(prefix=f"history-{seed:016x}-shrink-", dir=output_dir)
    )

    baseline = run_candidate(seed, cycles, attempts_dir / "baseline")
    signature = baseline["failure_signature"]
    if not isinstance(signature, str):
        raise RuntimeError(
            "the original seed/cycle count did not reproduce a tagged recovery assertion; "
            f"the unmodified trace is preserved at {original_path} and output at "
            f"{attempts_dir / 'baseline' / 'runtime.log'}"
        )

    attempts: list[dict[str, object]] = [
        {"cycles": cycles, **baseline, "role": "reproduced original"}
    ]

    def replay(candidate_cycles: int) -> str | None:
        result = run_candidate(
            seed, candidate_cycles, attempts_dir / f"cycles-{candidate_cycles}"
        )
        attempts.append({"cycles": candidate_cycles, **result})
        observed = result["failure_signature"]
        return observed if isinstance(observed, str) else None

    minimum, reductions = minimum_cycle_prefix(cycles, signature, replay)
    minimized_run = attempts_dir / ("baseline" if minimum == cycles else f"cycles-{minimum}")
    minimized_history = Path(minimized_run / f"history-{seed:016x}.json")
    if not minimized_history.is_file():
        raise RuntimeError(
            f"matching failure did not retain its history artifact: {minimized_history}"
        )

    minimized, _, _ = read_history(minimized_history)
    minimized[0]["minimization"] = {
        "method": "cycle-prefix",
        "failure_signature": signature,
        "original_cycles": cycles,
        "minimized_cycles": minimum,
        "original_trace": original_path.name,
        "attempts": len(reductions),
    }
    minimized_path = available_path(output_dir / f"history-{seed:016x}.minimized.json")
    minimized_path.write_text(json.dumps(minimized, indent=2) + "\n", encoding="utf-8")

    report = {
        "seed": seed,
        "failure_signature": signature,
        "original_cycles": cycles,
        "minimized_cycles": minimum,
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
