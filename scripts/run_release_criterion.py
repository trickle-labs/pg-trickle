#!/usr/bin/env python3
"""Compare release Criterion microbenchmarks with the previous published tag."""

from __future__ import annotations

import os
import json
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BASELINE_TAG = "v0.107.0"
BASELINE_VERSION = BASELINE_TAG.removeprefix("v")
QUALIFICATION = ROOT / "tests/release/v0.108.0-qualification.json"
MAX_COMPARISON_ATTEMPTS = 2


def run(command: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> None:
    print("$ " + " ".join(command), flush=True)
    subprocess.run(command, cwd=cwd, env=env, check=True)


def main() -> int:
    measurement = Path(os.environ["PGS_RELEASE_MEASUREMENT_JSON"])
    raw_dir = measurement.parent / "criterion-raw"
    baseline_snapshot = raw_dir / "baseline"
    target_criterion = ROOT / "target/criterion"
    env = os.environ.copy()
    env["BENCH_QUICK"] = "1"
    contract = json.loads(QUALIFICATION.read_text(encoding="utf-8"))
    criterion_budget = next(
        item for item in contract["performance_budgets"] if item["id"] == "criterion-regression"
    )
    # ponytail: 250 ns ignores code-layout jitter below 0.25 µs; lower with repeatable user-path evidence.

    try:
        shutil.rmtree(target_criterion, ignore_errors=True)
        shutil.rmtree(raw_dir, ignore_errors=True)

        # Run the candidate before the baseline so both measurements see the
        # same fresh runner conditions.  Measuring the baseline first biases
        # tiny benchmarks toward a slower candidate after the runner warms up.
        run(
            [
                "bash",
                "scripts/run_benchmarks.sh",
                "--",
            ],
            cwd=ROOT,
            env=env,
        )
        if not target_criterion.is_dir():
            raise RuntimeError("candidate Criterion run produced no raw estimates")
        candidate_snapshots = []
        candidate_snapshot = raw_dir / "candidate"
        shutil.copytree(target_criterion, candidate_snapshot, dirs_exist_ok=True)
        candidate_snapshots.append(candidate_snapshot)

        with tempfile.TemporaryDirectory(prefix="pgtrickle-v01060-baseline-") as temp_name:
            worktree = Path(temp_name) / "baseline"
            run(["git", "worktree", "add", "--detach", str(worktree), BASELINE_TAG], cwd=ROOT)
            try:
                run(
                    [
                        "bash",
                        "scripts/run_benchmarks.sh",
                        "--",
                        "--save-baseline",
                        BASELINE_VERSION,
                    ],
                    cwd=worktree,
                    env=env,
                )
                source = worktree / "target/criterion"
                if not source.is_dir():
                    raise RuntimeError("baseline Criterion run produced no raw estimates")
                shutil.copytree(source, baseline_snapshot, dirs_exist_ok=True)
                for baseline in source.rglob(BASELINE_VERSION):
                    if baseline.is_dir():
                        destination = target_criterion / baseline.relative_to(source)
                        shutil.copytree(baseline, destination, dirs_exist_ok=True)
            finally:
                subprocess.run(["git", "worktree", "remove", "--force", str(worktree)], cwd=ROOT, check=False)

        comparison_failed = True
        for attempt in range(MAX_COMPARISON_ATTEMPTS):
            if attempt:
                # ponytail: one retry absorbs transient hosted-run noise; a repeatable regression still fails.
                for result in target_criterion.rglob("new"):
                    if result.is_dir():
                        shutil.rmtree(result)
                run(
                    [
                        "bash",
                        "scripts/run_benchmarks.sh",
                        "--",
                        "--baseline",
                        BASELINE_VERSION,
                    ],
                    cwd=ROOT,
                    env=env,
                )
                if not target_criterion.is_dir():
                    raise RuntimeError("candidate Criterion run produced no raw estimates")
                snapshot = raw_dir / f"candidate-retry-{attempt}"
                shutil.copytree(target_criterion, snapshot, dirs_exist_ok=True)
                candidate_snapshots.append(snapshot)
            try:
                run(
                    [
                        sys.executable,
                        "scripts/criterion_regression_check.py",
                        "--threshold",
                        str(float(criterion_budget["threshold"]) / 100),
                        "--minimum-delta-ns",
                        str(criterion_budget["minimum_absolute_delta_ns"]),
                        "--baseline",
                        BASELINE_VERSION,
                        "--require-pairs",
                        "--dir",
                        str(target_criterion),
                        "--json-output",
                        str(measurement),
                    ],
                    cwd=ROOT,
                )
                comparison_failed = False
                break
            except subprocess.CalledProcessError:
                if attempt + 1 < MAX_COMPARISON_ATTEMPTS:
                    print("Criterion comparison failed; retrying once on the same runner", flush=True)

        archive = raw_dir / "criterion-estimates.tar.gz"
        with tarfile.open(archive, "w:gz") as evidence:
            evidence.add(baseline_snapshot, arcname="baseline")
            for attempt, snapshot in enumerate(candidate_snapshots):
                evidence.add(snapshot, arcname="candidate" if attempt == 0 else f"candidate-retry-{attempt}")

        result = json.loads(measurement.read_text(encoding="utf-8"))
        result["raw_evidence_files"] = [archive.relative_to(ROOT).as_posix()]
        measurement.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        return int(comparison_failed)
    except (OSError, subprocess.CalledProcessError, RuntimeError) as error:
        print(f"release Criterion comparison failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
