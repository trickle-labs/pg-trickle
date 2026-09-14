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
BASELINE = "v0.105.3"


def run(command: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> None:
    print("$ " + " ".join(command), flush=True)
    subprocess.run(command, cwd=cwd, env=env, check=True)


def main() -> int:
    measurement = Path(os.environ["PGS_RELEASE_MEASUREMENT_JSON"])
    raw_dir = measurement.parent / "criterion-raw"
    baseline_snapshot = raw_dir / "baseline"
    candidate_snapshot = raw_dir / "candidate"
    target_criterion = ROOT / "target/criterion"
    env = os.environ.copy()
    env["BENCH_QUICK"] = "1"

    try:
        with tempfile.TemporaryDirectory(prefix="pgtrickle-v01053-baseline-") as temp_name:
            worktree = Path(temp_name) / "baseline"
            run(["git", "worktree", "add", "--detach", str(worktree), BASELINE], cwd=ROOT)
            try:
                run(
                    ["bash", "scripts/run_benchmarks.sh", "--", "--save-baseline", BASELINE],
                    cwd=worktree,
                    env=env,
                )
                source = worktree / "target/criterion"
                if not source.is_dir():
                    raise RuntimeError("baseline Criterion run produced no raw estimates")
                shutil.copytree(source, target_criterion, dirs_exist_ok=True)
                shutil.copytree(source, baseline_snapshot, dirs_exist_ok=True)
            finally:
                subprocess.run(["git", "worktree", "remove", "--force", str(worktree)], cwd=ROOT, check=False)

        if target_criterion.is_dir():
            for result in target_criterion.rglob("new"):
                if result.is_dir():
                    shutil.rmtree(result)
        run(
            ["bash", "scripts/run_benchmarks.sh", "--", "--baseline", BASELINE],
            cwd=ROOT,
            env=env,
        )
        run(
            [
                sys.executable,
                "scripts/criterion_regression_check.py",
                "--threshold", "0.10",
                "--baseline", BASELINE,
                "--require-pairs",
                "--json-output", str(measurement),
            ],
            cwd=ROOT,
        )
        if not target_criterion.is_dir():
            raise RuntimeError("candidate Criterion run produced no raw estimates")
        shutil.copytree(target_criterion, candidate_snapshot, dirs_exist_ok=True)
        archive = raw_dir / "criterion-estimates.tar.gz"
        with tarfile.open(archive, "w:gz") as evidence:
            evidence.add(baseline_snapshot, arcname="baseline")
            evidence.add(candidate_snapshot, arcname="candidate")
        result = json.loads(measurement.read_text(encoding="utf-8"))
        result["raw_evidence_files"] = [archive.relative_to(ROOT).as_posix()]
        measurement.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        return 0
    except (OSError, subprocess.CalledProcessError, RuntimeError) as error:
        print(f"release Criterion comparison failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
