#!/usr/bin/env python3
"""Run the declared pgvector cases against the selected candidate image."""

import json
import os
import subprocess
import sys
from pathlib import Path


def main() -> int:
    version = sys.argv[1]
    if version not in {"0.8.1", "0.8.7"}:
        raise ValueError(f"unsupported pgvector version: {version}")
    contract = json.loads(Path("tests/release/v0.109.0-qualification.json").read_text())
    suite = next(item for item in contract["required_suites"] if item["id"] == f"pgvector-{version}")
    cases = [case.split("::", 1) for case in suite["required_cases"]]
    binaries = sorted({binary for binary, _ in cases})
    expression = " | ".join(f"test({name})" for _, name in cases)
    command = ["bash", "scripts/run_e2e_tests.sh", "--no-capture"]
    for binary in binaries:
        command.extend(["--test", binary])
    command.extend(["-E", expression])
    env = os.environ.copy()
    env["PGVECTOR_EXPECTED_VERSION"] = version
    return subprocess.run(command, env=env, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
