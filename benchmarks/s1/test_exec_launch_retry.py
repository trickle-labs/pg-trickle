"""Fail-closed tests for Docker OCI exec process-start failures (no Docker required)."""

import importlib.util
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

audit_dir = Path(__file__).resolve().parent / "qualification/tests/integrations/pg-mdm/time"
sys.path.insert(0, str(audit_dir))
spec = importlib.util.spec_from_file_location("s1_audit_exec_retry", audit_dir / "audit-r53-repair.py")
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)

args = ["docker", "exec", "pgreact-s1-123456abcdef", "psql", "-XAtq", "-c", "SELECT 1"]
did_not_start = subprocess.CompletedProcess(
    args, 1, "", "OCI runtime exec failed: exec failed: unable to start container process: "
                 "procReady not received\n")
healthy = subprocess.CompletedProcess(args, 0, "1\n", "")

receipts = []
with patch.object(audit.subprocess, "run", side_effect=[did_not_start, healthy]) as runner, \
     patch.object(audit.time, "sleep") as sleeper:
    result = audit.run_docker_exec_with_launch_retry(
        args, on_retry=lambda attempt, command: receipts.append((attempt, command)))
    assert result is healthy and runner.call_count == 2
    assert receipts == [(1, args)] and sleeper.call_count == 1
    assert sleeper.call_args.args == (1,)

with patch.object(audit.subprocess, "run", return_value=did_not_start) as runner, \
     patch.object(audit.time, "sleep") as sleeper:
    result = audit.run_docker_exec_with_launch_retry(args)
    assert result is did_not_start and runner.call_count == 3
    assert sleeper.call_count == 2

for command, response in (
    (args, subprocess.CompletedProcess(args, 1, "", "ERROR: permission denied")),
    (args, subprocess.CompletedProcess(args, 1, "", "Cannot connect to the Docker daemon")),
    (args, subprocess.CompletedProcess(args, 1, "a SQL result\n", did_not_start.stderr)),
    (args, subprocess.CompletedProcess(args, 1, "", "OCI runtime exec failed: context canceled")),
    (["docker", "run", "image"], did_not_start),
):
    with patch.object(audit.subprocess, "run", return_value=response) as runner, \
         patch.object(audit.time, "sleep") as sleeper:
        assert audit.run_docker_exec_with_launch_retry(command) is response
        assert runner.call_count == 1 and sleeper.call_count == 0

with patch.object(audit.subprocess, "run", side_effect=subprocess.TimeoutExpired(args, 300)) as runner, \
     patch.object(audit.time, "sleep") as sleeper:
    try:
        audit.run_docker_exec_with_launch_retry(args)
    except subprocess.TimeoutExpired:
        pass
    else:
        raise AssertionError("timeout must not be retried")
    assert runner.call_count == 1 and sleeper.call_count == 0

print("S1 OCI exec did-not-start retries and fail-closed negatives: PASS")
