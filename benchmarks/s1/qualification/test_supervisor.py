"""Fault-inject the two supervision failures without touching Docker."""
import importlib.util
import json
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("bounded", Path(__file__).with_name("run_bounded.py"))
bounded = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bounded)
image = "sha256:" + "a" * 64
name = "pgreact-s1-123456abcdef"

for failure in ("timeout", "daemon"):
    stopped = []

    class Process:
        def __init__(self, args, stdout, **kwargs):
            self.code = None
            stdout.write(f"CONTAINER: {name}\n")
            stdout.flush()

        def poll(self):
            return self.code

        def wait(self, timeout):
            self.code = 1
            return self.code

        def terminate(self):
            self.code = -15

        def kill(self):
            self.code = -9

    def run(args, **kwargs):
        if args[1] == "inspect":
            if args[-1] == "{{.Image}}":
                return subprocess.CompletedProcess(args, 0, image + "\n", "")
            if failure == "daemon":
                return subprocess.CompletedProcess(args, 1, "", "Cannot connect to the Docker daemon")
            return subprocess.CompletedProcess(args, 0, image + " true\n", "")
        if args[1] == "stop":
            stopped.append(args[-1])
            return subprocess.CompletedProcess(args, 0, "", "")
        if args[1] == "exec":
            raise subprocess.TimeoutExpired(args, 15)
        raise AssertionError(args)

    with tempfile.TemporaryDirectory(prefix="issue1168-supervisor-") as directory:
        root = Path(directory)
        with patch.object(bounded, "__file__", str(root / "run_bounded.py")), \
             patch.object(bounded.subprocess, "Popen", Process), \
             patch.object(bounded.subprocess, "run", run):
            assert bounded.main(image, failure) == 1
        receipt = json.loads((root / failure / "bounded-run.json").read_text())
        assert stopped == [name], stopped
        assert receipt["stop_reason"] == "resource supervision failed", receipt
        assert receipt["supervision_error"].startswith(
            "TimeoutExpired:" if failure == "timeout" else "RuntimeError: container inspection failed:"), receipt
        assert (root / failure / "supervision-error.log").is_file()

for failures, expected_calls in ((2, 3), (5, 5)):
    failed = subprocess.CompletedProcess([], 2, "", "connection to server failed: No such file or directory")
    healthy = subprocess.CompletedProcess([], 0, '{"database_bytes":1,"wal_bytes":2}', "")
    with patch.object(bounded.subprocess, "run", side_effect=[failed] * failures + [healthy]) as run_mock, \
         patch.object(bounded.time, "sleep") as sleep_mock:
        result = bounded.postgres_metrics(["docker", "exec", name, "psql"])
    assert run_mock.call_count == expected_calls
    assert sleep_mock.call_count == expected_calls - 1
    assert result is (healthy if failures == 2 else failed)
with patch.object(bounded.subprocess, "run", return_value=subprocess.CompletedProcess([], 1, "", "permission denied")) as run_mock:
    assert bounded.postgres_metrics([]).returncode == 1
    assert run_mock.call_count == 1

print("supervisor fault-injection and bounded restart retry checks: PASS")
