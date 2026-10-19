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

print("supervisor fault-injection checks: PASS")
