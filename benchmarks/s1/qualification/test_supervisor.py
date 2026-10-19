"""Fault-inject the two supervision failures without touching Docker."""
import importlib.util
import json
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location("bounded", Path(__file__).with_name("run_bounded.py"))
bounded = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bounded)
image = "sha256:" + "a" * 64
name = "pgreact-s1-123456abcdef"

with tempfile.TemporaryDirectory(prefix="issue1168-logs-") as directory:
    path = Path(directory) / "full.log"
    first, second = Mock(), Mock()
    first.poll.return_value = None
    first.wait.return_value = 0
    with patch.object(bounded.subprocess, "Popen", side_effect=[first, second]) as popen:
        previous = bounded.capture_logs(name, path)
        assert bounded.capture_logs(name, path, previous) is previous
        assert popen.call_count == 1
        path.write_text("before restart\n")
        first.poll.return_value = 0
        following = bounded.capture_logs(name, path, previous)
        assert previous[1].closed and following[0] is second
        assert popen.call_count == 2 and path.read_text() == ""
        following[1].close()
    first.wait.return_value = 1
    try:
        bounded.capture_logs(name, path, previous)
    except RuntimeError as error:
        assert str(error) == "container log capture failed"
    else:
        raise AssertionError("failed log capture must fail supervision")

for failure in ("timeout", "daemon"):
    stopped = []

    class Process:
        def __init__(self, args, stdout, **kwargs):
            self.logs = args[1] == "logs"
            self.code = None
            stdout.write("retained full log\n" if self.logs else f"CONTAINER: {name}\n")
            stdout.flush()

        def poll(self):
            return self.code

        def wait(self, timeout):
            self.code = 0 if self.logs else 1
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
        if failure == "timeout":
            assert (root / failure / f"{name}-full.log").read_text() == "retained full log\n"

for failures, expected_calls in ((2, 3), (5, 5)):
    failed = subprocess.CompletedProcess([], 2, "", "psql: error: connection to server failed: No such file or directory")
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

assert not bounded.postgres_is_restarting(subprocess.CompletedProcess([], 1, "", "ERROR: No such file or directory"))

for persists in (False, True):
    processes = []
    clock = [0]

    class RestartProcess:
        def __init__(self, args, stdout, **kwargs):
            self.logs = args[1] == "logs"
            self.code = None
            stdout.write("retained restart log\n" if self.logs else f"CONTAINER: {name}\n")
            stdout.flush()
            processes.append(self)

        def poll(self):
            return self.code

        def wait(self, timeout):
            if self.logs:
                return 0
            self.code = 1 if persists else 0
            return self.code

        def terminate(self):
            self.code = -15

    def restart_run(args, **kwargs):
        if args[1] == "inspect":
            value = image if args[-1] == "{{.Image}}" else image + " true"
            return subprocess.CompletedProcess(args, 0, value + "\n", "")
        if args[1] == "stop":
            processes[0].code = 1
            return subprocess.CompletedProcess(args, 0, "", "")
        if args[1] == "exec":
            if "psql" in args:
                return subprocess.CompletedProcess(args, 2, "", "psql: error: connection to server failed: the database system is shutting down")
            return subprocess.CompletedProcess(args, 0, "100 /data\n" if "du -sk" in args[-1] else "128\n", "")
        raise AssertionError(args)

    def advance(seconds):
        if seconds == 10:
            if persists:
                clock[0] = 61
            else:
                processes[0].code = 0

    with tempfile.TemporaryDirectory(prefix="issue1168-restart-sampling-") as directory:
        root = Path(directory)
        with patch.object(bounded, "__file__", str(root / "run_bounded.py")), \
             patch.object(bounded.subprocess, "Popen", RestartProcess), \
             patch.object(bounded.subprocess, "run", restart_run), \
             patch.object(bounded.time, "monotonic", lambda: clock[0]), \
             patch.object(bounded.time, "sleep", advance):
            assert bounded.main(image, "restart") == int(persists)
        receipt = json.loads((root / "restart/bounded-run.json").read_text())
        sample = json.loads((root / "restart/resource-samples.jsonl").read_text().splitlines()[0])
        assert sample["containers"][name]["data_bytes"] == 102400
        assert sample["memory_bytes"] == 128
        assert name in sample["postgres_metrics_unavailable"]
        assert (receipt["supervision_error"] is None) != persists
        if persists:
            assert "PostgreSQL metrics unavailable for 60 seconds" in receipt["supervision_error"]

vanished = subprocess.CompletedProcess([], 1, "1024 /data\n", "du: cannot access '/data/base/22293/t1_61725': No such file or directory\n")
healthy = subprocess.CompletedProcess([], 0, "1024 /data\n", "")
for failures, expected_calls in ((2, 3), (5, 5)):
    with patch.object(bounded.subprocess, "run", side_effect=[vanished] * failures + [healthy]) as run_mock, \
         patch.object(bounded.time, "sleep"):
        result = bounded.disk_metrics([])
    assert run_mock.call_count == expected_calls
    assert result is (healthy if failures == 2 else vanished)
for message in ("permission denied", "du: cannot access '/data/global/pg_control': No such file or directory",
                vanished.stderr + "permission denied"):
    with patch.object(bounded.subprocess, "run", return_value=subprocess.CompletedProcess([], 1, "", message)) as run_mock:
        assert bounded.disk_metrics([]).returncode == 1
        assert run_mock.call_count == 1

for exited in (False, True):
    processes = []
    persists = not exited
    memory_reads = [0]

    def memory_run(args, **kwargs):
        if args[1] == "inspect":
            value = (image if args[-1] == "{{.Image}}" else
                     ("false" if exited else "true") if args[-1] == "{{.State.Running}}" else
                     image + " true")
            return subprocess.CompletedProcess(args, 0, value + "\n", "")
        if args[1] == "stop":
            processes[0].code = 1
            return subprocess.CompletedProcess(args, 0, "", "")
        if args[1] == "exec":
            if "psql" in args:
                return subprocess.CompletedProcess(args, 0, '{"database_bytes":1,"wal_bytes":2}', "")
            if "du -sk" in args[-1]:
                return subprocess.CompletedProcess(args, 0, "100 /data\n", "")
            memory_reads[0] += 1
            if memory_reads[0] == 1:
                return subprocess.CompletedProcess(args, 0, "128\n", "")
            if exited:
                processes[0].code = 0
            return subprocess.CompletedProcess(args, 1, "", "memory read failed")
        raise AssertionError(args)

    with tempfile.TemporaryDirectory(prefix="issue1168-memory-exit-") as directory:
        root = Path(directory)
        with patch.object(bounded, "__file__", str(root / "run_bounded.py")), \
             patch.object(bounded.subprocess, "Popen", RestartProcess), \
             patch.object(bounded.subprocess, "run", memory_run), \
             patch.object(bounded.time, "sleep"):
            assert bounded.main(image, "memory") == int(not exited)
        receipt = json.loads((root / "memory/bounded-run.json").read_text())
        assert receipt["peak_memory_bytes"] == 128
        assert receipt["conservative_used_bytes"] >= 102400
        assert bool(receipt["measurement_errors"]) != exited
        assert bool(receipt["exited_container_samples"]) == exited
        assert (receipt["stop_reason"] is None) == exited

print("supervisor fault-injection and bounded restart retry checks: PASS")
