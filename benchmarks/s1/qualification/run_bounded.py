"""Run only the task's S1 harness, supervising its own containers and storage."""

import json
import os
import re
import shutil
import subprocess
import sys
import time
import traceback
from contextlib import ExitStack
from pathlib import Path


def usage(samples, log_bytes):
    return sum(max(row["data_bytes"], row["database_bytes"] + row["wal_bytes"])
               for row in samples.values()) + log_bytes


def container_has_exited(code, stdout, stderr):
    return (code == 0 and stdout.strip() == "false" or
            code != 0 and any(message in stderr.lower()
                              for message in ("no such object:", "no such container:")))


def postgres_is_restarting(result):
    # The harness deliberately restarts PostgreSQL, and Docker may report a
    # stopped container during a sample even if it is running again by the
    # next inspect. Retry only this exact transition, not Docker daemon errors.
    docker_transition = re.fullmatch(
        r"Error response from daemon: container [0-9a-f]{64} is not running",
        result.stderr.strip(),
    )
    return (result.returncode != 0 and (bool(docker_transition) or
            ('connection to server' in result.stderr
             and any(message in result.stderr for message in (
                 'No such file or directory', 'the database system is starting up',
                 'the database system is shutting down', 'server closed the connection unexpectedly')))))


def postgres_metrics(args):
    # The frozen worker-health checks deliberately restart PostgreSQL.
    for attempt in range(5):
        result = subprocess.run(args, capture_output=True, text=True, timeout=15)
        if not postgres_is_restarting(result) or attempt == 4:
            return result
        time.sleep(1)


def capture_logs(name, path, previous=None):
    if previous is not None:
        follower, stream = previous
        if follower.poll() is None:
            return previous
        stream.close()
        if follower.wait(timeout=0) != 0:
            raise RuntimeError("container log capture failed")
    # Docker ends a follow stream on restart; replay retained logs before following again.
    stream = path.open("w")
    try:
        follower = subprocess.Popen(["docker", "logs", "--follow", "--timestamps", name],
                                    stdout=stream, stderr=subprocess.STDOUT)
    except BaseException:
        stream.close()
        raise
    return follower, stream


def disk_metrics(args):
    # Temporary relations and pg_internal.init can disappear during PostgreSQL
    # maintenance/restarts while du walks the data directory. Retry these
    # specific ENOENT paths, never arbitrary missing files or permission errors.
    vanished = (r"du: cannot access '.*/base/[0-9]+/(?:"
                r"t[0-9]+_[0-9]+(?:_(?:fsm|vm|init))?(?:\.[0-9]+)?"
                r"|pg_internal\.init)': No such file or directory")
    for attempt in range(5):
        result = subprocess.run(args, capture_output=True, text=True, timeout=15)
        transient = result.stderr.splitlines()
        if (result.returncode == 0 or not transient or attempt == 4
                or not all(re.fullmatch(vanished, line) for line in transient)):
            return result
        time.sleep(0.2)


def main(image, run_name, profile_cases=None):
    root = Path(__file__).resolve().parent
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", image)
    assert re.fullmatch(r"[a-z0-9-]+", run_name)
    run = root / run_name
    run.mkdir(exist_ok=False)
    output = run / "results"
    harness = root / "tests/integrations/pg-mdm/time/audit-r53-repair.py"
    env = os.environ.copy()
    env.pop("R53_PROFILE_CASE_COUNT", None)
    if profile_cases is not None:
        assert 1 <= profile_cases <= 300
        env["R53_PROFILE_CASE_COUNT"] = str(profile_cases)
    samples = {}
    postgres_unavailable_since = {}
    peak_memory_bytes = 0
    measurement_errors = []
    exited_container_samples = []
    stop_reason = None
    started = time.monotonic()
    names = set()
    process = None
    supervision_error = None
    try:
        with (run / "run.log").open("w") as log, ExitStack() as cleanup:
            process = subprocess.Popen([sys.executable, str(harness), image, str(output)],
                                       stdout=log, stderr=subprocess.STDOUT, env=env)
            log_followers = {}

            def finish_logs():
                for follower, stream in log_followers.values():
                    try:
                        code = follower.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        follower.terminate()
                        follower.wait(timeout=15)
                        raise RuntimeError("container log capture did not finish")
                    finally:
                        stream.close()
                    if code != 0:
                        raise RuntimeError(f"container log capture failed: {code}")

            cleanup.callback(finish_logs)

            def stop_owned():
                if process.poll() is not None:
                    return
                for name in sorted(names):
                    inspected = subprocess.run(
                        ["docker", "inspect", name, "--format", "{{.Image}}"],
                        capture_output=True, text=True, timeout=15)
                    if inspected.returncode == 0 and inspected.stdout.strip() == image:
                        subprocess.run(["docker", "stop", "--time", "5", name],
                                       capture_output=True, timeout=20, check=True)
                try:
                    process.wait(timeout=120)
                except subprocess.TimeoutExpired:
                    process.terminate()
                    process.wait(timeout=30)

            cleanup.callback(stop_owned)
            while process.poll() is None:
                text = (run / "run.log").read_text()
                names = set(re.findall(r"^CONTAINER: (pgreact-s1-[0-9a-f]{12})$", text, re.M))
                memory_bytes = 0
                postgres_unavailable = {}
                for name in sorted(names):
                    inspected = subprocess.run(
                        ["docker", "inspect", name,
                         "--format", "{{.Image}} {{.State.Running}}"],
                        capture_output=True, text=True, timeout=15)
                    if inspected.returncode:
                        if container_has_exited(inspected.returncode, inspected.stdout, inspected.stderr):
                            continue
                        raise RuntimeError(f"container inspection failed: {inspected.stderr}")
                    identity, running = inspected.stdout.strip().split()
                    assert identity == image, (name, identity)
                    if running != "true":
                        continue
                    log_followers[name] = capture_logs(
                        name, run / f"{name}-full.log", log_followers.get(name))
                    measured = postgres_metrics(
                        ["docker", "exec", name, "psql", "-XAtq", "-U", "postgres",
                         "-d", "postgres", "-c", "SELECT json_build_object('database_bytes',"
                         "(SELECT sum(pg_database_size(oid)) FROM pg_database),"
                         "'wal_bytes',(SELECT wal_bytes FROM pg_stat_wal))"])
                    disk = disk_metrics(
                        ["docker", "exec", name, "sh", "-c", 'du -sk "$PGDATA"'])
                    if disk.returncode or measured.returncode and not postgres_is_restarting(measured):
                        state = subprocess.run(
                            ["docker", "inspect", name,
                             "--format", "{{.State.Running}}"],
                            capture_output=True, text=True, timeout=15)
                        if container_has_exited(state.returncode, state.stdout, state.stderr):
                            continue
                        raise RuntimeError(f"resource measurement failed: {measured.stderr} {disk.stderr}")
                    if measured.returncode:
                        now = time.monotonic()
                        since = postgres_unavailable_since.setdefault(name, now)
                        if now - since >= 60:
                            raise RuntimeError(f"PostgreSQL metrics unavailable for 60 seconds: {measured.stderr}")
                        postgres_unavailable[name] = {"seconds": now - since, "error": measured.stderr}
                        row = {key: samples.get(name, {}).get(key, 0)
                               for key in ("database_bytes", "wal_bytes")}
                    else:
                        postgres_unavailable_since.pop(name, None)
                        row = json.loads(measured.stdout)
                    row["data_bytes"] = int(disk.stdout.split()[0]) * 1024
                    samples[name] = {key: max(value, samples.get(name, {}).get(key, 0))
                                     for key, value in row.items()}
                    stats = subprocess.run(
                        ["docker", "exec", name, "cat", "/sys/fs/cgroup/memory.current"],
                        capture_output=True, text=True, timeout=15)
                    if stats.returncode:
                        state = subprocess.run(
                            ["docker", "inspect", name,
                             "--format", "{{.State.Running}}"],
                            capture_output=True, text=True, timeout=15)
                        exited = container_has_exited(state.returncode, state.stdout, state.stderr)
                        if exited:
                            exited_container_samples.append({"container": name,
                                "elapsed_seconds": time.monotonic() - started,
                                "measurement": "cgroup_memory", "container_exited": True})
                            continue
                        measurement_errors.append({"container": name, "exit_code": stats.returncode,
                                                   "container_exited": False})
                        stop_reason = "memory measurement failed for running container"
                        break
                    memory_bytes += int(stats.stdout.strip())
                if stop_reason is not None:
                    stop_owned()
                    break
                peak_memory_bytes = max(peak_memory_bytes, memory_bytes)
                log_bytes = sum(p.stat().st_size for p in run.rglob("*") if p.is_file())
                used = usage(samples, log_bytes)
                host_free_bytes = shutil.disk_usage(root).free
                with (run / "resource-samples.jsonl").open("a") as receipts:
                    receipts.write(json.dumps({"elapsed_seconds": time.monotonic() - started,
                                               "containers": samples, "log_bytes": log_bytes,
                                               "memory_bytes": memory_bytes,
                                               "postgres_metrics_unavailable": postgres_unavailable,
                                               "host_free_bytes": host_free_bytes,
                                               "conservative_used_bytes": used}) + "\n")
                if (used >= 9 * 1024**3 or memory_bytes >= 4 * 1024**3
                        or host_free_bytes < 10 * 1024**3 or time.monotonic() - started > 2700):
                    stop_reason = ("9 GiB storage stop threshold" if used >= 9 * 1024**3 else
                                   "4 GiB memory stop threshold" if memory_bytes >= 4 * 1024**3 else
                                   "10 GiB host free-space reserve" if host_free_bytes < 10 * 1024**3 else
                                   "45 minute timeout")
                    stop_owned()
                    break
                time.sleep(10)
            try:
                code = process.wait(timeout=120)
            except subprocess.TimeoutExpired:
                process.terminate()
                code = process.wait(timeout=30)
    except Exception as error:
        supervision_error = f"{type(error).__name__}: {error}"
        stop_reason = stop_reason or "resource supervision failed"
        (run / "supervision-error.log").write_text(traceback.format_exc())
        if process is None:
            code = -1
        else:
            code = process.poll()
            if code is None:
                process.terminate()
                try:
                    code = process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    code = process.wait(timeout=30)
    result = {"image": image, "exit_code": code, "stop_reason": stop_reason,
              "measurement_errors": measurement_errors,
              "exited_container_samples": exited_container_samples,
              "supervision_error": supervision_error,
              "peak_memory_bytes": peak_memory_bytes,
              "elapsed_seconds": time.monotonic() - started,
              "conservative_used_bytes": usage(samples, sum(
                  p.stat().st_size for p in run.rglob("*") if p.is_file()))}
    (run / "bounded-run.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))
    return code if code else int(stop_reason is not None)


if __name__ == "__main__":
    assert container_has_exited(0, "false\n", "")
    assert container_has_exited(1, "", "Error: No such object: removed")
    assert not container_has_exited(0, "true\n", "")
    assert not container_has_exited(1, "", "Cannot connect to the Docker daemon")
    assert usage({"one": {"data_bytes": 20, "database_bytes": 4, "wal_bytes": 6},
                  "two": {"data_bytes": 2, "database_bytes": 3, "wal_bytes": 4}}, 5) == 32
    if sys.argv[1:] == ["--self-check"]:
        print("self-check: PASS")
    else:
        sys.exit(main(*sys.argv[1:3], int(sys.argv[3]) if len(sys.argv) == 4 else None))
