#!/usr/bin/env python3
"""Verify temporary-server worker isolation without changing runtime settings."""
import json
from pathlib import Path
import subprocess
import sys
import time
import uuid


def main():
    image, destination = sys.argv[1:]
    output = Path(destination).resolve()
    output.mkdir(parents=True, exist_ok=True)
    observer = output / "observe-init.sql"
    observer.write_text("""COPY (SELECT json_build_object(
        'max_worker_processes', current_setting('max_worker_processes')::integer,
        'extension_workers', (SELECT count(*) FROM pg_stat_activity
            WHERE backend_type LIKE 'pg_trickle%' OR backend_type LIKE 'pg-react%')))
        TO '/tmp/s1-init-settings.json';
""", encoding="utf-8")
    container = "s1-bootstrap-" + uuid.uuid4().hex[:12]
    started = time.monotonic()
    created = False

    def command(*args):
        return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT, timeout=20).strip()

    try:
        command("docker", "run", "--detach", "--name", container, "--network", "none",
                "--env", "POSTGRES_HOST_AUTH_METHOD=trust", "--volume",
                f"{observer}:/docker-entrypoint-initdb.d/20-observe.sql:ro", image,
                "postgres", "-c", "shared_preload_libraries=pg_trickle,pg_react",
                "-c", "pg_trickle.enabled=off", "-c", "shared_buffers=8MB")
        created = True
        deadline = started + 60
        while time.monotonic() < deadline:
            ready = subprocess.run(["docker", "exec", container, "pg_isready", "-h",
                                    "127.0.0.1", "-U", "postgres"], capture_output=True, timeout=10)
            if ready.returncode == 0:
                break
            time.sleep(0.1)
        else:
            raise AssertionError("fresh server exceeded unchanged 60-second readiness bound")
        initialization = json.loads(command("docker", "exec", container, "cat", "/tmp/s1-init-settings.json"))
        runtime = json.loads(command("docker", "exec", container, "psql", "-XAtq", "-U", "postgres", "-c",
            "SELECT json_build_object('max_worker_processes', current_setting('max_worker_processes')::integer, "
            "'shared_preload_libraries', current_setting('shared_preload_libraries'), "
            "'enabled', current_setting('pg_trickle.enabled'), "
            "'shared_buffers', current_setting('shared_buffers'), "
            "'extensions', (SELECT array_agg(extname::text ORDER BY extname) FROM pg_extension "
            "WHERE extname IN ('pg_trickle','pg_react')))"))
        assert initialization == {"max_worker_processes": 0, "extension_workers": 0}, initialization
        assert runtime == dict(max_worker_processes=8, shared_preload_libraries="pg_trickle,pg_react",
                               enabled="off", shared_buffers="8MB", extensions=["pg_react", "pg_trickle"]), runtime
        (output / "result.json").write_text(json.dumps(dict(image=image, initialization=initialization,
            runtime=runtime, readiness_seconds=time.monotonic()-started, result="PASS"), indent=2) + "\n")
        print("temporary-server worker isolation and unchanged runtime settings: PASS")
    finally:
        if created:
            subprocess.run(["docker", "stop", "--time", "5", container], capture_output=True, timeout=15)
            logs = subprocess.run(["docker", "logs", "--timestamps", container], capture_output=True, text=True, timeout=10)
            (output / "postgres.log").write_text(logs.stdout + logs.stderr)
            subprocess.run(["docker", "rm", "--volumes", container], capture_output=True, timeout=10)


if __name__ == "__main__":
    main()
