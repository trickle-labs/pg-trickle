#!/usr/bin/env python3
"""Poll authorized ANN requests and run the one-request psql worker serially."""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PAGE_SQL = """
SET statement_timeout = '5s';
SELECT pg_catalog.jsonb_build_object(
    'id', (r->>'request_id')::bigint,
    'eligible', r->>'state' IN ('REQUESTED', 'FAILED', 'INTERRUPTED')
        AND COALESCE((r->>'identity_current')::boolean, false)
        AND (r->>'next_eligible_at' IS NULL OR (r->>'next_eligible_at')::timestamptz <= pg_catalog.clock_timestamp())
        AND COALESCE((r->>'consecutive_failures')::integer, 0) < :'max_failures'::integer
)::text
FROM pgtrickle.list_ann_maintenance_requests(:'cursor'::bigint, :'page_size'::integer) r
ORDER BY (r->>'request_id')::bigint;
"""


def bounded_env(name: str, default: int, minimum: int, maximum: int) -> int:
    value = int(os.environ.get(name, str(default)))
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def parse_page(output: str, cursor: int, page_size: int) -> list[dict]:
    rows = [json.loads(line) for line in output.splitlines() if line.strip()]
    if len(rows) > page_size:
        raise ValueError("request page exceeded its limit")
    previous = cursor
    for row in rows:
        if not isinstance(row, dict) or type(row.get("id")) is not int or row["id"] <= previous or type(row.get("eligible")) is not bool:
            raise ValueError("request page contains an invalid or unordered row")
        previous = row["id"]
    return rows


def main() -> int:
    interval = bounded_env("PGS_ANN_POLL_SECONDS", 30, 1, 60)
    page_size = bounded_env("PGS_ANN_PAGE_SIZE", 100, 1, 1000)
    max_pages = bounded_env("PGS_ANN_MAX_PAGES_PER_POLL", 10, 1, 100)
    max_failures = bounded_env("PGS_ANN_MAX_FAILURES", 6, 1, 6)
    environment = os.environ.copy()
    environment.setdefault("PGCONNECT_TIMEOUT", "10")
    stopped = False
    child = None

    def stop(_signum, _frame):
        nonlocal stopped
        stopped = True
        if child is not None and child.poll() is None:
            child.terminate()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    cursor = 0
    while not stopped:
        try:
            for _ in range(max_pages):
                child = subprocess.Popen(
                    ["psql", "-X", "-w", "-qAt", "-v", "ON_ERROR_STOP=1",
                     "-v", f"cursor={cursor}", "-v", f"page_size={page_size}",
                     "-v", f"max_failures={max_failures}"],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=environment,
                )
                output, _ = child.communicate(PAGE_SQL)
                if child.returncode != 0:
                    raise RuntimeError("request listing failed; retrying after poll interval")
                rows = parse_page(output, cursor, page_size)
                for row in rows:
                    if stopped:
                        break
                    if row["eligible"]:
                        child = subprocess.Popen(
                            ["psql", "-X", "-w", "-v", f"request_id={row['id']}",
                             "-v", f"ann_lock_timeout={os.environ.get('PGS_ANN_LOCK_TIMEOUT', '5s')}",
                             "-v", f"ann_statement_timeout={os.environ.get('PGS_ANN_STATEMENT_TIMEOUT', '30min')}",
                             "-f", str(ROOT / "scripts/ann_maintenance_worker.sql")],
                            env=environment,
                        )
                        if child.wait() != 0 and not stopped:
                            print(f"ANN request {row['id']} failed; inspect its status", file=sys.stderr, flush=True)
                if stopped:
                    break
                if len(rows) < page_size:
                    cursor = 0
                    break
                cursor = rows[-1]["id"]
        except (OSError, ValueError, json.JSONDecodeError, RuntimeError) as error:
            print(f"ANN supervisor: {error}", file=sys.stderr, flush=True)
        if not stopped:
            time.sleep(interval)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValueError as error:
        print(f"ANN supervisor: {error}", file=sys.stderr)
        raise SystemExit(2)
