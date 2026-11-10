#!/usr/bin/env python3
"""Drain revision-safe demo jobs with a deterministic, local fake provider."""

import argparse
import hashlib
import json
import subprocess
import time


CLAIM = """SELECT row_to_json(c)::text FROM embedding_demo.claim(:'model',
    pg_catalog.make_interval(secs => :'lease_seconds'::integer)) AS c;"""
COMPLETE = """SELECT embedding_demo.complete(:'job'::uuid, :'token'::uuid,
    :'embedding'::vector);"""


def embedding(body: str, model: str, dimensions: int = 3) -> str:
    source = (model + "\0" + body).encode("utf-8")
    digest = hashlib.sha256(source).digest() if dimensions <= 8 else hashlib.shake_256(source).digest(dimensions * 4)
    values = [int.from_bytes(digest[offset : offset + 4], "big") % 2001 - 1000 for offset in range(0, dimensions * 4, 4)]
    if not any(values):
        values[0] = 1
    return "[" + ",".join(str(value / 1000) for value in values) + "]"


def psql(sql: str, **variables: str) -> str:
    command = ["psql", "-X", "-w", "-qAt", "-v", "ON_ERROR_STOP=1"]
    for name, value in variables.items():
        command.extend(["-v", f"{name}={value}"])
    return subprocess.run(command, input=sql, text=True, stdout=subprocess.PIPE, check=True).stdout.strip()


def drain(model: str, lease_seconds: int, delay_seconds: float, max_jobs: int, duplicate: bool, dimensions: int = 3) -> dict[str, int]:
    counts = {"claimed": 0, "accepted": 0, "stale": 0, "duplicate_rejected": 0}
    for _ in range(max_jobs):
        result = psql(CLAIM, model=model, lease_seconds=str(lease_seconds))
        if not result:
            break
        claim = json.loads(result)
        counts["claimed"] += 1
        if delay_seconds:
            time.sleep(delay_seconds)
        args = {"job": claim["claimed_job"], "token": claim["claimed_token"],
                "embedding": embedding(claim["source_body"], model, dimensions)}
        accepted = psql(COMPLETE, **args)
        if accepted not in {"t", "f"}:
            raise ValueError("completion did not return a PostgreSQL boolean")
        counts["accepted" if accepted == "t" else "stale"] += 1
        if duplicate:
            if psql(COMPLETE, **args) != "f":
                raise ValueError("duplicate completion was unexpectedly accepted")
            counts["duplicate_rejected"] += 1
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True)
    parser.add_argument("--lease-seconds", type=int, default=60)
    parser.add_argument("--delay-seconds", type=float, default=0)
    parser.add_argument("--max-jobs", type=int, default=1000)
    parser.add_argument("--dimensions", type=int, default=3)
    parser.add_argument("--duplicate", action="store_true")
    args = parser.parse_args()
    if not args.model or not 1 <= args.lease_seconds <= 3600 or not 0 <= args.delay_seconds <= 3600 or not 1 <= args.max_jobs <= 1_000_000 or not 1 <= args.dimensions <= 1536:
        parser.error("model must be nonempty; lease, delay, job and dimension limits must be in range")
    result = drain(args.model, args.lease_seconds, args.delay_seconds, args.max_jobs, args.duplicate, args.dimensions)
    print(json.dumps({"model": args.model, "dimensions": args.dimensions, **result}, sort_keys=True))


if __name__ == "__main__":
    main()
