#!/usr/bin/env python3
"""Drain revision-safe demo jobs with a deterministic, local fake provider."""

import argparse
import hashlib
import json
import math
import random
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


def drain(model: str, lease_seconds: int, delay_seconds: float, max_jobs: int, duplicate: bool, dimensions: int = 3,
          delay_profile: str = "fixed", delay_seed: int = 0) -> dict[str, object]:
    counts = {"claimed": 0, "accepted": 0, "stale": 0, "duplicate_rejected": 0}
    rng = random.Random(delay_seed)
    delays = []
    for _ in range(max_jobs):
        result = psql(CLAIM, model=model, lease_seconds=str(lease_seconds))
        if not result:
            break
        claim = json.loads(result)
        counts["claimed"] += 1
        delay = rng.uniform(0, 2 * delay_seconds) if delay_profile == "uniform" else delay_seconds
        delays.append(delay)
        if delay:
            time.sleep(delay)
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
    ordered = sorted(delays)
    summary = {"samples": len(ordered), "p50": ordered[max(0, math.ceil(len(ordered) * .50) - 1)] if ordered else 0,
               "p95": ordered[max(0, math.ceil(len(ordered) * .95) - 1)] if ordered else 0,
               "p99": ordered[max(0, math.ceil(len(ordered) * .99) - 1)] if ordered else 0,
               "max": ordered[-1] if ordered else 0}
    return {**counts, "embedding_delay_seconds": summary}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True)
    parser.add_argument("--lease-seconds", type=int, default=60)
    parser.add_argument("--delay-seconds", type=float, default=0)
    parser.add_argument("--delay-profile", choices=("fixed", "uniform"), default="fixed",
                        help="fixed delay or deterministic uniform delay from zero to twice the mean")
    parser.add_argument("--delay-seed", type=int, default=0)
    parser.add_argument("--max-jobs", type=int, default=1000)
    parser.add_argument("--dimensions", type=int, default=3)
    parser.add_argument("--duplicate", action="store_true")
    args = parser.parse_args()
    if not args.model or not 1 <= args.lease_seconds <= 3600 or not 0 <= args.delay_seconds <= 3600 or not 1 <= args.max_jobs <= 1_000_000 or not 1 <= args.dimensions <= 1536:
        parser.error("model must be nonempty; lease, delay, job and dimension limits must be in range")
    result = drain(args.model, args.lease_seconds, args.delay_seconds, args.max_jobs, args.duplicate,
                   args.dimensions, args.delay_profile, args.delay_seed)
    print(json.dumps({"model": args.model, "dimensions": args.dimensions, "delay_profile": args.delay_profile,
                      "delay_seed": args.delay_seed, **result}, sort_keys=True))


if __name__ == "__main__":
    main()
