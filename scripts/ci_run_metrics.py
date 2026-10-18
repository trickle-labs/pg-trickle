#!/usr/bin/env python3
"""Summarize saved Actions REST run/jobs responses without conflating queue and execution."""

import argparse
import json
from datetime import datetime
from pathlib import Path


def minutes(start, end):
    return (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds() / 60


def summarize(run, jobs):
    active = [job for job in jobs if job["conclusion"] != "skipped"]
    if run["status"] != "completed" or any(not job["completed_at"] for job in active):
        raise ValueError("wait for a completed run; partial timings are not comparable")
    rows = [{
        "name": job["name"],
        "result": job["conclusion"],
        "creation_to_start_minutes": round(minutes(job["created_at"], job["started_at"]), 3),
        "execution_minutes": round(minutes(job["started_at"], job["completed_at"]), 3),
    } for job in active]
    return {
        "url": run["html_url"], "sha": run["head_sha"], "event": run["event"],
        "active_jobs": len(rows),
        "creation_to_start_minutes": round(sum(minutes(job["created_at"], job["started_at"]) for job in active), 3),
        "execution_minutes": round(sum(minutes(job["started_at"], job["completed_at"]) for job in active), 3),
        "jobs": rows,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run", type=Path)
    parser.add_argument("jobs", type=Path, help="gh api --paginate --slurp .../jobs JSON")
    args = parser.parse_args()
    pages = json.loads(args.jobs.read_text())
    if isinstance(pages, dict):
        pages = [pages]
    jobs = [job for page in pages for job in page["jobs"]]
    print(json.dumps(summarize(json.loads(args.run.read_text()), jobs), indent=2))
