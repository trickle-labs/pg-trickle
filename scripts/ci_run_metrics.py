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
        # Job creation follows dependency completion in Actions. This is elapsed
        # orchestration time, not a sum of dependency or runner execution times.
        "release_delay_minutes": round(minutes(run["created_at"], job["created_at"]), 3),
        "creation_to_start_minutes": round(minutes(job["created_at"], job["started_at"]), 3),
        "execution_minutes": round(minutes(job["started_at"], job["completed_at"]), 3),
        "steps": [{
            "name": step["name"], "result": step["conclusion"],
            "execution_minutes": round(minutes(step["started_at"], step["completed_at"]), 3),
        } for step in job.get("steps", [])
            if step.get("started_at") and step.get("completed_at")
            and step["conclusion"] != "skipped"],
    } for job in active]
    end = max((job["completed_at"] for job in active), default=run["created_at"])
    failures = [step["completed_at"] for job in active for step in job.get("steps", [])
                if step["conclusion"] == "failure" and step.get("completed_at")]
    wall = round(minutes(run["created_at"], end), 3)
    return {
        "url": run["html_url"], "sha": run["head_sha"], "event": run["event"],
        "conclusion": run.get("conclusion"),
        "attempt": run.get("run_attempt", 1),
        "wall_minutes": wall,
        "time_to_green_minutes": wall if run.get("conclusion") == "success" else None,
        "first_failed_step_minutes": round(minutes(run["created_at"], min(failures)), 3)
        if failures else None,
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
