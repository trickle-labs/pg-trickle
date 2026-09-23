#!/usr/bin/env python3
"""Attest the payload and PostgreSQL process used by one release suite."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path


PAYLOAD_ROOTS = (
    Path("usr/lib/postgresql/18/lib"),
    Path("usr/share/postgresql/18/extension"),
)


def payload_files(candidate_root: Path) -> list[dict[str, int | str]]:
    files: list[dict[str, int | str]] = []
    for relative_root in PAYLOAD_ROOTS:
        root = candidate_root / relative_root
        if not root.is_dir():
            raise ValueError(f"candidate payload is missing {relative_root}")
        for path in sorted(root.rglob("*")):
            if path.is_symlink() or not path.is_file():
                continue
            data = path.read_bytes()
            files.append(
                {
                    "path": path.relative_to(candidate_root).as_posix(),
                    "bytes": len(data),
                    "sha256": hashlib.sha256(data).hexdigest(),
                }
            )
    if not files:
        raise ValueError("candidate payload has no installed files")
    return files


def payload_digest(files: list[dict[str, int | str]]) -> str:
    encoded = json.dumps(files, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def docker_exec(container: str, *args: str) -> str:
    completed = subprocess.run(
        ["docker", "exec", container, *args],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or f"docker exec failed with {completed.returncode}")
    return completed.stdout.strip()


def server_observation(container: str) -> dict[str, object]:
    row = docker_exec(
        container,
        "psql",
        "-U",
        "postgres",
        "-d",
        "postgres",
        "-Atqc",
        "SELECT current_setting('server_version'), "
        "extract(epoch FROM pg_postmaster_start_time()), "
        "current_setting('fsync'), current_setting('synchronous_commit'), "
        "current_setting('full_page_writes'), current_setting('shared_preload_libraries')",
    )
    values = row.split("|")
    if len(values) != 6:
        raise ValueError(f"unexpected PostgreSQL identity response: {row!r}")
    version, start_epoch, fsync, synchronous_commit, full_page_writes, preload = values
    start = float(start_epoch)
    minimum_start = float(os.environ.get("PGT_RELEASE_MIN_POSTMASTER_START_EPOCH", "0"))
    container_id = subprocess.run(
        ["docker", "inspect", "--format={{.Id}}", container],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout.strip()
    return {
        "container_id": container_id or container,
        "server_version": version,
        "postmaster_start_epoch": start,
        "postmaster_start_time": start_epoch,
        "minimum_postmaster_start_epoch": minimum_start,
        "fresh_postmaster": start >= minimum_start,
        "settings": {
            "fsync": fsync,
            "synchronous_commit": synchronous_commit,
            "full_page_writes": full_page_writes,
            "shared_preload_libraries": preload,
        },
    }


def installed_files(container: str, files: list[dict[str, int | str]]) -> list[dict[str, int | str]]:
    observed: list[dict[str, int | str]] = []
    for expected in files:
        relative = str(expected["path"])
        installed = f"/{relative}"
        digest = docker_exec(container, "sha256sum", "--", installed).split()[0]
        size = int(docker_exec(container, "stat", "-c", "%s", "--", installed))
        observed.append({"path": relative, "bytes": size, "sha256": digest})
    return observed


def append_observation(path: Path, observation: dict[str, object]) -> None:
    previous = []
    if path.is_file():
        previous = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(previous, list):
            raise ValueError("release installation attestation must contain a list")
    previous.append(observation)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(previous, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--container", required=True)
    parser.add_argument("--candidate-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    files = payload_files(args.candidate_root.resolve())
    digest = payload_digest(files)
    server = server_observation(args.container)
    observed = installed_files(args.container, files)
    if observed != files:
        print("ERROR: installed PostgreSQL payload differs from the candidate payload", flush=True)
    append_observation(
        args.output.resolve(),
        {
            "candidate_commit": os.environ.get("PGT_RELEASE_CANDIDATE_COMMIT"),
            "artifact_id": os.environ.get("PGT_RELEASE_ARTIFACT_ID"),
            "artifact_digest": os.environ.get("PGT_RELEASE_ARTIFACT_SHA256"),
            "platform": os.environ.get("PGT_RELEASE_PLATFORM"),
            "build_kind": os.environ.get("PGT_RELEASE_BUILD_KIND", "exact-release"),
            "feature_scope": os.environ.get("PGT_RELEASE_FEATURE_SCOPE", "default-release"),
            "payload_digest": digest,
            "installed_payload_digest": payload_digest(observed),
            "candidate_files": files,
            "installed_files": observed,
            "server": server,
        },
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as error:
        raise SystemExit(f"release installation attestation failed: {error}")
