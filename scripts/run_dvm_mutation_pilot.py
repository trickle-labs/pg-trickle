#!/usr/bin/env python3
"""Run the two bounded source-level DVM mutants from issue #1289."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BUILDER_IMAGE = "pg_trickle_builder:pg18"
RUNTIME_IMAGE = "pg_trickle_i1289_runtime:pg18"
PACKAGE_SO = "/artefacts/pg_trickle-pg18/usr/lib/postgresql/18/lib/pg_trickle.so"
INSTALLED_SO = "/usr/lib/postgresql/18/lib/pg_trickle.so"
PACKAGE_ROOT = "/artefacts/pg_trickle-pg18"
RUN_LABEL = "com.pgtrickle.run-id"
SUITE_LABEL = "com.pgtrickle.suite"
MUTATION_SUITE = "issue-1289-mutation"
INFRA_MARKERS = (
    "Cannot connect to the Docker daemon",
    "error during connect",
    "No such file or directory (os error 2)",
    "failed to resolve source metadata",
    "TLS handshake timeout",
    "connection refused",
    "testcontainers error",
    "permission denied while trying to connect to the docker api",
    "not found locally",
    "no match for platform in manifest",
)


@dataclass(frozen=True)
class Mutant:
    name: str
    source: str
    anchor: str
    replacement: str
    test: str
    expected_sql: str
    mutant_sql: str
    assertion: str


MUTANTS = (
    Mutant(
        name="having-unknown-retraction",
        source="src/dvm/operators/filter.rs",
        anchor="let retraction = crate::aggregate_lifecycle::having_retraction_sql(&predicate_sql);",
        replacement='let retraction = format!("({predicate_sql}) IS FALSE");',
        test="test_diff_full_equivalence_having_unknown_transitions",
        expected_sql="IS NOT TRUE",
        mutant_sql="IS FALSE",
        assertion="M-HAVING-UNKNOWN-RETRACTION TRUE-to-UNKNOWN cycle",
    ),
    Mutant(
        name="count-null-delete",
        source="src/dvm/operators/aggregate.rs",
        anchor='format!("COUNT(CASE WHEN __pgt_action = \'D\'{filter_and} THEN {col} ELSE NULL END)"),',
        replacement='format!("COUNT(CASE WHEN __pgt_action = \'D\'{filter_and} THEN 1 ELSE NULL END)"),',
        test="test_diff_full_equivalence_count_composite_general",
        expected_sql="__pgt_action = 'D' THEN payload ELSE NULL END",
        mutant_sql='__pgt_action = \'D\' THEN 1 ELSE NULL END',
        assertion="M-COUNT-NULL-DELETE SQL-NULL payload deletion cycle",
    ),
)


class CampaignError(RuntimeError):
    def __init__(self, status: str, message: str):
        super().__init__(message)
        self.status = status


def parse_size(value: str) -> int:
    match = re.fullmatch(r"\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)\s*", value)
    if not match:
        raise CampaignError("disk_budget", f"unrecognized Docker size {value!r}")
    number, unit = match.groups()
    units = {
        "B": 1,
        "kB": 1_000,
        "KB": 1_000,
        "MB": 1_000_000,
        "GB": 1_000_000_000,
        "TB": 1_000_000_000_000,
        "KiB": 1_024,
        "MiB": 1_048_576,
        "GiB": 1_073_741_824,
        "TiB": 1_099_511_627_776,
    }
    if unit not in units:
        raise CampaignError("disk_budget", f"unrecognized Docker size unit {unit!r}")
    return int(Decimal(number) * units[unit])


class DiskMonitor:
    def __init__(self, root: Path, artifacts: Path, floor_bytes: int, interval_seconds: int):
        self.root = root
        self.artifacts = artifacts
        self.floor_bytes = floor_bytes
        self.interval_seconds = interval_seconds
        self.samples: list[dict] = []
        self.errors: list[str] = []
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.below_floor: list[dict] = []
        self.sample_path = artifacts / "disk-samples.jsonl"

    @staticmethod
    def host_df(path: Path) -> dict:
        result = subprocess.run(["df", "-Pk", str(path)], text=True, capture_output=True)
        if result.returncode:
            raise CampaignError("disk_budget", f"df failed for {path}: {result.stderr.strip()}")
        lines = result.stdout.strip().splitlines()
        if len(lines) < 2:
            raise CampaignError("disk_budget", f"df returned no filesystem row for {path}")
        fields = lines[-1].split()
        if len(fields) < 6:
            raise CampaignError("disk_budget", f"could not parse df output for {path}: {lines[-1]!r}")
        return {
            "path": str(path),
            "mount": fields[-1],
            "available_bytes": int(fields[3]) * 1024,
            "used_bytes": int(fields[2]) * 1024,
        }

    def snapshot(self, label: str) -> dict:
        paths = [self.root, self.artifacts]
        staging = self.artifacts / ".staging"
        if staging.exists():
            paths.append(staging)
        host = []
        seen = set()
        for path in paths:
            fs = self.host_df(path)
            key = fs["mount"]
            if key not in seen:
                host.append(fs)
                seen.add(key)

        raw = subprocess.run(
            ["docker", "system", "df", "--format", "{{json .}}"],
            text=True,
            capture_output=True,
        )
        if raw.returncode:
            raise CampaignError("disk_budget", f"docker system df failed: {raw.stderr.strip()}")
        docker_rows = [json.loads(line) for line in raw.stdout.splitlines() if line.strip()]
        docker_usage = sum(parse_size(row["Size"]) for row in docker_rows)

        info = subprocess.run(
            ["docker", "info", "--format", "{{.OperatingSystem}}|{{.DockerRootDir}}"],
            text=True,
            capture_output=True,
        )
        if info.returncode:
            raise CampaignError("disk_budget", f"docker info failed: {info.stderr.strip()}")
        operating_system, docker_root = info.stdout.strip().split("|", 1)
        docker_free = None
        if "desktop" in operating_system.lower():
            docker_budget = "not_verifiable_docker_desktop"
        elif Path(docker_root).exists():
            docker_free = self.host_df(Path(docker_root))["available_bytes"]
            docker_budget = "measured"
        else:
            docker_budget = "not_verifiable_docker_root_not_host_path"

        sample = {
            "label": label,
            "utc": datetime.now(timezone.utc).isoformat(),
            "host_filesystems": host,
            "docker_system_df": docker_rows,
            "docker_usage_bytes": docker_usage,
            "docker_operating_system": operating_system,
            "docker_root": docker_root,
            "docker_free_bytes": docker_free,
            "docker_budget_status": docker_budget,
        }
        with self.lock:
            self.samples.append(sample)
            self.sample_path.parent.mkdir(parents=True, exist_ok=True)
            with self.sample_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(sample, ensure_ascii=False) + "\n")
            low = [fs for fs in host if fs["available_bytes"] < self.floor_bytes]
            if low:
                self.below_floor.append({"label": label, "filesystems": low})
        return sample

    def check_before(self, label: str) -> dict:
        sample = self.snapshot(label)
        low = [fs for fs in sample["host_filesystems"] if fs["available_bytes"] < self.floor_bytes]
        if low:
            raise CampaignError(
                "disk_budget",
                f"free space below configured floor before {label}: {json.dumps(low)}",
            )
        with self.lock:
            below_floor = list(self.below_floor)
            telemetry_errors = list(self.errors)
        if below_floor:
            raise CampaignError("disk_budget", f"free-space floor was crossed during the run: {below_floor[-1]}")
        if telemetry_errors:
            raise CampaignError("disk_budget", f"disk telemetry failed during the run: {telemetry_errors[-1]}")
        return sample

    def start(self) -> None:
        self.thread = threading.Thread(target=self._sample_loop, name="i1289-disk-monitor", daemon=True)
        self.thread.start()

    def _sample_loop(self) -> None:
        while not self.stop_event.wait(self.interval_seconds):
            try:
                self.snapshot("periodic")
            except Exception as error:  # Record telemetry failures; boundary checks fail closed.
                with self.lock:
                    self.errors.append(str(error))

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=self.interval_seconds + 2)

    def report(self) -> dict:
        with self.lock:
            samples = list(self.samples)
            errors = list(self.errors)
            below = list(self.below_floor)
        if not samples:
            return {"samples": 0, "telemetry_errors": errors, "below_floor": below}
        mounts = sorted({fs["mount"] for sample in samples for fs in sample["host_filesystems"]})
        host_by_mount = {}
        for mount in mounts:
            matching = [
                next(fs for fs in sample["host_filesystems"] if fs["mount"] == mount)
                for sample in samples if any(fs["mount"] == mount for fs in sample["host_filesystems"])
            ]
            before = matching[0]["available_bytes"]
            peak_free = min(fs["available_bytes"] for fs in matching)
            after = matching[-1]["available_bytes"]
            host_by_mount[mount] = {
                "free_bytes": {"before": before, "peak": peak_free, "after": after},
                "observed_growth_bytes": max(0, before - peak_free),
            }
        docker_before = samples[0]["docker_usage_bytes"]
        docker_peak = max(sample["docker_usage_bytes"] for sample in samples)
        docker_after = samples[-1]["docker_usage_bytes"]
        return {
            "samples": len(samples),
            "sampling_interval_seconds": self.interval_seconds,
            "minimum_free_space_bytes": self.floor_bytes,
            "host_filesystems": host_by_mount,
            "host_observed_growth_bytes": max((item["observed_growth_bytes"] for item in host_by_mount.values()), default=0),
            "docker_usage_bytes": {"before": docker_before, "peak_observed": docker_peak, "after": docker_after},
            "docker_observed_growth_bytes": max(0, docker_peak - docker_before),
            "docker_usage_scope": "aggregate docker system df; concurrent Docker activity can affect observed growth",
            "docker_free_space_status": samples[-1]["docker_budget_status"],
            "docker_free_bytes": samples[-1]["docker_free_bytes"],
            "telemetry_errors": errors,
            "below_floor": below,
        }


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


@dataclass
class RunResources:
    run_id: str
    artifacts: Path
    worktrees: list[Path] = field(default_factory=list)
    staging_dirs: list[Path] = field(default_factory=list)
    ephemeral_images: list[str] = field(default_factory=list)


def owned_container_ids(run_id: str) -> tuple[list[str], str]:
    result = subprocess.run(
        ["docker", "ps", "-aq", "--filter", f"label={RUN_LABEL}={run_id}"],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    return result.stdout.splitlines() if result.returncode == 0 else [], result.stderr.strip()


def registered_worktree_paths() -> tuple[set[str], str]:
    result = subprocess.run(
        ["git", "worktree", "list", "--porcelain"], cwd=ROOT, text=True, capture_output=True
    )
    if result.returncode:
        return set(), result.stderr.strip()
    return {
        line.removeprefix("worktree ")
        for line in result.stdout.splitlines()
        if line.startswith("worktree ")
    }, ""


def cleanup_resources(resources: RunResources) -> dict:
    errors = []
    removed_containers = []
    anonymous_volumes = set()
    container_ids, list_error = owned_container_ids(resources.run_id)
    if list_error:
        errors.append(f"list run-owned containers: {list_error}")
    if container_ids:
        for container_id in container_ids:
            inspected = subprocess.run(
                ["docker", "inspect", "--format", "{{json .Mounts}}", container_id],
                cwd=ROOT,
                text=True,
                capture_output=True,
            )
            if inspected.returncode:
                errors.append(f"inspect run-owned container volumes {container_id}: {inspected.stderr.strip()}")
                continue
            try:
                anonymous_volumes.update(
                    mount["Name"] for mount in json.loads(inspected.stdout)
                    if mount.get("Type") == "volume" and mount.get("Name")
                )
            except (json.JSONDecodeError, TypeError, KeyError) as error:
                errors.append(f"parse volume mounts for {container_id}: {error}")
        removed = subprocess.run(
            ["docker", "rm", "-fv", *container_ids],
            cwd=ROOT,
            text=True,
            capture_output=True,
        )
        if removed.returncode:
            errors.append(f"remove run-owned containers: {removed.stderr.strip()}")
        else:
            removed_containers = removed.stdout.splitlines()
    remaining_volumes = []
    for volume in sorted(anonymous_volumes):
        check = subprocess.run(
            ["docker", "volume", "inspect", volume], cwd=ROOT, text=True, capture_output=True
        )
        if check.returncode == 0:
            remaining_volumes.append(volume)
        elif "no such volume" not in check.stderr.lower():
            errors.append(f"verify removed volume {volume}: {check.stderr.strip()}")

    registered, worktree_list_error = registered_worktree_paths()
    if worktree_list_error:
        errors.append(f"list worktrees for scoped cleanup: {worktree_list_error}")
    removed_worktrees = []
    for worktree in reversed(resources.worktrees):
        if worktree.exists() or str(worktree) in registered:
            result = subprocess.run(
                ["git", "worktree", "remove", "--force", str(worktree)],
                cwd=ROOT,
                text=True,
                capture_output=True,
            )
            if result.returncode:
                errors.append(f"remove worktree {worktree}: {result.stderr.strip()}")
            else:
                removed_worktrees.append(str(worktree))

    staging_root = (resources.artifacts / ".staging").resolve()
    removed_staging = []
    for path in reversed(resources.staging_dirs):
        resolved = path.resolve()
        try:
            resolved.relative_to(staging_root)
        except ValueError:
            errors.append(f"refuse staging cleanup outside {staging_root}: {resolved}")
            continue
        if resolved.exists():
            try:
                shutil.rmtree(resolved)
                removed_staging.append(str(resolved))
            except OSError as error:
                errors.append(f"remove staging path {resolved}: {error}")
    try:
        staging_root.rmdir()
    except OSError:
        pass

    removed_images = []
    for tag in resources.ephemeral_images:
        if docker_exists(tag, ROOT, resources.artifacts / f"{tag.replace(':', '-')}-cleanup-inspect.log"):
            result = subprocess.run(
                ["docker", "image", "rm", tag], cwd=ROOT, text=True, capture_output=True
            )
            if result.returncode:
                errors.append(f"remove ephemeral image {tag}: {result.stderr.strip()}")
            else:
                removed_images.append(tag)

    remaining_containers, remaining_error = owned_container_ids(resources.run_id)
    if remaining_error:
        errors.append(f"verify run-owned containers: {remaining_error}")
    registered, worktree_list_error = registered_worktree_paths()
    if worktree_list_error:
        errors.append(f"verify worktree cleanup: {worktree_list_error}")
    remaining_worktrees = [
        str(path) for path in resources.worktrees
        if path.exists() or str(path) in registered
    ]
    remaining_staging = [str(path) for path in resources.staging_dirs if path.exists()]
    remaining_images = [
        tag for tag in resources.ephemeral_images
        if docker_exists(tag, ROOT, resources.artifacts / f"{tag.replace(':', '-')}-leftover.log")
    ]
    return {
        "run_id": resources.run_id,
        "removed_container_ids": removed_containers,
        "remaining_container_ids": remaining_containers,
        "anonymous_volumes_removed": sorted(anonymous_volumes - set(remaining_volumes)),
        "remaining_volumes": remaining_volumes,
        "removed_worktrees": removed_worktrees,
        "remaining_worktrees": remaining_worktrees,
        "removed_staging_paths": removed_staging,
        "remaining_staging_paths": remaining_staging,
        "removed_ephemeral_images": removed_images,
        "remaining_ephemeral_images": remaining_images,
        "cleanup_errors": errors,
        "cleanup_complete": not errors and not remaining_containers and not remaining_worktrees
        and not remaining_volumes and not remaining_staging and not remaining_images,
    }


def retained_artifact_bytes(artifacts: Path) -> int:
    staging = (artifacts / ".staging").resolve()
    return sum(
        path.stat().st_size
        for path in artifacts.rglob("*")
        if path.is_file() and not path.resolve().is_relative_to(staging)
    )


def enforce_artifact_cap(artifacts: Path, cap_bytes: int, payload: dict | None = None) -> int:
    retained = retained_artifact_bytes(artifacts)
    if payload is not None:
        payload["retained_artifact_bytes_peak"] = max(
            retained, payload.get("retained_artifact_bytes_peak", 0)
        )
    if retained > cap_bytes:
        raise CampaignError(
            "disk_budget", f"retained artifacts {retained} bytes exceed cap {cap_bytes} bytes"
        )
    return retained


def replace_unique(source: str, anchor: str, replacement: str) -> str:
    count = source.count(anchor)
    if count != 1:
        raise CampaignError(
            "inconclusive", f"patch anchor occurs {count} times; expected exactly one"
        )
    return source.replace(anchor, replacement, 1)


def command(argv: list[str], cwd: Path, log_path: Path, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(argv, cwd=cwd, env=env, text=True, capture_output=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(result.stdout + result.stderr, encoding="utf-8")
    return result


def require_command(
    argv: list[str], cwd: Path, log_path: Path, stage: str, env: dict[str, str] | None = None
) -> str:
    result = command(argv, cwd, log_path, env)
    output = result.stdout + result.stderr
    if result.returncode:
        status = "infrastructure" if any(marker.lower() in output.lower() for marker in INFRA_MARKERS) else "unbuildable"
        raise CampaignError(status, f"{stage} failed; see {log_path}")
    return result.stdout.strip()


def parse_sha(text: str) -> str:
    match = re.search(r"\b([0-9a-f]{64})\b", text)
    if not match:
        raise CampaignError("stale", f"could not read a SHA-256 digest from: {text!r}")
    return match.group(1)


def docker_exists(image: str, cwd: Path, log_path: Path) -> bool:
    name, _, tag = image.partition(":")
    result = command(
        ["docker", "image", "ls", "--format", "{{.Repository}}:{{.Tag}}", f"{name}:{tag}"],
        cwd,
        log_path,
    )
    return result.returncode == 0 and f"{name}:{tag}" in result.stdout.splitlines()


def docker_platform() -> str:
    requested = os.environ.get("DOCKER_PLATFORM") or os.environ.get("DOCKER_DEFAULT_PLATFORM")
    if requested:
        return requested
    machine = platform.machine().lower()
    return "linux/amd64" if machine in ("x86_64", "amd64") else "linux/arm64"


def builder_image_for(platform_name: str) -> str:
    return f"{BUILDER_IMAGE}-{platform_name.rsplit('/', 1)[-1]}"


def ensure_builder(artifacts: Path, platform_name: str) -> tuple[str, str]:
    image = builder_image_for(platform_name)
    if docker_exists(image, ROOT, artifacts / "builder-inspect.log"):
        actual_platform = require_command(
            ["docker", "image", "inspect", image, "--format", "{{.Os}}/{{.Architecture}}"],
            ROOT,
            artifacts / "builder-platform.log",
            "inspect shared builder platform",
        )
        if actual_platform != platform_name:
            raise CampaignError(
                "stale", f"shared builder tag {image} has platform {actual_platform}, expected {platform_name}"
            )
        return image, image_id(image, ROOT, artifacts / "builder-image-id.log")
    require_command(
        [
            "docker", "build", "--platform", platform_name, "--provenance=false",
            "--tag", image, "--file", "tests/Dockerfile.builder", str(ROOT),
        ],
        ROOT,
        artifacts / "build-builder.log",
        "builder image build",
    )
    actual_platform = require_command(
        ["docker", "image", "inspect", image, "--format", "{{.Os}}/{{.Architecture}}"],
        ROOT,
        artifacts / "builder-platform.log",
        "verify shared builder platform",
    )
    if actual_platform != platform_name:
        raise CampaignError("stale", f"built builder platform {actual_platform}, expected {platform_name}")
    return image, image_id(image, ROOT, artifacts / "builder-image-id.log")


def image_library_sha(image: str, path: str, artifacts: Path, label: str, cwd: Path) -> str:
    output = require_command(
        ["docker", "run", "--rm", "--entrypoint", "sha256sum", image, path],
        cwd,
        artifacts / f"{label}-sha256.log",
        f"{label} library digest",
    )
    return parse_sha(output)


def image_id(image: str, cwd: Path, log_path: Path) -> str:
    return require_command(
        ["docker", "image", "inspect", image, "--format", "{{.Id}}"],
        cwd,
        log_path,
        f"inspect image {image}",
    )


def build_runtime_image(tag: str, builder_image: str, artifacts: Path, platform_name: str) -> str:
    dockerfile_sha = file_sha256(ROOT / "tests/Dockerfile.e2e")
    if docker_exists(tag, ROOT, artifacts / "runtime-image-preexistence.log"):
        metadata = require_command(
            [
                "docker", "image", "inspect", tag, "--format",
                '{{index .Config.Labels "com.pgtrickle.issue-1289.runtime-source-sha"}}|{{.Os}}/{{.Architecture}}',
            ],
            ROOT,
            artifacts / "runtime-image-existing-metadata.log",
            "inspect reusable runtime metadata",
        )
        source_sha, existing_platform = metadata.split("|", 1)
        if not source_sha:
            raise CampaignError("stale", f"refusing to overwrite unowned runtime image tag {tag}")
        if source_sha == dockerfile_sha and existing_platform == platform_name:
            return image_id(tag, ROOT, artifacts / "runtime-base-image-id.log")
    argv = [
        "docker", "build", "--platform", platform_name, "--provenance=false",
        "--tag", tag, "--file", "tests/Dockerfile.e2e", "--target", "runtime-base",
        "--build-arg", f"BUILDER_IMAGE={builder_image}",
        "--build-arg", f"ISSUE_1289_RUNTIME_SOURCE_SHA={dockerfile_sha}",
    ]
    require_command(
        argv + [str(ROOT)],
        ROOT,
        artifacts / "runtime-base-build.log",
        f"build reusable runtime image {tag}",
    )
    metadata = require_command(
        [
            "docker", "image", "inspect", tag, "--format",
            '{{index .Config.Labels "com.pgtrickle.issue-1289.runtime-source-sha"}}|{{.Os}}/{{.Architecture}}',
        ],
        ROOT,
        artifacts / "runtime-image-built-metadata.log",
        "verify reusable runtime metadata",
    )
    if metadata != f"{dockerfile_sha}|{platform_name}":
        raise CampaignError("stale", f"built runtime metadata does not match candidate: {metadata}")
    return image_id(tag, ROOT, artifacts / "runtime-base-image-id.log")


def build_package_image(
    context: Path,
    tag: str,
    builder_image: str,
    artifacts: Path,
    platform_name: str,
    resources: RunResources,
) -> str:
    argv = [
        "docker", "build", "--platform", platform_name, "--provenance=false",
        "--tag", tag, "--file", "tests/Dockerfile.e2e", "--target", "builder",
        "--build-arg", f"BUILDER_IMAGE={builder_image}",
        str(context),
    ]
    if docker_exists(tag, context, artifacts / f"{tag.replace(':', '-')}-preexistence.log"):
        raise CampaignError("stale", f"refusing to overwrite existing package image tag {tag}")
    resources.ephemeral_images.append(tag)
    require_command(
        argv,
        context,
        artifacts / f"{tag.replace(':', '-')}-build.log",
        f"build extension package {tag}",
    )
    return image_id(tag, context, artifacts / f"{tag.replace(':', '-')}-image-id.log")


def stage_package(image: str, destination: Path, run_id: str, artifacts: Path) -> dict:
    destination.mkdir(parents=True, exist_ok=False)
    container_id = require_command(
        [
            "docker", "create", "--label", f"{RUN_LABEL}={run_id}",
            "--label", f"{SUITE_LABEL}={MUTATION_SUITE}", image,
        ],
        ROOT,
        artifacts / f"{image.replace(':', '-')}-package-container.log",
        f"create disposable package container for {image}",
    ).splitlines()[0]
    try:
        require_command(
            ["docker", "cp", f"{container_id}:{PACKAGE_ROOT}/.", str(destination)],
            ROOT,
            artifacts / f"{image.replace(':', '-')}-package-copy.log",
            f"stage complete package from {image}",
        )
    finally:
        subprocess.run(["docker", "rm", "-fv", container_id], cwd=ROOT, check=False, capture_output=True)

    extension_dir = destination / "usr/share/postgresql/18/extension"
    library_dir = destination / "usr/lib/postgresql/18/lib"
    shared_library = library_dir / "pg_trickle.so"
    control = extension_dir / "pg_trickle.control"
    sql_files = sorted(extension_dir.glob("pg_trickle--*.sql"))
    if not shared_library.is_file() or not control.is_file() or not sql_files:
        raise CampaignError("stale", f"package at {destination} is incomplete")
    files = {}
    for path in sorted(p for p in destination.rglob("*") if p.is_file()):
        files[str(path.relative_to(destination))] = {
            "sha256": file_sha256(path),
            "size_bytes": path.stat().st_size,
        }
    return {"library_sha256": files[str(shared_library.relative_to(destination))]["sha256"], "files": files}


def remove_image(tag: str, artifacts: Path) -> bool:
    result = subprocess.run(["docker", "image", "rm", tag], cwd=ROOT, text=True, capture_output=True)
    (artifacts / f"{tag.replace(':', '-')}-remove.log").write_text(
        result.stdout + result.stderr, encoding="utf-8"
    )
    return result.returncode == 0


def run_test(
    case: Mutant,
    runtime_image: str,
    package_dir: Path,
    library_sha: str,
    sql_fragment: str,
    artifacts: Path,
    run_id: str,
    phase: str,
    force_failure: bool = False,
) -> tuple[int, str]:
    case_dir = artifacts / phase / case.name
    evidence_dir = case_dir / "evidence"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.pop("PGT_DVM_MUTATION_FORCE_FAILURE", None)
    env.update(
        {
            "PGT_DISABLE_NEXTEST": "1",
            "PGS_E2E_IMAGE": runtime_image,
            "PGS_E2E_PLATFORM": docker_platform(),
            "PGT_E2E_RUN_ID": run_id,
            "PGT_E2E_MUTATION_PACKAGE_DIR": str(package_dir),
            "PGT_DVM_MUTATION_CASE": case.name,
            "PGT_DVM_MUTATION_EXPECTED_LIBRARY_SHA256": library_sha,
            "PGT_DVM_MUTATION_EXPECTED_SQL_FRAGMENT": sql_fragment,
            "PGT_DVM_MUTATION_ARTIFACT_DIR": str(evidence_dir),
        }
    )
    if force_failure:
        env["PGT_DVM_MUTATION_FORCE_FAILURE"] = "1"
    result = subprocess.run(
        [
            "bash", str(ROOT / "scripts/run_e2e_tests.sh"),
            "--test", "e2e_diff_full_equivalence_tests", case.test,
        ],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
    )
    output = result.stdout + result.stderr
    (case_dir / "test-output.log").write_text(output, encoding="utf-8")
    return result.returncode, output


def evidence_is_valid(case: Mutant, evidence_dir: Path, expected_sha: str, expected_sql: str) -> bool:
    loaded_path = evidence_dir / "loaded-library.txt"
    route_path = evidence_dir / "target-refresh.txt"
    delta_path = evidence_dir / "target-delta.log"
    bags_path = evidence_dir / "expected-actual-bags.txt"
    if not (loaded_path.is_file() and route_path.is_file() and delta_path.is_file() and bags_path.is_file()):
        return False
    loaded = loaded_path.read_text(encoding="utf-8")
    route = route_path.read_text(encoding="utf-8")
    delta = delta_path.read_text(encoding="utf-8")
    return (
        INSTALLED_SO in loaded
        and "container_id=" in loaded
        and "runtime_image=" in loaded
        and "package_dir=" in loaded
        and expected_sha in loaded
        and "library_symbol=pg_trickle" in loaded
        and "library_search_path=$libdir" in loaded
        and "load=success" in loaded
        and "action=DIFFERENTIAL" in route
        and "status=COMPLETED" in route
        and "was_full_fallback=false" in route
        and re.search(r"delta_row_count=[1-9][0-9]*", route) is not None
        and "effective_refresh_mode=DIFFERENTIAL" in route
        and expected_sql in delta
    )


def infra_failure(output: str) -> bool:
    return any(marker.lower() in output.lower() for marker in INFRA_MARKERS)


def test_outcome(case: Mutant, returncode: int, output: str, evidence_ok: bool) -> tuple[str, str | None]:
    if evidence_ok and returncode and f"EXACT ORACLE INVARIANT VIOLATION in {case.assertion}" in output:
        counts = re.search(
            r"Counts: actual=(-?\d+), expected=(-?\d+), extra=(-?\d+), missing=(-?\d+)",
            output,
        )
        if counts and (int(counts.group(3)) > 0 or int(counts.group(4)) > 0):
            return "detected", f"EXACT ORACLE INVARIANT VIOLATION in {case.assertion}"
    if returncode == 0 and evidence_ok:
        return "survived", None
    if infra_failure(output):
        return "infrastructure", None
    if "error[E" in output or "could not compile" in output:
        return "unbuildable", None
    return "inconclusive", None


def worktree_patch(
    case: Mutant, commit: str, run_dir: Path, artifacts: Path, resources: RunResources
) -> tuple[Path, str, str, str]:
    worktree = run_dir / case.name
    resources.worktrees.append(worktree)
    require_command(
        ["git", "worktree", "add", "--detach", str(worktree), commit],
        ROOT,
        artifacts / f"{case.name}-worktree.log",
        f"{case.name} isolated worktree creation",
    )
    target = worktree / case.source
    source = target.read_text(encoding="utf-8")
    mutated = replace_unique(source, case.anchor, case.replacement)
    target.write_text(mutated, encoding="utf-8")

    names = subprocess.check_output(["git", "diff", "--name-only"], cwd=worktree, text=True).splitlines()
    if names != [case.source]:
        raise CampaignError("inconclusive", f"unexpected mutated worktree paths: {names}")
    patch = subprocess.check_output(["git", "diff", "--binary", "--", case.source], cwd=worktree)
    patch_path = artifacts / f"{case.name}.patch"
    patch_path.write_bytes(patch)
    require_command(
        ["git", "diff", "--check"],
        worktree,
        artifacts / f"{case.name}-diff-check.log",
        f"{case.name} patch validation",
    )
    return (
        worktree,
        sha256(patch),
        sha256(source.encode("utf-8")),
        sha256(mutated.encode("utf-8")),
    )


def write_summary(artifacts: Path, payload: dict) -> None:
    (artifacts / "outcomes.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    rows = [
        "| Mutant | Outcome | Evidence |",
        "|---|---|---|",
        "| M-IS-DELETE-SIGN (superseded) | inconclusive | INTERSECT is FullOnly; no admitted incremental path |",
        "| M-IS-ZERO-BOUNDARY (superseded) | inconclusive | INTERSECT is FullOnly; no admitted incremental path |",
    ]
    for result in payload.get("mutants", []):
        rows.append(
            f"| {result['name']} | {result['status']} | {result.get('artifacts', 'see outcomes.json')} |"
        )
    text = (
        "# Issue #1289 source-level DVM mutation pilot\n\n"
        f"Candidate: `{payload.get('candidate_commit', 'unknown')}`\n\n"
        "The superseded INTERSECT candidates were not built or counted as kills.\n\n"
        f"Campaign status: `{payload.get('status', 'running')}`\n\n"
        + "\n".join(rows)
        + "\n"
    )
    (artifacts / "outcomes.md").write_text(text, encoding="utf-8")


def self_test() -> None:
    for case in MUTANTS:
        source = (ROOT / case.source).read_text(encoding="utf-8")
        changed = replace_unique(source, case.anchor, case.replacement)
        assert changed != source
        assert case.replacement in changed
        assert source.count(case.anchor) == 1
    for source in ("", "anchor anchor"):
        try:
            replace_unique(source, "anchor", "replacement")
        except CampaignError:
            pass
        else:
            raise AssertionError("non-unique patch anchor must fail closed")
    exact_mismatch = (
        "EXACT ORACLE INVARIANT VIOLATION in M-HAVING-UNKNOWN-RETRACTION TRUE-to-UNKNOWN cycle\n"
        "Counts: actual=0, expected=1, extra=0, missing=1"
    )
    assert test_outcome(MUTANTS[0], 101, exact_mismatch, True)[0] == "detected"
    assert test_outcome(MUTANTS[0], 101, "assertion failed", True)[0] == "inconclusive"
    assert parse_size("1.5GiB") == 1_610_612_736
    assert parse_size("10MB") == 10_000_000
    assert builder_image_for("linux/amd64") == "pg_trickle_builder:pg18-amd64"
    assert builder_image_for("linux/arm64") == "pg_trickle_builder:pg18-arm64"
    for bad_size in ("10", "1XB"):
        try:
            parse_size(bad_size)
        except CampaignError:
            pass
        else:
            raise AssertionError("invalid Docker size must fail closed")
    print("mutation patch self-test passed")


def loaded_library_sha(evidence_dir: Path) -> str:
    loaded = (evidence_dir / "loaded-library.txt").read_text(encoding="utf-8")
    match = re.search(r"^library_sha256=([0-9a-f]{64})$", loaded, re.MULTILINE)
    if not match:
        raise CampaignError("stale", f"loaded library SHA missing from {evidence_dir}")
    return match.group(1)


def run_cleanup_probe(
    artifacts: Path,
    runtime_image: str,
    package_dir: Path,
    package_sha: str,
    run_id: str,
    commit: str,
    run_dir: Path,
    resources: RunResources,
) -> dict:
    sentinel_name = f"pgtrickle-i1289-unrelated-{run_id}"
    sentinel_label = "com.pgtrickle.i1289-cleanup-probe"
    sentinel_id = None
    result = {"unrelated_sentinel_name": sentinel_name}
    try:
        sentinel_id = require_command(
            [
                "docker", "create", "--name", sentinel_name,
                "--label", f"{RUN_LABEL}={run_id}-unrelated",
                "--label", f"{sentinel_label}={run_id}",
                runtime_image,
            ],
            ROOT,
            artifacts / "cleanup-probe-sentinel-create.log",
            "create unrelated cleanup sentinel",
        ).splitlines()[0]
        result["unrelated_sentinel_id"] = sentinel_id
        probe_image = f"pg_trickle_i1289_cleanup_probe:{run_id}"
        if docker_exists(probe_image, ROOT, artifacts / "cleanup-probe-image-preexistence.log"):
            raise CampaignError("stale", f"refusing to retag existing cleanup probe image {probe_image}")
        resources.ephemeral_images.append(probe_image)
        require_command(
            ["docker", "image", "tag", runtime_image, probe_image],
            ROOT,
            artifacts / "cleanup-probe-image-tag.log",
            "tag cleanup-probe image",
        )
        worktree = run_dir / "cleanup-probe"
        resources.worktrees.append(worktree)
        require_command(
            ["git", "worktree", "add", "--detach", str(worktree), commit],
            ROOT,
            artifacts / "cleanup-probe-worktree.log",
            "create cleanup-probe worktree",
        )
        scratch = artifacts / ".staging" / "cleanup-probe"
        resources.staging_dirs.append(scratch)
        scratch.mkdir(parents=True, exist_ok=False)
        (scratch / "owned.txt").write_text("run-owned scratch\n", encoding="utf-8")

        case = MUTANTS[0]
        code, output = run_test(
            case,
            runtime_image,
            package_dir,
            package_sha,
            case.expected_sql,
            artifacts,
            run_id,
            "forced-cleanup-failure",
            force_failure=True,
        )
        marker = "DVM_MUTATION_FORCED_CLEANUP_FAILURE"
        if code == 0 or marker not in output:
            raise CampaignError("cleanup_failure", "forced-failure probe did not reach the injected assertion")
        result["forced_failure_marker_observed"] = True
        result["test_exit_code"] = code
        result["run_owned_cleanup"] = cleanup_resources(resources)
        sentinel = subprocess.run(
            ["docker", "inspect", "--format", "{{.Id}}", sentinel_name],
            cwd=ROOT,
            text=True,
            capture_output=True,
        )
        result["unrelated_sentinel_survived_cleanup"] = sentinel.returncode == 0
        if not result["unrelated_sentinel_survived_cleanup"] or not result["run_owned_cleanup"]["cleanup_complete"]:
            raise CampaignError("cleanup_failure", f"scoped cleanup probe failed: {result}")
    finally:
        owner = subprocess.run(
            ["docker", "inspect", "--format", f'{{{{index .Config.Labels "{sentinel_label}"}}}}', sentinel_name],
            cwd=ROOT,
            text=True,
            capture_output=True,
        )
        sentinel_owned = owner.returncode == 0 and owner.stdout.strip() == run_id
        sentinel_cleanup = None
        if sentinel_owned:
            sentinel_cleanup = subprocess.run(
                ["docker", "rm", "-fv", sentinel_name], cwd=ROOT, text=True, capture_output=True
            )
        sentinel_left = False
        if sentinel_owned:
            sentinel_left = subprocess.run(
                ["docker", "inspect", "--format", "{{.Id}}", sentinel_name],
                cwd=ROOT,
                text=True,
                capture_output=True,
            ).returncode == 0
        result["unrelated_sentinel_removed_after_probe"] = sentinel_owned and not sentinel_left
        result["sentinel_cleanup_command_succeeded"] = sentinel_cleanup is not None and sentinel_cleanup.returncode == 0
        (artifacts / "cleanup-probe.json").write_text(
            json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        if sentinel_left:
            raise CampaignError("cleanup_failure", f"cleanup probe sentinel remains: {sentinel_name}")
    return result


def run_campaign(artifacts: Path, min_free_gib: Decimal, max_retained_mib: Decimal, interval: int) -> int:
    status = subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)
    if status.strip():
        raise CampaignError("inconclusive", "campaign requires a clean candidate worktree")
    candidate = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    artifacts = artifacts.resolve()
    run_id = f"i1289-{datetime.now(timezone.utc).strftime('%y%m%d%H%M%S%f')}-{os.getpid()}"
    platform_name = docker_platform()
    artifacts.mkdir(parents=True, exist_ok=False)
    run_dir = artifacts / "worktrees"
    run_dir.mkdir()
    resources = RunResources(run_id=run_id, artifacts=artifacts)
    monitor = DiskMonitor(
        ROOT,
        artifacts,
        int(min_free_gib * Decimal(1_073_741_824)),
        interval,
    )
    artifact_cap = int(max_retained_mib * Decimal(1_048_576))
    started = time.monotonic()
    original_sigterm = signal.getsignal(signal.SIGTERM)
    payload: dict = {
        "candidate_commit": candidate,
        "target": "main",
        "platform": platform_name,
        "retry_count": 0,
        "run_id": run_id,
        "configured_host_free_floor_bytes": monitor.floor_bytes,
        "retained_artifact_cap_bytes": artifact_cap,
        "superseded_intersect_mutants": {
            "M-IS-DELETE-SIGN": {"status": "inconclusive", "reason": "INTERSECT is FullOnly"},
            "M-IS-ZERO-BOUNDARY": {"status": "inconclusive", "reason": "INTERSECT is FullOnly"},
        },
        "baselines": [],
        "mutants": [],
    }
    write_summary(artifacts, payload)

    def handle_sigterm(signum: int, frame: object) -> None:
        raise KeyboardInterrupt(f"received signal {signum}")

    signal.signal(signal.SIGTERM, handle_sigterm)
    monitor.start()
    try:
        monitor.check_before("reusable runtime image build")
        builder_image, builder_id = ensure_builder(artifacts, platform_name)
        payload["builder_image"] = {"tag": builder_image, "image_id": builder_id}
        runtime_image = RUNTIME_IMAGE
        runtime_id = build_runtime_image(runtime_image, builder_image, artifacts, platform_name)
        payload["reusable_runtime_image"] = {
            "tag": runtime_image,
            "image_id": runtime_id,
            "dockerfile_target": "runtime-base",
            "package_injected_before_container_start": True,
        }
        monitor.snapshot("after-runtime-image")

        baseline_tag = f"pg_trickle_i1289_package:{run_id}-baseline"
        monitor.check_before("baseline package build")
        baseline_image_id = build_package_image(
            ROOT, baseline_tag, builder_image, artifacts, platform_name, resources
        )
        baseline_package_dir = artifacts / ".staging" / "baseline-package"
        resources.staging_dirs.append(baseline_package_dir)
        baseline_package = stage_package(baseline_tag, baseline_package_dir, run_id, artifacts)
        payload["baseline_package"] = {
            "image_id": baseline_image_id,
            "package_library_sha256": baseline_package["library_sha256"],
            "files": baseline_package["files"],
        }
        remove_image(baseline_tag, artifacts)
        enforce_artifact_cap(artifacts, artifact_cap, payload)

        for case in MUTANTS:
            monitor.check_before(f"baseline {case.name}")
            code, output = run_test(
                case, runtime_image, baseline_package_dir, baseline_package["library_sha256"],
                case.expected_sql, artifacts, run_id, "baseline",
            )
            evidence_dir = artifacts / "baseline" / case.name / "evidence"
            valid = evidence_is_valid(case, evidence_dir, baseline_package["library_sha256"], case.expected_sql)
            record = {
                "test": case.test,
                "exit_code": code,
                "route_evidence_valid": valid,
                "package_library_sha256": baseline_package["library_sha256"],
                "installed_library_sha256": loaded_library_sha(evidence_dir) if valid else None,
                "output": f"baseline/{case.name}/test-output.log",
                "bags": f"baseline/{case.name}/evidence/expected-actual-bags.txt",
            }
            payload["baselines"].append(record)
            monitor.snapshot(f"after-baseline-{case.name}")
            enforce_artifact_cap(artifacts, artifact_cap, payload)
            if code != 0 or not valid:
                raise CampaignError(
                    "infrastructure" if infra_failure(output) else "inconclusive",
                    f"baseline {case.name} failed or lacked route evidence",
                )

        payload["cleanup_failure_probe"] = run_cleanup_probe(
            artifacts, runtime_image, baseline_package_dir, baseline_package["library_sha256"],
            run_id, candidate, run_dir, resources,
        )
        monitor.snapshot("after-forced-failure-cleanup-probe")
        payload["mutant_base_package_library_sha256"] = baseline_package["library_sha256"]
        # The baseline package staging directory was removed by the scoped cleanup probe.

        for case in MUTANTS:
            record = {
                "name": case.name,
                "test": case.test,
                "expected_sql_arm": case.expected_sql,
                "mutated_sql_arm": case.mutant_sql,
            }
            try:
                enforce_artifact_cap(artifacts, artifact_cap, payload)
                monitor.check_before(f"mutant {case.name} package build")
                worktree, patch_sha, baseline_source_sha, mutated_source_sha = worktree_patch(
                    case, candidate, run_dir, artifacts, resources
                )
                record.update(
                    {
                        "patch_sha256": patch_sha,
                        "baseline_source_sha256": baseline_source_sha,
                        "mutated_source_sha256": mutated_source_sha,
                    }
                )
                build_tag = f"pg_trickle_i1289_package:{run_id}-{case.name}"
                build_dir = artifacts / case.name / "build"
                build_dir.mkdir(parents=True)
                package_image_id = build_package_image(
                    worktree, build_tag, builder_image, build_dir, platform_name, resources
                )
                package_dir = artifacts / ".staging" / f"{case.name}-package"
                resources.staging_dirs.append(package_dir)
                package = stage_package(build_tag, package_dir, run_id, artifacts)
                remove_image(build_tag, artifacts)
                record.update(
                    {
                        "package_image_id": package_image_id,
                        "package_library_sha256": package["library_sha256"],
                        "package_files": package["files"],
                        "runtime_image_id": runtime_id,
                    }
                )
                if package["library_sha256"] == baseline_package["library_sha256"]:
                    raise CampaignError("stale", "mutant package library SHA matches baseline")

                monitor.check_before(f"mutant {case.name} PostgreSQL test")
                code, output = run_test(
                    case, runtime_image, package_dir, package["library_sha256"],
                    case.mutant_sql, artifacts, run_id, "mutant",
                )
                evidence_dir = artifacts / "mutant" / case.name / "evidence"
                valid = evidence_is_valid(case, evidence_dir, package["library_sha256"], case.mutant_sql)
                outcome, assertion = test_outcome(case, code, output, valid)
                installed = loaded_library_sha(evidence_dir) if valid else None
                record.update(
                    {
                        "status": outcome,
                        "route_evidence_valid": valid,
                        "installed_library_sha256": installed,
                        "first_detecting_assertion": assertion,
                        "output": f"mutant/{case.name}/test-output.log",
                        "artifacts": f"mutant/{case.name}/evidence/",
                    }
                )
                if installed != package["library_sha256"]:
                    record.update({"status": "stale", "reason": "staged package and installed library hashes differ"})
                payload["mutants"].append(record)
                monitor.snapshot(f"after-mutant-{case.name}")
                enforce_artifact_cap(artifacts, artifact_cap, payload)
            except CampaignError as error:
                record.update({"status": error.status, "reason": str(error)})
                payload["mutants"].append(record)
                raise
            finally:
                cleanup = cleanup_resources(resources)
                record["cleanup_after_case"] = cleanup
                if not cleanup["cleanup_complete"]:
                    raise CampaignError("cleanup_failure", f"scoped cleanup left resources: {cleanup}")
                write_summary(artifacts, payload)

        payload["status"] = "complete" if all(item.get("status") == "detected" for item in payload["mutants"]) else "failed"
    except KeyboardInterrupt as error:
        payload["status"] = "interrupted"
        payload["error"] = str(error)
    except CampaignError as error:
        payload["status"] = error.status
        payload["error"] = str(error)
    except BaseException as error:
        payload["status"] = "error"
        payload["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        monitor.stop()
        payload["cleanup"] = cleanup_resources(resources)
        try:
            monitor.snapshot("after-cleanup")
        except Exception as error:
            payload["disk_snapshot_error"] = str(error)
        payload["disk"] = monitor.report()
        payload["retained_artifact_bytes"] = retained_artifact_bytes(artifacts)
        payload["retained_artifact_bytes_peak"] = max(
            payload.get("retained_artifact_bytes_peak", 0), payload["retained_artifact_bytes"]
        )
        payload["elapsed_seconds"] = round(time.monotonic() - started, 2)
        payload["cleanup_survivors"] = {
            "containers": payload["cleanup"]["remaining_container_ids"],
            "volumes": payload["cleanup"]["remaining_volumes"],
            "worktrees": payload["cleanup"]["remaining_worktrees"],
            "staging_paths": payload["cleanup"]["remaining_staging_paths"],
            "images": payload["cleanup"]["remaining_ephemeral_images"],
        }
        if not payload["cleanup"]["cleanup_complete"]:
            payload["previous_status"] = payload.get("status")
            payload["status"] = "cleanup_failure"
        elif payload["disk"].get("below_floor"):
            payload["previous_status"] = payload.get("status")
            payload["status"] = "disk_budget"
        elif payload["disk"].get("telemetry_errors") or payload.get("disk_snapshot_error"):
            payload["previous_status"] = payload.get("status")
            payload["status"] = "disk_measurement_failure"
        if payload["retained_artifact_bytes"] > artifact_cap:
            payload["status"] = "disk_budget"
        write_summary(artifacts, payload)
        signal.signal(signal.SIGTERM, original_sigterm)
    return 0 if payload.get("status") == "complete" else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true", help="assert patch anchors are unique and fail closed")
    parser.add_argument("--artifacts-dir", type=Path, help="artifact destination (defaults under ~/.p2p/work)")
    parser.add_argument("--min-free-gib", type=Decimal, default=Decimal("10"), help="minimum host filesystem free space (default: 10 GiB)")
    parser.add_argument("--max-retained-artifact-mib", type=Decimal, default=Decimal("512"), help="maximum retained campaign artifacts (default: 512 MiB)")
    parser.add_argument("--sample-interval-seconds", type=int, default=10, help="Docker and host disk sampling interval")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    artifacts = args.artifacts_dir or Path.home() / ".p2p" / "work" / "issue-1289-dvm-mutation-pilot" / run_id
    try:
        if args.min_free_gib <= 0 or args.max_retained_artifact_mib <= 0 or args.sample_interval_seconds < 1:
            raise CampaignError("disk_budget", "disk floors/caps must be positive and sample interval at least 1 second")
        result = run_campaign(artifacts, args.min_free_gib, args.max_retained_artifact_mib, args.sample_interval_seconds)
    except CampaignError as error:
        artifacts.mkdir(parents=True, exist_ok=True)
        (artifacts / "campaign-error.txt").write_text(f"{error.status}: {error}\n", encoding="utf-8")
        print(f"campaign {error.status}: {error}", file=sys.stderr)
        return 1
    print(f"campaign artifacts: {artifacts}")
    return result


if __name__ == "__main__":
    raise SystemExit(main())
