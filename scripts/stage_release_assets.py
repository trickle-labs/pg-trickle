#!/usr/bin/env python3
"""Verify qualification evidence and stage the files attached to a release."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tarfile
from pathlib import Path


def verify_reference(
    reference: object, *, label: str, workspace: Path, evidence_dir: Path
) -> Path:
    if not isinstance(reference, dict) or not isinstance(reference.get("path"), str):
        raise ValueError(f"{label} has no valid path")
    path = (workspace / reference["path"]).resolve()
    if not path.is_relative_to(evidence_dir):
        raise ValueError(f"{label} is outside the qualification evidence directory")
    if not path.is_file():
        raise ValueError(f"{label} is missing: {reference['path']}")
    data = path.read_bytes()
    if (
        not data
        or reference.get("bytes") != len(data)
        or reference.get("sha256") != hashlib.sha256(data).hexdigest()
    ):
        raise ValueError(f"{label} has a mismatched size or digest: {reference['path']}")
    return path


def stage_release_assets(
    evidence_dir: Path, artifact_dir: Path, output_dir: Path, *, workspace: Path
) -> None:
    workspace = workspace.resolve()
    evidence_dir = evidence_dir.resolve()
    artifact_dir = artifact_dir.resolve()
    output_dir = output_dir.resolve()
    if output_dir.is_relative_to(evidence_dir) or evidence_dir.is_relative_to(output_dir):
        raise ValueError("release output and qualification evidence must be separate directories")

    manifest_path = evidence_dir / "RELEASE-EVIDENCE.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("status") != "passed":
        raise ValueError("qualification evidence is not passed")

    logs = manifest.get("retained_logs")
    if not isinstance(logs, list) or not logs:
        raise ValueError("qualification evidence has no retained logs")
    for log in logs:
        verify_reference(log, label="retained log", workspace=workspace, evidence_dir=evidence_dir)

    measurements = manifest.get("retained_measurements", [])
    if not isinstance(measurements, list):
        raise ValueError("qualification evidence has invalid retained measurements")
    for measurement in measurements:
        verify_reference(
            measurement, label="retained measurement", workspace=workspace, evidence_dir=evidence_dir
        )
        attachments = measurement.get("attachments", [])
        if not isinstance(attachments, list):
            raise ValueError("qualification evidence has invalid measurement attachments")
        for attachment in attachments:
            verify_reference(
                attachment,
                label="retained measurement attachment",
                workspace=workspace,
                evidence_dir=evidence_dir,
            )

    packages = sorted(
        path
        for path in artifact_dir.rglob("*")
        if path.is_file() and (path.name.endswith(".tar.gz") or path.suffix == ".zip")
    )
    if not packages or any(path.stat().st_size == 0 for path in packages):
        raise ValueError("release package artifacts are missing or empty")
    if output_dir.exists() and any(output_dir.iterdir()):
        raise ValueError("release staging directory must be empty")

    output_dir.mkdir(parents=True, exist_ok=True)
    names: set[str] = set()
    for package in packages:
        if package.name in names:
            raise ValueError(f"duplicate release package name: {package.name}")
        names.add(package.name)
        shutil.copy2(package, output_dir / package.name)
    with tarfile.open(output_dir / "qualification-evidence.tar.gz", "w:gz") as archive:
        archive.add(evidence_dir, arcname="qualification-logs")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--artifact-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        stage_release_assets(
            args.evidence_dir,
            args.artifact_dir,
            args.output_dir,
            workspace=Path.cwd(),
        )
    except (OSError, ValueError, json.JSONDecodeError, tarfile.TarError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
