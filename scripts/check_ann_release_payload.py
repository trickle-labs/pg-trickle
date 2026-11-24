#!/usr/bin/env python3
"""Check that a release archive contains the exact ANN operator files."""

import sys
import tarfile
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FILES = (
    "scripts/ann_maintenance_supervisor.py",
    "scripts/ann_maintenance_worker.sql",
    "scripts/ann_threshold_request.sql",
    "docs/ANN_MAINTENANCE.md",
    "docs/SQL_REFERENCE.md",
)


def check_archive(archive: Path, source: Path = ROOT) -> None:
    name = archive.name
    if name.endswith(".tar.gz"):
        root = name.removesuffix(".tar.gz")
        with tarfile.open(archive, "r:gz") as payload:
            for path in FILES:
                matches = [member for member in payload.getmembers() if member.name == f"{root}/{path}"]
                if len(matches) != 1 or not matches[0].isfile():
                    raise ValueError(f"missing or duplicate operator file: {path}")
                stream = payload.extractfile(matches[0])
                if stream is None or stream.read() != (source / path).read_bytes():
                    raise ValueError(f"operator file differs from checkout: {path}")
    elif name.endswith(".zip"):
        root = name.removesuffix(".zip")
        with zipfile.ZipFile(archive) as payload:
            names = payload.namelist()
            for path in FILES:
                member = f"{root}/{path}"
                if names.count(member) != 1:
                    raise ValueError(f"missing or duplicate operator file: {path}")
                if payload.read(member) != (source / path).read_bytes():
                    raise ValueError(f"operator file differs from checkout: {path}")
    else:
        raise ValueError("expected a .tar.gz or .zip release archive")


if __name__ == "__main__":
    try:
        if len(sys.argv) != 2:
            raise ValueError("usage: check_ann_release_payload.py ARCHIVE")
        check_archive(Path(sys.argv[1]))
    except (OSError, ValueError, tarfile.TarError, zipfile.BadZipFile) as error:
        raise SystemExit(f"ANN release payload check failed: {error}")
    print("ANN release payload matches checkout")
