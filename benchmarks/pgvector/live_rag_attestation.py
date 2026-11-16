"""Bind a live RAG measurement to a candidate archive and Docker database."""

import hashlib
import json
import re
import subprocess
import tarfile
from pathlib import PurePosixPath


IDENTITY_SQL = "SELECT pg_catalog.json_build_object('system_identifier', (pg_catalog.pg_control_system()).system_identifier::text, 'postmaster_start_epoch', extract(epoch FROM pg_catalog.pg_postmaster_start_time())::text)::text"


def archive_payload(archive, expected_sha256, expected_commit):
    with open(archive, "rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    if digest != expected_sha256:
        raise ValueError("candidate archive SHA-256 does not match")
    files = {}
    manifest = None
    seen_manifest = False
    roots = set()
    with tarfile.open(archive, "r:gz") as package:
        for member in package:
            parts = PurePosixPath(member.name).parts
            if len(parts) < 2:
                continue
            roots.add(parts[0])
            relative = parts[1:]
            if relative == ("release-candidate.json",):
                if seen_manifest or not member.isfile():
                    raise ValueError("invalid or duplicate candidate manifest")
                seen_manifest = True
                manifest = json.load(package.extractfile(member))
            elif len(relative) == 2 and relative[0] in ("extension", "lib"):
                if not member.isfile() or relative[1] in (".", ".."):
                    raise ValueError("invalid candidate payload entry")
                if relative[0] == "lib" and relative[1] != "pg_trickle.so":
                    raise ValueError("unexpected candidate library")
                if relative[0] == "extension" and not re.fullmatch(r"pg_trickle(?:--[0-9.]+(?:--[0-9.]+)?\.sql|\.control)", relative[1]):
                    raise ValueError("unexpected candidate extension file")
                directory = "/usr/share/postgresql/18/extension" if relative[0] == "extension" else "/usr/lib/postgresql/18/lib"
                path = f"{directory}/{relative[1]}"
                if path in files:
                    raise ValueError("duplicate candidate payload entry")
                files[path] = hashlib.sha256(package.extractfile(member).read()).hexdigest()
    if len(roots) != 1 or "/usr/lib/postgresql/18/lib/pg_trickle.so" not in files or "/usr/share/postgresql/18/extension/pg_trickle.control" not in files or "/usr/share/postgresql/18/extension/pg_trickle--0.109.0.sql" not in files:
        raise ValueError("candidate archive has an incomplete payload")
    if not isinstance(manifest, dict) or manifest.get("candidate_commit") != expected_commit or manifest.get("build_kind") != "exact-release" or manifest.get("feature_scope") != "default-release":
        raise ValueError("candidate manifest does not identify the requested exact release")
    return files


def docker_output(container, *command):
    result = subprocess.run(["docker", "exec", container, *command], capture_output=True, text=True, check=False)
    if result.returncode:
        raise ValueError(f"container attestation failed: {result.stderr.strip()}")
    return result.stdout.strip()


def attest(container, archive, expected_sha256, expected_commit, database_identity):
    expected = archive_payload(archive, expected_sha256, expected_commit)
    actual = {}
    for line in docker_output(container, "sha256sum", "--", *sorted(expected)).splitlines():
        digest, path = line.split("  ", 1)
        if path in actual:
            raise ValueError("duplicate installed payload entry")
        actual[path] = digest
    if actual != expected:
        raise ValueError("installed extension payload differs from the candidate archive")
    container_identity = json.loads(docker_output(container, "psql", "-X", "-U", "postgres", "-d", "postgres", "-Atqc", IDENTITY_SQL))
    if container_identity != database_identity:
        raise ValueError("benchmark connection and attested container are different PostgreSQL instances")
    payload_digest = hashlib.sha256(json.dumps(expected, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {"archive_sha256": expected_sha256, "candidate_commit": expected_commit,
            "payload_sha256": payload_digest, "payload_file_count": len(expected),
            "database_identity": container_identity}
