#!/usr/bin/env python3
"""Generate and validate the current capability and strategy manifest."""

from __future__ import annotations

import hashlib
import json
import re
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VERSION = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))["package"]["version"]
SOURCE = ROOT / f"tests/release/v{VERSION.rsplit('.', 1)[0]}-admission-examples.json"
OUTPUT = ROOT / "docs/capability-manifest.json"
OUTCOMES = {"accepted", "rejected", "experimental-disabled"}
STRATEGIES = {"DIFFERENTIAL", "FULL", "IMMEDIATE", "WAL", "UNAVAILABLE"}
STATUSES = {"stable", "experimental", "unsupported"}


def read_source() -> dict:
    return json.loads(SOURCE.read_text(encoding="utf-8"))


def source_text() -> str:
    paths = list((ROOT / "src").rglob("*.rs")) + list((ROOT / "tests").rglob("*.rs"))
    return "\n".join(path.read_text(encoding="utf-8") for path in paths)


def validate(source: dict) -> None:
    errors: list[str] = []
    if source.get("manifest_version") != 1:
        errors.append("manifest_version must be 1")
    if source.get("release_version") != VERSION:
        errors.append(f"release_version must be {VERSION}")

    capabilities = source.get("capabilities")
    examples = source.get("examples")
    reasons = source.get("stable_reason_identifiers")
    if not isinstance(capabilities, list) or not capabilities:
        errors.append("capabilities must be a non-empty list")
        capabilities = []
    if not isinstance(examples, list) or not examples:
        errors.append("examples must be a non-empty list")
        examples = []
    if not isinstance(reasons, list) or not reasons:
        errors.append("stable_reason_identifiers must be a non-empty list")
        reasons = []

    capability_ids = [item.get("id") for item in capabilities if isinstance(item, dict)]
    example_ids = [item.get("id") for item in examples if isinstance(item, dict)]
    if len(capability_ids) != len(set(capability_ids)):
        errors.append("capability identifiers must be unique")
    if len(example_ids) != len(set(example_ids)):
        errors.append("example identifiers must be unique")
    if len(reasons) != len(set(reasons)):
        errors.append("reason identifiers must be unique")

    for item in capabilities:
        if not isinstance(item, dict):
            errors.append("each capability must be an object")
            continue
        if item.get("status") not in STATUSES:
            errors.append(f"unknown capability status: {item.get('status')!r}")
        if not isinstance(item.get("enabled"), bool):
            errors.append(f"capability {item.get('id')!r} must declare enabled")
        if item.get("strategy") not in {"trigger", "wal", "unavailable"}:
            errors.append(f"capability {item.get('id')!r} has an unknown strategy")
        reason = item.get("reason_code")
        if reason is not None and reason not in reasons:
            errors.append(f"capability {item.get('id')!r} uses an undeclared reason")
        if item.get("enabled") and item.get("reason_code") is not None:
            errors.append(f"enabled capability {item.get('id')!r} cannot have a reason")

    tests = source_text()
    for item in examples:
        if not isinstance(item, dict):
            errors.append("each example must be an object")
            continue
        if not isinstance(item.get("command"), str) or not item["command"].strip():
            errors.append(f"example {item.get('id')!r} is missing a command")
        if item.get("outcome") not in OUTCOMES:
            errors.append(f"example {item.get('id')!r} has an unknown outcome")
        if item.get("strategy") not in STRATEGIES:
            errors.append(f"example {item.get('id')!r} has an unknown strategy")
        test = item.get("test")
        if not isinstance(test, str) or not re.search(rf"\bfn\s+{re.escape(test)}\b", tests):
            errors.append(f"example {item.get('id')!r} references missing test {test!r}")
        reason = item.get("reason_code")
        if reason is not None and reason not in reasons:
            errors.append(f"example {item.get('id')!r} uses an undeclared reason")
        if reason is not None and reason not in tests:
            errors.append(f"example {item.get('id')!r} reason is absent from source: {reason}")
        if item.get("outcome") != "accepted" and reason is None:
            errors.append(f"non-accepted example {item.get('id')!r} must have a reason")

    for reason in reasons:
        if not isinstance(reason, str) or not reason.strip():
            errors.append("reason identifiers must be non-empty strings")
        elif reason not in tests:
            errors.append(f"stable reason identifier is absent from source: {reason}")

    docs = source.get("documentation", {})
    for relative in docs.get("reconciled_sources", []):
        if not (ROOT / relative).is_file():
            errors.append(f"missing reconciled documentation source: {relative}")

    if errors:
        raise ValueError("\n".join(f"- {error}" for error in errors))


def render(source: dict) -> bytes:
    manifest = {
        "manifest_version": source["manifest_version"],
        "release_version": source["release_version"],
        "capabilities": source["capabilities"],
        "examples": source["examples"],
        "stable_reason_identifiers": source["stable_reason_identifiers"],
        "documentation": source["documentation"],
    }
    canonical = json.dumps(manifest, indent=2, ensure_ascii=False) + "\n"
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    manifest["manifest_digest"] = f"sha256:{digest}"
    return (json.dumps(manifest, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def main() -> int:
    check = "--check" in sys.argv[1:]
    try:
        source = read_source()
        validate(source)
        rendered = render(source)
        if check:
            if not OUTPUT.is_file() or OUTPUT.read_bytes() != rendered:
                print("capability manifest is stale; run scripts/generate_capability_manifest.py")
                return 1
        else:
            OUTPUT.write_bytes(rendered)
        print(f"capability manifest {'check passed' if check else 'generated'}: {OUTPUT}")
        return 0
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"capability manifest failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
