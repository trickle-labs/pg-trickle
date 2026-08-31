#!/usr/bin/env python3
"""Small, offline release gate for the row-identity contract since v0.87.17."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def main() -> int:
    errors: list[str] = []
    cargo = read(ROOT / "Cargo.toml")
    version_match = re.search(r'^version\s*=\s*"([^"]+)"', cargo, re.MULTILINE)
    version = version_match.group(1) if version_match else ""
    parsed_version = tuple(int(part) for part in version.split(".")) if version else ()
    if parsed_version < (0, 87, 17):
        errors.append(f"Cargo.toml version is {version!r}, expected 0.87.17 or newer")

    archive = ROOT / "sql" / "archive" / "pg_trickle--0.87.17.sql"
    migration = ROOT / "sql" / "pg_trickle--0.87.16--0.87.17.sql"
    for path in (archive, migration):
        if not path.exists():
            errors.append(f"missing release SQL artifact: {path.relative_to(ROOT)}")

    source = read(ROOT / "src" / "lib.rs")
    migration_text = read(migration) if migration.exists() else ""
    archive_text = read(archive) if archive.exists() else ""
    implementation = source + migration_text + archive_text
    for required in (
        "row_identity_v2_recreation_preflight",
        "row_identity_v2_record_inventory",
        "row_identity_v2_register_consumer",
        "row_identity_v2_acknowledge_consumer",
        "pgt_row_identity_v2_consumers",
        "__pgt_row_id BYTEA NOT NULL",
        "Writes made during the recreation window are not replayed",
    ):
        if required not in implementation:
            errors.append(f"missing implementation contract: {required}")

    row_id_source = read(ROOT / "src" / "dvm" / "row_id_v2.rs")
    api_source = read(ROOT / "src" / "api" / "mod.rs")
    for required in (
        "row_probe_v1",
        "PROBE_VERSION_V1",
        "SUPPORTED_POSTGRES_MAJORS",
        "encode_interval_value",
    ):
        if required not in row_id_source:
            errors.append(f"missing V2 encoder contract: {required}")
    if "refresh_mode.is_immediate() && !identity_bounded" not in api_source:
        errors.append("missing planning rejection for unbounded unique IMMEDIATE identities")

    vectors = json.loads(read(ROOT / "tests" / "fixtures" / "row_id_v2_vectors.json"))
    wire = vectors.get("wire", {})
    if wire.get("identity_version") != 2 or wire.get("probe_version") != 1:
        errors.append("golden vectors do not declare identity version 2 and probe version 1")
    if not any(vector.get("name") == "window_interval_30_days" for vector in vectors.get("vectors", [])):
        errors.append("golden vectors are missing the complete interval 128-bit case")

    tests = read(ROOT / "tests" / "e2e_row_id_v2_tests.rs")
    for required in ("test_row_id_v2_recreation_preflight", "row_probe_v1", "encode_row_id_v2"):
        if required not in tests:
            errors.append(f"missing V2 E2E coverage: {required}")

    bench = read(ROOT / "benches" / "refresh_bench.rs")
    for required in ("bench_row_id_v2", "row_probe_v1", "interval_128_bit"):
        if required not in bench:
            errors.append(f"missing V2 benchmark coverage: {required}")

    for path in (ROOT / "docs" / "UPGRADING.md", ROOT / "docs" / "ROW_IDENTITY_V2.md"):
        text = read(path)
        for required in ("BYTEA", "not replayed", "resnapshot"):
            if required.lower() not in text.lower():
                errors.append(f"{path.relative_to(ROOT)} omits {required} guidance")
    if "row_identity_v2_recreation_preflight" not in read(ROOT / "docs" / "SQL_REFERENCE.md"):
        errors.append("SQL reference omits the V2 recreation preflight")

    if errors:
        print("row_id_v2_release_gate: FAILED")
        print("\n".join(f"- {error}" for error in errors))
        return 1
    print("row_id_v2_release_gate: passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
