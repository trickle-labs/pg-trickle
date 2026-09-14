#!/usr/bin/env python3
"""Verify a PGXN zip archive contains required files."""

from __future__ import annotations

import json
import sys
import zipfile
from pathlib import PurePosixPath


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: verify_pgxn_archive.py <archive.zip>", file=sys.stderr)
        return 2

    archive = sys.argv[1]

    try:
        with zipfile.ZipFile(archive) as zf:
            names = zf.namelist()
            meta_path = next(
                (name for name in names if PurePosixPath(name).name == "META.json"),
                None,
            )
            if not meta_path:
                print("Error: META.json not found in the archive.")
                return 1
            meta = json.loads(zf.read(meta_path))
            docfile = meta["provides"]["pg_trickle"]["docfile"]
            doc_path = str(PurePosixPath(meta_path).parent / docfile)
    except FileNotFoundError:
        print(f"Error: archive not found: {archive}", file=sys.stderr)
        return 1
    except zipfile.BadZipFile:
        print(f"Error: invalid zip archive: {archive}", file=sys.stderr)
        return 1
    except (KeyError, TypeError, ValueError) as error:
        print(f"Error: invalid PGXN metadata: {error}", file=sys.stderr)
        return 1

    if doc_path not in names:
        print(f"Error: META.json references missing documentation file: {docfile}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
