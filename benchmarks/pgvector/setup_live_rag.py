#!/usr/bin/env python3
"""Install the live RAG fixture for one fixed vector dimension."""

import argparse
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SOURCES = (
    ROOT / "tests/fixtures/pgvector_revision_pipeline.sql",
    ROOT / "benchmarks/pgvector/live_rag_baselines.sql",
)


def setup_sql(dimensions: int) -> str:
    if not 1 <= dimensions <= 1536:
        raise ValueError("dimensions must be between 1 and 1536")
    parts = []
    for path in SOURCES:
        source = path.read_text(encoding="utf-8")
        if "vector(3)" not in source:
            raise ValueError(f"fixture no longer declares its expected vector dimension: {path}")
        parts.append(source.replace("vector(3)", f"vector({dimensions})"))
    return "BEGIN;\n" + "\n".join(parts) + "\nCOMMIT;\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dimensions", type=int, default=3)
    args = parser.parse_args()
    try:
        sql = setup_sql(args.dimensions)
    except ValueError as error:
        parser.error(str(error))
    subprocess.run(["psql", "-X", "-w", "-v", "ON_ERROR_STOP=1"], input=sql, text=True, check=True)


if __name__ == "__main__":
    main()
