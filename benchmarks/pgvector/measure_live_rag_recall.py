#!/usr/bin/env python3
"""Compare filtered vector candidates with an exact scan in one snapshot."""

import argparse
import json
import math
import re
import subprocess
from pathlib import Path


SQL = Path(__file__).with_name("live_rag_recall.sql")
HYBRID_SQL = Path(__file__).with_name("live_rag_hybrid_query.sql")
CORPORA = {
    "application_current": "application_ann",
    "application_authorized": "application_ann",
    "periodic_full_current": "periodic_full_ann",
    "periodic_full_authorized": "periodic_full_ann",
    "differential_current": "differential_ann",
    "differential_authorized": "differential_ann",
}


def vector_arg(value: str, dimensions: int) -> str:
    values = json.loads(value)
    if not isinstance(values, list) or len(values) != dimensions:
        raise ValueError("query vector length must match dimensions")
    try:
        valid = all(type(v) in (int, float) and math.isfinite(v) for v in values)
    except OverflowError:
        valid = False
    if not valid or not any(values):
        raise ValueError("query vector must contain finite numbers and be nonzero")
    return json.dumps(values, separators=(",", ":"), allow_nan=False)


def index_names(node: object) -> set[str]:
    if isinstance(node, dict):
        found = {node["Index Name"]} if isinstance(node.get("Index Name"), str) else set()
        return found | set().union(*(index_names(value) for value in node.values()))
    if isinstance(node, list):
        return set().union(*(index_names(value) for value in node))
    return set()


def uses_index(node: object) -> bool:
    if isinstance(node, dict):
        return node.get("Node Type") in {"Index Scan", "Index Only Scan", "Bitmap Index Scan"} or any(uses_index(value) for value in node.values())
    if isinstance(node, list):
        return any(uses_index(value) for value in node)
    return False


def parse_output(output: str, corpus: str) -> dict:
    plans = {}
    for name in ("EXACT", "PLANNER", "ELIGIBILITY", "HYBRID_EXACT", "HYBRID_PLANNER"):
        begin = f"LIVE_RAG_{name}_PLAN_BEGIN\n"
        end = f"\nLIVE_RAG_{name}_PLAN_END"
        if output.count(begin) != 1 or output.count(end) != 1:
            raise ValueError(f"missing or duplicate {name} plan")
        plans[name.lower()] = json.loads(output.split(begin, 1)[1].split(end, 1)[0])
    lines = [line.removeprefix("LIVE_RAG_METRICS=") for line in output.splitlines() if line.startswith("LIVE_RAG_METRICS=")]
    if len(lines) != 1:
        raise ValueError("missing or duplicate recall metrics")
    metrics = json.loads(lines[0])
    if uses_index(plans["exact"]) or uses_index(plans["hybrid_exact"]):
        raise ValueError("exact baseline used an index scan")
    index = CORPORA[corpus]
    return {"metrics": metrics, "plans": plans,
            "planner_used_ann_index": index in index_names(plans["planner"]),
            "forced_plan_used_ann_index": index in index_names(plans["eligibility"]),
            "hybrid_planner_used_ann_index": index in index_names(plans["hybrid_planner"])}


def measurement_sql(dimensions: int) -> str:
    template = SQL.read_text(encoding="utf-8")
    hybrid = HYBRID_SQL.read_text(encoding="utf-8").strip()
    if template.count("{{HYBRID_QUERY}}") != 4 or not hybrid.endswith(";"):
        raise ValueError("hybrid measurement template is invalid")
    return template.replace("{{HYBRID_QUERY}}", hybrid[:-1]).replace("vector(3)", f"vector({dimensions})")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", choices=sorted(CORPORA), required=True)
    parser.add_argument("--tenant", type=int, required=True)
    parser.add_argument("--dimensions", type=int, required=True)
    parser.add_argument("--query-vector", required=True, help="JSON numeric array")
    parser.add_argument("--terms", required=True)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--iterative-scan", choices=("off", "strict_order", "relaxed_order"), default="strict_order")
    parser.add_argument("--candidate-commit", required=True)
    parser.add_argument("--package-sha256", required=True)
    parser.add_argument("--expected-pgvector", choices=("0.8.1", "0.8.7"), required=True)
    args = parser.parse_args()
    if not 1 <= args.dimensions <= 1536 or not 1 <= args.k <= 1000 or args.tenant < 0:
        parser.error("dimensions, k or tenant is outside the supported range")
    if not re.fullmatch(r"[0-9a-f]{40}", args.candidate_commit) or not re.fullmatch(r"[0-9a-f]{64}", args.package_sha256):
        parser.error("candidate commit and package SHA-256 must be lowercase hexadecimal")
    try:
        vector = vector_arg(args.query_vector, args.dimensions)
        sql = measurement_sql(args.dimensions)
        variables = {"corpus": args.corpus, "tenant": str(args.tenant), "query_vector": vector,
                     "terms": args.terms, "k": str(args.k), "iterative_scan": args.iterative_scan}
        command = ["psql", "-X", "-w", "-qAt", "-v", "ON_ERROR_STOP=1"]
        for name, value in variables.items():
            command.extend(["-v", f"{name}={value}"])
        output = subprocess.run(command, input=sql, text=True, stdout=subprocess.PIPE, check=True).stdout
        report = parse_output(output, args.corpus)
        metrics = report["metrics"]
        if metrics["pg_trickle"] != "0.109.0" or metrics["pgvector"] != args.expected_pgvector or not re.match(r"^18(?:\.|$)", metrics["postgres"]):
            raise ValueError("installed extension or PostgreSQL version differs from the requested candidate")
        report.update({"corpus": args.corpus, "dimensions": args.dimensions,
                       "candidate_commit": args.candidate_commit, "package_sha256": args.package_sha256,
                       "identity_source": "operator-declared; installed payload not attested by this script",
                       "notes": "Plans run separately from candidate materialization in the same snapshot. Hybrid recall compares the fixed top-10 fusion of 20 vector and 20 lexical candidates against the same fusion with exact vector candidates. Boundary ties make ID-based recall ambiguous. Guard joins and planner choice affect timings."})
        print(json.dumps(report, sort_keys=True))
    except (ValueError, KeyError, json.JSONDecodeError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
