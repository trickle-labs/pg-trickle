#!/usr/bin/env python3
"""Generate a scoped capability matrix from completed packaged test logs."""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "benchmarks/pgvector/results/2026-11-08-main-merge-arm64"
OUTPUT = EVIDENCE / "CAPABILITY_MATRIX.md"
VERSIONS = ("0.8.1", "0.8.7")

# These describe only the exercised cases; they are not a general support claim.
CASES = (
    (
        "Missing extension",
        "Helper rejects setup; pgvector is required",
        "conformance",
        ("test_pgvector_helper_missing_extension_is_actionable_and_atomic",),
    ),
    (
        "Row-local distances",
        "Dense, half and sparse values; differential projection/filter",
        "conformance",
        (
            "test_pgvector_distance_types_and_functions_differential",
            "test_pgvector_row_local_distance_differential",
        ),
    ),
    (
        "Top-K distance",
        "Whole-query Top-K recomputation, not row-local maintenance",
        "conformance",
        ("test_pgvector_distance_topk_recomputes",),
    ),
    (
        "Vector AVG",
        "Experimental; enable_vector_agg is off by default; affected-group rescan",
        "conformance",
        (
            "test_vector_aggregate_disabled_reports_actionable_error",
            "test_vector_centroid_rescan_matches_native_avg_under_sustained_churn",
        ),
    ),
    (
        "Halfvec AVG",
        "Experimental; enable_vector_agg is off by default; affected-group rescan",
        "conformance",
        (
            "test_pgvector_halfvec_avg_output_type",
            "test_pgvector_halfvec_avg_values_correct",
        ),
    ),
    (
        "Algebraic vector AVG",
        "Experimental; both aggregate and centroid-state settings are off by default; guarded fallback/rebase",
        "conformance",
        (
            "test_centroid_opt_in_applies_and_falls_back_after_corruption",
            "test_centroid_randomized_refresh_churn_matches_native_avg",
        ),
    ),
    (
        "Unknown dimensions",
        "Mixed dimensions are rejected without data loss",
        "conformance",
        (
            "test_vector_aggregate_unknown_dimensions_rejects_mixed_input_without_data_loss",
        ),
    ),
    (
        "Non-public extension schema",
        "Vector aggregate and distance expressions resolve the installed extension",
        "conformance",
        (
            "test_vector_aggregate_nonpublic_extension_schema",
            "test_pgvector_distance_non_public_extension_schema",
        ),
    ),
    (
        "ANN helper",
        "Explicit metric/options; index eligibility and half/sparse index cases",
        "conformance",
        (
            "test_embedding_stream_table_v2_metrics_and_options",
            "test_embedding_stream_table_halfvec_and_sparsevec_indexes",
        ),
    ),
    (
        "Distance subscription",
        "Typed threshold crossing events; unsupported notification modes rejected",
        "conformance",
        (
            "test_distance_subscription_typed_predicates_and_authorization",
            "test_distance_subscription_rejects_unsupported_notification_modes",
        ),
    ),
    (
        "Hybrid retrieval",
        "Example fixture with tenant isolation",
        "hybrid",
        ("test_hybrid_candidates_refresh_and_tenant_isolation",),
    ),
    (
        "Embedding generation",
        "External revision-safe generation and model rollover fixture",
        "revision",
        ("test_revision_safe_generation_and_model_rollover",),
    ),
)


def render(directory: Path) -> str:
    logs = {}
    for suite in {case[2] for case in CASES}:
        for version in VERSIONS:
            path = directory / f"{suite}-{version}.log"
            content = path.read_text(encoding="utf-8")
            results = re.findall(r"^test result: .*", content, re.MULTILINE)
            if len(results) != 1 or not re.match(
                r"test result: ok\. \d+ passed; 0 failed; 0 ignored;", results[0]
            ):
                raise ValueError(f"no successful suite result in {path}")
            logs[suite, version] = content

    rows = []
    for capability, contract, suite, tests in CASES:
        for version in VERSIONS:
            for test in tests:
                if not re.search(
                    rf"^test {re.escape(test)} \.\.\. ok$",
                    logs[suite, version],
                    re.MULTILINE,
                ):
                    raise ValueError(f"{suite}-{version}: missing successful {test}")
        links = ", ".join(f"[{version}]({suite}-{version}.log)" for version in VERSIONS)
        names = ", ".join(f"`{test}`" for test in tests)
        rows.append(f"| {capability} | {contract} | {names} ({links}) |")

    return "\n".join(
        [
            "# Packaged pgvector capability evidence",
            "",
            (
                "Generated from the recorded 2026-11-08 packaged test logs; see the "
                "[run details](README.md) for image identities and limitations. Each row "
                "requires the named tests to pass on both pgvector 0.8.1 and 0.8.7."
            ),
            (
                "This is scoped test evidence, not a promise for every query shape. "
                "Unlisted behavior is not established here; sparse aggregate support "
                "is not implied by sparse passthrough or indexing."
            ),
            "",
            "| Capability | Tested behavior and prerequisites | Passing tests |",
            "|---|---|---|",
            *rows,
            "",
        ]
    )


def main() -> int:
    try:
        result = render(EVIDENCE)
    except (OSError, ValueError) as error:
        print(f"pgvector evidence matrix: {error}", file=sys.stderr)
        return 1
    if "--check" in sys.argv[1:]:
        if not OUTPUT.exists() or OUTPUT.read_text(encoding="utf-8") != result:
            print(f"pgvector evidence matrix is stale: {OUTPUT}", file=sys.stderr)
            return 1
    else:
        OUTPUT.write_text(result, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
