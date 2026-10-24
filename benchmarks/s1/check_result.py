"""Require a complete S1 result; a zero-exit input-blocked harness is not a pass."""

import argparse
import json
import math
from datetime import datetime
from pathlib import Path


def check_result(bounded, observed, progress, baseline=False):
    rate_gate = "1000-case-due-burst-and-60-per-minute-churn"
    failed = [row for row in observed["checks"] if row["result"] != "passed"]
    rate = observed.get("workload", {}).get("observed_churn_cases_per_minute", 0)
    slow_baseline = baseline and math.isfinite(rate) and 0 < rate < 60
    if slow_baseline:
        expected_failure = {"name": rate_gate, "target_cases_per_minute": 60,
                            "observed_cases_per_minute": rate, "churn_cases": 300,
                            "window_minutes": 5, "result": "failed"}
        assert failed == [expected_failure], "baseline has other failed gates"
        assert observed.get("failure") == str(expected_failure), "baseline failed for another reason"
    # Owner approval revised on 2026-10-24 sets a 45/min acceptance floor while
    # retaining the 60/min workload/target. Below-target observed rates and
    # raw harness failures are never rewritten: accept the *sole* rate
    # failure only when >=45 and every other gate/vector is exact.
    owner_approved_floor = not baseline and math.isfinite(rate) and 45 <= rate < 60
    if owner_approved_floor:
        expected_failure = {"name": rate_gate, "target_cases_per_minute": 60,
                            "observed_cases_per_minute": rate, "churn_cases": 300,
                            "window_minutes": 5, "result": "failed"}
        assert failed == [expected_failure], "approved candidate has another failed gate"
        assert observed.get("failure") == str(expected_failure), "candidate failed for another reason"
    accepted_rate_failure = slow_baseline or owner_approved_floor
    assert bounded["exit_code"] == int(accepted_rate_failure) and bounded["stop_reason"] is None, "bounded run failed"
    assert not bounded["supervision_error"] and not bounded["measurement_errors"], "supervision incomplete"
    assert 0 < bounded["conservative_used_bytes"] < 10 * 1024**3, "storage limit failed"
    assert 0 < bounded["peak_memory_bytes"] < 4 * 1024**3, "memory limit failed"
    assert observed["image_id"] == bounded["image"], "image identity mismatch"
    assert ("failure" not in observed or accepted_rate_failure) and "diagnostic_profile" not in observed, "not a qualification result"
    checks = {row["name"]: row["result"] for row in observed["checks"]}
    required = {
        "future-idle-mdm-receipt", "callback-error-role-reset", "worker-missing-set-membership",
        "1000-case-due-receipt-latency", "1000-case-due-resource-growth",
        "1000-case-due-burst-and-60-per-minute-churn",
    }
    assert required <= checks.keys() and (not failed or accepted_rate_failure), "missing or failed gates"
    workload = observed["workload"]
    declared = workload["declared"]
    assert (declared["churn_case_count"], declared["new_cases_per_minute"],
            declared["churn_window_minutes"], declared["simultaneous_due_burst"]) == (300, 60, 5, 1000), "workload changed"
    assert math.isfinite(rate) and (rate >= 60 or accepted_rate_failure), "throughput failed"
    p95, maximum = workload["due_latency_p95_seconds"], workload["due_latency_max_seconds"]
    assert 0 <= p95 <= 150 and p95 <= maximum <= 180, "due-burst latency failed"
    assert len(workload["churn_admissions"]) == len(progress) == 300, "admissions incomplete"
    assert [row["batch"] for row in progress] == list(range(1, 301)), "admission sequence changed"
    assert not workload["receipts_for_future_churn_cases"] and not workload["churn_partial_receipts"], "future receipt"
    expected_sources = sorted(
        [row for admission in progress for row in admission["expected_source_rows"]],
        key=lambda row: row["id"],
    )
    assert len(expected_sources) == 1200 and len({row["id"] for row in expected_sources}) == 1200, "source population incomplete"
    assert workload["churn_partial_sources"] == expected_sources, "source vector mismatch"
    actual_cases = workload["churn_partial_cases"]
    assert len(actual_cases) == 300 and len({row["case_key"] for row in actual_cases}) == 300, "case population incomplete"
    expected_cases = sorted([row["expected_case_state"].copy() for row in progress], key=lambda row: row["case_key"])
    for expected, actual in zip(expected_cases, actual_cases):
        # A periodic observation can advance this metadata without changing case state.
        assert datetime.fromisoformat(actual["last_observed_at"]) >= datetime.fromisoformat(expected["last_observed_at"]), "observation time regressed"
        expected["last_observed_at"] = actual["last_observed_at"]
    assert actual_cases == expected_cases, "case vector mismatch"
    return {"image": bounded["image"], "admissions": 300, "source_rows": 1200,
            "case_vectors": 300, "future_receipts": 0, "cases_per_minute": rate,
            "due_p95_seconds": p95, "due_max_seconds": maximum,
            "conservative_used_bytes": bounded["conservative_used_bytes"],
            "peak_memory_bytes": bounded["peak_memory_bytes"],
            "raw_harness_exit_code": bounded["exit_code"],
            "throughput_target_cases_per_minute": 60,
            "owner_approved_acceptance_floor_cases_per_minute": 45,
            "throughput_gate": ("failed_below_60" if slow_baseline else
                                "owner_approved_45_to_60" if owner_approved_floor else "passed"),
            "s1_result": ("BASELINE_COMPLETE" if baseline else
                          "PASS_OWNER_APPROVED_FLOOR" if owner_approved_floor else "PASS")}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--baseline", action="store_true", help="allow only the retained below-60 control rate failure")
    args = parser.parse_args()
    root = args.root
    summary = check_result(
        json.loads((root / "bounded-run.json").read_text()),
        json.loads((root / "results/observations.json").read_text()),
        [json.loads(line) for line in (root / "results/churn-progress.jsonl").read_text().splitlines()],
        baseline=args.baseline,
    )
    (root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary))
