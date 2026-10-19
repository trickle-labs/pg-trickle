"""Exercise incomplete inputs, diagnostics, and full-vector rejection."""

from copy import deepcopy

from check_result import check_result

bounded = {"exit_code": 0, "stop_reason": None, "supervision_error": None,
           "measurement_errors": [], "image": "sha256:" + "a" * 64,
           "conservative_used_bytes": 1000, "peak_memory_bytes": 1000}
progress = [{"batch": i, "expected_source_rows": [{"id": 4 * i + j} for j in range(4)],
             "expected_case_state": {"case_key": i, "last_observed_at": "2026-10-19T00:00:00+00:00"}}
            for i in range(1, 301)]
names = ("future-idle-mdm-receipt", "callback-error-role-reset", "worker-missing-set-membership",
         "1000-case-due-receipt-latency", "1000-case-due-resource-growth",
         "1000-case-due-burst-and-60-per-minute-churn")
observed = {"image_id": bounded["image"], "checks": [{"name": n, "result": "passed"} for n in names],
            "workload": {"declared": {"churn_case_count": 300, "new_cases_per_minute": 60,
                                      "churn_window_minutes": 5, "simultaneous_due_burst": 1000},
                         "observed_churn_cases_per_minute": 60.1, "due_latency_p95_seconds": 100,
                         "due_latency_max_seconds": 120, "churn_admissions": [{}] * 300,
                         "receipts_for_future_churn_cases": [], "churn_partial_receipts": [],
                         "churn_partial_sources": [r for p in progress for r in p["expected_source_rows"]],
                         "churn_partial_cases": [p["expected_case_state"].copy() for p in progress]}}
assert check_result(bounded, observed, progress)["s1_result"] == "PASS"
for failure in ("blocked", "diagnostic", "slow", "source", "case", "receipt", "memory"):
    b, o = deepcopy(bounded), deepcopy(observed)
    if failure == "blocked":
        o = {"image_id": bounded["image"], "checks": [], "entry_decision": "INPUT_BLOCKED"}
    elif failure == "diagnostic":
        o["diagnostic_profile"] = {}
    elif failure == "slow":
        o["workload"]["observed_churn_cases_per_minute"] = 59.9
    elif failure == "source":
        o["workload"]["churn_partial_sources"][0]["id"] = -1
    elif failure == "case":
        o["workload"]["churn_partial_cases"][0]["unexpected"] = True
    elif failure == "receipt":
        o["workload"]["churn_partial_receipts"] = [{}]
    elif failure == "memory":
        b["peak_memory_bytes"] = 4 * 1024**3
    try:
        check_result(b, o, progress)
    except AssertionError:
        pass
    else:
        raise AssertionError(f"accepted {failure} result")
print("S1 complete-result and full-vector rejection checks: PASS")
