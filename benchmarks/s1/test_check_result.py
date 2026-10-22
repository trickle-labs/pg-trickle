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
assert check_result(bounded, observed, progress, baseline=True)["s1_result"] == "BASELINE_COMPLETE"
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

baseline_b, baseline_o = deepcopy(bounded), deepcopy(observed)
baseline_b["exit_code"] = 1
baseline_o["workload"]["observed_churn_cases_per_minute"] = 27.0
baseline_o["checks"][-1] = {"name": names[-1], "target_cases_per_minute": 60,
                            "observed_cases_per_minute": 27.0, "churn_cases": 300,
                            "window_minutes": 5, "result": "failed"}
baseline_o["failure"] = str(baseline_o["checks"][-1])
summary = check_result(baseline_b, baseline_o, progress, baseline=True)
assert summary["s1_result"] == "BASELINE_COMPLETE"
assert summary["throughput_gate"] == "failed_below_60" and summary["raw_harness_exit_code"] == 1
tolerated_b, tolerated_o = deepcopy(bounded), deepcopy(observed)
tolerated_b["exit_code"] = 1
tolerated_o["workload"]["observed_churn_cases_per_minute"] = 59.36
tolerated_o["checks"][-1] = {"name": names[-1], "target_cases_per_minute": 60,
                              "observed_cases_per_minute": 59.36, "churn_cases": 300,
                              "window_minutes": 5, "result": "failed"}
tolerated_o["failure"] = str(tolerated_o["checks"][-1])
# Both a narrow miss (59.36/min) and a larger miss must fail the exact
# candidate gate, even if every other vector and resource check passes.
for below_target in (59.36, 59.999, 58.99):
    failed_b, failed_o = deepcopy(tolerated_b), deepcopy(tolerated_o)
    failed_o["workload"]["observed_churn_cases_per_minute"] = below_target
    failed_o["checks"][-1]["observed_cases_per_minute"] = below_target
    failed_o["failure"] = str(failed_o["checks"][-1])
    try:
        check_result(failed_b, failed_o, progress)
    except AssertionError:
        pass
    else:
        raise AssertionError(f"accepted candidate below 60/min: {below_target}")

# Passing the hard boundary is not sufficient if the harness itself failed.
at_floor_b, at_floor_o = deepcopy(bounded), deepcopy(observed)
at_floor_o["workload"]["observed_churn_cases_per_minute"] = 60.0
assert check_result(at_floor_b, at_floor_o, progress)["s1_result"] == "PASS"
at_floor_b["exit_code"] = 1
try:
    check_result(at_floor_b, at_floor_o, progress)
except AssertionError:
    pass
else:
    raise AssertionError("accepted failed candidate harness at 60/min")
for failure in ("candidate", "other_error", "other_gate", "incomplete", "source", "case",
                "receipt", "memory", "supervision", "exit", "diagnostic"):
    b, o = deepcopy(baseline_b), deepcopy(baseline_o)
    if failure == "other_error":
        o["failure"] = "unrelated SPI error"
    elif failure == "other_gate":
        o["checks"][0]["result"] = "failed"
    elif failure == "incomplete":
        o["workload"]["churn_admissions"].pop()
    elif failure == "source":
        o["workload"]["churn_partial_sources"].pop()
    elif failure == "case":
        o["workload"]["churn_partial_cases"][0]["unexpected"] = True
    elif failure == "receipt":
        o["workload"]["churn_partial_receipts"] = [{}]
    elif failure == "memory":
        b["peak_memory_bytes"] = 4 * 1024**3
    elif failure == "supervision":
        b["supervision_error"] = "lost measurement"
    elif failure == "exit":
        b["exit_code"] = -9
    elif failure == "diagnostic":
        o["diagnostic_profile"] = {}
    try:
        check_result(b, o, progress, baseline=failure != "candidate")
    except AssertionError:
        pass
    else:
        raise AssertionError(f"accepted baseline {failure}")
print("S1 complete-result and full-vector rejection checks: PASS")
