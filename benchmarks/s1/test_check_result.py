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
# The owner approved a hard >=59/min floor on 2026-10-22, keeping the
# originally offered workload and 60/min target untouched. The original
# below-target raw failure/exit code must remain visible and *must* be
# precisely the sole gate failure.
for approved_rate in (59.0, 59.36, 59.999):
    approved_b, approved_o = deepcopy(tolerated_b), deepcopy(tolerated_o)
    approved_o["workload"]["observed_churn_cases_per_minute"] = approved_rate
    approved_o["checks"][-1]["observed_cases_per_minute"] = approved_rate
    approved_o["failure"] = str(approved_o["checks"][-1])
    summary = check_result(approved_b, approved_o, progress)
    assert summary["s1_result"] == "PASS_OWNER_APPROVED_FLOOR", summary
    assert summary["throughput_gate"] == "owner_approved_59_to_60", summary
    assert summary["throughput_target_cases_per_minute"] == 60
    assert summary["owner_approved_acceptance_floor_cases_per_minute"] == 59
    assert summary["raw_harness_exit_code"] == 1

for below_floor in (58.99, 58.0):
    failed_b, failed_o = deepcopy(tolerated_b), deepcopy(tolerated_o)
    failed_o["workload"]["observed_churn_cases_per_minute"] = below_floor
    failed_o["checks"][-1]["observed_cases_per_minute"] = below_floor
    failed_o["failure"] = str(failed_o["checks"][-1])
    try:
        check_result(failed_b, failed_o, progress)
    except AssertionError:
        pass
    else:
        raise AssertionError(f"accepted candidate below owner-approved 59/min floor: {below_floor}")

# No non-throughput failure, malformed/extra receipt or incongruent exit can
# be hidden by being inside the new narrow band.
for invalid in ("extra_gate", "different_failure", "missing_failure", "zero_exit",
                "incomplete_cases", "changed_source", "future_receipt", "memory"):
    failed_b, failed_o = deepcopy(tolerated_b), deepcopy(tolerated_o)
    if invalid == "extra_gate":
        failed_o["checks"][0]["result"] = "failed"
    elif invalid == "different_failure":
        failed_o["failure"] = "SQLSTATE XX000: unrelated failure"
    elif invalid == "missing_failure":
        failed_o.pop("failure")
    elif invalid == "zero_exit":
        failed_b["exit_code"] = 0
    elif invalid == "incomplete_cases":
        failed_o["workload"]["churn_partial_cases"].pop()
    elif invalid == "changed_source":
        failed_o["workload"]["churn_partial_sources"][0]["id"] = -1
    elif invalid == "future_receipt":
        failed_o["workload"]["churn_partial_receipts"] = [{}]
    elif invalid == "memory":
        failed_b["peak_memory_bytes"] = 4 * 1024**3
    try:
        check_result(failed_b, failed_o, progress)
    except AssertionError:
        pass
    else:
        raise AssertionError(f"owner-approved floor hid invalid evidence: {invalid}")

# Full target still passes in the ordinary exit-zero path, not the waiver.
at_target_b, at_target_o = deepcopy(bounded), deepcopy(observed)
at_target_o["workload"]["observed_churn_cases_per_minute"] = 60.0
summary = check_result(at_target_b, at_target_o, progress)
assert summary["s1_result"] == "PASS" and summary["throughput_gate"] == "passed"
at_target_b["exit_code"] = 1
try:
    check_result(at_target_b, at_target_o, progress)
except AssertionError:
    pass
else:
    raise AssertionError("accepted an unexplained exit-one candidate at 60/min")

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
