#!/usr/bin/env bash
# Repeat the named failure schedules and retain every attempt's witness log.

set -euo pipefail

failure_schedule_repetition() {
    local attempts="${FAILURE_RECOVERY_ATTEMPTS:-10}"
    local artifact_dir="${FAILURE_RECOVERY_ARTIFACT_DIR:-${RUNNER_TEMP:-target}/failure-recovery-repetition}"
    local filter='test(test_statement_timeout_during_refresh_recovers) | test(test_lock_timeout_during_refresh) | test(test_cancel_backend_during_refresh_recovers) | test(test_dvm_failpoint_preserves_last_committed_result_and_recovers) | test(test_refresh_after_apply_failure_rolls_back) | test(test_refresh_finalization_failure_rolls_back) | test(test_refresh_caller_rollback_preserves_committed_state) | test(test_refresh_savepoint_rollback_preserves_outer_transaction) | test(test_refresh_two_callers_publish_one_consistent_result) | test(test_refresh_concurrent_writer_preserves_next_batch) | test(test_refresh_auxiliary_state_failure_rolls_back) | test(test_refresh_partial_finalization_control_is_detected) | test(test_refresh_early_progress_control_is_detected)'
    local summary="${artifact_dir}/summary.tsv"
    local status=0

    mkdir -p "$artifact_dir"
    printf 'attempt\tstatus\tlog\n' >"$summary"

    for attempt in $(seq 1 "$attempts"); do
        local log_file="${artifact_dir}/attempt-${attempt}.log"
        set +e
        ./scripts/run_e2e_tests.sh \
            --test e2e_failure_recovery_tests \
            --test e2e_dvm_failpoint_tests \
            --test e2e_refresh_atomicity_tests \
            --retries 0 \
            --no-capture \
            -E "$filter" >"$log_file" 2>&1
        local attempt_status=$?
        set -e

        printf '%s\t%s\t%s\n' "$attempt" "$attempt_status" "$log_file" | tee -a "$summary"
        if ((attempt_status != 0)); then
            status=1
        fi
    done

    return "$status"
}

failure_schedule_repetition "$@"
