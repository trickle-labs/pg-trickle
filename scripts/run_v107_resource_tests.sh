#!/usr/bin/env bash
set -euo pipefail

bash scripts/run_light_e2e_tests.sh \
    --test e2e_coverage_error_tests \
    --filter test_wb2_wide_table_gt63_cols_differential_refresh
bash scripts/run_light_e2e_tests.sh \
    --test e2e_buffer_growth_tests \
    --filter test_buffer_growth_triggers_full_fallback \
    --ignored
bash scripts/run_light_e2e_tests.sh \
    --test e2e_failure_recovery_tests \
    --filter test_cancel_backend_during_refresh_recovers
