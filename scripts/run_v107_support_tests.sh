#!/usr/bin/env bash
set -euo pipefail

bash scripts/run_light_e2e_tests.sh --test e2e_stmt_cdc_tests \
    --filter test_stmt_cdc_default_trigger_is_statement_level
bash scripts/run_light_e2e_tests.sh --test e2e_cte_tests \
    --filter test_recursive_cte_non_monotone_agg_subquery_recomputation
bash scripts/run_light_e2e_tests.sh --test e2e_window_incremental_tests \
    --filter test_row_number_rejected_candidate_converges_via_partition_recompute
bash scripts/run_light_e2e_tests.sh --test e2e_dvm_composition_tests \
    --filter test_v0873_mandatory_composition_matrix
bash scripts/run_light_e2e_tests.sh --test e2e_diff_full_equivalence_tests \
    --filter test_diff_full_equivalence_intersect
bash scripts/run_light_e2e_tests.sh --test e2e_v078_tests \
    --filter test_dvm1_case_in_list_mutable_full_fallback
