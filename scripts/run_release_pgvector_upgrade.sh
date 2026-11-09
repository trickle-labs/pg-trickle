#!/usr/bin/env bash
# Exercise the published catalog snapshot with the candidate library.
set -euo pipefail

PGS_E2E_BASE_IMAGE=pg_trickle_e2e_pgvector:0.8.7 \
    tests/build_e2e_upgrade_image.sh 0.108.3 0.109.0
export PGS_E2E_IMAGE=pg_trickle_upgrade_e2e:latest
export PGS_UPGRADE_FROM=0.108.3 PGS_UPGRADE_TO=0.109.0
bash scripts/run_e2e_tests.sh --no-capture --run-ignored all --test e2e_upgrade_tests \
    -E 'test(test_upgrade_embedding_helpers_from_01083) | test(test_upgrade_preserves_existing_vector_expression_index) | test(test_upgrade_distance_subscriptions_preserves_legacy_rows) | test(test_upgrade_ann_epoch_handles_old_catalog_and_rollback)'
