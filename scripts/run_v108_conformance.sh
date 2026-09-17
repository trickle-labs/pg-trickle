#!/usr/bin/env bash
# Run the v0.108 Graph V1.2 and Delta V1.1 packaged conformance tests.

set -euo pipefail

export PGT_OUTPUT_DELTA_QUALIFICATION=on

if (($# > 0)); then
    exec bash ./scripts/run_e2e_tests.sh "$@"
fi

exec bash ./scripts/run_e2e_tests.sh \
    --test e2e_v108_conformance_tests \
    --test e2e_pg_mdm_compiler_v9_tests
