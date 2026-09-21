#!/usr/bin/env bash
# scripts/run_e2e_tests.sh — Run full E2E tests with deterministic container cleanup

set -euo pipefail

FULL_E2E_LABEL_TEST="com.pgtrickle.test=true"
FULL_E2E_LABEL_SUITE="com.pgtrickle.suite=full-e2e"

usage() {
    cat <<'EOF'
Usage: scripts/run_e2e_tests.sh <cargo test-selection args...>

Examples:
  scripts/run_e2e_tests.sh --test 'e2e_*'
  scripts/run_e2e_tests.sh --test e2e_tpch_tests --run-ignored all --no-capture
EOF
}

make_full_e2e_run_id() {
    printf 'full-e2e-%s-%s' "$$" "$(date +%s)"
}

cleanup_full_e2e_containers() {
    local status=$?
    local ids

    trap - EXIT INT TERM
    set +e

    if [[ -z "${PGT_E2E_RUN_ID:-}" ]]; then
        exit "$status"
    fi

    ids="$(docker ps -aq \
        --filter "label=${FULL_E2E_LABEL_TEST}" \
        --filter "label=${FULL_E2E_LABEL_SUITE}" \
        --filter "label=com.pgtrickle.run-id=${PGT_E2E_RUN_ID}")"

    if [[ -n "$ids" ]]; then
        echo "Cleaning up full E2E containers for run ${PGT_E2E_RUN_ID}"
        docker rm -fv $ids >/dev/null 2>&1 || true
    fi

    exit "$status"
}

if (($# == 0)); then
    usage >&2
    exit 1
fi

export PGT_E2E_RUN_ID="${PGT_E2E_RUN_ID:-$(make_full_e2e_run_id)}"
trap cleanup_full_e2e_containers EXIT INT TERM

echo "Full E2E run id: ${PGT_E2E_RUN_ID}"


if [[ "${PGT_RELEASE_MACHINE_FORMAT:-0}" == "1" ]]; then
    NEXTEST_EXPERIMENTAL_LIBTEST_JSON=1 cargo nextest run "$@" \
        --message-format libtest-json-plus \
        --retries "${PGT_RELEASE_NEXTEST_RETRIES:-2}"
elif [[ "${PGT_DISABLE_NEXTEST:-0}" == "1" ]]; then
    cargo_test_args=(--nocapture)
    if [[ -n "${PGT_RELEASE_ATTESTATION_PATH:-}" ]]; then
        cargo_test_args+=(--test-threads=1)
    fi
    cargo test --features pg18 "$@" -- "${cargo_test_args[@]}"
else
    cargo nextest run "$@"
fi
