#!/usr/bin/env bash
# scripts/run_light_e2e_tests.sh — Run curated light-E2E tests

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILDER_IMAGE="pg_trickle_builder:pg18"
DOCKER_PLATFORM="${DOCKER_PLATFORM:-linux/$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')}"
LIGHT_E2E_PACKAGE_DIR="${PROJECT_DIR}/target/light-e2e/pg_trickle-pg18"
LIGHT_E2E_LABEL_TEST="com.pgtrickle.test=true"
LIGHT_E2E_LABEL_SUITE="com.pgtrickle.suite=light-e2e"

is_valid_light_e2e_package_dir() {
    local dir="$1"

    [[ -n "$dir" ]] || return 1
    [[ -f "$dir/usr/share/postgresql/18/extension/pg_trickle.control" ]] || return 1
    [[ -f "$dir/usr/lib/postgresql/18/lib/pg_trickle.so" ]] || return 1
}

make_light_e2e_run_id() {
    printf 'light-e2e-%s-%s' "$$" "$(date +%s)"
}

cleanup_light_e2e_containers() {
    local status=$?
    local ids

    trap - EXIT INT TERM
    set +e

    if [[ -z "${PGT_LIGHT_E2E_RUN_ID:-}" ]]; then
        exit "$status"
    fi

    ids="$(docker ps -aq \
        --filter "label=${LIGHT_E2E_LABEL_TEST}" \
        --filter "label=${LIGHT_E2E_LABEL_SUITE}" \
        --filter "label=com.pgtrickle.run-id=${PGT_LIGHT_E2E_RUN_ID}")"

    if [[ -n "$ids" ]]; then
        echo "Cleaning up light-E2E containers for run ${PGT_LIGHT_E2E_RUN_ID}"
        docker rm -fv $ids >/dev/null 2>&1 || true
    fi

    exit "$status"
}

# Curated allowlist for tests that run against the light harness.
# ⚠ ORDER MATTERS — round-robin sharding (index % shard_count) assigns tests
# to CI shards.  This list is sorted by greedy bin-packing so that the three
# shards have roughly equal test counts (~230 each).  When adding new test
# files, append them at the END and re-balance with:
#   grep -c '#\[tokio::test\]' tests/e2e_*.rs | sort -t: -k2 -rn
LIGHT_E2E_TESTS=(
    e2e_cte_tests
    e2e_topk_tests
    e2e_expression_tests
    e2e_watermark_gating_tests
    e2e_create_tests
    e2e_error_tests
    e2e_window_tests
    e2e_refresh_tests
    e2e_ivm_tests
    e2e_aggregate_coverage_tests
    e2e_property_tests
    e2e_phase4_ergonomics_tests
    e2e_view_tests
    e2e_lateral_tests
    e2e_diamond_tests
    e2e_alter_tests
    e2e_create_or_replace_tests
    e2e_cdc_tests
    e2e_coverage_parser_tests
    e2e_pipeline_dag_tests
    e2e_lateral_subquery_tests
    e2e_all_subquery_tests
    e2e_smoke_tests
    e2e_coverage_error_tests
    e2e_getting_started_tests
    e2e_drop_tests
    e2e_lifecycle_tests
    e2e_dag_topology_tests
    e2e_stmt_cdc_tests
    e2e_dag_error_tests
    e2e_multi_window_tests
    e2e_having_transition_tests
    e2e_dag_operations_tests
    e2e_monitoring_tests
    e2e_set_operation_tests
    e2e_keyless_duplicate_tests
    e2e_full_join_tests
    e2e_multi_cycle_dag_tests
    e2e_concurrent_tests
    e2e_mixed_mode_dag_tests
    e2e_guard_trigger_tests
    e2e_rows_from_tests
    e2e_sublink_or_tests
    e2e_snapshot_consistency_tests
    e2e_dag_immediate_tests
    e2e_dag_concurrent_tests
    e2e_scalar_subquery_tests
    e2e_partition_tests
    e2e_mixed_pg_objects_tests
    e2e_tier_scheduling_tests
    e2e_unlogged_buffer_tests
    # ── TEST-5 v0.18.0 additions (light-eligible, 0 scheduler dependencies) ──
    e2e_null_group_by_tests
    e2e_cdc_edge_case_tests
    e2e_join_tests
    e2e_alter_query_tests
    e2e_append_only_tests
    e2e_diff_full_equivalence_tests
    e2e_multi_column_in_tests
    e2e_ddl_event_tests
    e2e_diagnostics_tests
    e2e_schema_evolution_tests
    e2e_differential_gaps_tests
    e2e_multi_cycle_tests
    e2e_guc_variation_tests
    e2e_immediate_concurrency_tests
    e2e_merge_template_tests
    e2e_rls_tests
    e2e_ownership_tests
    e2e_ec01_property_tests
)

usage() {
    cat <<'EOF'
Usage: scripts/run_light_e2e_tests.sh [options]

Options:
  --package                 Run cargo pgrx package before tests
  --package-only            Run cargo pgrx package and exit
    --test <name>             Run only the named light-E2E test target
  --list                    Print selected test targets and exit
  --shard-index <n>         1-based shard index
  --shard-count <n>         Total shard count
EOF
}

resolve_pg_config() {
    if [[ -n "${PG_CONFIG:-}" ]]; then
        echo "$PG_CONFIG"
        return
    fi

    if command -v pg_config >/dev/null 2>&1; then
        command -v pg_config
        return
    fi

    if command -v cargo >/dev/null 2>&1; then
        local pgrx_pg_config
        pgrx_pg_config="$(cargo pgrx info pg-config pg18 2>/dev/null || true)"
        if [[ -n "$pgrx_pg_config" ]]; then
            echo "$pgrx_pg_config"
            return
        fi
    fi

    if [[ -f "${HOME}/.pgrx/config.toml" ]]; then
        local config_pg_path
        config_pg_path="$(awk -F'"' '/^pg18/ {print $2; exit}' "${HOME}/.pgrx/config.toml")"
        if [[ -n "$config_pg_path" ]]; then
            echo "$config_pg_path"
            return
        fi
    fi

    echo "ERROR: Could not determine pg_config path for cargo pgrx package" >&2
    exit 1
}

ensure_builder_image() {
    local builder_platform
    builder_platform="$(docker image inspect "${BUILDER_IMAGE}" --format='{{.Os}}/{{.Architecture}}' 2>/dev/null || echo "")"

    if [[ "$builder_platform" == "$DOCKER_PLATFORM" ]]; then
        echo "  Builder image present (${builder_platform}): ${BUILDER_IMAGE}"
        return
    fi

    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    if [[ -z "$builder_platform" ]]; then
        echo "  Builder image not found: ${BUILDER_IMAGE}"
    else
        echo "  Builder image platform mismatch: got ${builder_platform}, need ${DOCKER_PLATFORM}"
    fi
    echo "  Building it now for Light E2E packaging …"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    docker build \
        --platform "${DOCKER_PLATFORM}" \
        -t "${BUILDER_IMAGE}" \
        -f "${PROJECT_DIR}/tests/Dockerfile.builder" \
        "${PROJECT_DIR}"
}

package_extension_in_builder() {
    local package_parent host_uid host_gid
    package_parent="$(dirname "${LIGHT_E2E_PACKAGE_DIR}")"
    host_uid="$(id -u)"
    host_gid="$(id -g)"

    ensure_builder_image

    mkdir -p "$package_parent"
    rm -rf "$LIGHT_E2E_PACKAGE_DIR"

    echo "Packaging Linux extension artifacts in builder image: ${BUILDER_IMAGE}"
    docker run --rm \
        --platform "${DOCKER_PLATFORM}" \
        -e HOST_UID="${host_uid}" \
        -e HOST_GID="${host_gid}" \
        -v "${PROJECT_DIR}:/build" \
        -w /build \
        "${BUILDER_IMAGE}" \
        bash -lc 'set -euo pipefail
            export CARGO_TARGET_DIR=/tmp/pgt-light-target
            cargo pgrx package --pg-config /usr/bin/pg_config >/tmp/pgt-light-e2e-package.log
            rm -rf /build/target/light-e2e/pg_trickle-pg18
            cp -a /tmp/pgt-light-target/release/pg_trickle-pg18 /build/target/light-e2e/pg_trickle-pg18
            chown -R "${HOST_UID}:${HOST_GID}" /build/target/light-e2e/pg_trickle-pg18'

    export PGT_EXTENSION_DIR="$LIGHT_E2E_PACKAGE_DIR"
}

package_extension() {
    if [[ "$(uname -s)" != "Linux" ]]; then
        package_extension_in_builder
        return
    fi

    local pg_config_path
    pg_config_path="$(resolve_pg_config)"

    echo "Packaging extension with pg_config: $pg_config_path"
    cargo pgrx package --pg-config "$pg_config_path"
    export PGT_EXTENSION_DIR="${PROJECT_DIR}/target/release/pg_trickle-pg18"
}

set_existing_extension_dir() {
    if is_valid_light_e2e_package_dir "${PGT_EXTENSION_DIR:-}"; then
        return
    fi

    if is_valid_light_e2e_package_dir "${LIGHT_E2E_PACKAGE_DIR}"; then
        export PGT_EXTENSION_DIR="${LIGHT_E2E_PACKAGE_DIR}"
        return
    fi

    if is_valid_light_e2e_package_dir "${PROJECT_DIR}/target/release/pg_trickle-pg18"; then
        export PGT_EXTENSION_DIR="${PROJECT_DIR}/target/release/pg_trickle-pg18"
    fi
}

validate_positive_integer() {
    local value="$1"
    local name="$2"

    if [[ ! "$value" =~ ^[0-9]+$ ]] || (( value < 1 )); then
        echo "ERROR: $name must be a positive integer" >&2
        exit 1
    fi
}

package_before_run=false
package_only=false
list_only=false
shard_index=1
shard_count=1
requested_tests=()

while (($# > 0)); do
    case "$1" in
        --package)
            package_before_run=true
            ;;
        --package-only)
            package_before_run=true
            package_only=true
            ;;
        --test)
            shift
            requested_tests+=("${1:-}")
            ;;
        --list)
            list_only=true
            ;;
        --shard-index)
            shift
            shard_index="${1:-}"
            ;;
        --shard-count)
            shift
            shard_count="${1:-}"
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "ERROR: Unknown option: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
    shift
done

validate_positive_integer "$shard_index" "shard index"
validate_positive_integer "$shard_count" "shard count"

if (( shard_index > shard_count )); then
    echo "ERROR: shard index must be less than or equal to shard count" >&2
    exit 1
fi

selected_tests=()
selected_count=0

if (( ${#requested_tests[@]} > 0 )); then
    for test_name in "${requested_tests[@]}"; do
        if [[ -z "$test_name" ]]; then
            echo "ERROR: --test requires a test target name" >&2
            exit 1
        fi
        selected_tests+=("$test_name")
        selected_count=$((selected_count + 1))
    done
else
    # Distribute tests round-robin across shards so adjacent entries do not end
    # up in the same shard when new tests are appended to the allowlist.
    for index in "${!LIGHT_E2E_TESTS[@]}"; do
        if (( index % shard_count == shard_index - 1 )); then
            selected_tests+=("${LIGHT_E2E_TESTS[$index]}")
            selected_count=$((selected_count + 1))
        fi
    done
fi

if (( selected_count == 0 )); then
    echo "ERROR: No tests selected for shard ${shard_index}/${shard_count}" >&2
    exit 1
fi

cd "$PROJECT_DIR"

if [[ "$package_before_run" == true ]]; then
    package_extension
fi

set_existing_extension_dir

if [[ "$package_before_run" != true ]] && ! is_valid_light_e2e_package_dir "${PGT_EXTENSION_DIR:-}"; then
    echo "No valid Linux light-E2E package found; packaging extension artifacts now"
    package_extension
fi

if [[ "$package_only" == true ]]; then
    exit 0
fi

echo "Running light-E2E shard ${shard_index}/${shard_count} with ${selected_count} test targets"
printf '  %s\n' "${selected_tests[@]}"

if [[ "$list_only" == true ]]; then
    exit 0
fi

export PGT_LIGHT_E2E_RUN_ID="${PGT_LIGHT_E2E_RUN_ID:-$(make_light_e2e_run_id)}"
trap cleanup_light_e2e_containers EXIT INT TERM

echo "Light-E2E run id: ${PGT_LIGHT_E2E_RUN_ID}"

# Start ONE shared PostgreSQL container for all 48+ test binaries.
# Each binary connects to this container via PGT_LIGHT_E2E_PORT instead of
# spawning its own testcontainers instance.  This avoids 48 simultaneous
# Docker containers overwhelming the host and causing StartupTimeout failures.
start_shared_light_e2e_container() {
    local run_id="$PGT_LIGHT_E2E_RUN_ID"
    local ext_dir="${PGT_EXTENSION_DIR}"

    echo "Starting shared light-E2E PostgreSQL container..."
    local cid
    cid=$(docker run -d \
        --platform "${DOCKER_PLATFORM}" \
        -e POSTGRES_PASSWORD=postgres \
        -e POSTGRES_DB=postgres \
        -p 5432 \
        -v "${ext_dir}:/tmp/pg_ext:ro" \
        --shm-size=512m \
        --label com.pgtrickle.test=true \
        --label com.pgtrickle.suite=light-e2e \
        --label "com.pgtrickle.run-id=${run_id}" \
        postgres:18.3)

    # Wait up to 120 s for PostgreSQL to accept connections
    local i=0
    until docker exec "$cid" pg_isready -U postgres >/dev/null 2>&1; do
        i=$((i + 1))
        if [[ $i -gt 120 ]]; then
            echo "ERROR: shared light-E2E container failed to become ready" >&2
            docker rm -f "$cid" >/dev/null 2>&1 || true
            return 1
        fi
        sleep 1
    done

    # Install pg_trickle into the container
    docker exec "$cid" sh -c \
        "cp /tmp/pg_ext/usr/share/postgresql/18/extension/pg_trickle* \
            /usr/share/postgresql/18/extension/ && \
         cp /tmp/pg_ext/usr/lib/postgresql/18/lib/pg_trickle* \
            /usr/lib/postgresql/18/lib/"

    local port
    port=$(docker inspect \
        --format='{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}' \
        "$cid")

    export PGT_LIGHT_E2E_PORT="$port"
    export PGT_LIGHT_E2E_CONTAINER_ID="$cid"
    echo "Shared container ${cid:0:12} ready on port ${port}"
}

start_shared_light_e2e_container

cargo_args=(--features light-e2e)
for test_name in "${selected_tests[@]}"; do
    cargo_args+=(--test "$test_name")
done


if command -v cargo-nextest >/dev/null 2>&1; then
    cargo nextest run "${cargo_args[@]}"
else
    cargo test "${cargo_args[@]}" -- --test-threads=1
fi
