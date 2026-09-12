#!/usr/bin/env bash
# scripts/run_dvm_integration_tests.sh — Run DVM execution-backed integration
# tests with pg_stub preloaded (macOS and Linux compatible).
#
# pgrx links test binaries against PostgreSQL server symbols
# (CurrentMemoryContext, SPI_connect, etc.) that are only present inside a
# live postgres process.  The DVM integration tests do NOT call these symbols
# themselves — they use pure-Rust DiffContext logic and connect to a
# Testcontainers Postgres instance via sqlx.  But on macOS (flat namespace),
# dyld forces every symbol to be resolved at load time, causing an immediate
# crash before any test code runs.
#
# This script applies exactly the same fix used by run_unit_tests.sh:
#   1. Compile pg_stub.c into a tiny stub shared library (NULL/no-op for every
#      PG symbol the binary references).
#   2. Compile each DVM integration test binary with --no-run.
#   3. Run the binary with DYLD_INSERT_LIBRARIES (macOS) or LD_PRELOAD (Linux)
#      pointing at the stub, so dyld is satisfied at load time.
#
# Requires: Docker (for testcontainers Postgres), Rust toolchain, cc.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
STUB_SRC="$SCRIPT_DIR/pg_stub.c"
FEATURES="${1:-pg18}"

DEFAULT_TARGET_DIR="$PROJECT_DIR/target"
if [[ ! -d "$DEFAULT_TARGET_DIR" ]] || [[ ! -w "$DEFAULT_TARGET_DIR" ]]; then
    FALLBACK_TARGET_DIR="$PROJECT_DIR/.cargo-target"
    mkdir -p "$FALLBACK_TARGET_DIR" 2>/dev/null || true
    if [[ -d "$FALLBACK_TARGET_DIR" ]] && [[ -w "$FALLBACK_TARGET_DIR" ]]; then
        export CARGO_TARGET_DIR="$FALLBACK_TARGET_DIR"
    else
        HOME_FALLBACK_TARGET_DIR="${HOME:-/tmp}/.cache/pg_trickle-target"
        mkdir -p "$HOME_FALLBACK_TARGET_DIR" 2>/dev/null || true
        if [[ -d "$HOME_FALLBACK_TARGET_DIR" ]] && [[ -w "$HOME_FALLBACK_TARGET_DIR" ]]; then
            export CARGO_TARGET_DIR="$HOME_FALLBACK_TARGET_DIR"
        else
            export CARGO_TARGET_DIR="${TMPDIR:-/tmp}/pg_trickle-target"
        fi
        mkdir -p "$CARGO_TARGET_DIR"
    fi
else
    export CARGO_TARGET_DIR="$DEFAULT_TARGET_DIR"
fi

OS="$(uname)"
case "$OS" in
    Darwin)
        STUB_LIB="$CARGO_TARGET_DIR/libpg_stub.dylib"
        STUB_CC_FLAGS="-shared -install_name @rpath/libpg_stub.dylib"
        PRELOAD_VAR="DYLD_INSERT_LIBRARIES"
        ;;
    *)
        STUB_LIB="$CARGO_TARGET_DIR/libpg_stub.so"
        STUB_CC_FLAGS="-shared -fPIC"
        PRELOAD_VAR="LD_PRELOAD"
        ;;
esac

# DVM integration test binaries (one per file in tests/dvm_*.rs).
DVM_TESTS=(
    dvm_aggregate_execution_tests
    dvm_full_join_tests
    dvm_join_tests
    dvm_natural_join_tests
    dvm_nested_full_join_tests
    dvm_nested_left_join_tests
    dvm_nested_natural_join_tests
    dvm_outer_join_tests
    dvm_semijoin_antijoin_tests
    dvm_window_scalar_subquery_tests
)

# ── Helper: ensure_stub ───────────────────────────────────────────────────
ensure_stub() {
    if [[ ! -f "$STUB_LIB" ]] || [[ "$STUB_SRC" -nt "$STUB_LIB" ]]; then
        echo "Building $(basename "$STUB_LIB") ..."
        mkdir -p "$(dirname "$STUB_LIB")"
        # shellcheck disable=SC2086
        cc $STUB_CC_FLAGS -o "$STUB_LIB" "$STUB_SRC" 2>&1
    fi
}

# ── Helper: run one integration test suite ────────────────────────────────
run_test_suite() {
    local test_name="$1"
    echo ""
    echo "═══════════════════════════════════════════════════"
    echo " Running: $test_name"
    echo "═══════════════════════════════════════════════════"

    # Compile without running; capture the executable path.
if command -v cargo-nextest >/dev/null 2>&1; then
        echo "Running with cargo-nextest (with $(basename "$STUB_LIB"))"
        export "$PRELOAD_VAR"="$STUB_LIB"
        cargo nextest run --test "$test_name" --features "$FEATURES" "${@:2}"
        return $?
    fi

    echo "Running $test_name with cargo (with $(basename "$STUB_LIB"))"
    export "$PRELOAD_VAR"="$STUB_LIB"
    cargo test --test "$test_name" --features "$FEATURES" -- --test-threads=1 "${@:2}"
}

# ── Main ──────────────────────────────────────────────────────────────────
cd "$PROJECT_DIR"

ensure_stub

OVERALL_EXIT=0

# If a specific test name is provided as the second argument, run only that.
if [[ -n "${2:-}" ]]; then
    run_test_suite "$2" "${@:3}" || OVERALL_EXIT=$?
else
    for test_name in "${DVM_TESTS[@]}"; do
        run_test_suite "$test_name" || OVERALL_EXIT=$?
    done
fi

exit $OVERALL_EXIT
