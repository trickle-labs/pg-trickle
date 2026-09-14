#!/usr/bin/env bash
# scripts/run_benchmarks.sh — Run Criterion benchmarks with pg_stub preloaded
#
# pgrx extensions reference PostgreSQL server symbols (e.g.
# CurrentMemoryContext, ErrorContext) that are only available inside the
# postgres process.  Criterion benchmarks exercise pure Rust logic and never
# call those symbols, but the benchmark binary still links them.
#
# Workaround (all platforms):
#   1. Compile the pg_stub shared library that provides NULL/no-op definitions.
#   2. Compile the bench binaries with `--no-run`.
#   3. Run each binary with LD_PRELOAD (Linux) or DYLD_INSERT_LIBRARIES (macOS).
#
# Usage:
#   ./scripts/run_benchmarks.sh                          # all benchmarks
#   ./scripts/run_benchmarks.sh diff_operators            # specific bench
#   ./scripts/run_benchmarks.sh -- --output-format bencher # pass criterion args
#
# Environment variables:
#   BENCH_QUICK=1    — Use reduced sample size (50), measurement time (5s), and
#                      warm-up time (1s) for CI regression gates. Still sufficient
#                      for detecting >10% regressions while cutting run time ~70%.
#   BENCH_FEATURES   — Cargo features (default: pg18)
#   BENCH_PROJECT_DIR — Project checkout to benchmark (default: this script's repo)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="${BENCH_PROJECT_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
TARGET_DIR="${CARGO_TARGET_DIR:-$PROJECT_DIR/target}"
STUB_SRC="$PROJECT_DIR/scripts/pg_stub.c"
FEATURES="${BENCH_FEATURES:-pg18}"

OS="$(uname)"
case "$OS" in
    Darwin)
        STUB_LIB="$TARGET_DIR/libpg_stub.dylib"
        STUB_CC_FLAGS="-shared -install_name @rpath/libpg_stub.dylib"
        PRELOAD_VAR="DYLD_INSERT_LIBRARIES"
        ;;
    *)
        STUB_LIB="$TARGET_DIR/libpg_stub.so"
        STUB_CC_FLAGS="-shared -fPIC"
        PRELOAD_VAR="LD_PRELOAD"
        ;;
esac

# All bench targets in the project
ALL_BENCHES=("refresh_bench" "diff_operators")

# ── Parse arguments ───────────────────────────────────────────────────────
BENCH_FILTER=()
CRITERION_ARGS=()
FOUND_DASHDASH=false

for arg in "$@"; do
    if [[ "$arg" == "--" ]]; then
        FOUND_DASHDASH=true
        continue
    fi
    if $FOUND_DASHDASH; then
        CRITERION_ARGS+=("$arg")
    else
        BENCH_FILTER+=("$arg")
    fi
done

# If no filter provided, run all benches
if [[ ${#BENCH_FILTER[@]} -eq 0 ]]; then
    BENCH_FILTER=("${ALL_BENCHES[@]}")
fi

# ── Quick mode for CI regression gates ────────────────────────────────────
# BENCH_QUICK=1 reduces sample_size and measurement_time so the full suite
# fits within CI timeout while still detecting >10% regressions reliably.
if [[ "${BENCH_QUICK:-0}" == "1" ]]; then
    echo "BENCH_QUICK=1: using reduced sample-size=50, measurement-time=5s, warm-up-time=1s"
    CRITERION_ARGS+=("--sample-size" "50" "--measurement-time" "5" "--warm-up-time" "1")
fi

# ── Helper: ensure_stub ───────────────────────────────────────────────────
ensure_stub() {
    if [[ ! -f "$STUB_LIB" ]] || [[ "$STUB_SRC" -nt "$STUB_LIB" ]]; then
        echo "Building $(basename "$STUB_LIB") ..."
        mkdir -p "$(dirname "$STUB_LIB")"
        # shellcheck disable=SC2086
        cc $STUB_CC_FLAGS -o "$STUB_LIB" "$STUB_SRC" 2>&1
    fi
}

# ── Helper: find bench binary ────────────────────────────────────────────
find_bench_binary() {
    local bench_name="$1"
    local profile_dir="$TARGET_DIR/release"

    # Look for the exact benchmark binary in release/deps
    if [[ "$OS" == "Darwin" ]]; then
        find "$profile_dir/deps" \
            -maxdepth 1 -name "${bench_name}-*" -type f -perm +111 \
            ! -name '*.d' ! -name '*.dylib' \
            2>/dev/null \
        | xargs ls -t 2>/dev/null \
        | head -1
    else
        find "$profile_dir/deps" \
            -maxdepth 1 -name "${bench_name}-*" -type f -executable \
            ! -name '*.d' ! -name '*.so' \
            2>/dev/null \
        | xargs ls -t 2>/dev/null \
        | head -1
    fi
}

# ── Main ──────────────────────────────────────────────────────────────────
cd "$PROJECT_DIR"

ensure_stub

# Build bench flags for cargo
CARGO_BENCH_FLAGS=()
for bench in "${BENCH_FILTER[@]}"; do
    CARGO_BENCH_FLAGS+=("--bench" "$bench")
done

# Compile benchmarks without running them
echo "Compiling benchmarks ..."
CARGO_OUTPUT=$(cargo bench --features "$FEATURES" "${CARGO_BENCH_FLAGS[@]}" --no-run 2>&1)
echo "$CARGO_OUTPUT"

# Run each benchmark binary with the stub preloaded
EXIT_CODE=0
for bench in "${BENCH_FILTER[@]}"; do
    BENCH_BIN=$(find_bench_binary "$bench")
    if [[ -z "${BENCH_BIN:-}" ]]; then
        echo "ERROR: Could not find benchmark binary for '$bench'" >&2
        EXIT_CODE=1
        continue
    fi

    echo ""
    echo "Running: $(basename "$BENCH_BIN") (with $(basename "$STUB_LIB"))"
    export "$PRELOAD_VAR"="$STUB_LIB"
    if ! "$BENCH_BIN" --bench "${CRITERION_ARGS[@]+"${CRITERION_ARGS[@]}"}"; then
        EXIT_CODE=1
    fi
done

exit $EXIT_CODE
