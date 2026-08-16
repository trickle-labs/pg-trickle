#!/usr/bin/env bash
# scripts/run_unit_tests.sh — Run unit tests with pg_stub preloaded
#
# pgrx extensions reference PostgreSQL server symbols (e.g.
# CurrentMemoryContext, SPI_connect) that are only available inside the
# postgres process.  Pure-Rust unit tests never call those symbols, but the
# test binary still links them.
#
# On macOS 26+ (Tahoe) dyld eagerly resolves all flat-namespace symbols at
# load time, causing an immediate crash.  On newer Linux toolchains the
# linker may use --no-as-needed / -z now, which has the same effect.
#
# Workaround (all platforms):
#   1. Compile a tiny C stub library that provides NULL/no-op definitions
#      for every PostgreSQL symbol the binary references.
#   2. Compile the test binary with `--no-run`.
#   3. Run the binary with LD_PRELOAD (Linux) or DYLD_INSERT_LIBRARIES
#      (macOS) pointing to the stub.

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
        # On Apple Silicon, native toolchain binaries (cc, ld) are compiled as
        # arm64e.  DYLD_INSERT_LIBRARIES requires the injected dylib to match
        # the architecture of the receiving process.  arm64e dylibs are also
        # loadable by plain arm64 processes (system libraries such as
        # libSystem.dylib are arm64e and are used by arm64 binaries every day),
        # so a single arm64e slice serves both the toolchain and the Rust test
        # binary on all Apple Silicon machines.
        if [[ "$(uname -m)" == "arm64" ]]; then
            STUB_CC_FLAGS="-shared -arch arm64e -install_name @rpath/libpg_stub.dylib"
        else
            STUB_CC_FLAGS="-shared -install_name @rpath/libpg_stub.dylib"
        fi
        PRELOAD_VAR="DYLD_INSERT_LIBRARIES"
        ;;
    *)
        STUB_LIB="$CARGO_TARGET_DIR/libpg_stub.so"
        STUB_CC_FLAGS="-shared -fPIC"
        PRELOAD_VAR="LD_PRELOAD"
        ;;
esac

# ── Helper: ensure_stub ───────────────────────────────────────────────────
ensure_stub() {
    if [[ ! -f "$STUB_LIB" ]] || [[ "$STUB_SRC" -nt "$STUB_LIB" ]]; then
        echo "Building $(basename "$STUB_LIB") ..."
        mkdir -p "$(dirname "$STUB_LIB")"
        # shellcheck disable=SC2086
        cc $STUB_CC_FLAGS -o "$STUB_LIB" "$STUB_SRC" 2>&1
    fi
}

# ── Main ──────────────────────────────────────────────────────────────────
cd "$PROJECT_DIR"

ensure_stub

# Unit tests are pure Rust and complete in a few seconds.  Run them in one
# thread because pgrx's test stubs reject concurrent PostgreSQL FFI access.
# This still avoids the ~1.8 s per-process startup overhead of nextest.
#
# Pass USE_NEXTEST=1 to force nextest (e.g. for filter expressions or CI
# retry support):
#   USE_NEXTEST=1 ./scripts/run_unit_tests.sh pg18 -E 'test(foo)'

if [[ "${USE_NEXTEST:-0}" == "1" ]] && command -v cargo-nextest >/dev/null 2>&1; then
    echo "Running with cargo-nextest (with $(basename "$STUB_LIB"))"
    export "$PRELOAD_VAR"="$STUB_LIB"
    cargo nextest run --lib --features "$FEATURES" "${@:2}"
    exit $?
fi

echo "Running unit tests with cargo test (with $(basename "$STUB_LIB"))"
export "$PRELOAD_VAR"="$STUB_LIB"
cargo test --lib --features "$FEATURES" "${@:2}" -- --test-threads=1
