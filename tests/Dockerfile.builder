# syntax=docker/dockerfile:1
# =============================================================================
# Pre-built base image for pg_trickle E2E test builds.
#
# Contains everything needed to compile the extension EXCEPT the source:
#   - PostgreSQL 18 + pg_config + headers
#   - Rust stable toolchain
#   - cargo-pgrx 0.18.0 (pre-compiled, ~3-4 min to build from source)
#   - pgrx initialized for PG 18
#
# This image only needs to be rebuilt when one of the above changes (i.e.
# when upgrading pgrx or the Rust toolchain — NOT on every source change).
#
# Usage:
#   # Build once:
#   just build-builder-image
#   # Then fast E2E builds forever:
#   just build-e2e-image    # ~2-3 min instead of ~10 min
#
# The image is tagged pg_trickle_builder:pg18 locally.
# For CI it can be pushed to ghcr.io to skip the builder build entirely.
# =============================================================================

FROM postgres:18.6

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        build-essential \
        libreadline-dev \
        zlib1g-dev \
        pkg-config \
        libssl-dev \
        libclang-dev \
        clang \
        postgresql-server-dev-18 \
        ca-certificates \
        git \
    && rm -rf /var/lib/apt/lists/*

# Install Rust stable toolchain
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"

# Compile and install cargo-pgrx.
# This is the main cost (~3-4 min). It lives in this layer so it's only
# ever recompiled when the version number changes.
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    cargo install --locked cargo-pgrx --version 0.18.0

# Initialize pgrx for PG 18 using the system pg_config.
# This writes per-version metadata into /root/.pgrx/.
RUN cargo pgrx init --pg18 /usr/bin/pg_config
