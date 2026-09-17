#!/usr/bin/env bash
# =============================================================================
# Build the Docker image for pg_trickle E2E integration tests.
#
# This script builds a multi-stage Docker image that:
#   1. Compiles the extension from source (Rust + cargo-pgrx)
#   2. Installs it into a clean postgres:18.3 image
#
# The resulting image can be used by testcontainers-rs in the E2E tests.
#
# Build speed:
#   On first call, the pre-built builder base image pg_trickle_builder:pg18
#   is built automatically (Rust + cargo-pgrx + pgrx init, ~7 min once).
#   Subsequent calls to this script (without --no-cache) skip the builder
#   image step and take ~2-3 min cold / ~30 s warm.
#
# Usage:
#   ./tests/build_e2e_image.sh              # default build (auto-builds builder)
#   ./tests/build_e2e_image.sh --no-cache   # force full rebuild of both images
# =============================================================================
set -euo pipefail

IMAGE_NAME="pg_trickle_e2e"
IMAGE_TAG="latest"
# Allow CI (or a developer) to override the builder image via the environment.
# In CI the "Pull builder image from GHCR" step pulls the pre-built image and
# tags it as pg_trickle_builder:pg18 so this default is found immediately.
# To point directly at GHCR without a retag step:
#   BUILDER_IMAGE=ghcr.io/trickle-labs/pg-trickle/builder:pg18 ./tests/build_e2e_image.sh
BUILDER_IMAGE="${BUILDER_IMAGE:-pg_trickle_builder:pg18}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DOCKER_PLATFORM="${DOCKER_PLATFORM:-${DOCKER_DEFAULT_PLATFORM:-linux/$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')}}"

# Pass through any extra args (e.g. --no-cache)
EXTRA_ARGS="${*:-}"

# ── Ensure the builder base image is available ───────────────────────────────
# Keep the compiler and runtime architectures identical. This also catches a
# stale local builder when DOCKER_DEFAULT_PLATFORM selects another target.
BUILDER_EXISTS=$(docker image inspect "${BUILDER_IMAGE}" \
    --format='{{.Os}}/{{.Architecture}}' 2>/dev/null || echo "")
if [[ "${BUILDER_EXISTS}" != "${DOCKER_PLATFORM}" ]]; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    if [[ -z "${BUILDER_EXISTS}" ]]; then
        echo "  Builder image not found: ${BUILDER_IMAGE}"
    else
        echo "  Builder image platform mismatch: got ${BUILDER_EXISTS}, need ${DOCKER_PLATFORM}"
    fi
    echo "  Building it now for ${DOCKER_PLATFORM} …"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    docker build \
        --platform "${DOCKER_PLATFORM}" \
        --load \
        --provenance=false \
        -t "${BUILDER_IMAGE}" \
        -f "${SCRIPT_DIR}/Dockerfile.builder" \
        "${PROJECT_ROOT}"
else
    echo "  Builder image present (${BUILDER_EXISTS}): ${BUILDER_IMAGE}"
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Building E2E test image: ${IMAGE_NAME}:${IMAGE_TAG}"
echo "  Project root: ${PROJECT_ROOT}"
echo "  Dockerfile:   ${SCRIPT_DIR}/Dockerfile.e2e"
echo "  Builder image: ${BUILDER_IMAGE}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [[ -n "${BUILDX_CACHE_SCOPE:-}" ]]; then
    docker buildx build \
        --platform "${DOCKER_PLATFORM}" \
        --load \
        --cache-from "type=gha,scope=${BUILDX_CACHE_SCOPE}" \
        --cache-to "type=gha,scope=${BUILDX_CACHE_SCOPE},mode=max,ignore-error=true" \
        -t "${IMAGE_NAME}:${IMAGE_TAG}" \
        -f "${SCRIPT_DIR}/Dockerfile.e2e" \
        --build-arg "BUILDER_IMAGE=${BUILDER_IMAGE}" \
        ${EXTRA_ARGS} \
        "${PROJECT_ROOT}"
else
    docker build \
        --platform "${DOCKER_PLATFORM}" \
        -t "${IMAGE_NAME}:${IMAGE_TAG}" \
        -f "${SCRIPT_DIR}/Dockerfile.e2e" \
        --build-arg "BUILDER_IMAGE=${BUILDER_IMAGE}" \
        ${EXTRA_ARGS} \
        "${PROJECT_ROOT}"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ✓ Image built: ${IMAGE_NAME}:${IMAGE_TAG}"
IMAGE_SIZE=$(docker image inspect "${IMAGE_NAME}:${IMAGE_TAG}" \
    --format='{{.Size}}' 2>/dev/null || echo "0")
if command -v numfmt &>/dev/null; then
    echo "  Image size: $(echo "${IMAGE_SIZE}" | numfmt --to=iec)"
elif command -v awk &>/dev/null; then
    echo "  Image size: $(echo "${IMAGE_SIZE}" | awk '{printf "%.0f MB", $1/1024/1024}')"
else
    echo "  Image size: ${IMAGE_SIZE} bytes"
fi
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "To test manually:"
echo "  docker run --rm -d --name pgs-e2e -e POSTGRES_PASSWORD=postgres -p 15432:5432 ${IMAGE_NAME}:${IMAGE_TAG}"
echo "  sleep 3"
echo "  psql -h localhost -p 15432 -U postgres -c \"CREATE EXTENSION pg_trickle;\""
echo "  docker stop pgs-e2e"
