#!/usr/bin/env bash
# OPS-10-03: Update base image SHA256 digests in Dockerfiles.
#
# Run this script quarterly or when a PostgreSQL patch release is needed.
# It resolves the current manifest-list digests for postgres:18.6-bookworm and
# postgres:18.7-bookworm, then patches the relevant Dockerfiles in-place.
#
# IMPORTANT: We pin the manifest-list digest (not a per-platform digest) so
# that multi-platform builds (linux/amd64 + linux/arm64) each pull the correct
# architecture variant.  A per-platform digest causes the wrong-arch image to
# be fetched on the other builder, making apt-get fail with exit code 255.
#
# Usage:
#   scripts/update_base_image_digests.sh
#
# Requirements:
#   - docker buildx (for imagetools inspect)
#
# See also: CONTRIBUTING.md — "Updating base image digests"

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

BASE_IMAGE="postgres:18.6-bookworm"
E2E_BASE_IMAGE="postgres:18.7-bookworm"

# Resolve the manifest-list (OCI image index) digest — platform-agnostic.
# Docker/buildx picks the right platform-specific image at build time.
echo "Resolving manifest-list digest for ${BASE_IMAGE}..."
DIGEST=$(docker buildx imagetools inspect "${BASE_IMAGE}" --format '{{.Manifest.Digest}}' 2>/dev/null)

if [[ -z "${DIGEST}" ]]; then
  echo "ERROR: Could not resolve digest for ${BASE_IMAGE}" >&2
  exit 1
fi

echo "Resolved: ${BASE_IMAGE}@${DIGEST}"

echo "Resolving manifest-list digest for ${E2E_BASE_IMAGE}..."
E2E_DIGEST=$(docker buildx imagetools inspect "${E2E_BASE_IMAGE}" --format '{{.Manifest.Digest}}' 2>/dev/null)

if [[ -z "${E2E_DIGEST}" ]]; then
  echo "ERROR: Could not resolve digest for ${E2E_BASE_IMAGE}" >&2
  exit 1
fi

echo "Resolved: ${E2E_BASE_IMAGE}@${E2E_DIGEST}"

# Patch Dockerfiles — replace any existing @sha256:... pin or tag-only reference.
DOCKERFILES=(
  "${REPO_ROOT}/Dockerfile.demo"
  "${REPO_ROOT}/Dockerfile.ghcr"
  "${REPO_ROOT}/tests/Dockerfile.e2e-asan"
)

for f in "${DOCKERFILES[@]}"; do
  if [[ ! -f "${f}" ]]; then
    echo "WARNING: ${f} not found, skipping"
    continue
  fi
  # Use perl for cross-platform in-place edit (macOS sed -i requires extension).
  perl -pi -e "s|FROM \Q${BASE_IMAGE%%@*}\E(@sha256:[a-f0-9]+)?|FROM ${BASE_IMAGE%%@*}@${DIGEST}|g" "${f}"
  echo "Patched ${f}"
done

E2E_DOCKERFILE="${REPO_ROOT}/tests/Dockerfile.e2e"
perl -pi -e "s|FROM \\Q${E2E_BASE_IMAGE}\\E(@sha256:[a-f0-9]+)?|FROM ${E2E_BASE_IMAGE}@${E2E_DIGEST}|g" "${E2E_DOCKERFILE}"
echo "Patched ${E2E_DOCKERFILE}"

echo ""
echo "Done. Commit the updated Dockerfiles:"
echo "  git add Dockerfile.demo Dockerfile.ghcr tests/Dockerfile.e2e tests/Dockerfile.e2e-asan"
echo "  git commit -m 'chore(docker): update base image digests'"
