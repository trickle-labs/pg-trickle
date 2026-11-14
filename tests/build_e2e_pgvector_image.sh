#!/usr/bin/env bash
# Build a version-specific image after tests/build_e2e_image.sh.
set -euo pipefail

version="${1:?expected pgvector version}"
case "$version" in
    0.8.1) image='pgvector/pgvector:0.8.1-pg18@sha256:508c5290cda481d4f5f846446a26e9c1b804766828a394a5861de1b348a18b4c' ;;
    0.8.7) image='pgvector/pgvector:0.8.7-pg18@sha256:c29f30688a9ad961691bdf0ec1650717cc132a5814619d80f80463d6e4c42e47' ;;
    *) printf 'Unsupported pgvector test version: %s\n' "$version" >&2; exit 2 ;;
esac

root="$(cd "$(dirname "$0")/.." && pwd)"
platform="${DOCKER_PLATFORM:-${DOCKER_DEFAULT_PLATFORM:-linux/$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')}}"
docker build --platform "$platform" \
    --build-arg "PGVECTOR_IMAGE=$image" \
    --build-arg "PGS_E2E_BASE_IMAGE=${PGS_E2E_BASE_IMAGE:-pg_trickle_e2e:latest}" \
    -t "pg_trickle_e2e_pgvector:${version}" \
    -f "$root/tests/Dockerfile.e2e-pgvector" "$root"
