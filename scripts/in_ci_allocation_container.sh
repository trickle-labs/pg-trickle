#!/usr/bin/env bash
# Keep compiler/userspace fixed despite GitHub-hosted runner image rollouts.
set -euo pipefail

: "${GITHUB_WORKSPACE:?requires the Actions workspace}"
: "${RUNNER_TEMP:?requires the Actions temporary directory}"
image_id=$(docker image inspect pg_trickle_comparison:latest --format '{{.Id}}')
exec docker run --rm --network host \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "$GITHUB_WORKSPACE:$GITHUB_WORKSPACE" \
    -v "$RUNNER_TEMP:$RUNNER_TEMP" \
    -v /tmp:/tmp \
    -w "$GITHUB_WORKSPACE" \
    -e NEXTEST_PROFILE -e NEXTEST_RETRIES -e CARGO_INCREMENTAL -e CARGO_TERM_COLOR \
    -e PGS_E2E_IMAGE -e PGT_EXTENSION_DIR -e DOCKER_PLATFORM \
    -e CI -e GITHUB_ACTIONS -e GITHUB_WORKSPACE -e RUNNER_TEMP \
    -e ImageOS -e ImageVersion -e ALLOCATION -e TIER \
    -e CI_ALLOCATION_CONTAINER="$image_id" \
    --entrypoint /bin/bash "$image_id" \
    -c 'git config --global --add safe.directory "$PWD"; exec "$@"' bash "$@"
