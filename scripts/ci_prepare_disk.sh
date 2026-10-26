#!/usr/bin/env bash
# Avoid minutes of deleting unused SDKs when the hosted runner has ample space.
set -euo pipefail

if [[ "${GITHUB_ACTIONS:-}" != true || "${RUNNER_OS:-}" != Linux ]]; then
    echo "Disk preparation is only supported on Linux Actions runners" >&2
    exit 1
fi

available_kb=$(df -Pk "${GITHUB_WORKSPACE:-.}" | awk 'NR == 2 { print $4 }')
[[ "$available_kb" =~ ^[0-9]+$ ]]
if ((available_kb < 60 * 1024 * 1024)); then
    sudo rm -rf /usr/share/dotnet /usr/local/lib/android /opt/ghc \
        /usr/local/share/boost "${AGENT_TOOLSDIRECTORY:?}"
fi
df -h
