#!/usr/bin/env bash
# check_stale_versions.sh — scan Dockerfile examples for stale pg_trickle image tags.
#
# Reads the current Cargo.toml version and reports any occurrence of a
# pg_trickle Docker image tag that hardcodes an old version number in
# a Dockerfile comment (# ... pgtrickle/pg_trickle:X.Y.Z or
# ghcr.io/trickle-labs/pg_trickle:X.Y.Z).
#
# Known safe patterns (never flagged):
#   - <version>                  — explicit placeholder
#   - @CARGO_VERSION@            — build-time substitution macro
#   - latest                     — Docker "latest" tag
#   - The current Cargo.toml version
#
# Usage:
#   ./scripts/check_stale_versions.sh
#
# Exit code:
#   0  — clean
#   1  — stale version reference(s) found

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

VERSION=$(python3 -c "
import re, pathlib
m = re.search(r'^version\s*=\s*\"([^\"]+)\"', pathlib.Path('Cargo.toml').read_text(), re.MULTILINE)
print(m.group(1))
")

echo "Current version: $VERSION"

# Pattern: Docker image tags in comments for pg_trickle images
# These look like:  pgtrickle/pg_trickle:X.Y.Z-pgNN  or  ghcr.io/trickle-labs/pg_trickle:X.Y.Z
PG_TRICKLE_TAG_RE='(pgtrickle/pg_trickle|ghcr\.io/trickle-labs/pg_trickle):[0-9]+\.[0-9]+\.[0-9]+'

PASS=true

for FILE in Dockerfile.hub Dockerfile.ghcr; do
    [[ -f "$FILE" ]] || continue

    while IFS= read -r MATCH; do
        # Extract just the version portion after the colon
        TAG_VERSION=$(echo "$MATCH" | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)
        [[ -z "$TAG_VERSION" ]] && continue

        # Allow the current version
        [[ "$TAG_VERSION" == "$VERSION" ]] && continue

        # Get line number and context for the match
        LINE_INFO=$(grep -n "$MATCH" "$FILE" | head -1)
        echo "  STALE  $FILE: image tag '$MATCH' has version '$TAG_VERSION' (current: '$VERSION')"
        echo "         Replace with: $(echo "$MATCH" | sed "s/$TAG_VERSION/<version>/g")"
        echo "         Line: $LINE_INFO"
        PASS=false
    done < <(grep -oE "$PG_TRICKLE_TAG_RE" "$FILE" || true)
done

if $PASS; then
    echo "check_stale_versions: all pg_trickle image tag references are clean."
    exit 0
else
    echo ""
    echo "check_stale_versions: FAILED — stale pg_trickle image tag(s) found."
    echo "Replace hard-coded versions with the current version ($VERSION)"
    echo "or use a placeholder: <version> or 'latest'."
    exit 1
fi
