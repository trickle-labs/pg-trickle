#!/usr/bin/env bash
# check_version_sync.sh — verify version references match Cargo.toml
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

PASS=true
EXPECTED_TAG=""
PACKAGE_DIR="${PG_TRICKLE_PKG_DIR:-}"
ARCHIVE_PATH=""
IMAGE_REF=""

usage() {
    cat <<'EOF'
Usage: scripts/check_version_sync.sh [options]

Options:
  --expected-tag <tag>   Require the triggering Git tag to equal v<Cargo version>.
  --package-dir <dir>    Inspect packaged pg_trickle.control and install SQL.
  --archive <path>       Inspect a built release/PGXN archive name and contents.
  --image <ref>          Inspect OCI image metadata labels for a local image.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --expected-tag)
            EXPECTED_TAG="${2:?missing value for --expected-tag}"
            shift 2
            ;;
        --package-dir)
            PACKAGE_DIR="${2:?missing value for --package-dir}"
            shift 2
            ;;
        --archive)
            ARCHIVE_PATH="${2:?missing value for --archive}"
            shift 2
            ;;
        --image)
            IMAGE_REF="${2:?missing value for --image}"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ -z "$EXPECTED_TAG" && "${GITHUB_REF:-}" == refs/tags/v* ]]; then
    EXPECTED_TAG="${GITHUB_REF_NAME:-${GITHUB_REF#refs/tags/}}"
fi

read_cargo_field() {
    local pattern="$1"
    python3 - "$pattern" <<'PY'
import pathlib
import re
import sys

pattern = sys.argv[1]
text = pathlib.Path("Cargo.toml").read_text()
match = re.search(pattern, text, re.MULTILINE)
print(match.group(1) if match else "")
PY
}

read_control_default_version() {
    local path="$1"
    python3 - "$path" <<'PY'
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
match = re.search(r"^default_version\s*=\s*'([^']+)'", text, re.MULTILINE)
print(match.group(1) if match else "")
PY
}

read_meta_field() {
    local expression="$1"
    python3 - "$expression" <<'PY'
import json
import sys

expression = sys.argv[1]
data = json.load(open("META.json"))
if expression == "version":
    print(data.get("version", ""))
elif expression == "provides.pg_trickle.version":
    print(data.get("provides", {}).get("pg_trickle", {}).get("version", ""))
else:
    print("")
PY
}

read_lock_version() {
    python3 <<'PY'
import pathlib
import re

text = pathlib.Path("Cargo.lock").read_text()
match = re.search(
    r'\[\[package\]\]\s+name = "pg_trickle"\s+version = "([^"]+)"',
    text,
    re.MULTILINE,
)
print(match.group(1) if match else "")
PY
}

check_pass() {
    echo "  OK  $1"
}

check_fail() {
    echo "  FAIL $1"
    PASS=false
}

VERSION="$(read_cargo_field '^version\s*=\s*"([^"]+)"')"
echo "Cargo version: $VERSION"

if [[ -z "$VERSION" ]]; then
    echo "Could not read Cargo.toml package version." >&2
    exit 2
fi

# 1. Cargo.lock package entry must match Cargo.toml
LOCK_VERSION="$(read_lock_version)"
if [[ "$LOCK_VERSION" == "$VERSION" ]]; then
    check_pass "Cargo.lock package version = $LOCK_VERSION"
else
    check_fail "Cargo.lock package version ($LOCK_VERSION) != Cargo.toml ($VERSION)"
fi

# 2. Archive SQL for the current version must exist
ARCHIVE_SQL="sql/archive/pg_trickle--${VERSION}.sql"
if [[ -f "$ARCHIVE_SQL" ]]; then
    check_pass "archive SQL exists: $ARCHIVE_SQL"
else
    check_fail "archive SQL missing: $ARCHIVE_SQL"
    echo "       Run cargo pgrx package, then archive the generated install SQL."
fi

# 3. Upgrade script from the previous step must exist
UPGRADE_SQL="$(find sql -maxdepth 1 -type f -name "pg_trickle--*--${VERSION}.sql" | sort | head -1)"
if [[ -n "$UPGRADE_SQL" ]]; then
    check_pass "upgrade script exists: $UPGRADE_SQL"
else
    check_fail "no upgrade script ending in --${VERSION}.sql found in sql/"
fi

# 4. CI upgrade E2E uses auto-discovery
if grep -q 'upgrade-e2e-prepare' .github/workflows/ci.yml && \
   grep -q 'fromJson(needs.upgrade-e2e-prepare.outputs.matrix)' .github/workflows/ci.yml; then
    check_pass "ci.yml upgrade matrix uses auto-discovery"
else
    check_fail "ci.yml upgrade-e2e-tests does not use auto-discovery prepare job"
fi

# 5. CI upgrade-check uses auto-discovery loop
if grep -q 'check_upgrade_completeness.sh' .github/workflows/ci.yml && \
   grep -q 'for f in sql/pg_trickle--\*--\*.sql' .github/workflows/ci.yml; then
    check_pass "ci.yml upgrade-check uses auto-discovery loop"
else
    check_fail "ci.yml upgrade-check does not use auto-discovery loop"
fi

# 6. justfile build-upgrade-image and test-upgrade defaults match
JF_TO="$(grep -E '^(build-upgrade-image|test-upgrade) ' justfile | sed 's/.*to="\([^"]*\)".*/\1/' | sort -u)"
BAD_JF="$(echo "$JF_TO" | grep -v "^${VERSION}$" || true)"
if [[ -z "$BAD_JF" ]]; then
    check_pass "justfile upgrade defaults = $VERSION"
else
    check_fail "justfile upgrade defaults are stale: $BAD_JF (expected $VERSION)"
fi

# 7. Upgrade E2E tests must not hardcode a version fallback
BAD_TEST_FALLBACKS="$(grep 'PGS_UPGRADE_TO' tests/e2e_upgrade_tests.rs | grep -E '"[0-9]+\.[0-9]+\.[0-9]+"' || true)"
if [[ -z "$BAD_TEST_FALLBACKS" ]]; then
    check_pass "e2e upgrade test defaults follow env!(\"CARGO_PKG_VERSION\")"
else
    check_fail "e2e_upgrade_tests.rs still hardcodes version fallback(s)"
    echo "$BAD_TEST_FALLBACKS" | sed 's/^/       /'
fi

# 8. META.json top-level and provides version must match
META_VERSION="$(read_meta_field 'version')"
META_PROVIDES="$(read_meta_field 'provides.pg_trickle.version')"
if [[ "$META_VERSION" == "$VERSION" ]]; then
    check_pass "META.json .version = $META_VERSION"
else
    check_fail "META.json .version ($META_VERSION) != Cargo.toml ($VERSION)"
fi
if [[ "$META_PROVIDES" == "$VERSION" ]]; then
    check_pass "META.json .provides.pg_trickle.version = $META_PROVIDES"
else
    check_fail "META.json .provides.pg_trickle.version ($META_PROVIDES) != Cargo.toml ($VERSION)"
fi

# 9. Source control template must either use @CARGO_VERSION@ or match directly
CONTROL_VERSION="$(read_control_default_version pg_trickle.control)"
if [[ "$CONTROL_VERSION" == "@CARGO_VERSION@" || "$CONTROL_VERSION" == "$VERSION" ]]; then
    check_pass "pg_trickle.control default_version is template-safe ($CONTROL_VERSION)"
else
    check_fail "pg_trickle.control default_version ($CONTROL_VERSION) is neither @CARGO_VERSION@ nor $VERSION"
fi

# 10. Dockerfile VERSION ARG defaults must match Cargo.toml
for dfile in Dockerfile.hub Dockerfile.ghcr; do
    if [[ -f "$dfile" ]]; then
        bad_df="$(grep 'ARG VERSION=' "$dfile" | grep -v "=${VERSION}$" || true)"
        if [[ -z "$bad_df" ]]; then
            check_pass "$dfile ARG VERSION defaults = $VERSION"
        else
            check_fail "$dfile has stale ARG VERSION default(s)"
            echo "$bad_df" | sed 's/^/       /'
        fi
    fi
done

# 11. Triggering Git tag must match v<Cargo version> when present
if [[ -n "$EXPECTED_TAG" ]]; then
    case "$EXPECTED_TAG" in
        v*)
            ;;
        *)
            EXPECTED_TAG="v${EXPECTED_TAG}"
            ;;
    esac
    if [[ "$EXPECTED_TAG" == "v${VERSION}" ]]; then
        check_pass "triggering Git tag = $EXPECTED_TAG"
    else
        check_fail "triggering Git tag ($EXPECTED_TAG) != v${VERSION}"
    fi
fi

# 12. Optional packaged control/install SQL checks
if [[ -n "$PACKAGE_DIR" ]]; then
    if [[ ! -d "$PACKAGE_DIR" ]]; then
        check_fail "package directory not found: $PACKAGE_DIR"
    else
        PKG_CONTROL="$(find "$PACKAGE_DIR" -type f -name 'pg_trickle.control' | head -1)"
        PKG_SQL="$(find "$PACKAGE_DIR" -type f -name "pg_trickle--${VERSION}.sql" | head -1)"
        if [[ -n "$PKG_CONTROL" ]]; then
            PKG_CONTROL_VERSION="$(read_control_default_version "$PKG_CONTROL")"
            if [[ "$PKG_CONTROL_VERSION" == "$VERSION" ]]; then
                check_pass "packaged control default_version = $PKG_CONTROL_VERSION"
            else
                check_fail "packaged control default_version ($PKG_CONTROL_VERSION) != $VERSION"
            fi
        else
            check_fail "packaged pg_trickle.control not found under $PACKAGE_DIR"
        fi
        if [[ -n "$PKG_SQL" ]]; then
            check_pass "packaged install SQL exists: $PKG_SQL"
        else
            check_fail "packaged install SQL pg_trickle--${VERSION}.sql not found under $PACKAGE_DIR"
        fi
    fi
fi

# 13. Optional archive checks (PGXN/release artifacts)
if [[ -n "$ARCHIVE_PATH" ]]; then
    if [[ ! -f "$ARCHIVE_PATH" ]]; then
        check_fail "archive not found: $ARCHIVE_PATH"
    else
        ARCHIVE_BASENAME="$(basename "$ARCHIVE_PATH")"
        case "$ARCHIVE_BASENAME" in
            "pg_trickle-${VERSION}.zip"|pg_trickle-"${VERSION}"-pg*.tar.gz|pg_trickle-"${VERSION}"-pg*.zip)
                check_pass "archive filename matches $VERSION: $ARCHIVE_BASENAME"
                ;;
            *)
                check_fail "archive filename does not encode version $VERSION: $ARCHIVE_BASENAME"
                ;;
        esac

        if python3 - "$ARCHIVE_PATH" "$VERSION" <<'PY'
import sys
import tarfile
import zipfile
from pathlib import PurePosixPath

archive, version = sys.argv[1:3]
names = []
if archive.endswith(".zip"):
    with zipfile.ZipFile(archive) as zf:
        names = zf.namelist()
elif archive.endswith(".tar.gz"):
    with tarfile.open(archive, "r:gz") as tf:
        names = tf.getnames()
else:
    raise SystemExit(1)

has_control = any(PurePosixPath(name).name == "pg_trickle.control" for name in names)
has_sql = any(PurePosixPath(name).name == f"pg_trickle--{version}.sql" for name in names)
needs_meta = archive.endswith(".zip") and PurePosixPath(archive).name == f"pg_trickle-{version}.zip"
has_meta = any(PurePosixPath(name).name == "META.json" for name in names)

ok = has_control and has_sql and (not needs_meta or has_meta)
raise SystemExit(0 if ok else 1)
PY
        then
            check_pass "archive contents include packaged version surfaces"
        else
            check_fail "archive contents are missing pg_trickle.control, pg_trickle--${VERSION}.sql, or META.json"
        fi
    fi
fi

# 14. Optional OCI metadata label check
if [[ -n "$IMAGE_REF" ]]; then
    if ! command -v docker >/dev/null 2>&1; then
        check_fail "docker is required for --image checks"
    else
        OCI_VERSION="$(docker image inspect "$IMAGE_REF" --format='{{ index .Config.Labels "org.opencontainers.image.version" }}' 2>/dev/null || true)"
        if [[ -z "$OCI_VERSION" ]]; then
            check_fail "could not read org.opencontainers.image.version for $IMAGE_REF"
        elif [[ "$OCI_VERSION" == "$VERSION" ]]; then
            check_pass "OCI label org.opencontainers.image.version = $OCI_VERSION"
        else
            check_fail "OCI label org.opencontainers.image.version ($OCI_VERSION) != $VERSION"
        fi
    fi
fi

if $PASS; then
    echo ""
    echo "All version checks passed for v${VERSION}."
else
    echo ""
    echo "One or more version checks FAILED. Fix the issues above."
    exit 1
fi
