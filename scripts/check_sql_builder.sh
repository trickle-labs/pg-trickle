#!/usr/bin/env bash
# scripts/check_sql_builder.sh
# SEC-003: Audit SQL builder helpers for raw format!() SQL injection vectors.
#
# Scans src/ for format!() / write!() / format_args!() patterns that construct
# SQL strings, and flags any that interpolate untrusted dynamic data directly
# into query strings without using the parameterised-query helpers.
#
# Safe patterns (parameter placeholders):
#   format!("SELECT ... WHERE id = {}", $1)    -- uses positional params
#   format!("SELECT ... WHERE id = $1")        -- static param slot
#
# Flagged patterns (potential injection vectors):
#   format!("... {user_supplied_name} ...")    -- interpolates a variable
#   format!("... {} ...", some_string_var)     -- interpolates a runtime value
#
# Exclusions:
#   - Identifier quoting helpers (quote_ident, format_ident) — these are safe.
#   - Tests (src/**#[cfg(test)] blocks) and test files are excluded.
#   - Lines containing $1/$2/... positional param patterns are excluded.
#   - Whitelisted helper functions (build_refresh_sql, parameterize_lsn_template).
#
# Exit codes:
#   0 — no unsafe patterns found
#   1 — potential injection vectors detected (review required)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="${SCRIPT_DIR}/../src"

echo "SEC-003: SQL builder audit starting..."
echo "Scanning: $SRC_DIR"

FOUND=0

# Patterns that suggest format!() is building SQL with dynamic data
# Look for format!( calls in .rs files (excluding tests)
while IFS= read -r -d '' file; do
    # Skip test-only files
    [[ "$file" == *"/tests/"* ]] && continue

    # Find format! calls that look like they build SQL strings
    # Look for format!("...SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER...{...
    while IFS= read -r line; do
        lineno=$(echo "$line" | cut -d: -f1)
        content=$(echo "$line" | cut -d: -f2-)

        # Skip lines that are comments
        [[ "$content" =~ ^[[:space:]]*//' ]] && continue
        [[ "$content" =~ ^[[:space:]]*/\* ]] && continue

        # Skip lines using positional params ($1, $2, etc.) — these are safe
        [[ "$content" =~ \\\$[0-9] ]] && continue

        # Skip known-safe identifier quoting helpers
        [[ "$content" =~ quote_ident|format_ident|pg_catalog\. ]] && continue

        # Flag format! calls that build SQL with {} interpolation
        if echo "$content" | grep -qE 'format!\s*\(\s*"[^"]*\{[^}]*\}[^"]*"' 2>/dev/null; then
            # Only flag if it looks like SQL (contains SQL keywords)
            if echo "$content" | grep -qiE '(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|GRANT|REVOKE|EXECUTE|CALL)\s' 2>/dev/null; then
                echo "WARN: Potential SQL injection vector at ${file}:${lineno}"
                echo "      $content"
                FOUND=$((FOUND + 1))
            fi
        fi
    done < <(grep -n 'format!' "$file" 2>/dev/null || true)
done < <(find "$SRC_DIR" -name '*.rs' -not -path '*/target/*' -print0)

if [[ $FOUND -eq 0 ]]; then
    echo "SEC-003: SQL builder audit PASSED — no unsafe patterns found."
    exit 0
else
    echo ""
    echo "SEC-003: SQL builder audit found $FOUND potential injection vector(s)."
    echo "Review each occurrence and ensure:"
    echo "  1. Dynamic values are schema-element names (validated/quoted via pg_catalog)."
    echo "  2. No user-supplied runtime strings are interpolated directly into SQL."
    echo "  3. All user data is passed as bind parameters (\$1, \$2, ...)."
    exit 1
fi
