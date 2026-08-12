#!/usr/bin/env bash
# scripts/check_security_definer.sh — A45-5: SECURITY DEFINER CI check
#
# Validates that every SECURITY DEFINER function in Rust source and SQL
# migration files:
#   1. Has a corresponding SET search_path clause.
#   2. Does not include user-writable schemas (public) unless explicitly
#      justified with a nosemgrep annotation.
#
# Exit codes:
#   0 — all checks pass
#   1 — one or more violations found
#
# Usage:
#   ./scripts/check_security_definer.sh
#   ./scripts/check_security_definer.sh --verbose

VERBOSE=false
for arg in "$@"; do
  [[ "$arg" == "--verbose" ]] && VERBOSE=true
done

ERRORS=0
CHECKED=0

log() {
  [[ "$VERBOSE" == "true" ]] && echo "$*"
}

# Check a Rust source file for SECURITY DEFINER violations.
# Skips lines where SECURITY DEFINER appears in a Rust // comment.
check_rust_file() {
  local file="$1"
  local linenos
  # Only find lines where SECURITY DEFINER is NOT in a // comment
  # (i.e., the SECURITY DEFINER appears before any // on that line,
  # or the line has no // at all)
  mapfile -t linenos < <(grep -n "SECURITY DEFINER" "$file" \
    | grep -v "^[0-9]*:[[:space:]]*//" \
    | cut -d: -f1)

  for linenum in "${linenos[@]}"; do
    CHECKED=$((CHECKED + 1))
    log "  Checking Rust $file:$linenum"

    # Grab the surrounding 5 lines for context
    local context
    context=$(sed -n "${linenum},$((linenum + 5))p" "$file")

    # Check 1: must have SET search_path within the next 5 lines
    if ! echo "$context" | grep -qi "SET search_path"; then
      echo "ERROR: $file:$linenum — SECURITY DEFINER without SET search_path"
      ERRORS=$((ERRORS + 1))
    fi

    # Check 2: search_path must not include 'public' unless nosemgrep-annotated
    if echo "$context" | grep -qi "SET search_path" \
      && echo "$context" | grep -i "SET search_path" | grep -qi "public"; then
      if echo "$context" | grep -q "nosemgrep.*public\|public.*nosemgrep"; then
        log "  SKIP: $file:$linenum — nosemgrep annotation found for public"
      else
        echo "ERROR: $file:$linenum — SECURITY DEFINER search_path includes 'public' without justification"
        ERRORS=$((ERRORS + 1))
      fi
    fi
  done
}

# Check a SQL migration file for SECURITY DEFINER violations.
# Skips lines where SECURITY DEFINER appears in a -- comment.
check_sql_file() {
  local file="$1"
  local linenos
  # Only find lines where SECURITY DEFINER is NOT in a -- comment
  mapfile -t linenos < <(grep -n "SECURITY DEFINER" "$file" \
    | grep -v "^[0-9]*:[[:space:]]*--" \
    | grep -v "^[0-9]*:.*--.*SECURITY DEFINER" \
    | cut -d: -f1)

  for linenum in "${linenos[@]}"; do
    CHECKED=$((CHECKED + 1))
    log "  Checking SQL $file:$linenum"

    local context
    context=$(sed -n "${linenum},$((linenum + 5))p" "$file")

    if ! echo "$context" | grep -qi "SET search_path"; then
      echo "ERROR: $file:$linenum — SECURITY DEFINER without SET search_path"
      ERRORS=$((ERRORS + 1))
    fi

    if echo "$context" | grep -qi "SET search_path" \
      && echo "$context" | grep -i "SET search_path" | grep -qi "public"; then
      if echo "$context" | grep -q "nosemgrep.*public\|public.*nosemgrep"; then
        log "  SKIP: $file:$linenum — nosemgrep annotation found for public"
      else
        echo "ERROR: $file:$linenum — SECURITY DEFINER search_path includes 'public' without justification"
        ERRORS=$((ERRORS + 1))
      fi
    fi
  done
}

# ── 1. Rust sources ─────────────────────────────────────────────────────────

echo "Checking Rust sources (src/**/*.rs)..."
while IFS= read -r file; do
  check_rust_file "$file"
done < <(find src -name "*.rs" -type f | sort)

# ── 2. SQL migration files (current only, not archive) ──────────────────────

if [ -d sql ]; then
  echo "Checking SQL migration files (sql/*.sql, excluding sql/archive/)..."
  while IFS= read -r file; do
    check_sql_file "$file"
  done < <(find sql -maxdepth 1 -name "*.sql" -type f | sort)
fi

# ── 3. Summary ──────────────────────────────────────────────────────────────

echo ""
echo "SECURITY DEFINER check: $CHECKED location(s) checked, $ERRORS error(s)."

if [[ $ERRORS -gt 0 ]]; then
  echo ""
  echo "Remediation:"
  echo "  - Add 'SET search_path = pgtrickle, pg_catalog, pg_temp' after every SECURITY DEFINER."
  echo "  - Remove 'public' from search_path, or add a nosemgrep annotation with justification."
  exit 1
fi

echo "All checks passed."
exit 0
