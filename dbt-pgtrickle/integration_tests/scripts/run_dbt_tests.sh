#!/usr/bin/env bash
# =============================================================================
# Run dbt-pgtrickle integration tests locally.
#
# Starts a pg_trickle E2E Docker container, runs the full dbt test flow, and
# cleans up. Requires Docker and a Python environment with dbt installed.
#
# Usage:
#   ./dbt-pgtrickle/integration_tests/scripts/run_dbt_tests.sh
#   ./dbt-pgtrickle/integration_tests/scripts/run_dbt_tests.sh --skip-build
#   ./dbt-pgtrickle/integration_tests/scripts/run_dbt_tests.sh --keep-container
#
# Environment variables:
#   DBT_VERSION       dbt-core version to install (default: 1.10)
#   PGPORT            PostgreSQL port (default: 15432, avoids conflicts)
#   CONTAINER_NAME    Docker container name (default: pgtrickle-dbt-local)
#   SKIP_BUILD        Set to "1" to skip Docker image rebuild
#   KEEP_CONTAINER    Set to "1" to keep the container after tests
# =============================================================================
set -euo pipefail

# ── Configuration ───────────────────────────────────────────────────────────
DBT_VERSION="${DBT_VERSION:-1.10}"
PGPORT="${PGPORT:-15432}"
CONTAINER_NAME="${CONTAINER_NAME:-pgtrickle-dbt-local}"
SKIP_BUILD="${SKIP_BUILD:-0}"
KEEP_CONTAINER="${KEEP_CONTAINER:-0}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INTEGRATION_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$INTEGRATION_DIR/../.." && pwd)"

# Ensure psql is on PATH (Homebrew postgresql kegs may not be linked)
for _pg_dir in \
    /opt/homebrew/opt/postgresql@18/bin \
    /opt/homebrew/opt/postgresql@17/bin \
    /opt/homebrew/opt/postgresql@16/bin \
    /usr/local/opt/postgresql@18/bin \
    /usr/local/opt/postgresql@17/bin; do
  if [[ -x "$_pg_dir/psql" ]]; then
    export PATH="$_pg_dir:$PATH"
    break
  fi
done

# Parse flags
for arg in "$@"; do
  case "$arg" in
    --skip-build) SKIP_BUILD=1 ;;
    --keep-container) KEEP_CONTAINER=1 ;;
    --help|-h)
      head -18 "$0" | tail -14
      exit 0
      ;;
    *)
      echo "Unknown argument: $arg" >&2
      exit 1
      ;;
  esac
done

# ── Cleanup trap ────────────────────────────────────────────────────────────
cleanup() {
  if [ "$KEEP_CONTAINER" = "0" ]; then
    echo ""
    echo "Cleaning up container ${CONTAINER_NAME}..."
    docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
  else
    echo ""
    echo "Container ${CONTAINER_NAME} kept running on port ${PGPORT}."
    echo "Connect: PGPORT=${PGPORT} psql -h localhost -U postgres"
    echo "Remove:  docker rm -f ${CONTAINER_NAME}"
  fi
}
trap cleanup EXIT

# ── Build Docker image (optional) ──────────────────────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  dbt-pgtrickle integration test runner"
echo "  dbt version: ${DBT_VERSION}"
echo "  Port: ${PGPORT}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

if [ "$SKIP_BUILD" = "0" ]; then
  echo "Building E2E Docker image..."
  "$PROJECT_ROOT/tests/build_e2e_image.sh"
  echo ""
else
  echo "Skipping Docker image build (--skip-build)"
  # Verify image exists
  if ! docker image inspect pg_trickle_e2e:latest &>/dev/null; then
    echo "ERROR: pg_trickle_e2e:latest not found. Run without --skip-build first." >&2
    exit 1
  fi
  echo ""
fi

# ── Start container ─────────────────────────────────────────────────────────
# Remove any stale container with the same name
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

echo "Starting PostgreSQL with pg_trickle on port ${PGPORT}..."
docker run -d \
  --name "$CONTAINER_NAME" \
  -e POSTGRES_PASSWORD=postgres \
  -p "${PGPORT}:5432" \
  pg_trickle_e2e:latest

echo "Waiting for PostgreSQL to accept connections..."
for i in $(seq 1 30); do
  if docker exec "$CONTAINER_NAME" pg_isready -U postgres -q 2>/dev/null; then
    echo "PostgreSQL is ready (attempt $i)"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERROR: PostgreSQL not ready after 30s" >&2
    docker logs "$CONTAINER_NAME" | tail -20
    exit 1
  fi
  sleep 1
done

# The postgres image does a controlled restart after running initdb init scripts.
# Retry the CREATE EXTENSION step so we don't hit the brief shutdown window.
echo "Creating pg_trickle extension..."
for i in $(seq 1 10); do
  if docker exec "$CONTAINER_NAME" \
      psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS pg_trickle;" 2>/dev/null; then
    break
  fi
  if [ "$i" -eq 10 ]; then
    echo "ERROR: Could not create pg_trickle extension after 10 attempts" >&2
    docker logs "$CONTAINER_NAME" | tail -20
    exit 1
  fi
  sleep 1
done

# ── Set up dbt Python environment ─────────────────────────────────────────
# dbt-core 1.10 requires Python <=3.13 (mashumaro dependency incompatible with 3.14).
# Use a dedicated .venv-dbt virtual environment so we don't pollute the main
# project venv (which may be Python 3.14+).
DBT_VENV="$PROJECT_ROOT/.venv-dbt"
if [[ ! -f "$DBT_VENV/bin/activate" ]]; then
  echo "Creating dbt venv at $DBT_VENV using Python 3.13..."
  PYTHON_DBT="$(command -v python3.13 || command -v python3.12 || command -v python3)"
  "$PYTHON_DBT" -m venv "$DBT_VENV"
fi
# shellcheck source=/dev/null
source "$DBT_VENV/bin/activate"
if ! command -v dbt &>/dev/null; then
  echo ""
  echo "Installing dbt-core~=${DBT_VERSION}.0 and dbt-postgres~=${DBT_VERSION}.0..."
  pip install --quiet "dbt-core~=${DBT_VERSION}.0" "dbt-postgres~=${DBT_VERSION}.0"
fi
echo ""
echo "dbt version:"
dbt --version
echo ""

# ── Run tests ───────────────────────────────────────────────────────────────
export PGHOST=localhost
export PGPORT="${PGPORT}"
export PGUSER=postgres
export PGPASSWORD=postgres
export PGDATABASE=postgres

cd "$INTEGRATION_DIR"

echo "════════════════════════════════════════════════════════════════"
echo "  Running dbt integration tests"
echo "════════════════════════════════════════════════════════════════"

echo ""
echo "── dbt deps ──"
dbt deps

echo ""
echo "── dbt seed ──"
dbt seed

echo ""
echo "── dbt run (initial create) ──"
dbt run

echo ""
echo "── Waiting for stream tables to populate ──"
./scripts/wait_for_populated.sh order_totals 30
./scripts/wait_for_populated.sh order_extremes 30
./scripts/wait_for_populated.sh customer_stats 30
./scripts/wait_for_populated.sh order_totals_compat 30

echo ""
echo "── dbt test (initial) ──"
dbt test --select assert_compat_model_created assert_compat_data_correct
# assert_compat_alter_applied requires the post-alter state; exclude it here
# so the initial full test run (schedule still '1m') does not fail it.
dbt test --exclude assert_compat_alter_applied

echo ""
echo "── T-5: dbt ALTER flow ─────────────────────────────────────────────"
echo "   Change order_totals_compat schedule from '1m' to '3m', then run"
echo "   dbt run — the adapter must issue ALTER STREAM TABLE (not recreate)."
# Patch schedule in the model file.
COMPAT_MODEL="models/marts/order_totals_compat.sql"
cp "$COMPAT_MODEL" "${COMPAT_MODEL}.bak"
sed -i.tmp "s/schedule='1m'/schedule='3m'/" "$COMPAT_MODEL"
rm -f "${COMPAT_MODEL}.tmp"

dbt run --select order_totals_compat
echo "  (dbt alter run completed)"

# Verify the alter was applied in the catalog.
dbt test --select assert_compat_alter_applied
echo "  (alter assertion passed)"

# Restore the original model definition.
mv "${COMPAT_MODEL}.bak" "$COMPAT_MODEL"
echo "── T-5: ALTER flow complete ────────────────────────────────────────"

echo ""
echo "── dbt run (idempotent no-op — same config, same query) ──"
dbt run
echo "  (no-op should succeed without errors)"

echo ""
echo "── dbt run --full-refresh (drop + recreate) ──"
dbt run --full-refresh

echo ""
echo "── Waiting for stream tables to populate (after recreate) ──"
./scripts/wait_for_populated.sh order_totals 30
./scripts/wait_for_populated.sh order_extremes 30
./scripts/wait_for_populated.sh customer_stats 30
./scripts/wait_for_populated.sh order_totals_compat 30

echo ""
echo "── dbt test (after full-refresh) ──"
# assert_compat_alter_applied requires the post-alter state; after a
# full-refresh the schedule reverts to '1m', so exclude it here.
dbt test --exclude assert_compat_alter_applied

echo ""
echo "── dbt run-operation refresh_all_stream_tables ──"
dbt run-operation refresh_all_stream_tables

echo ""
echo "── dbt run-operation pgtrickle_refresh (single table) ──"
dbt run-operation pgtrickle_refresh --args '{model_name: order_totals}'

echo ""
echo "── dbt run-operation pgtrickle_check_freshness ──"
dbt run-operation pgtrickle_check_freshness

echo ""
echo "── dbt run-operation drop_all_stream_tables ──"
dbt run-operation drop_all_stream_tables

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  All dbt integration tests passed!"
echo "════════════════════════════════════════════════════════════════"
