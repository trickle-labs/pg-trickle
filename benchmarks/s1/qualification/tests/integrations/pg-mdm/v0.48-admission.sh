#!/bin/sh
set -eu

test_db=${1:-pgreact_m2_missing_$$}
test_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
log=$(mktemp "${TMPDIR:-/tmp}/pgreact-m2-admission.XXXXXX")
created=0
helper_created=0

cleanup() {
    if [ "$created" -eq 1 ]; then dropdb --force "$test_db"; fi
    if [ "$helper_created" -eq 1 ]; then dropuser mdm_helper_owner; fi
    rm -f "$log"
}
trap cleanup 0 HUP INT TERM

createdb "$test_db"
created=1
if [ "$(psql --no-psqlrc --tuples-only --no-align --dbname=postgres \
    --command="SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'mdm_helper_owner'")" != '1' ]; then
    helper_created=1
fi
run_setup() {
    if ! psql --set=ON_ERROR_STOP=1 --dbname="$test_db" "$@" >"$log" 2>&1; then
        sed -n '1,$p' "$log" >&2
        exit 1
    fi
}
run_setup --command \
    "CREATE EXTENSION pg_trickle VERSION '0.108.3'; CREATE EXTENSION pg_react VERSION '0.46.1';"
run_setup --command \
    "DO \$\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'mdm_helper_owner') THEN CREATE ROLE mdm_helper_owner NOLOGIN; END IF; END \$\$;"
run_setup --file="$test_dir/../../../integrations/pg-mdm/sql/policy-inputs.sql"
run_setup --file="$test_dir/../../../integrations/pg-mdm/sql/request-store.sql"

if psql --set=ON_ERROR_STOP=1 --dbname="$test_db" \
    --file="$test_dir/../../../integrations/pg-mdm/sql/intent-worker.sql" >"$log" 2>&1; then
    echo 'adapter deployment unexpectedly accepted a missing MDM contract' >&2
    exit 1
fi
if ! grep -Fq 'ERROR:  schema "mdm_steward" does not exist' "$log"; then
    cat "$log" >&2
    exit 1
fi
actual=$(psql --no-psqlrc --tuples-only --no-align --field-separator='|' \
    --dbname="$test_db" --command \
    "SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'mdm_steward'), (SELECT count(*) FROM pgreact_mdm.intent_requests)")
if [ "$actual" != 't|0' ]; then
    printf 'missing-MDM postcondition mismatch: %s\n' "$actual" >&2
    exit 1
fi
echo 'v0.48 missing-MDM admission rejected before effects: PASS'
