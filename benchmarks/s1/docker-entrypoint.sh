#!/usr/bin/env bash
set -Eeuo pipefail
source /usr/local/bin/docker-entrypoint.sh

# Keep schedulers out of Docker's temporary init server and its fast shutdown.
docker_temp_server_start() {
    if [ "$1" = 'postgres' ]; then
        shift
    fi
    set -- "$@" -c listen_addresses='' -c max_worker_processes=0 -p "${PGPORT:-5432}"
    PGUSER="${PGUSER:-$POSTGRES_USER}" pg_ctl -D "$PGDATA" -o "$(printf '%q ' "$@")" -w start
}

_main "$@"
