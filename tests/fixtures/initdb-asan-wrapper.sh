#!/bin/sh
# PostgreSQL 18.6 setup utilities initdb, pg_ctl, and psql retain temporary
# process allocations at exit; pg_ctl also starts a disposable bootstrap server.
# Keep ASan access checks enabled and limit LeakSanitizer to the final PostgreSQL
# server and extension processes that execute the workloads.
export ASAN_OPTIONS=halt_on_error=1:abort_on_error=1:detect_leaks=0
helper="$(basename "$0").asan-helper"
exec "/usr/local/pgsql/bin/$helper" "$@"
