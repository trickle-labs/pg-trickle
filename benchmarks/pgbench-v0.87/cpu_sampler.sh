#!/bin/sh
# Sample PostgreSQL worker CPU while pgbench clients and refresh workers live.
set -eu

samples=$1
ready=$2
stop=$3
: > "$samples"

while :; do
    printf 'S\n' >> "$samples"
    for stat in /proc/[0-9]*/stat; do
        [ -r "$stat" ] || continue
        pid=${stat#/proc/}
        pid=${pid%/stat}
        cmdline=$(tr '\000' ' ' 2>/dev/null < "/proc/$pid/cmdline") || continue
        case "$cmdline" in
            *postgres:*pg_trickle*) ;;
            *) continue ;;
        esac
        IFS= read -r line 2>/dev/null < "$stat" || continue
        set -- ${line#*) }
        printf 'W %s %s %s\n' "$pid" "${20}" "$((${12} + ${13}))" >> "$samples"
    done
    : > "$ready"
    [ -e "$stop" ] && break
    # ponytail: 100ms polling can miss shorter workers; use kernel accounting if needed.
    sleep 0.1
done
