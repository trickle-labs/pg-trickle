#!/usr/bin/env bash
# Reproducible v0.87 three-configuration pgbench gate.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
BENCH_DIR="$ROOT_DIR/benchmarks/pgbench-v0.87"
PROFILE="blocking"
OUTPUT_DIR="$ROOT_DIR/target/pgbench-v0.87"

usage() {
    cat <<'EOF'
Usage: benchmarks/pgbench-v0.87/run.sh [options]

Options:
  --profile blocking|publication  Select the versioned workload profile.
  --output DIR                    Store raw logs and result JSON in DIR.
  --repetitions N                 Override the profile repetition count.
EOF
}

while (($#)); do
    case "$1" in
        --profile)
            PROFILE="$2"
            shift 2
            ;;
        --output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --repetitions)
            PGT_PGBENCH_REPETITIONS="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

case "$PROFILE" in
    blocking)
        scale="${PGT_PGBENCH_SCALE:-10}"
        clients="${PGT_PGBENCH_CLIENTS:-8}"
        jobs="${PGT_PGBENCH_JOBS:-2}"
        warmup="${PGT_PGBENCH_WARMUP:-10}"
        duration="${PGT_PGBENCH_DURATION:-30}"
        repetitions="${PGT_PGBENCH_REPETITIONS:-3}"
        ;;
    publication)
        scale="${PGT_PGBENCH_SCALE:-10}"
        clients="${PGT_PGBENCH_CLIENTS:-16}"
        jobs="${PGT_PGBENCH_JOBS:-4}"
        warmup="${PGT_PGBENCH_WARMUP:-30}"
        duration="${PGT_PGBENCH_DURATION:-60}"
        repetitions="${PGT_PGBENCH_REPETITIONS:-5}"
        ;;
    *)
        echo "unsupported profile: $PROFILE" >&2
        exit 2
        ;;
esac

absent_image="${PGT_PGBENCH_ABSENT_IMAGE:-postgres:18.3}"
installed_image="${PGT_PGBENCH_INSTALLED_IMAGE:-pg_trickle_e2e:latest}"
seed="${PGT_PGBENCH_SEED:-8700}"
postgres_version="${PGT_PGBENCH_POSTGRES_VERSION:-18.3}"
mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"
run_root="$OUTPUT_DIR/raw"
raw_file="$OUTPUT_DIR/raw.jsonl"
result_file="$OUTPUT_DIR/result.json"

command -v docker >/dev/null || { echo "docker is required" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 1; }
docker info >/dev/null 2>&1 || {
    echo "Docker daemon is unavailable" >&2
    exit 1
}

if ! docker image inspect "$installed_image" >/dev/null 2>&1; then
    echo "Missing $installed_image; build it with ./tests/build_e2e_image.sh" >&2
    exit 1
fi
if ! docker image inspect "$absent_image" >/dev/null 2>&1; then
    docker pull "$absent_image"
fi
runtime_arch="$(docker info --format '{{.Architecture}}' | sed 's/^aarch64$/arm64/; s/^x86_64$/amd64/')"
for image in "$absent_image" "$installed_image"; do
    image_arch="$(docker image inspect "$image" --format '{{.Architecture}}')"
    if [[ "$image_arch" != "$runtime_arch" ]]; then
        echo "image architecture mismatch: $image is $image_arch, Docker runtime is $runtime_arch" >&2
        exit 1
    fi
done

mkdir -p "$run_root"
rm -f "$raw_file" "$result_file"
container_names=()
cleanup() {
    local status=$?
    set +e
    for container in "${container_names[@]:-}"; do
        docker rm -f "$container" >/dev/null 2>&1 || true
    done
    exit "$status"
}
trap cleanup EXIT INT TERM

db_query() {
    docker exec "$container" psql -X -v ON_ERROR_STOP=1 -U postgres -d postgres -Atqc "$1"
}

cpu_sample() {
    docker exec "$container" sh -ceu '
        total=0
        worker=0
        found=0
        for stat in /proc/[0-9]*/stat; do
            [ -r "$stat" ] || continue
            ticks=$(awk "{print \$14 + \$15}" "$stat")
            case "$ticks" in
                ""|*[!0-9]*) continue ;;
            esac
            total=$((total + ticks))
            pid=${stat#/proc/}
            pid=${pid%/stat}
            comm=$(awk "{print \$2}" "$stat" 2>/dev/null || true)
            cmdline=$(tr "\000" " " < "/proc/$pid/cmdline" 2>/dev/null || true)
            if printf "%s %s" "$comm" "$cmdline" | grep -Eiq "pg_trickle|scheduler|refresh"; then
                worker=$((worker + ticks))
                found=1
            fi
        done
        if [ "$found" -eq 0 ]; then
            echo unsupported
        else
            echo "$worker,$total"
        fi
    '
}

wait_for_postgres() {
    for _ in $(seq 1 60); do
        if docker exec "$container" pg_isready -U postgres -d postgres >/dev/null 2>&1; then
            return
        fi
        sleep 1
    done
    echo "PostgreSQL did not become ready: $container" >&2
    exit 1
}

wait_for_scheduler() {
    local attempt
    for attempt in $(seq 0 450); do
        if [[ "$(db_query "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE backend_type = 'pg_trickle scheduler' AND datname = current_database())")" == t ]]; then
            return
        fi
        if (( attempt % 50 == 0 )); then
            db_query "SELECT pgtrickle._signal_launcher_rescan();" >/dev/null 2>&1 || true
            db_query "SELECT pg_reload_conf();" >/dev/null 2>&1 || true
        fi
        sleep 0.2
    done
    echo "pg_trickle scheduler did not start: $container" >&2
    exit 1
}

setup_active_streams() {
    db_query "ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = '1000';" >/dev/null
    db_query "SELECT pg_reload_conf();" >/dev/null
    db_query "SELECT pgtrickle.create_stream_table('bench_projection', \$\$SELECT aid, bid, abalance FROM pgbench_accounts\$\$, '1s', 'DIFFERENTIAL');" >/dev/null
    db_query "SELECT pgtrickle.create_stream_table('bench_balances', \$\$SELECT bid, sum(abalance) AS total_balance FROM pgbench_accounts GROUP BY bid\$\$, '1s', 'DIFFERENTIAL');" >/dev/null
    db_query "SELECT pgtrickle.create_stream_table('bench_join', \$\$SELECT a.aid, a.bid, a.abalance, b.bbalance FROM pgbench_accounts a JOIN pgbench_branches b ON b.bid = a.bid\$\$, '1s', 'DIFFERENTIAL');" >/dev/null
}

correctness_check() {
    db_query "SELECT (SELECT count(*) FROM pgtrickle.pgt_stream_tables WHERE status <> 'ACTIVE') = 0 AND NOT EXISTS ((SELECT aid, bid, abalance FROM pgbench_accounts EXCEPT ALL SELECT aid, bid, abalance FROM bench_projection) UNION ALL (SELECT aid, bid, abalance FROM bench_projection EXCEPT ALL SELECT aid, bid, abalance FROM pgbench_accounts)) AND NOT EXISTS ((SELECT bid, sum(abalance) FROM pgbench_accounts GROUP BY bid EXCEPT ALL SELECT bid, total_balance FROM bench_balances) UNION ALL (SELECT bid, total_balance FROM bench_balances EXCEPT ALL SELECT bid, sum(abalance) FROM pgbench_accounts GROUP BY bid)) AND NOT EXISTS ((SELECT a.aid, a.bid, a.abalance, b.bbalance FROM pgbench_accounts a JOIN pgbench_branches b ON b.bid = a.bid EXCEPT ALL SELECT aid, bid, abalance, bbalance FROM bench_join) UNION ALL (SELECT aid, bid, abalance, bbalance FROM bench_join EXCEPT ALL SELECT a.aid, a.bid, a.abalance, b.bbalance FROM pgbench_accounts a JOIN pgbench_branches b ON b.bid = a.bid))"
}

wait_for_correctness() {
    for _ in $(seq 1 120); do
        if [[ "$(correctness_check)" == t ]]; then
            return 0
        fi
        sleep 0.5
    done
    return 1
}

run_one() {
    local config="$1"
    local repetition="$2"
    local image="$3"
    local run_dir="$run_root/${repetition}-${config}"
    local stdout_file="$run_dir/pgbench.stdout"
    local log_dir="$run_dir/logs"
    local warmup_file="$run_dir/warmup.stdout"
    local history_before refresh_stats refresh_count refresh_duration correct
    local cpu_before cpu_after

    mkdir -p "$log_dir"
    chmod 777 "$run_dir" "$log_dir"
    container="pgtrickle-pgbench-${repetition}-${config}-$$"
    container_names+=("$container")
    docker run --rm -d --name "$container" -v "$run_dir:/bench" \
        -e POSTGRES_PASSWORD=postgres "$image" >/dev/null
    wait_for_postgres

    if [[ "$config" != absent ]]; then
        db_query "CREATE EXTENSION IF NOT EXISTS pg_trickle;" >/dev/null
    fi
    docker exec "$container" sh -ceu 'command -v pgbench >/dev/null'
    docker exec "$container" pgbench -U postgres -i -s "$scale" postgres >"$run_dir/init.stdout"

    if [[ "$config" == active ]]; then
        setup_active_streams
        wait_for_scheduler
    fi

    docker exec "$container" pgbench -U postgres -c "$clients" -j "$jobs" -T "$warmup" -n postgres >"$warmup_file"
    if [[ "$config" == active ]]; then
        history_before="$(db_query "SELECT coalesce(max(refresh_id), 0) FROM pgtrickle.pgt_refresh_history")"
    else
        history_before=0
    fi
    cpu_before="$(cpu_sample)"
    docker exec "$container" pgbench -U postgres -c "$clients" -j "$jobs" -T "$duration" -n \
        -l --sampling-rate=0.1 --log-prefix=/bench/logs/pgbench postgres >"$stdout_file"
    cpu_after="$(cpu_sample)"

    if [[ "$config" == active ]]; then
        if wait_for_correctness; then
            correct=true
        else
            correct=false
        fi
        IFS='|' read -r refresh_count refresh_duration <<<"$(db_query "SELECT count(*)::bigint, coalesce(sum(extract(epoch FROM end_time - start_time) * 1000), 0)::double precision FROM pgtrickle.pgt_refresh_history WHERE refresh_id > $history_before AND status = 'COMPLETED'")"
    else
        refresh_count=0
        refresh_duration=0
        correct=true
    fi

    python3 "$BENCH_DIR/parse_pgbench.py" \
        --config "$config" \
        --repetition "$repetition" \
        --stdout "$stdout_file" \
        --log-dir "$log_dir" \
        --cpu-before "$cpu_before" \
        --cpu-after "$cpu_after" \
        --refresh-count "$refresh_count" \
        --refresh-duration-ms "$refresh_duration" \
        --correct "$correct" \
        --commit "$(git -C "$ROOT_DIR" rev-parse HEAD)" \
        --postgres-version "$postgres_version" \
        --image "$image" >>"$raw_file"

    docker rm -f "$container" >/dev/null
    container_names=()
}

configs=(absent installed active)
for ((repetition = 0; repetition < repetitions; repetition++)); do
    first=$(((repetition + seed) % 3))
    for ((offset = 0; offset < 3; offset++)); do
        config="${configs[$(((first + offset) % 3))]}"
        if [[ "$config" == absent ]]; then
            image="$absent_image"
        else
            image="$installed_image"
        fi
        echo "[pgbench-v0.87] repetition=$repetition config=$config"
        run_one "$config" "$repetition" "$image"
    done
done

python3 "$BENCH_DIR/compare.py" \
    --raw "$raw_file" \
    --budgets "$BENCH_DIR/budgets.json" \
    --output "$result_file" \
    --expected-repetitions "$repetitions"
