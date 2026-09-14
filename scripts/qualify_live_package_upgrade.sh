#!/usr/bin/env bash
# Install the published package, preserve pending committed changes, then
# restart with and update to the candidate package inside the same database.
set -euo pipefail

FROM_VERSION="${1:?previous release version required}"
TO_VERSION="${2:?candidate release version required}"
CANDIDATE_DIR="${PGT_EXTENSION_DIR:?candidate package directory is required}"
ARCHIVE_NAME="pg_trickle-${FROM_VERSION}-pg18-linux-amd64.tar.gz"
WORK_DIR="$(mktemp -d)"
CONTAINER_ID=""

cleanup() {
    if [[ -n "$CONTAINER_ID" ]]; then
        docker stop "$CONTAINER_ID" >/dev/null 2>&1 || true
        docker rm -f "$CONTAINER_ID" >/dev/null 2>&1 || true
    fi
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT INT TERM

if [[ -n "${PGS_PREVIOUS_RELEASE_ARCHIVE:-}" ]]; then
    cp "$PGS_PREVIOUS_RELEASE_ARCHIVE" "$WORK_DIR/$ARCHIVE_NAME"
else
    curl --fail --location --retry 3 \
        "https://github.com/trickle-labs/pg-trickle/releases/download/v${FROM_VERSION}/${ARCHIVE_NAME}" \
        --output "$WORK_DIR/$ARCHIVE_NAME"
fi
mkdir "$WORK_DIR/previous"
tar xzf "$WORK_DIR/$ARCHIVE_NAME" -C "$WORK_DIR/previous" --strip-components=1

CONTAINER_ID="$(docker create \
    -e POSTGRES_PASSWORD=postgres postgres:18.3 \
    -c shared_preload_libraries=pg_trickle \
    -c track_commit_timestamp=on \
    -c wal_level=logical \
    -c max_replication_slots=10 \
    -c max_worker_processes=32)"
docker cp "$WORK_DIR/previous/lib/." "$CONTAINER_ID:/usr/lib/postgresql/18/lib/"
docker cp "$WORK_DIR/previous/extension/." "$CONTAINER_ID:/usr/share/postgresql/18/extension/"
docker start "$CONTAINER_ID" >/dev/null
for attempt in $(seq 1 60); do
    if docker exec "$CONTAINER_ID" pg_isready -U postgres >/dev/null 2>&1; then
        break
    fi
    if [[ "$attempt" == 60 ]]; then
        echo "PostgreSQL did not become ready" >&2
        docker logs "$CONTAINER_ID"
        exit 1
    fi
    sleep 1
done

PG_VERSION="$(docker exec "$CONTAINER_ID" psql -U postgres -d postgres -Atc 'SHOW server_version')"
echo "PGT_ACTUAL_POSTGRESQL_VERSION=${PG_VERSION}"
psql() {
    docker exec "$CONTAINER_ID" psql -X -v ON_ERROR_STOP=1 -U postgres -d postgres -Atc "$1"
}

psql 'CREATE EXTENSION pg_trickle CASCADE' >/dev/null
OLD_VERSION="$(psql 'SELECT pgtrickle.version()')"
[[ "$OLD_VERSION" == "$FROM_VERSION" ]] || {
    echo "previous package reports $OLD_VERSION, expected $FROM_VERSION" >&2
    exit 1
}

psql "CREATE TABLE public.v106_upgrade_source (id integer PRIMARY KEY, value text NOT NULL); \
      INSERT INTO public.v106_upgrade_source VALUES (1, 'one'), (2, 'two'); \
      SELECT pgtrickle.create_stream_table( \
          name => 'v106_upgrade_st', \
          query => 'SELECT id, value FROM public.v106_upgrade_source', \
          schedule => '1h', refresh_mode => 'DIFFERENTIAL', \
          initialize => false, orchestration_mode => 'EXTERNAL');" >/dev/null

CONSUMER_ID="$(psql "SELECT consumer_id FROM pgtrickle.register_output_delta_consumer( \
    'public.v106_upgrade_st'::regclass, 'v106-upgrade-cursor', \
    (SELECT contract_digest FROM pgtrickle.stream_table_contract('public.v106_upgrade_st')), \
    'RESNAPSHOT_REQUIRED')")"

refresh_graph() {
    psql "SELECT * FROM pgtrickle.refresh_graph_strict( \
        ARRAY['public.v106_upgrade_st'::regclass], \
        (SELECT graph_digest FROM pgtrickle.graph_contract( \
            ARRAY['public.v106_upgrade_st'::regclass])), 'ALLOW')" >/dev/null
}

# Create and acknowledge one output batch so the upgrade checks a live cursor.
psql "SELECT pgtrickle.refresh_graph_strict( \
    ARRAY['public.v106_upgrade_st'::regclass], \
    (SELECT graph_digest FROM pgtrickle.graph_contract( \
        ARRAY['public.v106_upgrade_st'::regclass])), 'ALLOW')" >/dev/null
RESNAPSHOT_TOKEN="$(psql "SELECT resnapshot_token FROM \
    pgtrickle.begin_output_delta_resnapshot('${CONSUMER_ID}'::uuid)")"
psql "SELECT pgtrickle.ack_output_delta_resnapshot( \
    '${CONSUMER_ID}'::uuid, '${RESNAPSHOT_TOKEN}'::uuid);" >/dev/null
psql "UPDATE public.v106_upgrade_source SET value = 'updated' WHERE id = 1;" >/dev/null
refresh_graph
BATCH_TOKEN="$(psql "SELECT batch_token FROM pgtrickle.output_delta_batches( \
    '${CONSUMER_ID}'::uuid, NULL::bigint) ORDER BY batch_token DESC LIMIT 1")"
[[ -n "$BATCH_TOKEN" ]] || { echo "output consumer did not receive a batch" >&2; exit 1; }
psql "SELECT pgtrickle.ack_output_delta('${CONSUMER_ID}'::uuid, ${BATCH_TOKEN}, 'APPLIED');" >/dev/null

# Leave a committed CDC backlog across the binary replacement and restart.
psql "INSERT INTO public.v106_upgrade_source VALUES (3, 'pending'); \
      UPDATE public.v106_upgrade_source SET value = 'pending-update' WHERE id = 2; \
      DELETE FROM public.v106_upgrade_source WHERE id = 1;" >/dev/null
SOURCE_OID="$(psql "SELECT 'public.v106_upgrade_source'::regclass::oid")"
STABLE_NAME="$(psql "SELECT pgtrickle.source_stable_name(${SOURCE_OID}::oid)")"
[[ "$STABLE_NAME" =~ ^[A-Za-z0-9_]+$ ]] || { echo "invalid stable source name" >&2; exit 1; }
BUFFER="pgtrickle_changes.changes_${STABLE_NAME}"
PENDING_BEFORE="$(psql "SELECT count(*) FROM ${BUFFER}")"
[[ "$PENDING_BEFORE" -gt 0 ]] || { echo "upgrade setup did not retain pending changes" >&2; exit 1; }

PREFLIGHT="$(psql 'SELECT pgtrickle.preflight_upgrade()::text')"
echo "Upgrade preflight: ${PREFLIGHT}"
GRAPH_BEFORE="$(psql "SELECT encode(graph_digest, 'hex') FROM \
    pgtrickle.graph_contract(ARRAY['public.v106_upgrade_st'::regclass])")"
STREAM_STATE_BEFORE="$(psql "SELECT pgt_id || ':' || status || ':' || orchestration_mode \
    FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'v106_upgrade_st'")"
DEPENDENCIES_BEFORE="$(psql "SELECT count(*) FROM pgtrickle.pgt_dependencies \
    WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
                    WHERE pgt_name = 'v106_upgrade_st')")"
CURSOR_BEFORE="$(psql "SELECT state || ':' || acknowledged_batch_token FROM \
    pgtrickle.pgt_output_delta_consumers WHERE consumer_id = '${CONSUMER_ID}'::uuid")"
QUIESCED="$(psql 'SELECT pgtrickle.quiesce(30)')"
[[ "$QUIESCED" == "t" ]] || { echo "upgrade quiesce failed" >&2; exit 1; }

test -f "$CANDIDATE_DIR/usr/share/postgresql/18/extension/pg_trickle--${FROM_VERSION}--${TO_VERSION}.sql"
docker cp "$CANDIDATE_DIR/usr/lib/postgresql/18/lib/." "$CONTAINER_ID:/usr/lib/postgresql/18/lib/"
docker cp "$CANDIDATE_DIR/usr/share/postgresql/18/extension/." "$CONTAINER_ID:/usr/share/postgresql/18/extension/"
docker restart "$CONTAINER_ID" >/dev/null
for attempt in $(seq 1 60); do
    if docker exec "$CONTAINER_ID" pg_isready -U postgres >/dev/null 2>&1; then
        break
    fi
    if [[ "$attempt" == 60 ]]; then
        echo "PostgreSQL did not restart with the candidate package" >&2
        docker logs "$CONTAINER_ID"
        exit 1
    fi
    sleep 1
done

LOADED_VERSION="$(psql 'SELECT pgtrickle.version()')"
[[ "$LOADED_VERSION" == "$TO_VERSION" ]] || {
    echo "candidate binary reports $LOADED_VERSION, expected $TO_VERSION" >&2
    exit 1
}
psql "ALTER EXTENSION pg_trickle UPDATE TO '${TO_VERSION}'" >/dev/null
UPDATED_VERSION="$(psql "SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle'")"
[[ "$UPDATED_VERSION" == "$TO_VERSION" ]] || {
    echo "extension update reported $UPDATED_VERSION, expected $TO_VERSION" >&2
    exit 1
}
psql 'SELECT pgtrickle.resume_all()' >/dev/null
CAPTURE_STATE="$(psql 'SELECT state FROM pgtrickle.pgt_capture_instance WHERE singleton')"
[[ "$CAPTURE_STATE" == "ACTIVE" ]] || { echo "capture did not resume after update" >&2; exit 1; }

PENDING_AFTER="$(psql "SELECT count(*) FROM ${BUFFER}")"
GRAPH_AFTER="$(psql "SELECT encode(graph_digest, 'hex') FROM \
    pgtrickle.graph_contract(ARRAY['public.v106_upgrade_st'::regclass])")"
STREAM_STATE_AFTER="$(psql "SELECT pgt_id || ':' || status || ':' || orchestration_mode \
    FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'v106_upgrade_st'")"
DEPENDENCIES_AFTER="$(psql "SELECT count(*) FROM pgtrickle.pgt_dependencies \
    WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
                    WHERE pgt_name = 'v106_upgrade_st')")"
CURSOR_AFTER="$(psql "SELECT state || ':' || acknowledged_batch_token FROM \
    pgtrickle.pgt_output_delta_consumers WHERE consumer_id = '${CONSUMER_ID}'::uuid")"
[[ "$PENDING_AFTER" == "$PENDING_BEFORE" ]] || { echo "pending CDC rows changed across upgrade" >&2; exit 1; }
[[ "$GRAPH_AFTER" == "$GRAPH_BEFORE" ]] || { echo "graph binding changed across upgrade" >&2; exit 1; }
[[ "$STREAM_STATE_AFTER" == "$STREAM_STATE_BEFORE" ]] || { echo "stream publication state changed across upgrade" >&2; exit 1; }
[[ "$DEPENDENCIES_AFTER" == "$DEPENDENCIES_BEFORE" ]] || { echo "graph dependencies changed across upgrade" >&2; exit 1; }
[[ "$CURSOR_AFTER" == "$CURSOR_BEFORE" ]] || { echo "consumer cursor changed across upgrade" >&2; exit 1; }

refresh_graph
psql "SELECT NOT EXISTS ( \
    (SELECT id, value FROM public.v106_upgrade_st EXCEPT ALL \
     SELECT id, value FROM public.v106_upgrade_source) UNION ALL \
    (SELECT id, value FROM public.v106_upgrade_source EXCEPT ALL \
     SELECT id, value FROM public.v106_upgrade_st))" | grep -qx t
echo "Published v${FROM_VERSION} binary upgraded to v${TO_VERSION} with pending changes, graph bindings, publication state, and consumer cursor preserved."
