#!/usr/bin/env bash
# Install the published package, preserve pending committed changes, then
# restart with and update to the candidate package inside the same database.
set -euo pipefail

FROM_VERSION="${1:?previous release version required}"
TO_VERSION="${2:?candidate release version required}"
IDENTITY_MIGRATION=false
VECTOR_UPGRADE=false
POSTGRES_IMAGE=postgres:18.6
if [[ "$FROM_VERSION" == "0.108.3" && "$TO_VERSION" == "0.109.0" ]]; then
    VECTOR_UPGRADE=true
    POSTGRES_IMAGE='pgvector/pgvector:0.8.7-pg18@sha256:c29f30688a9ad961691bdf0ec1650717cc132a5814619d80f80463d6e4c42e47'
fi
if [[ "$FROM_VERSION" == "0.106.1" && ( "$TO_VERSION" == "0.108.3" || "$TO_VERSION" == "0.109.0" ) ]]; then
    IDENTITY_MIGRATION=true
fi
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
echo "Published package SHA-256: $(shasum -a 256 "$WORK_DIR/$ARCHIVE_NAME" | cut -d' ' -f1)"

CONTAINER_ID="$(docker create \
    -e POSTGRES_PASSWORD=postgres "$POSTGRES_IMAGE" \
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

if [[ "$VECTOR_UPGRADE" == true ]]; then
    psql "CREATE EXTENSION vector; CREATE TABLE public.upgrade_vector_source(id int PRIMARY KEY, embedding vector(3));
        INSERT INTO public.upgrade_vector_source VALUES (1, '[1,0,0]'), (2, '[0,1,0]');
        SELECT * FROM pgtrickle.embedding_stream_table('upgrade_vector_st', 'public.upgrade_vector_source', 'embedding', NULL, '24h', 'hnsw', false);
        INSERT INTO pgtrickle.pgt_distance_subscriptions(stream_table,channel,vector_column,query_vector,op,threshold)
        VALUES ('public.upgrade_vector_st','upgrade_vector_channel','embedding','[1,0,0]','<->',0.5)" >/dev/null
    [[ "$(psql "SELECT extversion FROM pg_extension WHERE extname='vector'")" == '0.8.7' ]]
    VECTOR_BEFORE="$(psql "SELECT s.pgt_id::text || '|' || s.pgt_relid::text || '|' || s.defining_query || '|' ||
        i.indexrelid::text || '|' || pg_relation_filenode(i.indexrelid)::text || '|' || pg_get_indexdef(i.indexrelid)
        FROM pgtrickle.pgt_stream_tables s JOIN pg_index i ON i.indrelid=s.pgt_relid
        JOIN pg_opclass op ON op.oid=i.indclass[0]
        WHERE s.pgt_name='upgrade_vector_st' AND op.opcname='vector_l2_ops' AND i.indisvalid AND i.indisready")"
    [[ -n "$VECTOR_BEFORE" ]] || { echo "published helper did not create a valid L2 index" >&2; exit 1; }
    echo "Published vector baseline: pgvector=0.8.7 image=$POSTGRES_IMAGE index=$VECTOR_BEFORE"
fi

WAL_SLOT=""
if [[ "$FROM_VERSION" == "0.108.3" && "$TO_VERSION" == "0.109.0" ]]; then
    psql "ALTER SYSTEM SET pg_trickle.scheduler_interval_ms='1000'" >/dev/null
    psql 'SELECT pg_reload_conf()' >/dev/null
    psql "CREATE TABLE public.retirement_upgrade_source(id int PRIMARY KEY,value text);
        ALTER TABLE public.retirement_upgrade_source REPLICA IDENTITY FULL;
        INSERT INTO public.retirement_upgrade_source VALUES(1,'published baseline');
        SELECT pgtrickle.create_stream_table(name=>'retirement_upgrade_st',
            query=>'SELECT id,value FROM public.retirement_upgrade_source',schedule=>'24h',
            refresh_mode=>'DIFFERENTIAL',cdc_mode=>'wal')" >/dev/null
    for attempt in $(seq 1 90); do
        WAL_SLOT="$(psql "SELECT d.slot_name FROM pgtrickle.pgt_dependencies d
            JOIN pg_replication_slots s ON s.slot_name=d.slot_name
            WHERE d.source_relid='public.retirement_upgrade_source'::regclass
              AND d.cdc_mode IN ('TRANSITIONING','WAL')
              AND s.plugin='test_decoding' AND s.database=current_database()")"
        [[ -n "$WAL_SLOT" ]] && break
        sleep 0.05
    done
    [[ -n "$WAL_SLOT" ]] || { echo "published package did not establish actual WAL capture" >&2; exit 1; }
    psql "ALTER SYSTEM SET pg_trickle.enabled='false'" >/dev/null
    psql 'SELECT pg_reload_conf()' >/dev/null
    WAL_CDC_MODE="$(psql "SELECT cdc_mode FROM pgtrickle.pgt_dependencies WHERE slot_name='$WAL_SLOT'")"
    psql "SELECT to_regclass('pgtrickle.pgt_wal_slot_retirements') IS NULL" | grep -qx t
    WAL_BASELINE="$(psql "SELECT row_to_json(s)::text FROM (SELECT frontier,is_populated,needs_reinit
        FROM pgtrickle.pgt_stream_tables WHERE pgt_name='retirement_upgrade_st') s")"
    echo "Published WAL slot established before upgrade: slot=$WAL_SLOT cdc_mode=$WAL_CDC_MODE state=$WAL_BASELINE"
fi

psql "CREATE TABLE public.v106_upgrade_source (id integer PRIMARY KEY, value text NOT NULL); \
      INSERT INTO public.v106_upgrade_source VALUES (1, 'one'), (2, 'two'); \
      SELECT pgtrickle.create_stream_table( \
          name => 'v106_upgrade_st', \
          query => 'SELECT id, value FROM public.v106_upgrade_source', \
          schedule => '1h', refresh_mode => 'DIFFERENTIAL', \
          initialize => false, orchestration_mode => 'EXTERNAL');" >/dev/null

CONSUMER_REGISTRATION="$(psql "SELECT consumer_id::text || '|' || delta_relation FROM \
    pgtrickle.register_output_delta_consumer( \
    'public.v106_upgrade_st'::regclass, 'v106-upgrade-cursor', \
    (SELECT contract_digest FROM pgtrickle.stream_table_contract('public.v106_upgrade_st')), \
    'RESNAPSHOT_REQUIRED')")"
IFS='|' read -r CONSUMER_ID DELTA_RELATION <<< "$CONSUMER_REGISTRATION"

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

# Leave the batch and typed payload pending so the migration proves that both
# survive alongside the consumer cursor. Also leave a second consumer's
# resnapshot token outstanding to exercise the new fence-column backfill.
[[ "$DELTA_RELATION" =~ ^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$ ]] || {
    echo "invalid typed delta relation: $DELTA_RELATION" >&2
    exit 1
}
FENCED_CONSUMER_ID="$(psql "SELECT consumer_id FROM pgtrickle.register_output_delta_consumer( \
    'public.v106_upgrade_st'::regclass, 'v106-upgrade-fenced', \
    (SELECT contract_digest FROM pgtrickle.stream_table_contract('public.v106_upgrade_st')), \
    'RESNAPSHOT_REQUIRED')")"
FENCED_TOKEN="$(psql "SELECT resnapshot_token FROM \
    pgtrickle.begin_output_delta_resnapshot('${FENCED_CONSUMER_ID}'::uuid)")"

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
BATCHES_BEFORE="$(psql "SELECT md5(COALESCE(string_agg(row_to_json(b)::text, '|' \
    ORDER BY batch_token), '')) FROM pgtrickle.pgt_output_delta_batches b \
    WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_output_delta_consumers \
                    WHERE consumer_id = '${CONSUMER_ID}'::uuid)")"
PAYLOAD_BEFORE="$(psql "SELECT md5(COALESCE(string_agg(row_to_json(p)::text, '|' \
    ORDER BY batch_token, ordinal), '')) FROM ${DELTA_RELATION} p")"
QUIESCED="$(psql 'SELECT pgtrickle.quiesce(30)')"
[[ "$QUIESCED" == "t" ]] || { echo "upgrade quiesce failed" >&2; exit 1; }

UPGRADE_DIR="$CANDIDATE_DIR/usr/share/postgresql/18/extension"
if [[ ! -f "$UPGRADE_DIR/pg_trickle--${FROM_VERSION}--${TO_VERSION}.sql" ]]; then
    [[ "$FROM_VERSION" == "0.106.1" && ( "$TO_VERSION" == "0.108.1" || "$IDENTITY_MIGRATION" == true ) ]]
    test -f "$UPGRADE_DIR/pg_trickle--0.106.1--0.107.0.sql"
    test -f "$UPGRADE_DIR/pg_trickle--0.107.0--0.108.0.sql"
    test -f "$UPGRADE_DIR/pg_trickle--0.108.0--0.108.1.sql"
    if [[ "$IDENTITY_MIGRATION" == true ]]; then
        test -f "$UPGRADE_DIR/pg_trickle--0.108.1--0.108.3.sql"
    fi
    if [[ "$TO_VERSION" == "0.109.0" ]]; then
        test -f "$UPGRADE_DIR/pg_trickle--0.108.3--0.109.0.sql"
    fi
fi
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
if [[ -n "${PGT_RELEASE_ATTESTATION_PATH:-}" ]]; then
    python3 scripts/release_install_attestation.py \
        --container "$CONTAINER_ID" \
        --candidate-root "$CANDIDATE_DIR" \
        --output "$PGT_RELEASE_ATTESTATION_PATH"
fi
if [[ "$IDENTITY_MIGRATION" == true ]]; then
    psql "ALTER EXTENSION pg_trickle UPDATE TO '0.108.1'" >/dev/null
    if psql "ALTER EXTENSION pg_trickle UPDATE TO '${TO_VERSION}'" >"$WORK_DIR/blocked-upgrade.log" 2>&1; then
        echo "identity migration accepted an active consumer" >&2
        exit 1
    fi
    if ! grep -q 'PGT_EXT_CONSUMER_BLOCKED' "$WORK_DIR/blocked-upgrade.log"; then
        cat "$WORK_DIR/blocked-upgrade.log" >&2
        echo "identity migration failed without the expected active-consumer block" >&2
        exit 1
    fi
    [[ "$(psql "SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle'")" == "0.108.1" ]]
    psql "SELECT state FROM pgtrickle.request_output_delta_resnapshot('${CONSUMER_ID}'::uuid)" | grep -qx RESNAPSHOT_REQUIRED
    psql "SELECT state FROM pgtrickle.request_output_delta_resnapshot('${FENCED_CONSUMER_ID}'::uuid)" | grep -qx INVALIDATED
    CURSOR_BEFORE="$(psql "SELECT state || ':' || acknowledged_batch_token FROM \
        pgtrickle.pgt_output_delta_consumers WHERE consumer_id = '${CONSUMER_ID}'::uuid")"
fi
psql "ALTER EXTENSION pg_trickle UPDATE TO '${TO_VERSION}'" >/dev/null
UPDATED_VERSION="$(psql "SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle'")"
[[ "$UPDATED_VERSION" == "$TO_VERSION" ]] || {
    echo "extension update reported $UPDATED_VERSION, expected $TO_VERSION" >&2
    exit 1
}
if [[ "$VECTOR_UPGRADE" == true ]]; then
    VECTOR_AFTER="$(psql "SELECT s.pgt_id::text || '|' || s.pgt_relid::text || '|' || s.defining_query || '|' ||
        i.indexrelid::text || '|' || pg_relation_filenode(i.indexrelid)::text || '|' || pg_get_indexdef(i.indexrelid)
        FROM pgtrickle.pgt_stream_tables s JOIN pg_index i ON i.indrelid=s.pgt_relid
        JOIN pg_opclass op ON op.oid=i.indclass[0]
        WHERE s.pgt_name='upgrade_vector_st' AND op.opcname='vector_l2_ops' AND i.indisvalid AND i.indisready")"
    [[ "$VECTOR_AFTER" == "$VECTOR_BEFORE" ]] || { echo "vector stream or L2 index changed across upgrade" >&2; exit 1; }
    psql "SELECT count(*)=1 AND bool_and(pgt_id IS NULL) FROM pgtrickle.pgt_distance_subscriptions
        WHERE stream_table='public.upgrade_vector_st' AND channel='upgrade_vector_channel' AND threshold=0.5" | grep -qx t
    [[ "$(psql 'SHOW pg_trickle.enable_centroid_state')" == off ]]
    psql "SELECT NOT EXISTS (SELECT 1 FROM pg_class c, LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
        WHERE c.oid IN ('pgtrickle.pgt_ann_maintenance_requests'::regclass, 'pgtrickle.pgt_centroid_states'::regclass) AND a.grantee=0)
        AND NOT EXISTS (SELECT 1 FROM pg_proc p, LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
        WHERE p.oid='pgtrickle.claim_ann_maintenance(bigint)'::regprocedure AND a.grantee=0 AND a.privilege_type='EXECUTE')" | grep -qx t
    psql "SELECT NOT EXISTS ((SELECT id,embedding FROM public.upgrade_vector_st EXCEPT ALL SELECT id,embedding FROM public.upgrade_vector_source)
        UNION ALL (SELECT id,embedding FROM public.upgrade_vector_source EXCEPT ALL SELECT id,embedding FROM public.upgrade_vector_st))" | grep -qx t
    psql "SET enable_seqscan=off; EXPLAIN SELECT id FROM public.upgrade_vector_st ORDER BY (embedding::vector(3)) <-> '[1,0,0]'::vector LIMIT 1" | grep -q 'upgrade_vector_st_embedding_idx'
    echo "Published vector stream, L2 index, inactive legacy subscription and default-off centroid state preserved."
fi
psql 'SELECT pgtrickle.resume_all()' >/dev/null
if [[ "$VECTOR_UPGRADE" == true ]]; then
    psql "UPDATE public.upgrade_vector_source SET embedding='[0,0,1]' WHERE id=1;
        INSERT INTO public.upgrade_vector_source VALUES (3, '[1,1,0]');
        DELETE FROM public.upgrade_vector_source WHERE id=2" >/dev/null
    psql "SET pg_trickle.refresh_strategy='differential';
        SET pg_trickle.differential_max_change_ratio=0;
        SELECT pgtrickle.refresh_stream_table('public.upgrade_vector_st')" >/dev/null
    psql "SELECT NOT EXISTS ((SELECT id,embedding FROM public.upgrade_vector_st EXCEPT ALL SELECT id,embedding FROM public.upgrade_vector_source)
        UNION ALL (SELECT id,embedding FROM public.upgrade_vector_source EXCEPT ALL SELECT id,embedding FROM public.upgrade_vector_st))" | grep -qx t
    psql "SELECT action='DIFFERENTIAL' AND status='COMPLETED' AND NOT was_full_fallback
        FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id)
        WHERE s.pgt_name='upgrade_vector_st' ORDER BY h.refresh_id DESC LIMIT 1" | grep -qx t
fi
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
BATCHES_AFTER="$(psql "SELECT md5(COALESCE(string_agg(row_to_json(b)::text, '|' \
    ORDER BY batch_token), '')) FROM pgtrickle.pgt_output_delta_batches b \
    WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_output_delta_consumers \
                    WHERE consumer_id = '${CONSUMER_ID}'::uuid)")"
PAYLOAD_AFTER="$(psql "SELECT md5(COALESCE(string_agg(row_to_json(p)::text, '|' \
    ORDER BY batch_token, ordinal), '')) FROM ${DELTA_RELATION} p")"
[[ "$PENDING_AFTER" == "$PENDING_BEFORE" ]] || { echo "pending CDC rows changed across upgrade" >&2; exit 1; }
if [[ "$IDENTITY_MIGRATION" == true ]]; then
    [[ "$GRAPH_AFTER" != "$GRAPH_BEFORE" ]] || { echo "identity migration did not invalidate graph binding" >&2; exit 1; }
    psql "SELECT row_identity_version IS NULL AND needs_reinit FROM pgtrickle.pgt_stream_tables \
        WHERE pgt_name = 'v106_upgrade_st'" | grep -qx t
else
    [[ "$GRAPH_AFTER" == "$GRAPH_BEFORE" ]] || { echo "graph binding changed across upgrade" >&2; exit 1; }
fi
[[ "$STREAM_STATE_AFTER" == "$STREAM_STATE_BEFORE" ]] || { echo "stream publication state changed across upgrade" >&2; exit 1; }
[[ "$DEPENDENCIES_AFTER" == "$DEPENDENCIES_BEFORE" ]] || { echo "graph dependencies changed across upgrade" >&2; exit 1; }
[[ "$CURSOR_AFTER" == "$CURSOR_BEFORE" ]] || { echo "consumer cursor changed across upgrade" >&2; exit 1; }
[[ "$BATCHES_AFTER" == "$BATCHES_BEFORE" ]] || { echo "output batches changed across upgrade" >&2; exit 1; }
[[ "$PAYLOAD_AFTER" == "$PAYLOAD_BEFORE" ]] || { echo "typed output payload changed across upgrade" >&2; exit 1; }
if [[ "$IDENTITY_MIGRATION" != true ]]; then
    psql "SELECT pgtrickle.ack_output_delta_resnapshot( \
        '${FENCED_CONSUMER_ID}'::uuid, '${FENCED_TOKEN}'::uuid);" | grep -qx ACTIVE
fi

refresh_graph
if [[ "$IDENTITY_MIGRATION" == true ]]; then
    psql "SELECT row_identity_version = 3 AND NOT needs_reinit FROM pgtrickle.pgt_stream_tables \
        WHERE pgt_name = 'v106_upgrade_st'" | grep -qx t
    if psql "SELECT pgtrickle.ack_output_delta_resnapshot('${FENCED_CONSUMER_ID}'::uuid, '${FENCED_TOKEN}'::uuid)" >"$WORK_DIR/stale-token.log" 2>&1; then
        echo "identity migration accepted a pre-upgrade resnapshot token" >&2
        exit 1
    fi
    grep -q 'PGT_EXT_TOKEN_INVALID' "$WORK_DIR/stale-token.log"
    for consumer in "$CONSUMER_ID" "$FENCED_CONSUMER_ID"; do
        token="$(psql "SELECT resnapshot_token FROM pgtrickle.begin_output_delta_resnapshot('${consumer}'::uuid)")"
        psql "SELECT pgtrickle.ack_output_delta_resnapshot('${consumer}'::uuid, '${token}'::uuid)" | grep -qx ACTIVE
    done
fi
psql "SELECT NOT EXISTS ( \
    (SELECT id, value FROM public.v106_upgrade_st EXCEPT ALL \
     SELECT id, value FROM public.v106_upgrade_source) UNION ALL \
    (SELECT id, value FROM public.v106_upgrade_source EXCEPT ALL \
     SELECT id, value FROM public.v106_upgrade_st))" | grep -qx t
if [[ -n "$WAL_SLOT" ]]; then
    [[ "$(psql "SELECT row_to_json(s)::text FROM (SELECT frontier,is_populated,needs_reinit
        FROM pgtrickle.pgt_stream_tables WHERE pgt_name='retirement_upgrade_st') s")" == "$WAL_BASELINE" ]]
    psql "SELECT NOT EXISTS((SELECT id,value FROM retirement_upgrade_st EXCEPT ALL SELECT id,value FROM retirement_upgrade_source)
        UNION ALL (SELECT id,value FROM retirement_upgrade_source EXCEPT ALL SELECT id,value FROM retirement_upgrade_st))" | grep -qx t
    psql "SELECT c.relpersistence='p' AND NOT EXISTS(SELECT 1 FROM aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a WHERE grantee=0)
        FROM pg_class c WHERE c.oid='pgtrickle.pgt_wal_slot_retirements'::regclass" | grep -qx t
    psql "SELECT 'WHERE false'=e.extcondition[array_position(e.extconfig,'pgtrickle.pgt_wal_slot_retirements'::regclass::oid)]
        FROM pg_extension e WHERE e.extname='pg_trickle'" | grep -qx t
    psql "BEGIN; SELECT pgtrickle.drop_stream_table('retirement_upgrade_st'); ROLLBACK" >/dev/null
    psql "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name='$WAL_SLOT')
        AND NOT EXISTS(SELECT 1 FROM pgtrickle.pgt_wal_slot_retirements)" | grep -qx t
    psql "SELECT pgtrickle.drop_stream_table('retirement_upgrade_st')" >/dev/null
    psql "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name='$WAL_SLOT')
        AND EXISTS(SELECT 1 FROM pgtrickle.pgt_wal_slot_retirements WHERE slot_name='$WAL_SLOT')" | grep -qx t
    psql "ALTER SYSTEM SET pg_trickle.enabled='true'" >/dev/null
    psql 'SELECT pg_reload_conf()' >/dev/null
    for attempt in $(seq 1 90); do
        retired="$(psql "SELECT NOT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name='$WAL_SLOT')
            AND NOT EXISTS(SELECT 1 FROM pgtrickle.pgt_wal_slot_retirements WHERE slot_name='$WAL_SLOT')")"
        [[ "$retired" == t ]] && break
        sleep 1
    done
    [[ "$retired" == t ]] || { echo "upgraded scheduler did not retire committed slot" >&2; exit 1; }
    echo "Published WAL state survived upgrade; durable private retirement catalog, rollback and committed drain passed."
fi
echo "Published v${FROM_VERSION} binary upgraded to v${TO_VERSION} with pending changes and durable consumer data preserved."
