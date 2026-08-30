-- pg_trickle 0.87.16 -> 0.87.17
--
-- This migration installs the read-only V2 recreation preflight and the
-- explicit external-consumer inventory. Existing V1 stream-table state is
-- intentionally left untouched; the preflight reports it as a blocker.

-- These private catalogs make the destructive window explicit and auditable.
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_row_identity_v2_inventory (
    inventory_id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (inventory_id),
    recorded_by TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    notes TEXT,
    acknowledged BOOLEAN NOT NULL DEFAULT FALSE,
    acknowledged_by TEXT,
    acknowledged_at TIMESTAMPTZ,
    CHECK (NOT acknowledged OR acknowledged_at IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS pgtrickle.pgt_row_identity_v2_consumers (
    consumer_id BIGSERIAL PRIMARY KEY,
    consumer_name TEXT NOT NULL,
    consumer_owner TEXT NOT NULL,
    affected_stream_tables TEXT[] NOT NULL
        CHECK (cardinality(affected_stream_tables) > 0),
    consumes_row_id BOOLEAN NOT NULL DEFAULT FALSE,
    consumes_storage_layout BOOLEAN NOT NULL DEFAULT FALSE,
    required_schema_change TEXT NOT NULL,
    resnapshot_status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (resnapshot_status IN ('PENDING', 'IN_PROGRESS', 'COMPLETE', 'SKIPPED')),
    acknowledged BOOLEAN NOT NULL DEFAULT FALSE,
    acknowledged_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (NOT acknowledged OR acknowledged_at IS NOT NULL)
);

SELECT pg_catalog.pg_extension_config_dump(
    'pgtrickle.pgt_row_identity_v2_inventory', ''
);
SELECT pg_catalog.pg_extension_config_dump(
    'pgtrickle.pgt_row_identity_v2_consumers', ''
);

CREATE OR REPLACE FUNCTION pgtrickle._row_identity_v2_admin()
RETURNS BOOLEAN
LANGUAGE sql
STABLE
SET search_path TO pgtrickle, pg_catalog, pg_temp
AS $row_identity_v2_admin$
    SELECT EXISTS (
        SELECT 1
        FROM pg_roles r
        JOIN pg_extension e ON e.extname = 'pg_trickle'
        WHERE r.rolname = current_user
          AND (r.rolsuper OR r.oid = e.extowner)
    )
$row_identity_v2_admin$;

CREATE OR REPLACE FUNCTION pgtrickle.row_identity_v2_record_inventory(
    p_notes TEXT DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql
SET search_path TO pgtrickle, pg_catalog, pg_temp
AS $row_identity_v2_record_inventory$
BEGIN
    IF NOT pgtrickle._row_identity_v2_admin() THEN
        RAISE EXCEPTION 'pg_trickle: row-identity inventory requires the extension owner or superuser'
            USING ERRCODE = '42501';
    END IF;

    INSERT INTO pgtrickle.pgt_row_identity_v2_inventory (
        inventory_id, recorded_by, recorded_at, notes, acknowledged,
        acknowledged_by, acknowledged_at
    )
    VALUES (TRUE, current_user, now(), p_notes, FALSE, NULL, NULL)
    ON CONFLICT (inventory_id) DO UPDATE
       SET recorded_by = EXCLUDED.recorded_by,
           recorded_at = EXCLUDED.recorded_at,
           notes = EXCLUDED.notes,
           acknowledged = FALSE,
           acknowledged_by = NULL,
           acknowledged_at = NULL;
END
$row_identity_v2_record_inventory$;

CREATE OR REPLACE FUNCTION pgtrickle.row_identity_v2_register_consumer(
    p_consumer_name TEXT,
    p_consumer_owner TEXT,
    p_affected_stream_tables TEXT[],
    p_consumes_row_id BOOLEAN,
    p_consumes_storage_layout BOOLEAN,
    p_required_schema_change TEXT
)
RETURNS BIGINT
LANGUAGE plpgsql
SET search_path TO pgtrickle, pg_catalog, pg_temp
AS $row_identity_v2_register_consumer$
DECLARE
    v_consumer_id BIGINT;
BEGIN
    IF NOT pgtrickle._row_identity_v2_admin() THEN
        RAISE EXCEPTION 'pg_trickle: row-identity inventory requires the extension owner or superuser'
            USING ERRCODE = '42501';
    END IF;
    IF NULLIF(btrim(p_consumer_name), '') IS NULL
       OR NULLIF(btrim(p_consumer_owner), '') IS NULL
       OR NULLIF(btrim(p_required_schema_change), '') IS NULL
       OR p_affected_stream_tables IS NULL
       OR cardinality(p_affected_stream_tables) = 0
       OR EXISTS (
           SELECT 1
           FROM unnest(p_affected_stream_tables) AS affected(name)
           WHERE NULLIF(btrim(affected.name), '') IS NULL
              OR position('.' IN btrim(affected.name)) = 0
              OR to_regclass(btrim(affected.name)) IS NULL
       ) THEN
        RAISE EXCEPTION 'pg_trickle: consumer inventory requires a name, owner, schema-qualified existing stream tables, and a schema-change plan'
            USING ERRCODE = '22023';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pgtrickle.pgt_row_identity_v2_inventory
        WHERE inventory_id
    ) THEN
        RAISE EXCEPTION 'pg_trickle: record the external-consumer inventory before registering consumers'
            USING ERRCODE = '55000';
    END IF;

    INSERT INTO pgtrickle.pgt_row_identity_v2_consumers (
        consumer_name, consumer_owner, affected_stream_tables,
        consumes_row_id, consumes_storage_layout, required_schema_change
    )
    VALUES (
        btrim(p_consumer_name), btrim(p_consumer_owner), p_affected_stream_tables,
        p_consumes_row_id, p_consumes_storage_layout, btrim(p_required_schema_change)
    )
    RETURNING consumer_id INTO v_consumer_id;

    UPDATE pgtrickle.pgt_row_identity_v2_inventory
       SET acknowledged = FALSE,
           acknowledged_by = NULL,
           acknowledged_at = NULL;
    RETURN v_consumer_id;
END
$row_identity_v2_register_consumer$;

CREATE OR REPLACE FUNCTION pgtrickle.row_identity_v2_acknowledge_inventory()
RETURNS VOID
LANGUAGE plpgsql
SET search_path TO pgtrickle, pg_catalog, pg_temp
AS $row_identity_v2_acknowledge_inventory$
BEGIN
    IF NOT pgtrickle._row_identity_v2_admin() THEN
        RAISE EXCEPTION 'pg_trickle: row-identity inventory requires the extension owner or superuser'
            USING ERRCODE = '42501';
    END IF;
    UPDATE pgtrickle.pgt_row_identity_v2_inventory
       SET acknowledged = TRUE,
           acknowledged_by = current_user,
           acknowledged_at = now();
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_trickle: record the external-consumer inventory before acknowledging it'
            USING ERRCODE = '55000';
    END IF;
END
$row_identity_v2_acknowledge_inventory$;

CREATE OR REPLACE FUNCTION pgtrickle.row_identity_v2_acknowledge_consumer(
    p_consumer_id BIGINT,
    p_resnapshot_status TEXT DEFAULT 'PENDING'
)
RETURNS VOID
LANGUAGE plpgsql
SET search_path TO pgtrickle, pg_catalog, pg_temp
AS $row_identity_v2_acknowledge_consumer$
DECLARE
    v_status TEXT := upper(btrim(p_resnapshot_status));
BEGIN
    IF NOT pgtrickle._row_identity_v2_admin() THEN
        RAISE EXCEPTION 'pg_trickle: row-identity inventory requires the extension owner or superuser'
            USING ERRCODE = '42501';
    END IF;
    IF v_status NOT IN ('PENDING', 'IN_PROGRESS', 'COMPLETE', 'SKIPPED') THEN
        RAISE EXCEPTION 'pg_trickle: invalid resnapshot status %', p_resnapshot_status
            USING ERRCODE = '22023';
    END IF;
    UPDATE pgtrickle.pgt_row_identity_v2_consumers
       SET resnapshot_status = v_status,
           acknowledged = TRUE,
           acknowledged_at = now(),
           updated_at = now()
     WHERE consumer_id = p_consumer_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_trickle: external consumer % is not in the inventory', p_consumer_id
            USING ERRCODE = '22023';
    END IF;
END
$row_identity_v2_acknowledge_consumer$;

CREATE OR REPLACE FUNCTION pgtrickle.row_identity_v2_consumer_inventory()
RETURNS TABLE (
    consumer_id BIGINT,
    consumer_name TEXT,
    consumer_owner TEXT,
    affected_stream_tables TEXT[],
    consumes_row_id BOOLEAN,
    consumes_storage_layout BOOLEAN,
    required_schema_change TEXT,
    resnapshot_status TEXT,
    acknowledged BOOLEAN,
    acknowledged_at TIMESTAMPTZ
)
LANGUAGE plpgsql
STABLE
SET search_path TO pgtrickle, pg_catalog, pg_temp
AS $row_identity_v2_consumer_inventory$
BEGIN
    IF NOT pgtrickle._row_identity_v2_admin() THEN
        RAISE EXCEPTION 'pg_trickle: row-identity inventory requires the extension owner or superuser'
            USING ERRCODE = '42501';
    END IF;
    RETURN QUERY
    SELECT c.consumer_id, c.consumer_name, c.consumer_owner,
           c.affected_stream_tables, c.consumes_row_id,
           c.consumes_storage_layout, c.required_schema_change,
           c.resnapshot_status, c.acknowledged, c.acknowledged_at
      FROM pgtrickle.pgt_row_identity_v2_consumers c
     ORDER BY c.consumer_id;
END
$row_identity_v2_consumer_inventory$;

CREATE OR REPLACE FUNCTION pgtrickle.row_identity_v2_recreation_preflight()
RETURNS JSONB
LANGUAGE plpgsql
SET search_path TO pgtrickle, pg_catalog, pg_temp
AS $row_identity_v2_recreation_preflight$
DECLARE
    v_checks JSONB[] := ARRAY[]::JSONB[];
    v_check JSONB;
    v_check_list JSONB;
    v_ok BOOLEAN;
    v_server_major INTEGER;
    v_bad_metadata BIGINT;
    v_bad_storage BIGINT;
    v_bad_buffers BIGINT;
    v_bad_sources BIGINT;
    v_bad_identity_types BIGINT;
    v_bad_indexes BIGINT;
    v_bad_index_sizes BIGINT;
    v_max_index_bytes BIGINT;
    v_scheduler_paused BOOLEAN;
    v_inventory_ack BOOLEAN;
    v_unacknowledged_consumers BIGINT;
    v_change_schema TEXT := COALESCE(
        current_setting('pg_trickle.change_buffer_schema', TRUE),
        'pgtrickle_changes'
    );
BEGIN
    IF NOT pgtrickle._row_identity_v2_admin() THEN
        RAISE EXCEPTION 'pg_trickle: recreation preflight requires the extension owner or superuser'
            USING ERRCODE = '42501';
    END IF;

    v_server_major := COALESCE(
        current_setting('server_version_num', TRUE), '0'
    )::INTEGER / 10000;
    SELECT count(*) INTO v_bad_metadata
      FROM (
          SELECT 1
            FROM pgtrickle.pgt_stream_tables
           WHERE row_identity_version IS DISTINCT FROM 2
              OR row_probe_version IS DISTINCT FROM 1
          UNION ALL
          SELECT 1
            FROM pgtrickle.pgt_change_buffers
           WHERE row_identity_version IS DISTINCT FROM 2
              OR row_probe_version IS DISTINCT FROM 1
      ) invalid_metadata;

    SELECT count(*) INTO v_bad_storage
      FROM pgtrickle.pgt_stream_tables st
      LEFT JOIN pg_attribute a
        ON a.attrelid = st.pgt_relid
       AND a.attname = '__pgt_row_id'
       AND a.attnum > 0
       AND NOT a.attisdropped
     WHERE a.atttypid IS DISTINCT FROM 'bytea'::regtype
        OR a.attnotnull IS DISTINCT FROM TRUE;

    SELECT count(*) INTO v_bad_buffers
      FROM pgtrickle.pgt_change_buffers cb
      LEFT JOIN pg_attribute a
        ON a.attrelid = to_regclass(format('%I.%I', v_change_schema, cb.buffer_key))
       AND a.attname = '__pgt_row_id'
       AND a.attnum > 0
       AND NOT a.attisdropped
     WHERE a.atttypid IS DISTINCT FROM 'bytea'::regtype
        OR a.attnotnull IS DISTINCT FROM TRUE;

    SELECT count(*) INTO v_bad_sources
      FROM pgtrickle.pgt_dependencies d
      LEFT JOIN pg_class c ON c.oid = d.source_relid
     WHERE d.source_type IN ('TABLE', 'STREAM_TABLE', 'VIEW', 'MATVIEW', 'FOREIGN_TABLE')
       AND c.oid IS NULL;

    SELECT count(*) INTO v_bad_identity_types
      FROM pgtrickle.pgt_dependencies d
      CROSS JOIN LATERAL unnest(COALESCE(d.columns_used, ARRAY[]::TEXT[])) AS u(column_name)
      LEFT JOIN pg_attribute a
        ON a.attrelid = d.source_relid
       AND a.attname = u.column_name
       AND a.attnum > 0
       AND NOT a.attisdropped
      LEFT JOIN pg_type t ON t.oid = a.atttypid
      LEFT JOIN pg_collation co ON co.oid = a.attcollation
     WHERE d.source_type IN ('TABLE', 'STREAM_TABLE', 'VIEW', 'MATVIEW', 'FOREIGN_TABLE')
       AND (
           a.attrelid IS NULL
           OR (
               t.typtype <> 'e'
               AND t.typname <> ALL (ARRAY[
                   'bool', 'int2', 'int4', 'int8', 'oid', 'float4', 'float8',
                   'numeric', 'text', 'varchar', 'bpchar', 'bytea', 'uuid',
                   'date', 'time', 'timestamp', 'timestamptz', 'timetz',
                   'interval', 'inet', 'cidr', 'macaddr', 'macaddr8',
                   'bit', 'varbit'
               ]::NAME[])
           )
               OR a.atttypmod IS NULL
               OR (co.oid IS NOT NULL AND co.collisdeterministic IS DISTINCT FROM TRUE)
       );

    SELECT count(*) INTO v_bad_indexes
      FROM pgtrickle.pgt_stream_tables st
     WHERE NOT EXISTS (
         SELECT 1
           FROM pg_index i
           JOIN pg_class ic ON ic.oid = i.indexrelid
           JOIN pg_am am ON am.oid = ic.relam
          WHERE i.indrelid = st.pgt_relid
            AND i.indisvalid
            AND i.indisready
            AND am.amname = 'btree'
            AND NOT (0 = ANY (i.indclass))
            AND pg_get_indexdef(i.indexrelid) LIKE '%__pgt_row_id%'
     );
    SELECT count(*) INTO v_bad_index_sizes
      FROM pgtrickle.pgt_stream_tables st
      JOIN pg_index i ON i.indrelid = st.pgt_relid
     WHERE i.indisvalid
       AND pg_get_indexdef(i.indexrelid) LIKE '%__pgt_row_id%'
       AND pg_relation_size(i.indexrelid) IS NULL;
    SELECT COALESCE(max(pg_relation_size(i.indexrelid)), 0)
      INTO v_max_index_bytes
      FROM pgtrickle.pgt_stream_tables st
      JOIN pg_index i ON i.indrelid = st.pgt_relid
     WHERE i.indisvalid
       AND pg_get_indexdef(i.indexrelid) LIKE '%__pgt_row_id%';

    v_scheduler_paused := COALESCE(
        current_setting('pg_trickle.enabled', TRUE), 'on'
    )::BOOLEAN = FALSE;
    SELECT COALESCE(bool_and(acknowledged), FALSE)
      INTO v_inventory_ack
      FROM pgtrickle.pgt_row_identity_v2_inventory
     WHERE inventory_id;
    SELECT count(*) INTO v_unacknowledged_consumers
      FROM pgtrickle.pgt_row_identity_v2_consumers
     WHERE NOT acknowledged;

    v_check := jsonb_build_object(
        'check', 'SUPPORTED_POSTGRESQL_MAJOR', 'ok', v_server_major = 18,
        'detail', format('server major=%s; V2 registry supports major 18', v_server_major)
    );
    v_checks := array_append(v_checks, v_check);
    v_checks := array_append(v_checks, jsonb_build_object(
        'check', 'IDENTITY_METADATA', 'ok', v_bad_metadata = 0,
        'detail', format('%s stream-table/buffer rows have stale or unknown version markers', v_bad_metadata)
    ));
    v_checks := array_append(v_checks, jsonb_build_object(
        'check', 'STORAGE_SCHEMA', 'ok', v_bad_storage = 0,
        'detail', format('%s stream tables lack __pgt_row_id BYTEA NOT NULL', v_bad_storage)
    ));
    v_checks := array_append(v_checks, jsonb_build_object(
        'check', 'BUFFER_SCHEMA', 'ok', v_bad_buffers = 0,
        'detail', format('%s change buffers lack __pgt_row_id BYTEA NOT NULL', v_bad_buffers)
    ));
    v_checks := array_append(v_checks, jsonb_build_object(
        'check', 'IDENTITY_TYPES_AND_COLLATIONS', 'ok', v_bad_identity_types = 0,
        'detail', format('%s source identity columns are missing, unsupported, or nondeterministically collated', v_bad_identity_types)
    ));
    v_checks := array_append(v_checks, jsonb_build_object(
        'check', 'SOURCE_KEY_ELIGIBILITY', 'ok', v_bad_sources = 0,
        'detail', format('%s source relations are missing; keyless sources remain valid and use exact full-ID matching', v_bad_sources)
    ));
    v_checks := array_append(v_checks, jsonb_build_object(
        'check', 'ROW_ID_INDEXES', 'ok', v_bad_indexes = 0 AND v_bad_index_sizes = 0,
        'detail', format('%s stream tables lack a valid row-id index; %s index sizes could not be read; largest=%s bytes', v_bad_indexes, v_bad_index_sizes, v_max_index_bytes)
    ));
    v_checks := array_append(v_checks, jsonb_build_object(
        'check', 'SCHEDULER_PAUSED', 'ok', v_scheduler_paused,
        'detail', CASE WHEN v_scheduler_paused
            THEN 'pg_trickle.enabled is off for this database'
            ELSE 'set pg_trickle.enabled=off and reload configuration before teardown'
        END
    ));
    v_checks := array_append(v_checks, jsonb_build_object(
        'check', 'EXTERNAL_CONSUMER_INVENTORY',
        'ok', v_inventory_ack AND v_unacknowledged_consumers = 0,
        'detail', format('inventory acknowledged=%s; unacknowledged consumers=%s', v_inventory_ack, v_unacknowledged_consumers)
    ));

    SELECT jsonb_agg(value) INTO v_check_list FROM unnest(v_checks) AS checks(value);
    SELECT COALESCE(bool_and((value ->> 'ok')::BOOLEAN), TRUE)
      INTO v_ok
      FROM unnest(v_checks) AS checks(value);

    RETURN jsonb_build_object(
        'ok', v_ok,
        'release', '0.87.17',
        'checks', COALESCE(v_check_list, '[]'::JSONB),
        'instructions', jsonb_build_array(
            'Pause the scheduler and wait for in-flight refreshes to drain.',
            'Export stream-table definitions and metadata before dropping anything.',
            'Drop V1 stream tables in reverse dependency order and clean only unused V1 buffers.',
            'Install and restart the v0.87.17 binary, then run ALTER EXTENSION pg_trickle UPDATE.',
            'Recreate stream tables in dependency order and perform fresh initial refreshes.',
            'Update every external consumer to BYTEA, resnapshot after refresh, and acknowledge completion.',
            'Writes made during the recreation window are not replayed; resume writes only after resnapshot.'
        )
    );
END
$row_identity_v2_recreation_preflight$;

REVOKE ALL ON TABLE pgtrickle.pgt_row_identity_v2_inventory FROM PUBLIC;
REVOKE ALL ON TABLE pgtrickle.pgt_row_identity_v2_consumers FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtrickle._row_identity_v2_admin() FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtrickle.row_identity_v2_record_inventory(TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtrickle.row_identity_v2_register_consumer(TEXT, TEXT, TEXT[], BOOLEAN, BOOLEAN, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtrickle.row_identity_v2_acknowledge_inventory() FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtrickle.row_identity_v2_acknowledge_consumer(BIGINT, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtrickle.row_identity_v2_consumer_inventory() FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtrickle.row_identity_v2_recreation_preflight() FROM PUBLIC;
