-- pg_trickle 0.92.0 -> 0.93.0
--
-- v0.93.0 adds durable MANAGED/EXTERNAL orchestration ownership and the
-- versioned Graph V1 contract APIs. Existing stream tables remain MANAGED.

ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS orchestration_mode TEXT NOT NULL DEFAULT 'MANAGED';
ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS contract_generation BIGINT NOT NULL DEFAULT 1;

UPDATE pgtrickle.pgt_stream_tables
SET orchestration_mode = 'MANAGED'
WHERE orchestration_mode IS NULL
   OR upper(orchestration_mode) NOT IN ('MANAGED', 'EXTERNAL');
UPDATE pgtrickle.pgt_stream_tables
SET contract_generation = 1
WHERE contract_generation IS NULL OR contract_generation < 1;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint
        WHERE conname = 'pgt_stream_tables_orchestration_mode_check'
          AND conrelid = 'pgtrickle.pgt_stream_tables'::regclass
    ) THEN
        ALTER TABLE pgtrickle.pgt_stream_tables
            ADD CONSTRAINT pgt_stream_tables_orchestration_mode_check
            CHECK (orchestration_mode IN ('MANAGED', 'EXTERNAL'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint
        WHERE conname = 'pgt_stream_tables_contract_generation_check'
          AND conrelid = 'pgtrickle.pgt_stream_tables'::regclass
    ) THEN
        ALTER TABLE pgtrickle.pgt_stream_tables
            ADD CONSTRAINT pgt_stream_tables_contract_generation_check
            CHECK (contract_generation > 0);
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION pgtrickle._bump_contract_generation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pgtrickle, pg_catalog, pg_temp
AS $$
BEGIN
    IF ROW(OLD.pgt_relid, OLD.pgt_name, OLD.pgt_schema, OLD.defining_query,
           OLD.original_query, OLD.refresh_mode, OLD.requested_refresh_mode,
           OLD.functions_used, OLD.topk_limit, OLD.topk_order_by,
           OLD.topk_offset, OLD.diamond_consistency,
           OLD.diamond_schedule_policy, OLD.has_keyless_source,
           OLD.requested_cdc_mode, OLD.is_append_only, OLD.scc_id,
           OLD.st_partition_key, OLD.temporal_mode, OLD.storage_backend,
           OLD.row_identity_version, OLD.row_probe_version,
           OLD.defining_search_path, OLD.orchestration_mode)
       IS DISTINCT FROM
       ROW(NEW.pgt_relid, NEW.pgt_name, NEW.pgt_schema, NEW.defining_query,
           NEW.original_query, NEW.refresh_mode, NEW.requested_refresh_mode,
           NEW.functions_used, NEW.topk_limit, NEW.topk_order_by,
           NEW.topk_offset, NEW.diamond_consistency,
           NEW.diamond_schedule_policy, NEW.has_keyless_source,
           NEW.requested_cdc_mode, NEW.is_append_only, NEW.scc_id,
           NEW.st_partition_key, NEW.temporal_mode, NEW.storage_backend,
           NEW.row_identity_version, NEW.row_probe_version,
           NEW.defining_search_path, NEW.orchestration_mode) THEN
        NEW.contract_generation := OLD.contract_generation + 1;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS pgt_stream_tables_contract_generation
    ON pgtrickle.pgt_stream_tables;
CREATE TRIGGER pgt_stream_tables_contract_generation
BEFORE UPDATE OF pgt_relid, pgt_name, pgt_schema, defining_query,
    original_query, refresh_mode, requested_refresh_mode, functions_used,
    topk_limit, topk_order_by, topk_offset, diamond_consistency,
    diamond_schedule_policy, has_keyless_source, requested_cdc_mode,
    is_append_only, scc_id, st_partition_key, temporal_mode, storage_backend,
    row_identity_version, row_probe_version, defining_search_path,
    orchestration_mode
ON pgtrickle.pgt_stream_tables
FOR EACH ROW EXECUTE FUNCTION pgtrickle._bump_contract_generation();

CREATE OR REPLACE FUNCTION pgtrickle._bump_dependency_contract_generation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pgtrickle, pg_catalog, pg_temp
AS $$
BEGIN
    UPDATE pgtrickle.pgt_stream_tables
    SET contract_generation = contract_generation + 1
    WHERE pgt_id = CASE WHEN TG_OP = 'DELETE' THEN OLD.pgt_id ELSE NEW.pgt_id END;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS pgt_dependencies_contract_generation
    ON pgtrickle.pgt_dependencies;
CREATE TRIGGER pgt_dependencies_contract_generation
AFTER INSERT OR UPDATE OR DELETE ON pgtrickle.pgt_dependencies
FOR EACH ROW EXECUTE FUNCTION pgtrickle._bump_dependency_contract_generation();

-- The create APIs gain an optional orchestration mode. Rename the old C
-- functions instead of dropping them: dependent views and functions retain
-- their OID-bound references, while normal calls resolve to the new API.
ALTER FUNCTION pgtrickle.create_stream_table(
    text, text, text, text, boolean, text, text, text, boolean, boolean,
    text, integer, double precision, text, boolean, text, integer, text
) RENAME TO create_stream_table__v092;
ALTER FUNCTION pgtrickle.create_stream_table_if_not_exists(
    text, text, text, text, boolean, text, text, text, boolean, boolean,
    text, integer, double precision, text, boolean, text, integer, text
) RENAME TO create_stream_table_if_not_exists__v092;
ALTER FUNCTION pgtrickle.create_or_replace_stream_table(
    text, text, text, text, boolean, text, text, text, boolean, boolean,
    text, integer, double precision, text, boolean, text
) RENAME TO create_or_replace_stream_table__v092;

CREATE OR REPLACE FUNCTION pgtrickle."create_stream_table"(
    "name" TEXT,
    "query" TEXT,
    "schedule" TEXT DEFAULT 'calculated',
    "refresh_mode" TEXT DEFAULT 'AUTO',
    "initialize" bool DEFAULT true,
    "diamond_consistency" TEXT DEFAULT NULL,
    "diamond_schedule_policy" TEXT DEFAULT NULL,
    "cdc_mode" TEXT DEFAULT NULL,
    "append_only" bool DEFAULT false,
    "pooler_compatibility_mode" bool DEFAULT false,
    "partition_by" TEXT DEFAULT NULL,
    "max_differential_joins" INT DEFAULT NULL,
    "max_delta_fraction" double precision DEFAULT NULL,
    "output_distribution_column" TEXT DEFAULT NULL,
    "temporal" bool DEFAULT false,
    "storage_backend" TEXT DEFAULT NULL,
    "fillfactor" INT DEFAULT NULL,
    "target_freshness" TEXT DEFAULT NULL,
    "orchestration_mode" TEXT DEFAULT 'MANAGED'
)
RETURNS void
SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c
AS 'MODULE_PATHNAME', 'create_stream_table_wrapper';

CREATE OR REPLACE FUNCTION pgtrickle."create_stream_table_if_not_exists"(
    "name" TEXT,
    "query" TEXT,
    "schedule" TEXT DEFAULT 'calculated',
    "refresh_mode" TEXT DEFAULT 'AUTO',
    "initialize" bool DEFAULT true,
    "diamond_consistency" TEXT DEFAULT NULL,
    "diamond_schedule_policy" TEXT DEFAULT NULL,
    "cdc_mode" TEXT DEFAULT NULL,
    "append_only" bool DEFAULT false,
    "pooler_compatibility_mode" bool DEFAULT false,
    "partition_by" TEXT DEFAULT NULL,
    "max_differential_joins" INT DEFAULT NULL,
    "max_delta_fraction" double precision DEFAULT NULL,
    "output_distribution_column" TEXT DEFAULT NULL,
    "temporal" bool DEFAULT false,
    "storage_backend" TEXT DEFAULT NULL,
    "fillfactor" INT DEFAULT NULL,
    "target_freshness" TEXT DEFAULT NULL,
    "orchestration_mode" TEXT DEFAULT 'MANAGED'
)
RETURNS void
SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c
AS 'MODULE_PATHNAME', 'create_stream_table_if_not_exists_wrapper';

CREATE OR REPLACE FUNCTION pgtrickle."create_or_replace_stream_table"(
    "name" TEXT,
    "query" TEXT,
    "schedule" TEXT DEFAULT 'calculated',
    "refresh_mode" TEXT DEFAULT 'AUTO',
    "initialize" bool DEFAULT true,
    "diamond_consistency" TEXT DEFAULT NULL,
    "diamond_schedule_policy" TEXT DEFAULT NULL,
    "cdc_mode" TEXT DEFAULT NULL,
    "append_only" bool DEFAULT false,
    "pooler_compatibility_mode" bool DEFAULT false,
    "partition_by" TEXT DEFAULT NULL,
    "max_differential_joins" INT DEFAULT NULL,
    "max_delta_fraction" double precision DEFAULT NULL,
    "output_distribution_column" TEXT DEFAULT NULL,
    "temporal" bool DEFAULT false,
    "storage_backend" TEXT DEFAULT NULL,
    "orchestration_mode" TEXT DEFAULT NULL
)
RETURNS void
SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c
AS 'MODULE_PATHNAME', 'create_or_replace_stream_table_wrapper';

CREATE OR REPLACE FUNCTION pgtrickle."integration_capabilities"()
RETURNS TABLE (
    "capability" TEXT,
    "major_version" smallint,
    "minor_version" smallint,
    "enabled" bool,
    "details" jsonb
)
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'integration_capabilities_wrapper';

CREATE OR REPLACE FUNCTION pgtrickle."set_orchestration_mode"(
    "stream_table" regclass,
    "mode" TEXT
)
RETURNS TEXT
STRICT SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c
AS 'MODULE_PATHNAME', 'set_orchestration_mode_wrapper';

CREATE OR REPLACE FUNCTION pgtrickle."stream_table_contract"(
    "stream_table" regclass
)
RETURNS TABLE (
    "contract_version" smallint,
    "contract_generation" bigint,
    "contract_digest" bytea,
    "contract" jsonb
)
STRICT SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c
AS 'MODULE_PATHNAME', 'stream_table_contract_wrapper';

CREATE OR REPLACE FUNCTION pgtrickle."graph_contract"(
    "roots" regclass[]
)
RETURNS TABLE (
    "contract_version" smallint,
    "graph_digest" bytea,
    "contract" jsonb
)
STRICT SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
LANGUAGE c
AS 'MODULE_PATHNAME', 'graph_contract_wrapper';

GRANT EXECUTE ON FUNCTION pgtrickle.integration_capabilities() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.stream_table_contract(regclass) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.graph_contract(regclass[]) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.set_orchestration_mode(regclass, text) TO PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.create_stream_table__v092(text, text, text, text, boolean, text, text, text, boolean, boolean, text, integer, double precision, text, boolean, text, integer, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.create_stream_table_if_not_exists__v092(text, text, text, text, boolean, text, text, text, boolean, boolean, text, integer, double precision, text, boolean, text, integer, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.create_or_replace_stream_table__v092(text, text, text, text, boolean, text, text, text, boolean, boolean, text, integer, double precision, text, boolean, text) FROM PUBLIC;

INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES (
    '0.93.0',
    'Graph V1 contracts and durable external orchestration ownership'
)
ON CONFLICT (version) DO NOTHING;
