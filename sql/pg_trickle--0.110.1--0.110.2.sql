-- pg_trickle v0.110.1 -> v0.110.2
-- #1438: fence stale scheduled failure updates after stream-table recovery.
-- Historical jobs lack trustworthy enqueue-generation snapshots. Leave their
-- arrays empty so old jobs cannot overwrite an operator's successful resume.

ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS recovery_generation BIGINT NOT NULL DEFAULT 0
        CHECK (recovery_generation >= 0);

ALTER TABLE pgtrickle.pgt_scheduler_jobs
    ADD COLUMN IF NOT EXISTS member_recovery_generations BIGINT[] NOT NULL DEFAULT '{}'::BIGINT[];

-- Install the pre-DDL hook used for protected partition cuts and for the
-- first phase of concurrent DETACH. Existing streams remain unconfigured.
CREATE OR REPLACE FUNCTION pgtrickle._on_ddl_start()
RETURNS event_trigger
LANGUAGE c
AS 'MODULE_PATHNAME', 'pg_trickle_on_ddl_start_wrapper';
REVOKE EXECUTE ON FUNCTION pgtrickle._on_ddl_start() FROM PUBLIC;

DO $migration$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_event_trigger
        WHERE evtname = 'pg_trickle_ddl_start_tracker'
          AND (evtevent <> 'ddl_command_start'
               OR evtfoid <> 'pgtrickle._on_ddl_start()'::pg_catalog.regprocedure)
    ) THEN
        RAISE EXCEPTION 'event trigger pg_trickle_ddl_start_tracker has an unexpected definition';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_event_trigger
        WHERE evtname = 'pg_trickle_ddl_start_tracker'
    ) THEN
        CREATE EVENT TRIGGER pg_trickle_ddl_start_tracker
            ON ddl_command_start EXECUTE FUNCTION pgtrickle._on_ddl_start();
    END IF;
END
$migration$;

CREATE OR REPLACE FUNCTION pgtrickle.set_stream_table_partition_retention(
    name text, policy text, source_column text DEFAULT NULL, cutoff text DEFAULT NULL
) RETURNS void
LANGUAGE c SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'set_stream_table_partition_retention_wrapper';

CREATE OR REPLACE FUNCTION pgtrickle.get_stream_table_partition_retention(name text)
RETURNS jsonb
LANGUAGE c STRICT SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'get_stream_table_partition_retention_wrapper';

CREATE OR REPLACE FUNCTION pgtrickle.create_stream_table_with_retention(
    name text, query text, policy text, source_column text, cutoff text,
    schedule text DEFAULT 'calculated'
) RETURNS void
LANGUAGE c SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'create_stream_table_with_retention_wrapper';

REVOKE EXECUTE ON FUNCTION pgtrickle.set_stream_table_partition_retention(text, text, text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.get_stream_table_partition_retention(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.create_stream_table_with_retention(text, text, text, text, text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.set_stream_table_partition_retention(text, text, text, text) TO pgtrickle_admin;
GRANT EXECUTE ON FUNCTION pgtrickle.get_stream_table_partition_retention(text) TO pgtrickle_admin;
GRANT EXECUTE ON FUNCTION pgtrickle.create_stream_table_with_retention(text, text, text, text, text, text) TO pgtrickle_admin;

-- #1463: Declarative preflight for replacements that would change the
-- published stream table's storage relation. Owner checking happens in Rust.
CREATE OR REPLACE FUNCTION pgtrickle.would_replace_stream_table_storage(
    name text, query text
) RETURNS boolean
LANGUAGE c STRICT SECURITY DEFINER
SET search_path TO pgtrickle, pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'would_replace_stream_table_storage_wrapper';

REVOKE EXECUTE ON FUNCTION pgtrickle.would_replace_stream_table_storage(text, text) FROM PUBLIC;

-- Old refresh records cannot establish current contract provenance.
ALTER TABLE pgtrickle.pgt_refresh_history
    ADD COLUMN IF NOT EXISTS contract_generation BIGINT;
