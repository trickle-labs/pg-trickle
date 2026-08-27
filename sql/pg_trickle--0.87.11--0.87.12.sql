-- pg_trickle 0.87.11 -> 0.87.12 upgrade migration
-- Add immutable provenance for downstream publications and adopt only live,
-- exactly matching legacy publications.

CREATE TABLE IF NOT EXISTS pgtrickle.pgt_publication_bindings (
    pgt_id                  BIGINT PRIMARY KEY
                            REFERENCES pgtrickle.pgt_stream_tables(pgt_id)
                            ON DELETE CASCADE,
    stream_relid            OID NOT NULL,
    publication_oid         OID NOT NULL UNIQUE,
    publication_name        TEXT NOT NULL UNIQUE,
    publication_owner_oid   OID NOT NULL,
    expected_relation_oids  OID[] NOT NULL,
    CONSTRAINT pgt_publication_binding_relations_check
        CHECK (expected_relation_oids = ARRAY[stream_relid])
);
REVOKE ALL ON TABLE pgtrickle.pgt_publication_bindings FROM PUBLIC;

DO $migration$
DECLARE
    legacy_count    BIGINT := 0;
    inserted_count  BIGINT := 0;
    row_data        RECORD;
BEGIN
    FOR row_data IN
        SELECT st.pgt_id,
               st.pgt_schema,
               st.pgt_name,
               st.pgt_relid,
               st.downstream_publication_name,
               pub.oid AS publication_oid,
               pub.pubowner AS publication_owner_oid,
               pub.puballtables,
               COALESCE(
                   array_agg(pubrel.prrelid ORDER BY pubrel.prrelid)
                       FILTER (WHERE pubrel.prrelid IS NOT NULL),
                   ARRAY[]::oid[]
               ) AS relation_oids,
               COUNT(pubns.oid) AS schema_memberships
        FROM pgtrickle.pgt_stream_tables AS st
        LEFT JOIN pg_catalog.pg_publication AS pub
          ON pub.pubname = st.downstream_publication_name
        LEFT JOIN pg_catalog.pg_publication_rel AS pubrel
          ON pubrel.prpubid = pub.oid
        LEFT JOIN pg_catalog.pg_publication_namespace AS pubns
          ON pubns.pnpubid = pub.oid
        WHERE st.downstream_publication_name IS NOT NULL
        GROUP BY st.pgt_id, st.pgt_schema, st.pgt_name, st.pgt_relid,
                 st.downstream_publication_name, pub.oid, pub.pubowner,
                 pub.puballtables
    LOOP
        legacy_count := legacy_count + 1;

        IF row_data.publication_oid IS NULL THEN
            RAISE EXCEPTION
                'v0.87.12 publication backfill failed for stream %.%: publication % does not exist; run pgtrickle.lifecycle_preflight()',
                row_data.pgt_schema, row_data.pgt_name,
                row_data.downstream_publication_name;
        END IF;

        IF row_data.puballtables OR row_data.schema_memberships <> 0
           OR row_data.relation_oids <> ARRAY[row_data.pgt_relid]::oid[] THEN
            RAISE EXCEPTION
                'v0.87.12 publication backfill failed for stream %.% and publication %: publication scope or relation set is invalid; run pgtrickle.lifecycle_preflight()',
                row_data.pgt_schema, row_data.pgt_name,
                row_data.downstream_publication_name;
        END IF;

        INSERT INTO pgtrickle.pgt_publication_bindings (
            pgt_id,
            stream_relid,
            publication_oid,
            publication_name,
            publication_owner_oid,
            expected_relation_oids
        ) VALUES (
            row_data.pgt_id,
            row_data.pgt_relid,
            row_data.publication_oid,
            row_data.downstream_publication_name,
            row_data.publication_owner_oid,
            ARRAY[row_data.pgt_relid]::oid[]
        );
        inserted_count := inserted_count + 1;
    END LOOP;

    IF inserted_count <> legacy_count THEN
        RAISE EXCEPTION
            'v0.87.12 publication backfill inserted % bindings for % legacy publications',
            inserted_count, legacy_count;
    END IF;
END
$migration$;

ALTER FUNCTION pgtrickle.stream_table_to_publication(text)
    SECURITY DEFINER; -- nosemgrep: sql.security-definer.present — search_path is pinned immediately below.
ALTER FUNCTION pgtrickle.stream_table_to_publication(text)
    SET search_path = pgtrickle, pg_catalog, pg_temp;
ALTER FUNCTION pgtrickle.drop_stream_table_publication(text)
    SECURITY DEFINER; -- nosemgrep: sql.security-definer.present — search_path is pinned immediately below.
ALTER FUNCTION pgtrickle.drop_stream_table_publication(text)
    SET search_path = pgtrickle, pg_catalog, pg_temp;
