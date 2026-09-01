-- pg_trickle 0.88.0 -> 0.89.0

ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN window_strategy JSONB
    CHECK (window_strategy IS NULL OR jsonb_typeof(window_strategy) = 'object');

CREATE TABLE pgtrickle.pgt_window_states (
    pgt_id              BIGINT NOT NULL
                        REFERENCES pgtrickle.pgt_stream_tables(pgt_id)
                        ON DELETE CASCADE,
    node_ordinal        INTEGER NOT NULL,
    spec_ordinal        INTEGER NOT NULL,
    partition_relid     OID NOT NULL,
    row_relid           OID NOT NULL,
    peer_relid          OID,
    schema_version      SMALLINT NOT NULL,
    strategy_version    SMALLINT NOT NULL,
    query_hash          BIGINT NOT NULL,
    state_generation    BIGINT NOT NULL,
    status              TEXT NOT NULL
                        CHECK (status IN
                               ('BUILDING', 'READY', 'STALE', 'OVER_BUDGET')),
    estimated_bytes     BIGINT NOT NULL DEFAULT 0
                        CHECK (estimated_bytes >= 0),
    last_validated_at   TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pgt_id, node_ordinal, spec_ordinal)
);

REVOKE ALL ON TABLE pgtrickle.pgt_window_states FROM PUBLIC;

SELECT pg_catalog.pg_extension_config_dump(
    'pgtrickle.pgt_window_states',
    'WHERE false'
);

INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES (
    '0.89.0',
    'Incremental window strategy and private state registry'
)
ON CONFLICT (version) DO NOTHING;
