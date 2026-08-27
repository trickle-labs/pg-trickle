CREATE SCHEMA tide;
CREATE TABLE tide.tide_outbox_config (
    outbox_name TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

