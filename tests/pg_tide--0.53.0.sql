CREATE SCHEMA tide;

CREATE TABLE tide.tide_outbox_config (
    outbox_name TEXT PRIMARY KEY,
    retention_hours INTEGER NOT NULL DEFAULT 24,
    inline_threshold INTEGER NOT NULL DEFAULT 10000,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE tide.outbox_caller_log (caller_name TEXT NOT NULL);

CREATE OR REPLACE FUNCTION tide.outbox_create(
    p_name TEXT,
    p_retention_hours INTEGER DEFAULT 24,
    p_inline_threshold INTEGER DEFAULT 10000
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO tide.outbox_caller_log VALUES (current_user);
    INSERT INTO tide.tide_outbox_config (
        outbox_name, retention_hours, inline_threshold
    ) VALUES (p_name, p_retention_hours, p_inline_threshold);
END;
$$;

CREATE OR REPLACE FUNCTION tide.outbox_publish(
    p_name TEXT,
    p_payload JSONB,
    p_headers JSONB
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    IF to_regclass('public.tide_publish_log') IS NOT NULL THEN
        EXECUTE 'INSERT INTO public.tide_publish_log DEFAULT VALUES';
    END IF;
END;
$$;
