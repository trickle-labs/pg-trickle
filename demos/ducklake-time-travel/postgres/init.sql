-- Time-Travel Demo - Simplified for pg_trickle only
-- Note: Original demo requires DuckLake extension which is optional
-- This simplified version demonstrates pg_trickle stream tables

CREATE EXTENSION IF NOT EXISTS pg_trickle;

-- Create a simple events table for demonstration
CREATE TABLE IF NOT EXISTS events (
    event_id   BIGINT PRIMARY KEY,
    event_type TEXT   NOT NULL,
    user_id    BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Insert sample data
INSERT INTO events (event_id, event_type, user_id, created_at) VALUES
    (1, 'login', 100, now()),
    (2, 'click', 100, now()),
    (3, 'login', 101, now());

-- Create a stream table demonstrating pg_trickle functionality
SELECT pgtrickle.create_stream_table(
    name => 'events_by_type',
    query => $$
        SELECT event_type, MAX(event_id) as latest_event_id, MIN(user_id) as min_user
        FROM events GROUP BY event_type
    $$,
    schedule => '5s',
    refresh_mode => 'DIFFERENTIAL'
);
