-- Demo C: Multi-Engine Leaderboard
CREATE EXTENSION IF NOT EXISTS pg_trickle;

CREATE TABLE IF NOT EXISTS game_scores (
    score_id BIGSERIAL PRIMARY KEY,
    player_id INT NOT NULL,
    player_name TEXT NOT NULL,
    game_id INT NOT NULL,
    score INT NOT NULL,
    scored_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ducklake_table (
    table_id BIGSERIAL PRIMARY KEY,
    schema_name TEXT NOT NULL DEFAULT 'public',
    table_name TEXT NOT NULL,
    data_path TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS ducklake_data_file (
    data_file_id BIGSERIAL PRIMARY KEY,
    table_id BIGINT NOT NULL,
    begin_snapshot BIGINT,
    path TEXT NOT NULL,
    row_count BIGINT,
    file_size_bytes BIGINT,
    encryption_key_id TEXT
);

CREATE TABLE IF NOT EXISTS ducklake_table_stats (
    table_id BIGINT PRIMARY KEY,
    row_count BIGINT DEFAULT 0,
    file_count BIGINT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ducklake_snapshot (
    table_id BIGINT NOT NULL,
    snapshot_id BIGINT NOT NULL,
    snapshot_time TIMESTAMPTZ DEFAULT now(),
    created_by TEXT,
    PRIMARY KEY (table_id, snapshot_id)
);

CREATE TABLE IF NOT EXISTS ducklake_view (
    view_name TEXT PRIMARY KEY,
    view_definition TEXT
);

INSERT INTO ducklake_table (schema_name, table_name, data_path)
VALUES
    ('public', 'top_players', 's3://pg-trickle-demo/top_players/'),
    ('public', 'scores_by_game', 's3://pg-trickle-demo/scores_by_game/');

SELECT pgtrickle.create_stream_table(
    name => 'top_players',
    query => $$
        SELECT
            player_id,
            player_name,
            SUM(score) AS total_score,
            COUNT(1) AS games_played,
            RANK() OVER (ORDER BY SUM(score) DESC) AS rank
        FROM game_scores
        GROUP BY player_id, player_name
    $$,
    schedule => '5s',
    refresh_mode => 'DIFFERENTIAL',
    sink => 'ducklake',
    ducklake_sink_path => 's3://pg-trickle-demo/top_players/',
    ducklake_sink_table_id => (SELECT table_id FROM ducklake_table WHERE table_name = 'top_players')
);

SELECT pgtrickle.create_stream_table(
    name => 'scores_by_game',
    query => $$
        SELECT
            game_id,
            SUM(score) AS total_score,
            COUNT(DISTINCT player_id) AS player_count,
            MAX(score) AS high_score
        FROM game_scores
        GROUP BY game_id
    $$,
    schedule => '5s',
    refresh_mode => 'DIFFERENTIAL',
    sink => 'ducklake',
    ducklake_sink_path => 's3://pg-trickle-demo/scores_by_game/',
    ducklake_sink_table_id => (SELECT table_id FROM ducklake_table WHERE table_name = 'scores_by_game')
);
