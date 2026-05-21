-- Demo C: Multi-Engine Leaderboard — PostgreSQL initialization

CREATE EXTENSION IF NOT EXISTS pg_trickle;

-- Source table
CREATE TABLE IF NOT EXISTS game_scores (
    score_id   BIGSERIAL PRIMARY KEY,
    player_id  INT NOT NULL,
    player_name TEXT NOT NULL,
    game_id    INT NOT NULL,
    score      INT NOT NULL,
    scored_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Stream table: top players leaderboard
SELECT pgtrickle.create_stream_table(
    name     => 'top_players',
    query    => $$
        SELECT
            player_id,
            player_name,
            SUM(score)  AS total_score,
            COUNT(*)    AS games_played,
            RANK() OVER (ORDER BY SUM(score) DESC) AS rank
        FROM game_scores
        GROUP BY player_id, player_name
    $$,
    schedule     => '5s',
    refresh_mode => 'DIFFERENTIAL'
);

-- Stream table: scores by game
SELECT pgtrickle.create_stream_table(
    name     => 'scores_by_game',
    query    => $$
        SELECT
            game_id,
            AVG(score)  AS avg_score,
            MAX(score)  AS high_score,
            COUNT(*)    AS player_count
        FROM game_scores
        GROUP BY game_id
    $$,
    schedule     => '5s',
    refresh_mode => 'DIFFERENTIAL'
);
