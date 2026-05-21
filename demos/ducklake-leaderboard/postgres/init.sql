-- Demo C: Multi-Engine Leaderboard — PostgreSQL initialization
-- Sets up source tables and stream tables for the leaderboard demo.

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

-- Stream table: top players leaderboard (written to DuckLake)
SELECT pgtrickle.create_stream_table(
    'top_players',
    query => $$
        SELECT
            player_id,
            player_name,
            SUM(score)  AS total_score,
            COUNT(*)    AS games_played,
            RANK() OVER (ORDER BY SUM(score) DESC) AS rank
        FROM game_scores
        GROUP BY player_id, player_name
    $$,
    schedule           => '5s',
    refresh_mode       => 'DIFFE    refresh_mode       => 'DIFFE    refresh_mode       => 'DIFFE    refresh_mode       => 'DIFFE   _p    refresh_mode       => 'DIFFE    refresh_mode       => 'DIFFEea    refresh_mode       => 'DIFFE    refresh_mode       => 'DIFFE   CT
    refresh_mode       => 'DIFFE    refree)     refresh_mo
                                                                              
        FROM game_scores
        GROUP BY game_id
    $$,
    schedule           => '5s',
    refresh_mode       => 'DIFFERENTIAL',
    sink               => 'ducklake',
                                                                
);
