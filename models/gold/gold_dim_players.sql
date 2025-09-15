{{ config(
    materialized='table',
    description='Gold dimension table for players.'
) }}

WITH player_latest_game AS (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY player_id ORDER BY game_id DESC) as rn
    FROM {{ ref('silver_players') }}
)

SELECT
    player_id,
    first_name,
    last_name,
    hitter,
    hitter_weight,
    hitter_height,
    hitter_bad_hand,
    pitcher_hand,
    pitch_type,
    game_id
FROM player_latest_game
where rn = 1