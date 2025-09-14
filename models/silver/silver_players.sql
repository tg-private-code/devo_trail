{{ config(
    materialized='table',
    description='Cleaned and structured data about baseball games from the public dataset.'
) }}

WITH all_players AS (
    SELECT DISTINCT
        hitterLastName as last_name,
        hitterFirstName as first_name,
        1 as hitter,
        hitterWeight as hitter_weight,
        hitterHeight as hitter_height,
        hitterBatHand as hitter_bad_hand,
        "" as pitcher_hand,
        "" as pitch_type
     FROM {{ source('baseball', 'games_wide') }}
     UNION ALL
     SELECT DISTINCT
         pitcherLastName as last_name,
         pitcherFirstName as first_name,
         0 as hitter,
         0 as hitter_weight,
         0 as hitter_height,
         "" as hitter_bad_hand,
         pitcherThrowHand as pitcher_hand,
         pitchType as pitch_type
     FROM {{ source('baseball', 'games_wide') }}
)

SELECT
  MD5(CONCAT(last_name,first_name,hitter)) AS player_id,
  *
FROM all_players