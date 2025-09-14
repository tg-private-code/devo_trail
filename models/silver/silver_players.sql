{{ config(
    materialized='table',
    description='Cleaned and structured data about baseball games from the public dataset.'
) }}


WITH all_players AS (
    SELECT DISTINCT
        hitterLastName as last_name,
        hitterFirstname as first_name,
        1 as hitter,
        hitterWeigth as hitter_weight,
        hitterHeight as hitter_hight,
        hitterBatHand as hitter_bad_hand,
        NULL as pitch_hand,
        NULL as pitch_type
     FROM {{ source('baseball', 'games_wide') }}
     UNION
     SELECT DISTINCT
         pitcherLastName as last_name,
         pitcherFirstName as first_name,
         0 as hitter,
         Null as hitter_weight,
         NULL as hitter_height,
         NULL as hitter_bad_hand,
         pitcherThrowHand as pitcher_hand,
         pitchType as pitch_type
)

SELECT
  MD5(LOWER(last_name,first_name,hitter)) AS player_id,
  *
FROM all_players