{{ config(
    materialized='table',
    description='Gold dimension table for players.'
) }}

SELECT
    player_id,
    first_name,
    last_name,
    hitter,
    hitter_weight,
    hitter_height,
    hitter_bad_hand,
    pitcher_hand,
    pitch_type
FROM {{ ref('silver_players') }}