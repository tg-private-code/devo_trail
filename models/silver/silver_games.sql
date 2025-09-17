{{ config(
    materialized='table',
    description='Cleaned and structured data about baseball games from the public dataset.'
) }}

SELECT DISTINCT
    gameId AS game_id,
    extract(DATE from startTime) AS game_date,
    extract(TIME from startTime) AS game_time,
    homeTeamName AS home_team,
    awayTeamName AS visitor_team,
    venueid as venue_id,
    attendance,
    homeFinalRuns as home_final_runs,
    awayFinalRuns as away_final_runs "test error"
FROM {{ source('baseball', 'games_wide') }}
WHERE gameId IS NOT NULL
