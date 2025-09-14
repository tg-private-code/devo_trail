{{ config(
    materialized='table',
    description='Cleaned and structured data about baseball games from the public dataset.'
) }}

SELECT
    gameId AS game_id,
    extract(DATE from startTime) AS game_date,
    extract(TIME from startTime) AS game_time,
    homeTeamName AS home_team,
    awayTeamName AS visitor_team,
    venueid
    attendance,
    homeFinalRuns,
    awayFinalRuns
FROM {{ source('baseball', 'games_wide') }}
WHERE gameId IS NOT NULL
group by gameid