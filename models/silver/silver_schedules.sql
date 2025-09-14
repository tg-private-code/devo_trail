{{ config(
    materialized='table',
    description='Cleaned and structured data from the schedules table.'
)
}}

SELECT
    gameId AS game_id,
    gameNumber AS game_number,
    year,
    type,
    dayNight AS day_night,
    duration,
    duration_minutes,
    homeTeamId AS home_team_id,
    homeTeamName AS home_team_name,
    awayTeamId AS away_team_id,
    awayTeamName AS away_team_name,
    startTime AS start_time,
    attendance,
    status,
    created
FROM {{ source('baseball', 'schedules') }}
WHERE gameId IS NOT NULL