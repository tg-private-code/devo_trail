{{ config(
    materialized='table',
    description='A unique list of all teams extracted from the games_wide table.'
) }}

WITH all_teams AS (
  SELECT
    homeTeamName AS team_name
  FROM {{ source('baseball', 'games_wide') }}
  UNION ALL
  SELECT
    awayTeamName AS team_name
  FROM {{ source('baseball', 'games_wide') }}
)

SELECT DISTINCT
  LOWER(team_name) AS team_name,
  -- We'll use a hash to create a unique ID for each team
  md5(LOWER(team_name)) AS team_id
FROM all_teams