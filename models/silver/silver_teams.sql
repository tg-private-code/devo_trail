{{ config(
    materialized='table',
    description='A unique list of all teams extracted from the games_wide table.'
) }}

WITH all_teams AS (
  SELECT
    homeTeamName AS team_name,
    homeTeamId as team_id
  FROM {{ source('baseball', 'games_wide') }}
  UNION ALL
  SELECT
    awayTeamName AS team_name,
    awayTeamId as team_id
  FROM {{ source('baseball', 'games_wide') }}
)

SELECT DISTINCT
  LOWER(team_name) AS team_name,
  team_id
FROM all_teams