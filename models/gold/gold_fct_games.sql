{{ config(
    materialized='incremental',
    unique_key='game_id',
    description='A central fact table that joins key metrics from silver tables for analytics.'
) }}

-- models/gold/gold_fct_games.sql
WITH games AS (
    SELECT *
    FROM {{ ref('silver_games') }}
),

schedules AS (
    SELECT *
    FROM {{ ref('silver_schedules') }}
)

SELECT
    -- Primary Key
    g.game_id,

    -- Foreign Keys
    s.home_team_id,
    s.away_team_id,
    g.venue_id,

    -- Degenerate Dimensions (descriptive fields that belong in the fact table)
    g.game_date,
    s.year,
    s.type,
    s.day_night,

    -- Measures (numeric values for aggregation)
    COALESCE(g.attendance, s.attendance) AS attendance,
    g.home_final_runs AS home_runs,
    g.away_final_runs AS away_runs,
    s.duration_minutes

FROM games g
JOIN schedules s
  ON g.game_id = s.game_id

SELECT
    home_teams.team_name AS home_team_name,
    away_teams.team_name AS away_team_name,
    v.venue_name
FROM {{ ref('silver_games') }} AS g
LEFT JOIN {{ ref('silver_schedules') }} AS s
    ON g.game_id = s.game_id
LEFT JOIN {{ ref('silver_teams') }} AS home_teams
    ON lower(g.home_team) = home_teams.team_name
LEFT JOIN {{ ref('silver_teams') }} AS away_teams
    ON lower(g.visitor_team) = away_teams.team_name
LEFT JOIN {{ ref('silver_venues') }} AS v
    ON g.venue_id = v.venue_id
WHERE g.game_id IS NOT NULL

{% if is_incremental() %}
    AND g.game_date > coalesce((SELECT MAX(game_date) FROM {{ this }}), DATE '1900-01-01')
{% endif %}