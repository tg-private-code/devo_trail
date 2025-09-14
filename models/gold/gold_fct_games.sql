{{ config(
    materialized='incremental',
    unique_key='game_id',
    description='A central fact table that joins key metrics from silver tables for analytics.'
) }}

SELECT
    g.game_id,
    g.game_date,
    g.game_time,
    g.attendance,
    g.home_final_runs as home_runs,
    g.away_final_runs as visitor_runs,
    s.year,
    s.type,
    s.day_night,
    home_teams.team_id AS home_team_id,
    home_teams.team_name AS home_team_name,
    away_teams.team_id AS away_team_id,
    away_teams.team_name AS away_team_name,
    v.venue_id,
    v.venue_name
FROM {{ ref('silver_games') }} AS g
LEFT JOIN {{ ref('silver_schedules') }} AS s
    ON g.game_id = s.game_id
LEFT JOIN {{ ref('silver_teams') }} AS home_teams
    ON g.home_team = home_teams.team_name
LEFT JOIN {{ ref('silver_teams') }} AS away_teams
    ON g.visitor_team = away_teams.team_name
LEFT JOIN {{ ref('silver_venues') }} AS v
    ON g.venue_id = v.venue_id
WHERE g.game_id IS NOT NULL

{% if is_incremental() %}
    WHERE g.game_date > (SELECT MAX(game_date) FROM {{ this }})
{% endif %}