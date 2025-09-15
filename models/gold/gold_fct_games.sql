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

{% if is_incremental() %}
    AND g.game_date > coalesce((SELECT MAX(game_date) FROM {{ this }}), DATE '1900-01-01')
{% endif %}
