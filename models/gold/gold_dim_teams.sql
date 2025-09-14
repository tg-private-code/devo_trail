{{ config(
    materialized='table',
    description='Gold dimension table for teams.'
) }}

SELECT
    team_id,
    team_name
FROM {{ ref('silver_teams') }}