{{ config(
    materialized='table',
    description='Gold dimension table for venues.'
) }}

SELECT
    venue_id,
    venue_name,
    venue_capacity,
    venue_state,
    venue_zip
FROM {{ ref('silver_venues') }}