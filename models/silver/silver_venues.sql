{{ config(
    materialized='table',
    description='A unique list of all venues extracted from the games_wide table.'
) }}

SELECT DISTINCT
    venueId AS venue_id,
    VenueName AS venue_name,
    venueCapacity AS venue_capacity,
    venueCity as venue_city,
    venueState as venue_state,
    venueZip as venue_zip
FROM {{ source('baseball', 'games_wide') }}
WHERE venueId IS NOT NULL