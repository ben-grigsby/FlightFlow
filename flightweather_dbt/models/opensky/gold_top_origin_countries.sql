-- flightweather_dbt/models/opensky/gold_top_origin_countries.sql

{{ config(
    materialized='table',
    tags=['gold']
) }}

WITH country_flights AS (
    SELECT
        origin_country,
        COUNT(*) AS flight_count
    FROM {{ ref('gold_flight_ids') }}
    GROUP BY origin_country
),

ranked AS (
    SELECT
        *,
        RANK() OVER (ORDER BY flight_count DESC) AS rank
    FROM country_flights
)

SELECT
    origin_country,
    flight_count
FROM ranked
WHERE rank <= 10
ORDER BY flight_count DESC