-- models/opensky/gold_flight_stats_by_country.sql

{{ config(
    materialized='table',
    tags=['gold']
) }}

SELECT
    origin_country,
    DATE_TRUNC('hour', silver_created_at) AS hour,
    COUNT(*) AS num_flights,
    AVG(velocity) AS avg_velocity,
    AVG(geo_altitude) AS avg_geo_altitude,
    MAX(bronze_created_at) AS latest_data_point
FROM {{ ref('silver_main_table') }}
GROUP BY origin_country, hour
ORDER BY hour DESC, num_flights DESC



{% do log("gold_flight_stats_by_country last updated from silver at: " ~ run_started_at, info=True) %}