-- models/opensky/silver_main_table.sql

{{ config(materialized='incremental', tags=['silver']) }}

SELECT
    icao24,
    origin_country,
    longitude,
    latitude,
    baro_altitude,
    velocity,
    geo_altitude,
    id as bronze_id,
    created_at AS bronze_created_at,
    CURRENT_TIMESTAMP AS silver_created_at,
    CONCAT(icao24, '_', TO_CHAR(created_at, 'YYYYMMDD_HH24')) AS flight_id
FROM {{ source('opensky', 'bronze_info') }}
WHERE 
    icao24 IS NOT NULL
    AND longitude IS NOT NULL
    AND latitude IS NOT NULL
    {% if is_incremental() %}
    AND created_at > (SELECT MAX(bronze_created_at) FROM {{ this }})
    {% endif %}



{% do log("silver_main_table at " ~ run_started_at, info=True) %}