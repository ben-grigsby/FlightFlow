-- models/opensky/silver_secondary_table.sql

{{ config(materialized='incremental', tags=['silver']) }}

SELECT
    icao24,
    callsign,
    time_position,
    last_contact,
    on_ground,
    true_track,
    vertical_rate,
    sensors,
    squawk,
    spi,
    position_source,
    created_at AS bronze_created_at,
    id AS bronze_id,
    CURRENT_TIMESTAMP AS silver_created_at,
    CONCAT(icao24, '_', TO_CHAR(created_at, 'YYYYMMDD_HH24')) AS flight_id
FROM {{ source('opensky', 'bronze_info') }}
WHERE
    icao24 IS NOT NULL
    {% if is_incremental() %}
    AND created_at > (SELECT MAX(bronze_created_at) FROM {{ this }})
    {% endif %}

{% do log("silver_secondary_table at " ~ run_started_at, info=True) %}