-- models/opensky/silver_secondary_table.sql

{{ config(materialized='incremental', tags=['silver']) }}

SELECT
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
    CURRENT_TIMESTAMP AS silver_created_at
FROM {{ source('opensky', 'bronze_info') }}

    {% if is_incremental() %}
    AND created_at > (SELECT MAX(bronze_created_at) FROM {{ this }})
    {% endif %}


{% do log("silver_secondary_table at " ~ run_started_at, info=True) %}