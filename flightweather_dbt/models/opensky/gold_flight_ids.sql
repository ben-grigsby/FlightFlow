-- flightweather_dbt/models/opensky/gold_flight_ids.sql

{{ config(
    materialized='incremental',
    unique_key='bronze_id',
    tags=['gold'],
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_flight_ids_origin_country ON {{ this }} (origin_country)",
        "CREATE INDEX IF NOT EXISTS idx_flight_ids_icao24 ON {{ this }} (icao24)"
    ]
) }}

WITH base AS (
    SELECT
        *,
        LAG(silver_created_at) OVER (
            PARTITION BY icao24
            ORDER BY silver_created_at
        ) AS prev_time
    FROM {{ ref('silver_main_table') }}
),

marked AS (
    SELECT
        *,
        CASE 
            WHEN prev_time IS NULL THEN 'new'
            WHEN EXTRACT(EPOCH FROM (silver_created_at - prev_time)) > 3600 THEN 'new'
            ELSE 'dupe'
        END AS unique_status
    FROM base
)

SELECT *
FROM marked
WHERE unique_status = 'new'

{% if is_incremental() %}
AND bronze_created_at > (SELECT MAX(bronze_created_at) FROM {{ this }})
{% endif %}