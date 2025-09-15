-- infra/opensky/opensky_bronze_ddl.sql

CREATE TABLE IF NOT EXISTS opensky.bronze_info (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    icao24 TEXT,
    callsign TEXT,
    origin_country TEXT,
    time_position BIGINT,
    last_contact BIGINT,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    baro_altitude DOUBLE PRECISION,
    on_ground BOOLEAN,
    velocity DOUBLE PRECISION,
    true_track DOUBLE PRECISION,
    vertical_rate DOUBLE PRECISION,
    sensors BIGINT,
    geo_altitude DOUBLE PRECISION,
    squawk TEXT,
    spi BOOLEAN,
    position_source BIGINT,
    raw_json JSONB
);

CREATE INDEX idx_icao24_bronze ON opensky.bronze_info (icao24);