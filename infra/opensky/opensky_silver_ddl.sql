CREATE TABLE IF NOT EXISTS opensky.silver_main_table (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    icao24 TEXT NOT NULL,
    origin_country TEXT,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    baro_altitude DOUBLE PRECISION,
    velocity DOUBLE PRECISION,
    geo_altitude DOUBLE PRECISION
);

CREATE INDEX idx_icao24_main ON opensky.silver_main_table (icao24);


CREATE TABLE IF NOT EXISTS opensky.silver_secondary_table (
    id SERIAL PRIMARY KEY,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    callsign TEXT,
    time_position BIGINT,
    last_contact BIGINT,
    on_ground BOOLEAN,
    true_track DOUBLE PRECISION,
    vertical_rate DOUBLE PRECISION,
    sensors BIGINT,
    squawk TEXT,
    spi BOOLEAN,
    position_source BIGINT
);