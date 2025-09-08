CREATE TABLE IF NOT EXISTS op_dim_flight_info (
    flight_id SERIAL PRIMARY KEY,
    icao TEXT,
    callsign TEXT
);


CREATE TABLE IF NOT EXISTS op_fact_flight_telemetry (
    telemtry_id SERIAL PRIMARY KEY,
    flight_id BIGINT REFERENCES op_dim_flight_info(flight_id),
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
    position_source BIGINT
);