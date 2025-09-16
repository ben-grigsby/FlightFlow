-- infra/avstack/avstack_silver_ddl.sql

CREATE TABLE IF NOT EXISTS avstack.silver_flight_info (
    id BIGINT,
    flight_iata TEXT,
    flight_date TIMESTAMP,
    flight_icao TEXT,
    flight_number BIGINT,
    flight_status TEXT,
    airline_name TEXT,
    airline_iata TEXT,
    airline_icao TEXT,
    aircraft TEXT,
    live TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_av_silver_flight_icao ON avstack.silver_flight_info (flight_icao);
CREATE INDEX idx_av_silver_airline_icao ON avstack.silver_flight_info (airline_icao);


CREATE TABLE IF NOT EXISTS avstack.silver_dept_info (
    id BIGINT,
    flight_id SERIAL PRIMARY KEY,
    flight_iata TEXT,
    flight_date TIMESTAMP,
    airport TEXT,
    timezone TEXT,
    iata TEXT,
    icao TEXT,
    terminal TEXT,
    gate TEXT,
    dept_delay BIGINT,
    scheduled_dept TIMESTAMP,
    estimated_dept TIMESTAMP,
    actual_dept TIMESTAMP,
    estimated_runway TIMESTAMP,
    actual_runway TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_av_silver_dept_icao ON avstack.silver_dept_info (icao);


CREATE TABLE IF NOT EXISTS avstack.silver_arr_info (
    id BIGINT,
    flight_id SERIAL PRIMARY KEY,
    flight_iata TEXT,
    flight_date TIMESTAMP,
    airport TEXT,
    timezone TEXT,
    iata TEXT,
    icao TEXT,
    terminal TEXT,
    gate TEXT,
    baggage TEXT,
    arr_delay BIGINT,
    scheduled_arr TIMESTAMP,
    estimated_arr TIMESTAMP,
    actual_arr TIMESTAMP,
    estimated_runway TIMESTAMP,
    actual_runway TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_av_silver_arr_icao ON avstack.silver_arr_info (icao);