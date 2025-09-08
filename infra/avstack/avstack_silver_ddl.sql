-- infra/avstack/avstack_silver_ddl.sql

CREATE TABLE IF NOT EXISTS avstack.silver_flight_info (
    flight_iata TEXT PRIMARY KEY,
    flight_date DATE,
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


CREATE TABLE IF NOT EXISTS avstack.silver_dept_info (
    flight_iata TEXT,
    flight_date DATE,
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


CREATE TABLE IF NOT EXISTS avstack.silver_arr_info (
    flight_iata TEXT,
    flight_date DATE,
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