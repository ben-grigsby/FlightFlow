-- infra/avstack/avstack_gold_ddl.sql

CREATE TABLE IF NOT EXISTS avstack.gold_dim_timezone_info (
    timezone_id SERIAL PRIMARY KEY,
    timezone TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS avstack.gold_dim_airport_info (
    airport_id SERIAL PRIMARY KEY,
    airport_name TEXT NOT NULL,
    iata TEXT UNIQUE,
    icao TEXT UNIQUE,
    timezone_id BIGINT NOT NULL REFERENCES avstack.gold_dim_timezone_info(timezone_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS avstack.gold_dim_airline_info (
    airline_id SERIAL PRIMARY KEY,
    airline_iata TEXT NOT NULL,
    airline_icao TEXT,
    airline_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS avstack.gold_dim_flight_info (
    flight_id SERIAL PRIMARY KEY,
    bronze_id BIGINT,
    flight_number TEXT,
    flight_date TIMESTAMP,
    airline_id BIGINT NOT NULL REFERENCES avstack.gold_dim_airline_info(airline_id),
    flight_iata TEXT,
    flight_icao TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS avstack.gold_fact_arr_dept_table (
    id SERIAL PRIMARY KEY,
    bronze_id BIGINT,
    flight_id BIGINT NOT NULL REFERENCES avstack.gold_dim_flight_info(flight_id),
    dept_airport BIGINT NOT NULL REFERENCES avstack.gold_dim_airport_info(airport_id),
    arr_airport BIGINT NOT NULL REFERENCES avstack.gold_dim_airport_info(airport_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS avstack.gold_fact_departures_table (
    flight_event_id SERIAL PRIMARY KEY,
    bronze_id BIGINT,
    arr_dept_id BIGINT NOT NULL REFERENCES avstack.gold_fact_arr_dept_table(id),
    scheduled_dept TIMESTAMP,
    estimated_dept TIMESTAMP,
    actual_dept TIMESTAMP,
    terminal TEXT,
    gate TEXT,
    time_delay BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS avstack.gold_fact_arrivals_table (
    flight_event_id BIGINT PRIMARY KEY REFERENCES avstack.gold_fact_departures_table(flight_event_id),
    bronze_id BIGINT,
    arr_dept_id BIGINT REFERENCES avstack.gold_fact_arr_dept_table(id),
    scheduled_arr TIMESTAMP,
    estimated_arr TIMESTAMP,
    actual_arr TIMESTAMP,
    terminal TEXT,
    gate TEXT,
    time_delay BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);