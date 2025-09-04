CREATE TABLE IF NOT EXISTS as_dim_timezone_info (
    timezone_id SERIAL PRIMARY KEY,
    timezone TEXT
);


CREATE TABLE IF NOT EXISTS as_dim_airport_info (
    airport_id SERIAL PRIMARY KEY,
    airport_name TEXT,
    iata TEXT UNIQUE,
    icao TEXT UNIQUE,
    timezone_id BIGINT REFERENCES as_dim_timezone_info(timezone_id)
);


CREATE TABLE IF NOT EXISTS as_dim_flight_info (
    flight_id SERIAL PRIMARY KEY,
    flight_number BIGINT,
    airline_id BIGINT REFERENCES as_dim_airline_info(airline_id),
    flight_iata TEXT,
    flight_icao TEXT
);


CREATE TABLE IF NOT EXISTS as_dim_airline_info (
    airline_id SERIAL PRIMARY KEY,
    airline_iata TEXT,
    airline_icao TEXT,
    airline_name TEXT
);


CREATE TABLE IF NOT EXISTS as_fact_arr_dept_table (
    id SERIAL PRIMARY KEY,
    flight_id BIGINT REFERENCES as_dim_flight_info(flight_id),
    dept_airport BIGINT REFERENCES as_dim_airport_info(airport_id),
    arr_airport BIGINT REFERENCES as_dim_airport_info(airport_id)
);


CREATE TABLE IF NOT EXISTS as_fact_departures_table (
    flight_event_id SERIAL PRIMARY KEY,
    arr_dept_id BIGINT REFERENCES as_fact_arr_dept_table(id),
    scheduled_dept TEXT,
    estimated_dept TEXT,
    actual_dept TEXT,
    terminal TEXT,
    gate TEXT,
    time_delay BIGINT
);


CREATE TABLE IF NOT EXISTS as_fact_arrivals_table (
    flight_event_id BIGINT PRIMARY KEY REFERENCES as_fact_departures_table(flight_event_id),
    arr_dept_id BIGINT REFERENCES as_fact_arr_dept_table(id),
    scheduled_arr TEXT,
    estimated_arr TEXT,
    actual_arr TEXT,
    terminal TEXT,
    gate TEXT,
    time_delay BIGINT
);