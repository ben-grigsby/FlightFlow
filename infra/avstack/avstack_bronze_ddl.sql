-- infra/avstack/avstack_bronze_ddl.sql

CREATE TABLE IF NOT EXISTS avstack.bronze_info (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    flight_date TEXT,
    flight_status TEXT, -- 
    dept_airport TEXT, --
    dept_timezone TEXT, --
    dept_airport_iata TEXT, --
    dept_airport_icao TEXT, --
    dept_airport_terminal TEXT, --
    dept_airport_gate TEXT, --
    dept_delay TEXT, --
    scheduled_dept TEXT, --
    estimated_dept TEXT, --
    actual_dept TEXT, --
    estimated_dept_runway TEXT, --
    actual_dept_runway TEXT, -- 
    arr_airport TEXT, --
    arr_timezone TEXT, --
    arr_airport_iata TEXT, --
    arr_airport_icao TEXT, --
    arr_airport_terminal TEXT, -- 
    arr_airport_gate TEXT, --
    arr_airport_baggage TEXT, --
    scheduled_arr TEXT, --
    arr_delay TEXT, --
    estimated_arr TEXT, --
    actual_arr TEXT, --
    estimated_arr_runway TEXT, --
    actual_arr_runway TEXT, --
    airline_name TEXT, --
    airline_iata TEXT, --
    airline_icao TEXT, --
    flight_number TEXT, --
    flight_iata TEXT, --
    flight_icao TEXT, --
    aircraft TEXT, --
    live TEXT, --
    raw_json JSONB
);

CREATE INDEX idx_dept_airport_icao ON avstack.bronze_info (dept_airport_icao);
CREATE INDEX idx_arr_airport_icao ON avstack.bronze_info (arr_airport_icao);
CREATE INDEX idx_airline_icao ON avstack.bronze_info (airline_icao);
CREATE INDEX idx_flight_icao ON avstack.bronze_info (flight_icao);