# etl/SQL_queries.py

import sys
import os

scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(scripts_path)





insert_opensky_flight_data = """
    INSERT INTO opensky_flight_data (
        icao24, callsign, origin_country, time_position, last_contact,
        longitude, latitude, baro_altitude, on_ground, velocity, true_track,
        vertical_rate, sensors, geo_altitude, squawk, spi, position_source
    ) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


avstack_dim_timezone = """
    INSERT INTO gold.as_dim_timezone_info (
        timezone
    )
    VALUES (%s)
    ON CONFLICT (timezone) DO NOTHING
    RETURNING timezone_id
"""


avstack_dim_airport = """
    INSERT INTO gold.as_dim_airport_info (
        airport_name,
        iata,
        icao,
        timezone_id
    )
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (iata) DO NOTHING 
    RETURNING airport_id
"""



avstack_dim_airline_info = """
    INSERT INTO gold.as_dim_airline_info (
        airline_iata,
        airline_icao,
        airline_name
    )
    VALUES (%s, %s, %s)
    ON CONFLICT (airline_iata) DO NOTHING
    RETURNING airline_id
"""



avstack_dim_flight_info = """
    INSERT INTO gold.as_dim_flight_info (
        flight_number,
        airline_id,
        flight_iata,
        flight_icao
    )
    VALUES (%s, %s, %s, %s)
    RETURNING flight_id
"""



avstack_fact_arr_dept_info = """
    INSERT INTO gold.as_fact_arr_dept_table (
        flight_id,
        dept_airport,
        arr_airport
    )
    VALUES (%s, %s, %s)
    RETURNING id
"""



avstack_fact_dept_info = """
    INSERT INTO gold.as_fact_departures_table (
        arr_dept_id,
        scheduled_dept,
        estimated_dept,
        actual_dept,
        terminal,
        gate,
        time_delay
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    RETURNING flight_event_id
"""



avstack_fact_arr_info = """
    INSERT INTO gold.as_fact_arrivals_table (
        flight_event_id,
        arr_dept_id,
        scheduled_arr,
        estimated_arr,
        actual_arr,
        terminal,
        gate,
        time_delay
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING flight_event_id
"""