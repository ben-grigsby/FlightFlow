# etl/SQL_functions.py

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
    INSERT INTO as_dim_timezone_info (
        timezone
    )
    VALUES (%s)
    ON CONFLICT (timezone) DO NOTHING
    RETURNING timezone_id
"""


avstack_dim_airport = """
    INSERT INTO as_dim_airport_info (
        airport_name,
        iata,
        icao,
        timezone_id
    )
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (iata) DO NOTHING 
    RETURNING airport_id
"""


