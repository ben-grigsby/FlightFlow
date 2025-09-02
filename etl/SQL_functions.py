# etl/SQL_functions.py

insert_opensky_flight_data = """
    INSERT INTO flight_data (
        icao24, callsign, origin_country, time_position, last_contact,
        longitude, latitude, baro_altitude, on_ground, velocity, true_track,
        vertical_rate, sensors, geo_altitude, squawk, spi, position_source
    ) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""