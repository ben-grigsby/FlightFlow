# etl/gold/gold_sql_queries.py

# import sys
# import os

# scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
# sys.path.append(scripts_path)





insert_opensky_flight_data = """
    INSERT INTO opensky_flight_data (
        icao24, callsign, origin_country, time_position, last_contact,
        longitude, latitude, baro_altitude, on_ground, velocity, true_track,
        vertical_rate, sensors, geo_altitude, squawk, spi, position_source
    ) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


gold_avstack_timezone_table = """
    INSERT INTO avstack.gold_dim_timezone_info
        (timezone)
    SELECT DISTINCT timezone
    FROM avstack.silver_dept_info 
    WHERE timezone IS NOT NULL

    UNION

    SELECT DISTINCT timezone 
    FROM avstack.silver_arr_info
    WHERE timezone IS NOT NULL
    
    ON CONFLICT (timezone) DO NOTHING;
"""


gold_avstack_airline_table = """
    INSERT INTO avstack.gold_dim_airline_info 
        (
            airline_iata,
            airline_icao,
            airline_name
        )
    
    SELECT DISTINCT
        airline_iata,
        airline_icao,
        airline_name
    FROM avstack.silver_flight_info;
"""


gold_avstack_airport_table = """
    INSERT INTO avstack.gold_dim_airport_info
        (
            airport_name,
            iata,
            icao,
            timezone_id
        )
    SELECT * 
    FROM (
        SELECT 
            s.airport,
            s.iata,
            s.icao,
            g.timezone_id
        FROM avstack.silver_dept_info s
        JOIN avstack.gold_dim_timezone_info g
            ON s.timezone = g.timezone
        
        UNION

        SELECT 
            s.airport,
            s.iata,
            s.icao,
            g.timezone_id
        FROM avstack.silver_arr_info s
        JOIN avstack.gold_dim_timezone_info g
            ON s.timezone = g.timezone
    ) AS airport_union
    ON CONFLICT (iata) DO NOTHING;
"""


gold_avstack_flight_table = """
    INSERT INTO avstack.gold_dim_flight_info
        (
            bronze_id,
            flight_number,
            airline_id,
            flight_iata,
            flight_icao
        )
    
    SELECT DISTINCT
        s.id,
        s.flight_number,
        g.airline_id,
        s.flight_iata,
        s.flight_icao
    FROM avstack.silver_flight_info s
    JOIN avstack.gold_dim_airline_info g
        ON s.airline_iata = g.airline_iata;
"""


gold_avstack_arr_dept_table = """
    INSERT INTO avstack.gold_fact_arr_dept_table 
        (
            bronze_id,
            flight_id,
            dept_airport,
            arr_airport
        )
    
    SELECT
        sd.id,
        gf.flight_id,
        gda.airport_id,
        gaa.airport_id
    FROM avstack.silver_dept_info sd
    JOIN avstack.silver_arr_info sa
        ON sd.id = sa.id
    JOIN avstack.gold_dim_flight_info gf
        ON gf.bronze_id = sd.id
    JOIN avstack.gold_dim_airport_info gda
        ON gda.airport_name = sd.airport
    JOIN avstack.gold_dim_airport_info gaa
        ON gaa.airport_name = sa.airport
"""


gold_avstack_dept_table = """
    INSERT INTO avstack.gold_fact_departures_table 
        (
            bronze_id,
            arr_dept_id,
            scheduled_dept,
            estimated_dept,
            actual_dept,
            terminal,
            gate,
            time_delay
        )
    SELECT
        s.id,
        g.id,
        s.scheduled_dept,
        s.estimated_dept,
        s.actual_dept,
        s.terminal,
        s.gate,
        s.dept_delay
    FROM avstack.silver_dept_info s
    JOIN avstack.gold_fact_arr_dept_table g
        ON s.id = g.bronze_id
"""


gold_avstack_arr_table = """
    INSERT INTO avstack.gold_fact_arrivals_table
        (
            flight_event_id,
            bronze_id,
            arr_dept_id,
            scheduled_arr,
            estimated_arr,
            actual_arr,
            terminal,
            gate,
            time_delay
        )
    SELECT * FROM (
    SELECT 
        gd.flight_event_id,
        s.id,
        gad.id,
        s.scheduled_arr,
        s.estimated_arr,
        s.actual_arr,
        s.terminal,
        s.gate,
        s.arr_delay
    FROM avstack.silver_arr_info s
    JOIN avstack.gold_fact_departures_table gd
        ON s.id = gd.bronze_id
    JOIN avstack.gold_fact_arr_dept_table gad
        ON s.id = gad.bronze_id
    ) AS sub
    ON CONFLICT (flight_event_id) DO NOTHING;
"""