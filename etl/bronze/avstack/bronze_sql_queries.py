# etl/bronze/avstack/bronze_sql_queries.py

bronze_insert = """
    INSERT INTO avstack.bronze_info (
        flight_date,
        flight_status,
        dept_airport,
        dept_timezone, 
        dept_airport_iata,
        dept_airport_icao,
        dept_airport_terminal,
        dept_airport_gate,
        dept_delay,
        scheduled_dept,
        estimated_dept,
        actual_dept,
        estimated_dept_runway,
        actual_dept_runway,
        arr_airport,
        arr_timezone,
        arr_airport_iata,
        arr_airport_icao,
        arr_airport_terminal,
        arr_airport_gate,
        arr_airport_baggage,
        scheduled_arr,
        arr_delay,
        estimated_arr,
        actual_arr,
        estimated_arr_runway,
        actual_arr_runway,
        airline_name,
        airline_iata,
        airline_icao,
        flight_number,
        flight_iata,
        flight_icao,
        aircraft,
        live
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
"""