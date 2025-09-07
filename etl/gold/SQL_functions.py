# etl/SQL_functions.py

import os, sys, json
import psycopg2


scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(scripts_path)


def connect_to_db(db, user, password):
    """
    Creates the connector linked to a specified database
    """

    conn = psycopg2.connect(
        dbname=db,
        user=user,
        password=password,
        host="postgres",
        port="5432"
    )

    cur = conn.cursor()

    return cur, conn

# with open('infra/init_db.sql', 'r') as f:
#     sql = f.read()



def try_except_wrapper(fn, conn, *args, key_name=""):
    try:
        return fn(*args)
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Failed to insert {key_name}: {e}")
        return None



def init_sql_ddl(cur, conn, filepaths):
    """
    Initialize the SQL table schemas
    """
    
    for path in filepaths:

        with open(path, 'r') as f:
            sql = f.read()
        
        cur.execute(sql)
        conn.commit()
    
        print(f"Executed schema init: ", path)
    
    print(f"Executed all schemas.")



def insert_av_dim_timezone(cur, conn, insert_quer, info):
    """
    Insert timezone data into as_dim_timezone_info table and retrieves the timezone 
    """

    if info:
        cur.execute(insert_quer, info)
        result = cur.fetchone()


        if result:
            timezone_id = result[0]
        else:
            cur.execute(
                """
                SELECT timezone_id FROM as_dim_timezone_info WHERE timezone = %s
                """,
                (info[0],)
            )
            timezone_id = cur.fetchone()[0]
        
        conn.commit()
        return timezone_id

    else:
        print("[WARNING] Skipping timezone insert: timezone is None")



def insert_av_dim_airport(cur, conn, info, insert_quer, tz_id):
    """
    Insert airport information into as_dim_airport_info table and retrieves the airport_id
    """

    name = info['airport']
    iata = info['iata']
    icao = info['icao']

    if not iata:
        print(f"[WARNING] Skipping airport insert: missing IATA code in {info}")
        return None

    airport_info = [name, iata, icao, tz_id]

    cur.execute(insert_quer, airport_info)
    result = cur.fetchone()

    if result:
        airport_id = result[0]
    else:
        cur.execute(
            """
            SELECT airport_id FROM as_dim_airport_info WHERE iata = %s
            """,
            (iata,)
        )
        row = cur.fetchone()
        if row:
            airport_id = row[0]
        else:
            print(f"[ERROR] Airport not found after insert/select: {iata}")
            return None
    
    conn.commit()
    return airport_id


def insert_av_dim_airline(cur, conn, info, insert_quer):
    """
    Inerts airline information into as_dim_airline_info and retrieves the airline_id
    """

    airline_name = info['name']
    airline_iata = info['iata']
    airline_icao = info['icao']

    airline_info = [airline_name, airline_iata, airline_icao]

    print(airline_info)

    cur.execute(insert_quer, airline_info)
    result = cur.fetchone()

    if result:
        airline_id = result[0]
    else:
        cur.execute(
            """
            SELECT airline_id from as_dim_airline_info WHERE airline_iata = %s
            """,
            (airline_iata, )
        )
        airline_id = cur.fetchone()[0]
    
    conn.commit()
    return airline_id



def insert_av_dim_flight_info(cur, conn, info, insert_quer, airline_id):
    """
    Inserts flight information into as_dim_flight_info and retrieves flight_id
    """

    flight_number = info["number"]
    flight_iata = info["iata"]
    flight_icao = info["icao"]

    flight_info = [flight_number, airline_id, flight_iata, flight_icao]

    cur.execute(insert_quer, flight_info)
    result = cur.fetchone()

    if result:
        flight_id = result[0]
    else:
        flight_id = None
        print("No flight_id to return.")
    
    conn.commit()
    return flight_id



def insert_av_fact_arr_dept_info(cur, conn, insert_quer, flight_id, dept_airport_id, arr_airport_id):
    """
    Inserts the basic arrival and departure information into as_fact_arr_dept_table and retrieves the entry ID
    """

    arr_dept_info = [flight_id, dept_airport_id, arr_airport_id]

    cur.execute(insert_quer, arr_dept_info)
    result = cur.fetchone()

    if result:
        arr_dept_id = result[0]
    else:
        arr_dept_id = None
        print("No arr_dept_id to return")
    
    conn.commit()
    return arr_dept_id



def insert_av_fact_dept_info(cur, conn, insert_quer, info, arr_dept_id):
    """
    Inserts the full departure information into as_fact_departures_table and retrieves the flight_event_id
    """

    scheduled_dept = info['scheduled']
    estimated_dept = info['estimated']
    actual_dept = info['actual']
    terminal = info['terminal']
    gate = info['gate']
    delay = info['delay']

    flight_dept_info = [arr_dept_id, scheduled_dept, estimated_dept, actual_dept, terminal, gate, delay]

    cur.execute(insert_quer, flight_dept_info)
    result = cur.fetchone()

    if result:
        flight_event_id = result[0]
    else:
        flight_event_id = None
        print("No flight_event_id to return")
    
    conn.commit()
    return flight_event_id



def insert_av_fact_arr_info(cur, conn, insert_quer, info, arr_dept_id, flight_event_id):
    """
    Inserts the full arrival information into as_fact_arrivals_table and retrieves the flight_event_id
    """

    scheduled_arr = info['scheduled']
    estimated_arr = info['estimated']
    actual_arr = info['actual']
    terminal = info['terminal']
    gate = info['gate']
    delay = info['delay']

    flight_arr_info = [flight_event_id, arr_dept_id, scheduled_arr, estimated_arr, actual_arr, terminal, gate, delay]


    cur.execute(insert_quer, flight_arr_info)
    result = cur.fetchone()

    if result:
        flight_event_id = result[0]
    else:
        flight_event_id = None
        print("No flight_event_id to return")
    
    conn.commit()
    return flight_event_id
    



def all_table_insertion(
        db, 
        user, 
        password, 
        # filepaths, 
        data,
        timezone_quer, 
        airport_quer,
        airline_quer,
        flight_quer,
        arr_dept_quer,
        departure_quer,
        arrival_quer,
):
    """
    Calling necessary functions to input data into corresponding tables
    """

    cur, conn = connect_to_db(db, user, password)

    # init_sql_ddl(cur, conn, filepaths)

    ids = []
    i = 0

    for f_dict in data:

        dept_timezone_info = (f_dict['departure']['timezone'],)
        dept_timezone_id = insert_av_dim_timezone(cur, conn, timezone_quer, dept_timezone_info)
        arr_timezone_info = (f_dict['arrival']['timezone'],)
        arr_timezone_id = insert_av_dim_timezone(cur, conn, timezone_quer, arr_timezone_info)

        dept_info = f_dict['departure']
        dept_airport_id = try_except_wrapper(insert_av_dim_airport, conn, cur, conn, dept_info, airport_quer, dept_timezone_id, key_name="airport_id")
        arr_info = f_dict['arrival']
        arr_airport_id = try_except_wrapper(insert_av_dim_airport, conn, cur, conn, arr_info, airport_quer, arr_timezone_id, key_name="airport_id")

        airline_info = f_dict['airline']
        airline_id = try_except_wrapper(insert_av_dim_airline, conn, cur, conn, airline_info, airline_quer, key_name="airline_id")

        flight_info = f_dict['flight']
        flight_id = try_except_wrapper(insert_av_dim_flight_info, conn, cur, conn, flight_info, flight_quer, airline_id, key_name="flight_id")

        arr_dept_id = try_except_wrapper(insert_av_fact_arr_dept_info, conn, cur, conn, arr_dept_quer, flight_id, dept_airport_id, arr_airport_id, key_name="id")

        flight_event_id_dept = try_except_wrapper(insert_av_fact_dept_info, conn, cur, conn, departure_quer, dept_info, arr_dept_id, key_name="flight_event_id")
        flight_event_id_arr = try_except_wrapper(insert_av_fact_arr_info, conn, cur, conn, arrival_quer, arr_info, arr_dept_id, flight_event_id_dept, key_name="flight_event_id")

        coll_ids = (i, dept_timezone_id, arr_timezone_id, dept_airport_id, arr_airport_id, airline_id, flight_id, arr_dept_id, flight_event_id_dept, flight_event_id_arr)
        ids.append(coll_ids)
    
    print("Completed loading data into PostgreSQL tables.")
    print(f"Sample ids: ", ids[0])

    return ids