# etl/postgre.py

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

with open('infra/init_db.sql', 'r') as f:
    sql = f.read()



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



def insert_av_dim_timezone(cur, conn, info, insert_quer):
    """
    Insert timezone data into as_dim_timezone_info table and retrieves the timezone 
    """

    cur.execute(insert_quer, info)
    result = cur.fetchone()


    if result:
        timezone_id = result[0]
    else:
        cur.execute(
            """
            SELECT timezone_id FROM as_dim_timezone_info WHERE timezone = %s
            """,
            (info,)
        )
        timezone_id = cur.fetchone()[0]
    
    conn.commit()
    return timezone_id



def insert_av_dim_airport(cur, conn, info, insert_quer, tz_id):
    """
    Insert airport information into as_dim_airport_info table and retrieves the airport_id
    """

    name = info['airport']
    iata = info['iata']
    icao = info['icao']

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
        airport_id = cur.fetchone()[0]
    
    conn.commit()
    return airport_id




def all_table_insertion(
        db, 
        user, 
        password, 
        filepaths, 
        data,
        timezone_quer, 
        airport_quer,
):
    """
    Calling necessary functions to input data into corresponding tables
    """

    cur, conn = connect_to_db(db, user, password)

    init_sql_ddl(cur, conn, filepaths)

    for f_dict in data:

        dept_timezone_info = (f_dict['departure']['timezone'],)
        dept_timezone_id = insert_av_dim_timezone(cur, conn, timezone_quer, dept_timezone_info)

        arr_timezone_info = (f_dict['arrival']['timezone'],)
        arr_timezone_id = insert_av_dim_timezone(cur, conn, timezone_quer, arr_timezone_info)

        dept_airport_info = f_dict['departure']
        dept_airport_id = insert_av_dim_airport(cur, conn, dept_airport_info, airport_quer, dept_timezone_id)

        arr_airport_info = f_dict['arrival']
        arr_airport_id = insert_av_dim_airport(cur, conn, arr_airport_info, airport_quer, arr_timezone_id)