# etl/bronze/avstack/bronze_load.py

import os, sys, json
import psycopg2

scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(scripts_path)

from etl.sql_utilities import (
    connect_to_db,
    try_except_wrapper,
)

from etl.bronze.avstack.bronze_sql_queries import (
    bronze_insert
)

# ==================================================================
# Utility Functions
# ==================================================================

def insert_into_bronze_ddl(cur, conn, info, insert_query=bronze_insert):
    """
    Inserts raw JSON data into avstack_bronze_ddl table
    """

    if info:
        for data in info:
            dept_data = data['departure']
            arr_data = data['arrival']
            airline_data = data['airline']
            flight_data = data['flight']

            flight_date = data['flight_date']
            flight_status = data['flight_status']

            dept_airport = dept_data['airport']
            dept_timezone = dept_data['timezone']
            dept_iata = dept_data['iata']
            dept_icao = dept_data['icao']
            dept_terminal = dept_data['terminal']
            dept_gate = dept_data['gate']
            dept_delay = dept_data['delay']
            scheduled_dept = dept_data['scheduled']
            estimated_dept = dept_data['estimated']
            actual_dept = dept_data['actual']
            est_dept_runway = dept_data['estimated_runway']
            act_dept_runway = dept_data['actual_runway']

            arr_airport = arr_data['airport']
            arr_timezone = arr_data['timezone']
            arr_iata = arr_data['iata']
            arr_icao = arr_data['icao']
            arr_terminal = arr_data['terminal']
            arr_gate = arr_data['gate']
            arr_baggage = arr_data['baggage']
            scheduled_arr = arr_data['scheduled']
            arr_delay = arr_data['delay']
            estimated_arr = arr_data['estimated']
            actual_arr = arr_data['actual']
            est_arr_runway = arr_data['estimated_runway']
            act_arr_runway = arr_data['actual_runway']

            airline_name = airline_data['name']
            airline_iata = airline_data['iata']
            airline_icao = airline_data['icao']
            
            flight_num = flight_data['number']
            flight_iata = flight_data['iata']
            flight_icao = flight_data['icao']

            aircraft = data['aircraft']
            live = data['live']

            lst = [
                flight_date,
                flight_status,
                dept_airport,
                dept_timezone,
                dept_iata,
                dept_icao,
                dept_terminal,
                dept_gate,
                dept_delay,
                scheduled_dept,
                estimated_dept,
                actual_dept,
                est_dept_runway,
                act_dept_runway,
                arr_airport,
                arr_timezone,
                arr_iata,
                arr_icao,
                arr_terminal,
                arr_gate,
                arr_baggage,
                scheduled_arr,
                arr_delay,
                estimated_arr,
                actual_arr,
                est_arr_runway,
                actual_arr,
                airline_name,
                airline_iata,
                airline_icao,
                flight_num,
                flight_iata,
                flight_icao,
                aircraft,
                live
            ]

            cur.execute(insert_query, lst)

            conn.commit()

            print("[INFO] Ingested raw JSON data into avstack.bronze_ddl table")

    else:
        print("[WARNING] No data has been given to enter into bronze table.")
        