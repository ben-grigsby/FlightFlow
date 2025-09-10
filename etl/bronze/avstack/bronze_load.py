# etl/bronze/avstack/bronze_load.py

import os, sys, json
import psycopg2

from airflow.hooks.postgres_hook import PostgresHook

# scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
# sys.path.append(scripts_path)

sys.path.append('/opt/airflow')

from etl.bronze.avstack.bronze_sql_queries import (
    bronze_insert
)

# ==================================================================
# Utility Functions
# ==================================================================

def insert_into_bronze_ddl(ti, insert_query=bronze_insert):
    """
    Inserts raw JSON data into avstack_bronze_ddl table
    """

    filepath = ti.xcom_pull(task_ids='run_kafka_consumer')

    hook = PostgresHook(postgre_conn_id='postgres_default')
    conn = hook.get_conn()
    cur = conn.cursor()

    print("[INFO] Reading data from JSON file")
    with open(filepath, "r") as f:
        info = json.load(f)

    print("[INFO] Beginning to extract and load data into bronze_info table.")
    if info:
        for data in info:
            dept_data = data['departure']
            arr_data = data['arrival']
            airline_data = data['airline']
            flight_data = data['flight']

            flight_date = data['flight_date']
            flight_status = data['flight_status']

            dept_airport = dept_data.get('airport')
            dept_timezone = dept_data.get('timezone')
            dept_iata = dept_data.get('iata')
            dept_icao = dept_data.get('icao')
            dept_terminal = dept_data.get('terminal')
            dept_gate = dept_data.get('gate')
            dept_delay = dept_data.get('delay')
            scheduled_dept = dept_data.get('scheduled')
            estimated_dept = dept_data.get('estimated')
            actual_dept = dept_data.get('actual')
            est_dept_runway = dept_data.get('estimated_runway')
            act_dept_runway = dept_data.get('actual_runway')

            arr_airport = arr_data.get('airport')
            arr_timezone = arr_data.get('timezone')
            arr_iata = arr_data.get('iata')
            arr_icao = arr_data.get('icao')
            arr_terminal = arr_data.get('terminal')
            arr_gate = arr_data.get('gate')
            arr_baggage = arr_data.get('baggage')
            scheduled_arr = arr_data.get('scheduled')
            arr_delay = arr_data.get('delay')
            estimated_arr = arr_data.get('estimated')
            actual_arr = arr_data.get('actual')
            est_arr_runway = arr_data.get('estimated_runway')
            act_arr_runway = arr_data.get('actual_runway')

            airline_name = airline_data.get('name')
            airline_iata = airline_data.get('iata')
            airline_icao = airline_data.get('icao')
            
            flight_num = flight_data.get('number')
            flight_iata = flight_data.get('iata')
            flight_icao = flight_data.get('icao')

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
                act_arr_runway,
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
        cur.close()
        conn.close()

        print("[INFO] Ingested raw JSON data into avstack.bronze_ddl table")

    else:
        print("[WARNING] No data has been given to enter into bronze table.")
        