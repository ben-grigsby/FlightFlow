# etl/gold/gold_load.py

import os, sys, json
import psycopg2

from airflow.hooks.postgres_hook import PostgresHook


scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.append(scripts_path)


# from etl.sql_utilities import (
#     connect_to_db
# )

from etl.gold.avstack.gold_sql_queries import (
    gold_avstack_timezone_table,
    gold_avstack_airline_table,
    gold_avstack_airport_table,
    gold_avstack_flight_table,
    gold_avstack_arr_dept_table,
    gold_avstack_dept_table,
    gold_avstack_arr_table
)

DB = "flight_db"
USER = "user"
PASSWORD = "pass"


# cur, conn = connect_to_db(DB, USER, PASSWORD)

def load_gold():

    hook = PostgresHook(postgre_conn_id='postgres_default')
    conn = hook.get_conn()
    cur = conn.cursor()

    print("[INFO] Starting to load data into gold layer")
    try:
        print("Working on timezone")
        cur.execute(gold_avstack_timezone_table)
        conn.commit()
        print("Completed timezone")
    except Exception as e:
        print(f"[ERROR] Timezone table: {e}")

    try:
        print("Working on airline")
        cur.execute(gold_avstack_airline_table)
        conn.commit()
        print("Completed airline")
    except Exception as e:
        print(f"[ERROR] Airline table: {e}")

    try:
        print("Working on airport")
        cur.execute(gold_avstack_airport_table)
        conn.commit()
        print("Completed airport")
    except Exception as e:
        print(f"[ERROR] Airport table: {e}")

    try:
        print("Working on flight")
        cur.execute(gold_avstack_flight_table)
        conn.commit()
        print("Completed flight")
    except Exception as e:
        print(f"[ERROR] Flight table: {e}")

    try:
        print("Working on arr_dept_table")
        cur.execute(gold_avstack_arr_dept_table)
        conn.commit()
        print("Completed arr_dept table")
    except Exception as e:
        print(f"[ERROR] Arr_dept_table: {e}")

    try:
        print("Working on departure table")
        cur.execute(gold_avstack_dept_table)
        conn.commit()
        print("Completed departure table")
    except Exception as e:
        print(f"[ERROR] Departure table: {e}")

    try:
        print("Working on arrival table")
        cur.execute(gold_avstack_arr_table)
        conn.commit()
        print("Completed arrival table")
    except Exception as e:
        print(f"[ERROR] Arrival table: {e}")

    print("[INFO] Completed gold layer")