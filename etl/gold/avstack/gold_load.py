# etl/gold/gold_load.py

import os, sys, json
import psycopg2


scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.append(scripts_path)


from etl.sql_utilities import (
    connect_to_db
)

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


cur, conn = connect_to_db(DB, USER, PASSWORD)

try:
    print("Working on timezone")
    cur.execute(gold_avstack_timezone_table)
    conn.commit()
    print("Completed timezone")
except Exception as e:
    print(f"Error with timezone table: {e}")

try:
    print("Working on airline")
    cur.execute(gold_avstack_airline_table)
    conn.commit()
    print("Completed airline")
except Exception as e:
    print(f"Error with airline table: {e}")

try:
    print("Working on airport")
    cur.execute(gold_avstack_airport_table)
    conn.commit()
    print("Completed airport")
except Exception as e:
    print(f"Error with airport: {e}")

try:
    print("Working on flight")
    cur.execute(gold_avstack_flight_table)
    conn.commit()
    print("Completed flight")
except Exception as e:
    print(f"Error with flight: {e}")

try:
    print("Working on arr_dept_table")
    cur.execute(gold_avstack_arr_dept_table)
    conn.commit()
    print("Completed arr_dept table")
except Exception as e:
    print(f"Error with arr_dept_table: {e}")

try:
    print("Working on departure table")
    cur.execute(gold_avstack_dept_table)
    conn.commit()
    print("Completed departure table")
except Exception as e:
    print(f"Error with departure table: {e}")

try:
    print("Working on arrival table")
    cur.execute(gold_avstack_arr_table)
    conn.commit()
    print("Completed arrival table")
except Exception as e:
    print(f"Error with arrival table")

