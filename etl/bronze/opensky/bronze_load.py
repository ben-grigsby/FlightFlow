# etl/bronze/opensky/bronze_load.py

import os, sys, json, psycopg2

from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin

scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(scripts_path)

from bronze.opensky.bronze_sql_queries import(
    bronze_insert
)

log = LoggingMixin().log

conn = psycopg2.connect(
    host="postgres",     
    port=5432,                
    database="flight_db",
    user="user",
    password="pass"
)

cur = conn.cursor()

filepath = "data/kafka_logs/opensky/as_message_20250915_015207.json"

with open(filepath, "r") as f:
    info = json.load(f)

log.info("Beginning to extract and load data into bronze_info table.")

for entry in info:
    full_row = entry + [json.dumps(entry)]
    cur.execute(bronze_insert, full_row)

conn.commit()
cur.close()
conn.close()