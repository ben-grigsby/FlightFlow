# etl/bronze/opensky/bronze_load.py

import os, sys, json, psycopg2

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin

scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(scripts_path)

from bronze.opensky.bronze_sql_queries import(
    bronze_insert
)

def insert_into_bronze_ddl(ti, insert_query=bronze_insert):
    """
    Inserts raw JSON data into opensky.bronze_ddl table
    """
    log = LoggingMixin().log

    filepath = ti.xcom_pull(task_ids='run_kafka_consumer_slow')

    hook = PostgresHook(postgre_conn_id='postgres_default')
    conn = hook.get_conn()
    cur = conn.cursor()

    with open(filepath, "r") as f:
        info = json.load(f)

    log.info("Beginning to extract and load data into bronze_info table.")

    for entry in info:
        full_row = entry + [json.dumps(entry)]
        cur.execute(bronze_insert, full_row)
    
    log.info("Completed extracting and loading data int bronze_info table.")

    conn.commit()
    cur.close()
    conn.close()