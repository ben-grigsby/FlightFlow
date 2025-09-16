# etl/bronze/opensky/bronze_load.py

import os, sys, json, psycopg2
import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin

scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(scripts_path)

from bronze.opensky.bronze_sql_queries import bronze_insert

def sanitize_row(row):
    return [None if (isinstance(x, float) and (math.isnan(x) or math.isinf(x))) else x for x in row]

def insert_into_bronze_ddl(ti, insert_query=bronze_insert):
    """
    Inserts raw Parquet data into opensky.bronze_ddl table
    """
    log = LoggingMixin().log

    # Pull filepath from XCom
    filepath = ti.xcom_pull(task_ids='run_kafka_consumer_slow')

    # Read Parquet
    df = pd.read_parquet(filepath)

    # Logging
    log.info(f"Loaded {len(df)} rows from Parquet: {filepath}")
    log.info("Beginning to extract and load data into bronze_info table.")

    # Connect to PostgreSQL
    hook = PostgresHook(postgre_conn_id='postgres_default')
    conn = hook.get_conn()
    cur = conn.cursor()

    # Iterate and insert
    for _, row in df.iterrows():
        row_list = row.tolist()
        row_json = json.dumps(row_list)
        full_row = row_list + [row_json]
        clean_row = sanitize_row(full_row)
        cur.execute(insert_query, clean_row)

    conn.commit()
    cur.close()
    conn.close()

    log.info("Completed inserting all rows into opensky.bronze_ddl.")