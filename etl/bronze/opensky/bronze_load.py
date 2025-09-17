# etl/bronze/opensky/bronze_load.py

import os, sys, json, psycopg2, math, logging
import pandas as pd


from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin

scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(scripts_path)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
log_file_r = "/opt/airflow/data/logs/data_pipeline.log"
os.makedirs(os.path.dirname(log_file_r), exist_ok=True)

from bronze.opensky.bronze_sql_queries import bronze_insert

def sanitize_row(row):
    return [None if (isinstance(x, float) and (math.isnan(x) or math.isinf(x))) else x for x in row]


def get_data_receiving_logger():
    logger = logging.getLogger("bronze_logger")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fh = logging.FileHandler(log_file_r)
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger

def insert_into_bronze_ddl(ti, insert_query=bronze_insert):
    """
    Inserts raw Parquet data into opensky.bronze_ddl table
    """
    logger = get_data_receiving_logger()
    logger.info("Starting bronze load process...")

    # Pull filepath from XCom
    filepath = ti.xcom_pull(task_ids='run_kafka_consumer_slow')

    # Read Parquet
    df = pd.read_parquet(filepath)

    # Logging
    logger.info(f"[BRONZE] Loaded {len(df)} rows from Parquet: {filepath}")
    logger.info("[BRONZE] Beginning to extract and load data into bronze_info table.")

    # Connect to PostgreSQL
    hook = PostgresHook(postgre_conn_id='postgres_default')
    conn = hook.get_conn()
    cur = conn.cursor()

    # Iterate and insert
    for _, row in df.iterrows():
        # row_list = row.tolist()
        # row_json = json.dumps(row_list)
        # full_row = row_list + [row_json]
        # clean_row = sanitize_row(full_row)
        # try:
        #     cur.execute(insert_query, clean_row)
        # except Exception as e:
        #     logger.error(f"Error inserting row into PostgreSQL: {e}")
        #     logger.debug(f"Row content: {clean_row}")

        # Sanitize first
        row_list = row.tolist()
        sanitized_row = sanitize_row(row_list)

        # Now JSON-encode sanitized row
        try:
            row_json = json.dumps(sanitized_row, allow_nan=False)
        except ValueError as e:
            logger.error(f"JSON encoding failed for row: {sanitized_row} — {e}")
            continue  # Skip this row entirely

        # Add JSON column and insert
        full_row = sanitized_row + [row_json]

        try:
            cur.execute(insert_query, full_row)
        except Exception as e:
            logger.error(f"❌ Error inserting row into PostgreSQL: {e}")
            logger.debug(f"Row content: {full_row}")
            conn.rollback()
            continue

    conn.commit()
    cur.close()
    conn.close()

    logger.info("[BRONZE] Completed inserting all rows into opensky.bronze_ddl.")