# dags/avstack_daily_pull_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

import sys

sys.path.append("/opt/airflow/scripts/")
sys.path.append("/opt/airflow/etl/")
sys.path.append("/opt/airflow")

from avstack.avstack_kafka_producer import run_kafka_producer_historic
from avstack.avstack_kafka_consumer import run_kafka_consumer
from silver.silver_parquet_historic import process_json_files

with DAG(
    'avstack_daily_data',
    schedule_interval='0 2 * * * ',
    start_date=datetime(2025, 10, 28),
    catchup=False,
    tags=['avstack_daily']
) as dag:

    kafka_producer = PythonOperator(
        task_id='run_kafka_producer',
        python_callable=run_kafka_producer_historic,
        op_kwargs={  
            "start_date": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            "end_date": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            "limit": 1000,
            "daily_cap": 90000
        },
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True
    )

    kafka_consumer = PythonOperator(
        task_id='run_kafka_consumer',
        python_callable=run_kafka_consumer
    )

    json_to_parquet = PythonOperator(
        task_id='process_json_to_parquet',
        python_callable=process_json_files
    )

    update_iceberg = BashOperator(
        task_id='update_iceberg_table',
        bash_command="python3 /opt/airflow/etl/iceberg/update_iceberg_table.py"
    )

    # delete_json = BashOperator(
    # task_id="delete_json_files",
    # bash_command="python3 /Users/bengrigsby/Desktop/Jobs/flightweather/etl/delete_json_files.py",
    # )

    kafka_producer >> kafka_consumer >> json_to_parquet >> update_iceberg # >> delete_json