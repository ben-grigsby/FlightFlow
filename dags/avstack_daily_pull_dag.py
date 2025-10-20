# dags/avstack_daily_pull_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator, BashOperator
from datetime import datetime

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
    start_date=datetime(2025, 10, 20),
    catchup=False,
    tags=['avstack_daily']
) as dag:

    kafka_producer = PythonOperator(
        task_id='run_kafka_producer',
        pythoncallable=run_kafka_producer_historic,
        op_kwargs={
            "start_date": datetime.now().strftime("%Y-%m-%d"),
            "end_date": datetime.now().strftime("%Y-%m-%d"),
            "limit": 1000,
            "daily_cap": 90000
        }
    )

    kafka_consumer = PythonOperator(
        task_id='run_kafka_consumer',
        pythoncallable=run_kafka_consumer
    )

    json_to_parquet = PythonOperator(
        task_id='process_json_to_parquet',
        pythoncallable=process_json_files
    )

    update_iceberg = BashOperator(
        task_id='update_iceberg_table',
        bash_command="python3 /opt/airflow/etl/update_iceberg_table.py"
    )

    delete_json_files = BashOperator(
        # COMPLETE THE BASHOPERATOR CODE FOR THIS SECTION!!
    )
