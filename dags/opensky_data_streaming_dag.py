# dags/opensky_data_streaming_dag.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

import sys

# Allow Airflow to import from outside the dags folder
sys.path.append("/opt/airflow/scripts/opensky")
sys.path.append("/opt/airflow/etl")

with DAG(
    'opensky_data_streaming_dag',
    schedule_interval='0 9 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['opensky']
) as dag:
    
    start_streaming = BashOperator(
        task_id='start_streaming',
        bash_command='nohup python3 /opt/airflow/scripts/opensky/bash_streaming.py > /dev/null 2>&1 &'
    )

    wait_8_hours = BashOperator(
        task_id='wait_8_hours',
        bash_command='sleep 300'
    )

    stop_streaming = BashOperator(
        task_id='stop_streaming',
        bash_command='kill -9 $(cat /tmp/streaming.pid) && rm /tmp/streaming.pid || echo "No process to kill"'
    )

    start_streaming >> wait_8_hours >> stop_streaming