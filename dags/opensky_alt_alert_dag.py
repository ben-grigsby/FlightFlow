# dags/opensky_alt_alert_dag.py

from airflow import DAG 
from airflow.operators.bash import BashOperator
from datetime import datetime

import sys

# Allow Airflow to import from outside the dags folder
sys.path.append("/opt/airflow/scripts/opensky")

with DAG(
    'opensky_altitude_alert_dag',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['opensky']
) as dag:
    
    # start_watching = BashOperator(
    #     task_id='start_watching',
    #     bash_command='nohup python3 /opt/airflow/scripts/opensky/bash_altitude_alert.py > /dev/null 2>&1 &'
    # )

    start_watching = BashOperator(
    task_id='start_watching',
    bash_command='python3 /opt/airflow/scripts/opensky/bash_altitude_alert.py'
    )

    wait_8_hours = BashOperator(
        task_id='wait_8_hours',
        bash_command='sleep 300'
    )

    stop_watching = BashOperator(
        task_id='stop_streaming',
        bash_command='kill -9 $(cat /tmp/altitude_alert.pid) && rm /tmp/altitude_alert.pid || echo "No process to kill"'
    )

    start_watching >> wait_8_hours >> stop_watching