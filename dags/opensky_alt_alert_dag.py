# dags/opensky_alt_alert_dag.py

from airflow import DAG 
from airflow.operators.bash import BashOperator
from datetime import datetime

import sys

# Allow Airflow to import from outside the dags folder
sys.path.append("/opt/airflow/scripts/opensky")

pid_path = "/opt/airflow/data/alerting.pid"

with DAG(
    'opensky_altitude_alert_dag',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['opensky']
) as dag:

    start_watching = BashOperator(
    task_id='start_watching',
    bash_command='nohup python3 /opt/airflow/scripts/opensky/bash_altitude_alert.py > /opt/airflow/data/logs/altitude_alert.log 2>&1 &' 
    )

    wait_8_hours = BashOperator(
        task_id='wait_8_hours',
        bash_command='sleep 300'
    )

    kill_alerting_task = BashOperator(
    task_id='kill_altitude_alert',
    bash_command='''
    if [ -f /opt/airflow/data/alerting.pid ]; then
        kill -9 $(cat /opt/airflow/data/alerting.pid) || true
        rm /opt/airflow/data/alerting.pid || true
        echo "Stopped alerting process."
    else
        echo "No alerting process to kill.";
    fi''',
    dag=dag
)

    start_watching >> wait_8_hours >> kill_alerting_task