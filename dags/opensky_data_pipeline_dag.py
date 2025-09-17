# dags/opensky_data_pipeline_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

import sys
import os

# Allow Airflow to import from outside the dags folder
sys.path.append("/opt/airflow/scripts/opensky")
sys.path.append("/opt/airflow/etl")

from kafka_consumer import run_kafka_consumer_slow

from bronze.opensky.bronze_load import insert_into_bronze_ddl


with DAG(
    'opensky_data_pipeline_dag',
    schedule_interval = '*/1 * * * *', # '5-59/5 9-18 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['opensky']
) as dag:

    run_kafka_consumer_slow_dag = PythonOperator(
        task_id='run_kafka_consumer_slow',
        python_callable=run_kafka_consumer_slow,
        do_xcom_push=True
    )

    run_bronze_load_ddl = PythonOperator(
        task_id='run_bronze_load',
        python_callable=insert_into_bronze_ddl,
        provide_context=True
    )

    dbt_run_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command='cd /opt/airflow/flightweather_dbt && dbt run --select tag:silver --profiles-dir .',
        dag=dag
    )

    dbt_run_gold = BashOperator(
        task_id='dbt_run_gold',
        bash_command='cd /opt/airflow/flightweather_dbt && dbt run --select tag:gold --profiles-dir .',
        dag=dag
    )
    
    run_kafka_consumer_slow_dag >> run_bronze_load_ddl >> dbt_run_silver >> dbt_run_gold

